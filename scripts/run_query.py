from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from pathlib import Path

from hybrid_utils import DEFAULT_DB_PATH, DEFAULT_INDEX_DIR, connect_db, fetch_distinct_values, make_snippet
from routing_engine import (
    IndexLookupError,
    RouteDecision,
    detect_document_signals,
    estimate_io,
    route_query,
    run_faiss_product_search,
    run_vector_review_search,
)


TOP_RATED_PHRASES = ["top rated", "best rated", "highest rated"]
UNDER_PHRASES = ["under", "below", "less than"]
OVER_PHRASES = ["over", "above", "greater than"]
RATING_PHRASES = ["rating", "rated", "star", "stars"]
PRICE_HINTS = ["price", "prices", "dollar", "dollars", "cost", "costs"]
MIXED_CANDIDATE_LIMIT = 100

SEARCH_STOPWORDS = {
    "a",
    "an",
    "and",
    "any",
    "above",
    "best",
    "below",
    "by",
    "cost",
    "costs",
    "dollar",
    "dollars",
    "find",
    "for",
    "from",
    "in",
    "item",
    "items",
    "less",
    "me",
    "of",
    "on",
    "or",
    "price",
    "prices",
    "product",
    "products",
    "rated",
    "rating",
    "review",
    "reviews",
    "show",
    "something",
    "that",
    "than",
    "the",
    "to",
    "greater",
    "top",
    "under",
    "over",
    "with",
}


@dataclass
class QueryAnalysis:
    original_query: str
    normalized_query: str
    match_query: str
    intent: str
    label: str
    structured_intent: str | None
    structured_label: str | None
    matched_keywords: list[str]
    category_filter: str | None
    store_filter: str | None
    numeric_threshold: float | None
    search_terms: list[str]
    semantic_terms: list[str]
    review_signals: list[str]
    has_structured_constraints: bool
    has_semantic_product_terms: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze a query and route it to SQL, review-vector, or FAISS product search."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path. Defaults to {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--index-dir",
        type=Path,
        default=DEFAULT_INDEX_DIR,
        help=f"Embedding index directory. Defaults to {DEFAULT_INDEX_DIR}",
    )
    parser.add_argument("--query", required=True, help="User query to analyze.")
    parser.add_argument("--top-k", type=int, default=5, help="Maximum number of results to print.")
    return parser.parse_args()


def normalize_query(text: str) -> str:
    return " ".join(text.lower().replace("_", " ").split())


def normalize_for_match(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", text.lower().replace("_", " ")))


def numeric_threshold(query: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)", query)
    if not match:
        return None
    return float(match.group(1))


def contains_phrase(match_query: str, phrase: str) -> bool:
    return re.search(rf"\b{re.escape(phrase)}\b", match_query) is not None


def distinct_candidates(values: list[str]) -> list[str]:
    unique_values = {value.strip() for value in values if value and value.strip()}
    return sorted(unique_values, key=len, reverse=True)


def find_category_filter(match_query: str, conn) -> str | None:
    categories = fetch_distinct_values(conn, "products", "category")
    categories.extend(fetch_distinct_values(conn, "products", "main_category"))
    for category in distinct_candidates(categories):
        normalized = normalize_for_match(category)
        if normalized and normalized in match_query:
            return category
    return None


def find_store_filter(match_query: str, conn) -> str | None:
    for store in distinct_candidates(fetch_distinct_values(conn, "products", "store")):
        normalized = normalize_for_match(store)
        if normalized and normalized in match_query:
            return store
    return None


def collect_matched_keywords(match_query: str) -> list[str]:
    keywords: list[str] = []
    for phrase in TOP_RATED_PHRASES + UNDER_PHRASES + OVER_PHRASES + RATING_PHRASES + PRICE_HINTS:
        if contains_phrase(match_query, phrase) and phrase not in keywords:
            keywords.append(phrase)
    return keywords


def extract_search_terms(match_query: str) -> list[str]:
    terms: list[str] = []
    for token in match_query.split():
        if token in SEARCH_STOPWORDS or token.isdigit() or len(token) < 3:
            continue
        if token not in terms:
            terms.append(token)
    return terms


def infer_structured_intent(match_query: str, threshold: float | None) -> tuple[str | None, str | None]:
    if any(contains_phrase(match_query, phrase) for phrase in TOP_RATED_PHRASES):
        return "top_rated_products", "top-rated products"

    if any(contains_phrase(match_query, phrase) for phrase in UNDER_PHRASES) and threshold is not None:
        return "products_under_price", "products under price threshold"

    return None, None


def derive_semantic_terms(
    search_terms: list[str],
    category_filter: str | None,
    store_filter: str | None,
    review_signals: list[str],
) -> list[str]:
    if review_signals:
        return []

    blocked_terms: set[str] = set()
    if category_filter:
        blocked_terms.update(normalize_for_match(category_filter).split())
    if store_filter:
        blocked_terms.update(normalize_for_match(store_filter).split())

    return [term for term in search_terms if term not in blocked_terms]


def qualifies_for_mixed_search(
    structured_intent: str | None,
    category_filter: str | None,
    store_filter: str | None,
    semantic_terms: list[str],
) -> bool:
    if not semantic_terms:
        return False

    if category_filter or store_filter:
        return True

    if structured_intent in {"top_rated_products", "products_under_price"}:
        return True

    return False


def infer_intent(
    structured_intent: str | None,
    structured_label: str | None,
    review_signals: list[str],
    category_filter: str | None,
    store_filter: str | None,
    semantic_terms: list[str],
) -> tuple[str, str]:
    if review_signals:
        return "review_feedback_search", "review feedback search"

    if qualifies_for_mixed_search(structured_intent, category_filter, store_filter, semantic_terms):
        return "mixed_product_search", "mixed product search"

    if structured_intent and structured_label:
        return structured_intent, structured_label

    if category_filter or store_filter:
        if semantic_terms:
            return "mixed_product_search", "mixed product search"
        return "filtered_product_search", "filtered product search"

    if semantic_terms:
        return "semantic_product_search", "semantic product search"

    return "keyword_product_search", "keyword product search"


def analyze_query(query: str, conn) -> QueryAnalysis:
    normalized_query = normalize_query(query)
    match_query = normalize_for_match(query)
    threshold = numeric_threshold(query)
    category_filter = find_category_filter(match_query, conn)
    store_filter = find_store_filter(match_query, conn)
    review_signals = detect_document_signals(match_query)
    matched_keywords = collect_matched_keywords(match_query)
    if category_filter:
        matched_keywords.append(f"category:{category_filter}")
    if store_filter:
        matched_keywords.append(f"store:{store_filter}")

    search_terms = extract_search_terms(match_query)
    semantic_terms = derive_semantic_terms(search_terms, category_filter, store_filter, review_signals)
    structured_intent, structured_label = infer_structured_intent(match_query, threshold)
    has_structured_constraints = bool(structured_intent or category_filter or store_filter)
    has_semantic_product_terms = bool(semantic_terms)
    intent, label = infer_intent(
        structured_intent,
        structured_label,
        review_signals,
        category_filter,
        store_filter,
        semantic_terms,
    )

    return QueryAnalysis(
        original_query=query,
        normalized_query=normalized_query,
        match_query=match_query,
        intent=intent,
        label=label,
        structured_intent=structured_intent,
        structured_label=structured_label,
        matched_keywords=matched_keywords,
        category_filter=category_filter,
        store_filter=store_filter,
        numeric_threshold=threshold,
        search_terms=search_terms,
        semantic_terms=semantic_terms,
        review_signals=review_signals,
        has_structured_constraints=has_structured_constraints,
        has_semantic_product_terms=has_semantic_product_terms,
    )


def where_clause(clauses: list[str]) -> str:
    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)


def product_scope_clauses(analysis: QueryAnalysis) -> tuple[list[str], list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    if analysis.category_filter:
        clauses.append("(category = ? OR main_category = ?)")
        params.extend([analysis.category_filter, analysis.category_filter])

    if analysis.store_filter:
        clauses.append("store = ?")
        params.append(analysis.store_filter)

    return clauses, params


def structured_product_clauses(analysis: QueryAnalysis) -> tuple[list[str], list[object]]:
    clauses, params = product_scope_clauses(analysis)
    if analysis.structured_intent == "products_under_price":
        clauses.extend(["price_value IS NOT NULL", "price_value < ?"])
        params.append(analysis.numeric_threshold)
    return clauses, params


def mixed_candidate_order_by(analysis: QueryAnalysis) -> str:
    if analysis.structured_intent == "products_under_price":
        return "price_value ASC, average_rating DESC, rating_number DESC"
    return "average_rating DESC, rating_number DESC"


def fetch_mixed_candidate_rows(analysis: QueryAnalysis, conn, top_k: int) -> tuple[int, list[dict]]:
    clauses, params = structured_product_clauses(analysis)
    
    keyword_clauses = []
    for term in analysis.semantic_terms:
        keyword_clauses.append("LOWER(search_text) LIKE ?")
        params.append(f"%{term}%")
    if keyword_clauses:
        clauses.append("(" + " OR ".join(keyword_clauses) + ")")
    
    count_sql = f"""
    SELECT COUNT(*) AS candidate_count
    FROM products
    {where_clause(clauses)}
    """
    candidate_count = int(conn.execute(count_sql, params).fetchone()["candidate_count"])

    candidate_limit = max(MIXED_CANDIDATE_LIMIT, top_k * 20)
    sql = f"""
    SELECT parent_asin, title, store, average_rating, rating_number, price_value
    FROM products
    {where_clause(clauses)}
    ORDER BY {mixed_candidate_order_by(analysis)}
    LIMIT ?
    """
    rows = conn.execute(sql, (*params, candidate_limit)).fetchall()
    return candidate_count, [dict(row) for row in rows]


def run_top_rated_query(analysis: QueryAnalysis, conn, top_k: int) -> tuple[str, list[dict]]:
    clauses, params = product_scope_clauses(analysis)
    sql = f"""
    SELECT parent_asin, title, store, average_rating, rating_number, price_value
    FROM products
    {where_clause(clauses)}
    ORDER BY average_rating DESC, rating_number DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (*params, top_k)).fetchall()
    return "top-rated products", [dict(row) for row in rows]


def run_products_under_price_query(analysis: QueryAnalysis, conn, top_k: int) -> tuple[str, list[dict]]:
    clauses, params = structured_product_clauses(analysis)
    sql = f"""
    SELECT parent_asin, title, store, price_value, average_rating
    FROM products
    {where_clause(clauses)}
    ORDER BY price_value ASC, average_rating DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (*params, top_k)).fetchall()
    return "products under price threshold", [dict(row) for row in rows]


def run_keyword_product_search(analysis: QueryAnalysis, conn, top_k: int) -> tuple[str, list[dict]]:
    terms = analysis.search_terms[:5]
    if not terms:
        return run_top_rated_query(analysis, conn, top_k)

    score_parts: list[str] = []
    score_params: list[object] = []
    keyword_where_parts: list[str] = []
    keyword_where_params: list[object] = []

    for term in terms:
        pattern = f"%{term}%"
        score_parts.append(
            "(CASE WHEN LOWER(COALESCE(title, '')) LIKE ? THEN 2 ELSE 0 END + "
            "CASE WHEN LOWER(search_text) LIKE ? THEN 1 ELSE 0 END)"
        )
        score_params.extend([pattern, pattern])
        keyword_where_parts.append("LOWER(COALESCE(title, '')) LIKE ?")
        keyword_where_parts.append("LOWER(search_text) LIKE ?")
        keyword_where_params.extend([pattern, pattern])

    scope_clauses, scope_params = structured_product_clauses(analysis)
    where_parts = ["(" + " OR ".join(keyword_where_parts) + ")"]
    where_parts.extend(scope_clauses)

    sql = f"""
    SELECT
      parent_asin,
      title,
      store,
      average_rating,
      rating_number,
      price_value,
      ({' + '.join(score_parts)}) AS keyword_score
    FROM products
    {where_clause(where_parts)}
    ORDER BY keyword_score DESC, average_rating DESC, rating_number DESC
    LIMIT ?
    """
    params = [*score_params, *keyword_where_params, *scope_params, top_k]
    rows = conn.execute(sql, params).fetchall()
    return "keyword product search", [dict(row) for row in rows]


def run_sql_query(analysis: QueryAnalysis, conn, top_k: int) -> tuple[str, list[dict]]:
    if analysis.intent == "top_rated_products":
        return run_top_rated_query(analysis, conn, top_k)
    if analysis.intent == "products_under_price":
        return run_products_under_price_query(analysis, conn, top_k)
    return run_keyword_product_search(analysis, conn, top_k)


def get_bytes_per_row(conn) -> int:
    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    total_db_bytes = page_count * page_size

    product_rows = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    review_rows = conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]

    weighted_total = (product_rows * 2.5) + review_rows
    product_fraction = (product_rows * 2.5) / weighted_total if weighted_total else 1

    estimated_products_bytes = int(total_db_bytes * product_fraction)
    return estimated_products_bytes // product_rows


def run_mixed_product_search(
    analysis: QueryAnalysis,
    conn,
    index_dir: Path,
    top_k: int,
    bytes_per_row: int,
) -> tuple[str, str, list[dict], str | None]:
    candidate_count, candidate_rows = fetch_mixed_candidate_rows(analysis, conn, top_k)
    if candidate_count == 0:
        return "sql", "SQL candidate filter + FAISS rerank", [], None

    sql_cost, faiss_cost = estimate_io(candidate_count, index_dir, bytes_per_row)
    if sql_cost < faiss_cost * 0.8:
        label, results = run_keyword_product_search(analysis, conn, top_k)
        return "sql", "SQL-only mixed fallback", results, None

    try:
        label, results = run_faiss_product_search(
            " ".join(analysis.semantic_terms),
            conn,
            index_dir=index_dir,
            top_k=top_k,
            candidate_asins=[row["parent_asin"] for row in candidate_rows],
        )
        if results:
            return "mixed", "SQL candidate filter + FAISS rerank", results, None

        label, results = run_keyword_product_search(analysis, conn, top_k)
        return "sql", f"{label} (mixed fallback)", results, "FAISS returned no candidate rerank hits"
    except IndexLookupError as exc:
        label, results = run_keyword_product_search(analysis, conn, top_k)
        return "sql", f"{label} (mixed fallback)", results, str(exc)


def run_query(
    analysis: QueryAnalysis,
    conn,
    index_dir: Path,
    top_k: int,
    bytes_per_row: int,
) -> tuple[RouteDecision, str, str, list[dict]]:
    route = route_query(analysis.intent, analysis.review_signals)

    if route.engine == "review-vector":
        try:
            label, results = run_vector_review_search(
                analysis.original_query,
                index_dir=index_dir,
                top_k=top_k,
            )
            return route, "review-vector", label, results
        except IndexLookupError as exc:
            label, results = run_keyword_product_search(analysis, conn, top_k)
            fallback_route = RouteDecision(
                engine=route.engine,
                reason=f"{route.reason}; fallback to SQL because {exc}",
                matched_signals=route.matched_signals,
            )
            return fallback_route, "sql", f"{label} (fallback)", results

    if route.engine == "mixed":
        executed_engine, label, results, fallback_reason = run_mixed_product_search(
            analysis,
            conn,
            index_dir,
            top_k,
            bytes_per_row,
        )
        if fallback_reason:
            fallback_route = RouteDecision(
                engine=route.engine,
                reason=f"{route.reason}; fallback to SQL because {fallback_reason}",
                matched_signals=route.matched_signals,
            )
            return fallback_route, executed_engine, label, results
        return route, executed_engine, label, results

    if route.engine == "product-vector":
        try:
            label, results = run_faiss_product_search(
                " ".join(analysis.semantic_terms) if analysis.semantic_terms else analysis.original_query,
                conn,
                index_dir=index_dir,
                top_k=top_k,
                category_filter=analysis.category_filter,
                store_filter=analysis.store_filter,
            )
            if results:
                return route, "product-vector", label, results

            label, results = run_keyword_product_search(analysis, conn, top_k)
            fallback_route = RouteDecision(
                engine=route.engine,
                reason=f"{route.reason}; fallback to SQL because FAISS returned no scoped product hits",
                matched_signals=route.matched_signals,
            )
            return fallback_route, "sql", f"{label} (fallback)", results
        except IndexLookupError as exc:
            label, results = run_keyword_product_search(analysis, conn, top_k)
            fallback_route = RouteDecision(
                engine=route.engine,
                reason=f"{route.reason}; fallback to SQL because {exc}",
                matched_signals=route.matched_signals,
            )
            return fallback_route, "sql", f"{label} (fallback)", results

    label, results = run_sql_query(analysis, conn, top_k)
    return route, "sql", label, results


def print_results(
    analysis: QueryAnalysis,
    route: RouteDecision,
    executed_engine: str,
    label: str,
    results: list[dict],
    elapsed_seconds: float,
) -> None:
    print("Analyzer mode: simple-hybrid-cost")
    print(f"Normalized query: {analysis.normalized_query}")
    print(f"Detected intent: {analysis.label}")
    print(f"Matched keywords: {', '.join(analysis.matched_keywords) if analysis.matched_keywords else 'none'}")
    print(f"Category filter: {analysis.category_filter or 'none'}")
    print(f"Store filter: {analysis.store_filter or 'none'}")
    print(f"Numeric threshold: {analysis.numeric_threshold if analysis.numeric_threshold is not None else 'none'}")
    print(f"Search terms: {', '.join(analysis.search_terms) if analysis.search_terms else 'none'}")
    print(f"Semantic terms: {', '.join(analysis.semantic_terms) if analysis.semantic_terms else 'none'}")
    print(f"Review signals: {', '.join(analysis.review_signals) if analysis.review_signals else 'none'}")
    print(f"Structured constraints: {'yes' if analysis.has_structured_constraints else 'no'}")
    print(f"Routed engine: {route.engine}")
    print(f"Routing signals: {', '.join(route.matched_signals) if route.matched_signals else 'none'}")
    print(f"Routing reason: {route.reason}")
    print(f"Executed engine: {executed_engine}")
    print(f"Execution mode: {label}")
    print(f"Execution time: {elapsed_seconds:.4f} seconds")
    print("Top results:")

    if not results:
        print("  No results.")
        return

    for index, result in enumerate(results, start=1):
        print(f"  {index}.")
        for key, value in result.items():
            display_value = make_snippet(value) if key in {"review_text"} and isinstance(value, str) else value
            print(f"     {key}: {display_value}")


def main() -> None:
    args = parse_args()

    start = time.perf_counter()
    with connect_db(args.db) as conn:
        bytes_per_row = get_bytes_per_row(conn)
        analysis = analyze_query(args.query, conn)
        route, executed_engine, label, results = run_query(analysis, conn, args.index_dir, args.top_k, bytes_per_row)

    elapsed = time.perf_counter() - start
    print_results(analysis, route, executed_engine, label, results, elapsed)


if __name__ == "__main__":
    main()

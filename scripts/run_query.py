from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from pathlib import Path

from hybrid_utils import DEFAULT_DB_PATH, DEFAULT_INDEX_DIR, connect_db, fetch_distinct_values, make_snippet
from vector_engine import VectorEngine


PRODUCT_INDEX_PATH = DEFAULT_INDEX_DIR / "product_index.faiss"
PRODUCT_MAPPING_PATH = DEFAULT_INDEX_DIR / "product_index.npy"

TOP_RATED_PHRASES = ["top rated", "best rated", "highest rated"]
UNDER_PHRASES = ["under", "below", "less than"]
OVER_PHRASES = ["over", "above", "greater than"]
RATING_PHRASES = ["rating", "rated", "star", "stars"]
PRICE_HINTS = ["price", "prices", "dollar", "dollars", "cost", "costs"]


@dataclass
class QueryAnalysis:
    original_query: str
    normalized_query: str
    match_query: str
    intent: str
    label: str
    matched_keywords: list[str]
    category_filter: str | None
    store_filter: str | None
    numeric_threshold: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze a query and run a small set of supported SQLite searches."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path. Defaults to {DEFAULT_DB_PATH}",
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


def find_store_filter(match_query: str, conn) -> str | None:
    for store in distinct_candidates(fetch_distinct_values(conn, "products", "store")):
        if normalize_for_match(store) in match_query:
            return store
    return None


def find_category_filter(match_query: str, conn) -> str | None:
    categories = fetch_distinct_values(conn, "products", "category")
    categories.extend(fetch_distinct_values(conn, "products", "main_category"))
    for category in distinct_candidates(categories):
        if normalize_for_match(category) in match_query:
            return category
    return None


def collect_matched_keywords(match_query: str) -> list[str]:
    keywords: list[str] = []
    for phrase in TOP_RATED_PHRASES + UNDER_PHRASES + OVER_PHRASES + RATING_PHRASES + PRICE_HINTS:
        if contains_phrase(match_query, phrase) and phrase not in keywords:
            keywords.append(phrase)
    return keywords


def infer_intent(match_query: str, threshold: float | None, _store_filter: str | None) -> tuple[str, str]:
    if any(contains_phrase(match_query, phrase) for phrase in TOP_RATED_PHRASES):
        return "top_rated_products", "top-rated products"

    if any(contains_phrase(match_query, phrase) for phrase in UNDER_PHRASES) and threshold is not None:
        return "products_under_price", "products under price threshold"

    return "semantic_product_search", "semantic product search"


def analyze_query(query: str, conn) -> QueryAnalysis:
    normalized_query = normalize_query(query)
    match_query = normalize_for_match(query)
    threshold = numeric_threshold(query)
    store_filter = find_store_filter(match_query, conn)
    category_filter = find_category_filter(match_query, conn)
    matched_keywords = collect_matched_keywords(match_query)
    if category_filter:
        matched_keywords.append(f"category:{category_filter}")
    if store_filter:
        matched_keywords.append(f"store:{store_filter}")

    intent, label = infer_intent(match_query, threshold, store_filter)

    return QueryAnalysis(
        original_query=query,
        normalized_query=normalized_query,
        match_query=match_query,
        intent=intent,
        label=label,
        matched_keywords=matched_keywords,
        category_filter=category_filter,
        store_filter=store_filter,
        numeric_threshold=threshold,
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
    clauses, params = product_scope_clauses(analysis)
    clauses.extend(["price_value IS NOT NULL", "price_value < ?"])
    params.append(analysis.numeric_threshold)
    sql = f"""
    SELECT parent_asin, title, store, price_value, average_rating
    FROM products
    {where_clause(clauses)}
    ORDER BY price_value ASC, average_rating DESC
    LIMIT ?
    """
    rows = conn.execute(sql, (*params, top_k)).fetchall()
    return "products under price threshold", [dict(row) for row in rows]


def run_semantic_product_search(analysis: QueryAnalysis, conn, top_k: int) -> tuple[str, list[dict]]:
    engine = VectorEngine.from_saved(PRODUCT_INDEX_PATH, PRODUCT_MAPPING_PATH)
    vector_results = engine.search(analysis.original_query, k=top_k)
    asin_to_score = {asin: score for asin, score in vector_results}
    asins = list(asin_to_score.keys())
    placeholders = ",".join("?" * len(asins))
    rows = conn.execute(f"""
        SELECT parent_asin, title, store, average_rating, price_value
        FROM products
        WHERE parent_asin IN ({placeholders})
    """, asins).fetchall()
    results = []
    for row in rows:
        d = dict(row)
        d["similarity_score"] = asin_to_score[d["parent_asin"]]
        results.append(d)
    results.sort(key=lambda x: x["similarity_score"], reverse=True)
    return "semantic product search", results


def run_query(analysis: QueryAnalysis, conn, top_k: int) -> tuple[str, list[dict]]:
    if analysis.intent == "top_rated_products":
        return run_top_rated_query(analysis, conn, top_k)
    if analysis.intent == "products_under_price":
        return run_products_under_price_query(analysis, conn, top_k)
    return run_semantic_product_search(analysis, conn, top_k)


def print_results(analysis: QueryAnalysis, label: str, results: list[dict], elapsed_seconds: float) -> None:
    print("Analyzer mode: hybrid")
    print(f"Normalized query: {analysis.normalized_query}")
    print(f"Detected intent: {analysis.label}")
    print(f"Matched keywords: {', '.join(analysis.matched_keywords) if analysis.matched_keywords else 'none'}")
    print(f"Category filter: {analysis.category_filter or 'none'}")
    print(f"Store filter: {analysis.store_filter or 'none'}")
    print(f"Numeric threshold: {analysis.numeric_threshold if analysis.numeric_threshold is not None else 'none'}")
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
        analysis = analyze_query(args.query, conn)
        label, results = run_query(analysis, conn, args.top_k)

    elapsed = time.perf_counter() - start
    print_results(analysis, label, results, elapsed)


if __name__ == "__main__":
    main()

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from hybrid_utils import (
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_INDEX_DIR,
    load_embedding_model,
    make_snippet,
    read_json,
)
from vector_engine import VectorEngine, VectorEngineError


STRUCTURED_INTENTS = {"top_rated_products", "products_under_price"}
DOCUMENT_PHRASES = [
    "what do reviews say",
    "what reviews say",
    "customer review",
    "customer reviews",
    "based on reviews",
    "in the reviews",
]
DOCUMENT_TERMS = {
    "comment",
    "comments",
    "feedback",
    "mention",
    "mentioned",
    "mentions",
    "opinion",
    "opinions",
    "review",
    "reviews",
}


class IndexLookupError(RuntimeError):
    pass


@dataclass
class RouteDecision:
    engine: str
    reason: str
    matched_signals: list[str]


def detect_document_signals(match_query: str) -> list[str]:
    signals: list[str] = []
    for phrase in DOCUMENT_PHRASES:
        if phrase in match_query and phrase not in signals:
            signals.append(phrase)

    for token in match_query.split():
        if token in DOCUMENT_TERMS and token not in signals:
            signals.append(token)

    return signals


def route_query(intent: str, review_signals: list[str] | None = None) -> RouteDecision:
    if review_signals:
        return RouteDecision(
            engine="review-vector",
            reason="review or document language detected",
            matched_signals=review_signals,
        )

    if intent == "mixed_product_search":
        return RouteDecision(
            engine="mixed",
            reason="structured constraints and semantic product terms detected",
            matched_signals=[intent],
        )

    if intent in STRUCTURED_INTENTS:
        return RouteDecision(
            engine="sql",
            reason="structured price/rating intent detected",
            matched_signals=[intent],
        )

    if intent in {"filtered_product_search", "keyword_product_search"}:
        return RouteDecision(
            engine="sql",
            reason="use relational fallback for filtered or lexical product lookup",
            matched_signals=[intent],
        )

    return RouteDecision(
        engine="product-vector",
        reason="no explicit review/document signal; use semantic product search",
        matched_signals=[],
    )


def resolve_review_index(index_dir: Path) -> tuple[Path, Path]:
    metadata_path = index_dir / "review_index.json"
    vectors_path = index_dir / "review_index.npy"
    if metadata_path.is_file() and vectors_path.is_file():
        return metadata_path, vectors_path

    candidates: list[tuple[float, Path, Path]] = []
    if index_dir.exists():
        for candidate_metadata in index_dir.rglob("review_index.json"):
            candidate_vectors = candidate_metadata.with_suffix(".npy")
            if candidate_vectors.is_file():
                candidates.append(
                    (candidate_metadata.stat().st_mtime, candidate_metadata, candidate_vectors)
                )

    if not candidates:
        raise IndexLookupError(
            f"No review index found under {index_dir}. "
            "Run scripts/build_review_index.py before using vector search."
        )

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, newest_metadata, newest_vectors = candidates[0]
    return newest_metadata, newest_vectors


def resolve_product_index(index_dir: Path) -> tuple[Path, Path]:
    index_path = index_dir / "product_index.faiss"
    mapping_path = index_dir / "product_index.npy"
    if index_path.is_file() and mapping_path.is_file():
        return index_path, mapping_path

    candidates: list[tuple[float, Path, Path]] = []
    if index_dir.exists():
        for candidate_index in index_dir.rglob("product_index.faiss"):
            candidate_mapping = candidate_index.with_suffix(".npy")
            if candidate_mapping.is_file():
                candidates.append(
                    (candidate_index.stat().st_mtime, candidate_index, candidate_mapping)
                )

    if not candidates:
        raise IndexLookupError(
            f"No product index found under {index_dir}. "
            "Run scripts/build_product_index.py before using semantic product search."
        )

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, newest_index, newest_mapping = candidates[0]
    return newest_index, newest_mapping


def run_vector_review_search(
    query: str,
    index_dir: Path = DEFAULT_INDEX_DIR,
    top_k: int = 5,
) -> tuple[str, list[dict]]:
    metadata_path, vectors_path = resolve_review_index(index_dir)
    metadata = read_json(metadata_path)
    records = metadata.get("records", [])
    vectors = np.load(vectors_path)

    if len(records) != len(vectors):
        raise IndexLookupError(
            f"Index metadata row count does not match vectors in {metadata_path.parent}."
        )

    model_name = metadata.get("model_name") or DEFAULT_EMBEDDING_MODEL
    model = load_embedding_model(model_name)
    query_vector = model.encode(
        [query],
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )[0]

    scores = vectors @ query_vector
    ranked_indexes = np.argsort(scores)[::-1][:top_k]

    results: list[dict] = []
    for raw_index in ranked_indexes:
        index = int(raw_index)
        record = records[index]
        result = {
            "review_id": record.get("review_id"),
            "parent_asin": record.get("parent_asin"),
            "product_title": record.get("product_title"),
            "rating": record.get("rating"),
            "review_text": make_snippet(record.get("review_text") or record.get("snippet") or ""),
            "similarity_score": round(float(scores[index]), 4),
        }
        if record.get("review_title"):
            result["review_title"] = record["review_title"]
        results.append(result)

    return "review vector search", results


def run_faiss_product_search(
    query: str,
    conn,
    index_dir: Path = DEFAULT_INDEX_DIR,
    top_k: int = 5,
    category_filter: str | None = None,
    store_filter: str | None = None,
    candidate_asins: list[str] | None = None,
) -> tuple[str, list[dict]]:
    index_path, mapping_path = resolve_product_index(index_dir)
    try:
        engine = VectorEngine.from_saved(index_path, mapping_path)
    except VectorEngineError as exc:
        raise IndexLookupError(str(exc)) from exc

    if candidate_asins is not None:
        vector_results = engine.search_subset(query, candidate_asins, top_k=top_k)
    else:
        candidate_k = max(top_k * 10, 50)
        vector_results = engine.search(query, k=candidate_k)

    if not vector_results:
        return "FAISS semantic product search", []

    asin_to_score = {asin: score for asin, score in vector_results}
    asins = list(asin_to_score.keys())
    placeholders = ",".join("?" for _ in asins)
    where_clauses = [f"parent_asin IN ({placeholders})"]
    params: list[object] = [*asins]

    if category_filter:
        where_clauses.append("(category = ? OR main_category = ?)")
        params.extend([category_filter, category_filter])

    if store_filter:
        where_clauses.append("store = ?")
        params.append(store_filter)

    sql = f"""
    SELECT parent_asin, title, store, average_rating, rating_number, price_value
    FROM products
    WHERE {" AND ".join(where_clauses)}
    """
    rows = conn.execute(sql, params).fetchall()
    row_by_asin = {row["parent_asin"]: dict(row) for row in rows}

    results: list[dict] = []
    for asin, score in vector_results:
        row = row_by_asin.get(asin)
        if not row:
            continue

        row["similarity_score"] = round(float(score), 4)
        results.append(row)
        if len(results) >= top_k:
            break

    return "FAISS semantic product search", results

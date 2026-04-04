from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from hybrid_utils import (
    DEFAULT_DB_PATH,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_INDEX_DIR,
    connect_db,
    load_embedding_model,
    make_snippet,
    write_json,
)

INDEX_DB_PATH = DEFAULT_DB_PATH
INDEX_OUT_DIR = DEFAULT_INDEX_DIR
INDEX_REVIEW_LIMIT = 5000


def fetch_reviews(db_path: Path, limit: int) -> list[dict]:
    query = """
    SELECT
        r.review_id,
        r.parent_asin,
        p.title AS product_title,
        r.rating,
        r.review_text
    FROM reviews AS r
    LEFT JOIN products AS p
      ON p.parent_asin = r.parent_asin
    WHERE r.review_text IS NOT NULL
      AND TRIM(r.review_text) <> ''
    ORDER BY r.timestamp_ms DESC
    LIMIT ?
    """

    with connect_db(db_path) as conn:
        rows = conn.execute(query, (limit,)).fetchall()

    return [dict(row) for row in rows]


def main() -> None:
    reviews = fetch_reviews(INDEX_DB_PATH, INDEX_REVIEW_LIMIT)
    if not reviews:
        raise SystemExit("No non-empty review text found. Load reviews before building the index.")

    model = load_embedding_model(DEFAULT_EMBEDDING_MODEL)
    texts = [review["review_text"] for review in reviews]
    vectors = model.encode(
        texts,
        batch_size=64,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    INDEX_OUT_DIR.mkdir(parents=True, exist_ok=True)
    vectors_path = INDEX_OUT_DIR / "review_index.npy"
    metadata_path = INDEX_OUT_DIR / "review_index.json"

    np.save(vectors_path, vectors)
    metadata = {
        "model_name": DEFAULT_EMBEDDING_MODEL,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(reviews),
        "records": [
            {
                "review_id": review["review_id"],
                "parent_asin": review["parent_asin"],
                "product_title": review["product_title"],
                "rating": review["rating"],
                "review_text": review["review_text"],
                "snippet": make_snippet(review["review_text"]),
            }
            for review in reviews
        ],
    }
    write_json(metadata_path, metadata)

    print(f"Built review index with {len(reviews)} rows")
    print(f"Vectors saved to {vectors_path.resolve()}")
    print(f"Metadata saved to {metadata_path.resolve()}")


if __name__ == "__main__":
    main()

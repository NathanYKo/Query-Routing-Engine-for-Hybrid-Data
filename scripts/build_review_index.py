from __future__ import annotations

import argparse
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a small review embedding index.")
    parser.add_argument(
        "--db",
        type=Path,
        default=INDEX_DB_PATH,
        help=f"SQLite database path. Defaults to {INDEX_DB_PATH}",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=INDEX_OUT_DIR,
        help=f"Output directory for review_index.* files. Defaults to {INDEX_OUT_DIR}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=INDEX_REVIEW_LIMIT,
        help=f"Maximum number of reviews to index. Defaults to {INDEX_REVIEW_LIMIT}",
    )
    parser.add_argument(
        "--model-name",
        default=DEFAULT_EMBEDDING_MODEL,
        help=f"SentenceTransformer model name. Defaults to {DEFAULT_EMBEDDING_MODEL}",
    )
    return parser.parse_args()


def fetch_reviews(db_path: Path, limit: int) -> list[dict]:
    query = """
    SELECT
        r.review_id,
        r.parent_asin,
        p.title AS product_title,
        r.rating,
        r.review_title,
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
    args = parse_args()
    reviews = fetch_reviews(args.db, args.limit)
    if not reviews:
        raise SystemExit("No non-empty review text found. Load reviews before building the index.")

    model = load_embedding_model(args.model_name)
    texts = [review["review_text"] for review in reviews]
    vectors = model.encode(
        texts,
        batch_size=64,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    vectors_path = args.out_dir / "review_index.npy"
    metadata_path = args.out_dir / "review_index.json"

    np.save(vectors_path, vectors)
    metadata = {
        "model_name": args.model_name,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(reviews),
        "records": [
            {
                "review_id": review["review_id"],
                "parent_asin": review["parent_asin"],
                "product_title": review["product_title"],
                "review_title": review["review_title"],
                "rating": review["rating"],
                "review_text": review["review_text"],
                "snippet": make_snippet(review["review_text"]),
            }
            for review in reviews
        ],
    }
    write_json(metadata_path, metadata)

    print(f"Built review index with {len(reviews)} rows")
    print(f"Source DB: {args.db.resolve()}")
    print(f"Vectors saved to {vectors_path.resolve()}")
    print(f"Metadata saved to {metadata_path.resolve()}")


if __name__ == "__main__":
    main()

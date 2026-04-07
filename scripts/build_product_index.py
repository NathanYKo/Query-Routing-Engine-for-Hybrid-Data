from __future__ import annotations

import argparse
from pathlib import Path

from hybrid_utils import DEFAULT_DB_PATH, DEFAULT_EMBEDDING_MODEL, DEFAULT_INDEX_DIR, connect_db
from vector_engine import VectorEngine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a FAISS product embedding index.")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path. Defaults to {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_INDEX_DIR,
        help=f"Output directory for product_index.* files. Defaults to {DEFAULT_INDEX_DIR}",
    )
    parser.add_argument(
        "--model-name",
        default=DEFAULT_EMBEDDING_MODEL,
        help=f"SentenceTransformer model name. Defaults to {DEFAULT_EMBEDDING_MODEL}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of products to index.",
    )
    return parser.parse_args()


def fetch_products(db_path: Path, limit: int | None) -> list[tuple[str, str]]:
    sql = """
    SELECT parent_asin, search_text
    FROM products
    WHERE search_text IS NOT NULL
      AND TRIM(search_text) <> ''
    ORDER BY rating_number DESC, average_rating DESC
    """
    params: tuple[object, ...] = ()
    if limit is not None:
        sql += "\nLIMIT ?"
        params = (limit,)

    with connect_db(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()

    return [(row["parent_asin"], row["search_text"]) for row in rows]


def main() -> None:
    args = parse_args()
    rows = fetch_products(args.db, args.limit)
    if not rows:
        raise SystemExit("No product search text found. Load metadata before building the product index.")

    engine = VectorEngine(model_name=args.model_name)
    engine.add_documents(rows)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    index_path = args.out_dir / "product_index.faiss"
    mapping_path = args.out_dir / "product_index.npy"
    engine.save(index_path, mapping_path)

    print(f"Built product index with {len(rows)} products")
    print(f"Source DB: {args.db.resolve()}")
    print(f"Index saved to {index_path.resolve()}")
    print(f"Mapping saved to {mapping_path.resolve()}")


if __name__ == "__main__":
    main()

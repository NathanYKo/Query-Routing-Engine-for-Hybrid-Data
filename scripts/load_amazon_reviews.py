from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, TextIO


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "sqlite" / "hybrid_router.db"
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "schema" / "sqlite_schema.sql"


PRODUCT_UPSERT = """
INSERT INTO products (
    parent_asin,
    category,
    main_category,
    title,
    average_rating,
    rating_number,
    price_text,
    price_value,
    store,
    features_json,
    description_json,
    images_json,
    videos_json,
    categories_json,
    details_json,
    bought_together,
    search_text,
    raw_json
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(parent_asin) DO UPDATE SET
    category = excluded.category,
    main_category = excluded.main_category,
    title = excluded.title,
    average_rating = excluded.average_rating,
    rating_number = excluded.rating_number,
    price_text = excluded.price_text,
    price_value = excluded.price_value,
    store = excluded.store,
    features_json = excluded.features_json,
    description_json = excluded.description_json,
    images_json = excluded.images_json,
    videos_json = excluded.videos_json,
    categories_json = excluded.categories_json,
    details_json = excluded.details_json,
    bought_together = excluded.bought_together,
    search_text = excluded.search_text,
    raw_json = excluded.raw_json,
    ingested_at = CURRENT_TIMESTAMP
"""


REVIEW_UPSERT = """
INSERT INTO reviews (
    review_id,
    category,
    asin,
    parent_asin,
    user_id,
    rating,
    review_title,
    review_text,
    review_images_json,
    timestamp_ms,
    reviewed_at,
    helpful_vote,
    verified_purchase,
    raw_json
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(review_id) DO UPDATE SET
    category = excluded.category,
    asin = excluded.asin,
    parent_asin = excluded.parent_asin,
    user_id = excluded.user_id,
    rating = excluded.rating,
    review_title = excluded.review_title,
    review_text = excluded.review_text,
    review_images_json = excluded.review_images_json,
    timestamp_ms = excluded.timestamp_ms,
    reviewed_at = excluded.reviewed_at,
    helpful_vote = excluded.helpful_vote,
    verified_purchase = excluded.verified_purchase,
    raw_json = excluded.raw_json,
    ingested_at = CURRENT_TIMESTAMP
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load one Amazon Reviews'23 category into SQLite."
    )
    parser.add_argument("--category", required=True, help="Category label, for example All_Beauty")
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path. Defaults to {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help=f"Schema file path. Defaults to {DEFAULT_SCHEMA_PATH}",
    )
    parser.add_argument("--reviews", type=Path, help="Path to a review JSONL or JSONL.GZ file")
    parser.add_argument("--metadata", type=Path, help="Path to a metadata JSONL or JSONL.GZ file")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of records to read from each input file",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of rows to buffer before writing to SQLite",
    )
    args = parser.parse_args()

    if not args.reviews and not args.metadata:
        parser.error("Provide at least one of --reviews or --metadata.")

    return args


def initialize_database(db_path: Path, schema_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(schema_sql)
    conn.commit()
    return conn


def open_text_file(path: Path) -> TextIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def iter_json_lines(path: Path, limit: int | None = None) -> Iterator[dict]:
    with open_text_file(path) as handle:
        count = 0
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            yield json.loads(line)
            count += 1
            if limit is not None and count >= limit:
                break


def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def as_list(value: object) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def flatten_text(values: Iterable[object]) -> list[str]:
    parts: list[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            text = value.strip()
            if text:
                parts.append(text)
        elif isinstance(value, dict):
            for nested in value.values():
                parts.extend(flatten_text([nested]))
        elif isinstance(value, list):
            parts.extend(flatten_text(value))
        else:
            parts.append(str(value))
    return parts


def parse_price(raw_price: object) -> float | None:
    if raw_price is None:
        return None
    if isinstance(raw_price, (int, float)):
        return float(raw_price)

    text = str(raw_price).strip()
    if not text:
        return None

    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None

    return float(match.group(0))


def reviewed_at(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def make_review_id(category: str, record: dict) -> str:
    key_parts = [
        category,
        str(record.get("user_id", "")),
        str(record.get("parent_asin", "")),
        str(record.get("asin", "")),
        str(record.get("timestamp", record.get("sort_timestamp", ""))),
        str(record.get("rating", "")),
        str(record.get("title", "")),
        str(record.get("text", "")),
    ]
    digest = hashlib.sha256("|".join(key_parts).encode("utf-8")).hexdigest()
    return digest


def build_product_row(category: str, record: dict) -> tuple:
    features = as_list(record.get("features"))
    description = as_list(record.get("description"))
    images = as_list(record.get("images"))
    videos = as_list(record.get("videos"))
    categories = as_list(record.get("categories"))
    details = record.get("details") or {}

    search_parts = flatten_text(
        [
            record.get("title"),
            record.get("store"),
            features,
            description,
            categories,
            details,
        ]
    )
    search_text = "\n".join(search_parts)

    return (
        record["parent_asin"],
        category,
        record.get("main_category"),
        record.get("title"),
        record.get("average_rating"),
        record.get("rating_number"),
        record.get("price"),
        parse_price(record.get("price")),
        record.get("store"),
        json_text(features),
        json_text(description),
        json_text(images),
        json_text(videos),
        json_text(categories),
        json_text(details),
        record.get("bought_together"),
        search_text,
        json_text(record),
    )


def build_review_row(category: str, record: dict) -> tuple | None:
    timestamp_ms = record.get("timestamp", record.get("sort_timestamp"))
    parent_asin = record.get("parent_asin") or record.get("asin")
    user_id = record.get("user_id")
    rating = record.get("rating")

    if timestamp_ms is None or parent_asin is None or user_id is None or rating is None:
        return None

    return (
        make_review_id(category, record),
        category,
        record.get("asin"),
        parent_asin,
        user_id,
        rating,
        record.get("title"),
        record.get("text"),
        json_text(as_list(record.get("images"))),
        int(timestamp_ms),
        reviewed_at(int(timestamp_ms)),
        int(record.get("helpful_vote", record.get("helpful_votes", 0)) or 0),
        int(bool(record.get("verified_purchase", False))),
        json_text(record),
    )


def batched_upsert(
    conn: sqlite3.Connection,
    sql: str,
    rows: Iterator[tuple | None],
    batch_size: int,
) -> tuple[int, int]:
    inserted = 0
    skipped = 0
    batch: list[tuple] = []

    for row in rows:
        if row is None:
            skipped += 1
            continue

        batch.append(row)
        if len(batch) >= batch_size:
            conn.executemany(sql, batch)
            inserted += len(batch)
            batch.clear()

    if batch:
        conn.executemany(sql, batch)
        inserted += len(batch)

    conn.commit()
    return inserted, skipped


def main() -> None:
    args = parse_args()
    conn = initialize_database(args.db, args.schema)

    try:
        if args.metadata:
            metadata_rows = (
                build_product_row(args.category, record)
                for record in iter_json_lines(args.metadata, args.limit)
                if record.get("parent_asin")
            )
            product_count, product_skipped = batched_upsert(
                conn,
                PRODUCT_UPSERT,
                metadata_rows,
                args.batch_size,
            )
            print(
                f"Loaded metadata rows: {product_count} written, {product_skipped} skipped"
            )

        if args.reviews:
            review_rows = (
                build_review_row(args.category, record)
                for record in iter_json_lines(args.reviews, args.limit)
            )
            review_count, review_skipped = batched_upsert(
                conn,
                REVIEW_UPSERT,
                review_rows,
                args.batch_size,
            )
            print(f"Loaded review rows: {review_count} written, {review_skipped} skipped")

        print(f"SQLite database ready at {args.db.resolve()}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

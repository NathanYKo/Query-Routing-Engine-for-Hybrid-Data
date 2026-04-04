from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "sqlite" / "hybrid_router.db"
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "schema" / "sqlite_schema.sql"


def initialize_database(db_path: Path, schema_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = schema_path.read_text(encoding="utf-8")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(schema_sql)
        conn.commit()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create the SQLite database for the project.")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    initialize_database(args.db, args.schema)
    print(f"Initialized SQLite database at {args.db.resolve()}")


if __name__ == "__main__":
    main()

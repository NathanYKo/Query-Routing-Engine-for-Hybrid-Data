from __future__ import annotations

import json
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "sqlite" / "hybrid_router.db"
DEFAULT_INDEX_DIR = PROJECT_ROOT / "data" / "index"
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_sentence_transformers() -> object:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency 'sentence-transformers'. "
            "Install it with: python -m pip install -r requirements.txt"
        ) from exc

    return SentenceTransformer


def load_embedding_model(model_name: str):
    sentence_transformer_cls = ensure_sentence_transformers()
    return sentence_transformer_cls(model_name)


def fetch_distinct_values(conn: sqlite3.Connection, table: str, column: str) -> list[str]:
    rows = conn.execute(
        f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL AND TRIM({column}) <> ''"
    ).fetchall()
    return [row[0] for row in rows]


def normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def make_snippet(text: str, length: int = 140) -> str:
    normalized = normalize_whitespace(text)
    if len(normalized) <= length:
        return normalized
    return normalized[: length - 3].rstrip() + "..."


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))

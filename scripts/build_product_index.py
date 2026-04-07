import sqlite3
from vector_engine import VectorEngine
from hybrid_utils import DEFAULT_DB_PATH, DEFAULT_INDEX_DIR, connect_db

with connect_db(DEFAULT_DB_PATH) as conn:
    rows = conn.execute("""
        SELECT parent_asin, search_text
        FROM products
    """).fetchall()

engine = VectorEngine()
engine.add_documents([(row["parent_asin"], row["search_text"]) for row in rows])

DEFAULT_INDEX_DIR.mkdir(parents=True, exist_ok=True)
engine.save(DEFAULT_INDEX_DIR / "product_index.faiss", DEFAULT_INDEX_DIR / "product_index.npy")

print(f"Built product index with {len(rows)} products")
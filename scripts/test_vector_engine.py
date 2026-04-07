from vector_engine import VectorEngine
from hybrid_utils import DEFAULT_DB_PATH, DEFAULT_INDEX_DIR, connect_db

PRODUCT_INDEX_PATH = DEFAULT_INDEX_DIR / "product_index.faiss"
PRODUCT_MAPPING_PATH = DEFAULT_INDEX_DIR / "product_index.npy"

queries = [
    "leather conditioner for dry leather",
    "cleanser for acne prone skin",
    "waterproof eyebrow makeup",
    "something that smells good",
    "something for the morning routine",
]

engine = VectorEngine.from_saved(PRODUCT_INDEX_PATH, PRODUCT_MAPPING_PATH)

with connect_db(DEFAULT_DB_PATH) as conn:
    for query in queries:
        print(f"\n---- QUERY: {query} ----")
        results = engine.search(query, k=5)

        for parent_asin, score in results:
            row = conn.execute("""
                SELECT title, store, average_rating, price_value
                FROM products
                WHERE parent_asin = ?
            """, (parent_asin,)).fetchone()

            if row:
                print(f"  Title: {row['title']}")
                print(f"  Store: {row['store']}")
                print(f"  Rating: {row['average_rating']}")
                print(f"  Price: {row['price_value']}")
                print(f"  Similarity Score: {score:.4f}")
                print("  ---")
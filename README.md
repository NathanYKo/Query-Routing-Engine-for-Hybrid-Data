# Query Routing Engine for Hybrid Data

Small class project for routing product queries to the right search path.

It supports:

- SQL for structured queries
- review-vector search for review-style queries
- FAISS product search for semantic product queries
- mixed SQL + FAISS routing for queries that have both filters and semantic intent

## Setup

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Create the database:

```powershell
python scripts/init_sqlite_db.py
```

Load the sample data:

```powershell
python scripts/load_amazon_reviews.py `
  --category All_Beauty `
  --reviews data/sample/All_Beauty.sample.jsonl `
  --metadata data/sample/meta_All_Beauty.sample.jsonl
```

Build the review index:

```powershell
python scripts/build_review_index.py
```

Build the product FAISS index:

```powershell
python scripts/build_product_index.py
```

## Run

Structured query:

```powershell
python scripts/run_query.py --query "products under 15 dollars"
```

Review query:

```powershell
python scripts/run_query.py --query "what do reviews say about smell"
```

Semantic product query:

```powershell
python scripts/run_query.py --query "lightweight hair product"
```

Mixed query:

```powershell
python scripts/run_query.py --query "top rated shampoo for volume"
```

## Evaluation

Run the fixed evaluation set and write a markdown report:

```powershell
python scripts/evaluate_queries.py
```

The query list lives in `docs/evaluation_queries.md`, and the generated report is written to `docs/evaluation_results.md`.

## Main Files

- `scripts/load_amazon_reviews.py`: load Amazon data into SQLite
- `scripts/build_review_index.py`: build review embeddings
- `scripts/build_product_index.py`: build FAISS product index
- `scripts/run_query.py`: analyze and run queries
- `scripts/evaluate_queries.py`: run the fixed routing evaluation and generate the results report
- `scripts/routing_engine.py`: routing logic

## Notes

- Default database path: `data/sqlite/hybrid_router.db`
- Default index path: `data/index`
- Sample data lives in `data/sample`

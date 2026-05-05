# Evaluation Results

## Setup

- Database: `C:\Users\knath\Downloads\cs257\Project\Query-Routing-Engine-for-Hybrid-Data\data\sqlite\milestone_demo.db`
- Index directory: `C:\Users\knath\Downloads\cs257\Project\Query-Routing-Engine-for-Hybrid-Data\data\index\milestone_demo`
- Dataset size: `4` products and `8` reviews
- Top-k: `5`
- Query set: `13` fixed evaluation queries from `docs/evaluation_queries.md`
- Route matches: `13/13`

## Headline Findings

- Routing matched the expected engine for all `13/13` queries.
- Structured SQL was the fastest path, averaging `0.0007` seconds across `3` queries.
- All `6/6` pure vector queries returned non-empty results. Product-vector latency showed a cold-start effect in this run: max `6.3217` seconds versus median `0.1060` seconds.
- Mixed routing matched expectation on all `4/4` mixed queries, but only `2/4` completed reranking and `2/4` returned zero rows after SQL candidate filtering.

## Category Summary

| Category | Queries | Route Matches | Non-empty | Avg sec | Median sec | Executed engines | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Structured SQL | 3 | 3/3 | 3/3 | 0.0007 | 0.0003 | sql (3) | All structured queries stayed on SQL and returned results. |
| Product Vector | 3 | 3/3 | 3/3 | 2.1774 | 0.1060 | product-vector (3) | One slower first vector lookup dominates the average; later product-vector queries were warm. |
| Review Vector | 3 | 3/3 | 3/3 | 0.1037 | 0.1039 | review-vector (3) | All review-vector queries returned five review hits. |
| Mixed | 4 | 4/4 | 2/4 | 0.0550 | 0.0526 | mixed (2), sql (2) | 2 queries completed reranking; 2 queries stayed on SQL after candidate filtering. |

## Caveats

- This report checks routing correctness and basic execution behavior on the demo dataset; it is not a formal relevance benchmark.
- The demo database is intentionally small, so zero-result mixed queries mainly reflect sparse candidate coverage rather than a scalability result.
- `Routed` records the analyzer decision, while `Executed` records the engine actually used after mixed-query candidate filtering or fallbacks.
- Vector timings include one-time model loading inside the Python process, so the first vector lookup is a cold-start measurement.

## Detailed Results

| Category | Query | Expected | Routed | Executed | Mode | Seconds | Results | Top Result | Route |
| --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| Structured SQL | products under 15 dollars | sql | sql | sql | products under price threshold | 0.0017 | 3 | Volume Dry Shampoo | PASS |
| Structured SQL | top rated products | sql | sql | sql | top-rated products | 0.0001 | 4 | Hydrating Curl Cream | PASS |
| Structured SQL | products by Sample Beauty Co. | sql | sql | sql | keyword product search | 0.0003 | 2 | Hydrating Curl Cream | PASS |
| Product Vector | waterproof eyebrow makeup | product-vector | product-vector | product-vector | FAISS semantic product search | 6.3217 | 4 | Sea Salt Texture Spray | PASS |
| Product Vector | lightweight hair product | product-vector | product-vector | product-vector | FAISS semantic product search | 0.1046 | 4 | Unscented Leave-In Conditioner | PASS |
| Product Vector | something that smells good | product-vector | product-vector | product-vector | FAISS semantic product search | 0.1060 | 4 | Sea Salt Texture Spray | PASS |
| Review Vector | what do reviews say about smell | review-vector | review-vector | review-vector | review vector search | 0.1027 | 5 | Sea Salt Texture Spray | PASS |
| Review Vector | customer reviews about frizz control | review-vector | review-vector | review-vector | review vector search | 0.1045 | 5 | Hydrating Curl Cream | PASS |
| Review Vector | feedback on sensitive scalp products | review-vector | review-vector | review-vector | review vector search | 0.1039 | 5 | Unscented Leave-In Conditioner | PASS |
| Mixed | top rated waterproof eyebrow makeup | mixed | mixed | sql | SQL candidate filter + FAISS rerank | 0.0003 | 0 | none | PASS |
| Mixed | products under 20 dollars for dry skin | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 0.1049 | 1 | Volume Dry Shampoo | PASS |
| Mixed | Sample Beauty Co. product for volume | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 0.1146 | 1 | Sea Salt Texture Spray | PASS |
| Mixed | top rated product that smells good | mixed | mixed | sql | SQL candidate filter + FAISS rerank | 0.0004 | 0 | none | PASS |

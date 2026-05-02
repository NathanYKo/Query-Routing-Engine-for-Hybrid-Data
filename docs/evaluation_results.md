# Evaluation Results

- Database: `C:\Users\knath\Downloads\cs257\Project\Query-Routing-Engine-for-Hybrid-Data\data\sqlite\milestone_demo.db`
- Index directory: `C:\Users\knath\Downloads\cs257\Project\Query-Routing-Engine-for-Hybrid-Data\data\index\milestone_demo`
- Top-k: `5`
- Route matches: `13/13`

| Category | Query | Expected | Routed | Executed | Mode | Seconds | Results | Top Result | Route |
| --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| Structured SQL | products under 15 dollars | sql | sql | sql | products under price threshold | 0.0028 | 3 | Volume Dry Shampoo | PASS |
| Structured SQL | top rated products | sql | sql | sql | top-rated products | 0.0003 | 4 | Hydrating Curl Cream | PASS |
| Structured SQL | products by Sample Beauty Co. | sql | sql | sql | keyword product search | 0.0005 | 2 | Hydrating Curl Cream | PASS |
| Product Vector | waterproof eyebrow makeup | product-vector | product-vector | product-vector | FAISS semantic product search | 6.8307 | 4 | Sea Salt Texture Spray | PASS |
| Product Vector | lightweight hair product | product-vector | product-vector | product-vector | FAISS semantic product search | 0.1145 | 4 | Unscented Leave-In Conditioner | PASS |
| Product Vector | something that smells good | product-vector | product-vector | product-vector | FAISS semantic product search | 0.1039 | 4 | Sea Salt Texture Spray | PASS |
| Review Vector | what do reviews say about smell | review-vector | review-vector | review-vector | review vector search | 0.1076 | 5 | Sea Salt Texture Spray | PASS |
| Review Vector | customer reviews about frizz control | review-vector | review-vector | review-vector | review vector search | 0.1091 | 5 | Hydrating Curl Cream | PASS |
| Review Vector | feedback on sensitive scalp products | review-vector | review-vector | review-vector | review vector search | 0.1122 | 5 | Unscented Leave-In Conditioner | PASS |
| Mixed | top rated waterproof eyebrow makeup | mixed | mixed | sql | SQL candidate filter + FAISS rerank | 0.0004 | 0 | none | PASS |
| Mixed | products under 20 dollars for dry skin | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 0.1063 | 1 | Volume Dry Shampoo | PASS |
| Mixed | Sample Beauty Co. product for volume | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 0.1140 | 1 | Sea Salt Texture Spray | PASS |
| Mixed | top rated product that smells good | mixed | mixed | sql | SQL candidate filter + FAISS rerank | 0.0004 | 0 | none | PASS |

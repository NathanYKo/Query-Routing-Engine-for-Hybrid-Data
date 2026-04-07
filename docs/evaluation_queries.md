# Evaluation Query Set

This query set is intended for the current hybrid routing demo and milestone report.

## Structured SQL Queries

1. `top rated beauty products` -> expected route: `sql`, expected mode: `top-rated products`
2. `products under 15 dollars` -> expected route: `sql`, expected mode: `products under price threshold`
3. `products by Sample Beauty Co.` -> expected route: `sql`, expected mode: `keyword product search`

## Review-Vector Queries

1. `what do reviews say about smell` -> expected route: `review-vector`, expected mode: `review vector search`
2. `customer reviews about frizz control` -> expected route: `review-vector`, expected mode: `review vector search`
3. `feedback on sensitive scalp products` -> expected route: `review-vector`, expected mode: `review vector search`

## FAISS Product Queries

1. `lightweight hair product` -> expected route: `product-vector`, expected mode: `FAISS semantic product search`
2. `product for curls and frizz` -> expected route: `product-vector`, expected mode: `FAISS semantic product search`
3. `unscented scalp product` -> expected route: `product-vector`, expected mode: `FAISS semantic product search`
4. `volume between washes` -> expected route: `product-vector`, expected mode: `FAISS semantic product search`

## Mixed Product Queries

1. `products under 15 dollars for frizzy hair` -> expected route: `mixed`, expected mode: `SQL-only mixed fallback` or `SQL candidate filter + FAISS rerank`
2. `top rated shampoo for volume` -> expected route: `mixed`, expected mode: `SQL candidate filter + FAISS rerank`
3. `Sample Beauty Co. product for curls` -> expected route: `mixed`, expected mode: `SQL-only mixed fallback` or `SQL candidate filter + FAISS rerank`
4. `All_Beauty products under 20 dollars for sensitive scalp` -> expected route: `mixed`, expected mode: `SQL candidate filter + FAISS rerank`

## Notes

- Mixed queries may legitimately choose SQL-only fallback when the SQL candidate set is already very small.
- Review-language queries take precedence over mixed product routing in the current prototype.
- If the FAISS product index is missing, product-vector and mixed queries fall back to SQL keyword search.

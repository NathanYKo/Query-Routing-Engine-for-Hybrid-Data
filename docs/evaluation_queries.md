# Evaluation Query Set

This fixed query set is used by `scripts/evaluate_queries.py` to compare expected routing behavior with the actual route and execution engine.

| Category | Query | Expected route |
| --- | --- | --- |
| Structured SQL | `products under 15 dollars` | `sql` |
| Structured SQL | `top rated products` | `sql` |
| Structured SQL | `products by Sample Beauty Co.` | `sql` |
| Product Vector | `waterproof eyebrow makeup` | `product-vector` |
| Product Vector | `lightweight hair product` | `product-vector` |
| Product Vector | `something that smells good` | `product-vector` |
| Review Vector | `what do reviews say about smell` | `review-vector` |
| Review Vector | `customer reviews about frizz control` | `review-vector` |
| Review Vector | `feedback on sensitive scalp products` | `review-vector` |
| Mixed | `top rated waterproof eyebrow makeup` | `mixed` |
| Mixed | `products under 20 dollars for dry skin` | `mixed` |
| Mixed | `Sample Beauty Co. product for volume` | `mixed` |
| Mixed | `top rated product that smells good` | `mixed` |

The evaluation result table records both the routed engine and executed engine. This matters because mixed routing can fall back to SQL when there are too few SQL candidates to justify FAISS reranking or when an index is missing.

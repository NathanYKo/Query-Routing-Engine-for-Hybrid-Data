# Evaluation Results

## Setup

- Database: `data\sqlite\all_beauty_100k.db`
- Index directory: `data\index\all_beauty_100k`
- Dataset size: `112590` products and `99678` reviews
- Top-k: `5`
- Query set: `17` fixed evaluation queries from `scripts/evaluate_queries.py`
- Route matches: `17/17`

## Headline Findings

- Routing matched the expected engine for all `17/17` queries.
- Structured SQL was the fastest path, averaging `1.7011` seconds across `3` queries.
- All `6/6` pure vector queries returned non-empty results. Product-vector latency showed a cold-start effect in this run: max `46.8019` seconds versus median `0.8956` seconds.
- Mixed routing matched expectation on all `8/8` mixed queries, and `8/8` completed reranking.

## Category Summary

| Category | Queries | Route Matches | Non-empty | Avg sec | Median sec | Executed engines | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| Structured SQL | 3 | 3/3 | 3/3 | 1.7011 | 1.5036 | sql (3) | All structured queries stayed on SQL and returned results. |
| Product Vector | 3 | 3/3 | 3/3 | 16.1623 | 0.8956 | product-vector (3) | One slower first vector lookup dominates the average; later product-vector queries were warm. |
| Review Vector | 3 | 3/3 | 3/3 | 1.0700 | 1.0270 | review-vector (3) | All review-vector queries returned five review hits. |
| Mixed | 8 | 8/8 | 8/8 | 1.1887 | 1.1116 | mixed (8) | 8 queries completed reranking; 0 queries stayed on SQL after candidate filtering. |

## Caveats

- This report checks routing correctness and basic execution behavior on the selected dataset; it is not a formal relevance benchmark.
- Zero-result mixed queries can reflect sparse candidate coverage or aggressive structured filtering, not necessarily a routing error.
- `Routed` records the analyzer decision, while `Executed` records the engine actually used after mixed-query candidate filtering or fallbacks.
- Vector timings include one-time model loading inside the Python process, so the first vector lookup is a cold-start measurement.

## Detailed Results

| Category | Query | Expected | Routed | Executed | Mode | Seconds | Results | Top Result | Route |
| --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |
| Structured SQL | products under 15 dollars | sql | sql | sql | products under price threshold | 2.9177 | 5 | Children's Hairpin Decorative Bow Headdress Hairpin Horsetail Clip | PASS |
| Structured SQL | top rated products | sql | sql | sql | top-rated products | 0.6820 | 5 | WB06X10309 Filter Microwave Oven Grease Filter for GE and Hotpoint Microwave-Aftermarke... | PASS |
| Structured SQL | products by Bath & Body Works | sql | sql | sql | keyword product search | 1.5036 | 5 | Bath and Body Works 2 Pack Snowflakes & Cashmere Shower Gel 10 Oz. | PASS |
| Product Vector | waterproof eyebrow makeup | product-vector | product-vector | product-vector | FAISS semantic product search | 46.8019 | 5 | Waterproof Eye Brow Eyebrow Pen Pencil With Brush Makeup 2-in-1 Cosmetic Tool Makeup Ac... | PASS |
| Product Vector | lightweight hair product | product-vector | product-vector | product-vector | FAISS semantic product search | 0.7893 | 5 | 14 inch Long Straight Soft Human Hair Easy to Wear 1pc 45g #1 Natural Black | PASS |
| Product Vector | something that smells good | product-vector | product-vector | product-vector | FAISS semantic product search | 0.8956 | 5 | It's All Good - 100% Natural Deodorant - Non-Toxic, No Aluminum, Parabens, Talc, Propyl... | PASS |
| Review Vector | what do reviews say about smell | review-vector | review-vector | review-vector | review vector search | 1.1698 | 5 | InstaNatural Cellulite Cream - With Caffeine & Retinol - Body Firming Solution & Tighte... | PASS |
| Review Vector | customer reviews about frizz control | review-vector | review-vector | review-vector | review vector search | 1.0132 | 5 | White Rain Volumizing Weightless Mousse (Pack of 3) | PASS |
| Review Vector | feedback on sensitive scalp products | review-vector | review-vector | review-vector | review vector search | 1.0270 | 5 | OKAY BLACK JAMAICAN CASTOR OIL MOISTURE GROWTH SHAMPOO 12oz / 355ml | PASS |
| Mixed | top rated waterproof eyebrow makeup | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.5721 | 5 | UCANBE Black Precision Waterproof Liquid Eyeliner - Set of 2 | PASS |
| Mixed | products under 20 dollars for dry skin | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.1250 | 5 | Vaseline All Purpose Cream with Vaseline Jelly with Glycerine & Vitamin E | PASS |
| Mixed | Maybelline New York waterproof eyebrow makeup | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.0502 | 5 | Maybelline Define-A-Lash Waterproof Mascara, Very Black [811], 0.22 oz (Pack of 2) | PASS |
| Mixed | Bath & Body Works product that smells good | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.0752 | 5 | Bath and Body Works - Confetti Daydream - Gift Set - Fine Fragrance Mist & Body Cream | PASS |
| Mixed | L'Oreal Paris product for volume | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.1016 | 5 | L'Oreal Advanced Haircare Volume Filler Fiber Amplifying Concentrate Ampoules 0.5 oz, 3... | PASS |
| Mixed | products under 15 dollars for sensitive skin | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.1216 | 5 | Bath Exfoliating Gloves Nylon Shower Gloves, Bath Scrubber bath body brush, Body Spa Ma... | PASS |
| Mixed | Bath & Body Works product for dry skin | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.1011 | 5 | Bath and Body Works Bahamas Passionfruit and Banana Flower (8fl oz./ 236 ml) Super Smoo... | PASS |
| Mixed | top rated product that smells good | mixed | mixed | mixed | SQL candidate filter + FAISS rerank | 1.3631 | 5 | Old Spice Body Wash for Men, Moisturize with Shea Butter, 25 Ounce (Pack of 4) | PASS |

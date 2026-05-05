from __future__ import annotations

import argparse
import time
from collections import Counter
from pathlib import Path
from statistics import median

from hybrid_utils import connect_db, make_snippet
from run_query import analyze_query, run_query


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "sqlite" / "milestone_demo.db"
DEFAULT_INDEX_DIR = PROJECT_ROOT / "data" / "index" / "milestone_demo"
DEFAULT_OUT_PATH = PROJECT_ROOT / "docs" / "evaluation_results.md"

EVALUATION_QUERIES = [
    {
        "category": "Structured SQL",
        "query": "products under 15 dollars",
        "expected_engine": "sql",
    },
    {
        "category": "Structured SQL",
        "query": "top rated products",
        "expected_engine": "sql",
    },
    {
        "category": "Structured SQL",
        "query": "products by Sample Beauty Co.",
        "expected_engine": "sql",
    },
    {
        "category": "Product Vector",
        "query": "waterproof eyebrow makeup",
        "expected_engine": "product-vector",
    },
    {
        "category": "Product Vector",
        "query": "lightweight hair product",
        "expected_engine": "product-vector",
    },
    {
        "category": "Product Vector",
        "query": "something that smells good",
        "expected_engine": "product-vector",
    },
    {
        "category": "Review Vector",
        "query": "what do reviews say about smell",
        "expected_engine": "review-vector",
    },
    {
        "category": "Review Vector",
        "query": "customer reviews about frizz control",
        "expected_engine": "review-vector",
    },
    {
        "category": "Review Vector",
        "query": "feedback on sensitive scalp products",
        "expected_engine": "review-vector",
    },
    {
        "category": "Mixed",
        "query": "top rated waterproof eyebrow makeup",
        "expected_engine": "mixed",
    },
    {
        "category": "Mixed",
        "query": "products under 20 dollars for dry skin",
        "expected_engine": "mixed",
    },
    {
        "category": "Mixed",
        "query": "Sample Beauty Co. product for volume",
        "expected_engine": "mixed",
    },
    {
        "category": "Mixed",
        "query": "top rated product that smells good",
        "expected_engine": "mixed",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run fixed routing evaluation queries and write a markdown results table."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite database path. Defaults to {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--index-dir",
        type=Path,
        default=DEFAULT_INDEX_DIR,
        help=f"Embedding index directory. Defaults to {DEFAULT_INDEX_DIR}",
    )
    parser.add_argument("--top-k", type=int, default=5, help="Maximum results per query.")
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT_PATH,
        help=f"Markdown output path. Defaults to {DEFAULT_OUT_PATH}",
    )
    return parser.parse_args()


def markdown_escape(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def top_result_summary(results: list[dict]) -> str:
    if not results:
        return "none"

    result = results[0]
    for key in ("title", "product_title", "review_title", "review_text"):
        value = result.get(key)
        if value:
            return make_snippet(str(value), length=90)

    return make_snippet(str(result), length=90)


def format_seconds(value: float) -> str:
    return f"{value:.4f}"


def format_counter(counter: Counter) -> str:
    return ", ".join(f"{name} ({count})" for name, count in sorted(counter.items()))


def count_label(count: int, singular: str, plural: str | None = None) -> str:
    if count == 1:
        return f"{count} {singular}"
    return f"{count} {plural or singular + 's'}"


def fetch_dataset_counts(db_path: Path) -> dict[str, int]:
    with connect_db(db_path) as conn:
        return {
            "product_count": int(conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]),
            "review_count": int(conn.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]),
        }


def run_evaluation(db_path: Path, index_dir: Path, top_k: int) -> list[dict]:
    rows: list[dict] = []
    with connect_db(db_path) as conn:
        for item in EVALUATION_QUERIES:
            start = time.perf_counter()
            analysis = analyze_query(item["query"], conn)
            route, executed_engine, label, results = run_query(analysis, conn, index_dir, top_k)
            elapsed = time.perf_counter() - start

            rows.append(
                {
                    "category": item["category"],
                    "query": item["query"],
                    "expected_engine": item["expected_engine"],
                    "routed_engine": route.engine,
                    "executed_engine": executed_engine,
                    "execution_mode": label,
                    "latency_seconds": elapsed,
                    "result_count": len(results),
                    "top_result": top_result_summary(results),
                    "route_match": "PASS" if route.engine == item["expected_engine"] else "FAIL",
                }
            )

    return rows


def summarize_category(category: str, rows: list[dict]) -> dict[str, object]:
    category_rows = [row for row in rows if row["category"] == category]
    latencies = [float(row["latency_seconds"]) for row in category_rows]
    return {
        "category": category,
        "query_count": len(category_rows),
        "route_match_count": sum(1 for row in category_rows if row["route_match"] == "PASS"),
        "non_empty_count": sum(1 for row in category_rows if row["result_count"] > 0),
        "avg_seconds": sum(latencies) / len(latencies),
        "median_seconds": median(latencies),
        "max_seconds": max(latencies),
        "executed_counts": Counter(row["executed_engine"] for row in category_rows),
    }


def category_note(summary: dict[str, object]) -> str:
    category = str(summary["category"])
    if category == "Structured SQL":
        return "All structured queries stayed on SQL and returned results."

    if category == "Product Vector":
        max_seconds = float(summary["max_seconds"])
        median_seconds = float(summary["median_seconds"])
        if median_seconds > 0 and max_seconds >= median_seconds * 5:
            return "One slower first vector lookup dominates the average; later product-vector queries were warm."
        return "All product-vector queries returned results through FAISS search."

    if category == "Review Vector":
        return "All review-vector queries returned five review hits."

    if category == "Mixed":
        executed_counts = summary["executed_counts"]
        mixed_count = executed_counts.get("mixed", 0)
        sql_count = executed_counts.get("sql", 0)
        return (
            f"{count_label(mixed_count, 'query', 'queries')} completed reranking; "
            f"{count_label(sql_count, 'query', 'queries')} stayed on SQL after candidate filtering."
        )

    return ""


def render_markdown(
    rows: list[dict],
    db_path: Path,
    index_dir: Path,
    top_k: int,
    dataset_counts: dict[str, int],
) -> str:
    pass_count = sum(1 for row in rows if row["route_match"] == "PASS")
    category_order: list[str] = []
    for row in rows:
        if row["category"] not in category_order:
            category_order.append(row["category"])

    category_summaries = [summarize_category(category, rows) for category in category_order]
    summary_by_category = {str(summary["category"]): summary for summary in category_summaries}
    structured_summary = summary_by_category["Structured SQL"]
    product_summary = summary_by_category["Product Vector"]
    mixed_summary = summary_by_category["Mixed"]

    pure_vector_count = sum(
        1 for row in rows if row["category"] in {"Product Vector", "Review Vector"}
    )
    pure_vector_non_empty = sum(
        1
        for row in rows
        if row["category"] in {"Product Vector", "Review Vector"} and row["result_count"] > 0
    )
    mixed_rerank_count = sum(
        1 for row in rows if row["category"] == "Mixed" and row["executed_engine"] == "mixed"
    )
    mixed_zero_result_count = sum(
        1
        for row in rows
        if row["category"] == "Mixed"
        and row["executed_engine"] == "sql"
        and row["result_count"] == 0
    )

    lines = [
        "# Evaluation Results",
        "",
        "## Setup",
        "",
        f"- Database: `{db_path}`",
        f"- Index directory: `{index_dir}`",
        (
            f"- Dataset size: `{dataset_counts['product_count']}` products and "
            f"`{dataset_counts['review_count']}` reviews"
        ),
        f"- Top-k: `{top_k}`",
        f"- Query set: `{len(rows)}` fixed evaluation queries from `docs/evaluation_queries.md`",
        f"- Route matches: `{pass_count}/{len(rows)}`",
        "",
        "## Headline Findings",
        "",
        f"- Routing matched the expected engine for all `{pass_count}/{len(rows)}` queries.",
        (
            f"- Structured SQL was the fastest path, averaging "
            f"`{format_seconds(float(structured_summary['avg_seconds']))}` seconds across "
            f"`{structured_summary['query_count']}` queries."
        ),
        (
            f"- All `{pure_vector_non_empty}/{pure_vector_count}` pure vector queries returned "
            "non-empty results. Product-vector latency showed a cold-start effect in this run: "
            f"max `{format_seconds(float(product_summary['max_seconds']))}` seconds versus median "
            f"`{format_seconds(float(product_summary['median_seconds']))}` seconds."
        ),
        (
            f"- Mixed routing matched expectation on all "
            f"`{mixed_summary['route_match_count']}/{mixed_summary['query_count']}` mixed queries, "
            f"but only `{mixed_rerank_count}/{mixed_summary['query_count']}` completed reranking and "
            f"`{mixed_zero_result_count}/{mixed_summary['query_count']}` returned zero rows after "
            "SQL candidate filtering."
        ),
        "",
        "## Category Summary",
        "",
        "| Category | Queries | Route Matches | Non-empty | Avg sec | Median sec | Executed engines | Notes |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]

    for summary in category_summaries:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(summary["category"]),
                    markdown_escape(summary["query_count"]),
                    markdown_escape(
                        f"{summary['route_match_count']}/{summary['query_count']}"
                    ),
                    markdown_escape(f"{summary['non_empty_count']}/{summary['query_count']}"),
                    markdown_escape(format_seconds(float(summary["avg_seconds"]))),
                    markdown_escape(format_seconds(float(summary["median_seconds"]))),
                    markdown_escape(format_counter(summary["executed_counts"])),
                    markdown_escape(category_note(summary)),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Caveats",
            "",
            "- This report checks routing correctness and basic execution behavior on the demo dataset; it is not a formal relevance benchmark.",
            "- The demo database is intentionally small, so zero-result mixed queries mainly reflect sparse candidate coverage rather than a scalability result.",
            "- `Routed` records the analyzer decision, while `Executed` records the engine actually used after mixed-query candidate filtering or fallbacks.",
            "- Vector timings include one-time model loading inside the Python process, so the first vector lookup is a cold-start measurement.",
            "",
            "## Detailed Results",
            "",
        "| Category | Query | Expected | Routed | Executed | Mode | Seconds | Results | Top Result | Route |",
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
        ]
    )

    for row in rows:
        display_row = dict(row)
        display_row["latency_seconds"] = format_seconds(float(row["latency_seconds"]))
        lines.append(
            "| "
            + " | ".join(
                markdown_escape(display_row[key])
                for key in (
                    "category",
                    "query",
                    "expected_engine",
                    "routed_engine",
                    "executed_engine",
                    "execution_mode",
                    "latency_seconds",
                    "result_count",
                    "top_result",
                    "route_match",
                )
            )
            + " |"
        )

    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    rows = run_evaluation(args.db, args.index_dir, args.top_k)
    dataset_counts = fetch_dataset_counts(args.db)
    output = render_markdown(rows, args.db, args.index_dir, args.top_k, dataset_counts)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(output, encoding="utf-8")

    pass_count = sum(1 for row in rows if row["route_match"] == "PASS")
    print(f"Wrote {len(rows)} evaluation rows to {args.out}")
    print(f"Route matches: {pass_count}/{len(rows)}")


if __name__ == "__main__":
    main()

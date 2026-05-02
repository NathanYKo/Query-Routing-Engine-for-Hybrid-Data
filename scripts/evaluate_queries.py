from __future__ import annotations

import argparse
import time
from pathlib import Path

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
                    "latency_seconds": f"{elapsed:.4f}",
                    "result_count": len(results),
                    "top_result": top_result_summary(results),
                    "route_match": "PASS" if route.engine == item["expected_engine"] else "FAIL",
                }
            )

    return rows


def render_markdown(rows: list[dict], db_path: Path, index_dir: Path, top_k: int) -> str:
    pass_count = sum(1 for row in rows if row["route_match"] == "PASS")
    lines = [
        "# Evaluation Results",
        "",
        f"- Database: `{db_path}`",
        f"- Index directory: `{index_dir}`",
        f"- Top-k: `{top_k}`",
        f"- Route matches: `{pass_count}/{len(rows)}`",
        "",
        "| Category | Query | Expected | Routed | Executed | Mode | Seconds | Results | Top Result | Route |",
        "| --- | --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
    ]

    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                markdown_escape(row[key])
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
    output = render_markdown(rows, args.db, args.index_dir, args.top_k)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(output, encoding="utf-8")

    pass_count = sum(1 for row in rows if row["route_match"] == "PASS")
    print(f"Wrote {len(rows)} evaluation rows to {args.out}")
    print(f"Route matches: {pass_count}/{len(rows)}")


if __name__ == "__main__":
    main()

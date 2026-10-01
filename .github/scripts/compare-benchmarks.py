#!/usr/bin/env python3
"""Compare RediSearchQueryBenchmark results from a base and a head build.

Usage: compare-benchmarks.py --base base-1.csv [base-2.csv ...] --head head-1.csv [head-2.csv ...]

Samples from multiple runs of the same side are pooled. Prints a Markdown table. A query is
flagged as slower (or faster) when its median moves by more than the threshold and the two
sides' interquartile ranges don't overlap, so ordinary run-to-run noise isn't reported.
Exits with status 1 if any query returns a different number of rows on the two sides.
"""

import argparse
import csv
import statistics
import sys


def load(paths):
    queries = {}
    for path in paths:
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                entry = queries.setdefault(row["query"], {"rows": set(), "samples": []})
                entry["rows"].add(int(row["rows"]))
                entry["samples"].extend(float(s) for s in row["samples_ms"].split(";"))
    return queries


def quartiles(samples):
    q1, median, q3 = statistics.quantiles(samples, n=4, method="inclusive")
    return q1, median, q3


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", nargs="+", required=True)
    parser.add_argument("--head", nargs="+", required=True)
    parser.add_argument("--threshold", type=float, default=10.0, help="percent change to flag (default 10)")
    args = parser.parse_args()

    base, head = load(args.base), load(args.head)
    rows_differ = False
    print("| Query | Rows | Base median (ms) | Head median (ms) | Change | |")
    print("|---|---:|---:|---:|---:|---|")
    for query in list(base) + [query for query in head if query not in base]:
        if query not in base or query not in head:
            print(f"| `{query}` | | | | | only in {'head' if query in head else 'base'} |")
            continue
        b, h = base[query], head[query]
        b_q1, b_median, b_q3 = quartiles(b["samples"])
        h_q1, h_median, h_q3 = quartiles(h["samples"])
        change = (h_median - b_median) / b_median * 100
        if b["rows"] != h["rows"]:
            rows_differ = True
            rows, verdict = f"{sorted(b['rows'])} vs {sorted(h['rows'])}", "❌ rows differ"
        else:
            rows = str(next(iter(b["rows"])))
            if change > args.threshold and h_q1 > b_q3:
                verdict = "⚠️ slower"
            elif change < -args.threshold and h_q3 < b_q1:
                verdict = "faster"
            else:
                verdict = ""
        print(f"| `{query}` | {rows} | {b_median:.1f} | {h_median:.1f} | {change:+.1f}% | {verdict} |")

    print()
    print(f"Base: {sum(len(q['samples']) for q in base.values())} samples from {len(args.base)} run(s); "
          f"head: {sum(len(q['samples']) for q in head.values())} samples from {len(args.head)} run(s).")
    return 1 if rows_differ else 0


if __name__ == "__main__":
    sys.exit(main())

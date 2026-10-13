#!/usr/bin/env python3
"""Print a table of matrix results grouped by case: report.py PREFIX [--results DIR]

PREFIX selects result directories, for example main-reference. Ranges are the
lowest and highest value across repeated runs.
"""
import argparse
import json
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent


def span(values, fmt="{:.2f}"):
    values = [v for v in values if v is not None]
    if not values:
        return "-"
    lo, hi = min(values), max(values)
    return fmt.format(lo) if lo == hi else f"{fmt.format(lo)}-{fmt.format(hi)}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prefix")
    parser.add_argument("--results", type=Path, default=HERE / "results")
    args = parser.parse_args()
    groups = defaultdict(list)
    failed = []
    for path in sorted(args.results.glob(f"{args.prefix}*/summary.json")):
        summary = json.loads(path.read_text())
        if summary.get("status") != "passed":
            failed.append(path.parent.name)
            continue
        # Drop the run index so repeats group together.
        groups[path.parent.name.rsplit("-", 1)[0]].append(summary)
    print("| Case | Delivered/s | p50 ms | p99 ms | p99.9 ms | CPU s/M | Switches/msg | Runs |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for name, runs in groups.items():
        worst = lambda key: [max(q[key] for q in r["latency_ms"]) for r in runs]
        print(f"| {name} | {span([r['delivered_per_sec'] for r in runs], '{:.0f}')} | {span(worst('p50_ms'))} "
              f"| {span(worst('p99_ms'))} | {span(worst('p999_ms'))} "
              f"| {span([r['cpu_seconds_per_million'] for r in runs])} "
              f"| {span([r['switches_per_message'] for r in runs])} | {len(runs)} |")
    if failed:
        print("\nFailed:", ", ".join(failed))


if __name__ == "__main__":
    main()

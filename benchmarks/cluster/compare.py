#!/usr/bin/env python3
"""Side-by-side medians for two or more result tags: compare.py TAG_A TAG_B...

Groups each tag's runs by suite, shape and queue mode, and prints the median
of every repeat, so the arms of an A/B line up row by row. Runs whose name
does not match a matrix.py result are skipped.
"""
import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
# After the tag: suite, shape, arm and run index, as matrix.py names results.
NAME = re.compile(r"^(?P<suite>[a-z0-9]+)-(?P<shape>(?:solo-)?[a-z]+[0-9]*)-(?P<arm>.+)-\d+$")


def load(results, tag):
    groups, failed = defaultdict(list), []
    for path in results.glob(f"{tag}-*/summary.json"):
        m = NAME.match(path.parent.name[len(tag) + 1:])
        if not m:
            continue
        summary = json.loads(path.read_text())
        if summary.get("status") != "passed":
            failed.append(path.parent.name)
            continue
        groups[(m["suite"], m["shape"], summary["label"]["queue_mode"])].append(summary)
    return groups, failed


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tags", nargs="+")
    parser.add_argument("--results", type=Path, default=HERE / "results")
    args = parser.parse_args()
    arms = {tag: load(args.results, tag) for tag in args.tags}
    med = statistics.median
    keys = sorted({key for groups, _ in arms.values() for key in groups})
    print("| Suite | Shape | Mode | Tag | Delivered/s | p50 ms | p99 ms | CPU s/M | Switches/msg | Runs |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for key in keys:
        for tag, (groups, _) in arms.items():
            runs = groups.get(key)
            if not runs:
                continue
            worst = lambda k: med([max(q[k] for q in r["latency_ms"]) for r in runs])
            print(f"| {key[0]} | {key[1]} | {key[2]} | {tag} | {med([r['delivered_per_sec'] for r in runs]):.0f} "
                  f"| {worst('p50_ms'):.2f} | {worst('p99_ms'):.1f} | {med([r['cpu_seconds_per_million'] for r in runs]):.1f} "
                  f"| {med([r['switches_per_message'] for r in runs]):.2f} | {len(runs)} |")
    for tag, (_, failed) in arms.items():
        if failed:
            print(f"\n{tag} failed: {', '.join(sorted(failed))}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Run a named suite of bench.py cases and keep every result.

Each case runs as its own bench.py invocation, so a crash in one never hides
another. Paired comparisons run in ABBA order. Results land in
--results/<tag>-<case>, and report.py prints them as a table.

    matrix.py reference --storage /mnt/nvme/fibril-bench --tag main
    matrix.py modes --storage /mnt/nvme/fibril-bench --tag main --nvme nvme0
    matrix.py lowrate --storage /mnt/nvme/fibril-bench --tag linger0 --linger-us 0

Flags matrix.py does not know, like --linger-us above, go to every bench.py run.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# Shapes: (copies, rate per queue, burst pattern). Rate 0 is saturation.
SHAPES = {
    "saturation": (3, 0, None),
    "smooth100": (3, 100, None),
    "burst100": (3, 100, "200,100"),
    "smooth10000": (3, 10000, None),
    "smooth2000": (3, 2000, None),
    "solo-saturation": (1, 0, None),
    "solo-smooth100": (1, 100, None),
    "solo-smooth10000": (1, 10000, None),
}


def abba(shape, a, b):
    return [(shape, arm, str(i)) for i, arm in enumerate([a, b, b, a])]


def repeat(shapes, arm, times):
    return [(shape, arm, str(i)) for i in range(times) for shape in shapes]


DURABLE = ("durable", [])
BUFFERED = ("buffered", ["--durability", "buffered"])
ALL_COPIES = ("all3", ["--assignment", "replica_durable:3"])
EARLY_OFF = ("early0", ["--early-replication", "0"])

SUITES = {
    # The reference matrix in BENCHMARKING.md, on the shipped defaults.
    "reference": repeat(["solo-saturation", "saturation", "smooth10000", "smooth100", "burst100"], DURABLE, 2)
    + repeat(["saturation", "smooth10000"], ALL_COPIES, 2)
    + repeat(["solo-saturation"], BUFFERED, 2),
    # Durable against buffered queues.
    "modes": abba("smooth100", DURABLE, BUFFERED) + abba("burst100", DURABLE, BUFFERED)
    + abba("solo-smooth100", DURABLE, BUFFERED) + abba("smooth10000", DURABLE, BUFFERED)
    + abba("saturation", DURABLE, BUFFERED),
    # Latency at low and bursty rates, three runs each.
    "lowrate": repeat(["smooth100", "burst100"], DURABLE, 3) + repeat(["smooth100", "burst100"], BUFFERED, 3),
    # Early replication off against on, durable queues.
    "early": abba("smooth10000", EARLY_OFF, DURABLE) + abba("saturation", EARLY_OFF, DURABLE)
    + abba("burst100", EARLY_OFF, DURABLE),
    # Background sync interval for buffered queues.
    "cadence": [("smooth10000", ("buffered-sync" + ms, ["--durability", "buffered", "--sync-interval-ms", ms]), str(i))
                for i, ms in enumerate(["50", "100", "250", "1000", "1000", "250", "100", "50"])],
    # Durable and buffered at the rates that matter for wakeup and CPU work.
    "cpu": repeat(["smooth100", "smooth10000", "saturation"], DURABLE, 2)
    + repeat(["smooth100", "smooth10000", "saturation"], BUFFERED, 2),
    # Peak throughput, durable and buffered, four runs each.
    "peak": repeat(["saturation"], DURABLE, 4) + repeat(["saturation"], BUFFERED, 4),
    # One queue fed by 1, 4 and 16 publisher connections at the same total
    # rate and credit, interleaved.
    "publishers": [(shape, (f"{mode}-c{n}", flags + ["--connections", str(n)]), str(i))
                   for i in range(2) for shape in ("smooth10000", "saturation")
                   for mode, flags in (DURABLE, BUFFERED) for n in (1, 4, 16)],
    # Publisher fan-in, 1 against 16 connections only.
    "fanin": [(shape, (f"{mode}-c{n}", flags + ["--connections", str(n)]), str(i))
              for i in range(2) for shape in ("smooth10000", "saturation")
              for mode, flags in (DURABLE, BUFFERED) for n in (1, 16)],
    # Idle queue cost: 0, 200 and 1000 extra quiet queues against the Keratin
    # writer buffer factor, with one durable queue at 10k/s.
    "idle": [("smooth10000", (f"q{n}-wb{factor}", ["--idle-queues", str(n), "--node-env",
                                                 f"FIBRIL_KERATIN_WRITER_BUFFER_FACTOR={factor}"]), str(i))
             for i in range(2) for factor in (128, 32, 16) for n in (0, 200, 1000)],
    # Eight queues at 2k/s each, owners spread over the nodes.
    "manyq": [(shape, (name + "-q8", flags + ["--queues", "8"]), run) for shape, (name, flags), run
              in abba("smooth2000", DURABLE, BUFFERED)],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("suite", choices=sorted(SUITES))
    parser.add_argument("--storage", type=Path, required=True, help="Filesystem root for broker data")
    parser.add_argument("--results", type=Path, default=HERE / "results")
    parser.add_argument("--tag", required=True, help="Names this batch, for example a commit or a setting")
    parser.add_argument("--nvme", help="NVMe controller (nvme0) whose APST latency tolerance must stay 0")
    parser.add_argument("--duration-secs", type=int, default=12)
    # Flags this script does not know go to every bench.py run.
    args, extra = parser.parse_known_args()
    tolerance = Path(f"/sys/class/nvme/{args.nvme}/power/pm_qos_latency_tolerance_us") if args.nvme else None
    args.results.mkdir(parents=True, exist_ok=True)
    failed = 0
    for shape, (arm, flags), run in SUITES[args.suite]:
        copies, rate, burst = SHAPES[shape]
        name = f"{args.tag}-{args.suite}-{shape}-{arm}-{run}"
        output = args.results / name
        summary = output / "summary.json"
        if summary.exists() and json.loads(summary.read_text()).get("status") == "passed":
            continue
        if output.exists():
            # Keep a failed attempt next to the rerun.
            attempt = 1
            while output.with_name(f"{name}-failed{attempt}").exists():
                attempt += 1
            output.rename(output.with_name(f"{name}-failed{attempt}"))
        if tolerance and tolerance.read_text().strip() != "0":
            sys.exit(f"{tolerance} is not 0, drive power management would distort latency")
        command = [sys.executable, str(HERE / "bench.py"), "--output", str(output), "--storage", str(args.storage),
                   "--copies", str(copies), "--rate", str(rate), "--duration-secs", str(args.duration_secs),
                   *(["--burst-pattern", burst] if burst else []), *flags, *extra]
        print("run", name, flush=True)
        with (args.results / f"{name}.log").open("w") as log:
            code = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT).returncode
        if code:
            failed += 1
            print("failed", name, f"see {args.results / (name + '.log')}", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()

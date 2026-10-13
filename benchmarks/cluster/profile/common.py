"""Shared helpers for reading perf recordings made by bench.py --perf."""
import json
import subprocess
from pathlib import Path


def samples(path, fields="ip,sym"):
    """Each sample as a list of (address, symbol) frames, innermost first."""
    out = subprocess.run(["perf", "script", "-i", str(path), "-F", fields],
                         capture_output=True, text=True, check=True).stdout
    for block in out.split("\n\n"):
        lines = [line.strip() for line in block.strip().splitlines() if line.strip()]
        if lines:
            yield lines


def frames(lines):
    return [line.split(" ", 1)[1] if " " in line else "" for line in lines]


def issued(path):
    """Messages issued in the run that produced a perf recording."""
    summary = Path(path).resolve().parent / "summary.json"
    return json.loads(summary.read_text()).get("issued") if summary.exists() else None

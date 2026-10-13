#!/usr/bin/env python3
"""Share of CPU samples per system call: syscalls.py perf-node-0.data...

Needs kernel symbols (kernel.kptr_restrict=0) when recorded. Samples outside a
system call count as user space or other kernel work (interrupts, scheduler).
"""
import re
import sys
from collections import Counter

from common import frames, samples

for path in sys.argv[1:]:
    counts, total = Counter(), 0
    for lines in samples(path):
        total += 1
        names = frames(lines)
        call = next((n for n in names if re.match(r"__(x64|do)_sys_", n)), None)
        if call:
            counts[re.sub(r"^__(x64|do)_sys_", "", call)] += 1
        elif lines[0].startswith("ffffffff"):
            counts["(kernel, outside a system call)"] += 1
        else:
            counts["(user space)"] += 1
    print(f"== {path}: {total} samples")
    for name, n in counts.most_common(14):
        print(f"  {100 * n / total:5.1f}% {name}")

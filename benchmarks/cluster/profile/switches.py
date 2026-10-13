#!/usr/bin/env python3
"""Where threads go to sleep: switches.py perf-node-0.data...

For recordings made with bench.py --perf switches. Groups context switches by
the call that blocked. Keratin's threads inherit their spawner's name, so the
blocking site tells them apart from Tokio workers.
"""
import sys
from collections import Counter

from common import issued, samples

SITES = [
    ("Tokio worker idle (condvar park)", "park_condvar"),
    ("Tokio I/O driver (epoll)", "epoll_wait"),
    ("Keratin writer waiting for work", "crossbeam_channel"),
    ("fdatasync", "fdatasync"),
    ("blocking pool idle or lock wait", "futex_wait"),
    ("socket write", "sendto"),
    ("socket writev", "writev"),
    ("file write", "__libc_write"),
    ("sched_yield", "sched_yield"),
    ("sleep", "nanosleep"),
]

for path in sys.argv[1:]:
    counts, total = Counter(), 0
    for lines in samples(path, "comm,tid,ip,sym"):
        total += 1
        block = "\n".join(lines)
        counts[next((name for name, key in SITES if key in block), "other")] += 1
    messages = issued(path)
    rate = f", {total / messages:.2f} per message (sampled)" if messages else ""
    print(f"== {path}: {total} switches{rate}")
    for name, n in counts.most_common():
        print(f"  {100 * n / total:5.1f}% {name}")

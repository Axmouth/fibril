#!/usr/bin/env python3
"""Inclusive CPU share by broker subsystem: subsystems.py perf-node-0.data...

For frame-pointer recordings (bench.py --perf fp with a build that keeps frame
pointers). A sample counts toward every subsystem on its stack.
"""
import sys

from common import samples

SUBSYSTEMS = [
    ("owner push replication", "run_owner_queue_push"),
    ("follower push apply", "QueuePushSink"),
    ("owner replication read", "read_owner_queue_records"),
    ("Keratin writer thread", "spawn_writer"),
    ("Keratin fsync loop", "fsync_loop"),
    ("client connection handler", "handle_connection"),
    ("publish path", "get_publisher"),
    ("delivery path", "delivery_loop"),
    ("queue actor", "stroma_core::state"),
    ("history validation", "validate_history_replication"),
    ("coordination", "ganglion"),
    ("admin HTTP", "axum"),
    ("auth hashing", "argon2"),
]

for path in sys.argv[1:]:
    blocks = ["\n".join(lines) for lines in samples(path)]
    total = len(blocks)
    kernel = sum(1 for b in blocks if b.startswith("ffffffff"))
    print(f"== {path}: {total} samples, {100 * kernel / total:.1f}% ending in the kernel")
    for name, key in SUBSYSTEMS:
        share = sum(1 for b in blocks if key in b)
        if share:
            print(f"  {100 * share / total:5.1f}% {name}")
    parked = sum(1 for b in blocks if "park" in b and not any(k in b for _, k in SUBSYSTEMS))
    print(f"  {100 * parked / total:5.1f}% Tokio parking with no broker frame")

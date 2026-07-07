#!/usr/bin/env python3
"""Measure fdatasync/fsync latency on a target filesystem.

Mirrors a durable-log append: pwrite a fixed block into a preallocated file,
then fdatasync (or fsync), timing each sync. Reports per-op latency percentiles
and the implied ceiling of sequential durable appends per second.
"""
import os, sys, time, statistics, argparse

ap = argparse.ArgumentParser()
ap.add_argument("dir")
ap.add_argument("-n", "--iters", type=int, default=2000)
ap.add_argument("-b", "--block", type=int, default=4096)
ap.add_argument("--fsync", action="store_true", help="use fsync instead of fdatasync")
a = ap.parse_args()

path = os.path.join(a.dir, f".fsync_probe_{os.getpid()}")
buf = os.urandom(a.block)
sync = os.fsync if a.fsync else os.fdatasync
fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o600)
try:
    os.ftruncate(fd, a.block * 256)  # preallocate so size metadata does not change
    sync(fd)
    lat = []
    off = 0
    for _ in range(a.iters):
        os.pwrite(fd, buf, off)
        t0 = time.perf_counter_ns()
        sync(fd)
        lat.append((time.perf_counter_ns() - t0) / 1e6)  # ms
        off = (off + a.block) % (a.block * 256)
finally:
    os.close(fd)
    os.unlink(path)

lat.sort()
def pct(p): return lat[min(len(lat) - 1, int(len(lat) * p))]
mean = statistics.fmean(lat)
print(f"  op={'fsync' if a.fsync else 'fdatasync'} block={a.block}B iters={a.iters}")
print(f"  mean={mean:.3f}ms  p50={pct(.50):.3f}  p90={pct(.90):.3f}  "
      f"p99={pct(.99):.3f}  max={lat[-1]:.3f}ms")
print(f"  -> ~{1000/mean:,.0f} durable appends/s (single-stream, one fsync each)")

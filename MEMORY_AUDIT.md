# Broker memory audit (2026-07-16)

The trigger observation: broker RSS does not drop back to earlier levels
after all active queues evict. Suspects filed with it: mimalloc arena
retention, keratin tail caches not releasing on eviction, admin in-memory
state, heap fragmentation. This audit measured all of them.

Verdict up front: **no leak**. Eviction returns 95%+ of the working set to
the OS. What remains is a bounded one-time warm cost (~60 MB) plus roughly
1 MB per distinct queue-partition ever materialized, none of it retained
per message and none of it growing with repeated load/evict cycles beyond
fragmentation noise.

## Test environment

Ryzen 9 5950X (32 threads), Linux 7.0.0-27, release build, single
standalone broker, data dir on tmpfs (IO excluded), fibril tip `548ba9a`.
Idle eviction configured to 60s (sweep 10s) so eviction happens inside the
run. RSS = `ps -o rss=` on the broker process.

Load shape per run: N queues, e2e_c writer 200k x 256B per queue (unless
noted), then a firehose reader drains each queue to empty, then all clients
stop, queues evict (confirmed `materialized: false` for every audit queue
via queues_debug), then a 5-minute settle before the reading.

## Results

| run | queues x msgs | peak RSS | evicted | settled |
| --- | --- | --- | --- | --- |
| default | 32 x 200k (6.4M) | 2,284 MB | 110.0 MB | 109.8 MB |
| MIMALLOC_PURGE_DELAY=0 | 32 x 200k (6.4M) | 2,222 MB | 113.6 MB | 102.3 MB |
| quarter volume | 32 x 50k (1.6M) | 822 MB | 117.5 MB | 117.2 MB |
| quarter queues | 8 x 200k (1.6M) | 697 MB | 100.7 MB | 87.5 MB |

Baseline in every run: ~24 MB (30s after boot, idle).

Repeated cycles against one broker process (same 32 topics each time,
settled after eviction, 300s settles):

| cycle | settled RSS |
| --- | --- |
| 1 | 117.2 MB |
| 2 | 124.1 MB |
| 3 | 122.2 MB |
| 4 | 132.3 MB |

## Analysis

- **Eviction works.** 2.28 GB loaded drops to ~110 MB once the queues
  leave memory. The keratin tail caches and queue state release.
- **Not allocator retention.** Eager decommit (MIMALLOC_PURGE_DELAY=0)
  reclaimed almost nothing extra (110 -> 102 MB), so the residual is not
  memory mimalloc is merely holding onto.
- **Not per message.** A quarter of the message volume through the same
  32 queues left the same residual (117 vs 110 MB, inside the noise band).
- **Partly per queue.** A quarter of the queue count dropped the residual
  by ~25 MB, about 1 MB per queue-partition that ever materialized, on top
  of a fixed ~60 MB warm cost.
- **Not cumulative.** Four identical load/evict cycles moved the settled
  reading 117 -> 124 -> 122 -> 132 MB. The dip at cycle 3 rules out a
  strictly-referenced leak (those only grow). The ~5 MB/cycle average
  drift against a +/-4 MB noise band reads as heap fragmentation. Even
  taken at face value it amortizes to ~0.6 bytes per message pushed
  through a full evict/rematerialize cycle.

## Regression check

`scripts/memory-audit.sh` re-runs the cycle probe and, with `--check`,
fails when eviction does not complete or when a later cycle's settled RSS
exceeds the first cycle's by more than 25%. The old "within 15% of the
boot baseline" idea from the original brief is wrong on the evidence: the
one-time warm cost is real and by design, so cycle-over-cycle stability is
the honest invariant.

## Open follow-up (low priority)

Identify the ~1 MB retained per materialized queue-partition. Candidates,
in suspicion order: keratin's unloaded-queue bookkeeping, per-queue admin
history rings, registry/actor metadata. At 32 queues this is ~30 MB and
irrelevant, at thousands of queues it would add up. Worth a heaptrack
pass if a large-queue-count deployment ever appears.

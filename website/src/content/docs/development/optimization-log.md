---
title: Optimization log
description: Benchmark-first notes for low-level Fibril performance work.
---

This page tracks low-level optimization ideas before they become code changes.
The rule is simple: define the starting point, explain why the change should
help, measure before and after, then keep the result even when the idea does
not work.

Optimization work should not start from taste. It should start from a concrete
cost model and a benchmark that can prove the result.

## Current measurement surface

Fibril already has useful end-to-end benchmarks:

- `scripts/bench-steady-c.sh` measures controlled-rate publish and delivery,
  latency percentiles, missing messages, publish errors, confirmation errors,
  server RSS, and queue snapshots.
- `scripts/bench-matrix.sh` runs repeatable steady-state scenarios and writes
  one results file plus one log file per case.
- `scripts/bench-e2e-c.sh` measures burst-style ingress and egress saturation.
- `scripts/bench-results-table.sh` turns steady benchmark result files into a
  Markdown table.

The current microbenchmark surface is still small, but
`crates/protocol/benches/encode.rs` is wired as a Criterion encode/decode bench
for publish frames. Before changing serialization, headers, batching, or
queue-handle lookup behavior, add a targeted microbench or an explicit
end-to-end benchmark case that isolates the expected effect.

## Baseline protocol

Before changing a low-level path, record:

- Git commit and whether the Stroma source is local or git-sourced.
- Rust version and build mode.
- Machine, CPU governor if known, disk type, and `ulimit -n`.
- Benchmark command, full environment variables, and output directory.
- Median result across at least three runs when the run is short enough.
- p50, p95, p99, max latency, missing messages, errors, server RSS avg and
  peak, and queue end state.
- Runtime warnings and errors from the benchmark log.

Use the same machine, same data directory class, same payload size, same
durability settings, same client count, same prefetch, and same benchmark
duration when comparing before and after.

## Standard baseline commands

Quick smoke:

```sh
OUT_DIR=/tmp/fibril-opt-smoke scripts/bench-matrix.sh smoke
```

Short 1KB baseline:

```sh
OUT_DIR=/tmp/fibril-opt-baseline WARMUP_SECS=2 DURATION_SECS=5 \
  scripts/bench-matrix.sh baseline confirmed
```

Latency knee check:

```sh
OUT_DIR=/tmp/fibril-opt-throughput WARMUP_SECS=2 DURATION_SECS=5 \
  scripts/bench-matrix.sh throughput-1k
```

Payload-size check:

```sh
OUT_DIR=/tmp/fibril-opt-payload WARMUP_SECS=2 DURATION_SECS=5 \
  scripts/bench-matrix.sh payload
```

For noisy low-level work, prefer running one scenario three times over running
many scenarios once. A one-run improvement is a clue, not a conclusion.

## Acceptance rules

Use these as defaults unless the experiment says otherwise:

- A change aimed at throughput should improve measured throughput or the
  latency knee by at least 10% without increasing p95 or p99 latency in the
  normal-load cases.
- A change aimed at latency should improve p95 or p99 by at least 10% without
  lowering sustainable throughput.
- A change aimed at memory should reduce server RSS peak or steady RSS by at
  least 10% without adding missing messages, errors, or a clear latency
  regression.
- A change that adds hot-path instrumentation must show that the instrumentation
  overhead is small enough to keep enabled.
- A change that moves bottlenecks between CPU, storage, and memory must say
  which bottleneck moved and why.

Small cleanups can still land without a visible benchmark win, but they should
not be described as optimizations unless the numbers support it.

## Candidate experiments

### Delivery loop allocation and scanning

Hypothesis: the delivery loop rebuilds a `Vec<Arc<ConsumerState>>`, scans
capacity, clones queue keys, allocates delivery records, and wakes the queue
for every delivered message. Reducing per-message work here could improve
1KB throughput and the high-rate latency knee.

Why it might help: this path runs once per delivered message and its cost is
mostly CPU, atomics, map operations, and allocation. These costs matter most
for small payloads where storage bandwidth is not yet the bottleneck.

Benchmark needed: `baseline`, `confirmed`, and `throughput-1k`. A microbench
for round-robin consumer selection would help if the implementation is
localized enough.

Risk: batching delivery too aggressively can hurt fairness, backpressure, and
tail latency.

Status: tuned once, then revisited in the 2026-07 hot-path audit
(PERF_AUDIT_HOT_PATHS.md at the repo root). The per-message costs are now
reduced: the delivery pump moves owned fields into the wire frame instead of
cloning (including the payload), metrics and activity tracking are per poll
batch, the loop no longer notifies itself per message, and the reverse
tag-lookup map is gone from the delivery and settle paths. Measured effect on
the standard scenarios was neutral and the 1KB latency knee did not move
(still between 350k/s and 400k/s), so the knee is set elsewhere, most likely
the engine pipeline or the per-message channel hop. The broker-to-pump hop was then batched (a `Vec` per consumer per
poll batch) and turned out to be the knee itself, moving saturation onset
from 350-400k/s to 500-600k/s.

### Publish sink batch shape

Hypothesis: the current publish sink uses immediate draining plus a 1ms coalesce
window for small batches. Tuning batch size, coalesce behavior, or confirmation
handoff could improve throughput under high publisher concurrency.

Why it might help: fewer storage append calls and fewer completion handoffs can
reduce scheduler and channel pressure.

Benchmark needed: `baseline`, `confirmed` with multiple confirmation windows,
and burst `bench-e2e-c`. Include low-rate steady runs to catch latency
regressions.

Risk: coalescing can make low-rate latency worse. Confirmation behavior is part
of client-visible correctness, so errors and missing confirms matter as much as
throughput.

Status: idea only.

### Protocol encode and header layout

Hypothesis: frame encode/decode and header maps may cost too much for small
messages, especially when most publishes have no user headers.

Why it might help: fewer allocations and fewer map entries can reduce CPU and
memory traffic. The current design already stores common metadata positionally,
which should be preserved unless a benchmark proves otherwise.

Benchmark needed: a properly wired Criterion bench for encode/decode with:

- 1KB payload, no headers.
- 1KB payload, content type only.
- 1KB payload, several user headers.
- 64KB payload, no headers.

Then run `baseline` to confirm the microbench result matters end to end.

Risk: custom serialization changes can make the protocol harder to evolve and
can lose against `rmp-serde` on real data.

Status: microbench exists for publish encode/decode.

### Keratin writer pipelining

Hypothesis: staging encode, write, fsync, and notification as a deeper pipeline
could improve durable write throughput.

Why it might help: the writer may be serializing work that could overlap,
especially for larger payloads where encoding, file I/O, and fsync do not need
to block the same stage.

Benchmark needed: payload and large-backlog scenarios, plus Keratin-level
writer microbenchmarks in the Keratin repository.

Risk: this lives below Fibril and must preserve durability ordering. It can
also improve large payloads while doing little or nothing for small payloads.

Status: idea only, belongs mostly in Keratin.

### Read-ahead for deliverable payloads

Hypothesis: a bounded memory cache of likely next deliverable messages could
reduce log reads on hot queues.

Why it might help: Stroma knows ready offsets. When the queue is draining in
order, reading ahead can improve locality and reduce per-message read overhead.

Benchmark needed: read-heavy steady runs, burst egress runs, and memory ceiling
tests with multiple payload sizes.

Risk: memory growth and cache churn can easily make this worse. This should not
start without a strict byte budget and visibility in metrics.

Status: idea only.

### Topic and group representation

Hypothesis: replacing repeated `String` clones with validated topic and group
types backed by compact or shared storage could reduce allocation and memory
traffic.

Why it might help: topic and group keys are cloned into records, maps, messages,
and admin snapshots.

Benchmark needed: microbench for publish and deliver record creation, plus
steady 1KB throughput runs. Also test many sparse queues because the memory
benefit may show there before it shows in single-queue throughput.

Risk: interners and shared strings can add locks or lifetime complexity. A bad
implementation can be slower than cloning small strings.

Status: idea only.

### Replica-durable confirm latency

Hypothesis: replica-durable confirmed publish latency is currently bounded by
the follower pull worker's caught-up poll interval. When a follower is already
caught up, the default next poll delay is `1000ms`, so the first confirmed
publish after an idle period may wait for the follower to poll again before the
owner can observe durable follower progress.

Why it might help: changing this path to owner notification, long-poll
replication reads, or a lower adaptive delay could reduce cluster confirmed
publish latency without changing standalone behavior.

Benchmark needed: one owner and one follower with replica-durable confirms
enabled. Run low offered loads to expose idle penalty, then a small knee sweep.
Record confirmed publish p50, p95, p99, timeout/error count, follower lag, and
server RSS.

Risk: lowering the poll interval globally can add idle CPU and network wakeups.
Notification or long-poll machinery can add complexity and must not hide owner
failure.

Status: audit finding, benchmark not yet written.

### Multi-queue follower catch-up

Hypothesis: the current protocol replication resolver keeps one serialized
connection per owner. This is simple and conservative, but it may bottleneck a
node that follows many queues from the same owner.

Why it might help: per-queue peers, a small peer pool, or request multiplexing
could increase catch-up throughput if the one-connection design becomes the
limit.

Benchmark needed: one follower catching up many queues from one owner, with both
small and medium payloads. Compare catch-up wall time, owner CPU, follower CPU,
connection count, and error behavior.

Risk: more connections or multiplexing can make failure handling harder and can
increase owner pressure. Keep the current design unless the benchmark proves it
is the bottleneck.

Status: audit finding, benchmark not yet written.

### Cluster routing and fan-in

Hypothesis: partitioned publish routing, not-owner redirects, and multi-partition
subscription fan-in can regress independently of standalone publish/delivery
benchmarks.

Why it might help: cluster routing adds topology lookup, redirect retry, multiple
client engines, and merged subscription streams. These costs should be visible
before TypeScript parity or further routing polish.

Benchmark needed: Rust client scenarios for keyed publish, keyless round-robin,
redirected publish, and fan-in subscriptions across multiple partitions.

Risk: optimizing routing too early can make the client API more complex. Measure
first, then only change the pieces that show up.

Status: audit finding, benchmark not yet written.

## Experiment record

Add entries here when an optimization is actually tried.

| Date | Commit | Experiment | Expected effect | Benchmark | Result | Decision |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-06-08 | investigation | Establish benchmark-first optimization process | Avoid unmeasured low-level changes | Existing benchmark inventory | No runtime change | Keep this log updated |
| 2026-06-08 | local experiment | Batch delivery metrics and queue activity touch per poll batch | Reduce per-message time reads, atomics, and queue wakes in the delivery loop | `throughput-1k`, 250k-500k/s, 2s warmup, 5s steady | Neutral at 250k/s, worse p95/p99 and RSS at 350k/s, roughly neutral to worse above that | Reverted. The existing per-message updates stay until a better-targeted change proves itself |
| 2026-06-08 | local experiment | Wire protocol encode/decode Criterion bench and add content-type/header/payload-size cases | Make protocol/header optimization measurable before changing wire layout | `cargo bench -p fibril-protocol --bench encode -- --sample-size 20` | 1KB encode+decode about 6.3-6.7µs. Three small user headers were only slightly slower than no headers. 64KB encode+decode about 373µs | Keep the bench. Header layout is not the first obvious bottleneck for current end-to-end runs |
| 2026-06-08 | local experiment | Reduce publish sink small-batch coalesce window from 1ms to 250µs | Keep useful batching while reducing artificial wait in normal and near-saturation runs | `baseline`, `confirmed`, and `throughput-1k`, 2s warmup, 5s steady | Low-rate 50k/150k was neutral. High-rate p95 improved from 471ms to 385ms at 350k/s, 1598ms to 1533ms at 400k/s, and 3304ms to 3267ms at 500k/s. RSS was also lower in high-rate runs | Keep 250µs. It improves the knee without increasing the window |
| 2026-06-08 | local experiment | Remove publish sink small-batch wait entirely | Avoid any coalescing delay and rely only on immediate channel draining | `throughput-1k`, 2s warmup, 5s steady | Good at 250k/s, but worse than 250µs at 350k/s and 400k/s. Similar at 500k/s | Reverted. Some short coalescing still helps around the backlog knee |
| 2026-06-08 | local experiment | Start the publish sink coalesce window at the previous flush instead of after the next message arrives | Avoid artificial sparse-message latency while keeping burst batching when traffic is already flowing | `baseline`, `confirmed`, and `throughput-1k`, 2s warmup, 5s steady | Low-rate 50k/150k stayed neutral. High-rate p95 improved from the 250µs always-wait run: 385ms to 337ms at 350k/s, 1533ms to 1418ms at 400k/s, and 3267ms to 3152ms at 500k/s | Keep. This gives the sparse case immediate flush behavior and improves the measured saturation knee |
| 2026-06-15 | tooling | Let the steady benchmark target an external broker/admin pair and reuse `cluster-tryout.sh` for live cluster-routed runs | Make cluster-routed and later replica-durable confirm runs comparable without changing the benchmark client shape | `cargo check -p fibril-benches --bin steady_c --locked`, shell syntax checks, 3-node Ganglion tryout smoke at 500/s | The tryout-backed run delivered 500 measured messages with 0 missing and p99 publish-to-deliver latency of 15ms. A later audit showed the follower had not applied the measured range, so these are cluster-routed numbers, not replica-durable numbers. External targets report RSS as unavailable because the wrapper does not own the server process | Keep. Next step is an operator or test harness path that can require follower catch-up and replica-durable confirmation |
| 2026-06-15 | tuning | Add replica-durable latency split and follower read-budget runtime settings | Distinguish client/confirm backlog from broker delivery time, then test whether follower batch limits were the replica-durable throughput ceiling | 3-node Ganglion tryout, `replica_durable:2`, 1 KiB payload, 50k/s target, 50k confirm window, heartbeat 1s, liveness TTL 30s, follower budgets 1024 messages/read, 1024 events/read, 16 iterations/tick | Measured about 45.6k/s with zero missing and zero errors. Follower tail reached benchmark writes and owner/follower cursors matched. publish-to-server-receive p99 was about 3.9s, while server-receive-to-deliver p99 was about 141ms | Keep the knobs. The first ceiling was partly follower-read-budget limited. The next question is confirm-window and client-side backpressure behavior |
| 2026-06-15 | investigation | Add temporary protocol codec timing logs gated by `FIBRIL_PROTOCOL_CODEC_TIMING` | Check whether large-payload replicated runs are starving the server, the benchmark client, or both during encode/decode/frame-copy work | 3-node Ganglion tryout, `replica_durable:2`, 1 MiB payloads, explicit codec timing enabled | The probe showed large encode/decode costs on both sides, and especially expensive follower-side decode of large `ReplicationReadOk` frames. Moving large replication response decode to `spawn_blocking` protects Tokio workers but does not reduce deserialization time | Refactor or remove the logs before merge. If this signal remains useful, convert it to sampled or aggregated metrics rather than per-frame logs. Real speedup likely needs a different replication wire shape or dedicated decode/apply pipeline |
| 2026-06-15 | fix | Add byte-aware owner replication read caps | Stop large-payload followers from requesting giant single `ReplicationReadOk` responses when record-count limits allow too much payload data | 256 KiB replica-durable run exposed responses growing into tens or hundreds of MiB, with follower decode taking seconds and publish confirms timing out | With `max_bytes_per_read = 8388608`, the same 256 KiB, 170/s, confirm-window 100 run completed with 0 publish errors, 0 confirm errors, 0 missing messages, p99 publish-to-deliver about 69ms, and matching owner/follower cursors | Keep. Longer-term transport may split payload and event streams so size limits are natural rather than envelope-level |
| 2026-06-15 | investigation | Sweep confirm windows for high-rate 1 KiB replica-durable publishes | Separate sustainable low-latency throughput from throughput achieved by allowing a large confirmation backlog | 3-node Ganglion tryout, `replica_durable:2`, 1 KiB payload, 50k/s target, confirm windows 5k, 7.5k, 10k, 20k, 50k | 5k-7.5k windows stayed low latency at about 30k-32k/s with p99 publish-to-deliver 17ms. 10k reached about 34.8k/s but p99 rose to 841ms. 20k-50k reached about 38.5k-46.9k/s but p99 stayed around 2.8s. Raising event/message read limits at the 7.5k window did not improve throughput | Keep the split latency metrics. The next target is owner ingest/confirm scheduling, client confirm pacing, or storage isolation, not just larger follower read batches |
| 2026-06-15 | investigation | Compare tmpfs-backed and disk-backed local cluster data dirs | Check whether the current replica-durable low-latency ceiling is mostly physical disk contention | Same 3-node tryout, 1 KiB payload, 50k/s target, 7.5k confirm window. Default `/tmp` is tmpfs. Disk comparison used `CLUSTER_TRYOUT_RUN_ROOT` under the repo filesystem | Tmpfs reached about 31.8k/s with p99 publish-to-deliver 17ms. Disk-backed reached about 28.2k/s with p99 28ms. Storage placement matters, but tmpfs still has the same broad ceiling shape | Keep the script run-root override. Prioritize scheduler, protocol, confirm pacing, and single-partition owner-path analysis before assuming this is only disk throughput |
| 2026-06-15 | investigation | Split replica-durable publish-only and consume-drain paths, then inspect protocol codec logs | Find whether the current ceiling is publish replication, consume/ack, client windowing, roundtrips, or serialization | 3-node Ganglion tryout on tmpfs, `replica_durable:2`, 1 KiB payload, 50k/s publish-only target. Tuned follower budgets: 4096 messages/read, 4096 events/read, 16 MiB/read, 64 iterations/tick. Larger-batch variant used 16k records/read and 64 MiB/read | Consume-drain reached about 105k/s after preload, so consume/ack was not the bottleneck. Publish-only stayed around 27k/s. Lowering retry poll to 1ms did not help. Larger batches barely changed throughput but raised confirm latency from about 360ms p50 to about 1.36s p50. Codec logs showed owner `ReplicationReadOk` encode around 13-16ms for ~4.8 MiB frames, while follower decode took about 117-130ms. Larger ~19 MiB frames decoded in roughly 460-590ms | Current primary bottleneck is follower-side bulk `ReplicationReadOk` MessagePack decode, not physical disk or simple roundtrip count. Next useful work is a replication-specific wire shape or a bounded decode/apply pipeline |
| 2026-07-03 | df89e33 | Release consumer credit at settle accept instead of after the settle fsync | Remove the ack group-commit latency from the consumer flow-control cycle | `confirmed` at 50k/150k with PREFETCH=64 and default 16384, 3 runs each side | At prefetch 64 and 150k/s, publish-to-deliver collapsed from p50 ~800ms / p99 ~1290ms to p50 10ms / p99 14ms, matching the prefetch-16k profile. Default-prefetch runs unchanged | Keep. The credit-bound regime is gone and at-least-once semantics are unchanged |
| 2026-07-03 | 86fdbe7 | Remove the per-publish ConfirmStream offset send and drain tasks | Remove one channel hop and task wakeup per confirmed publish plus a bounded-channel stall hazard | `baseline` plus `confirmed` at 50k/150k, 3 runs each side | Latency-neutral, all percentiles within run noise | Keep as overhead and hazard removal, not claimed as a latency win |
| 2026-07-03 | 3179dae, 150a65a, b7ef762 | Delivery pump field moves, per-batch delivery bookkeeping, batched ack settle path, reverse tag map removal, publish-arm cache-first ordering, cached transport sink | Cut per-message allocations, atomics, map ops, and channel awaits on the delivery and settle paths | `baseline`, `confirmed`, `throughput-1k` (250k-500k), 2 runs per step | Sub-knee scenarios neutral, and the 1KB knee did not move (still between 350k/s and 400k/s). This supersedes the reverted 2026-06-08 metrics-batching trial: bundled with the payload move and wake-path changes it showed no 350k/s regression | Keep. The knee is set elsewhere, most likely the engine pipeline or the per-message broker-to-pump channel hop |
| 2026-07-03 | 9d1efb9 | Batch the broker-to-pump delivery hop (one Vec per consumer per poll batch) | Amortize the per-message channel send and task wakeup on the delivery path | `baseline`, `confirmed`, `throughput-1k`, low-prefetch confirmed, plus 600k/700k rate probes | This hop was the knee. 400k/s went from p50 ~750ms to 16ms and 500k/s from p50 ~2.4s to 26-27ms at full rate with zero missing. Saturation onset moved from 350-400k/s to 500-600k/s and high-rate RSS roughly halved. Sub-knee and low-prefetch unchanged | Keep. The next ceiling sits past 500k/s, most likely serialization or the engine pipeline |
| 2026-06-16 | investigation | Add broker replication timing metrics to the cluster tryout output | Separate client-observed confirm latency from owner replica-gate wait, owner read cost, follower owner-read wait, follower apply cost, and whole follower tick time | 3-node Ganglion tryout, `replica_durable:2`, 1 KiB payload, 50k/s publish-only target, confirm window 1024, 4096 messages/events per read, 16 MiB/read | Actual rate was about 49k/s with confirm p50/p95/p99/max = 204/243/244/246ms. Owner `replica_confirm_wait` averaged about 0.033ms over 144k samples, while owner reads averaged about 1ms, follower owner-read await averaged about 6.3ms, follower apply averaged about 19.2ms, and whole follower ticks averaged about 552ms | Keep the metrics. This run says the visible latency is not mostly waiting after local append for follower progress. Next isolate local append completion latency and follower tick batching before changing durability semantics or poll cadence |

## Lessons so far

- End-to-end benchmarks are good enough to catch large regressions, but they do
  not isolate small serialization or allocation changes.
- Low-rate steady runs are as important as saturation runs because batching can
  improve peak throughput while making normal latency worse.
- Reducing obvious per-message metric work did not improve the measured
  high-rate TCP path in the first trial. Scheduler behavior and delivery-loop
  wake timing need more precise measurement before changing this path.
- For 1KB publish frames, MessagePack encode/decode is measurable but not large
  enough by itself to explain the current backlog knee.
- For internal replication reads, MessagePack was the wrong shape. The biggest
  replicated-throughput win so far came from replacing the hot
  `ReplicationReadOk` response with a raw, easy-to-parse binary frame carrying
  fixed metadata, raw message header/payload bytes, raw event bytes, and explicit
  offsets. That removed a large nested decode/re-encode cost from follower
  catch-up.
- Record-count limits are not enough for replication batches. Payload size can
  dominate, so byte caps must be part of the protocol and must rewrite progress
  metadata to the actual returned frontier.
- The publish sink benefits from a much shorter coalescing window than the
  original 1ms. Removing the wait entirely lost useful batching around the
  350k-400k/s knee. Measuring the window from the previous flush gives sparse
  traffic immediate sends while preserving useful batching under load.
- Large payload results are hardware-specific on the current SATA SSD machine.
  They are still useful for detecting memory and storage-regression direction.
- For replica-durable runs, publish-to-deliver latency can be misleading by
  itself. The current 50k/s tuned run shows most latency before server receive,
  while server-receive-to-deliver remains much lower. Keep reporting both.
- The first broker timing metrics show that replica-durable confirm latency is
  not automatically the same thing as post-local-append follower wait. In the
  50k/s window-1024 run, owner `replica_confirm_wait` averaged about 0.033ms
  while client-observed confirmation p50 was about 204ms. The next useful split
  is local append completion latency versus client/window backlog and follower
  tick/apply work.
- Replica-durable replication is now usable enough to benchmark seriously, but
  it is not a finished performance story. Current runs prove correctness and
  useful throughput, while the latency caveat is still real: higher throughput
  currently depends on enough outstanding confirms, and that backlog becomes
  visible client latency.
- Compared with mature replicated queues and logs, the main open performance
  questions are architectural rather than micro-optimizations: committed
  visibility watermarking, long-poll or push-hinted follower replication,
  cheaper watermark-based confirmation, better separation of payload transfer
  from event/progress movement, whole-frame versus streaming decode, and raw
  Keratin range replication that avoids decode/re-encode work where safe.
- Local multi-node benchmarks are useful stress tests, but they are not a full
  deployment model. They share CPU, page cache, and one drive, while also hiding
  real network cost.
- The next narrow measurement should split local append completion, confirm sink
  backlog, replica-gate wait, and follower apply/tick time before making more
  protocol or scheduling changes.
- Temporary timing probes are acceptable during an audit, but they should not
  remain as ad hoc logs. Promote useful signals to cheap aggregated metrics or
  sampled diagnostics, then remove the raw per-frame logging.
- Operator metrics should stay cheap and useful. Percentile-heavy measurement
  belongs in benchmark tools unless there is a clear production need.

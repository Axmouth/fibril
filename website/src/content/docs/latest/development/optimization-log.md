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

The current microbenchmark surface is thin. `crates/protocol/benches/encode.rs`
contains a Criterion encode/decode benchmark, but it is not yet wired as a
normal Cargo bench target. Before changing serialization, headers, batching, or
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

Status: idea only.

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

Status: microbench setup needed.

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

## Experiment record

Add entries here when an optimization is actually tried.

| Date | Commit | Experiment | Expected effect | Benchmark | Result | Decision |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-06-08 | investigation | Establish benchmark-first optimization process | Avoid unmeasured low-level changes | Existing benchmark inventory | No runtime change | Keep this log updated |

## Lessons so far

- End-to-end benchmarks are good enough to catch large regressions, but they do
  not isolate small serialization or allocation changes.
- Low-rate steady runs are as important as saturation runs because batching can
  improve peak throughput while making normal latency worse.
- Large payload results are hardware-specific on the current SATA SSD machine.
  They are still useful for detecting memory and storage-regression direction.
- Operator metrics should stay cheap and useful. Percentile-heavy measurement
  belongs in benchmark tools unless there is a clear production need.

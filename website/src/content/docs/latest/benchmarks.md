---
title: Benchmarks
description: Early performance observations for Fibril.
---

Current benchmark numbers are informal architecture checks, not claims of production capacity.

## Early observations

Internal measurements with the current TCP transport and durable path on a single Ubuntu node have observed:

| Workload | Observation |
| --- | --- |
| Ingress | roughly `250k+` messages/sec |
| Egress | roughly `250k+` messages/sec |
| Payload | 1KB messages |
| Machine | Ryzen 5950X |

Memory usage during these runs ranged from a few hundred MB at lower load to roughly 1-2GB near peak throughput, depending on queue depth, batching, and inflight state.

## Interpretation

These numbers are useful mostly as a sanity check:

- the durable path is not obviously too slow
- batching and the actor-like queue model are promising
- memory behavior still needs tuning
- larger payloads will shift bottlenecks toward memory, copying, storage, and network I/O

## Missing benchmark work

The project still needs:

- payload-size sweeps
- durability-setting comparisons
- richer latency histograms and structured output
- restart/replay timing
- multi-consumer fairness and backpressure scenarios

## Current TCP benchmark

The current TCP-layer benchmark helper is `e2e_c`. It is still an early
benchmark, but it now reports wall throughput, active receive throughput, sent
and received counts, missing receive count, retry metadata observed on
delivered messages, and latency percentiles from publish time to reader
delivery.

For a quick local run, use the benchmark script:

```sh
MESSAGES=500000 CLIENTS=10 SIZE=1024 PREFETCH=16384 scripts/bench-e2e-c.sh
```

The script builds the release server and benchmark binary, waits for
`/healthz`, runs a small warmup so lazy queue setup is out of the main
measurement path, then starts a reader and writer for the measured run.

Useful knobs:

| Variable | Default | Meaning |
| --- | --- | --- |
| `MESSAGES` | `500000` | Messages per client in the measured run |
| `CLIENTS` | `10` | Parallel reader and writer client count |
| `SIZE` | `1024` | Raw payload size in bytes |
| `PREFETCH` | `16384` | Reader subscription prefetch |
| `WARMUP_MESSAGES` | `1000` | Warmup messages before the measured run |
| `READY_SETTLE_SECONDS` | `0.5` | Pause after reader readiness before starting the writer |
| `IDLE_TIMEOUT_MS` | `10000` | Reader idle timeout before reporting partial receive counts |
| `CONFIRMED` | `0` | Set `1` to wait for publish confirmations for correctness/debug checks |
| `LOG_FILE` | temporary file | Build, server, and noisy runtime logs |
| `RESULTS_FILE` | temporary file | Deterministic benchmark summary and queue snapshots |

The helper can also be run manually. Start the server in one terminal:

```sh
cargo run --release --bin fibril-server
```

Start the writer in another terminal:

```sh
cargo run --release --bin e2e_c -- -m 500000 -c 10 --writer --size 1024
```

Start the reader in a third terminal, as close to the writer start time as
practical:

```sh
cargo run --release --bin e2e_c -- -m 500000 -c 10 --reader --prefetch 16384
```

The reader side prints latency percentiles when it receives messages. If the
reader goes idle before receiving the target count, it reports the partial
count and missing count instead of waiting indefinitely. The wall throughput
includes any idle timeout tail, while active receive throughput uses the span
between the first and last received message. Retry counts are read from
Fibril's reserved delivery metadata headers when present. Structured benchmark
output and scenario tables are still future work.

## Steady-state TCP benchmark

The burst benchmark intentionally lets writers run as fast as possible. That is
useful for saturation checks, but it can build backlog and make latency look
larger as the message count grows.

For latency at a controlled offered load, use the steady-state helper:

```sh
WRITERS=10 READERS=10 RATE_PER_SEC=100000 WARMUP_SECS=3 DURATION_SECS=10 \
  SIZE=1024 PREFETCH=16384 scripts/bench-steady-c.sh
```

The steady helper runs readers and writers in one coordinated process. It marks
warmup messages separately, measures only the configured steady window, and
prints both publish-to-delivery and server-receive-to-delivery latency. The
wrapper also writes full server logs and full benchmark results to files, then
prints a compact summary.

Useful knobs:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WRITERS` | `10` | Parallel writer clients |
| `READERS` | `10` | Parallel reader clients |
| `RATE_PER_SEC` | `100000` | Target aggregate publish rate |
| `WARMUP_SECS` | `5` | Warmup duration excluded from measured results |
| `DURATION_SECS` | `30` | Steady measurement duration |
| `DRAIN_TIMEOUT_SECS` | `10` | Time allowed for measured messages to drain |
| `SIZE` | `1024` | Raw payload size in bytes |
| `PREFETCH` | `16384` | Reader subscription prefetch |
| `CONFIRMED` | `0` | Set `1` to wait for publish confirmations |
| `LOG_FILE` | temporary file | Build, server, and noisy runtime logs |
| `RESULTS_FILE` | temporary file | Deterministic benchmark summary and queue snapshots |

Recent local exploratory run, using `WRITERS=10`, `READERS=10`, `SIZE=1024`,
`PREFETCH=16384`, `WARMUP_SECS=3`, and `DURATION_SECS=10`:

| Target rate | Actual measured rate | Missing | publish→deliver p50/p95/p99/max | server-receive→deliver p50/p95/p99/max | End queue |
| ---: | ---: | ---: | --- | --- | --- |
| 50k/s | 49,962/s | 0 | 17 / 25 / 29 / 63 ms | 11 / 17 / 19 / 52 ms | ready=0, inflight=0 |
| 100k/s | 99,785/s | 0 | 13 / 18 / 62 / 136 ms | 10 / 14 / 57 / 130 ms | ready=0, inflight=0 |
| 150k/s | 149,831/s | 0 | 12 / 17 / 173 / 225 ms | 10 / 14 / 171 / 222 ms | ready=0, inflight=0 |
| 200k/s | 199,733/s | 0 | 13 / 78 / 259 / 294 ms | 11 / 76 / 258 / 292 ms | ready=0, inflight=0 |
| 250k/s | 249,591/s | 0 | 14 / 260 / 367 / 397 ms | 12 / 258 / 365 / 394 ms | ready=0, inflight=0 |
| 300k/s | 298,528/s | 0 | 18 / 578 / 623 / 659 ms | 16 / 577 / 613 / 655 ms | ready=0, inflight=0 |

These results suggest the current local setup can drain the tested rates without
leaving ready or inflight messages behind, while latency tails start increasing
noticeably above roughly 150-200k messages/sec. Treat the table as a reproducible
development checkpoint, not a capacity promise.

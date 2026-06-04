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

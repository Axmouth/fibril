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

- repeatable benchmark scripts with recorded configuration
- payload-size sweeps
- durability-setting comparisons
- latency histograms, not only throughput
- restart/replay timing
- multi-consumer fairness and backpressure scenarios

## Current manual TCP benchmark

The current TCP-layer benchmark helper is `e2e_c`. It is useful for quick
throughput checks, but it is not yet a repeatable benchmark suite.

Start the server in one terminal:

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
cargo run --release --bin e2e_c -- -m 500000 -c 10 --reader
```

The current helper reports throughput only. Latency histograms, configurable
prefetch, structured output, and repeatable scenario tables are still future
work.

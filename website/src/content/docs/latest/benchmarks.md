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

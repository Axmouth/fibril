---
title: Project status
description: A candid feature matrix for the current Fibril implementation.
---

Fibril is pre-alpha infrastructure. This table distinguishes the working baseline from active design and wiring work.

| Feature | Status | Notes |
| --- | --- | --- |
| Durable queues | Available | Append-only message and event logs, snapshot, and replay |
| Publish and subscribe | Available | Custom TCP protocol, Rust client, and TypeScript client |
| Explicit settlement | Available | ACK, fail, immediate retry, and delayed retry paths |
| Leasing | Available | Expired leases can return to ready |
| Backpressure | Available | Pull-based delivery and bounded prefetch |
| Delayed publish | Available | Broker path and Rust/TypeScript client methods are wired |
| Dead lettering | Available | Global and per-queue policy are configurable, replay tooling is still early |
| Sparse queues | Available | Lazy loading and idle eviction are wired, observability is still growing |
| Message inspection | Available | Browse active queue messages from admin tooling, with optional settled offsets and payload previews |
| Partition ownership | Planned | Future nodes can split active queue traffic by owning different partitions |
| Replication | Planned | Future followers can keep partition copies for failover and recovery |
| Transactions | Out of scope | Not planned; transactional publish/consume workflows are intentionally excluded |

## Early performance observations

Informal internal measurements on a Ryzen 5950X system have observed roughly `250k+` messages/sec ingress and egress with 1KB payloads on the durable path.

These numbers are architecture sanity checks, not a rigorous benchmark suite. Hardware, durability settings, batching, queue depth, workload, and storage behavior all matter.

## What can move now

The docs can describe current queue semantics, delayed publish and retry, configurable DLQ behavior, deployment shape, sparse-queue behavior, and early benchmarks.

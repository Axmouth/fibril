---
title: Project status
description: A candid feature matrix for the current Fibril implementation.
---

Fibril is pre-alpha infrastructure. This table distinguishes the working baseline from active design and wiring work.

For a more detailed checklist of what is wired and what conditions apply, see
[implemented surface](/latest/implemented-surface/).

| Feature | Status | Notes |
| --- | --- | --- |
| Durable queues | Available | Durable message storage, queue state, snapshots, and replay |
| Publish and subscribe | Available | Custom TCP protocol, Rust client, and TypeScript client |
| Explicit settlement | Available | ACK, fail, immediate retry, and delayed retry paths |
| Leasing | Available | Expired leases can return to ready |
| Backpressure | Available | Pull-based delivery and bounded prefetch |
| Delayed publish | Available | Broker path and Rust/TypeScript client methods are wired |
| Dead lettering | Available | Global and per-queue policy are configurable, replay tooling is still early |
| Sparse queues | Available | Lazy loading and idle cleanup are wired, observability is still growing |
| Message inspection | Available | Browse active queue messages from admin tooling, with optional settled offsets and payload previews |
| Partitioned queues | Available | Declared queues can have multiple partitions, with client-side key routing and transparent fan-in |
| Partition ownership | Experimental | Embedded coordination can assign queue ownership, fence stale owners, and redirect clients to the current owner |
| Replication | Experimental | Follower pull replication, failover promotion, in-sync checks, and replica-durable confirms are wired on this branch |
| Live repartitioning | Experimental | Grow or shrink a queue's partition count in coordinated mode, from the admin topology page |
| Recovery quarantine | Available | A damaged queue log is detected on recovery and isolated per the `recovery.on_mismatch` policy, with operator repair |
| Exclusive consumer groups | Partial | Rust client opt-in for one active consumer per partition, with sticky assignment and cross-broker coordinator wiring |
| Transactions | Out of scope | Not planned. Transactional publish/consume workflows are intentionally excluded |

## Early performance observations

Informal internal measurements on a Ryzen 5950X system have observed roughly `250k+` messages/sec ingress and egress with 1KB payloads on the durable path.

These numbers are architecture sanity checks, not a rigorous benchmark suite. Hardware, durability settings, batching, queue depth, workload, and storage behavior all matter.

## What can move now

The docs can describe current queue semantics, delayed publish and retry,
configurable DLQ behavior, deployment shape, sparse-queue behavior, partitioned
queues, exclusive consumer groups, and early benchmarks.

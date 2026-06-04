---
title: Project status
description: A candid feature matrix for the current Fibril implementation.
---

Fibril is pre-alpha infrastructure. This table distinguishes the working baseline from active design and wiring work.

| Feature | Status | Notes |
| --- | --- | --- |
| Durable queues | Tested baseline | Append-only message and event logs, snapshot, and replay |
| Publish and subscribe | Available | Custom TCP protocol, Rust client, and TypeScript client |
| Explicit acknowledgements | Available | ACK, immediate requeue, terminal failure |
| Leasing | Available | Expired leases can return to ready |
| Backpressure | Available | Pull-based delivery and bounded prefetch |
| Delayed publish | Available | Broker/Stroma support exists; Rust and TypeScript clients expose delayed publish methods |
| Delayed retry | Partial | Stroma state supports delayed retry; public client/broker path is not wired end to end |
| Dead lettering | Partial | Stroma has custom/discard DLQ policy and tests; public broker configuration is not exposed yet |
| TypeScript client | Early | Lives under `clients/typescript` |
| Replication | Planned | Design work in progress |
| Clustering | Planned | Not implemented |
| Transactions | Out of scope | Not planned; transactional publish/consume workflows are intentionally excluded |

## Early performance observations

Informal internal measurements on a Ryzen 5950X system have observed roughly `250k+` messages/sec ingress and egress with 1KB payloads on the durable path.

These numbers are architecture sanity checks, not a rigorous benchmark suite. Hardware, durability settings, batching, queue depth, workload, and storage behavior all matter.

## What can move now

The docs can describe the current state-layer semantics, delayed publish, deployment shape, and early benchmarks. Public `msg.retry_after(..)` and configurable DLQ behavior should wait for broker/client wiring before the site presents them as supported user-facing behavior.

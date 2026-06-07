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
| Delayed retry | Available | Broker/protocol/Rust client path is wired and tested; TypeScript helper parity is pending |
| Dead lettering | Partial | Global target is admin/API configurable; per-queue policy is client/API configurable; replay tooling is still early |
| TypeScript client | Early | Lives under `clients/typescript` |
| Replication | Planned | Design work in progress |
| Clustering | Planned | Not implemented |
| Transactions | Out of scope | Not planned; transactional publish/consume workflows are intentionally excluded |

## Early performance observations

Informal internal measurements on a Ryzen 5950X system have observed roughly `250k+` messages/sec ingress and egress with 1KB payloads on the durable path.

These numbers are architecture sanity checks, not a rigorous benchmark suite. Hardware, durability settings, batching, queue depth, workload, and storage behavior all matter.

## What can move now

The docs can describe current queue semantics, delayed publish and retry, configurable DLQ behavior, deployment shape, and early benchmarks. TypeScript client parity for delayed retry is still a short follow-up.

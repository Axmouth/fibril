---
title: Project status
description: A candid feature matrix for the current Fibril implementation.
---

Fibril is pre-alpha infrastructure. This table distinguishes the working baseline from active design and wiring work.

For a more detailed checklist of what is wired and what conditions apply, see
[implemented surface](/implemented-surface/).

| Feature | Status | Notes |
| --- | --- | --- |
| Durable queues | Available | Durable message storage, queue state, snapshots, and replay |
| Publish and subscribe | Available | Custom TCP protocol with Rust, TypeScript, and Python clients |
| Explicit settlement | Available | ACK, fail, immediate retry, and delayed retry paths |
| Leasing | Available | Expired leases can return to ready |
| Backpressure | Available | Pull-based delivery and bounded prefetch |
| Delayed publish | Available | Broker path and Rust, TypeScript, and Python client methods are wired |
| Message TTL | Available | Per-message or per-queue default; expired messages drop or dead-letter. Not queue expiration |
| Dead lettering | Available | Global and per-queue policy are configurable, replay tooling is still early |
| Sparse queues | Available | Lazy loading and idle cleanup are wired, observability is still growing |
| Message inspection | Available | Browse active queue messages from admin tooling, with optional settled offsets and payload previews |
| Partitioned queues | Available | Declared queues can have multiple partitions, with client-side key routing and transparent fan-in |
| Plexus streams | Available | Fan-out channel type: every subscriber receives every record, durable named cursors, per-stream durability tiers, partitioning with client-side fan-in, and header filters. Rust, TypeScript, and Python clients |
| Wildcard subscribe | Available | Opt-in `client.routing()` subscribes to every queue or stream whose topic matches a `*`-glob and auto-attaches channels that start matching later, driven by a live cluster catalogue. Client-side only. Rust, TypeScript, and Python clients |
| Partition ownership | Experimental | Embedded coordination can assign queue ownership, fence stale owners, and redirect clients to the current owner |
| Replication | Experimental | Follower pull replication, failover promotion, in-sync checks, and replica-durable confirms are wired on this branch |
| Stream replication | Experimental | Durable-tier streams replicate record and cursor logs to followers with replica-durable confirms and caught-up failover, placed and owned through embedded coordination. Express tiers stay owner-only |
| Live repartitioning | Experimental | Grow or shrink a queue's partition count in coordinated mode, from the admin topology page |
| Recovery quarantine | Available | A damaged queue log is detected on recovery and isolated per the `recovery.on_mismatch` policy, with operator repair |
| TLS in transit | Available | The broker listener serves TLS from operator PEMs or generated per-deployment material, mismatches are named in both directions, all three clients connect with CA-file, fingerprint-pin, or OS-roots trust, the dashboard serves HTTPS from the same material, and first-boot setup mode offers generate/supply/skip before the broker ever serves. Inter-broker replication and coordination traffic is encrypted too (`tls.inter_broker`, shared-CA lane for generated material), and the serving certificate rotates live via `fibrilctl admin reload-tls`. mTLS client auth is planned |
| Broker authentication | Available | Argon2 user store seeded from config, managed from the dashboard and `fibrilctl`, replicated across the cluster. Built-in `fibril`/`fibril` credentials work from loopback only. Node-to-node connections authenticate with a cluster shared secret, never a user account |
| Prometheus metrics | Available | `GET /metrics` on the admin listener behind the same auth and HTTPS as the dashboard: node-level aggregates always, per-channel series from materialized channels gated by `admin.metrics_per_channel` |
| Exclusive consumer groups | Partial | Rust, TypeScript, and Python client opt-in for one active consumer per partition, with sticky assignment and cross-broker coordinator wiring |
| Transactions | Out of scope | Not planned. Transactional publish/consume workflows are intentionally excluded |

## Early performance observations

Informal internal measurements on a Ryzen 5950X system have observed roughly `250k+` messages/sec ingress and egress with 1KB payloads on the durable path. Plexus stream fan-out reaches roughly `1.5M` delivered records/sec across sixteen readers on a single partition, and one partition fans out to hundreds of readers when delivery throughput is not the bottleneck.

These numbers are architecture sanity checks, not a rigorous benchmark suite. Hardware, durability settings, batching, queue depth, workload, and storage behavior all matter. See [benchmarks](/benchmarks/) for the fuller queue and stream tables.

## What can move now

The docs can describe current queue semantics, delayed publish and retry,
configurable DLQ behavior, deployment shape, sparse-queue behavior, partitioned
queues, exclusive consumer groups, Plexus streams with their durability tiers,
and early benchmarks.

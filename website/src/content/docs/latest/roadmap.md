---
title: Roadmap
description: Near-term work for Fibril.
---

Fibril is still early. The near-term roadmap is about making the existing semantics complete and user-facing before adding larger distributed-system features.

## Near term

- Wire `msg.retry_after(..)` through client, protocol, broker, and Stroma.
- Add CLI helpers and broader tests for dead-letter policy workflows.
- Add broker-level tests for delayed retry public paths.
- Make server configuration production-friendly instead of hard-coded development defaults.
- Keep improving memory behavior for sparse workloads with many rarely used queues.
- Improve TCP protocol ergonomics and error behavior.

## Medium term

- Package runnable broker images or binaries.
- Add deployment guidance for the broker itself.
- Improve admin interface observability.
- Produce repeatable benchmark reports.
- Tighten memory behavior under large queue depth and high inflight load.

## Longer term

- Replication design and implementation.
- Clustering and partition ownership.
- More complete client ecosystem.

## Out of scope

Transactions are not planned. Fibril is aiming for clear durable messaging semantics, not a clone of every broker feature, and transactional publish/consume workflows are intentionally out of scope.

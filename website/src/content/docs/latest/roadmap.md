---
title: Roadmap
description: Near-term work for Fibril.
---

Fibril is still early. The near-term roadmap is about making the existing semantics complete and user-facing before adding larger distributed-system features.

## Near term

- Add richer DLQ replay and message inspection workflows.
- Add the next storage-level runtime settings.
- Add sparse-queue observability around lazy loading, idle cleanup, and eviction decisions.
- Improve TCP protocol ergonomics and error behavior.

## Medium term

- Package runnable broker images or binaries, including `fibrilctl` in the server image.
- Add deployment guidance for the broker itself.
- Improve admin interface observability.
- Produce repeatable benchmark reports.
- Tighten memory behavior under large queue depth, high inflight load, and many idle queues.

## Longer term

- Replication design and implementation.
- Clustering and partition ownership.
- More complete client ecosystem.

## Out of scope

Transactions are not planned. Fibril is aiming for clear durable messaging semantics, not a clone of every broker feature, and transactional publish/consume workflows are intentionally out of scope.

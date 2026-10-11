---
title: Partition routing
description: Development policy for who chooses a queue partition and where partition indexes appear.
---

This is a development note. User-facing docs should describe queues by topic and optional group, plus a partition count and partition keys where ordering or scale matters.

## Policy

Partition selection is owned by Fibril and its client libraries, not by normal users.

Queues can have several partitions. Operators choose a partition count when creating a queue or repartitioning it, and producers may supply a partition key, but they do not pick a partition index for normal publishing.

The client library chooses the partition from the partition key, or round-robin for keyless publishes, and the broker fences that choice with the queue's partitioning version. Dead-letter routing follows the same rule: users choose the DLQ topic and optional group, and Fibril dead-letters into the same partition index as the source.

## Why

Asking operators to pick partition indexes creates a misleading contract. It suggests they are responsible for a routing decision that depends on placement and repartitioning, which the system already handles.

Keeping indexes internal lets ownership, placement and repartitioning evolve without changing the normal user model.

## Implementation Guidance

- Admin UI and client docs expose partition count and partition keys, and show partition indexes only for inspection and recovery.
- Admin API request bodies can omit partition where possible.
- API responses may include partition where storage keys or recovery operations need it.
- Tests can use partition `0` when exercising single-partition behavior, but should not describe it as an operator choice.
- If a future feature needs explicit partition targeting, document it as a specialized routing mode rather than the default queue model.

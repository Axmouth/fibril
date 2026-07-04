---
title: Partition routing
description: Development policy for keeping partition selection internal until
  sharding is designed.
slug: 0.4/development/partition-routing
---

This is a development note. User-facing docs should describe queues by topic and optional group unless partition ownership becomes a real operator concern.

## Policy

Partition selection should be owned by Fibril/Stroma, not by normal users.

For current single-partition behavior, APIs may still carry an internal `part` field because storage keys are shaped as topic, partition, and optional group. User-facing forms and examples should not ask operators to choose it.

When queue sharding lands, routing should choose an available partition according to broker/storage policy. Dead-letter routing should follow the same rule: users choose the DLQ topic and optional group, and Fibril chooses the partition.

## Why

Exposing partition too early creates a misleading contract. It suggests operators are responsible for a routing decision that the system cannot yet make meaningful, and it would make later sharding harder to explain.

Keeping partition internal lets the implementation evolve from today's default partition to future ownership and load-placement rules without changing the normal user model.

## Implementation Guidance

* Admin UI and client docs should avoid partition fields.
* Admin API request bodies can omit partition where possible.
* API responses may include partition while internal storage keys still expose it.
* Tests can use partition `0` when exercising current storage behavior, but should not describe it as an operator choice.
* If a future feature needs explicit partition targeting, document it as a specialized routing mode rather than the default queue model.

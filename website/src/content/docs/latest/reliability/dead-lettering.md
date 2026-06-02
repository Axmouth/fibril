---
title: Dead lettering
description: How Fibril's dead-letter behavior is modeled today.
---

Dead lettering exists in the Stroma state layer, but it is not yet exposed as a polished public broker configuration surface.

## Current state-layer behavior

Stroma can model:

- retry counts per offset
- max-retry exhaustion
- pending dead-letter state
- discard policy
- custom DLQ target topic, partition, and optional group
- durable snapshot and replay of DLQ policy and pending DLQ state

When a message reaches the retry limit, Stroma moves it to pending DLQ rather than immediately pretending it is settled. A second phase commits the DLQ action or discards the pending entry according to policy.

## Custom target

The state layer can route a dead-lettered message to a custom target. DLQ copies include source metadata such as source topic and source offset in message headers.

## Discard policy

For discard policy, a rejected or exhausted message is locally settled without writing a DLQ message.

## Public status

The important caveat: this behavior is not yet a stable public Fibril feature.

Before the site should call dead lettering “available,” the broker needs:

- user-facing DLQ declaration or configuration
- clear retry-limit configuration
- protocol/client support where needed
- broker-level integration tests that exercise the public path

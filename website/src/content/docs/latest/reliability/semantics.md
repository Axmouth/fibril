---
title: Reliability semantics
description: The current durability and delivery guarantees in Fibril.
---

Fibril currently targets best-effort at-least-once delivery.

## Durability

Queue state is backed by append-only logs through Keratin:

- message log
- event and state log
- snapshot and replay

Commands are designed to be idempotent and replay-safe.

## Acknowledgements

Acknowledgements are explicit, idempotent, and final. A bounded ACK window tracks out-of-order acknowledgements near the monotonic `settled_until` frontier without requiring unbounded memory.

If a subscription is dropped with prefetched but unacknowledged messages, those messages are returned for redelivery instead of being left behind until lease expiry. This keeps unsubscribe and consumer shutdown behavior aligned with at-least-once delivery.

Short socket breaks can be handled with [reconnect grace](/latest/reliability/reconnects/) when it is configured and the client reconnects with a valid resume identity before the grace window expires.

## Retries

Immediate requeue is implemented. Delayed retry can hold a nacked message until a `not_before` deadline before making it ready again. Lease expiry can also move inflight messages back to ready.

Delayed retries are wired through the broker/protocol/client path. Max-retry dead-letter routing is also wired, but replay and inspection tooling is still early.

## Backpressure

Delivery is pull-based. Subscriptions set bounded prefetch, so consumers do not accept unlimited inflight work.

## Not a production claim

Durability is a real part of the broker and is tested. The project is still pre-alpha, and production readiness is not claimed.

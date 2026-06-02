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

## Retries

Immediate requeue is implemented. Lease expiry can also move inflight messages back to ready.

Delayed retries and dead lettering are still evolving:

- `retry_after(..)` exists in the Rust client but is not implemented end to end.
- basic max-retry dead-letter behavior exists below the public surface but is incomplete.

## Backpressure

Delivery is pull-based. Subscriptions set bounded prefetch, so consumers do not accept unlimited inflight work.

## Not a production claim

Durability is a real part of the broker and is tested. The project is still pre-alpha, and production readiness is not claimed.

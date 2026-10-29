---
title: Reliability semantics
description: The current durability and delivery guarantees in Fibril.
---

Fibril currently targets best-effort at-least-once delivery.

## Durability

Fibril stores message payloads and queue state in an append-only log, and
recovers from snapshots plus replay of the log after them. Commands are
designed to be idempotent and replay-safe.

When a publish is confirmed depends on the queue's mode and the cluster's
assignment durability:

- **Durable queues** (the default) confirm and deliver a message only after
  every required copy has synced it to disk. A confirmed message survives a
  crash or power loss of those nodes.
- **Buffered queues** confirm and deliver once every required copy has written
  the message to the operating system, and a background sync makes it durable
  shortly after. A power loss on every required node at once can lose confirmed
  messages from that window. See [buffered queues](/reliability/buffered-queues/).
- **Required copies.** A standalone broker has one copy. Clusters default to
  `majority_durable`, so a queue with three copies confirms once two have the
  message, and survives losing one node without losing confirmed messages. See
  [replication](/reliability/replication/).

Streams have their own durability tiers, described in
[Plexus streams](/concepts/plexus-streams/#durability-tiers).

## Acknowledgements

Acknowledgements are explicit, idempotent, and final. Settlements are tracked as a coalesced set of settled offset ranges, from which the monotonic `settled_until` frontier is derived, so out-of-order acknowledgements are handled without a fixed window.

If a subscription is dropped with prefetched but unacknowledged messages, those messages are returned for redelivery instead of being left behind until lease expiry. This keeps unsubscribe and consumer shutdown behavior aligned with at-least-once delivery.

Short socket breaks can be handled with [reconnect grace](/reliability/reconnects/) when it is configured and the client reconnects with a valid resume identity before the grace window expires.

## Retries

Immediate requeue is implemented. Delayed retry can hold a nacked message until a `not_before` deadline before making it ready again. Lease expiry can also move inflight messages back to ready.

Delayed retries are wired through the broker/protocol/client path. Max-retry dead-letter routing is also wired, but replay and inspection tooling is still early.

## Backpressure

Delivery is pull-based. Subscriptions set bounded prefetch, so consumers do not accept unlimited inflight work.

## Not a production claim

Durability is a real part of the broker and is tested. The project is still pre-alpha, and production readiness is not claimed.

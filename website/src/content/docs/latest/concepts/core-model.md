---
title: Core model
description: How messages move through a Fibril queue.
---

Fibril uses a strict queue state machine. Each offset exists in at most one of three states:

```txt
Ready -> Inflight -> Acked
  ^         |
  +---------+
   retry or expired lease
```

## Ready

A ready message is eligible for delivery. Consumers pull work rather than receiving an unbounded push stream.

## Inflight

Polling leases a message to a consumer until a deadline. If the consumer does not settle the message before expiry, the broker can return it to `Ready`.

## Acked

Acknowledgement is final. An acked offset does not re-enter delivery.

## Explicit settlement

Manual-ack consumers decide what happens after processing:

```rust
msg.complete().await?; // terminal success
msg.retry().await?;    // requeue immediately
msg.fail().await?;     // terminal failure
```

`retry_after(..)` is represented in the client API but is not wired through the full path yet.

## Why leasing

Leasing makes failure recovery explicit. A consumer can disappear without permanently taking messages with it, and bounded prefetch gives the broker a concrete backpressure mechanism.

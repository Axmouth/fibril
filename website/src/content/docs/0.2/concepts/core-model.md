---
title: Core model
description: How messages move through a Fibril queue.
slug: 0.2/concepts/core-model
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
msg.retry_after(30).await?; // requeue after a delay
msg.fail().await?;     // terminal failure
```

Delayed retry keeps the message out of ready delivery until its retry deadline.

## Why leasing

Leasing makes failure recovery explicit. A consumer can disappear without permanently taking messages with it, and bounded prefetch gives the broker a concrete backpressure mechanism.

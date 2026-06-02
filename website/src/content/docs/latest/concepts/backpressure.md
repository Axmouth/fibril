---
title: Backpressure
description: Pull-based delivery and prefetch limits in Fibril.
---

Fibril’s current backpressure model is intentionally plain: consumers pull work and each subscription has a prefetch limit.

## Prefetch

Prefetch bounds the number of messages a consumer may have inflight at once.

```rust
let mut sub = client
    .subscribe("email.send")
    .prefetch(32)
    .sub_manual_ack()
    .await?;
```

The default client subscription prefetch is small. The broker also clamps subscription prefetch to at least one.

## Inflight slots

Each delivered message occupies an inflight slot. A slot is released when the message is durably settled, or eventually by lease expiry if the consumer disappears.

This keeps slow consumers from accepting unlimited work and gives the broker a concrete signal for who can receive more messages.

## What this is not

This is not a full adaptive flow-control system yet. There is no production tuning guide, no automatic per-topic policy, and no cluster-level balancing.

The current model is useful because it is simple, explicit, and easy to reason about under failure.

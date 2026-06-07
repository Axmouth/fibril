---
title: Retries and delays
description: Current retry, lease-expiry, and delayed-delivery behavior in Fibril.
---

Fibril separates three related behaviors:

- immediate retry after a consumer rejects work
- redelivery after a lease expires
- delayed delivery or delayed retry after a specific timestamp

## Immediate retry

Manual-ack consumers can currently request immediate requeue through the public Rust and TypeScript clients:

```rust
msg.retry().await?;
```

```ts
await msg.retry();
```

At the state layer, this removes the offset from inflight, increments the retry count, and returns the offset to ready unless the retry policy is exhausted.

## Lease expiry

When a message is delivered, it becomes inflight with a lease deadline. If the consumer disappears or does not settle the message, the broker expiry worker asks Stroma to collect expired inflight offsets and returns them to ready.

This is the core failure-recovery path for best-effort at-least-once delivery.

## Delayed publish

Stroma has durable delayed-enqueue state. Offsets can be held until `not_before`, and delayed enqueue state is included in snapshots.

The Rust and TypeScript clients expose delayed publish methods:

```rust
publisher.publish_delayed(payload, delay).await?;
publisher.publish_delayed_confirmed(payload, delay).await?;
```

```ts
await publisher.publishDelayed(payload, 30_000);
await publisher.publishDelayedConfirmed(payload, 30_000);
```

Numeric TypeScript delays are milliseconds. Passing a `Date` uses that absolute Unix-millisecond deadline.

The delayed publish path uses a distinct protocol frame instead of adding an optional delay field to the common publish frame.

## Delayed retry

The Stroma event model includes delayed retry forms such as `RetryLater` and `RequeueLater`, and queue state tracks delayed retry deadlines.

The public consumer API already points in the intended direction:

```rust
msg.retry_after(30).await?;
```

That method is currently not implemented end to end in the Rust client. The site should keep saying “planned/partial” until this path is wired through client, protocol, broker, state, and tests.

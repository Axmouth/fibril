---
title: Retries and delays
description: Current retry, lease-expiry, and delayed-delivery behavior in Fibril.
slug: 0.2/reliability/retries-delays
---

Fibril separates three related behaviors:

* immediate retry after a consumer rejects work
* redelivery after a lease expires
* delayed delivery or delayed retry after a specific timestamp

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

When a message is delivered, it becomes inflight with a lease deadline. If the consumer disappears or does not settle the message, the broker checks for expired inflight messages and returns them to ready.

This is the core failure-recovery path for best-effort at-least-once delivery.

## Delayed publish

Fibril persists delayed-delivery state. Messages can be held until `not_before`, and delayed-delivery state is included in recovery snapshots.

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

Manual-ack consumers can ask the broker to retry a message after a delay. Fibril records a `not_before` Unix-millisecond deadline on the settlement event, keeps the offset out of ready delivery until that deadline, and then makes it eligible for redelivery.

Rust client numeric delays are seconds. Use `std::time::Duration` when the unit should be explicit:

```rust
msg.retry_after(30).await?;
msg.retry_after(std::time::Duration::from_millis(250)).await?;
```

TypeScript numeric delays are milliseconds. Passing a `Date` uses that absolute retry deadline:

```ts
await msg.retryAfter(30_000);
await msg.retryAfter(new Date(Date.now() + 30_000));
```

## Message TTL (drop by age)

A message can be dropped if it is not consumed before a deadline. This is the
work-queue "do not process stale work" behavior, and it is distinct from queue
expiration (auto-deleting an idle queue), which is not implemented.

Set a TTL per message, or a per-queue default that applies when a message
carries no TTL of its own. A per-message TTL wins over the queue default; with
neither set a message never expires. The owner resolves the deadline against its
own clock at publish, so it survives recovery and replication.

An expired message is **never** dropped while it is in flight. When it does
drop, it follows the queue's dead-letter policy: discarded when no DLQ is
configured, otherwise dead-lettered with reason `expired`.

Per-message TTL via an "expiring" publisher (Rust numeric = seconds, or a
`Duration`; TypeScript = milliseconds):

```rust
let publisher = client.publisher("rpc.reply")?.expiring(30);
publisher.publish(reply).await?;
```

```ts
const publisher = client.publisher("rpc.reply").expiring(30_000);
await publisher.publish(reply);
```

Per-queue default TTL at declare time:

```rust
client
    .declare_queue(QueueConfig::new("rpc.reply")?.default_message_ttl(30))
    .await?;
```

```ts
await client.declareQueue(
  new QueueConfig("rpc.reply").defaultMessageTtl(30_000),
);
```

The broker resolves the deadline from `Publish.ttl_ms` (or the queue's
`default_message_ttl_ms`) and the expiry worker drops expired ready messages on
its normal tick.

---
title: Dead Lettering
description: Configure where failed messages should go.
---

Dead lettering gives failed messages somewhere explicit to go after retry handling is exhausted. It is useful when you want to inspect, replay, or separately process messages that could not be handled normally.

## What You Can Configure

Fibril currently exposes two dead-lettering controls:

- a global dead-letter queue target, configured by operators through the admin UI/API
- a per-queue policy for what should happen after retries are exhausted, declared by applications through the client, `fibrilctl`, or the admin API

The global target is the fallback destination for queues configured to use the global DLQ policy. It is a live storage-owned runtime setting: it is persisted, survives restart, and is not controlled by the startup TOML/env/CLI config.

Per-queue policy can:

- discard failed messages
- send failed messages to the global DLQ target
- send failed messages to a custom queue-specific DLQ target

You can also set the queue retry limit when declaring the queue policy. Leave it blank in the admin UI when you only want to change the DLQ routing policy and keep the queue's current retry setting.

## Client Setup

Applications can declare retry and dead-letter policy when they start. This is usually the clearest place to put per-queue behavior, because the code that publishes or consumes a queue also documents what should happen when processing keeps failing.

Rust:

```rust
use fibril_client::QueueConfig;

client
    .declare_queue(
        QueueConfig::new("orders.created")?
            .group("workers")?
            .use_global_dead_letter_queue()
            .max_retries(3),
    )
    .await?;
```

TypeScript:

```ts
import { QueueConfig } from "@fibril/client";

await client.declareQueue(
  new QueueConfig("orders.created")
    .group("workers")
    .useGlobalDeadLetterQueue()
    .maxRetries(3),
);
```

CLI:

```sh
fibrilctl queue declare orders.created \
  --group workers \
  --dlq global \
  --max-retries 3
```

For a queue-specific target:

```sh
fibrilctl queue declare orders.created \
  --dlq custom \
  --dlq-topic _dlq.orders \
  --max-retries 3
```

## Admin API

Global target:

```http
GET /admin/api/global-dlq
PUT /admin/api/global-dlq
```

`GET` returns the current version and target:

```json
{
  "version": 1,
  "target": {
    "tp": "_dlq.orders",
    "part": 0,
    "group": null
  }
}
```

`PUT` updates the target. Include the version you last read as `expected_version`:

```json
{
  "expected_version": 1,
  "target": {
    "tp": "_dlq.orders",
    "group": null
  }
}
```

If another operator changed the setting first, Fibril returns `409 Conflict`
with the current setting. Set `target` to `null` to clear the global target.

Queue policy:

```http
PUT /admin/api/queue-dlq
```

Use the global DLQ target for a queue:

```json
{
  "tp": "orders.created",
  "group": null,
  "policy": "global",
  "target": null,
  "max_retries": 3
}
```

Use a queue-specific DLQ target:

```json
{
  "tp": "orders.created",
  "group": null,
  "policy": "custom",
  "target": {
    "tp": "_dlq.orders",
    "group": null
  },
  "max_retries": 3
}
```

Discard after retries are exhausted:

```json
{
  "tp": "orders.created",
  "group": null,
  "policy": "discard",
  "target": null,
  "max_retries": 3
}
```

## Target Fields

| Field | Meaning |
| --- | --- |
| `tp` | Topic to write dead-lettered messages to. |
| `group` | Optional group. Use `null` for an ungrouped target. |

Topics and groups use the same validation rules as normal Fibril topics and groups.

Partitioning is currently an internal detail for dead-letter configuration. Admin API responses may include `part`, but request bodies can omit it. Once queue sharding is available, Fibril should choose an available partition for DLQ routing rather than asking operators to pick one here. See [partition routing](/latest/development/partition-routing/) for the development policy.

## Metadata on DLQ Messages

When Fibril copies a message into a DLQ target, it preserves the original user
headers and adds storage-owned metadata under the reserved `stroma.dlq.*`
namespace:

| Header | Meaning |
| --- | --- |
| `stroma.dlq.source_topic` | Source topic the message came from. |
| `stroma.dlq.source_group` | Source group, only present when the source queue has a group. |
| `stroma.dlq.source_offset` | Source offset. |
| `stroma.dlq.retry_count` | Retry count when the message was dead-lettered. |
| `stroma.dlq.reason` | Why the message was dead-lettered, such as `retries_exhausted`. |
| `stroma.dlq.dead_lettered_at_ms` | Broker/storage timestamp in Unix milliseconds. |

Application code should not publish headers starting with `fibril.` or
`stroma.`. Those prefixes are reserved for system metadata and are rejected on
publish.

## Current Limits

Dead-letter routing is usable, but some surrounding workflow is still intentionally small.

Still in progress:

- replay tooling and message inspection for DLQ queues
- richer end-to-end coverage around replay and inspection workflows

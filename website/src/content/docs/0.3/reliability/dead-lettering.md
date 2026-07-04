---
title: Dead Lettering
description: Configure where failed messages should go.
slug: 0.3/reliability/dead-lettering
---

Dead lettering gives failed messages somewhere explicit to go after retry handling is exhausted. It is useful when you want to inspect, replay, or separately process messages that could not be handled normally.

## What You Can Configure

Fibril currently exposes two dead-lettering controls:

* a global dead-letter queue target, configured by operators through the admin UI/API or `fibrilctl`
* a per-queue policy for what should happen after retries are exhausted, declared by applications through the client, `fibrilctl`, or the admin API

The global target is the fallback destination for queues configured to use the global DLQ policy. It is a live storage-owned runtime setting: it is persisted, survives restart, and is not controlled by the startup TOML/env/CLI config.

Per-queue policy can:

* discard failed messages
* send failed messages to the global DLQ target
* send failed messages to a custom queue-specific DLQ target

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

Operators can also manage the global DLQ target through the CLI:

```sh
fibrilctl admin global-dlq get
fibrilctl admin global-dlq set _dlq.orders
fibrilctl admin global-dlq clear
```

To inspect active messages in a queue:

```sh
fibrilctl admin messages orders.created
```

By default this only returns messages that are still active in queue state, such
as ready, inflight, delayed, or pending DLQ messages. Add
`--include-settled` when you also need persisted log records that are no longer
tracked as active. Add `--include-payload` to include base64 payload previews.
The admin dashboard exposes the same inspection path from the queues page. It
starts near the queue's settled offset by default and lets you page backward or
forward by offset.

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

Message inspection:

```http
GET /admin/api/messages?topic=orders.created&from=0&limit=50
```

Useful query flags:

* `include_settled=true` also returns persisted records that are no longer active
* `include_payload=true` includes base64 payload previews
* `payload_limit_bytes=4096` caps each payload preview
* `status=ready,pending_dlq` filters returned rows by status

Message inspection reads persisted message data and queue state. Use it for
debugging and operations, not as a live polling view.

The default page size is `50`. The current hard cap is `5000` messages per
request. Payload previews default to `4096` bytes per message and are capped at
`1048576` bytes. The dashboard asks for confirmation when you go beyond the
normal lightweight range, because large pages or large payload previews can
touch a lot of persisted data.

Replay selected active DLQ offsets back to their recorded source queue:

```http
POST /admin/api/dlq/replay
```

```json
{
  "dlq_topic": "_dlq.orders",
  "dlq_group": null,
  "offsets": [0, 3, 8]
}
```

The replay operation copies the stored payload and user headers back to the
source topic recorded in `stroma.dlq.source_topic` and
`stroma.dlq.source_group`. It strips system metadata from the replayed copy and
does not remove or acknowledge the DLQ message.

Replay is intentionally explicit. In the dashboard, select specific DLQ offsets
from message inspection and confirm the replay. The result reports each offset
as replayed or skipped with the target queue when available.

The same operation is available from the CLI:

```sh
fibrilctl admin dlq replay _dlq.orders --offset 0 --offset 3
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

Partitioning is currently an internal detail for dead-letter configuration. Admin API responses may include `part`, but request bodies can omit it. Once queue sharding is available, Fibril should choose an available partition for DLQ routing rather than asking operators to pick one here. See [partition routing](/0.3/development/partition-routing/) for the development policy.

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

* richer replay workflows, such as bulk replay filters and explicit delete/ack controls
* broader end-to-end coverage around replay and inspection workflows

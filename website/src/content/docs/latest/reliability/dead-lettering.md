---
title: Dead Lettering
description: Configure where failed messages should go.
---

Dead lettering gives failed messages somewhere explicit to go after retry handling is exhausted. It is useful when you want to inspect, replay, or separately process messages that could not be handled normally.

## What You Can Configure

Fibril currently exposes two dead-lettering controls through the admin UI and admin API:

- a global dead-letter queue target
- a per-queue policy for what should happen after retries are exhausted

The global target is the fallback destination for queues configured to use the global DLQ policy. It is a live storage-owned runtime setting: it is persisted, survives restart, and is not controlled by the startup TOML/env/CLI config.

Per-queue policy can:

- discard failed messages
- send failed messages to the global DLQ target
- send failed messages to a custom queue-specific DLQ target

You can also set the queue retry limit when declaring the queue policy. Leave it blank in the admin UI when you only want to change the DLQ routing policy and keep the queue's current retry setting.

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

## Current Limits

Dead-letter routing is usable, but some surrounding workflow is still intentionally small.

Still in progress:

- friendlier client and CLI helpers for declaring DLQ policy
- replay tooling and message inspection for DLQ queues
- stable system metadata for recording where a dead-lettered message came from
- broader end-to-end coverage around the full public workflow

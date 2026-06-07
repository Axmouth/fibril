---
title: Dead Lettering
description: Configure where failed messages should go.
---

Dead lettering gives failed messages somewhere explicit to go after retry handling is exhausted. It is useful when you want to inspect, replay, or separately process messages that could not be handled normally.

## What You Can Configure

Fibril currently exposes a global dead-letter queue target through the admin API. The global target is the default destination for queues configured to use the global DLQ policy.

The global target is persisted and survives restart.

## Admin API

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
    "part": 0,
    "group": null
  }
}
```

If another operator changed the setting first, Fibril returns `409 Conflict` with the current setting. Set `target` to `null` to clear the global target.

## Target Fields

| Field | Meaning |
| --- | --- |
| `tp` | Topic to write dead-lettered messages to. |
| `part` | Partition number. |
| `group` | Optional group. Use `null` for an ungrouped target. |

Topics and groups use the same validation rules as normal Fibril topics and groups.

## Current Limits

The global DLQ target is configurable, but queue-level DLQ policy is not yet a complete user-facing feature.

Still in progress:

- queue-level DLQ declaration or configuration
- clear retry-limit configuration
- protocol/client support where needed
- broader broker integration tests for the full public workflow

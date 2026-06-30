---
title: Many idle queues
description: Running sparse workloads where many durable queues exist but only a few are active at once.
---

Some systems naturally create a lot of queues: per-customer work streams, per-tenant routing, scheduled jobs, rarely used webhooks, or low-traffic integrations.

The operational goal is simple: it should be reasonable to keep those queues durable without paying full memory cost for every queue all the time.

## What This Solves

Without lazy loading and idle cleanup, every queue that has ever been touched can keep runtime state alive until the broker restarts. That is wasteful when most queues are quiet most of the time.

Fibril addresses this in two steps:

- Queues are loaded lazily. Existing queues on disk do not need to be loaded into memory at startup.
- Queues can be unloaded from memory after they have been unused for long enough.

Unloading is not deletion. Messages, queue state, snapshots, and logs stay on disk.

## When a Queue Is in Memory

A queue is loaded into memory when the broker needs to operate on it. Common examples:

- a publisher writes to the queue
- a subscriber subscribes to the queue
- an admin or broker operation needs the queue state
- delayed or delivery work touches the queue

Publisher and subscriber creation load the queue before returning a usable handle. This means the admin queues page should not normally show active publishers or subscribers for a queue that is still only on disk.

A queue may not be in memory when:

- the broker has just started and the queue only exists on disk
- the queue was previously active, then became idle and was unloaded
- no current broker operation needs that queue

If the queue is used again, Fibril reloads it from durable state and continues from there.

## When a Queue Becomes Idle

A queue becomes eligible for idle cleanup only after all of these are true:

- no active subscribers remain for the queue
- no active publishers remain for the queue
- no messages are currently leased to consumers
- the configured idle window has elapsed

Subscribers stop keeping a queue active when they unsubscribe or their connection closes.

Publishers normally keep a queue active while the connection that used them is still open. If publisher idle expiry is enabled, an unused publisher can stop keeping the queue active before the connection closes.

The cleanup worker checks periodically. This means cleanup is intentionally approximate: a queue may unload on the first sweep after it qualifies, or on a later sweep.

After cleanup has already unloaded an idle queue, the worker does not repeatedly ask storage to unload the same queue again. It will consider that queue again after new activity makes it active and idle in a later cycle.

Admin message inspection can also load a queue because it reads queue state and
persisted message data. That does not create a publisher or subscriber lease. If
idle cleanup is enabled, the queue can be unloaded again after the idle window.

## What Happens to Messages

Ready messages remain durable while a queue is unloaded. If a subscriber comes back later, the queue is loaded again and those messages can be delivered.

Inflight messages block unloading. Fibril should not unload a queue while messages are already handed to consumers and waiting to be completed, retried, failed, or recovered.

When a subscription ends, prefetched but unsettled messages are returned for redelivery instead of being silently stranded behind the closed subscriber. This matters for sparse queues because it lets work move to another subscriber without waiting for the old prefetch window to drain naturally.

## Settings

Idle cleanup is controlled by runtime settings:

```toml
[runtime_seed.idle_queue_cleanup]
enabled = false
evict_after_ms = 600000
sweep_interval_ms = 60000
publisher_idle_timeout_ms = 600000
```

These values seed persisted runtime settings on first boot. After runtime settings exist, operators should edit them through the admin settings page or runtime settings API unless the setting group is locked by startup config.

For the full config reference, see [configuration](/configuration/).

## Operator Guidance

Use conservative idle windows in real deployments. Very small values are useful in tests, but they can make a busy sparse workload repeatedly unload and reload the same queues.

Use the admin queues page to distinguish loaded queues from queues that are only
indexed on disk. The same page shows active publisher/subscriber counts, idle
time for queues seen in the current process, and the last idle-cleanup result or
skip reason.

Enable publisher idle expiry when you have long-lived producer connections that only occasionally write to many different queues. Without it, a connection that has published to a queue can keep that queue active until the connection closes.

Treat idle cleanup as resource management, not as a correctness boundary. It
reduces memory use for quiet queues. It does not change retention,
acknowledgement semantics, retry policy, or dead-lettering.

The tradeoff is that the first operation on a cold queue may pay the cost of loading the queue back into memory.

## Current Scope

Available now:

- lazy loading for durable queues
- configurable idle queue unloading
- configurable publisher idle expiry
- live runtime updates through the admin settings page and API

Not covered by this feature:

- deleting queues
- expiring messages by age
- changing message retention
- dead-letter policy
- exact cleanup timing guarantees

For implementation details, see [idle queue internals](/development/idle-queue-internals/).

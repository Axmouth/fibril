---
title: Inactivity and eviction
description: How Fibril should treat unused queue resources.
---

Fibril queue data is durable. Resource eviction should only remove cached in-memory broker state, not message history, event logs, snapshots, or queue configuration.

The reason this matters is sparse queues. Some systems naturally create a large number of queues, but only a small fraction are active at once. Fibril should make that shape practical: storage aside, defining or touching many queues should not require keeping all of them fully live in memory forever.

Idle eviction pairs with lazy loading:

- lazy loading materializes a queue when publish, subscribe, admin inspection, expiry, or configuration work needs it
- idle eviction unmaterializes a queue after it has been inactive long enough
- the durable slot remains known, so the next operation can materialize it again

Together, those two behaviors make often-unused queues cheap without changing delivery semantics.

The intended behavior is:

- idle in-memory queue state can be dropped after a configurable inactivity window
- durable queue data stays on disk
- the next publish or subscribe for that queue recreates the in-memory state from storage
- queues with active consumers, active publishers, pending deliveries, pending settlements, ready messages, delayed messages, or inflight messages should not be evicted

This is a resource management feature, not a retention policy. Message TTL, dead lettering, and log retention are separate concerns.

## Current behavior

The current broker keeps queue loop state in memory once a queue has been touched. TCP publisher handles are cached for the lifetime of their connection, and subscriptions are removed when the connection closes or unsubscribes.

There is not yet a user-facing idle eviction setting. Treat queue inactivity eviction as planned behavior, not something to depend on in the current pre-alpha build.

## Planned accounting

The broker should track queue activity at the broker or queue-loop boundary:

- last publish, subscribe, delivery, settle, retry, expiry, or queue configuration change
- active subscription count
- active publisher count or active publisher-handle leases
- pending publish confirms and pending settlements
- whether storage reports ready, delayed, or inflight messages

A queue should only be an eviction candidate when it has been idle for the configured window and has no active work.

The broker-side sweep can also own publisher cache expiry. A sparse worker that wakes occasionally is enough; publisher and queue eviction do not need to run on every operation.

Storage should expose explicit materialization controls rather than making the broker guess:

```txt
materialize(queue)      # load or recover the queue handle
unmaterialize(queue)    # snapshot or drain as needed, then drop the handle
is_materialized(queue)  # observe current memory state
has_inflight(queue)     # guard broker-side eviction decisions
```

Eviction should use an atomic slot transition so a queue is not reopened while its old logs and background tasks are shutting down. During that window, new materialization should wait for eviction to finish or return a retryable error.

If eviction loses a race to new work, it should simply report that and leave the queue alone.

## Publishers

For now, connection-lifetime publisher caching is the safer default. It avoids explicit create/destroy publisher protocol messages that can desync if a client creates and drops publisher objects frequently.

If publisher resource pressure becomes meaningful, the next step should be server-side publisher idle expiry. The server can let unused cached publisher handles fall out after a timeout without requiring the client to perfectly report object lifetimes.

An explicit publisher destroy command can still be considered later, but it should use server-issued or otherwise unique-enough publisher ids and still have a timeout as a backstop.

## Configuration direction

The eventual configuration should be conservative and easy to reason about:

```txt
queue_idle_evict_after_ms = null      # disabled, or a generous default
queue_idle_sweep_interval_ms = 60000
publisher_idle_evict_after_ms = null  # optional server-side publisher cache expiry
```

When enabled, eviction should be best-effort. It should reduce memory and background work, but it should not change delivery semantics.

## Storage-side guardrails

The storage layer should only unmaterialize a queue when it can do so cleanly:

- no inflight messages that require live lease tracking
- no active command loop work that must complete before shutdown
- no periodic snapshot task still able to touch the old handle
- dirty state snapshotted when configured before the handle is dropped
- logs closed after the queue actor has drained and exited

If a stale handle receives a command after eviction has begun, returning an error is acceptable. The normal broker path should prevent this by only evicting queues with no active publishers, subscribers, pending confirms, or pending settlements.

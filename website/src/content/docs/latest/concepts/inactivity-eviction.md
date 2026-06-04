---
title: Inactivity and eviction
description: How Fibril should treat unused queue resources.
---

Fibril queue data is durable. Resource eviction should only remove cached in-memory broker state, not message history, event logs, snapshots, or queue configuration.

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

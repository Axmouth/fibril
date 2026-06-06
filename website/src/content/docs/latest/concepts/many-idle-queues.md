---
title: Many idle queues
description: How Fibril handles sparse workloads with many durable queues but few active ones.
---

Some workloads naturally create many queues while only using a small fraction at any one time. Examples include per-customer queues, per-tenant workflows, scheduled jobs, or rarely used routing keys.

Fibril is designed to make that shape practical. A queue can remain durable on disk without staying fully loaded in memory forever.

This page describes what users should expect when idle queue cleanup is enabled.

## The Problem

It should be reasonable to define many queues even if most of them are quiet most of the time.

Without cleanup, every touched queue can keep runtime resources alive even after the last producer and consumer stop using it. That is wasteful for sparse workloads.

Idle queue cleanup is meant to reduce that always-on memory and background work. It is not message retention, queue deletion, TTL, or dead-lettering.

## What Fibril Does

Fibril uses two related behaviors:

- Queues are loaded lazily. A durable queue on disk does not have to be loaded into memory at process start.
- Idle queues can be unloaded from memory after they have no active publishers, no active subscribers, and no inflight work for a configured amount of time.

Unloading a queue from memory does not delete it.

The following remain durable:

- message log
- event log
- snapshots
- ready messages
- settled history needed for replay
- queue identity and persisted state

When the queue is used again, Fibril loads it from those records and continues delivery.

## When a Queue Is Considered Idle

A queue becomes idle after the last active publisher and subscriber for that queue are gone.

For normal client usage:

- a subscriber stops being active when it unsubscribes or its connection closes
- a publisher stops being active when its connection closes
- if publisher idle expiry is enabled, an unused publisher can stop being active before the connection closes

The idle timeout starts only after Fibril observes no active publishers or subscribers for that queue.

Fibril will not unload a queue just because a consumer is slow. It also checks for active delivery work before unloading.

## When Cleanup Can Run

Idle queue cleanup is disabled by default. Configure it with:

```toml
[runtime_seed.idle_queue_cleanup]
enabled = false
evict_after_ms = 600000
sweep_interval_ms = 60000
publisher_idle_timeout_ms = 600000
```

`runtime_seed` values seed the persisted runtime settings document on first boot. Once runtime settings exist, persisted runtime settings own these values unless a setting group is explicitly locked by startup config.

When idle queue cleanup is enabled, a background worker wakes every `sweep_interval_ms` and checks tracked queues.

The current server binary also accepts environment variables for compatibility:

```txt
FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS
FIBRIL_QUEUE_IDLE_SWEEP_INTERVAL_MS
FIBRIL_PUBLISHER_CACHE_IDLE_TIMEOUT_MS
```

`evict_after_ms` controls how long a queue must stay idle before it can be unloaded. `sweep_interval_ms` controls how often the background cleanup worker checks for idle queues.

For sparse workloads with long-lived publishing connections, also set `publisher_cache_idle_timeout_ms`. Otherwise, a connection that published to a queue may keep that queue active until the connection closes.

## What Blocks Cleanup

Fibril skips cleanup if it sees any of the following:

- an active publisher for the queue
- an active subscriber for the queue
- messages already handed to consumers but not settled yet
- pending acknowledgement or retry work
- storage-reported inflight messages
- the configured idle window has not elapsed yet

This means ready messages can exist on disk while a queue is unloaded, but inflight messages keep the queue active until they are resolved.

## Why This Works

The common sparse-queue case has long periods where a queue has no producers, no consumers, and no inflight messages. In that state, keeping the queue loaded is not necessary for correctness.

Ready messages are durable. If a subscriber returns later, Fibril reloads the queue and delivers them.

The tradeoff is simple: idle queues use less runtime memory, while the next operation on a cold queue may pay the cost of loading it again.

## Operational Guidance

Use a conservative idle window. Very small windows are useful in tests, but a real deployment should avoid constantly unloading and reloading queues that are only briefly quiet.

Treat cleanup as best-effort resource management, not as a correctness boundary. The broker may skip a queue during one sweep and unload it during a later sweep after the guards pass.

If you care about exact builds or behavior, check the [project status](/latest/status/) and deployment settings before relying on this in production-like environments.

## Current Status

Implemented now:

- lazy loading of durable queues
- configurable idle queue unloading
- configurable publisher idle expiry for long-lived connections
- live updates for idle cleanup and publisher idle expiry runtime settings
- admin JSON API for reading and updating runtime settings
- tests for active publishers, active subscribers, idle threshold behavior, inflight guards, disabled cleanup, live enabling, direct cleanup, and worker-driven cleanup

Still early:

- runtime settings are persisted and versioned, but the admin editing UI is not wired yet
- delayed-message cleanup behavior needs more end-to-end coverage before treating it as a documented guarantee
- the broker may keep small in-process bookkeeping for a queue even after unloading the durable queue state

For implementation details, see [idle queue internals](/latest/development/idle-queue-internals/).

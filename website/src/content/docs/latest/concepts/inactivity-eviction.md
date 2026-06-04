---
title: Inactivity and eviction
description: Lazy queue loading and idle memory cleanup for sparse workloads.
---

Some workloads naturally create many queues while only using a small fraction at any one time. Examples include per-customer queues, per-tenant workflows, scheduled jobs, or rarely used routing keys.

Fibril should support that shape without forcing every durable queue to keep an actor, logs, snapshots, timers, and broker-side delivery state live in memory forever. Storage usage is still real, but inactive queues should be close to free at runtime.

This page describes the pair of features that address that:

- lazy materialization loads a queue into memory only when work needs it
- inactivity eviction drops idle in-memory queue handles while keeping durable data on disk

These are resource-management features. They are not retention, TTL, deletion, or dead-letter policies.

## What Stays Durable

Eviction does not delete queue data.

The following stay on disk:

- message log
- event log
- snapshots
- ready messages
- settled history needed for replay
- queue identity and durable state known to storage

When the queue is needed again, storage can materialize it from those durable records.

## What Can Leave Memory

There are two useful levels to distinguish:

- storage materialization: the Stroma queue handle, queue actor, open logs, snapshot task, and recovered queue state
- broker tracking: the broker's lightweight per-queue loop state, activity counters, delivery tags, and subscription/publisher leases

Current eviction is mainly about storage materialization. An idle queue can stop having a live Stroma queue handle even though the broker may still remember a small `QueueLoopState` for that topic/group during the current process lifetime.

After a process restart, durable queues on disk are not all automatically hot in the broker. A queue becomes materially live when an operation needs it, such as publishing, subscribing, polling delivery, settling, expiry processing, admin inspection, or future configuration work.

## When Eviction Can Run

Idle queue eviction is controlled by two broker settings:

```txt
queue_idle_evict_after_ms = null
queue_idle_sweep_interval_ms = 60000
```

The default is disabled. When `queue_idle_evict_after_ms` is set, a sparse background worker wakes every `queue_idle_sweep_interval_ms` and checks tracked queues.

A queue becomes an eviction candidate only after the last active publisher lease and subscriber lease for that queue are gone. At that moment, the broker records an idle timestamp. The queue must remain idle for at least `queue_idle_evict_after_ms`.

The broker will skip eviction when it sees active work:

- an active publisher handle for the queue
- an active subscriber handle for the queue
- broker delivery tags for messages already handed to consumers
- pending settlement work
- storage-reported inflight messages
- the idle window has not elapsed yet

If those checks pass, the broker asks storage to unmaterialize the queue. Storage can still refuse if it finds a race or a guard condition.

## Why This Works

The common sparse-queue case has long periods where a queue has no active publishers, no active consumers, and no inflight messages. In that state, keeping a live queue actor and open log handles is unnecessary.

Ready messages do not require a hot in-memory queue. They are durable offsets on disk. If a subscriber returns later, the queue can materialize and deliver them.

This lets Fibril optimize for the shape "many queues defined, few active right now" without changing delivery semantics. The cost shifts from always-live memory to materialization work when a cold queue is touched again.

## Current Status

Implemented now:

- broker-side publisher and subscriber lease accounting
- idle timestamp tracking after the last lease drops
- Stroma materialize, unmaterialize, `is_materialized`, and `has_inflight` APIs
- broker-side eviction gate using activity, delivery tags, pending settlements, and storage inflight checks
- optional background sweep worker
- tests for active publishers, active subscribers, untracked queues, idle threshold, broker-held deliveries, already-unmaterialized queues, direct sweep, disabled worker behavior, and worker eviction

Still early or not yet user-facing:

- configuration is not surfaced as a polished runtime/server config
- delayed-message eviction needs more end-to-end coverage before treating it as a documented guarantee
- publisher cache expiry is not implemented separately from connection lifetime
- the broker still keeps lightweight per-queue loop entries during the process lifetime

## Publisher Caches

For now, connection-lifetime publisher caching is the safer default. It avoids create/destroy publisher protocol messages that can desync if a client creates and drops publisher objects frequently.

If publisher resource pressure becomes meaningful, the next step should be server-side publisher idle expiry. That can let unused cached publisher handles fall out after a timeout without requiring clients to perfectly report object lifetimes.

An explicit publisher destroy command can still be considered later, but it should use server-issued or otherwise unique-enough publisher ids and still have a timeout as a backstop.

## Operational Notes

Eviction is best-effort. It should reduce memory and background work, but it should not be used as a correctness boundary.

Use a conservative idle window. Very small windows are useful in tests, but a real deployment should avoid constantly evicting and rematerializing queues that are only briefly quiet.

If a stale storage handle receives a command after eviction has begun, returning an error is acceptable. The normal broker path is designed to prevent that by evicting only queues with no active publishers, subscribers, pending deliveries, pending settlements, or storage inflight work.

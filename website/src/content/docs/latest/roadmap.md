---
title: Roadmap
description: Near-term work for Fibril.
---

Fibril is still early. Treat this page as the current checkpoint for what has
landed recently and what is still pending. The near-term focus is making the
existing semantics complete, observable, and usable before adding larger
distributed-system features.

For the reverse view, use [implemented surface](/latest/implemented-surface/) to
check what is already wired and under what conditions.

## Recently landed

- Runtime broker settings can be read and updated through the admin surface.
- The global DLQ target is stored as versioned Stroma state and can be edited at runtime.
- Queue-specific retry and dead-letter policy can be declared from clients and `fibrilctl`.
- Message inspection and DLQ replay are available from the admin API, dashboard, and CLI.
- Sparse queues use lazy loading plus idle cleanup, with queue-state visibility in the admin dashboard and CLI.
- Idle cleanup is guarded against racing with newly created publisher and subscriber leases.
- Message inspection can temporarily load a queue, and idle cleanup can unload that queue again after the idle window.
- The TypeScript demo now closes subscriptions promptly on Ctrl-C while remaining continuous by default.
- The admin dashboard has moved to the same visual language as the public site, with Clarity removed from the vendored UI assets.
- The TypeScript client tracks the Rust client for delayed publish, confirmed publish pipelining, content-type metadata, queue declaration, and group-default behavior.
- Reconnection resume identity is now part of the TCP handshake, and Rust and TypeScript clients send it on explicit reconnect.
- The TCP handler can keep a logical connection dormant during a configured grace window, accept late settles after resume, and requeue unsettled inflight messages when grace expires.

## Near term

- Add automatic client reconnect loops that use reconnect grace.
- Keep improving DLQ replay and message inspection workflows, especially bulk operations and clearer operator feedback.
- Add the next storage-level startup/runtime settings where they have clear operational value.
- Keep refining sparse-queue observability where it helps operators decide why a queue is loaded, idle, or not yet unloaded.
- Improve TCP protocol ergonomics and error behavior.
- Keep Rust and TypeScript client APIs aligned as public-path behavior changes.

## Medium term

- Continue improving runnable broker images and binaries, including `fibrilctl` in the server image.
- Add more production deployment guidance for the broker itself.
- Improve admin interface observability.
- Produce repeatable benchmark reports.
- Tighten memory behavior under large queue depth, high inflight load, and many idle queues.

## Longer term

- Replication design and implementation.
- Clustering and partition ownership.
- More complete client ecosystem. Priority order: Python, including a blocking
  client, then C#, Go, and Java.

## Out of scope

Transactions are not planned. Fibril is aiming for clear durable messaging semantics, not a clone of every broker feature, and transactional publish/consume workflows are intentionally out of scope.

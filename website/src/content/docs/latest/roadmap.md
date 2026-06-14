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

- Partitioned queues: declare can create multiple partitions, producers can use
  `partition_key` for stable key routing or rely on round-robin spread, and
  subscriptions transparently fan in across known partitions.
- Cluster coordination: Ganglion-backed coordination can register brokers and
  queues, run the placement controller, expose topology, and drive owner and
  follower role transitions.
- Replication data plane: followers pull from owners, install checkpoints when
  needed, report durable progress, and can be promoted after owner loss under
  epoch fencing.
- Replica-durable publish confirms: replica progress can gate confirmed publish
  replies, with `min_in_sync_replicas` fail-fast behavior and ISR observability.
- Exclusive consumer groups: an opt-in `.exclusive()` subscription gives ordered,
  balanced, sticky, self-healing consumption of a partitioned queue (one consumer
  per partition), with reconnect-safe membership and a soft per-consumer target.
  Single-node works end to end; the cross-broker coordinator is wired (multi-node
  integration test pending). See [consumer groups](/latest/concepts/consumer-groups/).
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
- Reconnection resume identity is now part of the TCP handshake, and Rust and TypeScript explicit reconnect calls send it and report whether resume was accepted.
- Existing Rust and TypeScript publisher handles use the latest engine after explicit reconnect.
- Rust and TypeScript clients make one conservative automatic reconnect attempt before a new operation when the previous engine is known closed.
- The TCP handler can keep a logical connection dormant during a configured grace window, accept late settles after resume, and requeue unsettled inflight messages when grace expires.
- Rust and TypeScript clients now send subscription metadata after a successful resume, and the broker replies with a reconciliation result.
- Rust and TypeScript clients now keep active subscription streams alive when reconciliation confirms the subscription should be kept.
- Reconnection reconciliation now has a conservative default plus an opt-in
  restore-client-subscriptions policy. Server-only subscriptions are dropped,
  metadata mismatches close client streams, and restored subscriptions can remap
  to a fresh server subscription id.
- Reconnect and subscription reconciliation outcomes are now visible through
  admin overview counters, TCP metrics logs, and structured reconciliation logs.
- The admin overview now includes compact Stroma timing and health signals, and
  the diagnostics page exposes the broader Stroma metrics snapshot.

## Near term

- Refactor server bootstrap wiring into the `fibril` library enough to make
  multi-node coordination and cohort-controller tests stand up real brokers
  without going through `main`.
- Add the pending multi-node integration test for cross-broker exclusive
  consumer-group coordination.
- Finish the combined Offset plus Topic/Group newtype pass after the current
  partition newtype pass, including the planned `Arc<str>` direction for
  Topic/Group.
- Add a user-facing replication, partitioning, and consumer-group explainer that
  connects the pieces without exposing implementation noise.
- Improve client feedback for reconnect cases where reconciliation closes a
  subscription stream.
- Keep improving DLQ replay and message inspection workflows, especially bulk operations and clearer operator feedback.
- Add the next storage-level startup/runtime settings where they have clear operational value.
- Keep refining sparse-queue observability where it helps operators decide why a queue is loaded, idle, or not yet unloaded.
- Explore cluster-wide immutable runtime policy. Hardware-shaped startup
  settings such as storage paths and log tuning should stay local, while any
  cluster-wide runtime locks need a replicated policy rather than node-local
  boot config.
- Improve TCP protocol ergonomics and error behavior.
- Keep Rust and TypeScript client APIs aligned as public-path behavior changes.

## Medium term

- Add cross-broker replication lag aggregation to `fibrilctl topology` and the
  admin topology diagram.
- Add programmatic node management and scale-up or scale-down flows around the
  Ganglion-backed cluster path.
- Add fault-injection and unreliable-infrastructure testing for partitions,
  latency, dropped traffic, and leadership churn.
- Add consumer-group assignment narrowing so clients can subscribe only to their
  assigned partition subset instead of relying only on the delivery gate.
- Continue improving runnable broker images and binaries, including `fibrilctl` in the server image.
- Add more production deployment guidance for the broker itself.
- Improve admin interface observability.
- Produce repeatable benchmark reports and keep the
  [optimization log](/latest/development/optimization-log/) current as
  low-level performance work is investigated.
- Tighten memory behavior under large queue depth, high inflight load, and many idle queues.

## Longer term

- Hardening the experimental replication and clustering path into production
  guidance and supported defaults.
- Live repartitioning. The current direction is fixed-at-create partition count
  first, with live grow or shrink treated as a separate migration feature.
- Durable broker restart reconciliation. This would let clients reclaim
  persisted session and inflight ownership after a broker process restart,
  using a startup grace window before normal redelivery resumes. Current
  reconnect grace is live-process only.
- More complete client ecosystem. Priority order: Python, including a blocking
  client, then C#, Go, and Java.

## Out of scope

Transactions are not planned. Fibril is aiming for clear durable messaging semantics, not a clone of every broker feature, and transactional publish/consume workflows are intentionally out of scope.

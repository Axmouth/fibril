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

- Plexus streams: a fan-out channel type alongside the work queue, where every
  subscriber receives every matching record and position is a durable named
  cursor rather than consume-and-delete. Per-stream durability tiers (durable,
  speculative, ephemeral) trade delivery latency for durability, partitioning
  spreads writes with transparent client-side fan-in, header filters narrow a
  subscription, and acks advance the durable cursor. Wired in the broker,
  protocol, Rust/TypeScript/Python clients, and the admin streams page. See
  [Plexus streams](/latest/concepts/plexus-streams/).
- Stream cluster placement and ownership: durable streams are placed across nodes
  (spread owner-first), their config lives in coordination, and a non-owner
  redirects publishers and subscribers to the current owner.
- Durable stream replication, tier-gated: the durable tier replicates its record
  and cursor logs to followers, a durable publish confirms once enough replicas
  are durable, and a caught-up follower is promoted on owner loss. The express
  tiers stay owner-only by design. Owner-only durable survives restart, replicated
  durable survives node loss.
- Stream cursor-commit microbatching: high-fan-out auto-ack coalesces cursor
  commits per partition into one durable write and one actor message per window,
  so durable subscriptions stay fast under many readers.
- Live repartitioning (experimental): grow or shrink a queue's partition count in
  coordinated mode from the admin topology page, with versioned routing and
  drain-on-shrink.
- A Python client landed at full feature parity with the Rust and TypeScript
  clients, with both an async API and a thin blocking facade over the same core.
  See [client usage](/latest/clients/).
- The TypeScript client reached parity with the Rust reference, including
  exclusive consumer groups, the assignment-events stream, live-grow partition
  pickup, typed wire errors, and producer-id dedup headers.
- Message TTL: messages can expire by age, set per message with an `expiring`
  publisher or per queue with a default TTL on declare. Expired messages drop
  through the dead-letter or discard path rather than being silently consumed.
  Wired in the broker and all three clients.
- Single-node queue creation and deletion are available from the admin API and
  dashboard, and the queues page can hide inactive queues and filter by name.
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
  Single-node works end to end, and a coordination-level multi-node test covers
  cross-broker membership aggregation and rebalance. Fuller broker/client
  scenario coverage is still growing. See [consumer groups](/latest/concepts/consumer-groups/).
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
- Add fuller multi-broker client scenario tests for cross-broker exclusive
  consumer-group coordination and failure handling.
- Finish the combined Offset plus Topic/Group newtype pass after the current
  partition newtype pass, including the planned `Arc<str>` direction for
  Topic/Group.
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
- Keep the Rust, TypeScript, and Python client APIs aligned as public-path
  behavior changes.

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

- Hardening the experimental replication and clustering path (queues and streams)
  into production guidance and supported defaults.
- Durable broker restart reconciliation. This extends the live-process graceful
  reconnect (see [reconnection grace](/latest/development/reconnection-grace/))
  across a broker process restart. A client whose connection is still within its
  grace window would reclaim its persisted session and inflight ownership
  transparently, using a startup grace window before normal redelivery resumes,
  so a restart is invisible to in-flight work rather than a disconnect. The same
  property makes rolling upgrades trivial. Current reconnect grace is live-process
  only. The partition side already has its counterpart: on cold restart a
  partition reassigned away while the node was down is retained as inert on-disk
  cold storage rather than served stale or discarded.
- More complete client ecosystem. The Python client (async plus a blocking
  facade) has landed. The next targets are C#, Go, and Java.

## Out of scope

Transactions are not planned. Fibril is aiming for clear durable messaging semantics, not a clone of every broker feature, and transactional publish/consume workflows are intentionally out of scope.

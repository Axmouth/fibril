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

Highlights only. For the full wired surface and its conditions, see
[implemented surface](/latest/implemented-surface/).

- Plexus streams: a fan-out channel type alongside the work queue, with durable
  named cursors, per-stream durability tiers, partitioning with client-side
  fan-in, and header filters. Durable streams are placed and owned across nodes,
  the durable tier replicates record and cursor logs with caught-up failover, and
  high-fan-out auto-ack coalesces cursor commits. See
  [Plexus streams](/latest/concepts/plexus-streams/).
- Wildcard subscribe / discovery routing: an opt-in client view subscribes to
  every queue or stream matching a `*`-glob and auto-attaches new matches from a
  live cluster catalogue. Client-side only, in all three clients.
- Partitioned queues: multiple partitions per queue with `partition_key` stable
  routing or round-robin spread, transparent subscription fan-in, and live grow
  or shrink from the admin topology page.
- Cluster coordination and replication: Ganglion-backed placement and ownership,
  follower pull replication with checkpointing and epoch-fenced promotion, and
  replica-durable publish confirms with `min_in_sync_replicas`.
- Exclusive consumer groups: opt-in ordered, balanced, sticky, self-healing
  consumption of a partitioned queue (one consumer per partition). See
  [consumer groups](/latest/concepts/consumer-groups/).
- Message TTL: per-message or per-queue expiry that drops through the dead-letter
  or discard path rather than being silently consumed.
- Reconnection grace: server-issued resume identity in the handshake, dormant
  logical connections across a transient break, late settles after resume, and
  conservative subscription reconciliation with an opt-in restore policy. Rust,
  TypeScript, and Python.
- Clients: a Python client at full parity (async plus a blocking facade) joined
  the Rust and TypeScript clients, which track each other across the public path.
  See [client usage](/latest/clients/).
- Admin and ops: single-node queue create and delete, message inspection, DLQ
  replay, runtime broker settings, a versioned global DLQ target, and Stroma
  timing and health signals, from the dashboard, API, and `fibrilctl`.

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
- Planned restart announcement (drain). Before a graceful shutdown or upgrade the
  broker would push a "going away" notice with a grace deadline, so clients stop
  issuing new work, settle in-flight, and enter their resume path proactively
  instead of after a dropped socket. Combined with durable restart reconciliation
  this turns a rolling upgrade into announce, drain, restart, resume.
- More complete client ecosystem. The Python client (async plus a blocking
  facade) has landed. The next targets are C#, Go, and Java.

## Out of scope

Transactions are not planned. Fibril is aiming for clear durable messaging semantics, not a clone of every broker feature, and transactional publish/consume workflows are intentionally out of scope.

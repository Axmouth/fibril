---
title: Replication design
description: Development notes on Fibril's follower-pull replication, durability invariants, and the streaming catch-up model.
---

This is a development note. User-facing behavior lives in
[replication](/reliability/replication/). This page records the internal
invariants and why they were chosen.

## Follower progress is local-append progress

An owner read is only a proposal from the follower's point of view. A follower
advances its durable replication cursor only after its own local append path
accepts the owner's batch, never from the owner-returned offsets alone.

This keeps the durability contract honest: a replica-durable confirm waits on
what followers have accepted durably, not on what the owner attempted to send. It
also keeps gaps, overlaps, stale epochs, and future prefix-validation failures
meaningful, because a follower cannot report itself caught up without actually
holding the corresponding local durable records.

## Queue payloads and state share a log

Each enqueue carries its payload at the containing event offset. ACK, NACK and
other state changes refer to earlier offsets. A follower validates the frame,
appends its records, waits for the required local durability and applies events
in order before reporting progress. Failed or partially completed application
fences promotion until recovery restores an admitted history.

Streams retain separate record and cursor-event logs. Their follower path drains
both storage operations and advances the stream tail only after accepted writes.
Queue checkpoint backfill may include payload-bearing records older than the
snapshot cut. Those records restore payload coverage without reapplying state
already represented by the snapshot.

## Overlap is a checkpoint repair

Keratin's replicated append rejects partial overlap by default, which is correct:
"I already have offsets 0..10" does not prove those bytes are identical to a new
owner's 0..10, and accepting a suffix over an unproven prefix would silently bless
divergence.

An overlap or gap triggers history repair. An admitted replica cannot silently
reset its accepted history. A replacement uses an authorized learner or recovery
installation, with a verified snapshot and the retained payload range it needs.
Exact duplicate appends preserve idempotence. Incompatible evidence stays fenced.

## Replica-durable visibility: consumers see only committed data

Durability (when a publish confirms) and visibility (when a consumer may receive
a message) must agree. A replica-durable queue gates delivery on the
committed-replicated watermark, the Kafka high-watermark model: a delivery lease
is only allowed for offsets below the highest offset durable on the required
number of replicas.

The watermark derives from the required followers’ durable queue event frontiers
and the owner’s locally durable, applied dependencies. The same progress reports
drive the publish-confirm gate. Payload positions are sparse event offsets. It is exposed to the delivery loop as one per-queue
atomic ceiling, so the hot path stays lock-free.

Local-durable queues are never gated: with the owner alone defining durability
there is no committed watermark to wait on, so single-node delivery is unchanged.
The consequence on a replica-durable queue is that delivery latency is bounded
below by commit latency. That is correct: consumers trade a few milliseconds for
never acting on data that could vanish on failover.

## Streaming catch-up is credit-based

Strict request-then-response per batch left the follower idle on the wire while
applying and idle applying while fetching. Once batch sizes made fsync cheap,
that serialization (an owner-read round trip plus an apply, repeated per tick)
became the dominant cluster latency.

The streaming model is a continuous stream with credit-based flow control. The
follower opens a stream and grants the owner a byte budget. The owner pushes
offset-ordered batches down one connection and pauses when credit runs low. The
follower reads into a bounded in-order buffer and applies concurrently, so the
next batch arrives while the current one is applied. TCP already gives in-order,
reliable single-connection delivery, so ordering needs no per-batch round trips.
Credit refills at a low watermark (not at zero) so the pipe never drains. The
applier still runs sequentially through the unchanged durable apply path, so the
failover-safety ordering is untouched: only fetching is overlapped.

## See also

- [Replication](/reliability/replication/) for the user-facing model.
- [Clustering](/concepts/clustering/) for ownership and failover.
- [Recovery quarantine](/reliability/recovery-quarantine/) for damaged-log handling.

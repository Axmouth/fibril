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

## Queues replicate one log

A queue partition has a single log. Each enqueue record carries its payload, and
a message's id is the log offset of that record. ACK, NACK and other state
changes refer to earlier offsets in the same log. A follower validates the frame,
appends its records, waits for the required local durability and applies them
in order before reporting progress. Failed or partially completed application
fences promotion until recovery restores an admitted history.

Replication tracks one position per queue. A follower's cursor, its progress
reports, the owner's view of each follower and the heartbeat applied-tail label
all carry a single queue log offset. Publication dependencies and the
compaction retention floor are single offsets too.

Streams retain separate record and cursor-event logs. Their follower path drains
both storage operations and advances the stream tail only after accepted writes.
Stream progress is a record and cursor-event pair. A progress session fixes its
resource kind when it opens and checks it against the partition's own kind, so a
queue never opens a stream session and the reverse. Queue confirms count only
queue progress and stream confirms count only stream record progress, so cursor
events never release a record confirm.

A caught-up stream follower long-polls the owner with `max_wait_ms`. The owner's
read waits on the queue replication wakeup and on the stream channel's durability
signal, which fires when new stream records or cursor commits become durable. A
caught-up follower therefore gets new durable data right away instead of
sleeping out the full wait, and replica-durable stream confirms do not wait for
the long-poll timeout. The signal has no memory, so the read registers for it
before checking for data.

Queue checkpoint backfill may include payload-bearing records older than the
snapshot cut. Those records restore payload coverage without reapplying state
already represented by the snapshot.

### Internal frames

Replication uses node-only internal operations. Each body starts with a
four-byte magic, so a peer on a mismatched build fails the decode instead of
misreading the layout.

| Ops | Name | Body |
| --- | --- | --- |
| 80, 81 | `QueueReplicationRead`, `QueueReplicationReadOk` | One start offset (`FQR1`), one queue log batch (`FQO1`) |
| 84, 85 | `QueueCheckpointExport`, `QueueCheckpointExportOk` | The reply carries one epoch, the backfill start and the applied boundary (`FQC1`). A reply whose start is past its boundary is refused |
| 93 to 98 | `QueuePush` family: start, batch, progress, reset, stop, end | Credit-based queue push. Start, progress and reset carry one offset (`FQS1`, `FQP1`, `FQX1`). A batch uses the read reply body |
| 99, 100 | `StreamReplicationRead`, `StreamReplicationReadOk` | Stream record and cursor-event logs, two lanes |

Ops 82 and 83, a follower push apply, and ops 86 and 87, a follower checkpoint
install, are retired. A follower below the owner's retained history recovers
through coordinated queue reseeding. Client message frames do not use these
operations. Upgrade every participating broker together when these bodies
change.

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

The watermark derives from the required followers' durable queue log positions
and the owner's locally durable, applied dependencies. The same progress reports
drive the publish-confirm gate. Visibility advances only to completed
publication boundaries, so a follower holding part of a publish batch neither
confirms nor exposes it. The watermark is exposed to the delivery loop as one
per-queue atomic ceiling, so the hot path stays lock-free.

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

The streaming model is a continuous stream with credit-based flow control, carried
by the `QueuePush` operations. The follower opens a push and grants the owner a byte budget. The owner pushes
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

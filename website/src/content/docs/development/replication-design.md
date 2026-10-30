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

Cursor commits skip the per-commit fsync. The owner writes them with
`AfterFlush` and the cursor log's background sync, about every 100 ms, makes
them durable. The owner serves cursor events to followers only up to the cursor
log's durable frontier, even when early replication serves records from the
written frontier. A follower therefore never holds a cursor event the owner can
lose, and an owner power cut cannot make the owner reuse an offset a follower
already applied. A follower writes a replicated batch of only cursor commits
with `AfterFlush` too, so the record append in the same frame does not wait on
the cursor log fsync. A batch with any other event, such as a retention
truncate, keeps its fsync. The cursor-event position a stream follower reports
counts written events. It is for reporting only, and nothing trims history by
it.

A power cut can lose cursor advances from about the last background sync
interval, on the owner or on a follower. Readers resuming from those cursors
receive those records again, which at-least-once delivery allows. Records are
never lost this way. A follower restarts from the end of its local cursor log
and pulls the lost events again. A stream snapshot syncs the cursor log through
its boundary first, so after a power cut the log never ends below the snapshot.

A `durable` stream without followers keeps its records and cursors on the owner
alone. It survives an owner restart but not the loss of the owner node, so the
partition waits for that owner to return. See
[failure modes](/reliability/failure-modes/).

A caught-up stream follower long-polls the owner with `max_wait_ms`. The owner's
read waits on the queue replication wakeup and on the stream channel's durability
signal, which fires when new stream records become durable, and on the cursor
log's durable frontier, which moves when the background sync lands cursor
commits. A caught-up follower therefore gets new durable data right away instead
of sleeping out the full wait, and replica-durable stream confirms do not wait
for the long-poll timeout. The signal has no memory, so the read registers for it
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

### Overlap diagnostics

A record that conflicts with an existing offset is an error, and diagnostics only
help reconstruct how the two histories diverged. They never bypass validation or
discard either history, and a content mismatch does not request a repair on its
own.

To read a report, join these log lines by node and process, queue identity, log
and `diagnostic_id`. IDs are local to a process.

1. `applying queue assignment transition` records the intended owner and epoch.
   It is an attempt, not proof that the transition completed.
2. `replication overlap diagnostic` holds the conflicting offset, the local head,
   next and durable-next boundaries, the compared range, which record components
   differ, and the recent control operations on that log.
3. `replication overlap event coordinates` lists local events near the conflict,
   the latest local events and the incoming events near the conflict, with their
   offsets, kinds and the highest payload offset each references.
4. `follower replication worker tick failed` adds the source owner, the
   assignment epoch and the worker's continuation offset.

A control history offset is the boundary observed at that operation, not
necessarily a record's identity, so use the event windows for exact offsets.
Control timestamps and sequence numbers order observations within one log only.
Reports never contain message bodies, headers, hashes, declaration settings,
cursor names or DLQ destinations.

Limits, chosen so diagnostics cost nothing on the normal path:

- Each open log keeps its last 32 control operations in memory. Role changes,
  declarations, epoch changes, truncations and checkpoint resets add one, and
  ordinary appends do not.
- Each log emits a full report at most once per 30 s. Repeats keep the same ID,
  skip the event scans and still fail, and the next full report counts the
  suppressed ones.
- Each event window holds at most 8 entries, records over 64 KiB are not decoded,
  and the reads run on the blocking pool only for a full report.
- History is lost when a log reopens or the process restarts, and a concurrent
  truncation or checkpoint replacement can change what the windows show, so
  capture the first failure before attempting recovery.

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

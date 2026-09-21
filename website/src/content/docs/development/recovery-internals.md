---
title: Queue recovery and checkpoint internals
description: Event-prefix checkpoints, replay boundaries and recoverable suffix repair.
---

Queue recovery loads a checkpoint and replays its event suffix on first use.
The operator-facing corruption policy is described in
[recovery quarantine](/reliability/recovery-quarantine/).

## Exact application boundary

Durable queue events complete actor application in log order. Disk appends remain
concurrent. The application gate advances an exclusive `event_next` only after
all actor commands for that event batch have completed; zero represents an empty
applied prefix.

Checkpoint capture acquires this gate, passes its permit to the actor command and
clones state with the actual boundary. A cancelled receiver cannot release the
permit while its command remains queued. Encoding and file I/O run after the
actor releases the application gate. Delivery leases and local timer state are
also present in a live capture, which matters when comparing it with event-only
replay.

Partial application invalidates the sequence and blocks later owner application,
checkpoint capture and promotion. Role changes preserve that failure. Completed
recovery or a validated checkpoint installation establishes the next boundary;
replaying verified follower overlap skips already-applied events. Reapplying a
NACK can increment retries again, so replay starts at the exclusive checkpoint
boundary.

## Persistence and compatibility

Version-two snapshot envelopes store an exclusive event boundary. Legacy
version-one envelopes store an inclusive value and remain readable, including the
existing ambiguous-zero recovery handling. Older binaries cannot read version-two
snapshots; preserve a compatible data copy when testing an older revision.

On Unix, the file and directory entries are synchronized before periodic prefix
compaction. A failed write leaves the queue dirty for retry. Snapshot capture,
persistence and compaction serialize with follower application and recovery
installation; later owner applications can proceed after capture and mark the
queue dirty again. Directory durability and crash acceptance on other platforms
remain separate validation work.

## Parallel append recovery

Message and enqueue-event durability can overlap. Recovery folds enqueue and
cancel events against the durable message tail to identify an unconfirmed suffix
whose payloads were lost. Ordinary confirmation requires both durable logs, so
this suffix can be removed without dropping a confirmed publication.

Corrupt records and unexplained missing non-enqueue dependencies use the configured
corruption policy. A preceding dangling enqueue does not conceal later corruption.
The suffix-repair journal preserves the valid prefix and resumes an interrupted
cut before ordinary log opening. Checkpoint installation has a separate journal
covering replacement of both logs and actor state.

## Recovery comparison

Sealed inspection can replay different exact checkpoints to a common boundary and
compare state and live payload identities. A separate lease projection accounts
for ordinary owner delivery, which does not append a lease event. Timer state,
resource incarnation and checkpoint authority still require explicit proof before
source selection; see [recovery sealing](/reliability/recovery-sealing/) and the
[failover plan](/development/failover-plan/).

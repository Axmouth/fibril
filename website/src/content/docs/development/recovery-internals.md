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
all actor commands for that event batch have completed. Zero represents an empty
applied prefix.

Checkpoint capture acquires this gate, passes its permit to the actor command and
clones state with the actual boundary. A cancelled receiver cannot release the
permit while its command remains queued. Encoding and file I/O run after the
actor releases the application gate. Delivery leases and local timer state are
also present in a live capture, which matters when comparing it with event-only
replay.

Partial application invalidates the sequence and blocks later owner application,
checkpoint capture and promotion. Role changes preserve that failure. Completed
recovery or a validated checkpoint installation establishes the next boundary.
Replaying verified follower overlap skips already-applied events. Reapplying a
NACK can increment retries again, so replay starts at the exclusive checkpoint
boundary.

## Persistence and compatibility

Version-two snapshot envelopes store an exclusive event boundary. Legacy
version-one envelopes store an inclusive value and remain readable, including the
existing ambiguous-zero recovery handling. Older binaries cannot read version-two
snapshots, so preserve a compatible data copy when testing an older revision.

The file and directory entries are synchronized before periodic prefix
compaction. A failed write leaves the queue dirty for retry. Snapshot capture,
persistence and compaction serialize with follower application and recovery
installation. Later owner applications can proceed after capture and mark the
queue dirty again. Crash acceptance tests run on Linux only.

## Single-log queue recovery

An enqueue record carries its payload. Its log offset is the message id, and
subsequent state changes refer to earlier payload offsets in the same log.
Recovery decodes retained records and rejects forward references. Corruption
uses the configured quarantine, refusal or explicit local-repair policy.

Local suffix repair requires an unbound storage history. Accepted replicated
histories preserve their bytes for coordinated recovery and cannot independently
truncate or reuse offsets. Checkpoint installation has a separate journal covering
the retained payload-bearing event prefix and actor snapshot. The prefix may start
before the snapshot cut when older messages remain live.

## Evidence and stage formats

Recovery evidence names its logs by resource kind. A queue has one log and a
stream has a record log and a cursor event log. Seal replies (op 89, `RSO3`),
retained history ids, recovery inspection, source selection, plans and stages
all carry that shape, and each refuses evidence of the other kind. Recovery
inspection accepts queue histories only and refuses a stream command itself,
since the seal history already carries the kind.

The current formats ship with Stroma storage layout version 3. Seal receipts,
history ids, inspection journals, phase caches, selections, plans and stage
intents written before that layout are not reused:

- Retained history ids use the `fibril-retained-history-v2` tag.
- Source selections use `fibril-queue-recovery-selection-v2` and plans use
  `fibril-queue-recovery-plan-v2`.
- A recovery stage keeps its staged log under `log/` with a version 3 intent
  header (`RSTAGE`).
- Recovery transfer frames (ops 110 and 111) use the `RXT2` magic.
- Agreed checkpoint proposal and evidence digests move to `-v2`, and the
  checkpoint capsule tag is `stroma-queue-checkpoint-capsule-v2`.

Inspection journals and phase caches from an earlier build are rebuilt. Seals,
plans and stages from an earlier build are refused, so a recovery in flight
during an upgrade must be restarted after every broker runs the new build.

## Recovery comparison

Sealed inspection can replay different exact checkpoints to a common boundary and
compare state and live payload identities. A separate lease projection accounts
for ordinary owner delivery, which does not append a lease event. Timer state,
resource incarnation and checkpoint authority still require explicit proof before
source selection. See [recovery sealing](/reliability/recovery-sealing/) and the
[failover plan](/development/failover-plan/).

## Bounded sequential inspection

Automatic sealed-history inspection uses the node-only `RecoveryReadSequential`
operation (112). Each connection owns at most one tentative cursor, bound to the
resource, partition/group, transition, complete retained-history identity and log
source. Every page receives fresh consensus authorization and seal/storage-receipt
validation. Records are checked for CRC and contiguous offsets while reading,
and the final page is withheld if the complete canonical digest differs from the seal.
The receiver independently verifies complete histories, checkpoints and replay
before producing an artifact or selecting a source. Partial pages confer no
recovery authority.

Each storage engine admits two sessions and one active sealed read. Idle sessions
expire after ten seconds without a successful recovery page. Completion, disconnect,
error and cancellation release cursor resources. Admitted blocking work retains its
own lifecycle guards until it finishes. Read buffers and pages are bounded, and
existing whole-inspection deadlines, byte/record/page and replay limits still apply.
Snapshot reads retain complete envelope verification. No payload history cache is
kept across attempts.

Strict `RecoveryRead` (105), used for diagnostics and target copying, still verifies
every retained log per page. A selected artifact can be reused within its attempt
only after verification against the persisted plan. Restart falls back to source
reconstruction or a completed target snapshot. Inspection connections are reused
only after a valid reply, and discarded on timeout, cancellation or invalid data.

This adds an internal broker opcode without changing the storage format. All
participating brokers must support operation 112 for automatic inspection. An
older peer rejects it and recovery remains fenced. Mixed-version rolling recovery
with older binaries is not supported by this change.


## Compatible retained-data reuse

A target with the exact selected log range can initialize its recovery stage
from its own sealed log. The target checks its resource incarnation and durable
seal, then reads the actual records to validate CRCs, the full payload digest and
the selected snapshot's live-payload dependencies. Unequal bounds or payloads use
the existing verified transfer path. A matching local actor state is not required, because
installation still uses the selected, independently verified snapshot.

Keratin closes the outgoing writable segment and gives each generation a new,
private active tail. Closed payload segments can share filesystem inodes, while indexes
and metadata stay independent. Installation similarly forks the completed stage.
Later suffix repair or unclean-open truncation copies a shared segment before
mutating it. Appends use the private tail, and retention only removes local names.
Logs report shared bytes, copied bytes and shared segment counts.

Sharing requires manifest version 3, persisted on the source before linking.
Older binaries reject these logs. Version 2 logs remain readable, and ordinary
unshared logs keep their existing format. A failed filesystem link falls back to
copying. Never downgrade a shared log by editing its manifest version.

The existing intent, stage completion, installation receipt and atomic route
publication still control admission. Interrupted destinations remain unreferenced.
Process-kill tests cover file sharing, private-copy repair and publication. All retained payloads and event records still require verification. Opt-in agreed
queue checkpoints can advance the retained event boundary to a common snapshot.
Comparison then verifies that snapshot and its later suffix. Live payloads retain
their ordinary integrity checks. See [checkpoint policy](/development/failover-plan/#4-agreed-checkpoints-and-suffix-comparison).

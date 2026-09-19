# Replication overlap diagnostics

A conflicting record at an existing offset remains an error. Diagnostics help
reconstruct how the two histories diverged; they do not bypass validation or
discard either history.

## Reading a report

Join the following messages by node/process, queue identity, log namespace and
`diagnostic_id`. IDs are process-local, so an ID alone is not globally unique.

1. `applying queue assignment transition` records the intended owner and epoch
   transition. It is an attempt, not proof that the transition completed.
2. `replication overlap diagnostic` contains the conflicting offset, local head,
   next and durable-next boundaries, the compared range, comparison-result
   booleans, and the most recent control operations for that log.
3. `replication overlap event coordinates` contains local events near the conflict,
   the latest local events, and incoming events near the conflict. Entries contain
   event offsets, event kinds and the highest referenced message offset, if any.
   Message-log conflicts report numeric batch coordinates instead.
4. `follower replication worker tick failed` adds the assigned source owner,
   assignment epoch and the worker's message/event continuation offsets.
5. `replication checkpoint installation starting` records the checkpoint
   requirements and proposed replacement boundaries at WARN level. A following
   `replication checkpoint installed; follower catch-up will resume` records
   successful installation at INFO level. Installation is not proof that catch-up
   has finished; check the follower's progress and in-sync status separately.

Message and event offsets belong to different logs. A control-history `offset`
is the observed boundary at the operation, not necessarily an appended record's
identity. Use the event windows for exact event offsets. Control timestamps and
sequence numbers order observations within that log; they are not a distributed
causality clock.

The diagnostic schema excludes message bodies, headers, hashes, declaration
settings, cursor names and DLQ destinations. Existing queue identity and log-root
fields identify the affected resource. Comparison booleans report which record
components differ without revealing their contents.

## Cost and limits

Each open log retains at most 32 control observations in memory. Ordinary appends
do not add breadcrumbs. Role changes, declarations, epoch changes, truncations
and checkpoint resets do. Repeated role-setting checks use an atomic swap and
only record an actual change.

Each log emits full details at most once per 30 seconds. Repeated failures keep
the same diagnostic ID, omit event scans and still return an error; the next full
report includes the number of suppressed detailed reports. Existing worker
warnings continue at the worker retry cadence.

Each event window contains at most eight entries. Event decoding is skipped for
records larger than 64 KiB. The underlying reader may still allocate the encoded
record; this is not a total diagnostic-memory cap. Reads happen only on a detailed
failure report and run on the blocking pool.

History is lost when a log handle is reopened or the process restarts. Event
windows are best effort: concurrent truncation/checkpoint replacement can make
them unavailable or change what they show. Capture the original failure before
attempting recovery.

## Recovery remains separate

Missing retained history, gaps and ExactFit batches overlapping the local tail
can already request owner-checkpoint installation. A detected content mismatch
returns an error and does not itself request a checkpoint. A later, larger batch
can nevertheless take the overlap/checkpoint path. Recovery is thus possible
after a mismatch, but is not an explicit content-conflict policy.

Checkpoint installation now preserves the source message/event epochs from both
the owner-pull path and the existing wire request. Both must match their local
log epochs before either reset is submitted. Each writer checks its expected
epoch again before clearing records or pending completions, ordered with epoch
advancement on that writer. Stale and future checkpoints are rejected; an install
cannot advance the assignment epoch. There is no wire-format change.

This fence protects individual resets from an already-advanced epoch. It does
not make the two-log reset and queue-state installation one atomic transaction,
prove that same-epoch histories agree, or establish which divergent history
contains all acknowledged messages.

An automatic divergent-follower rebuild would need a separately tested policy:
preserve evidence and alert, exclude the follower from promotion and durability
credit, establish an authoritative source under the current assignment/epoch,
install consistent message/event/state boundaries, then catch up and verify
before restoring eligibility. Recheck authority if ownership changes mid-repair.
Bound retries and escalate repeated divergence instead of continually rebuilding.

The existing checkpoint installation destructively resets logs and installs
state in several steps. Enabling it for content conflicts requires auditing
interruption/crash recovery and committed-history guarantees; a log message
alone does not make discarding a divergent history safe.

---
title: Buffered queues (AfterFlush)
description: Proposed opt-in queue mode that confirms and delivers once the required copies are written to the OS, with periodic background sync.
---

This design note records proposed work. Nothing in it is implemented as a queue
mode. Keratin has the `KDurability::AfterFlush` completion primitive and live
writer settings, but Fibril queues always confirm and deliver at fsync. Current
guarantees are in [delivery guarantees](/reliability/semantics/),
[replication](/reliability/replication/) and
[failover and recovery](/reliability/recovery-sealing/). The stream tiers that
serve as the main precedent are in
[Plexus streams](/concepts/plexus-streams/#durability-tiers). Measurements are
summarized in the [optimization log](/development/optimization-log/).

## Problem

A queue publish is confirmed, and its message becomes deliverable, only after
the owner's fsync and, with replication, after the required followers report
durable progress. On a quiet NVMe disk that fsync is a few milliseconds. Under
write load it grows, and with replication several fsyncs sit on the path.

Many workloads can accept losing the newest messages after a power loss, as long
as a process crash or the loss of one machine does not lose them. Kafka's
default is the common example: `acks=all` confirms once the in-sync replicas
have the record in their log, and fsync is left to the OS. Fibril has no queue
mode with that trade today.

The goal is an opt-in queue mode, called **written** in this note, where:

- a confirm means the required copies have written the record to the OS (page
  cache), without waiting for fsync,
- delivery follows the same boundary,
- a periodic background sync makes written records durable soon after,
- the default stays durable, and nothing about durable queues changes,
- every place that reports progress says honestly which boundary it means.

Out of scope: stream tiers (they have their own contract), a queue speculative
mode (durable confirm with early delivery, tracked as
[speculative delivery adoption](/roadmap/#immediate-rounds)), and changing
the replica count rules.

## Evidence so far

All numbers come from isolated experiments on 28 September 2026, Keratin
`cef6c35`, `a2e7745` and `fff9f79` and Fibril probe `aa72fb24`, on branch
`experiment/afterflush-delivery-20260928`. Setup: 1 KiB payloads, every node on
one NVMe ext4 filesystem, APST latency tolerance 0, no preallocation, early
replication on in both replicated arms, ABBA order with the same binary. Ranges
are per-run percentiles, not pooled histograms. Latency is intended publish
time to delivery.

| Case | Durable p50 / p99 ms | Written p50 / p99 ms |
| --- | --- | --- |
| Storage only, one append in flight | 2.535 / 5.763 (append) | 0.0147 / 0.0226 (append) |
| Solo, steady 100/s | 3.43-3.79 / 8.78-9.06 | 1.20-1.24 / 1.73-2.67 |
| 3 nodes, steady 100/s | 8.49-8.58 / 21.47-22.30 | 4.76-5.22 / 11.19-11.46 |
| 3 nodes, 200/100 bursts | 22.48-22.54 / 29.05-29.54 | 5.95-6.91 / 6.92-8.39 |
| 3 nodes, steady 10k/s | 13.82-13.95 / 30.45-30.66 | 2.42-2.50 / 88.77-96.00 |

At 10k/s the median fell but p99 rose and broker CPU per million messages went
from 97.73-98.47 to 198.47-198.93 seconds. Both weak runs had a backlog about
ten seconds in, aligned with the periodic checkpoint.

A follow-up added a per-queue background sync. At replicated 10k/s:

| Background sync | p50 ms | p99 ms | CPU s per million |
| --- | --- | --- | --- |
| None | 2.40-2.44 | 96.58-110.21 | 212.92-217.56 |
| 100 ms | 2.45-2.47 | 11.25-11.73 | 207.60-213.60 |
| 500 ms | 2.45-2.46 | 14.12-14.37 | 218.00-221.96 |
| 1,000 ms | 2.42-2.46 | 14.55-16.50 | 208.88-217.80 |

With 100 ms sync, replicated 100/s was 4.76-4.98 / 11.35-11.51 ms against
8.71-9.39 / 21.65-22.27 ms durable, and solo 100/s was 0.825-0.885 /
4.891-5.223 ms against 3.273-3.963 / 8.671-9.191 ms.

What the experiments also showed:

- Switching the default to `AfterWrite` stalled, because small appends completed
  at staging without reaching the OS, and replication and delivery read only
  durable records.
- `AfterFlush` with early replication off stalled too, since the ordinary
  replication source reads only durable records. With early replication on,
  publishes confirmed but nothing was delivered until a written payload read
  path was added.
- The probe counted written follower copies through fields named durable. That
  was a measurement shortcut and is a production blocker, not a design.
- None of these runs tested crash, power loss, failover or sustained cache
  pressure. Confirms in those runs prove nothing about power-loss durability.

## Current foundation

| Piece | State on main |
| --- | --- |
| `KDurability::AfterFlush` | Landed in Keratin `3de5c7b`. One flush per staged group, no fsync, durable frontier unchanged. A failed write latches the log. |
| Written frontier | Keratin `4830462`. `written_next` and `watch_written_next`, never below `durable_next`. A latched log moves it back to the durable end at once (`1ea4318`). |
| Written reads | `scan_written_with_byte_budget` (Keratin `1ea4318`) reads up to the written frontier from the tail cache, or from the segment files on a miss. `scan_for_replication` still falls back to a durable-only scan on a miss. |
| Buffered declaration | The mode lives in the cluster queue declaration (`QueueDeclaration` in `crates/broker/src/queue_engine.rs`), fixed at first declare. Each node mirrors it into Stroma's `set_queue_buffered`, stored as a `buffered` partition marker (Keratin `0272d81`), when it opens the queue as owner or follower. Standalone brokers set it on first declare. |
| Queue delivery reads | `read_selected` in `stroma/core/src/stroma/combined.rs` reads at the written frontier for buffered queues and at the durable frontier otherwise. Dead-letter payload fetches follow the same rule. |
| Queue apply | `append_combined` applies events only after the append completes at the requested durability, so under `AfterFlush` apply follows the flush. |
| Queue append durability | Durable queues: the log config default, `AfterFsync`. Buffered queues: `AfterFlush` for publishes, acks, nacks, releases, discards and dead-letter markers. Legacy helpers that hard-code `AfterFsync` keep it. |
| Follower apply | Push-path followers of a buffered queue append with `AfterFlush` and report written and durable positions (`QueuePushProgress` frame `FQP2`). The pull path reports one position, so it still appends with `AfterFsync`. |
| Confirm gate | `await_dependency` waits for owner completion and `nodes - 1` followers past the publish, counting written positions when the publish was admitted buffered (`QueuePublishCommit::buffered`). |
| Delivery gate | `refresh_visibility_ceiling` and the publish observer compute the ceiling from written positions on a buffered queue and durable ones otherwise. Solo queues have no ceiling because apply follows the append. |
| `replica_accepted` | Deprecated. Config maps it to `replica_durable` with a warning. |
| `replica_buffered:N` | Cluster config mode. Assignments get `replica_durable:N`, and new queues declared without a mode are buffered. Admin shows a buffered queue's policy as `replica_buffered`. |
| Early replication | Runtime setting `replication.early_replication`, off by default, history-bound assignments only. Ships written records to followers before owner fsync. Always on for buffered queues with a history-bound assignment. |
| Background sync cadence | Runtime setting `storage.background_sync_interval_ms`, applied to open queue logs and to logs opened later (Keratin `9a69acb`). |
| Seal fence | An epoch advance syncs every written record first, also when no append waits (Keratin `9a69acb`). Before that a seal of a follower holding unsynced buffered records refused. |
| Live writer settings | Keratin `e333bce`. Revision-checked changes to default durability, batch limits and fsync timers, adopted after a persistence barrier, reset on reopen. |
| Periodic sync | Keratin `5acd5ee`. Every log syncs written records in the background, 100 ms by default, 0 turns it off. Logs that expect every write synced warn if a background sync ever covers an unsynced write (`766ebd1`). Streams' ephemeral tier still syncs every 5 ms from a broker task. |
| Checkpoints | Keratin `d8f542a`. The local checkpoint, the agreed checkpoint pin and the learner export sync the log up to the captured boundary before the snapshot becomes recovery authority. |
| Latched heal | Keratin `87e24ff` and `9ddf850`. A latched log records a cut to its last successful sync in `latched-cut.pending`, and the next open carries it out first, for bound and unbound logs alike. |
| Live seal | The writer flushes, fsyncs and waits for in-flight fsyncs before advancing the epoch, so written records on a live replica are counted. |
| Dirty reopen | `sync_recovered_tail` fsyncs records found in the page cache before publishing the frontier. |

## 1. Semantics

A queue has two boundaries per record: **written** (the record reached the OS
on a copy) and **durable** (fsync completed on a copy). Confirm and delivery can
each wait for either.

| Confirm at | Deliver at | Name | Decision |
| --- | --- | --- | --- |
| durable | durable | `durable` | Current behavior and the default. |
| durable | written | `speculative` | Separate roadmap item. Same shape as the stream speculative tier. |
| written | written | `buffered` | **Proposed here.** |
| written | durable | none | Rejected. Delivery lags a weaker confirm, which is the stall the first probe hit, with no benefit to anyone. |

**Recommendation.** In a `buffered` queue, a confirm means the owner and the
required followers have each written the record to the OS, and a delivery is
released at that same boundary. Delivering before the required copies exist
would make a delivered message weaker than a confirmed one, and after failover
a consumer could have processed a message that no surviving copy holds.

Alternatives considered:

- Deliver once the owner has written, without waiting for followers. Lowest
  delivery latency, but a delivered message can vanish on owner loss alone.
  Revisit only as a combination with `speculative`.
- Confirm at written but keep a durable delivery gate. Rejected above.

### What survives

W is the configured write threshold, owner included. "Written set" means the W
copies that acknowledged a confirm.

| Event | Durable queue | Buffered queue |
| --- | --- | --- |
| Owner process crash, OS stays up | Survives | Survives. Page cache is kept and the dirty reopen syncs the recovered tail. |
| Follower process crash | Survives | Survives, same reason. |
| Owner machine lost, followers alive | Survives on W durable copies | Survives if a live member of the written set seals, because a live seal fsyncs first. |
| Power loss on one machine of the written set | Survives | Survives on the other written copies when W is 2 or more. Lost for a solo queue. |
| Power loss on every machine of the written set | Survives | Records written since the last completed sync can be lost, confirmed or not. Acks for them can be lost too. |
| Storage failure (fsync error) | Unconfirmed tail at risk | Unbound logs cut back to the last good sync, which drops confirmed written records. See section 6. |

"Simultaneous" here includes a shared rack power feed, one hypervisor host, or
all replicas sharing one disk, as in the local benchmarks above.

### Surfacing it

- Name the mode `buffered` in declare APIs, the dashboard and docs, with the
  Keratin name AfterFlush in development notes only. `durable` stays the
  default. `ephemeral` is the wrong word, since stream ephemeral confirms at
  staging and is owner-only.
- No per-message header in the first version. Every delivery from a written
  queue is pre-durable under load, so a header such as `fibril.speculative`
  would carry the queue's mode rather than news about the message. Report the
  mode in declare replies, queue info and the dashboard instead. A header can be
  added later if consumers ask to tell which deliveries were already durable.
- The user-facing warning, adapted from the experiment notes: "Buffered queues
  confirm and deliver without waiting for fsync on the required nodes. Confirmed
  messages can be lost after power loss or storage failure on every required
  node at once. Background sync usually shortens that window, but slow I/O or
  failures can extend it."

### Compared with other brokers

| System | Closest equivalent |
| --- | --- |
| Fibril streams | `ephemeral` confirms at staging, owner-only, 5 ms sync. `speculative` delivers early but confirms at fsync. |
| Kafka | `acks=all` without forced flush. Durability comes from replicas in separate failure domains. Same shape as `buffered` with W equal to the in-sync set. |
| NATS JetStream | Acknowledges after a buffered write and syncs on a timer, with an always-sync option. The optimization log tracks NATS timed-sync and always-sync as distinct profiles. |
| RabbitMQ quorum queues | Confirms after fsync on a majority. Closest to `majority_durable`. |

## 2. Configuration

**Recommendation.** A per-queue `durability` field in queue declaration, values
`durable` (default) and `buffered`, stored with the rest of the declaration in
the `fibril/queue_declaration/...` Raft attribute. It is a contract with the
publisher, so it belongs to queue state, in line with
[configuration policy](/development/config-policy/).

- Phase 1 makes it fixed at first declare. A re-declare that names a different
  mode is rejected with a clear error, not silently ignored. Streams currently
  ignore a conflicting tier on re-declare, which is worth fixing at the same
  time.
- Omitting the field on re-declare keeps the current mode, matching how other
  declaration fields merge.
- An optional cluster runtime default for **new** queues can follow later. It
  never changes existing queues.
- The replica count and policy (`replica_durable`, `majority_durable`) stay
  separate. Written changes which boundary counts, not how many copies.

Alternatives considered:

- A cluster runtime setting for all queues, as sketched in the experiment's
  policy gates. Rejected as the primary control, because a node-local or
  cluster-wide swap cannot coherently change what a publisher was promised,
  and different queues need different trades.
- Reuse `replica_accepted` as the only control. It already means "weaker than
  fsync" in the docs, but it is resolved per assignment, has no solo form, and
  behaves like `replica_durable` today, so redefining it would silently weaken
  existing configs. The per-queue mode is the primary control, and the new
  `replica_buffered:N` policy is its cluster form.

### Changing mode on an existing queue

Phase 3 adds fenced transitions, using the existing assignment, history and
process fences.

- **Durable to written.** Explicit opt-in on that queue. The owner records an
  applied revision for the new mode, publishes it, and only then admits
  publishes under the weaker rule. Earlier confirms were already durable.
- **Written to durable.** Close admission at a boundary, sync the owner and the
  required followers up to that boundary, verify their durable progress, then
  publish the new applied revision and resume. In-flight publishes keep their
  original promise or wait for the stronger one. A missing or failing replica
  leaves the transition pending, it never lowers the count. Confirms returned
  before the switch keep their weaker meaning.
- Report desired mode, applied mode per replica, and pending or failed adoption
  in queue info and the dashboard.

## 3. Progress tracking

### Keratin

- Add a public written frontier (`written_next`, exclusive, never below
  `durable_next`) and a watch for it. `subscribe_written` already fires on tail
  cache pushes, so the signal exists.
- Keep `durable_next` exactly as it is. Ordinary scans and recovery evidence
  stay durable-only.

### Stroma

- The mode is a per-partition declaration, `set_queue_buffered`, which the
  broker sets from the queue declaration, as it does for stream tiers. Stroma
  keeps it in memory, so a reopen before the broker declares it again runs
  the queue as durable, which is slower but never weaker. Queues on the
  two-log layout are refused.
- Owner writes of a buffered queue use at most `AfterFlush`: publishes, acks,
  nacks, releases, discards and dead-letter markers. Paths that request
  `AfterFsync` explicitly keep it, since a stronger request only forces an
  earlier sync. See the decisions below for why settles need no fsync.
- Delivery reads go through the gap-skipping `read_selected_with_limits` with
  a flag that picks the frontier. Buffered queues read with Keratin's
  `scan_written_with_byte_budget`, which serves the tail cache and falls back
  to the segment files below the written frontier, with the usual CRC, offset,
  type and header checks. The flag comes from the declaration, never from a
  process-wide `OnceLock` or environment variable. Since a buffered queue
  applies a record once written, its selection and its read both use the
  written frontier (audit item S2).
- Turning the mode off pauses owner operations, waits for those in flight,
  syncs the log and only then clears the flag, so no delivery selects a record
  that its read can no longer see.
- `queue_replication_next_offset` syncs a follower log that holds unsynced
  records before answering. The answer stays durable progress, and the owner
  does not need to resend a written suffix.

### Broker and wire

- Push-path followers of a buffered queue append with `AfterFlush` and report
  both positions. `QueuePushProgress` carries the written position in a new
  `FQP2` frame, sent only when it is ahead, so durable queues keep the `FQP1`
  frame older brokers read. No durable field ever carries written progress.
  An idle follower's keepalive rereads its durable frontier, which moves with
  the background sync.
- The pull path reports a single position, so it keeps appending with
  `AfterFsync`. It only runs as the fallback for catch-up and checkpoints.
- A queue never changes mode in this phase, so `QueueDependencies` keeps one
  folded frontier, computed from written positions for a buffered queue and
  durable ones otherwise. Retention keeps using durable positions.
- A buffered queue with a history-bound assignment always uses the written
  replication source. Without that binding it replicates from the durable
  source, one background sync behind. Written-source reads that miss the tail
  cache still fall back to durable-only file reads, a performance item.
- Solo buffered queues have no ceiling, as before, because apply follows the
  flush.
- Declares carry the mode as one trailing byte, only when set, and a reply
  that answers such a declare uses `FDK2` with the mode and warnings. Older
  brokers refuse a declare naming a mode instead of making a durable queue.

### Acks and settles

Acks are fire-and-forget on the wire. The broker applies a settle after its
append completes, so in a buffered queue a settle is applied once written.

After power loss on the written set, two things can happen to the unsynced
suffix. The message survives but its ack is lost, so it is redelivered, a normal
at-least-once duplicate. Or the message is lost along with its ack, so a
consumer processed a message the broker no longer has. Both fit at-least-once
for confirmed messages that survive. The second is new and must be in the docs.

Offsets after a solo cut need care. A cut back to the durable end lets new
publishes reuse offsets that were already delivered. Consumers that deduplicate
on delivery offset alone could drop a new message. Either the delivery identity
includes something that changes on a cut (a log incarnation or epoch), or the
docs tell consumers to deduplicate on their own message keys. This needs a
check of what the delivery tag contains today (open question).

## 4. Periodic background sync

**Recommendation.** A per-log background sync inside the Keratin writer, rather
than a broker task per queue as in `fff9f79`.

- When `written_next > durable_next` and the interval has passed since the last
  sync started, submit one sync through the existing fsync pipeline. At most one
  background sync is outstanding per log. Missed ticks are skipped. Idle logs do
  nothing.
- The cadence is a new writer setting, `background_sync_interval_ms`, adopted
  through the live writer settings path. Fibril drives it from a cluster runtime
  setting (for example `storage.written_sync_interval_ms`), with per-log applied
  revision reported the way segment preallocation is today. A new interval
  rearms an idle scheduler.
- The writer already knows its dirty range and can merge the background sync
  with any `AfterFsync` request that arrives, which a broker task cannot.
- Report per log: last successful sync time, durable lag in records and bytes,
  pending sync, last error. The dashboard shows the worst lag per node.
- A sync error latches the log like any other storage failure.

The interval is a target, not a bound on loss. Slow I/O, scheduling delays and
failures can stretch the unsynced window. Never document it as a maximum loss
window.

The 100 ms cadence had the best p99 in the short screen. 500 ms and one second
also helped a lot. 100 ms stays the default, see the
[October measurements](#october-measurements). Longer runs, many queues and
separate disks are still to measure.

Alternative: the broker task from the experiment. It works, but it duplicates
dirty tracking, needs its own lifecycle per queue incarnation, and cannot merge
with writer-side syncs. Keep it only as a fallback if writer changes prove hard.

### Barriers that still force syncs

- **Retention.** The experiment traced owner retention spans of 95.95 and
  92.84 ms without background sync, and 8.95 and 3.77 ms with it.
  `truncate_before_inner` on current main returns before its flush and fsync
  when nothing is deletable, so the empty case no longer forces a sync. When a
  segment is deletable it still syncs first. Remeasure on main.
- **Manifest publication.** About 11-13 ms writer spans in the trace. It runs
  inline at fsync completion unless `KERATIN_BACKGROUND_MANIFEST=1`.
- **Segment rollover.** Owner staging took 14.01 ms and followers about 25 ms
  at a rollover with background sync, against 34.58 ms and about 50 ms without.
  Rollover runs several fsyncs inline.

None of these barriers may be removed to improve the written numbers. They can
be moved off the writer thread or prepared ahead, which also helps durable
queues.

## 5. Replication and failover

- **Followers.** Append with `AfterFlush`, report written and durable progress
  separately, run the same background sync.
- **Policies.** `replica_durable:N` and `majority_durable` keep their node
  counts. On a buffered queue the count applies to written copies. The policy
  name should read naturally, so the admin view shows both, for example
  "written on 2 of 3".
- **Early replication.** Buffered queues always use the written source.
  `replication.early_replication` keeps its current meaning for durable queues.
- **Failover candidates.** New owners come from placement and source selection
  picks the sealed witness with the longest history. Neither needs to change,
  because a live seal syncs before counting. Recovery still requires E-W+1
  seals, which intersects every written set of W copies.
- **Recovery sealing.** Only durable records count, and that stays. The written
  mode depends on the live seal syncing first, which it does today. A latched
  replica cannot seal until it reopens, and the reopen cuts it back to its
  durable end first, so its seal never counts unsynced records.
- **Owner restart after power loss.** An owner that lost its unsynced suffix
  comes back with a shorter log than its followers hold. If it resumed as
  owner it would write new records at offsets the followers already hold with
  different bytes. It does not resume: a restarted process is a new replica
  instance, and history admission sends the queue through coordinated
  recovery, which fences and seals every replica and installs the longest
  history before anything is written. This holds whether the owner returns
  before the controller notices or after its queue moved, and covers early
  replication too. `crates/fibril/tests/buffered_queues.rs` checks both
  orders with a simulated power loss.

### What a client observes after owner loss

| Owner loss | Followers | Durable queue | Buffered queue |
| --- | --- | --- | --- |
| Process crash | Running | Confirmed messages kept | Confirmed messages kept |
| Machine down | Running | Kept | Kept, the live seal syncs written copies |
| Power loss, one host | Running | Kept | Kept on the other written copies |
| Power loss, whole written set | Restarting | Kept | Recent confirmed messages and acks can be lost. Recovery picks the longest surviving history |
| Owner-only (W of 1) | None | Waits for owner | Waits for owner, unsynced suffix can be lost |

## 6. Storage failure

The latched-fsync heal records a cut back to the last successful sync, and the
next open carries it out before anything reads the tail, for every log. Linux
reports a writeback error once and may drop the dirty pages, so anything past
the durable end is suspect. For a buffered queue that cut can drop confirmed
records. That matches the mode's promise only if the promise says so, which is
why the warning in section 1 names storage failure as well as power loss.

In a cluster this means:

- a latched owner stops confirming. Nothing fails it over automatically yet,
  for durable queues either, so the queue waits for the broker to restart,
  which then recovers as after a power loss,
- its reopen cuts its suffix past the durable end before it can seal, so that
  suffix never counts as recovery evidence,
- surviving written copies on followers carry the confirmed records.

Followers latch the same way and fall out of the written set until replaced or
repaired. A persistently failing follower must not silently lower W.

## 7. Checkpoints and retention

Under durable queues, applied state is always durable, so a checkpoint can
capture applied state and publish a snapshot without syncing the log. Under a
buffered queue that is no longer true. `write_exact_queue_checkpoint` captures,
writes the snapshot, then truncates. A crash after snapshot publication could
leave a recovery-authoritative snapshot that refers to records that never
reached disk.

**Decision.** Before a snapshot becomes recovery authority, sync the log up to
the captured boundary and check that `durable_next` covers it. The local
periodic checkpoint, the agreed checkpoint pin and the owner checkpoint exported
to learners all do this after capture. The pin also syncs before capture, but
a buffered write can land between that sync and the capture. The check costs
nothing when the log is already durable that far. Still to cover in phase 4:
concurrent appends, role changes, cancellation and a failed sync.

Retention:

- The truncation floor must stay at or below `durable_next` on every copy.
- The follower retention floor uses follower durable progress, not written, so
  an owner keeps enough history to repair a follower that lost its written
  suffix in a power loss.
- With background sync the durable frontier trails written by roughly one
  interval, so the retention cost is small.

## 8. CPU and memory

At 10k/s, broker CPU roughly doubled (97.73-98.47 to 198.47-198.93 CPU seconds
per million). At 100/s it fell (solo 433.04-439.71 to 293.14-306.46,
replicated 4683.54-4743.50 to 4257.16-4270.49). Background sync did not change
the high-rate cost (207.60-221.96 across intervals). Sampled RSS was lower at
10k/s (525.01-536.69 against 577.48-592.52 MiB summed), but broker RSS does not
include dirty page cache.

### October measurements

Taken on 13 October 2026 on main, with the same setup as the evidence above:
1 KiB payloads, every node on one NVMe ext4 filesystem, APST tolerance 0,
early replication on in both arms, ABBA order, the mode set by declaration.
Ranges are per run. CPU is broker CPU seconds per million messages.

| Case | Durable p50 / p99 ms | Buffered p50 / p99 ms |
| --- | --- | --- |
| Solo, steady 100/s | 3.32-4.02 / 8.47-9.37 | 1.16-1.32 / 3.55-3.78 |
| Solo, steady 10k/s | 8.62-8.98 / 20.29-20.93 | 2.06-2.08 / 3.67-4.05 |
| 3 nodes, steady 100/s | 8.82-8.92 / 22.05-23.89 | 4.72-5.18 / 11.52-14.10 |
| 3 nodes, 200/100 bursts | 30.94-32.00 / 70.08-150.53 | 8.61-9.50 / 26.61-31.15 |
| 3 nodes, steady 10k/s | 13.78-13.82 / 28.18-28.30 | 2.27-2.28 / 8.13-8.58 |

The 10k/s rows come from the build with the history cache below. Bursts have
only eight bursts per run, so their p99 is the worst burst.

The CPU gap was replication, not storage. Solo 10k/s cost about the same in
both modes (25.2-25.5 durable, 28.0-28.5 buffered), and staged batches per
message barely moved. Replicated 10k/s cost 98.5-98.6 durable against
209-216 buffered, with about twice the write syscalls per message.

A `perf` profile with frame pointers put 35% of the durable owner and 52% of
the buffered owner in `validate_history_replication`. Every push read and
progress report cloned the whole coordination snapshot and parsed the queue's
history documents from JSON. Buffered queues make more, smaller reads, so
they paid it more often. The coordination layer now caches the accepted
history per resource, keyed by the committed Raft log id, so it is rebuilt
only after a committed change. Replicated 10k/s then cost 56.9-59.4 durable
and 81.7-83.6 buffered.

What is left is per-frame cost: buffered applies return without waiting for
an fsync, so the same traffic moves in smaller groups with more frames and
progress reports. Owner samples differ by about 11% between modes after the
cache, follower samples by about 32%. A small apply linger for buffered
queues could trade some latency back for grouping, not measured yet.

Background sync interval at replicated 10k/s, buffered:

| Interval | p50 ms | p99 ms | p99.9 ms | CPU | Max unsynced records |
| --- | --- | --- | --- | --- | --- |
| 50 ms | 2.52-2.55 | 8.95-10.78 | 22.50-24.73 | 210.47-218.13 | 604-609 |
| 100 ms | 2.53-2.55 | 9.43-10.46 | 22.73-25.05 | 209.40-218.33 | 1107-1127 |
| 250 ms | 2.50-2.55 | 6.45-9.19 | 23.82-25.92 | 210.80-224.67 | 2500-2732 |
| 1,000 ms | 2.48-2.54 | 8.49-9.27 | 41.22-45.28 | 211.00-223.40 | 9967-10008 |

These ran before the history cache. The interval changes neither CPU nor the
median. The unsynced window grows with it, and 1,000 ms also worsens the
tail because each sync carries more. 100 ms stays the default: it keeps the
window near a tenth of a second at no measurable cost, and every queue log
syncs on its own timer, so a shorter default multiplies syncs on nodes with
many queues.

A control run of the 28 September binary on the same disk matched main on
steady 100/s (9.10-9.46 / 21.38-23.22 ms) but not on bursts (22.51-27.10 /
30.37-31.31 ms against 30.94-32.00 / 70.08-150.53 ms durable). That points to
a durable burst regression since then, separate from buffered queues and
still to investigate.

Not measured yet: separate drives per node, cache pressure with a small tail
cache, a many-queue screen, and a sustained run of ten minutes or more.

## 9. Acceptance test plan

Every case validates confirmed IDs, counts, delivery order, settlement and
assignment, and states which messages may be lost under the mode's promise.

**Correctness**

- Process kill (SIGKILL) of owner and followers at random points under load.
  No confirmed message lost, durable or written.
- Power-loss simulation. Use Keratin's I/O fault injection to drop unsynced
  writes at reopen, and a block layer that discards unflushed writes (for
  example `dm-log-writes` replay or LazyFS) where available. A VM hard reset is
  the closest full-system check. Expected loss is limited to records past the
  last completed sync on every copy.
- Failover: owner machine loss with live followers, owner power loss with live
  followers, whole written set power loss, owner restart after power loss with
  a shorter log.
- Storage failure: owner and follower fsync errors, `FsyncAfterSuccess`, failed
  flush, failed background sync.
- Checkpoints: crash before and after snapshot publication with unsynced
  records in the captured range.
- Transitions (phase 3): both directions under load, delayed follower adoption,
  owner replacement during transition, lost replies.
- Cache pressure: tiny tail cache budget, evicted entries, oversized records,
  long retained history, many queues sharing memory.
- Sustained load: at least ten minutes at 10k/s with checkpoints, rollovers and
  retention, watching durable lag.

**Performance**

Follow the [optimization log](/development/optimization-log/) protocol:

- ABBA order with the same binary, the mode switched by declaration.
- APST policy recorded before and after each run. An idle-gap fsync cost turned
  out to be drive power management, not broker code.
- Separate tables for comparisons that change the guarantee (durable against
  written) and comparisons that hold it fixed (sync cadence).
- Traced runs separate from clean timings, since `writer-stage-trace` adds CPU
  and memory.
- Replicated cases on separate drives as well as one shared disk.
- Report publish-to-receive and receive-to-deliver separately, plus saturation
  and a many-queue screen.

## 10. Phased plan

1. **Keratin (done).** Public written frontier and watch. Writer background
   sync with `background_sync_interval`, status and failure latch. Tests for
   idle, cancellation, merge with `AfterFsync`, and failed sync.
2. **Stroma (done).** Per-queue buffered declaration. Written-visible selected
   reads on the current read path. Settle and control-event durability audit.
   Sync before checkpoint publication. Follower resume syncs first.
3. **Broker (done).** Declare field, fixed at first declare. Written progress
   on the wire, mode-aware confirm and delivery, written source for buffered
   queues. Admin shows the mode and the queues page each partition's unsynced
   records. Runtime setting for cadence. The Rust client has
   `QueueConfig::buffered()`. The other SDKs follow.
4. **Failure acceptance (done).** Simulated power loss through Keratin's
   `lose_unsynced_on_next_open`, which cuts a closed log back to a given
   durable end the way a power loss drops unsynced writes. Covered: owner
   power loss with a quick restart and after failover (nothing confirmed is
   lost), power loss of every replica (the queue recovers from the longest
   surviving history and keeps serving), a single-node power loss after a
   checkpoint (the checkpointed state returns exactly and new records reuse
   the lost offsets), and process kills. The user page is
   [buffered queues](/reliability/buffered-queues/), marked experimental.
   An injected fsync failure on a buffered owner keeps every confirmed
   message: the owner latches and refuses new publishes, the queue fails
   over, and the old owner rejoins after cutting its log back
   (`buffered_owner_sync_failure_keeps_confirmed_messages`).
5. **Performance (done).** Matrix rerun, CPU gap traced to the per-read
   history check and fixed with a cache, 100 ms background sync kept as the
   default. See the [October measurements](#october-measurements).
6. **Transitions.** Fenced mode changes on existing queues, with per-replica
   applied status.

## Decisions and open questions

Decided:

1. Name: `buffered`, in the API, the declare field and the dashboard. AfterFlush
   stays the Keratin name in development notes.
2. Solo queues may use `buffered`. The declare reply and the docs warn that
   one power loss or storage failure can lose recent confirmed messages.
3. The cluster policy form is a new `replica_buffered:N`: a buffered queue's
   assignment confirms once N copies are buffered, and a durable queue keeps
   `replica_durable:N`. `replica_accepted` is not redefined, since existing
   configs that name it behave like `replica_durable` today and would silently
   weaken. It stays an alias for `replica_durable`, logs a deprecation warning,
   and is removed in a later release.
4. Background sync defaults to 100 ms and may be turned off with 0, which
   leaves syncs to the operating system and removes any bound on the unsynced
   window. The settings page and docs say so plainly.
5. Settled: delivery tags come from a per-process counter, map to an offset
   only inside the broker, and settle only on the subscription that holds
   them (crates/broker/src/broker.rs, `records_by_tags`). A restart resumes
   sessions as `ResumedAfterRestart`, which the clients treat as a new
   incarnation, so held deliveries settle to `StaleDelivery` and redeliver.
   Reused offsets after a cut therefore cannot misroute a settle. An
   in-process reopen of a buffered queue's log must drop that queue's tag
   records first, as ownership transitions already do.
6. Settled by the latched heal: a latched log cannot seal, and its reopen cuts
   back to its durable end before it can.

7. Every owner write of a buffered queue completes at `AfterFlush`,
   settles and lease expiry included. A cut back to the last sync always
   leaves a prefix of the log, so a lost settle moves the queue back to an
   earlier consistent state and the message is delivered again, which fits
   at-least-once. A dead-letter copy lands in the target queue under the
   target's own mode. A durable source with a buffered dead-letter queue can
   therefore lose a dead-lettered message on power loss, as any buffered
   publish can.

8. Where the mode lives: in the cluster queue declaration, mirrored into
   each node's storage when it owns or follows the queue. Ganglion's
   assignment policy enum is unchanged, so no Raft format change. The
   assignment's `replica_durable:N` count applies to written copies on a
   buffered queue.
9. `replica_buffered:N` as the cluster setting means `replica_durable:N`
   copies with buffered as the default for new queues, which also settles
   whether a cluster default for new queues exists. A declare naming a mode
   wins.
10. Background sync stays at 100 ms by default. The October runs found no CPU or
    median cost between 50 ms and 1,000 ms, a worse tail at 1,000 ms, and a
    window that grows with the interval.

Still open:

1. Should Fibril reserve `speculative` for the queue form of early delivery so
   the queue and stream tier names line up?
2. Should buffered queues carry a per-delivery header after all?
3. Should the stream ephemeral tier move from `AfterWrite` to `AfterFlush` so
   its documented process-crash promise holds?

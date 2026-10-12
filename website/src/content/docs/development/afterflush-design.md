---
title: Written-durability queues (AfterFlush)
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
| Written reads | `scan_for_replication` serves written records from the tail cache and falls back to a durable-only scan on a miss (`keratin-log/src/reader.rs`). No public written frontier accessor, `flushed_through` is private. |
| Queue delivery reads | `read_selected` in `stroma/core/src/stroma/combined.rs` is durable-fenced on both cache and disk paths. |
| Queue apply | `append_combined` applies events only after the append completes at the requested durability, so under `AfterFlush` apply would follow the flush. |
| Queue append durability | Derived from the log config default (`queue_event_log_config`), which is `AfterFsync`. Some paths pass the event log default explicitly (release, nack, DLQ commit) and a few hard-code `AfterFsync`. |
| Follower apply | Fibril requests `AfterFsync` for follower appends (`queue_apply_batch` in `crates/broker/src/replication.rs`). Followers report `DurableProgress::Queue { next }` through `QueuePushProgress`. |
| Confirm gate | `await_dependency` waits for owner completion and `nodes - 1` followers with durable progress past the publish. |
| Delivery gate | `refresh_visibility_ceiling` sets the replica-durable ceiling from follower progress. Solo queues have no ceiling because apply already implies durable. |
| `replica_accepted` | Parsed and resolved, but the `acknowledgement` field is not read at runtime, so it behaves exactly like `replica_durable`. |
| Early replication | Runtime setting `replication.early_replication`, off by default, history-bound assignments only. Ships written records to followers before owner fsync. Confirm and visibility still use durable progress. |
| Live writer settings | Keratin `e333bce`. Revision-checked changes to default durability, batch limits and fsync timers, adopted after a persistence barrier, reset on reopen. |
| Periodic sync | None for queues. With only `AfterFlush` traffic the writer never schedules an interval fsync, because a commit needs a pending fsync waiter. Streams' ephemeral tier syncs every 5 ms from a broker task. |
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
| written | written | `written` | **Proposed here.** |
| written | durable | none | Rejected. Delivery lags a weaker confirm, which is the stall the first probe hit, with no benefit to anyone. |

**Recommendation.** In a `written` queue, a confirm means the owner and the
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

| Event | Durable queue | Written queue |
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

- Name the mode `written` in declare APIs, the dashboard and docs, with the
  Keratin name AfterFlush in development notes only. `durable` stays the
  default. `ephemeral` is the wrong word, since stream ephemeral confirms at
  staging and is owner-only.
- No per-message header in the first version. Every delivery from a written
  queue is pre-durable under load, so a header such as `fibril.speculative`
  would carry the queue's mode rather than news about the message. Report the
  mode in declare replies, queue info and the dashboard instead. A header can be
  added later if consumers ask to tell which deliveries were already durable.
- The user-facing warning, adapted from the experiment notes: "Written queues
  confirm and deliver without waiting for fsync on the required nodes. Confirmed
  messages can be lost after power loss or storage failure on every required
  node at once. Background sync usually shortens that window, but slow I/O or
  failures can extend it."
- Fix the stream docs while touching this area. The ephemeral tier is described
  as surviving a process crash, but it confirms at `AfterWrite`, which can
  complete before the bytes leave Keratin's buffers.

### Compared with other brokers

| System | Closest equivalent |
| --- | --- |
| Fibril streams | `ephemeral` confirms at staging, owner-only, 5 ms sync. `speculative` delivers early but confirms at fsync. |
| Kafka | `acks=all` without forced flush. Durability comes from replicas in separate failure domains. Same shape as `written` with W equal to the in-sync set. |
| NATS JetStream | Acknowledges after a buffered write and syncs on a timer, with an always-sync option. The optimization log tracks NATS timed-sync and always-sync as distinct profiles. |
| RabbitMQ quorum queues | Confirms after fsync on a majority. Closest to `majority_durable`. |

## 2. Configuration

**Recommendation.** A per-queue `durability` field in queue declaration, values
`durable` (default) and `written`, stored with the rest of the declaration in
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
- Reuse `replica_accepted`. It already means "weaker than fsync" in the docs,
  but it is resolved per assignment, has no solo form, and does nothing today.
  Better to define the persistence boundary explicitly and then either retire
  `replica_accepted` or map it onto `written` (open question).

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

- A queue log carries its mode in config. Publish and settle appends use it.
  Audit the paths that pass `keratin_cfg_event.default_durability` or hard-code
  `AfterFsync` (release, nack, DLQ commit, leased appends) and decide each one.
  A stronger request on a written queue is always allowed and simply forces an
  earlier sync.
- Delivery reads need a written-visible selected read, as `a2e7745` did: serve
  from the tail cache, fall back to an exact raw read of each selected offset on
  a miss, keep CRC, offset, type and header checks. It must be rebuilt on the
  current `read_selected_with_limits` gap-skipping code and selected only for
  written queues, never through a process-wide `OnceLock` or environment flag.
  The selection bound and the read source must use the same frontier (audit
  item S2).
- `queue_replication_next_offset` refuses unless the log is fully durable. For
  written followers it should resume from the durable frontier and let the
  owner resend the written suffix, which the `AfterFlush` overlap check already
  verifies byte for byte.

### Broker and wire

- Followers of a written queue append with `AfterFlush` and report both
  `written_next` and `durable_next`. `QueuePushProgress` gains the written field.
  No durable field ever carries written progress.
- `QueueDependencies` keeps two ceilings per queue: written visibility and
  durable visibility. `await_dependency` and the delivery gate pick the one
  matching the queue mode. Retention, checkpoints and diagnostics use durable.
- A written queue always uses the written replication source, whatever
  `replication.early_replication` says. With a durable-only source it cannot
  make progress, as the first probe showed. Early replication stays a separate
  switch for durable queues.
- Solo written queues have no ceiling, as today, because apply follows the
  flush.

### Acks and settles

Acks are fire-and-forget on the wire. The broker applies a settle after its
append completes, so in a written queue a settle is applied once written.

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
also helped a lot. Choosing a default needs longer runs, more queues and
separate disks.

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
  counts. On a written queue the count applies to written copies. The policy
  name should read naturally, so the admin view shows both, for example
  "written on 2 of 3".
- **Early replication.** Written queues always use the written source.
  `replication.early_replication` keeps its current meaning for durable queues.
- **Failover candidates.** New owners come from placement and source selection
  picks the sealed witness with the longest history. Neither needs to change,
  because a live seal syncs before counting. Recovery still requires E-W+1
  seals, which intersects every written set of W copies.
- **Recovery sealing.** Only durable records count, and that stays. The written
  mode depends on the live seal syncing first, which it does today. A latched
  replica cannot seal until it reopens, and the reopen cuts it back to its
  durable end first, so its seal never counts unsynced records.
- **Owner restart after power loss.** This is the new hazard. An owner that lost
  its unsynced suffix can come back with a shorter log than its followers hold.
  If it resumed as owner it would write new records at offsets the followers
  already hold with different bytes. A written queue must treat a dirty
  restart whose log is shorter than reported follower progress as a recovery
  event: fence, seal and select the longest history. Early replication has the
  same open item for old-owner restart.

### What a client observes after owner loss

| Owner loss | Followers | Durable queue | Written queue |
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
the durable end is suspect. For a written queue that cut can drop confirmed
records. That matches the mode's promise only if the promise says so, which is
why the warning in section 1 names storage failure as well as power loss.

In a cluster this means:

- a latched owner stops confirming, and the queue fails over,
- its reopen cuts its suffix past the durable end before it can seal, so that
  suffix never counts as recovery evidence,
- surviving written copies on followers carry the confirmed records.

Followers latch the same way and fall out of the written set until replaced or
repaired. A persistently failing follower must not silently lower W.

## 7. Checkpoints and retention

Under durable queues, applied state is always durable, so a checkpoint can
capture applied state and publish a snapshot without syncing the log. Under a
written queue that is no longer true. `write_exact_queue_checkpoint` captures,
writes the snapshot, then truncates. A crash after snapshot publication could
leave a recovery-authoritative snapshot that refers to records that never
reached disk.

**Recommendation.** Before a snapshot becomes recovery authority, sync the log
up to the captured boundary and check that `durable_next` covers it. The agreed
checkpoint pin already syncs before capture. The local periodic checkpoint needs
the same step. Cover owner and follower capture, concurrent appends, role
changes, cancellation and a failed sync.

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

Likely causes, none attributed yet:

- **Smaller groups.** The weak runs staged about 26.1-26.6k batches against
  19.1-19.3k for the same 150,001 messages, and 161.6-161.8k staged records
  against 155.6-155.7k. An fsync wait batches naturally. Without it, each wake
  ships a smaller group through staging, flush, replication and apply.
- **More rounds downstream.** Smaller groups mean more replication frames,
  progress reports, ceiling refreshes, delivery polls and confirm wakeups.
- **Known per-message costs** that the weak mode exposes: per-connection
  publisher sinks, per-message confirm dependency waits, repeated event codecs
  and payload copies (audit items P3 to P5).
- **Fixes since the experiment.** The writer no longer spins at the fsync cap
  (Keratin `561cbb1`), and selected reads skip unread records by length
  (`c995932`). Rerun before attributing.

What to measure on the rebuilt mode:

- CPU per message, plus write and fsync syscalls per message from `strace -c`
  or eBPF counters.
- Staged batches and records per message on owner and followers.
- Replication frames and progress reports per message.
- Tail cache hit rate for written delivery reads, and raw fallback reads.
- Dirty and Writeback from `/proc/meminfo`, and durable lag per log.
- A `perf` profile at 10k/s in both modes.
- Whether a small linger for written queues restores grouping without giving
  back the median.

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

1. **Keratin.** Public written frontier and watch. Writer background sync with
   `background_sync_interval_ms`, status and failure latch. Tests for idle,
   cancellation, merge with `AfterFsync`, and failed sync.
2. **Stroma.** Per-queue mode in log config. Written-visible selected reads on
   the current read path. Settle and control-event durability audit. Sync before
   checkpoint publication. Follower resume from the durable frontier.
3. **Broker.** Declare field, fixed at first declare. Written progress on the
   wire, two ceilings, mode-aware confirm and delivery, written source for
   written queues. Admin and dashboard show mode, written and durable progress,
   and durable lag. Runtime setting for cadence.
4. **Failure acceptance.** Section 9 correctness cases, including owner restart
   after power loss. Docs for the mode, warning text and consumer guidance.
   Ship as an experimental opt-in.
5. **Performance.** Rerun the matrix, profile the CPU cost, decide a default
   cadence.
6. **Transitions.** Fenced mode changes on existing queues, with per-replica
   applied status.

## Open questions

1. Name: `written` or something else (`flushed`, `relaxed`)? Should Fibril
   reserve `speculative` for the queue form of early delivery so the queue and
   stream tier names line up?
2. Should a solo queue be allowed to use `written`, given that one power loss
   can lose confirmed messages?
3. What happens to `replica_accepted`: retire it, or redefine it as written
   acknowledgement on N nodes?
4. Default background sync interval, and whether operators may turn it off.
5. Should written queues carry a per-delivery header after all?
6. Does the delivery identity survive a solo cut without offset reuse
   confusion, or must the cut bump an incarnation that clients see?
7. Which queue events keep `AfterFsync` in a written queue: DLQ moves, delayed
   retry promotion, lease expiry?
8. Settled by the latched heal: a latched log cannot seal, and its reopen cuts
   back to its durable end before it can.
9. Should a cluster runtime default for new queues exist at all, or only the
   declare field?
10. Should the stream ephemeral tier move from `AfterWrite` to `AfterFlush` so
    its documented process-crash promise holds?

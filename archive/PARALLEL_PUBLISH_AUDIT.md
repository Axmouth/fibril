# Parallel Durable Publish — Correctness Audit (OPEN)

Working doc for the `/code-review` findings on the parallel-durable-publish arc
(keratin `git diff fb066de..HEAD`). Persist here until resolved.

**Status: ALL RESOLVED, merged to keratin `main` (through `e0fe575`) + fibril `main`.**
Latency re-check post-findings matched the pre-findings numbers (no regression).

**Detail:**
- **#1 FIXED** (option A, per-partition publish-order lock).
- **S2 RESOLVED** — analyzed benign + documented (head-of-line stop is correct and
  self-healing; nothing durable sits behind a non-durable offset).
- **S3 FIXED** — the compensating cancel append logs on failure instead of swallowing.
- **#4 FIXED** — fusion attributes the fdatasync cost to one request, no stats inflation.
- **#5 FIXED** — the fusion comment now states the real invariant (roll fsyncs synchronously).
- **#7 MITIGATED** — a non-enqueue dangling reference now logs loudly (still truncates;
  promoting to a hard quarantine is a possible further hardening).
- **#6 FIXED** — `max_inflight_fsyncs` and `pipeline_commit_records` are now
  `KeratinConfig` fields (the fsync channel is sized from `max_inflight_fsyncs`, so the
  bound and the gate stay coupled), exposed through `storage.keratin.*` config and
  `FIBRIL_KERATIN_MAX_INFLIGHT_FSYNCS` / `FIBRIL_KERATIN_PIPELINE_COMMIT_RECORDS` env.

The arc itself (parallel publish + CancelEnqueue recovery, adaptive fsync fusion,
segment preallocation, DurableFrontier watermark) is committed on `main` in keratin
(`809080d` tip) + fibril. The prealloc/frontier/fusion parts reviewed clean; the
findings below are in the **parallel-publish design** (commit `89e5b11` onward).

## #1 — CRITICAL (verified) — FIXED on branch `fix-parallel-publish-reorder`

**Resolution (option A — prevent the reorder):** a per-partition `tokio::Mutex`
(`QueueHandleInner.publish_event_order`) held across `stage -> receive assigned offset
-> event-log SEND` (via the synchronous `append_batch_enqueue_receiver`), released
before the durability waits. Offset assignment and the event append now happen in ONE
critical section per partition, so the event log is always in msg-offset order. Only the
cheap staging step is serialized; the two fsyncs still overlap (the durability `join!`
runs in a spawned task outside the lock). Correctness comes from mutual exclusion, not
lock fairness. `gate_and_apply_enqueue` replaced `append_events_durable_msg_gated`
(takes the pre-sent event receiver instead of sending).

**Validation:** `reorder_probe.rs` was ~4.9% out-of-order before, 0 after (x3 at N=5000);
full stroma-core suite green (205+); e2e failover crash test PASS (SIGKILL owner mid-run
over preallocated segments, LOSS=0 PHANTOM=0); throughput A/B (fix vs baseline) free
across 500/s->1M/s on tmpfs+nvme, mixed + publish-only (within noise, sometimes faster).
Bonus finding en route: the broker delivers ~1.12M/s end-to-end on one partition (tmpfs)
with process-per-reader; the ~800k single-client number was the steady_c process, not the
broker.

---

### Original analysis (for the record)



**Where:** `stroma/core/src/stroma.rs` `append_message_batch` (parallel path, ~3908)
+ `append_events_durable_msg_gated` (~2397) + the recovery fold (~3208).

**Root cause chain (verified against code):**
1. `append_message_batch` acquires `begin_owner_operation()` — this is a **counter**
   (`state.rs:1385`, `owner_operations.fetch_add`), NOT a serializer. Concurrent
   publishes run fully in parallel.
2. It stages msgs via `msg_log.append_batch_enqueue_staged(...)` (serial msg writer
   assigns offsets in call order) then **`tokio::spawn`s a detached task** and returns.
3. The spawned task awaits `staged_rx` (the assigned base offset), then calls
   `append_events_durable_msg_gated`, which does
   `let event_fut = event_log.append_batch(msgs, ..)` + `tokio::join!(event_fut, msg_barrier)`.
   `append_batch` is `async fn`, so the **send to the event-log writer happens on
   first poll of `event_fut` at the join!** — inside the task's run-slice, right after
   `staged_rx.await`, with no `.await` between.
4. Nothing orders the two spawned tasks. Task A (offsets 5,6,7) is woken before task B
   (8,9) — staged_tx fires in offset order on the serial msg-writer thread — but the
   wakes land in tokio's global queue from a non-worker thread, and if two workers pick
   A and B up concurrently they race from wake to the event-log send. A has only a
   ~one-staging-op head start. **No ordering guarantee** → event log can be
   `[EnqueueMany{8,9}, EnqueueMany{5,6,7}]`.

**Loss scenario (internally consistent):**
- msg_tail = 8 (offsets 0..7 durable, 8+ lost). So A's msgs 5,6,7 durable, B's 8,9 not.
- A confirmed = A's event durable + A's msgs durable. B = crash-at-seam (event durable,
  msgs not, cancel not yet written).
- Event log reordered `[B{8,9}, A{5,6,7}]`, both events durable.
- Fold: i=0 `{8,9}` → pending={8,9}; i=1 `{5,6,7}` (all < msg_tail, skipped) → pending
  still non-empty → `valid_len=0` → **truncate all, including A's confirmed enqueue**.
- A's msgs are durable in the msg log but their enqueue is dropped → never delivered.
  Breaks `confirmed ⇒ delivered`.
- In correct order `[A, B]` the fold keeps A (valid_len=1) and drops only B. So the
  reorder is exactly what breaks it. The fold comment ("non-durable enqueues form a
  contiguous event-log tail", stroma.rs:3204) assumes event order == msg-offset order,
  which the parallel path does not guarantee.

**Why tests miss it:** happy-path test uses ONE batch (no cross-batch concurrency);
failover_verify checks loss via the FOLLOWER (replication), not single-node recovery of
a reordered log after a crash-at-seam.

**MEASURED (stroma/core/tests/reorder_probe.rs, worker_threads=8, N=1000 concurrent
single-msg publishes):** 22 adjacent inversions, **49 of 1000 events out of msg-offset
order (~4.9%)**. The reorder is common, not a rare theoretical race. (Loss still needs
the additional crash-at-seam-losing-a-middle-publish to trigger.)

## Fix options (design fork — pending decision)

The deciding invariant: recovery must be able to drop dangling enqueues WITHOUT dropping
a confirmed one. Two ways to satisfy it:

**A. PREVENT the reorder (recommended).** Append event-log entries in msg-offset order
per partition, so dangling enqueues really do form a contiguous tail and the existing
suffix-truncation fold is correct. The reorder exists because `append_message_batch`
spawns an INDEPENDENT task per publish (each `staged_rx.await` then races to
`event_log.append_batch`). Fix: a per-partition ordered pipeline — the staged signals
feed an ordered channel, a single consumer appends events in that order (non-blocking
sends preserve order), and hands the durability-gate + confirm/cancel off to concurrent
tasks so the two fsyncs still OVERLAP (the whole point of the arc is kept). Localized to
the stroma publish path; does not touch replication or the fold. Also incidentally fixes
the S2 reasoning (dangling entries stay at the tail).

**B. IMMUNE fold (order-independent).** Drop dangling enqueues selectively wherever they
sit, keeping all other events. Safe because a dangling enqueue's msgs are non-durable ->
never applied -> no downstream Ack/Nack references it. BUT it breaks the "event log is a
clean prefix" model: either leave dangling entries physically and skip-on-replay (log
accrues cruft over crashes, and replication/snapshot must also skip them) or rewrite the
log without them (expensive, offset/replication churn). Wider surface (recovery +
replication + snapshot must all agree), so higher risk.

Recommendation: **A**. Restoring event-order == msg-order is the invariant the whole
design already assumes; it is the smaller, more local change and keeps the fsync overlap.

**Fix direction (not yet applied):** sequence the event-log appends in msg-offset order
per partition (preserving the two-fsync overlap, just ordering the event append), OR make
the fold order-independent (drop only the dangling enqueues, not the whole suffix — riskier,
breaks prefix model). Prefer ordering the appends. Add a regression test: concurrent
publishes + crash at the seam losing a middle publish, assert the confirmed one survives.

## #2 — SEVERE (plausible): replication head-of-line stall on a dangling EnqueueMany

**Where:** fibril `crates/broker/src/replication.rs` — `is_event_available`
(`reqs.iter().all(|req| req.off < message_frontier)`, ~847) + head-of-line `break` in
`cap_owner_event_read_to_message_frontier` (~824).

On a **persistent** msg-fsync failure (`Ok(None)`), the leader durably records
`EnqueueMany{5,6,7}` then `CancelEnqueueMany{5,6,7}`, but msgs 5,6,7 never become durable
so `message_frontier` sticks at 5. The gate blocks the `EnqueueMany` (`all(off<5)` false)
and head-of-line-`break`s; the resolving `CancelEnqueueMany{5,6,7}` is also `>= frontier`
and blocked → the follower stalls for that partition forever. Requires a persistent
last-batch msg-fsync failure (rarer than #1).

## #3 — robustness: compensating cancel is fire-and-forget

`stroma.rs:3958` `let _ = stroma.append_events_durable_leased(qh, vec![cancel], ...)`.
If the cancel append fails, the dangling `EnqueueMany` persists on a live leader → feeds
#2. Should log + retry rather than drop.

## Medium / lower

- **#4 (metrics):** fusion reports the single fdatasync's `elapsed` to every drained
  request → `stats.fsync` inflated ~N×. `writer.rs` fsync_loop → `finish_fsync_job`.
  Fix: attribute elapsed to one request, `Duration::ZERO` to the rest.
- **#5 (fragility, 3 agents):** fsync fusion across a segment roll is safe only because
  `roll()` fsyncs synchronously before swapping `self.active`; the comment ("requests
  share the log file") is wrong across a roll and the real invariant is undocumented.
  Fix: correct the comment + assert shared segment identity (or drain in-flight before roll).
- **#6 (convention):** `MAX_INFLIGHT_FSYNCS=8` and `PIPELINE_COMMIT_RECORDS=2048` are
  hardcoded consts vs settings-discipline (should be `KeratinConfig`, like
  `segment_preallocate_bytes`).
- **#7 (safety net):** `DanglingReference` now always auto-truncates (warn-only); the
  quarantine fallback is gone. Safe for the Enqueue artifact (dangling ⇒ unconfirmed),
  but a non-Enqueue dangling ref (Ack/Nack, from a hypothetical accounting bug) would be
  silently dropped. `referenced_msg_offsets` DOES include Ack/Nack/MarkInflight/etc.
- **#8 (latent):** `durable_end_exclusive` derives from the still-inclusive `durable_offset`
  (`+1 .min(next_offset)`); correct only because called post-fsync. Add a debug-assert
  `durable_offset+1 <= next_offset`.
- **minor:** `reqs.last().expect("at least one request")` in fsync_loop (provable invariant,
  but literal no-expect violation); a few semicolons in new doc comments (pervasive in
  codebase, deprioritized).

## Cleared (checked, no bug)
Frontier arithmetic (empty / offset-0 / boundary), lazy `offs` rebuild matches assigned
offsets, prealloc crash-safety (scan stops at zero padding, trim-then-clean ordering),
reader durable-bound (net fix), CancelEnqueueMany encode/decode + both apply arms + no
silent `_=>` skip in fibril, LogState.durable type change has no raw-atomic callers, all
Log::open paths thread prealloc, the /simplify cleanups.

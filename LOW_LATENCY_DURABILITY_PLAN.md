# Low-latency durable publish plan

Cut the single-node durable publish->confirm/deliver latency floor on real
(non-tmpfs) storage from ~15-20ms toward ~4-6ms, without weakening durability or
the crash-recovery guarantees.

## The investigation (how we got here)

Bench: `steady_c --mode mixed --confirmed` at low rate on the nvme scratch
(`/run/media/george/ubuntubuntubuntu`), 4 writers / 4 readers, 1 KB payloads.
`steady_c` reports four latencies that decompose the path: publish->server-receive,
publish->confirmation (write to durable), server-receive->deliver, publish->deliver.

Findings, each with the datum that established it:

- **The floor is entirely publish->durable.** `confirm p50 == deliver p50 == 20ms`
  and `publish->server-receive p50 = 0`. Delivery adds ~0 over confirmation.
- **Not the batch linger.** Sweeping `batch_linger_ms` 5 -> 2 -> 1 left the floor at
  20ms (adaptive linger already collapses to ~0 at low load).
- **Not throughput queuing.** Sweeping rate 500 -> 2000 -> 5000 /s: floor is
  ~15ms at 500/s (one publish per 2ms, far under the ~1150 fsync/s ceiling), rising
  only slightly with load. So it is a fixed per-commit cost, not a serial-fsync
  backlog.
- **Not the replication confirm gate.** For `local` durability `await_confirm`
  returns immediately (`required_followers == 0`).
- **Not a periodic tick / scheduling.** tmpfs shows p50=2ms with the identical code,
  hops, and (at low load) batch sizes. A time-based tick or a hand-off/scheduling
  cost would show on tmpfs too. It does not. => the ~13ms delta is the fsync cost.

Direct fsync probe on the nvme scratch (`scripts/fsync-probe.py`):
`fdatasync` p50=0.67ms, `fsync` p50=2.43ms (syncs metadata, 3.6x slower), max 16.6ms.

## Root cause

A single publish costs **~4 serialized fdatasyncs**:

1. `keratin sync()` (log.rs:89) does **two** fdatasyncs per commit: `.log` then `.idx`.
2. A publish is **two serial durability round-trips**. `stroma.rs` `MsgBatchCompletion`
   is an `AppendCompletion` that fires only on **msg-log durability**, then
   `runtime.spawn`s the event-log append (`EnqueueMany`), and the client confirmation
   fires only after **event-log durability**. So: msg-log durable -> spawn hop ->
   event-log durable -> confirm.

**Correction (measured 2026-07-07, supersedes the "device-bound fsync" guess):**
- The `.idx` fdatasync is NOT a meaningful cost. A/B skipping it saved ~1ms (500/s:
  15->14, 5k/s: 20->18). The index is sparse (one entry per `index_stride_bytes` =
  64KB), so most commits add no index bytes and its `sync_data()` is a near-no-op.
  => Dropping the `.idx` sync is not worth a latency-driven Phase 1.
- The raw fdatasync is NOT slow under concurrency. Concurrent fsync-probe streams on
  the nvme: p50 stays ~0.67 -> 0.70 -> 0.78ms at 1/2/4 streams (only p99 tail climbs
  2.5 -> 4.2 -> 7.9ms). The device has median headroom, so it is NOT device-bound.
- Therefore the ~15ms floor is **structural, not the fsync syscall** (<1ms even
  concurrent): the two serial durability round-trips' machinery - commit cadence, the
  `runtime.spawn` hop between msg-log and event-log completion, and the multi-thread
  notify chain (writer -> fsync worker -> writer -> notifier -> tokio task, x2). A real
  0.7ms fsync wait lets those threads park between hops, and the wakeup/scheduling
  latency stacks. tmpfs keeps them hot (instant fsync) -> 2ms.
- **CONFIRMED it is the two serial round-trips (parallel-fsync A/B, 2026-07-07).** A
  throwaway hack that gets the msg offset at staging and fires the event-log append
  immediately (both fsyncs overlap, `join!` on both-durable) ~halved the floor:
  confirm p50 500/s 15 -> 8ms (-47%), 5k/s ~19 -> 13ms (-40%); deliver tracks it. The
  same run leaked exactly the predicted correctness gaps (receive rate 498/500 and
  4959/5000, a 30s max outlier) because the hack applies events in-memory before msg
  durability with no CancelEnqueue - which is why the safe design below is required, not
  optional. => The structural cost is the serialization itself, and parallelizing is the
  fix. `.idx`-drop Phase 1 is dropped (not worth it for latency).

## Why the serialization exists (the invariant we must keep)

Events are written **after** msg-log durability so the event log never references a
non-durable message payload. If a durable `EnqueueMany(O)` referenced a msg offset
`O` whose payload was not durable, recovery would see a **dangling reference** ->
today that **quarantines the partition** (default `RecoveryMismatchPolicy::Quarantine`,
stroma.rs:927). The serialization is what makes a dangling reference "supposed to be
impossible" in normal operation.

Existing recovery machinery (stroma.rs:795):
- `RecoveryMismatchKind::DanglingReference { msg_offset, msg_tail }` - an event refers
  to a msg offset the msg log has not durably accepted ("only possible as a lost-tail
  suffix, since events are written after their messages").
- Recovery scan returns the valid **prefix** up to the first dangling ref + the mismatch.
- Policy application (stroma.rs:3240): `Ignore` auto-truncates the dangling suffix and
  continues; `Quarantine` (default) / `Refuse` stop and require a manual `repair()`
  (stroma.rs:1316, truncate-to-valid).

## The design (three coupled changes)

The safety anchor throughout: **confirm the producer only when BOTH logs are durable.**
Therefore any `EnqueueMany(O)` lacking a durable payload is necessarily **unconfirmed**
- the producer got no ack and retries - so dropping or truncating it loses nothing
acknowledged. Keratin's durable watermark is monotonic and contiguous (an fdatasync
makes everything <= `through_offset` durable), so a dangling reference is **always a
clean suffix** above `msg_tail`.

### Phase 1 - Drop the per-commit `.idx` fsync (independent, do first)

- `keratin sync()` stops fdatasyncing `.idx` per commit. Keep fsyncing `.idx` on clean
  shutdown and on segment rotation (both rare).
- On a **dirty** open (crash: manifest clean flag clear), rebuild only the `.idx`
  **tail** past the last durable index entry by scanning the `.log` tail.
- Uses the existing keratin **manifest clean-shutdown flag** as-is: clean shutdown
  already flushes+fsyncs everything before setting the bit, so clean => `.idx`
  consistent; dirty => rebuild tail. No new marker.
- Removes ~1 fdatasync per commit (2 per publish). Independent of Phases 2-3.

### Phase 2 - Specialized cancel event + runtime compensation

- New event variant `CancelEnqueue(offset)` (name TBD) - a dedicated "this offset
  never committed, annihilate its enqueue." Preferred over overloading `Ack`
  (lease/consumer semantics) or `DeadLetter` (DLQ routing). New event variant + codec
  + one fold arm.
- **Runtime**: when the msg-log fsync fails but the process survives, and an
  `EnqueueMany(O)` is already durable, write `CancelEnqueue(O)` (best-effort) so the
  **live** derived state stays consistent (O not deliverable) and the log carries its
  own compensation. Producer is nacked -> retries.
- Best-effort caveat: a disk-level failure may fail the `CancelEnqueue` fsync too. The
  Phase-3 recovery path is the backstop for that (and for plain crashes, where the
  process dies before writing the cancel).

### Phase 3 - Parallel fsync + fold-then-validate recovery

- **Perf**: append the event off the msg-log **append** result (offset `base + i` is
  known at append time, not durability time), issue **both fsyncs concurrently**, and
  confirm the producer on **both-durable** (`max`, not `sum`). Roughly halves the floor.
- **Recovery reordering (the load-bearing part)**: fold the event log to its **net
  final state first** (so `EnqueueMany(O)` + `CancelEnqueue(O)` annihilate), **then**
  validate only the **surviving** enqueues against the msg-log durable tail. A
  compensated pair never reaches the msg-log check -> no dangling -> no quarantine.
- **Reclassify the benign shape**: a surviving dangling reference that is a contiguous
  suffix starting at `msg_tail + 1` is the **expected** crash artifact (like keratin
  already silently truncating its own unfsynced log tail). Treat it as ordinary
  lost-tail truncation - unconditional, not policy-gated. Only an **anomalous** shape
  (a gap, a mid-log dangling ref, or corruption) falls through to `Quarantine`/`Refuse`,
  so the policy still catches real corruption.
- **Cost note (verified 2026-07-07)**: `recover_events_from_log` (stroma.rs:3081) already
  runs on every partition open, bounded **snapshot -> tail**, to rebuild in-memory state,
  and the dangling check (`max_ref >= msg_tail`, stroma.rs:3125) rides along it as one
  comparison per event. So fold-then-validate **restructures this existing bounded replay**
  (fold cancels, validate survivors, reclassify the clean suffix) rather than adding a new
  scan - snapshots already bound the cost, no separate clean-flag gate needed here. The
  clean/dirty flag's real job is the **Phase-1 `.idx` rebuild**. `truncate_event_log_tail`
  (stroma.rs:3066, via follower-gated `destructive_reset_to_checkpoint`) is the existing
  suffix-drop mechanism to reuse.

## Correctness invariant (the one thing all tests assert)

`confirmed  <=>  both logs durable  <=>  delivered (at least once)`
`unconfirmed  =>  may or may not survive, but NEVER corrupts, phantom-delivers, or quarantines`

This matches what `benches/bin/failover_verify.rs` already asserts at the cluster level
(loss = confirmed absent from received MUST be empty; phantom MUST be empty;
unconfirmed-delivered is fine).

## Testing

Existing basis to expand:
- **Cluster / failover**: `benches/bin/failover_verify.rs` (kill owner mid-run, assert
  no confirmed loss, no phantom) and `crates/protocol/tests/simulation_tests.rs`
  (deterministic turmoil sim: smoke + catch-up + partition-failover). Tests the layer
  above; keep as the end-to-end safety net and extend to the parallel scheme (a
  follower must not inherit a dangling enqueue either - same fold applies on the
  follower).
- **Single-node crash/recovery**: stroma `core/tests/crash.rs`, `replay.rs`, `roles.rs`.
  The right home for the new seams; likely needs crash-point and fsync-failure
  injection hooks added.

New coverage:
- **Crash injection** at each seam: between msg-fsync and event-fsync; after
  `EnqueueMany` before `CancelEnqueue`; after `CancelEnqueue`. Assert: no quarantine,
  no phantom delivery, no confirmed message lost.
- **Runtime fsync-failure injection**: msg fsync errors but process lives ->
  `CancelEnqueue(O)` written -> live state consistent -> producer nacked.
- **Fold-then-validate**: `EnqueueMany + CancelEnqueue` annihilates (never checked
  against msg log); a surviving clean-suffix dangling -> truncates silently; a
  synthetic mid-log dangling -> STILL quarantines (guard against a regression that
  hides real corruption).
- **`.idx`**: dirty open rebuilds the tail to a byte-identical index; clean open skips
  the rebuild.
- **Clean/dirty gate**: clean shutdown skips the fold; crash triggers it.
- **Property / fuzz** over random crash points, asserting the invariant above.

## Sequencing (revised after the A/Bs)

`.idx` Phase 1 is dropped (A/B: ~1ms, sparse index). The measured win is the
parallelization, gated on its safety machinery. Build order:

1. **`CancelEnqueue(offset)` event** - DONE 2026-07-07 (uncommitted, all 204+ stroma-core
   tests green). `CancelEnqueueMany { offs }` in stroma-core: event.rs (EventType=4, codec,
   `max_referenced_msg_offset=None`, round-trip tests), state.rs
   (`QueueState::cancel_enqueue_many` + `QueueCommand::CancelEnqueueMany` + handler + test),
   stroma.rs (both apply arms). Inert until the parallel path emits it.
2. **Recovery fold-then-validate** - DONE 2026-07-07 (uncommitted, all 211 tests green).
   Rewrote `recover_events_from_log`: fold (pending set of non-durable enqueue refs minus
   `CancelEnqueueMany`; valid prefix = up to the last pending-empty point) then truncate
   the unresolved tail. `DanglingReference` now AUTO-TRUNCATES (the expected parallel-crash
   artifact, always unconfirmed); `CorruptRecord` still policy/quarantine. Added
   `referenced_msg_offsets()`, flipped the dangling test to auto-truncate, added a fold test.
3. **Parallel append** - DONE + BENCH-VALIDATED 2026-07-07. Event append off the msg
   staging offset, `tokio::join!` on both durabilities (they must OVERLAP - sequential
   await does not and loses the win), in-memory apply gated on msg durability,
   `CancelEnqueue(O)` on msg-fsync failure. Immediate-only batches take the parallel path;
   delayed/mixed keep serial (extending the cancel to the delayed heap is a follow-up).
   A/B (nvme, clean build): confirm p50 500/s 15->8ms (-47%), 5k/s 20->13ms, receive 100%,
   no 30s outlier. Also fixed the fibril-broker replication event-availability match
   (`CancelEnqueueMany` = delete directive, no frontier gate).
4. **Expand the test suites** (failover_verify + simulation_tests + stroma crash/replay)
   per the Testing section, then flip it on / commit. NOT YET DONE.

## Open verification items (before/while coding)

- Confirm whether the stroma event-log dangling scan already gates on the manifest
  clean flag or runs unconditionally (governs the Phase-3 fold cost gate).
- Confirm keratin recovery can rebuild the `.idx` tail from the `.log` (Phase 1), and
  whether a helper already exists.
- Decide the `CancelEnqueue` event: new variant vs. an existing removal event that fits.
- Replication interaction: verify the follower catch-up path keeps the two logs
  consistent under the parallel scheme (the cache/file durable-gating asymmetry in
  keratin-log's tail cache is relevant here - see TAIL_CACHE_PLAN.md follow-up 1).

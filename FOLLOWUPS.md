# Follow-ups and pending work

## RESUME HERE (post-compaction 2026-06-30)

DELETE once underway. Transient cursor; durable detail in the task tracker + memory
([[release-process]], [[versioning-and-dst]]). Standing rules: commit per-brick to
main, DO NOT PUSH (user pushes fibril + ganglion + keratin); terse non-conversational
commits, no Co-Authored-By; comments self-contained (no convo refs, no semicolons,
plain ASCII); no unwrap/expect outside tests; keep docs + surface inventory current;
cross-repo via ../ganglion + ../keratin sibling local patch (authorized).

DONE THIS SESSION (all committed UNPUSHED except the working-tree handoff edits):
- #97 DST harness COMPLETE: 11 turmoil scenarios in crates/protocol/tests/simulation_tests.rs
  (run: `cargo test -p fibril-protocol --features simulation --test simulation_tests`).
  Cross-repo: ganglion raft transport made injectable over a RaftDialer (committed in
  ../ganglion, UNPUSHED), + fixed a pre-existing ganglion flaky test, + CoordinationSnapshot
  re-export.
- Replication client read + connect timeouts (real fix the durable scenario surfaced),
  now config-driven (replication.read_timeout_slack_ms / owner_connect_timeout_ms).
- #115 chaos/soak suite: crates/broker/tests/soak.rs (FIBRIL_SOAK_* knobs).
- #116 real multi-node run: it ALREADY existed - scripts/cluster-tryout.sh --failover-verify
  and --chaos, both run green. DO NOT write a new cluster script.
- #124 cluster-confidence gate MET (DST + soak + multi-node).
- 0.2 RELEASE PROCESS (this last chunk): shared [workspace.package] version, all crates +
  TS/Python clients bumped to 0.2.0; CHANGELOG.md; scripts/release.sh (per-repo template,
  changelog-gate + bump + docs /latest->/0.2 snapshot + cargo-check + annotated tag, no push)
  + scripts/release-all.sh overlord + .github/workflows/release.yaml (v* tag -> GHCR
  :version/:minor/:latest + GitHub release); development/releasing.md;
  roadmap now 0.2=current(gate1 met)/0.3=reconnect-polish+freeze.
  A LOCAL annotated tag v0.2.0 EXISTS, UNPUSHED - pushing it triggers the release workflow.
  NOTE: the v0.2.0 tag predates the docs-versioning rework below, so its tree still has the
  old naive /0.2 mirror. If re-cutting matters, delete the local tag and re-run release.sh.
- #133 DOCS VERSIONING DONE: replaced the naive cp -r snapshot with the starlight-versions
  plugin (full-site, working-tree). Current docs moved from /latest/ to the SITE ROOT;
  released versions frozen under /<minor> (e.g. /0.2/) with auto link-rewrite + a version
  picker; redirects /latest/* -> /*. The marketing homepage (src/pages/index.astro) stays at
  / and IS the docs landing (current root index.md dropped as redundant; /0.2 keeps its
  overview); marketing CTAs now point at /quickstart etc. release.sh docs step rewritten to
  register the minor in astro.config + run the site build (plugin generates the folder).
  smoke-dist.mjs updated to root paths + a /0.2 snapshot assertion. `npm run verify` green.

NEXT: (a) push (user) - branch + `git push origin v0.2.0` to actually cut 0.2 (see tag note
above re re-cutting after the docs rework); (b) cross-repo: give ../ganglion and ../keratin
their own scripts/release.sh + CHANGELOG + version model so the overlord drives them;
(c) remaining 1.0 gates: API/wire freeze (#125), operational lifecycle (#126), security
baseline (#127); 0.3 work = #102/#103/#105/#109.

---

## Test and expiry notes (2026-07-03)

- delayed_publish_over_tcp_waits_until_not_before flaked once under a parallel
  full-suite run (passes solo and in 3 repeat full runs). Timing-sensitive
  deadline assertion, same class as the fixed follower-loop cancel race. Worth
  a determinism pass if it recurs in CI.
- Mass-expiry cost: the expiry worker now resolves expired offsets to delivery
  tags with one scan of the inflight records per pass. If a mass consumer
  die-off with a very large inflight set ever shows up as a pause, the lever
  is engine-side batching of the requeue appends, not parallelizing the scan.

- Dead-consumer delivery recovery: a failed delivery send removes the consumer
  and leaves the messages inflight until TTL expiry redelivers them. With
  per-consumer batch dispatch the blast radius is a whole poll batch, not one
  message. Immediate release-and-requeue on send failure would tighten
  recovery (the release_inflight_batch engine path already exists).

- Adaptive tunables direction (2026-07-03): prefer deriving operating points
  from measured behavior within configured bounds. Candidates in value order:
  keratin adaptive fsync floor (with the dev-only latency injection flag),
  coalesce windows keyed to measured round-trip plus a startup timer-floor
  probe, and an adaptive spawn_blocking offload threshold for replication
  encode/decode. Guardrails: hysteresis, bounded ranges with a pin override,
  derived values visible in the debug surface, bench/DST validation.
  Window budget rule: a coalesce window should stay at or under about 10
  percent of expected overall latency (20 percent as the stretch bound).
  First review candidate: the 500us settle coalesce window is now 12-25
  percent of the improved 2-4ms delivery p50.

- Slow-storage levers (2026-07-03, from the SATA bracket data): (1) assess
  merging the msg-log and event-log durability legs into one commit, two
  barriers per confirm become one with no semantic change, the strongest
  slow-drive win on merit; (2) speculative queue delivery as a per-queue
  opt-in tier (deliver from staged state before the enqueue fsync, ghosts
  possible on crash, producer confirms unaffected), design it together with
  the API freeze (#111) and mirror the existing stream speculative tier.

- benchmarks.md overhaul once the perf arc settles: replace the informal
  250k+ figures with the measured post-audit numbers (paced 500k/s at 1KB
  with p50 under 10ms on fast storage, knee 500-600k), a storage-class table
  (tmpfs/NVMe-class vs SATA vs slow-VPS with the fsync floors), the zero-loss
  saturation results, and the honest cluster replica-durable state. Method
  notes: bench-matrix scenarios, run counts, drive-variance caveat.

Consolidated open items, extracted before the replication-effort working docs
were archived so nothing is lost. Full detail and rationale live in
`archive/replication-sharding-plan/` (the worklog, replication planning, and
design notes). Audit follow-ups live in [AUDITS.md](AUDITS.md), the audit status
board, and are not duplicated here.

Source tags: `[WL]` worklog, `[PLAN]` replication planning, `[DN]` design notes,
`[MEM]` memory, `[RACE]` race-windows, `[AUDIT]` audit board, `[AUTHOR]` author note.
Tiers are grouped by concern, not strictly ordered.

## #97 deterministic simulation - status

DONE: net seam (`fibril_util::net`, tokio normally / turmoil under `simulation`),
protocol + client crates converted and building both modes. Multi-broker turmoil
harness in `crates/protocol/tests/simulation_tests.rs` (compiled only under
`--features simulation`), with three deterministic tests: (1) broker runs inside a
turmoil host (no-net smoke - construction, keratin disk I/O, publish-confirm on the
simulated clock); (2) a follower catches up to the owner over the simulated
network via its supervised watcher; (3) owner partitioned away -> follower promotes
under a fenced epoch bump and serves a fresh publish, with the promoted log = the
replicated tails + 1 (no loss). Key constraint learned + documented: turmoil gives
each host its OWN current-thread runtime + LocalSet, so a broker must be built
INSIDE its host closure and can only be driven from there - cross-host
orchestration goes via the simulated network or shared atomics (the failover test
steps the sim and injects the partition from the main thread once a shared
caught-up flag is set). A fourth test stands up a 3-node ganglion raft cluster
inside turmoil (leader election + replicated committed write over the simulated
network). Run: `cargo test -p fibril-protocol --features simulation --test
simulation_tests`. Dev note "Deterministic simulation testing" stages 3, 4, 4a
marked done.

CROSS-REPO DONE (ganglion, committed, UNPUSHED): the raft TCP transport is now
injectable over a `RaftDialer` (TokioDialer = production, TcpNetworkFactory /
TcpRaftConnection kept as aliases so callers are unchanged); `serve_connection` +
frame codec are generic over the stream and `serve_connection` is pub - so a
caller runs its own accept loop over any transport. ganglion takes NO turmoil dep:
the TurmoilDialer lives in fibril test code. Also fixed a pre-existing timing-race
flake in ganglion's `learner_joins_catches_up_and_gets_promoted` (read-after-write
+ stale-leader-id), and re-exported `CoordinationSnapshot` from the ganglion
umbrella. fibril sim test binds through the `ganglion` umbrella crate, not
ganglion-openraft directly.

SPLIT-BRAIN DONE: `ganglion_returning_old_owner_is_demoted_under_simulated_partition`
runs 3 ganglion raft nodes inside turmoil (a-owner + b-follower carry brokers,
coordinator is raft-only for majority), partitions the old owner, the majority's
leader-only controller reassigns under a bumped epoch, the follower promotes, and
on heal the old owner observes its demotion and refuses publishes. Two integration
lessons baked into the test: (1) each turmoil host shares ONE current-thread
runtime across its broker + raft node, so a busy broker starves raft heartbeats and
replication serving - the old owner is kept idle through catch-up + partition, and
raft uses widened election timeouts (heartbeat 200ms, election 1000-2000ms).
(2) The controller keeps all coordination WRITES on the leader (register +
control_iteration only when is_leader, and only when state must change) so it never
hits the follower-forward path - `client_write_remote` still dials via TokioDialer,
so cross-host forwarding from a non-leader/non-member is NOT sim-compatible yet
(members forward internally over the RaftNetwork, which is on the dialer). Making
client_write_remote dialer-generic is the remaining gap if a future scenario needs
follower-forwarded writes under sim.

SCENARIO SET (8 green, deterministic): smoke, static-coordination catch-up,
static-coordination failover, raft-cluster-over-turmoil, split-brain,
lossy-link catch-up (flapping follower), raft-cluster-converges-under-message-loss,
durable-publish-unconfirmed-while-replica-partitioned (ReplicaDurable 2-node: no
false durability ack while the replica is partitioned). The lossy ones use turmoil
fail_rate/repair_rate + latency bounds + a fixed rng_seed; loss/latency are kept
under the raft timers (heartbeat 200ms, election 1000-2000ms) so a majority stays
connected. Gotcha baked in: start_split_brain_node HARDCODES membership addrs
a-owner/b-follower/coordinator, so any cluster test reusing it must name its turmoil
hosts those exact names.

FINDING (surfaced by the durable scenario) - FIXED: the follower replication client
had NO client-side timeout, so a partition that DROPS an in-flight read response or
a connect SYN (turmoil partition, not an RST) left the worker on a dead connection
until the transport itself broke. Fixed in protocol replication.rs: a read waits at
most its long-poll window + DEFAULT_READ_TIMEOUT_SLACK_MS (10s), and connection
setup (TCP connect + HELLO/AUTH) at most DEFAULT_OWNER_CONNECT_TIMEOUT_MS (5s); on
either the worker errors, drops the conn, and redials. The durable scenario now
asserts recovery-after-heal too. (Both are named const defaults with builder
overrides; full config-crate/runtime-settings wiring is a minor follow-up.)

SCENARIO SET COMPLETE (11 green, deterministic): the 8 above plus
follower_installs_checkpoint_after_owner_truncates (owner truncates past the
follower start via the new StromaEngine::truncate_messages_before, follower
installs the owner checkpoint to reach the tail) and
repartition_cutover_waits_for_delayed_topology_ack (real client acks over the sim
net; turmoil hold/release delays the topology exchange so the adoption fence holds
below the new generation, then finalizes on release - note: use hold/release, NOT
partition_oneway, for delaying a live TCP stream, since one-way drop desyncs the
byte stream with no retransmit). #97 can be considered functionally done as an
evaluation+harness; madsim remains the documented later escalation only if
scheduling-order determinism is ever needed.

NEXT (post-#97): the read/connect timeout slack is now config-driven - DONE
(ReplicationSettings seed + ReplicationRuntimeSettings -> BrokerConfig ->
ProtocolOwnerPeerResolverConfig::with_timeouts -> peer; documented in
configuration.md; protocol consts remain the defaults). Captured at peer
construction (consistent with auth/client peer config), not per-read-live, so a
runtime change applies when the peer is next rebuilt - acceptable for connection
timeouts.

CHAOS/SOAK (#115) DONE (first suite): crates/broker/tests/soak.rs - real broker,
real wall-clock, real fsync. (1) durable_crash_recovery_soak restarts the engine
from disk across cycles, asserts every confirmed message survives + delivered
exactly once + strictly increasing offsets + settled (acked) messages never
redelivered after restart. (2) concurrent_load_no_loss_soak runs N producers +
a consumer for a wall-clock window, asserts every confirmed publish consumed
exactly once (no dup offsets). CI-small defaults; FIBRIL_SOAK_CYCLES/BATCH/SECS/
PRODUCERS scale to a real soak.

REAL MULTI-NODE RUN (#116) DONE - and it already existed: scripts/cluster-tryout.sh
stands up N real fibril-server PROCESSES forming one Ganglion raft cluster over
real TCP. `--failover-verify` runs an identity-tagged producer/consumer through
public client routing, kills the partition owner mid-run, and asserts every
confirmed id survives failover (zero loss, no phantoms); `--chaos` repeats mixed
faults (pause/resume, kill/rejoin) under sustained load and asserts zero loss +
reconvergence. Both executed green (3-node, failover-verify 3000 msgs @3000/s
zero-loss; chaos 2 rounds zero-loss + reconverged). Lesson: this harness predated
my work - reuse it, do NOT add a new cluster script (a redundant cluster_validation.sh
was written then deleted). The dedicated verifier bin is fibril-benches::failover_verify.

CLUSTER-CONFIDENCE GATE (#124) - all three legs now hold: deterministic simulation
(11 turmoil scenarios), chaos/soak suite (#115), and a real multi-node run (#116
via cluster-tryout). The multi-broker chaos-under-load variant the soak-growth note
wanted is exactly cluster-tryout --chaos, so that follow-up is satisfied too.

With the DST harness proven across election, replication, failover,
split-brain, and lossy networks, the cluster-confidence gate (#124) has a real
deterministic base to build on.

- Stream/staging perf levers from the staging-efficiency audit. DONE: removed the
  per-publish replication-cache clone (cache removed entirely, keratin 27940f8) and
  the per-record fan-out round-trip in the stream drain (fibril 115b370, +~23%
  ephemeral single-partition). OPEN: (a) keratin encode_record builds the fixed
  32-byte record header via 8 extend_from_slice calls - pack into a stack [u8;32] +
  one extend (micro-opt). (b) BIG: the payload is memcpied into write_buf before the
  single write(); a vectored write (writev of [header, headers, payload, crc] with
  CRC fed incrementally) would avoid the copy - real lever for large payloads, but a
  substantial write-path rewrite (partial writes, segment rolls). (c) durable
  single-partition throughput is noisy run-to-run (~196k-245k at 300k offered, 1KB);
  worth tracing what causes the variance (fsync batching cadence? scheduling?).
  Client-side: a single reader's multi-partition fan-in tops out ~260k records/s -
  its own bottleneck, separate from the broker. See tasks #61/#62/#65 for the topic
  routing + Arc<str> interning follow-ups.

- Stream (fan-out) filter performance + filtering expansion (task #129).
  PHASE 1 ASSESSED 2026-06-29 (microbench `crates/broker/benches/stream_filter.rs`,
  Ryzen 5950X): the common cases are already optimal - an empty/no filter
  short-circuits at ~3ns and patterns are precompiled at subscribe time
  (`WildcardPattern`), with an alloc-free `matches`. A filtered match costs ~22ns
  for 1 clause and ~57ns for 3, dominated by the `HashMap<String,String>` header
  KEY lookup (~18ns), not the wildcard match (exact 4ns / glob 7ns / multi-seg
  21ns). The remaining lever is the redundant per-subscriber eval when many
  subscribers SHARE one filter: it is linear, ~39ns/sub, so a single 2-clause
  filter across 4096 subscribers is ~160us/record of filter eval. Two phase-2
  levers, both justified by these baselines: (a) evaluate once per distinct
  filter per record - cleanest via interning filters as `Arc` and grouping
  subscribers by Arc identity (note: `try_send` per matched subscriber likely
  dominates the loop, so dedup saves filter-eval, not the whole per-sub cost); (b)
  a faster header-key lookup (the SipHash key hashing is the per-clause cost).
  PHASE 2: expand the filtering vocabulary (OR groups, negation, value ranges,
  header-set membership) while staying declarative and bounded - NOT
  content-routing scripting, which is out of scope (see roadmap). The bench is the
  regression baseline for both.

This file tracks the replication and clustering roadmap leftovers. Non-replication
feature ideas live in their own track, summarized at the end.

Idea backlog (pick from these): express lane / speculative delivery + deferred
publisher confirm (the ghost-flag pattern); in-memory non-durable queues
(pluggable keratin write target); producer dedup / max-unconfirmed-per-publisher;
client embedded-retry (is_retryable + retry handle); documented failure semantics
/ operator runbook; crash-recover "leftover inflight without message" smell;
split-brain epoch-fence test; multi-broker-same-storage must-fail test; #97 DST
simulation. Inflight persistence is confirmed (encode_snapshot writes inflight
(offset, deadline) pairs, load_snapshot restores them, MarkInflight events
replay) so leased-unacked survives crash/restart.

## Done since the inventory was last curated (2026-06-22)

The `implemented-surface.md` inventory was recurated on 2026-06-22 and now
reflects everything below. Keep this list empty-ish: fold new surface into the
inventory as it lands (see the docs-currency directive in the Docs section).

- (curated into the inventory 2026-06-22) Admin create-queue, delete-queue
  (single-node), hide-inactive toggle + search on the queues page.
- (curated 2026-06-22) Message TTL: per-message `ttl_ms` + per-queue
  `default_message_ttl_ms`, DLQ-routed expired drop (`DeadLetterReason::Expired`),
  client `expiring` publisher + `default_message_ttl`. Rust + TypeScript.
- Topology page glowup: adaptive ellipse/staggered-grid layout, click-to-inspect
  broker panel, and a Diagram/List view toggle.
- Admin SPA-feel: boosted navigation, vanilla, no framework.
- Admin node management from the topology page: repartition plus add/remove
  coordination voting member controls.
- CI guard: inline template-JS syntax check (`scripts/check-template-js.sh`,
  wired into `scripts/check.sh`).

## Correctness and durability

- FIXED 2026-06-28 (found via the cluster tryout after the admin error-swallow fix):
  the admin /admin/api/queues endpoint 500d on a node hosting a Plexus stream, and
  the expiry worker logged "queue actor is gone" (broker.rs:3842) every poll. The
  earlier owner->follower-demotion hypothesis was WRONG. Real cause: the work-queue
  sweeps (get_queues_stats, collect_expired, next_expiry_hint, the TTL-drop sweep)
  iterated ALL materialized handles, including stream partitions. A stream runs a
  StreamEngine with no work-queue command actor, so a status report or lease-expiry
  collection routed to one failed with "Status report failed: channel closed", and
  one such failure aborted the whole sweep. Leader-specific only because that node
  happened to host the stream. Fixes shipped: (a) per-queue tolerance - get_queues_stats
  reports a tagged Ok/Error enum per queue (keratin 9ad589c, fibril a331b35) and the
  admin route returns the error instead of swallowing it to {} (fibril 86b82dc);
  (b) ROOT CAUSE - exclude stream-kind partitions from the work-queue sweeps
  (keratin 4994353); (c) STRUCTURAL FIX (keratin 7404042, task #99): engine-specific
  ops now live on typed WorkQueueHandle/StreamHandle projections reachable only via
  as_work_queue()/as_stream(), so a work-queue command on a stream partition is a
  compile error, not a runtime channel failure. The runtime kind guards in the
  sweeps were replaced by the projection, and the two remaining latent siblings
  (the TTL-drop sweep and validate, which iterated all partitions and only guarded
  on role) are fixed by the same change.

- Plexus stream routing after failover relies on the per-partition `.kind` marker
  being present on the new owner. The marker is a LOCAL file written at
  `create_stream` / queue `declare` time on whichever broker first declared the
  channel, and it is not replicated with the log. Two consequences on a failover to
  a node that never ran the declare: (1) a stream publish can fall down the queue
  path until the stream channel is opened there, and (2) the same-topic kind guard
  and the materialize-by-marker path default to Queue when the marker is absent, so
  a node could in principle materialize a stream partition as a queue (a MIXED-kind
  topic across nodes). Same-node mixing is already prevented (the declare guards key
  off the marker and both declares iterate from partition 0, so a conflict aborts
  before any partition of the other kind is created). The cluster fix is to make
  kind durable/replicated: replicate the marker, or persist kind in the replicated
  partition state, or have owner-activation/catalogue-sync open the correct engine
  for owned partitions. Until then, declaring the channel against the new owner
  re-materializes it. Low risk in single-node / declare-before-publish flows.

- DECIDED 2026-06-29 (task #101): cold-restart orphaned on-disk partitions. On a
  cold restart a partition the node was disowned of while down is indexed as an
  unmaterialized slot but the empty assignment cache makes the planner compute
  Noop, so the on-disk data is never materialized and never cleaned. Decision:
  leave it as inert cold storage, do NOT auto-destroy on restart. It is already
  safe - serving is ownership-gated (ensure_queue_owner runs before
  materialize_owned_queue), so an orphaned partition is never served and never
  mis-materialized (regression tests cold_restart_disowned_partition_is_inert_
  cold_storage + orphan_reconciliation_ignores_partitions_unknown_to_coordination).
  Retained rather than reclaimed because a later re-acquisition reuses the on-disk
  log (fast failover-back without re-replication) and the startup snapshot can lag
  a reassignment about to hand the partition back. `Broker::orphaned_on_disk_
  partitions` surfaces the set (logged once at startup) for operator visibility.
  Two follow-ups left open: (a) opt-in reclaim - an admin action or a setting-gated
  startup purge of partitions coordination has provably reassigned elsewhere, so
  disk does not leak forever (Kafka-style stray-log move-aside is the safer shape
  than immediate delete); (b) a deeper hazard, separate from this item: if a node
  RE-ACQUIRES ownership directly from cold (BecomeOwner, not via a caught-up
  follower) it would materialize its STALE local log and could serve messages the
  current owner accepted while it was down. Re-acquisition should catch up from the
  authoritative owner (or epoch-fence the stale tail) before serving. Needs a test
  and likely a become-follower-first-then-promote path on cold re-ownership.
  This cold-storage decision is the partition-side counterpart of the broader
  vision (roadmap, longer term): durable broker restart reconciliation that
  extends the live-process graceful reconnect across a process restart, so a
  client within its grace window continues transparently and rolling upgrades
  become trivial.

- Durable stream throughput RESOLVED (was ~847/s). The earlier pipelining was
  correct but the per-channel ingest awaited the staged offset between appends
  (append_stream_records_batch), so keratin's writer never held more than one append
  at a time and its fsync coalescing could not engage: one fsync per record. The fix
  (keratin afb507f, fibril 588e51c) is append_stream_records_enqueue, which returns
  before stage (housekeeping moves to a background task), so the ingest enqueues the
  next batch without waiting for keratin to report back and keratin coalesces fsyncs
  across the queued appends. Durable now reaches line rate (~197k records/s at 1KB,
  ~14ms confirm) on SSD; SSD ~= tmpfs because the fsync is amortized. A follow-up
  (keratin a430df3, fibril 4058b3f) routes ALL tiers through the same batched ingest
  (drain sets fan-out/confirm timing by tier), so speculative now matches durable
  (~190k/s) and ephemeral keeps the lowest latency (~1ms deliver). Bench via
  scripts/bench-stream.sh (DURABILITY + DATA_DIR knobs). Open: ephemeral throughput
  in confirmed mode is lower than speculative despite no fsync - a client-side churn
  artifact of confirming at stage (very fast confirms -> high re-publish churn), not
  a server limit (1ms delivery). Also: light-load confirm/deliver latency is the
  keratin fsync interval, config-tunable, separate from this.

- Ephemeral writeback tail RESOLVED (keratin db92777 + fibril da35400). It had a
  fat delivery tail on real disk despite the best p50 (SSD, 1KB, 150k/s: p50=1ms
  but p95~530ms) because AfterWrite never fsyncs, so dirty pages piled up until the
  kernel throttled the writer mid-write. Confirmed by tmpfs (no writeback) having no
  tail. Fix: a per-ephemeral-channel periodic flush task (5ms) calls Stroma::
  sync_stream, and WriterCmd::Sync now hands the fsync to keratin's fsync WORKER
  stage (carries the responder through FsyncReq/FsyncDone) instead of running it on
  the writer thread, so it drains dirty pages without stalling staging. Ephemeral is
  now p50=1ms / p95=2ms / p99=7ms at full line rate, beating tmpfs, lowest RSS, and
  stays the lighter tier (still AfterWrite, no per-record fsync). The 5ms interval is
  a broker-local const (EPHEMERAL_FLUSH_INTERVAL) like the ring capacities; promote
  to a runtime setting if it needs tuning in the field.

- Possible future channel mode: a true memory-only stream (no log at all, lost on
  restart, no durable cursors/replay/retention, lowest possible latency). Distinct
  from the `ephemeral` durability tier, which is defined as log-backed (persist
  async, no fsync, do not gate). Would be a separate channel flag or a 4th mode,
  not a redefinition of `ephemeral`. The writeback finding above strengthens the
  case: a memory-only mode sidesteps writeback entirely. Examine when there is a
  real need.

- Offsets are an unstable internal storage detail, not a stable consumer-facing
  identity (Fibril follows work-queue semantics, not a replayable-log model). The
  clients deliberately do NOT expose starting a stream subscription at a raw offset
  (resume is via a durable cursor name). The wire `StreamStart::Offset` variant is
  kept for internal/ops use only. Open question the user raised: also make the
  delivered-message `.offset` accessor less prominent or remove it across both
  channel types (it is currently exposed on Message in all three clients). Confirm
  before removing, since queue consumers may read it for logging.

- FIXED (found by the chaos soak): a consumer stopped receiving and never caught up
  after the OWNER broker was killed and rejoined WITHOUT a failover (a bounce faster
  than failure detection, so committed ownership stayed on the same broker). It was
  never data loss (confirmed stayed durable, zero phantoms/dups) - the consumer's
  subscription to that owner just did not re-establish after the owner's restart.
  Two-part client fix: (1) `start_engine` now reconciles active subscriptions on
  ANY reconnect, not only a `Resumed` session, so a fresh broker session restores
  or closes them; (2) the subscription supervisor proactively re-dials the bound
  owner's connection on its owner-check even when the owner endpoint is unchanged,
  because nothing else reconnects a passive subscription's connection. Confirmed:
  the 20-round chaos that lost ~70% now passes 4/4 with zero loss, plus a
  deterministic unit regression (`reconnect_reconciles_subscriptions_when_resume_rejected`).
- Idempotent producer dedup: broker reads `fibril.client.producer_id`/`seq` for
  effectively-once delivery (the headers are already on the wire). The one
  success criterion left genuinely not done. [WL/DN/PLAN phase 8]
- Split-brain: believed addressed by epoch fencing in Keratin plus the Stroma
  freeze. Verify, and add adversarial tests for the reappearing-stale-owner case
  (reject any write or replicate whose epoch is below the local partition epoch). [PLAN]
- Durable queue role (Stroma hardening, defense-in-depth for the ownership gate):
  a Stroma queue defaults to `QueueRole::Owner` on create/recovery and the role
  is in-memory only (`stroma/core/src/state.rs`). So a Frozen/Follower queue
  loses that role on eviction or restart and re-materializes as Owner, which can
  resurface a stale owner accepting writes if the broker gate is bypassed or
  diverges. Two layers found while fixing
  `ganglion_returning_old_owner_is_demoted_and_refuses_publishes`:
  (A) the broker gate/watcher divergence - FIXED (broker `locally_owned` reconcile,
  commit aea4d50, demotes a de-facto owner even with no observed BecomeOwner);
  (B) this engine-role durability gap - OPEN. Lighter increment: pin a
  frozen-for-transition queue against eviction (survives eviction, not restart).
  Robust fix: persist the role (or "not owner") so recovery restores a non-owner
  state and ownership is always coordination's decision, never a default (covers
  eviction AND restart). The gate masks this in normal operation, so it is
  defense-in-depth. [AUTHOR]
- Ex-owner rejoins the cluster after losing privileges while its replicas were
  not fully caught up and its data was not shared: define and handle the
  mechanics (ties to epoch fencing plus recovery verification). [WL]
- Ensure follower queues are materialized in memory on demand. [WL]
- Low priority: verify snapshot replay strictly begins at the offset after the
  snapshot (believed done via `recovery_replays_only_events_after_snapshot_offset`). [WL]
- `[RACE]` STALE, needs re-verification against current code: ack versus
  redelivery-worker idempotency (a snapshot `list_expired` can race an ack). The
  delivery-tag epoch work may already cover this. Confirm or add a generation or
  is-acked guard before requeue. [RACE Race 2]

## Performance and scale

- Arc<str> + soft interning for topic/group (#65): ASSESSED 2026-06-28, NOT WORTH
  IT - do not pursue the broad refactor. Findings: the per-message hot path
  already allocates the topic exactly once (publish decodes `reader.str()?
  .to_owned()`) and routing is alloc-free (`slot_lookup_no_alloc` takes `&str`),
  so there is no repeated-clone pattern for Arc<str> to optimize - the many
  topic `.to_string()` / `.clone()` sites in broker.rs are cold (admin, topology,
  list, assignment events). A microbench (50 topics, 5M ops) showed interning is
  ~3x SLOWER than the current short-string alloc even with no lock contention
  (String::to_owned 7.8 ns/op; Mutex<HashMap> intern 24.5; RwLock read+clone
  24.4) because you still materialize the str to hash it, then pay a map lookup +
  atomic refcount. Memory dedup is also small (topics <=128 B; duplicated topic
  strings across partitions are KB-scale unless partition counts are enormous).
  Cost would be large (60+ String->Arc<str> fields across keratin/fibril/protocol
  + wire). If extreme partition-count memory ever shows up in profiling, intern
  ONLY the cold registry/state storage, never the message path. Verdict: skip.

- Owner-side read and encode fan-out (shared tail, private catch-up) for RF >= 3. [WL]
- Parallel-fsync / async-fsync for replicated append (the top durable-replication
  perf lever; user-reconfirmed 2026-06-29). Concrete: keratin's replicated-append
  path (writer.rs `stage_replicated_req`) does a SYNCHRONOUS inline `log.fsync()`
  per batch, so on disk the writer blocks on every replicated fsync. Route it
  through the batcher + async fsync pipeline (`fsync_tx` / `drain_fsync_done`),
  mirroring the local ephemeral sync_stream rework, so replicated commit/event
  fsyncs coalesce off the hot path. Full analysis + bench numbers in
  REPLICATION_WORKLOG.md (search "stage_replicated_req" / "async-fsync"). [WL]
- Replication-lag backpressure hook in the append path (slow accept when
  followers lag). [PLAN]
- Replication streamed decode (decode while fetching and applying), separate
  payload and event replication streams, and a more push-focused replication
  architecture. [WL]
- Low priority: make `BLOCKING_DECODE_BYTES` adaptive instead of a CPU-tuned
  const (piecewise decode-time model, startup calibration first). Details in the
  code comment. [WL]

## Operability and quality of life

- Broker advertise address, separate from the bind address - MOSTLY DONE (#94).
  Was: the broker registered `config.broker.listener.bind` as its endpoint, so in
  a container (`0.0.0.0:9876`) peers/clients could not dial it back. Built:
  `broker.listener.advertise: Vec<String>` + `FIBRIL_BROKER_ADVERTISE` (config
  crate), with a zero-config default derived from the coordination peer host (our
  own `FIBRIL_COORDINATION_PEERS` entry) + the broker port, falling back to the
  bind. The node table holds the primary (first) endpoint as a connectable String
  (service names survive - the SocketAddr parse that dropped them is gone), and the
  full priority list rides a per-node heartbeat label (`fibril/advertise`), so
  `owner_endpoints` is an ORDERED LIST of `AdvertisedAddress{host,port,tags}` on the
  wire (Queue/Stream topology + Redirect). All three clients decode the list; the
  TUI, TS, and Python clients connect by the first endpoint (by name, so service
  names work). Containerized routing + replication now work.
  - REMAINING (b3c): the high-level Rust client's connection pool is keyed by
    `SocketAddr`, so it takes the first owner address that PARSES as one and
    logs+skips a service-name address (`first_socket_endpoint`). Making it connect
    by string and try the list in order (the real probe) is a focused
    connection-manager refactor (pool key + `engine_slot` + bootstrap + prune +
    `EngineSlot` connect/reconnect, ~25 `SocketAddr` sites). Until then a Rust app
    using the high-level client against a service-name-advertising cluster routes
    via bootstrap, not owners. The TUI/TS/Python paths already connect by name.
  - try-in-order client behavior (all clients currently use the FIRST endpoint, not
    a true connect-probe): give the per-attempt connect a short timeout so a
    black-hole first entry does not stall the fall-through. Tags ride the wire
    unused, for future selection conventions.
  - Heavier evolution if try-in-order's connect-failure latency ever bites:
    Kafka-style named bind listeners with selection by the listener the client
    bootstrapped through. More moving parts; the ordered-list probe is the lighter
    cut and the `AdvertisedAddress{...tags}` shape is a clean stepping stone. [AUTHOR]
- Programmatic scale up and down: join (learner to voter to rebalance) and
  drain-and-leave via fibrilctl plus the admin API, autoscaler-drivable. [PLAN]
- Consumer assignment push and client fan-in narrowing: today a cohort client
  fans in to all partitions of a queue and the per-partition delivery gate
  enforces exclusivity. The deferred optimization is an assignment-change push
  (`Op::AssignmentChanged`) so a client only pulls from partitions it is
  assigned, with the gate staying as the correctness backstop. This also enables
  per-partition leave (today `leave` drops the whole connection subscription).
  [WL phase-2a limitation (c)] [MEM]
- Unclean-leader-election toggle, off by default. Minor. [PLAN]
- Settings tiering: basic, advanced, expert, with collapsible sections. [DN/WL]
- Settings presets, orthogonal to tiers: opinionated bundles such as low-latency,
  hands-off, and power-user. Tiers are how much you see, presets are what the
  defaults do. [AUTHOR]
- Relational settings nudges (soft warn, not reject): for example warn when a
  failover-sensitive timeout is set below the failure-detection cadence. Needs
  cross-setting advisories at config load and runtime PUT, plus an inline admin
  hint. Pairs with settings tiering. [AUTHOR]
- Eager opt-in startup recovery: `recover_all` exists but is unused (recovery is
  lazy via `queue_handle`). A config to eagerly recover all on-disk partitions at
  boot makes `recovery.on_mismatch = refuse` a literal refuse-to-start. Lazy
  stays the default. [AUTHOR]
- Snapshot cadence: wire `snap_cfg.every_events` as an additional knob alongside
  the time and dirty triggers. The gate is commented out in
  `periodic_snapshot_step`, `last_snapshot_event_offset` is already tracked, so
  wiring is low-risk. Currently `#[allow(dead_code)]` with a FIXME. [AUTHOR]
- Onboarding and easy trial - LARGELY DONE, verified end to end 2026-06-28. The
  one-command path exists and works: `ghcr.io/axmouth/fibril-server` is public
  (anonymous `docker pull` confirmed), `compose.cluster.example.yaml` brings up a
  three-broker ganglion cluster with service-DNS peers, a healthcheck-gated
  one-shot seeder (demo queues + a stream), and each admin dashboard exposed, and
  `website/public/tryout.sh` (served at `fibril.sh/tryout.sh`, documented in
  `concepts/clustering.md`) is the curl-to-shell bootstrap. The quickstart page
  covers single-broker `docker run` and links the under-a-minute cluster tryout.
  Verified: cluster forms (healthy consensus, leader elected), seed declares
  orders/payments/emails + the events stream, and `/admin/api/topology` shows
  partition ownership spread across the three brokers.
  - REMAINING (genuine): in-memory (non-durable) mode for an even lighter trial
    (no data volume, lost on restart). This is really a storage feature (a
    pluggable Keratin write target / memory-only tier), larger than onboarding -
    see the "In-memory (non-durable) queues" item in the non-replication track.
  - The leader `/admin/api/queues` returning `{}` after seeding, noticed during the
    smoke test, was NOT lazy materialization - it was the stream-in-work-queue-sweep
    bug, now FIXED (see the FIXED entry under "Correctness and durability"). Not a
    trial-path issue.
  [WL/AUTHOR]
- Admin dashboard: a lost-connection banner. When the admin page can no longer
  reach its broker (broker down, failover, network blip), show a clear banner
  instead of silently stale data. [AUTHOR]
### Queue lifecycle, retention, expiry (ordered plan)

Small user-facing features, ordered cheapest-and-highest-visibility first. The
first three are admin-thin (the primitive exists, just expose it).

1. **Admin create-queue** - DONE first cut (commit f2d91b7). POST
   /admin/api/queues declares partitions 0..count locally with an optional DLQ
   policy + max retries, plus a create form on the queues page. PARITY TODO: go
   through `declare_partitioning` for an authoritative count + conflict detection
   (multi-partition cluster currently relies on the catalogue-sync loop to
   register the declared partitions).
2. **Queue purge.** RE-SCOPED to M after digging in: "purge is reset" was
   optimistic. `QueueCommand::Reset` / `state.reset()` only re-inits the in-memory
   consumption state (offsets/acks/inflight) - it does NOT drop log messages and
   does NOT replicate (messages re-present after it). `StromaEvent::ResetQueue`
   exists and is applied/decodable but is only EMITTED in tests (unwired
   scaffolding). A real purge = truncate the message log to head (the
   `message truncate ... before` primitive in `stroma.rs` exists) + reset state +
   emit a replicated `ResetQueue` event, plumbed Stroma -> engine -> admin, with
   replication correctness. Cross-crate, so do after the genuinely-thin items.
3. **Hide-inactive-queues toggle + basic search** on the queues page - DONE.
   Frontend-only filter bar (search by topic/group, hide queues with no active
   publishers/subscribers); summary cards stay full counts.
4. **Admin delete-queue** - WE WANT IT (the earlier "no delete yet" meant "we do
   not have it", not "skip it"). Split into two:
   - SINGLE-NODE - DONE. `POST /admin/api/queues/delete` (`delete_queue` handler +
     `DeleteQueueRequest{ tp, group, partition_count }` in `routes.rs`) loops
     `destroy_partition` over 0..count, mapping HasInflight -> 409 and other
     errors -> 500, gated off in cluster (`server.coordination.is_some()` ->
     501 "cluster_delete_unsupported"). UI: per-row Delete button on the queue
     agg row in `queues.html` (carries data-group/topic/partitions, confirm
     dialog, `apiPost` then refresh). Tests cover empty-topic -> 400 and a
     destroy-declared-partitions happy path.
   - MULTI-NODE (planned, M): a coordinated teardown - deregister from the
     coordination catalogue (so the controller stops placing it and the
     catalogue-sync loop stops re-registering it), destroy on ALL replicas (not
     just the clicked node), ordered so sync cannot resurrect it between steps.
     destroy_partition is only the local primitive - this is the real work.
5. **Time-based retention.** Primitive `safe_message_truncate_before` exists.
   Add a sparse worker: map time -> offset (crude binary search on the stored
   `published` is fine, no new index needed), truncate before the cutoff, and
   clear the relevant state entries. Per-queue retention config.
6. **Message TTL (drop by age).** GENUINELY NEW - do not conflate with the
   existing `expiry_heap` / `collect_expired`, which is the lease-timeout
   REDELIVERY path (inflight -> requeue, never ack), not age-drop. DESIGN (locked,
   traced against current code):
   - Fork resolved - do BOTH per-message and per-queue-default, collapsed to one
     absolute deadline at publish on the owner (broker = clock authority):
     `expire_at = per_message_ttl ?? queue_default_ttl ?? none`. Per-message =
     optional `ttl_ms` on the existing `Publish` op (no new op/frame). Per-queue
     default = `default_ttl_ms: Option<u64>` on `DeclareMeta` (declare-time).
   - State (stroma `state.rs`): a single `ttl_deadlines: RangeMap<Offset,
     Deadline>` keyed by OFFSET (quantized deadline). NOT a deadline-ordered heap.
     Keying by offset is the win: the mapping persists across
     ready->inflight->ready and is removed only on terminal settle, so there is no
     "bucket fired while inflight then requeued and never re-checked" bug, and no
     re-insert logic. RangeMap (not heap) because a TTL structure is bounded by
     the READY BACKLOG (every msg can carry a deadline), largest exactly when TTL
     matters (lagging consumers); RangeMap collapses contiguous same-deadline
     spans -> near-O(1) per burst for uniform/queue-default TTL. `ready` is already
     a RangeSet, so this is idiomatic. (Plain offset-keyed structure also answers
     reactive lookups; a deadline-keyed map/heap would need a reverse index.)
   - Two-tier check: (a) REACTIVE / correctness - `next_deliverable` + mark-inflight
     drop any offset with deadline <= now, so stale work is never delivered
     regardless of worker timing; (b) PROACTIVE / cleanup - the EXISTING
     expiry_worker (broker.rs `spawn_expiry_worker`) scans the front of
     ttl_deadlines each tick, dropping expired READY offsets bounded by
     `expiry_batch_max`. Hint piggybacks the front entry (exact for uniform TTL,
     else falls back to poll cadence). NEVER drop inflight.
   - Drop = a DURABLE settle (Ack-path event) - unlike lease-requeue and
     delayed-enqueue, which are derived/non-durable. v1 = discard; DLQ-on-expiry
     is a cheap fast-follow (`DeadLetterReason::Expired` + existing DLQ path).
   - Durability/replication/recovery: `expire_at` rides the `Enqueue` event
     (bump `STROMA_VER` 2->3) so followers + recovery rebuild ttl_deadlines.
     Snapshots (`encode_snapshot`/`load_snapshot`, bump `FORMAT_VERSION` 2->3)
     append the ttl_deadlines ranges, since snapshots compact away the Enqueue
     events. Quantization granularity = a delivery runtime setting (ceil to bucket
     so we never drop early); guarantee mirrors delayed-publish ("dropped within
     granularity + worker period after expiry").
   - Client: a `publisher.expiring(ttl)` builder (a SETTING, not a new type) that
     stamps a default per-publish ttl; ttl via the Delayable-style trait (bare
     number = seconds, or a Duration). Composable with delayed/reliable modes.
   - Brick order (each its own green commit): (1) `DeclareMeta.default_ttl_ms` +
     `Enqueue` `expire_at` + STROMA_VER bump (stroma codec/event); (2)
     ttl_deadlines + reactive drop in next_deliverable/mark_inflight + proactive
     drop in collect_expired + snapshot FORMAT_VERSION bump (stroma state); (3)
     broker publish-path wiring (resolve effective ttl) + worker durable-drop
     emit; (4) wire `ttl_ms` on `Publish` (protocol); (5) client `expiring()`.
7. **Queue expiration (auto-delete idle queues).** A DECLARE-TIME queue setting
   (the queue carries its own idle-TTL, e.g. expire-after-idle), not an external
   config. A sparse worker destroys queues idle beyond their declared TTL. Builds
   on delete (#4) + idle tracking.
   - DISTINCT from message-TTL default (#6): #6 drops individual MESSAGES by age
     (per-partition, no coordination); #7 deletes the whole QUEUE after idle. Use
     a clearly separate DeclareMeta field (e.g. `expire_after_idle_ms`), never
     reuse the message-TTL default.
   - DESIGN NOTE: queue expiration is ideally GLOBAL (per queue), not
     per-partition. "Is the queue idle?" must aggregate activity across ALL the
     queue's partitions and replica nodes, so a per-partition timer is wrong - the
     decision + the destroy need to be coordinated (controller-side aggregation of
     last-activity, then a coordinated teardown like multi-node delete in #4).
     Needs brainstorming; deferred. Per-partition idle is only a building block.

Shared time-based internals (map for #5/#6/#7 - they are "close" but share CONFIG
more than MECHANISM, so do them one-by-one, not as one unified subsystem):
- Granularity differs: TTL (#6) is per-OFFSET (RangeMap + collect_expired +
  expiry_worker + durable settle); retention (#5) is per-SEGMENT by age
  (`safe_message_truncate_before` + message `published` ts + a coarse truncate
  worker); queue-expiration (#7) is per-QUEUE idle (`destroy_partition` (done) +
  idle tracking, which broker observability already exposes as `idle_for_ms`).
  Different core data structures - do NOT try to unify the mechanisms.
- GENUINELY shared surface = `DeclareMeta` (all three are declare-time per-queue
  settings: `default_ttl_ms`, retention window, idle-TTL). Worth shaping ONCE: a
  consistent `Option<u64>`-ms field convention + declare->replicate->recover
  plumbing (the `Declare` event already carries DeclareMeta). TTL's
  `default_ttl_ms` lands in the shape #5/#7 reuse.
- Also shared: the worker + runtime-settings cadence pattern
  (`expiry_poll_min_ms`/`expiry_batch_max` is the template; #5/#7 each want their
  own cadence knob), and SNAPSHOTS - any per-offset/per-queue durable state added
  for these must round-trip `encode_snapshot`/`load_snapshot` (FORMAT_VERSION bump)
  since snapshots compact away the events that would otherwise rebuild it.

Nack semantics enrichment - plumb the richer nack vocabulary
through. The `NackType` enum already exists in stroma `event.rs` (Discard,
RetryNow, RetryLater, RequeueNow, RequeueLater) but is reserved/dead-code: the
live wire + state path only carries the simpler `(requeue, not_before)` pair.
Goal: let a CONSUMER choose the disposition explicitly - e.g. discard/trash
immediately (straight to DLQ-or-drop, skip retries), requeue WITHOUT bumping the
retry counter, retry-now vs retry-later. This likely changes the meaning of the
current nack "verbs" on the wire and in the handler, so it needs a deliberate
pass (wire op fields + handler mapping + state transitions + client API), not a
bolt-on. Related: the TTL drop (#6) already reuses the terminal-nack ->
DLQ/discard pipeline with `DeadLetterReason::Expired`, which is the first
non-TerminalNack reason flowing through that path - a useful precedent for
threading explicit dispositions.

Usefulness read (value, anchored to Fibril being a work queue - consumed=gone):
- Create + purge: HIGH value, low effort. Round out the admin board (we can
  repartition / set DLQ today but not create or empty a queue) and cover common
  ops needs. Do first.
- Message TTL (+ the expiring publisher): HIGH product value - "do not process
  stale work" is core work-queue semantics (a RabbitMQ flagship). Higher value
  than its cheapest-first slot implies, so consider pulling it forward right after
  the admin-thin wins.
- Time retention: MEDIUM. A backlog safety valve for slow/dead consumers, DLQs,
  and delayed messages. Less central than in a log system since acked messages
  are already gone.
- Hide-inactive + search: MEDIUM, scales with queue count. Low-effort QoL win.
- Delete: MEDIUM-HIGH. Closes the lifecycle / orphan-cleanup gap, but
  safety-sensitive, so deferred for now.
- Queue expiration (declare-time idle-TTL): MEDIUM / niche. Great for ephemeral
  per-session queues (RPC reply, per-client), and needs delete first.

Also queued (correctness): the split-brain adversarial test (see "Split-brain"
under Correctness and durability) - assert a returning stale owner's write AND
replicate at an epoch below the local partition epoch are both rejected. The
mechanism is in place (epoch bump in placement + persisted per-log epoch fence +
the demotion fix aea4d50). This is the missing proof. Non-trivial (needs a
replication + stale-epoch-apply harness), so it lands after the admin-thin items.

## Features (replication-related)

- Plexus: fan-out / stream channel type. See the dedicated "Plexus streams"
  design section below. [DN]

## Plexus streams (fan-out channels) - BUILT (kept as design reference)

Plexus = a fan-out / stream channel type beside the work queue. Every consumer
sees every message, vs the queue's consumed=gone. Selected per channel via
`declare(type: queue | plexus)`. NOW BUILT end to end (the BUILD ORDER steps 1-4
below are all shipped, including durable replication; step 5 topology-as-a-stream
stays deferred as #52). Kept here as the design reference. Supersedes the older
sketch in archive DESIGN_NOTES.md 584-661.

LAYER BOUNDARY (firm):
- stroma stores the retained record log (reuse) plus a retention policy plus a
  small named-cursor map (name -> committed offset). It has NO concept of
  consumers, subscriptions, or fan-out. A cursor is an opaque named bookmark, not
  a consumer.
- fibril owns consumer matching, fan-out delivery, the newest-X in-memory ring
  (live tail), the express lane plus confirm timing, fan-in, per-sub filtering,
  and the consumer -> cursor-name mapping.

STROMA StreamEngine (new, minimal, much smaller than the work-queue engine):
- Plugs into the SAME substrate as the queue engine (per-partition keratin
  message log + event log + snapshot + replication). No lease/ack/inflight/ready/
  DLQ/TTL-redelivery.
- State: cursors {name -> offset}, retention config, head/tail watermarks.
- Events: CursorCommit{name, offset} and retention truncation (reuse
  safe_message_truncate_before). They ride the existing event-log + replication
  path, so failover restores cursors.
- API to fibril: append (reuse), read(from_offset, max) (reuse the replication
  read path), commit_cursor(name, offset), get_cursor(name), plus a retention
  worker. Snapshot encodes the cursor map.
- Build via a surgical substrate/engine seam: formalize the trait the work queue
  already satisfies, keep WorkQueueEngine as-is, add StreamEngine.

FIBRIL stream actor (per channel + partition, owner-side):
- Holds the newest-X ring, the live-subscriber registry, each sub's position.
- Subscribe resolves a start position (latest / earliest / offset / N-back /
  by-time / durable-resume), backfills from the stroma log read until caught up to
  the ring window, then attaches to the live tail. Same catch-up-then-tail pattern
  the replication followers already use.
- Publish (express lane): append to the ring and multicast to live subs
  immediately, hand to stroma to persist in parallel. Confirm timing and ghost
  flag follow the durability tier. Express lane is far simpler here than for the
  queue (no lease, ack, or single-consumer selection).
- Rebuilds the ring from the stroma log tail on start or failover.

CURSOR TRACKING (decided): support BOTH. Broker-side is the RECOMMENDED default
for peace of mind.
- Broker-side durable cursor (DEFAULT, blessed): the client supplies a durable
  NAME (trusted, gated by auth, the Kafka group.id / JetStream durable norm, no
  broker cookie needed). The broker persists and replicates the cursor, advances
  it on ack (at-least-once), and lets a fresh process resume anywhere after a
  crash or redeploy. This is the "name it, it remembers, no offset bookkeeping"
  reassurance most users want. An optional single-active-consumer lease on the
  name stops two clients clobbering one cursor (ownership, separate from identity).
- Client-driven start (OPT-IN, ephemeral or advanced): the client states a start
  position, the broker stays stateless about it, retention is the only bound
  ("beyond grace you are at the mercy of retention"). For live tails, replays, and
  fire-hose fan-out where per-message ack is unwanted.
- Two existing identity layers stay distinct: resume_token (broker-minted, for
  in-grace SESSION resume) vs durable name (client-supplied, for cross-process
  consumer resume).

DURABILITY TIERS (per-channel knob, not a third channel type):
- ephemeral: persist to the log async, do not gate delivery or confirm (lowest
  latency).
- speculative: deliver now plus an X-Speculative header, with the producer confirm
  DEFERRED until durable (fast and honest, the ghost-flag pattern from
  TODOTHOUGHTS).
- durable: persist (and replicate to min-ISR if configured) then confirm.

RETENTION wins over slow cursors. A stream drops by policy. A durable cursor that
falls behind the new head is clamped to head and flagged "lagged" to the consumer.
Confirmed by the "mercy of the retention policy" call.

SELECTION ("subscribe to one X or all") - NO broker routing (NATS subjects
rejected). Three composable dumb-broker pieces:
- channel granularity = coarse separation (the user's scaling lever).
- per-sub header FILTER = fine in-channel selection. STREAM-ONLY. Minimal fixed
  grammar: a set of header == value matches, AND-ed, with an optional `*` glob on
  the value (e.g. region == eu-*). EXPLICITLY OUT, with rationale: regex, OR/NOT,
  nesting, numeric ranges, SQL-style selectors. It is a consumer skip-predicate,
  not routing, so the broker keeps no routing state. Saves egress, not the scan.
- topology-as-a-stream = the broker streams its own facts (catalogue, topology,
  ownership, assignment changes) on a system Plexus channel. Clients subscribe and
  do their OWN routing (pattern fan-in, auto-pickup of new matching channels which
  replaces the grow-pickup and catalogue polls, load-routing later). Dumb broker,
  smart client.

  REFRAMED (2026-06-25): for LIVE ROUTING UPDATES + repartition cutover fencing,
  the mechanism is a TopologyUpdate PUSH FRAME + client ack over the existing
  connection (see #62), not a stream. A stream is the wrong tool there: subscribing
  to a topology stream needs topology to find its owner (circular bootstrap), a
  fan-out stream gives no per-client apply ack (which fencing requires), and a push
  can be targeted per connection. Clients already get the initial snapshot via
  TopologyRequest, so push deltas + ack covers live routing end to end.

  STATUS: DONE end to end (#62, #88, #89, #90). The broker pushes a TopologyUpdate
  on generation change; the Rust, TS, and Python clients apply it (replace + pool
  prune, generation-guarded) and ack the generation. The routing cache + pool are
  created before the bootstrap connection so a push sent right after HELLO lands
  with no wiring race. #90: the broker records per-connection acked generations
  (TopologyAdoptionTracker), reports the cluster minimum as a heartbeat label, and
  the repartition controller fences a cutover's finalize on cluster-wide adoption
  (drained AND adopted-or-timed-out), bounded by repartition_adoption_timeout_ms.
  Full mechanism + assumptions: website dev note "Live routing and cutover".

  #93 DONE (push efficiency): the broker now triggers the push on the routing
  CONTENT (queues + streams), not the raw coordination generation. CORRECTION to
  the original premise: heartbeats do NOT bump the generation - the openraft state
  machine absorbs label-only RegisterNode updates silently (storage.rs special
  case). The generation DOES bump on any committed metadata change cluster-wide
  (other topics' declares, transition markers, runtime settings, unrelated
  failovers), so generation-triggering pushed identical topology to every client on
  any such change; content-triggering narrows to this client's routing. The pushed
  frame still carries the live generation to ack. Because content-gated pushes stop
  firing once a cutover settles (while unrelated activity keeps bumping generation),
  adoption_generation is stamped EAGERLY at the cutover (not lazily), so a
  connection's acked generation stays at a value the gate can clear. Tests:
  broker_pushes_topology_update_on_generation_change (content change pushes),
  broker_does_not_push_topology_when_content_unchanged (churn does not).

  HARDENING (from a pathological-case review): global_topology_adoption now counts
  only LIVE nodes (heartbeat within liveness TTL). A dead node's frozen adoption
  label represented departed clients and pinned the cluster minimum down, stalling
  every cutover on the timeout. Added pure-fn tests (live_topology_adoption excludes
  dead nodes; repartition_adoption_satisfied gate decision) and a client test that
  a stale/out-of-order push is ignored but still acked with the current generation.

  topology-as-a-stream is therefore NOT the routing path. It survives only as an
  OPTIONAL higher-level DISCOVERY layer: subscribe-to-a-pattern / auto-pickup of
  matching channels (the "replace NATS subjects" vision) and a consumable catalogue
  feed for tools. Build it on the same facts ONLY if/when that discovery feature is
  wanted; it is not a prerequisite for routing or cutover. (#52)

  CLOSED 2026-06-28: #52 is done/dropped. The discovery layer shipped over the main
  protocol, not as an actual stream - catalogue snapshot + change subscription (#91)
  and pattern subscribe + auto-pickup (#92) across the Rust/TS/Python clients. A
  literal topology Plexus stream was rejected (circular bootstrap, no per-client
  ack, no per-connection targeting), so nothing of value remains. The only
  untouched sliver is a catalogue feed for EXTERNAL (non-fibril-client) tools,
  which the admin HTTP API already covers by polling.

  DISCOVERY LAYER 1 DONE (#91): all three clients expose a live cluster catalogue
  (declared queues + streams with partition counts) via a snapshot accessor
  (catalogue()) AND a change subscription (Rust catalogue_events() broadcast;
  TS/Python on_catalogue_change handler). Derived from the topology already held,
  refreshed on every full replace (push or fetch), generation-guarded, emits only
  on real add/remove/partition-count change (not owner-only churn). No wire change.

  DISCOVERY LAYER 2 (#92, NOT building yet, user-deferred): client-side rough
  routing - subscribe_pattern(glob) fans in across all matching queues/streams and
  auto-attaches newly matching channels on each catalogue change. Reuses the
  existing fan-in supervisor + grow-pickup + the Layer 1 change feed. Small glob
  grammar only (prefix/suffix *), matching the per-sub filter grammar. No broker
  change. Scope line: catalogue is declared channels + owners, NOT node membership
  or connection presence (those are a separate observability concern).

QUEUE-SIDE FILTERING: UNCERTAIN, reevaluate later, maybe. Per-sub filters break
the work queue (orphans that match no consumer plus head-of-line, starvation
across differing filters, an unbounded smart scan, inflight discipline) - the
classic JMS/ActiveMQ message-selector trap. Kept STREAM-ONLY for now, queue engine
left untouched. Guidance for selective consumption on a queue: use separate queues
or use a stream. If ever demanded, the clean version is a deliberate design
(bounded scan depth plus an orphan no-match TTL to DLQ plus fairness round-robin),
not a bolt-on.

CLIENT IMPACT: modest and additive. Reuse the Deliver frame, fan-in, and failover.
New: declare(type=plexus, durability, retention), subscribe with a start position
plus optional durable-name plus optional header filter, the speculative header,
and settle = cursor commit (durable) or no-op (ephemeral). Recommend
advance-on-ack (at-least-once) for the durable default rather than
advance-on-delivery.

FUTURE ESCAPE HATCHES (named so we do not paint ourselves in, not building now):
- a keyed-index read in stroma ("records for key=K from offset X") for efficient
  one-of-millions selective consumption. A storage index, opt-in, NOT routing.
- queue-side filtering (above), if ever justified.
- subjects/wildcards stay rejected as a broker feature.

BUILD ORDER (prerequisite chain, each step is final-form, not an MVP gate):
1. Carve the stroma substrate/engine seam (formalize the trait the work queue
   satisfies). Low-risk enabler.
2. StreamEngine in stroma (cursors, retention, apply, snapshot, replication).
3. Fibril stream actor (ring, fan-out, backfill, express lane, durability tiers,
   per-sub filter) plus broker routing by channel type.
4. Protocol and client (declare plexus, subscribe start + durable-name + filter,
   speculative header, settle = commit, fan-in reuse).
5. Topology-as-a-stream system channel plus client-side pattern fan-in and
   auto-pickup.
[DN/AUTHOR/MEM]

## Clients

- Per-client feature matrix lives at `clients/FEATURE_MATRIX.md` (Rust reference
  vs each client). Keep it updated as bricks land - it is the at-a-glance parity
  view and the checklist for any new client.
- `clients/ARCHITECTURE.md` is the language-agnostic design reference (layering,
  invariants, continuity/routing/reliability models, porting lessons). Read it
  before starting a new client (the Python client is next).
- Python client: spec LOCKED in `clients/PYTHON_CLIENT_PLAN.md` (async core +
  blocking facade, Python 3.11, build straight to full parity). Implementation is
  a deliberate fresh-context big push - not started yet.
- Future: Python 3.10 backport. 3.11 is the floor; keep the few 3.11-only bits
  (mainly `asyncio.TaskGroup`) isolated behind helpers so dropping to 3.10 is a
  localized change. 3.9 is not a target (forces future-annotations everywhere +
  older-asyncio quirks). Driven by real-world clinging to old runtimes.
- TS<->Rust feature parity: CLOSED (2026-06-23). All FEATURE_MATRIX rows are
  done for both: assignment-events stream (`onAssignmentChange`), live-grow
  partition pickup, typed `WireError` taxonomy, message TTL, producer-id dedup.
- Per-client throughput expectation: define a target each client should sustain
  (msgs/s ingress+egress at a stated payload size, confirmed vs unconfirmed),
  measure it with a small client-side bench, and record it next to the matrix so
  a new client has a perf bar to hit, not just a feature checklist. The server
  bench (`benches/`) measures the broker; this is the client-overhead view.
- TypeScript client parity pass (BIG, multi-brick): `clients/typescript` is
  basically pre-replication-branch (~3100 lines). It has the single-broker basics
  (publish/confirm/delayed, manual+auto ack, reconnect) but is behind on two big
  fronts. Brick-by-brick, foundation first:
  - BRICK 1 (prerequisite) WIRE FORMAT: DONE. The broker moved frame bodies from
    msgpack to a custom simple binary format (`crates/protocol/src/v1/wire.rs`:
    magic + field-by-field put_u16/put_u32/put_u64/put_uuid/put_str/put_bytes/
    put_headers, ~20 client-facing ops) - much faster, fixed scheduler starvation.
    Ported to TS as `clients/typescript/src/wire.ts` (byte-exact codec with
    round-trip + byte-layout tests) and a symmetric `frames.ts` adapter that maps
    protocol.ts structs to the wire bodies. codec.ts now delegates body encode/
    decode to the adapter while user payloads keep msgpack. The 20-byte frame
    header is unchanged - only bodies moved. Remaining wire fields the protocol
    structs do not carry yet (partition_key, consumer routing) default null/0 and
    get populated by bricks 2-6.
  - BRICK 2 topology + owner routing: DONE (publish path). FetchTopology op +
    TopologyCache + owner resolution per (topic, partition) + a connection pool
    keyed by endpoint + follow-redirect retry on confirmed publishes (bounded by
    maxRedirects). Mirrors the Rust pool/cache shape in idiomatic single-threaded
    TS (plain Map/object/counter, no locks/atomics). NOTE: subscribe still uses
    the bootstrap connection - routing subscribes to partition owners is folded
    into bricks 4-6.
  - BRICK 3 partitioning: DONE (publish path). routePartition (FNV-1a key routing
    + keyless round-robin) stamps partition/partition_key/partitioning_version on
    the wire, byte-exact hashing vs the broker. Still TODO: transparent
    multi-partition fan-in on subscribe (pairs with the subscribe-routing work in
    bricks 4-6).
  - FOLLOWUP (bricks 2-3): a multi-node real-broker smoke for routing/redirects.
    The unit + integration tests cover routing against the real wire codec, and
    brick 1 validated the publish path against a live standalone broker, but a
    ganglion cluster smoke that exercises a real cross-owner redirect is still
    worth adding (fits the brick 8 examples-as-light-tests runner).
  - BRICK 4 reconnect + reconcile + resume: DONE. ResumeIdentity/Outcome on Hello
    and the Reconcile* ops were already wired. The gap was the owner-restart case.
    Reconcile now fires on ANY reconnect that has active subscriptions (not only a
    resumed session) so a bounced owner's fresh session restores or closes the
    streams instead of leaving them open-but-unfed. Regression-tested. Done in
    brick 5: the supervisor now triggers a reconnect for a passive consumer whose
    connection drops.
  - BRICK 5 failover ride-through: DONE (single-partition). Publish side: transient
    owner-failover retry with throttled topology refresh, jittered backoff,
    deadline, not-found fast-fail (publishTimeoutMs), same loop as redirect-follow.
    Consume side: a subscription supervisor reads from a merged queue and
    re-subscribes to the current owner when the per-connection stream closes
    (owner death/restart), tagging each delivery with its engine so manual ack
    settles correctly. Continuity model: supervised subs own continuity via fresh
    re-subscribe and stay out of the reconcile registry (mutually exclusive with
    the brick-4 reconcile path, which is the supervision-off behavior).
    Multi-partition fan-in and graceful-owner-move detection (periodic topology
    owner-check) are now DONE too. STILL TODO (actionable, see below): picking up
    partitions added by a live grow, and lease preservation across re-subscribe
    (today an unsettled InflightMessage from a dead owner fails its ack and is
    redelivered, at-least-once safe).
  - BRICK 6 exclusive consumer groups: DONE. SubscriptionBuilder.consumerGroup/
    consumerTarget, plus cluster-scoped member-id mint-and-carry (server mints on
    the first exclusive subscribe, client latches and stamps every later one).
    Exclusivity is enforced by the broker per-partition gate. The AssignmentChanged
    push is consumed as an assignment-events stream (DONE, parity closed 2026-06-23).
  - BRICK 7 reliability: DONE. retryAdvice/isRetryable classification, the
    reserved-namespace header guard, and a ReliablePublisher that stamps producer
    id + monotonic seq (fibril.client.*) and retries until confirmed. At-least-once
    today. Effectively-once once the broker dedups on those keys (both sides TODO).
  - BRICK 8 examples-as-light-tests: DONE. Self-validating examples/*.example.ts
    (roundtrip, confirmed-delayed, manual-ack-retry, stream) + run-all.sh that
    starts a broker and runs them all. Continuous examples take --check for a
    bounded validated burst. Wired into CI (typescript-client-ci "examples" job)
    by reusing the published fibril-server image rather than building from source.
    STILL TODO: a multi-node cluster smoke for a real cross-owner redirect (needs
    a ganglion cluster, not just one container).
  - ACTIONABLE client gaps - now DONE (TS/Rust parity closed 2026-06-23, Python
    followed). Both items below shipped across all clients; kept as a record:
    - Live-grow partition pickup: the fan-in growth poll subscribes to partitions
      added by a live grow (FEATURE_MATRIX "Failover resubscribe + live-grow pickup").
    - Assignment-events stream: AssignmentChanged is decoded and exposed
      (onAssignmentChange / assignment_events; FEATURE_MATRIX "Assignment events
      stream"). Observability, not correctness (the per-partition gate enforces
      exclusivity).
  - Effectively-once: TWO paths. (a) CLIENT-ONLY and actionable - a consumer-side
    dedup helper that skips already-seen (producer_id, seq); the headers already
    reach the consumer, so no server change is needed. (b) SERVER-GATED - broker
    dedup that drops dups at publish (only a "read by broker producer-dedup later"
    comment exists). At-least-once today until one of these ships.
  - SERVER-GATED client items (build the client half when the server side lands):
    - Lease preservation across re-subscribe (a shared at-least-once limitation).
  Pairs with AUDITS.md "Client API parity" and the client reliability docs item.
  [AUTHOR/AUDIT]
  See clients/ARCHITECTURE.md for the design reference and clients/FEATURE_MATRIX.md
  for status. Next client: Python.

## Code health and structure

- DONE 2026-06-29: `plan_local_assignment_transitions` (coordination.rs) now
  sorts keys by borrowed `&str` (topic/group `as_str()`/`as_deref()`) instead of
  allocating a `(String, _, String)` tuple per comparison, so the transition
  planner sort is zero-alloc.

- Routing/pattern-subscribe parity: DONE on Rust, TS, async + blocking Python.
  Integration coverage: Rust (static queue fan-in), TS (queue fan-in AND
  auto-attach via a pushed topology update), Python (queue fan-in). Remaining
  smaller gaps: (a) no auto-attach integration test for Rust or Python (TS proves
  the shared design end to end. The watcher rides the already-tested catalogue
  feed); (b) no STREAM-pattern integration test in any client (the wire mocks do
  not exercise the stream-subscribe + deliver path for a pattern); (c) assess
  whether pattern subscribe deserves its own docs page rather than the clients.mdx
  entry.

- ACK WINDOW: BUILT 2026-06-28 (keratin e952542 settled RangeSet + 78de9ef
  is_settled rename). The settled set replaced the bitset window, the frontier is
  derived, and the dead ack-window command/handle surface was deleted.
  FORMAT_VERSION bumped to 4. Residual follow-ups: (a) DONE 2026-06-28 (keratin
  cd738d3 + fibril af21b63) - renamed `lowest_unacked_offset` ->
  `lowest_unsettled_offset` and the sibling `lowest_not_acked_offset` ->
  `lowest_not_settled_offset` (plus the GetLowestUnacked / GetLowestNotAcked
  commands and the broker `partition_lowest_unsettled_offset`) across both repos.
  (b) Broader dead-command audit: several
  `QueueCommand` query variants (e.g. GetLowestUnacked, GetLowestNotAcked,
  GetNextDeliverable, GetRetries, FilterNotEnqueued) may have no external handle
  caller now that the broker surface has narrowed - sweep for handle methods with
  zero callers and trim. The original assessment is kept below for the record.

- Dead-command audit (2026-06-28, keratin 7c51a96): swept every command-wrapping
  WorkQueueHandle method for callers across keratin + fibril. Removed the six
  batch wrappers with ZERO callers anywhere (enqueue_many, enqueue_delayed,
  enqueue_delayed_many, ack_many, nack_many, mark_pending_dlq_many) - the broker
  builds those commands directly on the apply path, so the wrappers were
  redundant. Their command variants + state methods stay (the apply path uses
  them). The named suspects (GetLowestUnacked / GetLowestNotAcked /
  GetNextDeliverable / GetRetries / FilterNotEnqueued) turned out LIVE - their
  handle methods have callers. LOWER-CONFIDENCE candidates left for review (each
  has a single keratin caller that is a test or an admin-only Stroma API the
  broker does not exercise, so they are test-covered surface rather than clearly
  dead): retries, filter_not_enqueued, is_inflight, dump_inflight,
  collect_ttl_expired, mark_inflight_batch, canonical / debug_dump_queue,
  inspect_offsets. Decide per item whether the Stroma API is intended product
  surface or can go with its test.
  - REVIEWED 2026-06-28 (keratin bb322d6). Removed the two genuinely dead chains:
    `filter_not_enqueued` (Stroma method + handle + QueueCommand::FilterNotEnqueued
    + state method, zero callers anywhere) and `count_inflight` (Stroma wrapper,
    zero callers; the underlying `inflight_len` handle stays, used by has_inflight
    and evict). Kept the rest as live or test-covered: `inspect_offsets` /
    `inspect_messages` (admin, used by fibril), `collect_ttl_expired` (the TTL
    worker), `dump_inflight` (via `validate`, a crash-recovery invariant test),
    `retries` / `is_inflight` (queue-state assertions in dlq tests),
    `canonical` / `debug_dump_queue` (snapshot round-trip assertions in replay
    tests), and the `mark_inflight_one` / `mark_inflight_batch` test-helper block.
  - QUEUE-ENGINE pass 2026-06-28 (fibril 05ce6a6): swept the broker `QueueEngine`
    + `StreamStore` traits and `StromaEngine` inherent methods. Every QueueEngine
    trait method has a live broker caller. Removed three dead ones:
    `StromaEngine::become_queue_owner` (the epoch-fenced
    `become_queue_owner_with_epoch` is the live owner-activation path), the
    singular `StreamStore::commit_stream_cursor` (superseded by the batched
    `commit_stream_cursors` from the cursor-commit microbatcher, #83), and
    `StromaEngine::stream_replication_next_offsets` (never invoked). Note: the
    keratin `Stroma::commit_stream_cursor` it forwarded to stays - it is exercised
    by keratin's own streams tests.

- ACK WINDOW assessment (2026-06-28): KEEP it, it is load-bearing. In
  `QueueInternalState` (keratin stroma/core/src/state.rs) the ack state is
  `settled_until` (the contiguous acked-prefix frontier) plus an out-of-order
  record `ack_window_base` + `ack_bits` (a fixed `ACK_WINDOW`-wide bitset). On
  `ack(offset)`: an ack at the frontier advances `settled_until` then
  `advance_frontier` slides the bitset absorbing the now-contiguous run; an ack
  ABOVE the frontier sets a bit so the frontier can advance through it later. So
  the window is exactly the out-of-order ack tracker that makes `settled_until`
  contiguous and monotonic.
  - NOT derivable from the inflight set: an out-of-order ack removes the offset
    from `inflight`, so without the bitset there is no record that it was acked
    (vs never-existed) and the frontier could not advance through it.
  - NOT redundant with the frontier alone: the frontier is only the contiguous
    prefix, out-of-order acks live strictly above it.
  - What it OFFERS: a bounded, snapshot-able, replicable record of out-of-order
    settlements driving a contiguous monotonic `settled_until`, which gates
    message-log retention/truncation, delivery start, and the replication
    confirm. The byte-blob accessors (`ack_bits_bytes` /
    `SetAckWindowFromBytes`) + `ack_window_base` exist to ship that state in
    snapshots / state-checkpoints and to followers.
  - Refined direction (2026-06-28): replace the fixed-size bitset window with an
    unbounded `settled: RangeSet<Offset>` of terminal settlements ABOVE the
    frontier (mirroring `ready: RangeSet`). Key points:
    - Rename ack -> settle. `ack()` is the shared frontier-advance for ack,
      terminal nack, and DLQ commit (it is called from nack_at and
      dead_letter_commit, "reuse existing frontier-advance logic"), so the
      structure already tracks all terminal settlements, not just acks. The
      honest name is `settled` + `settled_until`.
    - Store the WHOLE settled set from 0, not just the part above the frontier.
      With a RangeSet `[0, frontier)` is a single `(0, N)` entry regardless of N,
      so including the prefix costs one range and makes the set the single source
      of truth. (With the old bitset window the frontier HAD to be separate
      because the window only covered a bounded span above it. The RangeSet
      removes that constraint, so this is now a semantics call, not a cost one.)
    - No window. A RangeSet is unbounded, so the fixed `ACK_WINDOW` cap and its
      far-ack in-memory-drop divergence both go away (this subsumes the old
      follow-up about documenting the cap).
    - Shape: `is_settled(o) = settled.contains(o)`; `settled_until()` is DERIVED
      as the end of the range covering 0 (else 0). The frontier advance is free:
      inserting a settled offset adjacent to the `[0, frontier)` range coalesces
      it, so the explicit `advance_frontier` slide logic disappears. No separate
      `settled_until` field to keep in sync.
    - Hot-path note: deriving the frontier is a first-range lookup rather than a
      bare field read, but the range count is tiny (about out-of-order span + 1),
      so it is cheap. If a hot path ever needs a bare read, cache the frontier as
      a pure memo of the set (recomputed on insert), not as a second independent
      field.
    - Cost / deciding factor: this is a snapshot + replication FORMAT change. The
      state-checkpoint and follower sync ship `ack_window_base` + `ack_bits_bytes`
      today; they would ship the ranges instead, needing a `STROMA_VER` bump and
      replacing the `SetAckWindow` / `SetAckWindowFromBytes` / `ack_bits_bytes` /
      `ack_window_base` command + client surface with a `settled`-range
      equivalent. The in-memory swap to RangeSet is the small part.

- Code-TODO triage (from a quick scan, ~58 in fibril crates + ~37 in keratin/stroma. Most
  are minor inline idea-markers, the full sweep is the #63 hygiene pass). The
  few worth elevating rather than leaving buried in comments:
  - `protocol/.../handler.rs` publish drowning out delivery - LARGELY SOLVED by
    cursor-commit microbatching (#83), so the inline TODO is now a NOTE. Re-examine
    only if delivery fairness regresses under heavy publish (no action otherwise).
  - `broker/src/broker.rs` x3 "do not keep handle (memory leak) ..." - INVESTIGATED,
    NOT a genuine leak (TODOs were stale, now reworded). They are per-QUEUE tasks
    (publisher_sink, confirm_sink, delivery_loop), not per-connection. TaskGroup
    wraps tokio_util TaskTracker, which tracks only an in-flight count and does not
    retain JoinHandles, so finished tasks are reaped. All three break on
    owner_runtime_shutdown (fired by cancel_owner_runtime on demotion/eviction) and
    the TaskGroup cancel token (broker shutdown). Residual is only an invariant:
    every owner-runtime teardown must call cancel_owner_runtime (it does today).
  - `keratin-log/src/writer.rs` "tests showing guaranteed order" + "more tests for
    failures and edge cases (batch flush on shutdown, etc.)" - test-coverage gaps in
    the durability-critical writer, plus the noted "more pipelining" lever.
  - Already tracked elsewhere (no action): the snapshot-cadence FIXME (stroma.rs ->
    "Snapshot cadence" item), the BrokerConfig builder TODO (the item below), and the
    failover-retry-constant knobs (settings tiering). Stale comment trimmed: the
    client reconnection-path "possibly resubscribe/redeliver" TODOs were superseded
    by the failover supervisor + reconcile and reworded in place. [AUTHOR]
- Convert wide config structs (starting with `BrokerConfig`, and `StromaOptions`)
  to a builder pattern. They are currently constructed with exhaustive struct
  literals across many call sites (tests, replication, main), so adding a field
  churns all of them and discourages putting new tunables in config (e.g. the
  stream ring/live-channel sizes are module consts in broker.rs as a result). A
  builder with defaults lets new fields land without touching existing call sites,
  and is the clean home for those stream tunables. [AUTHOR]

- Rework the `tui-example` (`crates/tui-example`): a small TUI app that connects
  to a broker and visualizes messages (packs) flowing in and out. It has disabled
  instrumentation (latency tracking + compute_stats were dead, removed in the
  dedup sweep). Bring it back to a clean, illustrative live-client demo. Also
  `benches/bin/bench_e2e.rs` is half-disabled (dead channels, hardcoded
  reporter/broker params) and wants the same treatment. [AUTHOR]
- `stroma.rs` by-concern file split: a readability refactor independent of
  clustering. A full module sketch is preserved in the archived worklog. Do the
  low-risk type modules first, then the engine impl split incrementally. [WL]
- substrate-versus-engine split (WorkQueueEngine plus StreamEngine over a shared
  substrate), which enables Plexus. Now spec'd as build step 1 in the "Plexus
  streams" design section above. [DN]
- Optional de-raft finish: fibril-side de-raft is complete. Remaining is optional,
  routing the protocol dev-dep and coordination-ganglion through the `ganglion`
  umbrella crate, or the bigger approach-B of moving raft-node construction up out
  of fibril entirely. [WL]
- Ganglion domain hygiene: keep all coordination-domain code in ganglion so fibril
  depends on a stable surface, not internals. Ongoing. [DN/MEM]
- Clustering-module separation: done for broker, stroma-core, and client (protocol
  was already done). Kept here only as a reference point. [MEM/DN]
- Replace `::MAX` config branches with `Option`s for clearer semantics, and a
  mutex-refactor pass (concurrency-primitive discipline). [WL]
- Assess persisting the queue catalogue (which queues plus path) in the stroma
  store instead of filesystem discovery. Filesystem discovery is likely good
  enough. [WL]
- Low priority: `lifecycle_locks` map pruning plus a bench (rare optimization, no
  latency or throughput impact). [WL]

## Docs

- Client reliability example or tutorial on the docs site (`clients.mdx`):
  confirmed publish with the `is_retryable`/`retry_advice` match pattern, the
  ReliablePublisher opt-in, producer ids and the dedup path, and a short
  failover-behavior note. Keep it copy-paste-able. [WL]
- Manual failover runbook (partially covered by `FAILURE_MODES.md`). [PLAN]
- Keep the docs current in the SAME change as any user-visible surface change
  (protocol/client API, admin endpoint/page, CLI, config/runtime setting,
  behavior/limit): update `website/.../implemented-surface.md`,
  `clients/FEATURE_MATRIX.md`, the relevant `website/.../` guide, `README.md` if
  it lists the feature, and the "Done since the inventory was last curated"
  section. Last full recuration: 2026-06-22.

## Testing and hardening

- Chaos soak harness: `scripts/cluster-tryout.sh --chaos` runs repeated mixed
  faults (kill+rejoin and SIGSTOP/SIGCONT pause) under confirmed load and asserts
  zero loss plus reconvergence. It found (and now passes after the fix for) the
  owner-bounce consumer-resume bug. Each round deterministically faults the
  topic's current owner with the consumer connected outside the replica set, so a
  run reliably exercises the recovery path. Manual diagnostic, not in CI. A future
  step would be wiring a trimmed version into CI as a nightly soak. [chaos]
- Adversarial tests through all layers, plus a realistic chaotic benchmark
  (bursty, non-steady supply, consume, and bandwidth, not steady saturation). [WL]
- Cluster benchmark profiles: replica-durable confirms, follower catch-up,
  partitioned fan-in, and redirects. [AUDIT]
- A test pass to ensure tests pin correct behavior, not current bugs. [WL]
- Revisit the audits in [AUDITS.md](AUDITS.md) and harvest anything still
  actionable (several are Audited with open Next items). [AUTHOR]

## Far horizon (v2+)

- Multi-region and geo placement: region and zone labels feeding the planner,
  per-queue placement hints, region-aware placement strategy. [PLAN]
- Load-aware placement and routing: node and partition load scores (advisory,
  off the coordination log, hysteresis, power-of-two-choices), plus a consumer
  scheduling policy and a per-consumer override of the global partition target. [DN/MEM]
- Leadership health transfer (working name "abdication"): let the coordination
  (raft) leader hand off leadership when it is the bottleneck, so the control
  plane does not run on a degraded node. NEEDS EXAMINATION, not committed. Notes:
  - Scope: this is the CONTROLLER leader (commits placements, declares, failover
    decisions, settings, membership), NOT the message hot path (queue/stream data
    replication has its own caught-up-follower failover). So "cannot keep up" is
    almost never throughput saturation, it is a degraded leader node (slow log
    fsync, pegged CPU, noisy neighbor). The real harm is delayed failover planning
    and repartition cutovers, the brain reacting slowly to OTHER failures. That is
    the resilience win: do not run the brain on the sickest node.
  - Core discriminator (makes or breaks it): leader-LOCAL slowness vs CLUSTER-WIDE
    slowness. Transfer only helps the former. The trigger must be "I am slow AND a
    specific follower is demonstrably healthier and caught up", not absolute commit
    latency, or it ping-pongs between equally slow nodes.
  - Signals to combine: openraft 0.9 self-metrics (quorum-ack / time-to-commit
    latency, per-follower replication lag, millis_since_quorum_ack), local log
    fsync latency, and controller-loop schedule drift (actual vs expected cadence,
    the user's "starting updates on schedule" intuition: sustained drift, not a
    blip).
  - Guards (non-negotiable): use graceful leadership TRANSFER to a chosen caught-up
    voter (openraft transfer), never a bare step-down (which risks re-electing the
    same slow node or a lagging one, with an election gap). Never transfer to a
    non-caught-up follower. Minimum leadership tenure, cooldown, hysteresis, a
    global transfer rate limit, and anti-ping-pong memory. All thresholds as config
    settings. Start ADVISORY (log "would transfer to N because X", surface health
    scores in the admin topology panel), measure, then enable.
  - Fit: phase-2 of the load-aware direction above (reuse the node load/health
    scoring substrate rather than a bespoke mechanism). Gate on a feasibility check
    of openraft 0.9's leadership-transfer API and metric access, and instrument
    what actually causes leader slowness in practice BEFORE building the policy. [AUTHOR]
- Live repartitioning beyond fixed-at-create: partition_count is already
  versioned and routing is version-parameterized. The hard deferred semantic is
  per-key ordering across a resize. [PLAN/MEM]
- Transactional or cross-partition writes. [PLAN]
- Self-hosted metadata to replace external coordination. Explicitly deferred. [PLAN]
- Follower reads: rejected for the work-queue model (no analog). [PLAN]

## Non-replication track (own roadmap)

These elevate Fibril feature-wise but are tracked separately, not on the
replication roadmap. Raw notes are preserved in
`archive/replication-sharding-plan/TODOTHOUGHTS.md`.

- TTL and message expiration, time-based retention, queue purge, and queue
  deletion lifecycle (the author's first post-wrap nice-to-haves).
- Broker restart reconciliation and update reconciliation: persistent session
  continuity built on the existing reconnect model. Noted as the most distinctive
  of the bunch.
- In-memory (non-durable) queues via a pluggable Keratin write target.
- Express-lane and speculative delivery with deferred publisher confirmation.
- Client opt-out of convenience features and client-enforced rate limits.
- Settings proverbs (a delight touch on the durability override), more Keratin
  writer pipelining, topic and node id interning, dashboard QoL such as hiding
  inactive queues plus search, and a RabbitMQ-compatibility easter egg.

# Replication and Sharding Work Log

Branch: `replication-sharding-plan`

This is the running feature log for replication, partition ownership, and
sharding work. It should record what was tried, what was rejected, what worked,
and what remains pending.

## Starting Direction

Replication and sharding should be designed as one cluster model:

- a partition has exactly one owner serving client traffic
- a partition can have multiple followers applying replicated data
- a node can own many partitions and follow many other partitions
- coordination decides ownership and follower assignment
- Keratin remains a local log library, unaware of peers or elections
- Stroma owns queue role behavior, because queue semantics live there
- Fibril broker routes client traffic to the owner or returns a not-owner error
- Stroma is an engine that can host many queue identities. Roles should be per
  queue identity `(topic, partition, group)`, not one role for the whole Stroma
  instance.

Keratin must stay generic. It may expose local log primitives useful for
replication, checkpoints, fencing, and repair, but it should not know about
Fibril brokers, Stroma queues, cluster membership, coordination leases, or
application-level ownership. Those concepts belong above it.

The first implementation focus should be replication-safe storage primitives,
not network transport. If a follower cannot apply leader-assigned offsets
locally, every higher layer would be built on the wrong foundation.

## Current Findings

- Keratin currently assigns offsets inside `Log::stage_append` and
  `Log::stage_append_batch`.
- Keratin records already store offsets on disk, and readers can scan/fetch
  records with offsets.
- The missing primitive is an append path that accepts caller-provided offsets,
  validates the range, and writes the records without assigning new offsets.
- Stroma currently writes message records first, then appends events that refer
  to those message offsets. This can support replication as long as delivery
  remains event-log driven.
- Stroma queue handles are per `(topic, partition, group)`, which is compatible
  with per-partition role and ownership.
- Stroma now has a minimal follower-ingest API for a queue identity. It accepts
  caller-offset message batches and caller-offset event batches, requires the
  queue to be in follower role, writes through Keratin replicated append, then
  applies the replicated events to local queue state through the internal replay
  path.

## Decisions

- Use external coordination long-term, with etcd as the likely target.
- Static config is acceptable for the first local/in-process milestone if it
  does not leak into the storage or queue APIs.
- Fibril should ideally own cluster metadata eventually. External coordination
  is a practical starting point, not the desired final shape.
- Replication is pull-oriented unless later evidence says otherwise.
- Followers must not assign offsets.
- Followers must not run independent queue-time decisions such as expiry or DLQ
  spawning.
- Do not expose offsets as client-facing routing concepts.
- Keratin replicated append should be exposed through a deliberately gated API.
  A type-level split between leader-write and follower-apply handles is worth
  exploring, but should not block the first primitive if it becomes too heavy.
  An extension trait or separate writer command can still make the unsafe path
  explicit and hard to call accidentally.
- Keratin durability should stay local. `AfterReplicated` does not belong in
  `KDurability`; replication quorum semantics belong in Stroma or broker-level
  policy.
- Checkpoint and snapshot installation semantics belong in Stroma. Keratin can
  reset a local log continuation point and append caller-assigned records, but it
  should not know what a Stroma snapshot means or how a queue catches up.
- Normal replicated append remains contiguous. If a follower is too far behind
  because old log ranges were truncated, it should install a checkpoint or
  snapshot and then continue from the checkpoint offset. It should not fill gaps
  with fake records.
- Keratin role/mode is runtime API protection, not durable truth. On restart,
  higher layers should decide how to set local log modes after consulting their
  coordination or configuration source.
- Type-level owner/follower Keratin handles are not automatically safer while
  handles can circulate. The problem is not only `Arc<Keratin>`; any cloneable or
  otherwise retained handle can outlive a promotion. A type-only design would
  need a way to prove all old handles are gone before promotion, or else every
  handle still needs to observe shared runtime state. Until that lifecycle is
  solved, runtime role checks are the more honest guardrail.
- Stroma uses the same principle at the queue layer. Queue role is shared
  runtime state under each `QueueHandle`, with a generation counter for future
  transition fencing. Typed owner/follower wrappers may still be useful as API
  shape later, but they cannot replace runtime checks while queue handles can
  be cloned and held by background tasks.
- Stroma owner operations must reject follower or frozen queues before durable
  event/message appends. Recovery and future replicated ingest may use internal
  replay paths that bypass ordinary owner checks, because followers need to
  apply decisions made by the owner.
- Leader-only queue-time decisions should be phrased as owner-only in Stroma:
  expiry collection, delivery leasing, DLQ spawning, and DLQ commit appends.
  Followers may still snapshot and truncate local data once those paths are
  safe, because that does not create new queue decisions.
- Replication should carry the owner's durable log decisions. A follower should
  record and apply replicated message/event logs, but it should not independently
  execute owner side effects. In particular, DLQ spawning/copying is an owner
  decision. A follower only applies the resulting replicated events and message
  records. A source-queue follower still applies source events such as
  `DeadLetter` and `DeadLetterCommit` so its local source state matches the
  owner. The copied DLQ payload is replicated through the DLQ queue's own
  message/event logs, so only followers of that DLQ queue apply the DLQ message
  itself.

## Phase Plan

1. Done: validate Stroma freeze/drain semantics with real durable paths. Cover publish
   and event completions first, then add DLQ-specific coverage when there is a
   deterministic owner-side hook rather than a timing guess.
2. Done for local mechanics: define the Stroma replication surface around owner
   append, follower replicated ingest, owner freeze before role change, checked
   follower promotion, and owner demotion. Keep it in queue terms only.
3. Done: add follower ingest in Stroma. It should use Keratin replicated append,
   reject ordinary owner traffic, apply replicated queue state, and avoid
   owner-only side effects.
4. In progress: define local promotion and demotion checks. A follower can now
   be promoted only when its local message and event log tails exactly match the
   externally supplied expected tails, and its event state is applied through the
   event tail. An owner can now be demoted by freezing owner traffic, draining
   accepted work, recording local tails, and switching the queue and Keratin logs
   to follower mode. Cluster fencing and handoff ownership still belong above
   Stroma.
5. In progress: add the first broker ownership model. The broker now has a
   queue ownership provider interface and a static test implementation. Publisher
   and subscriber creation are rejected before queue materialization when this
   node does not own the queue. Protocol-level not-owner frames and real
   coordinator-backed ownership are still pending.
6. Prototype replication transport. Prefer pull from follower to owner for the
   first version because the follower best knows its local offset and checkpoint
   state.
7. Add operator visibility after the mechanics exist: queue role, replicated
   offset, owner offset, lag, freeze/drain state, and promotion/refusal reasons.

## Current Work Plan

This section should stay in sync with the active implementation plan so work can
resume after context compaction.

1. Done: Keratin exposes generic replicated append and checkpoint primitives.
   Keratin remains a local log library and does not know about Fibril, Stroma
   queue ownership, replication topology, or coordination.
2. Done: Stroma exposes local queue roles and role guards. Owner operations are
   rejected on follower and frozen queues, and normal expiry work skips non-owner
   queues.
3. Done: Stroma freeze/drain protects role transitions. Freeze blocks new owner
   work, accepted owner work is allowed to complete under its operation lease,
   and tests cover publish, ack, NACK, and DLQ continuation paths.
4. Done: Stroma follower ingest applies replicated message and event batches to
   follower queues. Source queue DLQ events stay source-local, and DLQ payloads
   are replicated through the DLQ queue's own logs.
5. Done: Stroma checked follower promotion verifies exact local message and
   event tails plus applied event state before switching to owner.
6. Done: Stroma checked owner demotion freezes, drains, records local tails, and
   switches queue plus Keratin logs to follower.
7. In progress: broker ownership gate. The broker has a sync ownership provider
   interface suitable for a future coordinator watch cache, with `OwnAllQueues`
   as the default and `StaticQueueOwnership` for tests and early wiring.
8. Next: map broker `NotOwner` into protocol/client-visible errors, still
   without designing full routing or forwarding.
9. Next: expose owner log read APIs needed by follower pull replication. The
   first transport should let followers ask owners for message and event batches
   from known offsets.
10. Next: prototype follower pull replication and local catch-up loop, then use
    checked promotion/demotion APIs for handoff tests.
11. Later: replace static ownership with coordinator-backed ownership, likely
    based on an etcd-style lease/watch model.
12. Later: admin and metrics visibility for queue role, local offsets,
    replicated offsets, lag, transition state, and refusal reasons.

## Pending Decisions

- Whether Keratin should ever expose distinct handle types for owner logs and
  follower logs. Runtime checks are the current choice because circulating
  handles make type-only promotion unsafe without a handle-drain protocol.
- How much snapshot installation belongs in Keratin versus Stroma.
- Whether `destructive_reset_to_checkpoint` is enough for the first follower
  snapshot path. Richer snapshot bytes/checksums should be designed in Stroma,
  with Keratin remaining a local log primitive.
- Whether normal owner append should also carry an epoch, or whether the higher
  layer should advance/fence the Keratin epoch before entering owner mode. The
  safer intuition is to always supply epoch, but the current Keratin owner append
  path intentionally stays minimal until Stroma role wiring clarifies the API.
- Whether the old `Coordination` trait should be removed or replaced with the
  queue ownership provider once real coordination starts. Its leader terminology
  does not match the current owner/follower partition model.
- Exact etcd deployment assumptions are not decided. The likely production shape
  is a small odd-numbered etcd cluster that stores ownership leases and epochs,
  while broker nodes watch keys and maintain a local ownership cache. A single
  etcd container remains fine for local development and early integration tests.
- Exact epoch/checkpoint handoff during Stroma role transition. The local
  freeze/drain primitive exists, but the higher layer still has to decide when
  Keratin epochs advance and what checkpoint state is installed before a queue
  becomes follower or owner.
- Whether Stroma owner-operation leases should stay on every owner operation or
  move outward to larger batch boundaries if benchmarks show measurable
  overhead.
- First sharding metadata shape for static config and later etcd.
- What the migration path is from external coordination to Fibril-owned metadata
  without forcing a data-path rewrite.
- Not-owner error shape and topology refresh story for clients.

## Proposed First Milestones

1. Add Keratin replicated append primitives and tests.
2. Add Keratin epoch metadata and stale-epoch rejection.
3. Add Stroma queue role and write rejection on follower.
4. Add role-aware Stroma background tasks.
5. Add in-process leader/follower replication test with fake transport.
6. Add broker partition ownership routing with static config.
7. Add not-owner errors and client retry/topology refresh hooks.
8. Add real coordination and replication transport.

## Things Not To Do Yet

- Do not build automatic failover first.
- Do not put networking or coordination into Keratin.
- Do not make followers serve reads.
- Do not build around client-visible offsets.
- Do not optimize replication throughput before correctness tests exist.
- Do not make `Frozen` mean "allowed while some owner operation is active".
  That would let unrelated new writes slip through during a transition.

## Stroma Transition Notes

The first role guard is intentionally not the full role transition protocol.
The transition hazard is async owner work that has already crossed part of the
durable path:

- `append_message_batch` queues message-log writes, then `MsgBatchCompletion`
  later appends the matching enqueue events.
- single-message append spawns a task after message-log completion before the
  enqueue event is appended.
- `ApplyThenComplete` applies ack/nack-style events from a completion callback.
- `dlq_copy_then_commit` can outlive the request that spawned it, then append a
  DLQ commit for the source queue.
- periodic snapshot tasks hold queue handles, but snapshots are not owner-only
  decisions and can remain role-neutral as long as truncation stays safe.

A correct owner-to-follower transition should therefore look like:

1. Set the queue role to `Frozen` so new public owner operations fail before
   durable appends.
2. Stop or fence owner-only background work for that queue, including expiry
   decisions and DLQ spawning.
3. Wait for owner-operation leases that were acquired before the freeze.
4. Let those leased operations finish their already-started durable sequence,
   including the event-log append that corresponds to an accepted message-log
   append.
5. Advance or install the appropriate epoch/checkpoint state.
6. Switch the queue to `Follower` or back to `Owner`.

The lease should be explicit. It can be a small shared counter plus notify under
`QueueHandle`, but applying already accepted owner events will probably also
need an internal event-apply path that is authorized by the lease rather than by
the current role bit. Otherwise a freeze that begins after a message log append
but before the enqueue event would create orphan payload records. Orphan
payloads are not corruption if delivery stays event-log driven, but transitions
should still avoid creating them as normal behavior.

Performance note: the first lease implementation uses atomic accounting on
owner operation entry/exit. That is the right correctness-first shape, but it is
a possible point of friction for hot publish, delivery, ack, and nack paths. If
benchmarks show regression, move lease acquisition outward to larger Stroma
batch boundaries or specialize the hottest paths. Do not remove the transition
guard without replacing it with an equivalent drain mechanism.

Tests needed before implementing transition:

- freeze waits for a deliberately blocked message-batch completion and then
  permits the matching enqueue event to finish.
- new publish, ack, nack, delivery leasing, and declare calls reject once frozen.
- DLQ copy spawned before freeze either completes under lease or is cleanly
  rejected and left recoverable.
- periodic snapshot can keep running on a follower without creating owner-only
  events.

## Log

- 2026-06-09: Created feature branch in Fibril and Keratin. Read
  `REPLICATION_PLANNING.md` and inspected Keratin append/read paths plus Stroma
  queue append paths. Conclusion: first useful slice is Keratin replicated
  append with explicit offsets and gap rejection.
- 2026-06-09: Added direction that etcd/static config are stepping stones.
  Longer term, Fibril should own metadata if that can be done without making the
  early data-path replication more complex. Also noted the preference for
  type-level or otherwise gated Keratin access so follower offset application is
  explicit and not mixed into ordinary owner appends.
- 2026-06-09: Implemented the first Keratin replicated append primitive on the
  Keratin branch. It supports exact-fit append, gap reporting, already-present
  reporting, and an explicit `AppendSuffixAfterKnownPrefix` mode for appending
  only the missing suffix after an overlapping known prefix. Removed
  `AfterReplicated` from Keratin durability. Normal replicated append still
  refuses true gaps; followers that missed truncated history need a separate
  checkpoint/snapshot install operation that sets the log continuation point.
- 2026-06-09: Added Keratin `reset_to_checkpoint(next_offset)`. It clears local
  log segments, creates a fresh active segment at the checkpoint offset, updates
  head/tail/durable watermarks, and persists the manifest. This gives follower
  catch-up a clean "install checkpoint, then apply from here" primitive without
  making ordinary replicated append sparse. Also fixed scans that start before
  the first retained segment so they advance to the first available record.
- 2026-06-09: Next design topic is API gatekeeping. Options to evaluate:
  distinct leader/follower Keratin handle types, a capability/token for
  replicated operations, an extension trait only used by replication code, or an
  internal mode flag. Promotion is tricky because many callers hold
  `Arc<Keratin>`, so any type-level design must avoid making role transitions
  impossible.
- 2026-06-09: Added Keratin API gatekeeping with a `KeratinReplicaExt` trait and
  a local `KeratinRole` guard. Default role is owner. Normal append requires
  owner mode. Replicated append and checkpoint reset require follower mode.
  Frozen mode rejects both. This is local misuse protection only; Stroma/broker
  still own real cluster role decisions.
- 2026-06-09: Noted very future partition shrink path. Expanding partition count
  is not the only lifecycle operation; shrinking should eventually be possible
  by marking partitions as no-longer-receiving, draining them, then deleting
  their ownership/data once empty and safe.
- 2026-06-09: Added a generic Keratin manifest epoch. Replicated append now
  carries an epoch, rejects stale epochs, and persists newer epochs before
  applying replicated records. This is local fencing only; Keratin does not know
  who owns a partition or why an epoch advanced.
- 2026-06-09: Renamed the public checkpoint primitive to
  `destructive_reset_to_checkpoint` so the API makes local data deletion obvious.
  The operation remains follower-gated and still preserves the current Keratin
  epoch.
- 2026-06-09: Tightened suffix-overlap replication. `AppendSuffixAfterKnownPrefix`
  now verifies that the overlapping prefix already stored locally matches the
  incoming records before appending the missing suffix.
- 2026-06-09: Rechecked offset tracking while adding tests. `next_offset`,
  `head_offset`, durable watermark after fsync, exact `fetch`, and reopen
  behavior now have focused coverage. One existing cleanup item remains:
  empty-log `durable_offset` is initialized as `0` even though the comment says
  `u64::MAX if none`; that was not changed in this slice to avoid broadening
  behavior unexpectedly.
- 2026-06-09: Started Stroma role wiring. Added queue-level
  `Owner | Follower | Frozen` runtime role state, exposed role debug fields,
  and guarded ordinary owner operations. Public Stroma append paths now reject a
  follower before writing durable owner events, and expiry scanning skips
  non-owner queues. Focused tests cover stale/follower handles, rejection before
  event-log append, and follower expiry skip behavior. Remaining role work is
  the transition/freeze protocol and replicated ingest API.
- 2026-06-09: Reviewed Stroma long-lived handle holders before implementing
  transition draining. The clean solution needs an explicit owner-operation
  lease that spans async completion callbacks. A naive `Frozen` exception based
  on active operation count would be unsafe because unrelated new writes could
  pass while the queue is transitioning. Left this as a documented transition
  protocol instead of adding a half-measure.
- 2026-06-09: Added the first Stroma owner-operation lease implementation. New
  owner operations acquire an atomic lease, freeze stops new leases, and
  `freeze_queue_for_transition` waits for already accepted owner work before the
  higher layer switches the queue role. Publish completions now carry the lease
  from message-log append through matching event-log append so graceful freeze
  should not create normal orphan payloads. This may need benchmark attention
  because the hot paths now include atomic lease accounting.
- 2026-06-09: Added real freeze/drain tests around started publish and ack
  completions. Also fixed the owner-side NACK path to apply already accepted
  NACK events through the internal path under its lease, and let spawned DLQ
  continuation work inherit a continuation lease instead of reopening the public
  owner gate after freeze.
- 2026-06-09: Added first Stroma follower-ingest API. A caller can apply
  replicated message and event batches to a follower queue. The method requires
  follower role, sets the queue's Keratin logs to follower mode, uses Keratin
  replicated append, and applies replicated events without running owner-only
  side effects. Focused tests cover normal replicated ingest, rejecting owner
  queues, and source-queue DLQ events not writing the DLQ target queue.
- 2026-06-09: Added checked Stroma follower promotion. The caller supplies the
  expected message and event next offsets from the coordination or replication
  layer. Stroma verifies the queue is a follower, local log tails match exactly,
  and event state has applied through the event tail before switching the queue
  and both Keratin logs to owner mode. Outcomes now distinguish behind, ahead,
  and not-yet-applied states so stale coordination data cannot silently promote
  unexpected local state. This is only a local readiness check. It does not prove
  the old owner is fenced, and it does not replace the future ownership election
  protocol.
- 2026-06-09: Added checked Stroma owner demotion. Freezing now requires the
  queue to still be owner instead of unconditionally forcing `Frozen`, which
  prevents a stale transition caller from freezing an existing follower. Demotion
  freezes owner traffic, waits for accepted owner operations to drain, freezes
  both Keratin logs, records local message and event tails, then switches the
  queue and logs to follower mode. Focused tests cover demotion while a publish
  completion is still in flight, rejection of new owner traffic during freeze,
  replicated ingest after demotion, and refusal to demote non-owner queues.

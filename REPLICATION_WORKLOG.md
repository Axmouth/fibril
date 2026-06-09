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
- Exact Stroma role transition protocol. Freezing should stop new owner work,
  drain or reject in-flight owner commands, advance epochs where needed, and
  only then switch to follower or owner mode. This is separate from the first
  role guard because it needs careful handling of asynchronous completions and
  background tasks.
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

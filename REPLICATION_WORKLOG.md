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
- Normal replicated append remains contiguous. If a follower is too far behind
  because old log ranges were truncated, it should install a checkpoint or
  snapshot and then continue from the checkpoint offset. It should not fill gaps
  with fake records.

## Pending Decisions

- Exact epoch persistence location in Keratin, Stroma, or both.
- Whether Keratin epoch tracking is a manifest field, separate metadata file, or
  both.
- Exact name and shape of replicated append APIs.
- Whether Keratin should expose distinct handle types for owner logs and
  follower logs, or keep one handle with restricted replicated-append entry
  points.
- How much snapshot installation belongs in Keratin versus Stroma.
- Shape of the Keratin checkpoint-install primitive that lets a follower say
  "start this local log from offset N" after receiving a snapshot.
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

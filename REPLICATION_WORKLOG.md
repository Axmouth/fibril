# Replication and Sharding Work Log

Branch: `replication-sharding-plan`

<!-- ===== START HERE (post-compaction handoff, refreshed) ===== -->
## START HERE

STATE: all green, all committed on branch `replication-sharding-plan`. The branch
gets rebased between sessions, so reference commits by MESSAGE, not hash. keratin
is pushed (its replication-sharding-plan origin matches local) and fibril builds
it via the [patch] in fibril/Cargo.toml. The codebase moved during a background
interlude (cluster fixes, audits, live-cluster test scripts/smokes, ganglion
docs, and a server-bootstrap EXTRACTION), so re-grep before trusting old
locations.

STYLE + WORKING RULES (memories): prose avoids semicolons, uses em dashes only
where the visual value is real, and uses plain ASCII apostrophes/quotes
(prose-style-comments-docs). No roadmap/phase labels in .rs files
(no-phase-labels-in-code); the .md worklog is fine. No Co-Authored-By trailer on
commits (no-coauthor-trailer-in-commits). Mutex only on cold paths, prefer
atomics/CAS/OnceLock/RwLock (concurrency-primitive-discipline). All tunables go
through config/runtime settings (settings-discipline). By the end, move the whole
CLUSTERING concern (partitioning + replication + cohorts/coordination) into a
dedicated module in every stack crate except ganglion (ganglion is all clustering
already), opportunistically where already separable. Name likely `clustering`,
final TBD (replication-module-separation).

THE MODEL (memories fibril-core-model, consumer-cohort-purpose): fibril is a
RabbitMQ-style durable WORK QUEUE (consumed = gone, lease/ack), not a log. `group`
is a namespace prefix, not fan-out. Default many-consumers = competing (no
ordering). An exclusive cohort (opt-in) gives ordered, balanced, sticky,
self-healing consumption of a partitioned queue, one consumer per partition.

DONE — exclusive consumer groups (cohorts), the whole feature:
- Single-node: complete and tested (split, failover, target, push, reconnect,
  one-cohort-per-queue guard). Client API: `subscribe(t).exclusive()` (id-free) +
  `.consumer_target(n)`, plus `Client::assignment_events()`.
- Cross-broker correctness: verified (each owner gates its partitions
  independently, member sets consistent via the cluster member id).
- Cross-broker COORDINATOR: built and wired. Member identity (server-minted
  cluster `member_id`, client carries via OnceLock). Per-partition delivery GATE
  (`QueueLoopState.exclusive_assignee`, AtomicU64) is the correctness backstop.
  Broker `apply_exclusive_assignment` (external plan override). Membership
  snapshot + `aggregate_cohort_membership` + `ClusterCohortController` (global
  sticky/target plan). In coordination-ganglion: COHORT_MEMBERSHIP_LABEL
  encode/decode, cohort assignment attribute (entry sequence, never a
  Partition-keyed map), and provider methods global_cohort_membership /
  publish_cohort_assignment / cohort_assignment / run_cohort_controller_tick
  (leader-gated). The 3 bootstrap spawns (heartbeat label, controller tick, owner
  watcher) now live in `crates/fibril/src/lib.rs` (moved there by the
  server-bootstrap extraction, NOT server.rs main anymore).

INVARIANTS: the gate enforces one-consumer-per-partition always, so the global
plan is advisory/eventually-consistent (about one heartbeat of lag affects
balance only, never correctness; a departed member's partitions pause, never
mis-deliver). Single-node is unaffected by all coordinator code (no controller ->
no external plan -> local computation).

DONE — multi-node cohort e2e: cohort_controller_aggregates_across_brokers_and_rebalances
in coordination-ganglion stands up a ganglion node with two brokers each
reporting only their local member, asserts the controller aggregates both labels
into one globally balanced plan, then drops a member and asserts the survivor
absorbs the orphaned partitions. Also fixed a forwarded-write regression found
along the way (no_leader_retries had defaulted to 0, so a standby whose topology
had not yet learned the leader failed forwarded writes instantly; restored a
6-attempt safety-net default, and the standby-forward test now runs at 0 retries
to prove the deterministic forward + hint-redirect path).

DONE — cohort plan GENERATION (the unified global-generation follow-on): the
published cohort assignment document carries a durable generation the controller
bumps only when the assignment content changes (read back from the committed
attribute before each publish, so it stays monotonic across a leader change).
Owners fence any plan older than the one they hold (equal generation still
re-resolves, since local subs may have changed), and apply_exclusive_assignment +
exclusive_assignment_generation expose the applied version for convergence
observability. Design choices: generation is PER-COHORT (each cohort doc is its
own authoritative version, no cross-cohort write amplification), and it lives in
the durable doc, not an in-memory counter. KNOWN follow-up: a new leader's
controller starts with empty sticky state, so the first plan after a leader
change can differ from the prior one (one rebalance, gate keeps correctness).
Seeding the controller from the published plan would remove that churn and
overlaps with the cooperative-incremental-rebalance item.

AFTER (user direction): hardening and refactoring. Includes the per-crate
replication module separation, the combined Offset + Topic/Group newtype pass
(Arc<str> for Topic/Group), and the broader replication + partitioning docs
explainer (the consumer-groups docs page already landed).

MERGE READINESS NOTE: merging `replication-sharding-plan` to main should mean
"experimental replication and sharding foundation generally works", not
"production HA is done". The branch can plausibly hit that milestone before all
clustering follow-up is complete.

Candidate pre-merge checklist:
- Run full verification from a clean tree.
- Run cluster tryout smokes at small and medium sizes, at minimum 3, 5, and a
  moderate multi-node count such as 15.
- Split clustering-related logic into dedicated modules per crate where this
  improves reviewability, especially partitioning, replication, cohorts, and
  coordination glue. Do not do broad rewrites just to satisfy the split.
- Ensure a real cluster scenario is covered or manually demonstrated: declare a
  queue, publish, consume, follower catches up, owner dies, promoted owner serves
  traffic.
- Keep docs explicit that clustered mode is experimental and not production HA.
- Keep the TypeScript cluster/cohort gap visible until the cohesive parity pass
  lands.
- Confirm single-node mode needs no coordination config and keeps existing
  behavior.
- Do a debug-leftover and stale-planning-wording pass.

Feature work still considered in-scope before or shortly after merge:
- Opt-in client narrowing for exclusive cohorts, so clients can subscribe only
  to their assigned partition subset instead of relying only on the delivery
  gate.
- Cooperative incremental rebalance. The leader-change churn piece is already
  handled by controller seeding, but whole-set recompute can still be improved.

Useful but not merge-blocking:
- sub_id-scoped leave for cross-connection reconnect takeover.
- Keratin pass: README/docs refresh, slogan, performance audit, and follow-up
  optimization experiments.
- Repository split for Stroma and Keratin, once it helps iteration more than it
  disrupts the replication branch.

DOCS HOUSEKEEPING (at some point, user request): do a relevance pass over all the
.md files we have collected. We should not keep them all, especially planning and
handoff docs that go stale. Prune or fold the dead ones. While at it, give the
keratin README and basic docs a look, and the stroma ones too, for the same
freshness check.

DONE — controller seeding + member-id validation:
- Controller seeding: a freshly elected controller seeds from each cohort's
  published plan before its first tick (seed_published -> seed_if_absent), so a
  leader change keeps the existing assignment instead of churning. With the
  generation work, leader changes are now generation-stable.
- Member-id validation (LOCAL GUARD, trusted-client model): the broker rejects a
  nil member id (ERR_INVALID) and enforces one member identity per connection
  (first exclusive subscribe establishes it, later ones must reuse it, else
  ERR_CONFLICT). Scoped to the current trusted/authed-client model. See
  DESIGN_NOTES.md for the threat-model assumption and when to revisit (signed
  token / coordinator issuance for untrusted multi-tenant).

DEFERRED (cohort follow-ons, gate de-risks them): opt-in client narrowing (with
per-partition leave), cooperative incremental rebalance (vs whole-set recompute;
note controller seeding already removed the leader-change churn piece), and
sub_id-scoped `leave` for cross-connection reconnect takeover (today a stale
connection's whole-member leave can clobber a member that re-subscribed on a new
connection; impact is a transient pause, gate stays correct; reworks leave's
failover atomicity so it is its own brick). The unified-global-generation and
coordinator-issued-member-id items are addressed (generation DONE, member-id done
as a local guard).

CODE POINTERS: cohort assignor/router/controller-brain + membership types =
crates/broker/src/coordination.rs. Gate + ExclusiveGroupRouter + apply path =
crates/broker/src/broker.rs. Wire fields + handler join/leave/reconcile =
crates/protocol/src/v1/{mod.rs,handler.rs}. Client `.exclusive()` + member-id
cache + assignment_events = crates/client/src/lib.rs. Transport + provider methods
+ controller tick = crates/coordination-ganglion/src/lib.rs. Bootstrap spawns =
crates/fibril/src/lib.rs.
<!-- ===== END START HERE ===== -->


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

- Use etcd as the default HA coordination backend. Static coordination remains
  useful for tests and local manual setups, but the first serious HA path should
  assume a small etcd cluster for leases, watches, CAS assignment writes, and
  controller election.
- Static config is acceptable for the first local/in-process milestone if it
  does not leak into the storage or queue APIs.
- Fibril may absorb more coordination complexity eventually to reduce operator
  burden, but that should happen behind the same coordination interface and only
  after the etcd-backed HA path is correct.
- Coordination should expose both ownership and follower assignments. Ownership
  is the single-writer lease/fencing path; follower assignment tells each node
  which partitions to replicate and where their owners are.
- Cluster assignment decisions should be made by one active controller at a
  time, acquired through a lease-backed metadata key. Brokers can all be
  controller-capable, but only the active controller computes and writes desired
  placement.
- The controller should be stable by default. Later, it may voluntarily hand off
  its lease if sustained health scoring shows another eligible node is clearly
  better. Candidate signals include controller loop latency, etcd write latency,
  local load, command queue depth, disk pressure, and data-plane degradation.
- Controller handoff is not partition ownership failover. It only changes who
  computes metadata-plane assignments; queue ownership changes still require
  assignment writes, fencing epochs, local freeze/drain, catch-up checks, and
  promotion checks.
- `REPLICATION_PLANNING.md` now contains the detailed controller and
  coordination spec: metadata keyspace sketch, controller lease, controller
  loop, assignment transitions, failover policy, balancing policy, partition
  scaling, health scoring, broker watch behavior, and client topology behavior.
  Future coordination trait work should converge on that shape rather than the
  current minimal leader-only stub.
- Balancing is a policy above coordination. It can later consider target
  follower count, max owned partitions per node, max followed partitions per
  node, disk pressure, and replication lag. It should not leak into Keratin or
  Stroma.
- Partition scaling is a separate queue/topology operation. Growing adds
  partitions and assigns owners/followers. Shrinking should stop new routing to
  retiring partitions, drain them, then remove them after they are empty.
- Clients eventually need topology-aware routing. A logical queue may span
  partitions owned by different brokers, so clients may need connections to
  multiple owners while keeping partition choice out of the normal user API.
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
- Broker-level replication durability should use explicit operator-facing
  contracts:
  - `local_durable`: confirm when the owner has durably written locally.
  - `replica_accepted(n)`: confirm when `n` assigned nodes, including the
    owner, have accepted the append. This is lower latency but weaker than
    durable replication.
  - `replica_durable(n)`: confirm when `n` assigned nodes, including the owner,
    have durably written the append.
  - `majority_durable`: confirm when a durable majority of the assigned replica
    set has written the append.
  "Written" means fsynced or otherwise durably committed by the local log.
  Enforcement belongs in broker/Stroma publish-confirm flow, not in Keratin.
- Checkpoint and snapshot installation semantics belong in Stroma. Keratin can
  reset a local log continuation point and append caller-assigned records, but it
  should not know what a Stroma snapshot means or how a queue catches up.
- Normal replicated append remains contiguous. If a follower is too far behind
  because old log ranges were truncated, it should install a checkpoint or
  snapshot and then continue from the checkpoint offset. It should not fill gaps
  with fake records.
- State checkpoints are not message checkpoints. They carry compacted queue
  bookkeeping such as ready/inflight/acked/retry state, not payload bytes.
  Message catch-up remains a separate transfer from the message log. The first
  implementation can use contiguous message-log catch-up from the checkpoint
  continuation offset, but this may become very large when an old referenced
  message pins a large retained range. Future optimization should consider
  fetching only message offsets still referenced by compacted state, likely with
  explicit offset-list/range requests, chunking, resume support, and optional
  compression. That should be designed deliberately rather than implied by the
  state snapshot install path.
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
- Partition placement is a pure policy boundary above coordination storage.
  The default policy should be deterministic and testable without a broker,
  Stroma, Keratin, or etcd instance. It computes owners and followers from a
  node set, queue set, existing assignments, target follower count, and
  generation. The future controller calls a policy, then writes the resulting
  assignment snapshot through coordination.
- The default placement policy prioritizes stability over perfect balance:
  keep an existing owner if the node still exists, keep valid existing followers
  in order, drop missing or duplicate followers, fill missing followers from
  sorted nodes, and reassign ownership only when the previous owner is no
  longer present. More advanced policies can later consider health, disk
  pressure, lag, and balancing rules behind the same trait.

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
   node does not own the queue. TCP protocol handling now returns a stable
   conflict response for not-owner publish and subscribe attempts without closing
   the connection. Real coordinator-backed ownership is still pending.
6. Done: add assignment computation as a pluggable placement policy.
   The first policy is deterministic, pure, and unit-tested. It does not talk to
   etcd or mutate broker runtime state. The controller-backed implementation
   should later call the policy and publish the resulting coordination snapshot.
7. Prototype replication transport. Prefer pull from follower to owner for the
   first version because the follower best knows its local offset and checkpoint
   state.
   Owner notifications are useful as wake-up hints, not as correctness. A
   follower should still be able to catch up by polling or long-polling from its
   last known message and event offsets. The eventual worker shape is:
   follower pulls until the owner returns empty batches, then waits by long poll
   or a bounded timer. The owner may notify followers that new data exists, but
   notifications should be coalesced per queue/follower at a small cooldown and
   can be lost without breaking replication. If a notification is lost, the
   follower catches up on the next long poll or periodic retry.
   Contiguous log catch-up only works while the owner still retains the
   requested message and event ranges. Once the follower asks for data older
   than the retained head, the owner should not stream all history and may no
   longer have it. The follower must install a compact owner checkpoint or
   snapshot, reset its local continuation point, then resume normal replicated
   ingest from the snapshot's message and event offsets. This is a Stroma-level
   concern: the snapshot contains compact queue state, while Keratin only needs
   generic log reset/continuation primitives.
8. Add operator visibility after the mechanics exist: queue role, replicated
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
7. Done: broker ownership gate. The broker has a sync ownership provider
   interface suitable for a future coordinator watch cache, with `OwnAllQueues`
   as the default and `StaticQueueOwnership` for tests and early wiring.
8. Done: broker `NotOwner` maps to protocol-visible conflict errors for publish
   and subscribe setup. The connection stays open so clients can refresh
   topology or retry elsewhere later.
9. Done: Stroma exposes owner-only log read APIs needed by follower pull
   replication. Reads return explicit offsets and either a contiguous batch or a
   checkpoint-required outcome when the requested offset is older than the local
   head.
10. In progress: prototype follower pull replication and local catch-up loop.
    The Stroma surfaces now compose in tests: owner reads provide message/event
    batches, follower ingest applies them, and checked promotion accepts the
    follower after exact catch-up. Broker owner-read helpers now provide the
    first transport-facing boundary: they check broker queue ownership before
    touching Stroma, then return owner message and event records for a follower
    pull loop. Broker follower-apply helpers now convert owner-read batches into
    Stroma replicated ingest batches, refuse to partially apply when a
    checkpoint is required, and leave snapshot installation for a later
    checkpoint path. Real TCP/admin transport and follower scheduling are still
    pending. A bounded local catch-up helper now repeatedly reads from an owner
    broker, applies batches to a follower broker, advances message/event
    offsets, and stops when both streams return empty at the owner's current
    tail.
    Protocol v1 now has replication read request/response frame definitions for
    this pull shape. The frames carry raw message headers/payloads and raw event
    bytes so the wire contract stays log-shaped instead of Stroma-shaped.
    Handler wiring now exists for owner read and follower apply. Background
    scheduling is still pending. The follower worker now depends on a small
    owner-peer trait rather than a concrete owner broker, so the same worker
    tick can use an in-process broker today and a protocol-backed peer later.
    A first loop scaffold now resolves the owner peer, runs bounded catch-up
    ticks, waits according to worker policy, and exits when cancelled or when
    the local follower worker is no longer assigned.
11. Done: replace ad hoc assignment computation with a pluggable
    partition placement policy. The default policy is deterministic and keeps
    stable assignments where possible, so later controller tests can focus on
    lease/CAS behavior instead of placement details.
12. Later: replace static ownership with coordinator-backed ownership, likely
    based on an etcd-style lease/watch model.
13. Later: admin and metrics visibility for queue role, local offsets,
    replicated offsets, lag, transition state, and refusal reasons.

## Active Implementation Plan

Current focus: follower transport boundary, visibility, and role lifecycle.

1. Done: replace the old leader-only `Coordination` stub with a cached
   assignment model shaped like the controller spec: nodes, partition
   assignments, owners, followers, epochs, and watchable snapshots.
2. Done: add a static coordination implementation for tests/local manual setup.
   It should model the same assignment snapshot shape the future etcd backend
   will expose.
3. Done: make static coordination usable as the broker's `QueueOwnership`
   provider so existing owner gates can consume assignment data without waiting
   for the full broker assignment watcher.
4. Done: add focused coordination tests and run broker checks.
5. Done: commit the static coordination checkpoint.
6. Done: add a snapshot-diff planner that converts coordination changes
   into local queue role intents: become owner, become follower, demote owner,
   freeze/unassign, or no-op.
7. Done: add tests for assignment transitions, including owner to follower,
   follower to owner, new follower assignment, and removed assignment.
8. Done: add a minimal broker-facing apply method that can invoke existing
   Stroma role transition primitives where enough local information exists.
   Follower promotion is intentionally deferred until catch-up offsets are
   known. Follower stop is intentionally deferred until shutdown semantics are
   defined. New owner assignments do not materialize queues by themselves;
   sparse queues should stay cold until traffic or an explicit operation loads
   them.
9. Done: commit the assignment transition planner/apply checkpoint.
10. Done: broker assignment watcher task that watches coordination snapshots
   and applies role transitions. This should be opt-in at first so standalone
   broker construction does not change behavior.
11. Done: commit the assignment watcher checkpoint.
12. Done: add a follower worker state machine that tracks local
   replication offsets, schedules pull attempts, records checkpoint-required
   state, and remains independent of the eventual protocol transport.
13. Done: add focused tests for follower worker scheduling decisions:
   caught-up cooldown, iteration-limit retry, checkpoint-required blocking,
   and progress advancing offsets.
14. Done: commit the follower worker state checkpoint.
15. Done: attach follower worker lifecycle to local follower
   assignments. This checkpoint should register local worker state when a queue
   becomes a follower, but should not start protocol/network replication yet.
16. Done: add tests proving follower assignment creates worker state and owner
   assignment keeps sparse queues cold.
17. Done: commit the follower worker lifecycle checkpoint.
18. Done: add a typed broker assignment durability policy that records
   whether confirms require local durability, replica acceptance, replica
   durability, or durable majority. This should be modeled but not enforced yet.
19. Done: record the publish-confirm enforcement hook. The publish-confirm path
   should resolve the assignment policy after the owner append has a local
   result, then wait for follower acknowledgements that satisfy the required
   node count and acknowledgement kind. This requires follower accepted/durable
   offset reporting before enforcement can be real.
20. Done: pin the Fibril checkpoint-required boundary. If an owner read
   reports that the follower is behind the retained log range, the broker should
   surface `CheckpointRequired`, avoid partial apply, and avoid materializing a
   follower queue just to report that status.
21. Done: add Stroma state-checkpoint install support for followers that are
   behind the owner's retained event log range.
22. Done: add a Stroma follower state-checkpoint install API. It resets follower
   logs to safe continuation offsets, installs compacted queue state, keeps the
   queue in follower role, and then lets normal replicated append continue from
   those offsets. For the current contiguous message-log contract, it rejects an
   install that would advance the message log past messages still referenced by
   the installed state.
23. Done: add an owner state-checkpoint export API in Stroma. The first
   version may briefly freeze and drain owner operations before encoding the
   checkpoint so the exported state, applied event offset, and log continuation
   offsets describe one coherent point. This is a controlled handoff/catch-up
   primitive, not a hot-path background checkpoint.
24. Done: add broker-facing methods that fetch an owner state checkpoint and
   install it on a follower through the existing Stroma install API.
25. Done: add an explicit checkpoint-aware catch-up helper so a
   checkpoint-required response can drive owner state checkpoint export, follower
   state checkpoint install, contiguous message catch-up, event catch-up, and
   promotion checks. Keep the existing bounded helper unchanged for worker paths
   that should only report checkpoint-required without pausing the owner.
26. Done: add follower worker policy for checkpoint install. Default
   behavior should remain conservative: workers report checkpoint-required and
   retry later. A separate flag can allow worker-triggered checkpoint-aware
   catch-up because it briefly pauses the owner while exporting state.
27. In progress: wire the follower worker transport boundary. The run-once
   worker now consumes a `BrokerOwnerReplicationPeer` abstraction that exposes
   owner record reads and owner checkpoint export. `Broker<StromaEngine>` and
   `Arc<Broker<StromaEngine>>` implement this for current in-process tests.
   The loop scaffold resolves an owner peer through a separate resolver trait,
   runs one bounded catch-up tick at a time, and waits through a
   cancellation-aware delay. A real owner transport can implement the same peer
   boundary later, and a real coordinator watch cache can implement the resolver
   later. The assignment watcher now has an opt-in variant that starts
   supervised follower loops when local follower assignments appear and stops
   them when the assignment is removed. Checkpoint-aware worker catch-up remains
   policy-gated and is not automatically used by the loop yet.
28. Priority order from here:
   - checkpoint export/install over the replication transport
   - protocol-backed owner peer and coordination-backed peer resolver
   - coordinator-backed assignments using the existing snapshot/watch shape
   - publish-confirm quorum enforcement from follower progress
   - failover and promotion orchestration
   - topology-aware clients and admin/operator visibility
   The original planning document should remain useful as historical grounding.
   New details should be appended here as implementation checkpoints rather
   than rewriting the starting plan.
29. Important broker-loop transition work item: role changes must transition
   broker-level queue runtime too, not only Stroma queue role. Today
   `QueueLoopState` can remain in `queues` with publisher sink tasks, consumer
   delivery loops, leases, and broker inflight/tag maps even after an assignment
   demotes the queue to follower. Stroma role guards prevent durable owner
   writes from succeeding, but the normal path should make those errors mostly
   unreachable by draining or closing broker owner loops before starting the
   follower replication worker. Owner to follower should stop accepting new
   publishers/subscribers, close or fail existing owner handles, drain/requeue
   broker deliveries, remove stale tag records, then demote Stroma and start the
   follower worker. Follower to owner should stop the follower worker after
   verified catch-up and promotion, then allow the owner queue loop to be
   created lazily by real traffic. StopFollower has since been given explicit
   local shutdown semantics.
30. Done for the first owner-runtime boundary: `QueueLoopState` now has a
   per-queue owner-runtime cancellation token. Assignment demotion cancels and
   removes the broker owner queue runtime first, collects broker-tracked
   delivery offsets, writes a durable Stroma release-inflight event for those
   offsets, then demotes the queue to follower and starts the follower worker.
   Release-inflight is intentionally not a NACK: it returns already leased work
   to ready without consuming retry budget or entering the DLQ path. Stale
   publisher handles close instead of continuing as normal owner traffic.
31. Done for StopFollower: broker assignment application now removes the local
   follower replication worker and asks Stroma to stop the follower queue for
   transition. Stroma validates the queue is currently a follower, freezes the
   queue role and both Keratin logs, and rejects further replicated ingest.
   This keeps an unassigned local queue from accepting owner traffic or follower
   traffic by accident. Remaining detail work: decide whether StopFollower
   should also try memory-only unmaterialization once no local role remains, and
   keep the current frozen state as the conservative default until that is
   explicitly designed.
32. Done for FreezeOwner delivery handling: freeze without demotion now follows
   the same owner-runtime boundary as demotion. Broker assignment application
   removes the owner runtime, collects broker-tracked delivery offsets, writes a
   durable Stroma release-inflight event for those offsets while the queue is
   still owner, then freezes the Stroma queue. This means local owner removal
   does not leave stale broker leases stuck as inflight, and stale publisher
   handles close after freeze.
33. Done for follower worker status detail: the local follower replication
   worker state now stores the latest catch-up progress alongside current
   message/event offsets, status, and next delay. Status remains intentionally
   small: caught up, pending retry, or checkpoint required. The progress field
   records what the last tick actually applied or attempted, which gives later
   admin/controller code a clear watermark and recent movement signal without
   adding coordination behavior yet.
34. Done for follower checkpoint policy tightening: `run_follower_replication_worker_once`
   now uses the worker state's `should_install_checkpoint` predicate before
   choosing the checkpoint-aware catch-up helper. That keeps the first
   checkpoint-required worker tick conservative: it records the blocked status
   and waits. A later tick may install a checkpoint only when the worker is
   already checkpoint-blocked and `allow_checkpoint_install` is enabled. A full
   broker test that triggers a real checkpoint-required owner read is still
   deferred until there is a clean transport/test hook for owner log
   truncation.
35. Done for follower transport loop shape: the worker state already
   records `next_delay_ms` after each tick. That is the scheduler contract for
   now: caught-up workers poll slowly, iteration-limited workers retry quickly,
   and checkpoint-required workers wait on the checkpoint retry cadence. A
   permanent background loop should use this field plus future owner wake-up
   hints, rather than hard-coding sleep behavior in transport code.
36. Done for replication observability: broker sparse queue
   observability now includes follower worker reports and summary counts. The
   admin queue debug API passes these through as `replication_followers` and
   `replication_summary`. Worker state is read with a non-blocking lock so
   observability never stalls an active catch-up tick; a busy worker can report
   `busy: true` with no state snapshot for that instant.
37. Done for checkpoint transport: protocol v1 now has explicit
   state-checkpoint export/install frames. Export is owner-side and returns a
   compact queue-state checkpoint plus message/event continuation offsets.
   Install is follower-side and installs only queue state; message payloads
   still catch up through the normal replication read/apply path from the
   checkpoint's message continuation offset.
38. Done for assignment lifecycle cleanup: follower refresh transitions
   are intentionally no-ops for local worker lifecycle and must not reset
   replication progress. This keeps coordination metadata refreshes from
   rewinding a follower that is already catching up.
39. Done for placement policy boundary: `PartitionPlacementPolicy` now models
   assignment computation as a pure broker-level policy. The deterministic
   default sorts queues and nodes, preserves existing owners when still present,
   repairs missing owners by assigning a replacement, preserves valid followers,
   fills follower slots up to the target, caps follower count to available
   non-owner nodes, and preserves durability settings across recomputation. This
   is deliberately not an etcd controller yet. It is the testable algorithm a
   future controller can call before writing a coordination snapshot.
40. Done for owner-peer catch-up boundary: follower catch-up no longer requires
   a concrete owner broker argument. `BrokerOwnerReplicationPeer` is the minimal
   owner-side interface needed by a follower worker: read owner message/event
   replication records and export an owner state checkpoint. A focused fake-peer
   test verifies that the worker tick updates state through this boundary. The
   permanent background loop is still pending because it needs owner discovery,
   connection management, and retry behavior around the same trait.
41. Done for the first follower-loop scaffold: broker code can now run a
   follower replication loop around the existing run-once worker. The loop is
   cancellation-aware, records ticks only after a completed catch-up attempt,
   retries after unresolved owners or tick errors, and exits as `WorkerStopped`
   when the local follower assignment has been removed. Normal role transitions
   should prefer this graceful path: stop routing or assigning new work first,
   let the current follower tick finish, then remove the worker and change local
   role. Hard cancellation exists for process shutdown or broken workers, not as
   the preferred ownership-transition mechanism. Remaining detail work:
   actually spawn and supervise these loops from the assignment watcher, define
   the resolver backed by coordination snapshots and protocol clients, and
   prevent StopFollower from racing a mid-batch follower ingest once loops are
   fully supervised.
42. Done for follower loop supervision: follower worker state is now wrapped in
   a small runtime object with a shutdown token, single-start guard, and active
   tick counter. StopFollower marks the runtime as stopping, wakes the loop, and
   waits for any active catch-up tick before freezing the local follower role.
   This is the production reason for the wrapper: it prevents role changes from
   racing a replicated ingest batch. The assignment watcher now has an opt-in
   `spawn_assignment_watcher_with_follower_replication` variant that starts the
   supervised loop after `BecomeFollower` or `DemoteOwnerToFollower` applies.
   The default watcher remains unchanged for standalone/local mode. Remaining
   work: implement a resolver backed by coordination snapshots and protocol
   clients, then make that the real cluster watcher path.
43. Done for protocol-backed follower loop proof: protocol tests now cover a
   follower broker running its real follower replication loop through
   `StaticProtocolOwnerPeerResolver`. The resolver opens a protocol TCP
   connection to an owner broker, the worker pulls records through
   `BrokerOwnerReplicationPeer`, the follower reaches `CaughtUp`, and checked
   promotion succeeds against the owner's checkpoint offsets. This proves the
   worker/resolver/protocol boundary works for ordinary contiguous catch-up.
44. Done for protocol-backed checkpoint loop proof: protocol tests now cover the
   follower worker's policy-gated checkpoint install path through
   `StaticProtocolOwnerPeerResolver`. The test uses a small fake owner protocol
   server to force a checkpoint-required read, return a real broker-exported
   owner state checkpoint, then return retained message records. The follower
   records the blocked checkpoint status on the first tick, installs the
   checkpoint on the second tick when policy allows it, catches up, and passes
   checked promotion. This avoids adding a production truncation hook just to
   make an integration test deterministic. Lower-level Stroma tests still own
   real retained-head/checkpoint-required behavior.
45. Done for not-owner loop reaction: the follower worker loop now treats a
   `BrokerError::NotOwner` from its resolved owner peer as an ownership/topology
   change, not as an ordinary retryable transport failure. It exits with
   `OwnerChanged { ticks }` so a future coordination watcher can refresh
   topology or wait for a newer assignment instead of sleeping against a stale
   owner. Protocol peers already map wire `ERR_NOT_OWNER` into this broker error,
   so the behavior applies to real protocol owner connections.
46. Done for protocol peer connection reuse: `StaticProtocolOwnerPeerResolver`
   now caches one protocol owner peer per owner id instead of opening a new TCP
   connection on every worker tick. The cached peer opens lazily on the first
   request and clears its connection after transport-level failures, so a later
   tick can reconnect through the same stable peer object. Protocol errors such
   as `ERR_NOT_OWNER` do not poison the connection; they still surface as typed
   broker errors for the loop/topology layer to handle. Focused resolver tests
   cover peer reuse, authenticated reads, and the protocol-backed worker loop.
   Remaining transport work: explicit reconnect/backoff observability,
   topology-watch refresh, and longer-running supervised watcher coverage.
47. Done for coordination-backed protocol peer resolution:
   `CoordinationProtocolOwnerPeerResolver` reads the current coordination
   snapshot on each resolve, looks up the assignment owner in `NodeInfo`, and
   keeps a conservative peer cache keyed by owner id. Stable owner addresses
   reuse the same lazy protocol peer. Missing owner nodes return `None` and
   clear that owner's cache entry. Changed owner broker addresses replace the
   cached peer so follower workers do not keep talking to a stale endpoint after
   topology refresh. This is still snapshot-driven, not a watcher loop. The
   watcher/supervisor layer remains responsible for reacting to
   `OwnerChanged`, reading a newer assignment, and restarting or stopping the
   follower loop as appropriate.
48. Future test cleanup pass: broker replication tests now repeat enough setup
   that helpers are worth adding before the next large group of tests. Keep
   helpers behavior-shaped, not assertion-hiding. Good candidates:
   owner/follower broker pair setup, follower assignment setup, publishing `N`
   messages to an owner, running catch-up once or until caught up, waiting for a
   follower worker status with timeout, default `PartitionAssignment` builders,
   and fake owner peer builders for records and checkpoints. The purpose is to
   reduce boilerplate in future adversarial/concurrency tests without hiding the
   state transition being tested. This is cleanup, not a reason to add more
   abstractions to production code.
49. Future admin dashboard topology view: once coordination-backed assignments
   exist, add a topology page to the dashboard. Ideally this is a live diagram
   showing nodes, partition owners, followers, lag, role transitions, and
   unhealthy or disconnected links. The first version can be table-first if
   needed, but the target operator experience should make the cluster shape
   obvious during incidents.

## Medium-Term Plan

This section is the current execution guide. It should be updated as items land,
but the older starting plan should remain useful as historical grounding.

### 1. Protocol-Backed Owner Peer

Goal: make follower worker loops pull from real owner nodes instead of only
in-process fake peers.

Breakdown:

- Done: add a broker/client-side peer implementation of `BrokerOwnerReplicationPeer`
  that speaks protocol v1 replication read and checkpoint frames.
- Done: reuse existing protocol handler frames before adding new wire surface.
- Done for the static resolver path: cache one protocol peer per owner id, open
  the TCP connection lazily on first use, clear it after transport failures, and
  let the worker retry by its existing policy.
- Done at the loop boundary: keep ownership errors explicit. If the contacted
  node is not owner anymore, the protocol peer returns `BrokerError::NotOwner`
  and the follower loop exits with `OwnerChanged`, giving the future
  resolver/topology watcher a precise refresh signal.
- Done for contiguous catch-up: add TCP end-to-end tests with two local brokers:
  owner publishes, follower worker pulls over protocol, follower reaches
  matching offsets, and checked promotion succeeds.
- Done for checkpoint install policy: add TCP path coverage where a
  checkpoint-required owner read leads the follower worker to install an owner
  state checkpoint on the next policy-enabled tick, resume message catch-up, and
  pass checked promotion.

Risks:

- Avoid coupling the peer directly to static coordination. It should only need
  an address and protocol client behavior.
- Do not hide partial network failures as normal empty batches.

### 2. Coordination-Backed Peer Resolver

Goal: resolve an assignment owner into a usable replication peer from the local
coordination snapshot/watch cache.

Breakdown:

- Done for the resolver core: add a resolver that reads
  `PartitionAssignment.owner` and looks up `NodeInfo` in the current
  coordination snapshot.
- Done for the initial cache: add a small peer cache keyed by owner id.
- Done for address refresh: replace cached peers when the owner's broker
  address changes, and clear a cached peer when the owner node disappears.
- Keep resolver output as `Option<Arc<dyn BrokerOwnerReplicationPeer>>` for now:
  unresolved owner should remain retryable.
- Add tests for missing owner node, changed owner address, stable owner cache,
  and assignment owner moving to another node. These resolver-boundary cases
  are covered. The full owner-change reaction remains a follower supervisor
  concern because it consumes a new `PartitionAssignment`.

Risks:

- This is where too many abstractions can creep in. Prefer one resolver with a
  small cache over separate registry/client-factory layers until the protocol
  implementation proves more is needed.

### 3. Checkpoint Catch-Up Over Transport

Goal: let a follower recover when it asks for records older than the owner's
retained log head.

Breakdown:

- Wire owner checkpoint export and follower checkpoint install through the
  protocol-backed peer.
- Keep worker-triggered checkpoint install policy-gated. Default remains report
  and retry.
- After checkpoint install, resume normal message/event catch-up from the
  checkpoint continuation offsets.
- Add tests for: checkpoint-required response, checkpoint export, install,
  resume catch-up, and promotion eligibility after catch-up.
- Track large-message/log-range behavior in tests, even if optimization waits.

Risks:

- State checkpoints are not payload checkpoints. Message catch-up can still be
  large and must be treated as a separate transfer.
- Owner checkpoint export pauses owner work briefly, so worker policy must stay
  conservative by default.

### 4. Coordinator-Backed Assignments

Goal: replace static coordination in cluster mode with an etcd-backed watch/CAS
implementation while preserving the existing snapshot interface.

Breakdown:

- Define the etcd key layout for nodes, assignments, controller lease, and
  generation.
- Implement node registration with TTL/lease refresh.
- Implement watch-to-`CoordinationSnapshot` cache.
- Implement CAS assignment update with generation/fencing checks.
- Keep static coordination for tests and local deterministic setups.
- Add integration tests against a real or embedded/test etcd if practical.

Risks:

- Coordination failures must be visible and conservative. A broker that loses
  coordination should not silently keep acting as owner forever without a valid
  lease.
- Etcd dependency should be isolated behind the coordination trait so another
  backend remains possible later.

### 5. Controller Loop and Placement Application

Goal: have exactly one active controller compute desired assignments and write
them through coordination.

Breakdown:

- Add controller lease acquisition and renewal.
- Feed current nodes, queue set, existing assignments, and policy settings into
  `PartitionPlacementPolicy`.
- Preserve stable owners/followers where valid. Do not move owners just because
  the planner ran.
- Add controller tests for missing owner repair, missing follower repair,
  stable healthy assignments, and node removal.
- Add basic controller observability: active controller id, last plan time,
  generation, and last error.

Risks:

- Do not combine controller handoff with partition failover semantics. They are
  related but not the same operation.

### 6. Failover and Promotion Orchestration

Goal: convert local primitives into a real ownership transfer sequence.

Breakdown:

- Detect owner loss from coordination lease/node health.
- Pick a caught-up follower, or choose checkpoint/catch-up path before
  promotion.
- Fence old owner through assignment generation/epoch before new owner serves
  traffic.
- Stop follower worker, verify local tails/applied state, promote Stroma queue,
  update broker ownership view.
- Return not-owner or retryable errors while transition is in progress.
- Add adversarial tests: stale owner tries to publish, follower promotes before
  caught up, owner returns during failover, and assignment generation races.

Risks:

- This is the highest correctness area. Prefer explicit refusal over optimistic
  promotion.

### 7. Replicated Publish Confirm Policy

Goal: enforce configured replication durability contracts in publish confirms.

Breakdown:

- Track follower accepted/durable offsets per assignment.
- Have follower workers report progress to broker/coordination or an owner-side
  acknowledgement path.
- In owner publish-confirm flow, wait for policy: local durable, replica
  accepted, replica durable, or majority durable.
- Add timeout/error handling that reports which durability condition was not
  met.
- Add tests for each policy and for insufficient assigned replicas.

Risks:

- Do not block unconfirmed publish paths more than their contract requires.
- The policy must be understandable to operators, not just internally correct.

### 8. Sharding and Client Topology

Goal: let a logical queue span partitions owned by different brokers while
keeping partition choice out of the normal user API.

Breakdown:

- Define queue topology metadata: partition count, owners, followers, generation.
- Add client topology fetch/refresh path.
- Route publish/subscribe traffic to the correct owner connection.
- Decide initial partition selection policy: hash key later, simple round-robin
  or server-selected partition first.
- Keep partition ids mostly internal in docs and user-facing client APIs.
- Add tests for not-owner reroute, owner movement, and multi-partition publish.

Risks:

- Avoid exposing partition mechanics too early. Users should think in queues
  unless they intentionally need advanced routing.

### 9. Operator Visibility and Topology Dashboard

Goal: make cluster state understandable during incidents.

Breakdown:

- Add API surfaces for topology: nodes, assignments, owners, followers, lag,
  health, controller, and transition state.
- Add dashboard topology page. Target: live diagram showing nodes and
  owner/follower links, with table fallback if needed first.
- Add queue-level replication details: role, owner, lag, last catch-up, last
  error, checkpoint-required status.
- Add logs/events for freeze started, drained, role changed, replicated batch
  applied, promotion accepted/refused.

Risks:

- Avoid dumping every low-level metric on the overview page. Put detailed
  replication/debug state on the topology/queue drilldown pages.

### 10. Test Helper Cleanup and Adversarial Tests

Goal: reduce boilerplate enough to make future race and multi-broker tests easy
to write.

Breakdown:

- Add broker test helpers for owner/follower pairs, assignments, publishing,
  catch-up, worker waits, and fake peers.
- Keep helpers small and behavior-shaped. They should not hide the assertion or
  the state transition under test.
- Add adversarial tests around role changes during active work.
- Add TCP end-to-end tests for replication read/apply and checkpoint install.
- Later, add multi-process or containerized chaos tests.

Risks:

- Helper refactors should be separate from behavior changes where possible.

### 11. Code Organization After Mechanics Settle

Goal: reorganize broker/Stroma replication code once behavior is stable enough
that the refactor can be mostly mechanical.

Breakdown:

- Move broker replication types, peer traits, follower worker state, and worker
  supervision into a dedicated broker replication module where practical.
- Split large Stroma responsibilities into focused modules after the current
  replication mechanics stop moving: roles/transitions, replicated ingest,
  checkpoints, snapshots, inspection, and test utilities are likely candidates.
- Keep public surfaces stable during the move. Prefer module extraction over
  behavior changes in the same commit.
- Use the extraction pass to identify abstractions that can be removed or
  collapsed because the real implementation shape is now clearer.
- Keep Keratin generic. Any Fibril/Stroma-shaped concept found during the split
  should move upward, not into Keratin.

Risks:

- Refactoring too early will churn interfaces that are still being designed.
  Wait until protocol peer, resolver, and checkpoint catch-up behavior are
  mostly settled.

### 12. High-Level Scenario Runner

Goal: make end-to-end cluster behavior testable without writing bespoke async
plumbing for every scenario.

Breakdown:

- Add a test-only scenario harness that can start multiple brokers with temp
  data dirs, TCP ports, static or fake coordination, and controlled assignment
  snapshots.
- Provide scenario operations: publish, subscribe/read, advance assignment,
  wait for follower catch-up, stop owner runtime, promote follower, restart
  broker, and inspect queue state.
- Keep it deterministic enough for CI: explicit waits on observed state where
  possible, bounded timeouts only at scenario edges.
- Use it for multi-broker TCP replication, checkpoint recovery, failover,
  not-owner reroute, and later chaos-style tests.
- Keep this as test infrastructure. It should not create production framework
  code unless production behavior genuinely needs it.

Risks:

- A too-powerful scenario DSL can hide important assertions. Start with helper
  functions and a simple harness struct before inventing a language.

## Core Completion Estimate

Rough status as of 2026-06-14, measured against a functional first version with
embedded coordination, pull replication, partition ownership, local failover
mechanics, partitioned queues, and basic operator visibility. These are not
additive percentages, and they are not a production-readiness score.

- Keratin replication primitives: about 85%. Caller-assigned append,
  checkpoint/reset primitives, epoch fencing, and failover-tail promotion exist.
  Remaining work is cleanup, warnings, range/repair refinements if checkpoint
  fallback proves too blunt, and broader corruption/fault coverage.
- Stroma queue replication mechanics: about 85%. Queue roles, freeze/drain,
  follower ingest, state checkpoint export/install, checked promotion, demotion,
  epoch advance, and local-tail failover promotion exist. Remaining work is
  module split, transition cleanup, and more adversarial role-change coverage.
- Broker ownership and assignment model: about 80%. Static and Ganglion-backed
  coordination, assignment planning, owner gates, not-owner redirects,
  assignment watchers, follower-loop supervision, stable placement, and
  per-partition ownership are wired. Remaining work is hardening the server
  bootstrap, retry/escalation policy for promotion refusal, and more failure
  tests.
- Follower pull replication worker: about 80%. Worker state, checkpoint policy,
  protocol-backed owner peer, coordination-backed resolver, supervised loop, and
  durable progress reporting exist. Remaining work is richer backoff/health
  observability, restart/partition chaos, and long-running soak coverage.
- Replication transport: about 75%. Protocol frames, owner reads, follower
  applies, checkpoint export/install, reporter stamping, connection reuse, and
  TCP e2e coverage exist. Remaining work is larger-transfer behavior,
  compression/streaming decisions, and harsher network-fault tests.
- Coordination and controller: about 75%. Ganglion mode starts real multi-process
  coordination, registers brokers and queues, runs the placement controller,
  syncs runtime settings, exposes topology, and drives assignment watches.
  Remaining work is server bootstrap extraction for testability, node-management
  flows, and failure-mode hardening.
- Failover and promotion orchestration: about 65%. TTL-driven owner loss,
  epoch-bumped reassignment, progress-aware candidate selection, follower drain,
  local-tail promotion, and stale-owner demotion are wired. Remaining work is
  promotion-refusal retry/escalation, generation races under partitions,
  follower restart mid-checkpoint-transfer, and operator controls.
- Partitioning and sharding: about 70%. Declared partition counts, catalogue
  entries, client topology, round-robin/keyed routing, partitioning-version
  fencing, per-partition server publish/subscribe, and client fan-in exist.
  Remaining work is live repartitioning, auto-create policy if desired, and
  assignment-narrowed subscriptions for consumer groups.
- Publish-confirm replication durability: about 75%. Replica-durable confirm
  gating, follower durable progress, timeout errors, min-in-sync fail-fast
  refusal, ISR freshness, and two-broker wire e2e exist. Remaining work is
  cross-broker lag/ISR aggregation in topology surfaces and possible per-topic
  overrides.
- Exclusive consumer groups: about 70%. Rust `.exclusive()`, soft targets,
  sticky assignment, per-partition delivery gate, assignment push, reconnect
  restore, member identity, and cross-broker coordinator wiring exist. Remaining
  work is the multi-node coordinator e2e, TypeScript API parity, and optional
  client narrowing with graceful revoke/drain.
- Operator/admin visibility: about 65%. Admin topology API/page, CLI topology,
  queue replication/follower observability, owner-side ISR view, runtime
  settings, and sparse queue visibility exist. Remaining work is cross-broker
  lag aggregation, topology diagram enrichment, node-management UI, and clearer
  incident runbooks.
- Testing for replication and sharding: about 65%. Storage, Stroma, broker,
  protocol, client routing, two-broker replication, cluster tryout, Ganglion
  failure tests, and large placement tests exist. Remaining work is the
  multi-node cohort coordinator e2e, bootstrap test extraction, proxy/fault
  injection, longer chaos/soak tests, and helper cleanup.

Overall for a decent first replication/sharding milestone: about 65-70%.
Overall for production-ready clustered HA: closer to 45-50%. The mechanics are
real now, but production confidence still needs failure injection, runbooks,
more multi-node e2e coverage, bootstrap cleanup, and operator workflows.

Previous completed implementation checkpoints:

1. Done: add StromaEngine forwarding methods for owner message and event
   replication reads.
2. Done: add a broker owner-replication read helper that checks queue ownership
   before touching storage.
3. Done: add broker tests for successful owner reads and not-owner
   rejection before materialization.
4. Done: run focused broker checks.
5. Done: commit the owner-read checkpoint.
6. Next: start the follower pull catch-up loop using the broker owner-read
   boundary and Stroma follower-ingest boundary.
7. Done: add broker follower-apply helpers and an in-process catch-up
   test from owner broker to follower broker.
8. Next: use the same boundaries from a repeated pull loop that can stop when
   both streams return empty batches at the owner's current tail.
9. Done: add a bounded local catch-up helper using owner-read and
   follower-apply broker boundaries. Add multi-pass coverage with small read
   limits. Full checkpoint-install testing remains pending until the snapshot
   handoff path exists.
10. Done: add protocol v1 replication read frame definitions and codec
    tests. Keep this as wire shape only before adding handler behavior.
11. Done: wire protocol handler support for `ReplicationRead`. Keep the
    conversion at the protocol edge: Stroma and broker return typed owner log
    records, while protocol responses carry raw message headers, payloads, and
    raw event bytes.
12. Done: add handler coverage for owner success and not-owner rejection.
    Malformed requests use the existing decode-to-400 path, and there is no
    extra validation yet beyond ordinary frame decoding.
13. Done: add protocol follower-apply frames and handler support. Apply
    requests should carry contiguous raw message/event records plus an epoch.
    The handler converts them back into the broker's owner-read shape before
    calling follower apply. Non-contiguous records are malformed and should be
    rejected before offsets are stripped. Plain apply returns a small success
    summary only. Checkpoint negotiation remains a read/catch-up concern, not
    a raw apply response.
14. Done: prove manual protocol catch-up by composing
    `ReplicationRead` from an owner connection with `ReplicationApply` to a
    follower connection, then promoting the follower after exact catch-up.
15. Next: add protocol/admin or CLI support for a higher-level catch-up command
    after manual read/apply behavior is tested. Avoid adding background
    scheduling until manual pull/apply mechanics are observable and boring.
    This should be the next implementation boundary. A manual catch-up command
    can compose the existing read/apply frames and gives a deterministic way to
    debug replication before introducing follower scheduling, topology watches,
    or retry/backoff policy.
    The protocol crate now has a reusable manual catch-up helper that composes
    two already-connected protocol streams: one owner connection for
    `ReplicationRead`, one follower connection for `ReplicationApply`. It
    repeats until both streams return empty batches, a checkpoint is required,
    or an iteration limit is reached.
16. Next: design the follower background worker around pull correctness:
    repeated pulls from follower-owned offsets, optional owner wake-up hints
    with cooldown, and long-poll or bounded timer safety.
17. Next: design snapshot/checkpoint install for followers that fall behind the
    retained owner logs. The owner should expose compact Stroma state plus the
    continuation offsets where replicated message and event ingest resumes.
18. Later: add coordinator-backed ownership and follower assignment. The first
    implementation can be config-backed for local tests, but the API should
    look like a watch/cache over ownership and follow assignments.

### Protocol Apply Implementation Notes

The first protocol follower-apply pass adds:

- `crates/protocol/src/v1/mod.rs`
  - added `ReplicationApply = 82` and `ReplicationApplyOk = 83`
  - added apply request/response wire structs
  - `ReplicationMessageRecord` now includes `flags: u16` so replication
    preserves Keratin message records rather than only payload/header bytes
- `crates/protocol/src/v1/helper.rs`
  - added codec roundtrip tests for `ReplicationApply` and `ReplicationApplyOk`
- `crates/broker/src/queue_engine.rs`
  - re-exported `OwnerReplicationBatch` so the protocol handler can convert
    through the broker boundary instead of importing Stroma directly
- `crates/protocol/src/v1/handler.rs`
  - added conversion helpers from raw apply batches into
    `BrokerOwnerReplicationRecords`
  - validates contiguous message/event offsets before stripping offsets for the
    broker follower-apply helper
  - decodes raw event bytes back into `StromaEvent`
  - maps successful follower apply into a small `ReplicationApplyOk` summary
  - logs and returns an error if plain apply somehow reaches the broker's
    checkpoint-required branch, because that should be handled before apply
  - added a `ReplicationApply` handler arm
- `crates/protocol/tests/handler_tests.rs`
  - added tests for successful follower apply and non-contiguous malformed
    apply records
  - success test promotes the follower afterward to verify the applied
    message/event logs and state are coherent

### Bounded Read Regression

While adding the protocol catch-up helper, a one-record read limit exposed a
replication correctness bug in Stroma owner reads. `OwnerReplicationBatch`
reported the log tail as `next_offset`, even when the returned batch contained
only an earlier prefix. That would make a follower skip unread offsets under
bounded reads.

The fixed contract is:

- `next_offset` in a normal batch means "resume immediately after this returned
  batch"
- empty batches return `next_offset == requested_offset`
- checkpoint-required responses may still report the owner log's current
  `next_offset`, because they are describing the checkpoint/snapshot handoff
  boundary rather than a consumed batch

Stroma now has regression coverage for bounded message and event owner reads.

Current verification:

- `cargo fmt --all`
- `cargo test --quiet -p fibril-protocol replication_apply --locked`
- `cargo test --quiet -p fibril-protocol replication --locked`
- `cargo check -p fibril-protocol --locked`
- `git diff --check`

- empty apply batches currently become empty `OwnerReplicationRead::Batch`
  values with epoch/offset zero. That is acceptable for this first handler pass
  because the broker treats empty batches as no-op, but revisit if epoch
  accounting needs to distinguish "empty stream at known tail" from "not
  provided"

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
- Split `stroma.rs` into responsibility-focused modules once the current
  replication mechanics are stable enough. It currently holds queue lifecycle,
  replication, snapshots, inspection, global settings, tests, and helper code.
  Doing this after the mechanics settle should keep the refactor mechanical and
  reviewable.

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
- 2026-06-09: Added broker assignment durability policy vocabulary:
  `local_durable`, `replica_accepted(n)`, `replica_durable(n)`, and
  `majority_durable`. The model resolves policies against the assigned replica
  set and validates impossible requirements, but enforcement is intentionally
  deferred until follower accepted/durable offset acknowledgements exist.
- 2026-06-09: Pinned the checkpoint-required broker boundary with tests. Fibril
  keeps checkpoint-required reads as an explicit status without attempting
  partial follower apply or materializing a queue just to report the status. The
  actual checkpoint install operation still belongs in Stroma because it must
  reset local queue logs and compacted queue state together.
- 2026-06-09: Added Stroma follower state-checkpoint install. The API is named
  around state, not messages, because compacted queue snapshots do not contain
  payload bytes. It installs compacted queue state, resets the local event and
  message logs to continuation offsets, keeps the queue in follower role, and
  rejects an install that would advance the follower message log beyond the
  lowest message still referenced by installed state.
- 2026-06-09: Planned owner state-checkpoint export as a controlled
  freeze/drain operation for the first implementation. That avoids exporting a
  snapshot whose bytes and applied event offset can race under live owner
  traffic. A later non-disruptive checkpoint path can move applied-offset
  tracking inside the queue actor if this pause becomes too costly.
- 2026-06-09: Added owner state-checkpoint export and broker forwarding. The
  exported checkpoint separates `message_checkpoint_offset`, the first payload
  the installed state may still reference, from `message_next_offset`, the owner
  message-log tail the follower must reach before promotion. Broker tests now
  cover export, follower install, contiguous message catch-up, and promotion.
- 2026-06-09: Planned checkpoint-aware catch-up as a separate broker helper,
  not a silent behavior change to the existing bounded catch-up loop. The old
  helper is still useful for worker paths that should report
  checkpoint-required and wait for policy before asking the owner to pause and
  export a state checkpoint.
- 2026-06-09: Added the checkpoint-aware broker catch-up helper. It preserves
  the normal bounded catch-up path, and when the old helper reports
  checkpoint-required it exports an owner state checkpoint, installs it on the
  follower, then resumes contiguous catch-up from the checkpoint offsets.
  Focused broker coverage verifies the normal path, while the actual
  checkpoint install mechanics remain covered by Stroma checkpoint tests and
  the broker export/install handoff test. A full branch-triggering broker test
  would currently need either a slow snapshot-worker wait or a test-only
  truncation hook, so it is deferred.
- 2026-06-09: The follower worker currently has state and lifecycle
  registration, but no running owner-transport loop. Checkpoint install policy
  should therefore be recorded in worker config before wiring the loop. The
  default should be conservative because checkpoint-aware catch-up can pause the
  owner to export state.
- 2026-06-09: Added `allow_checkpoint_install` to follower worker config,
  defaulting to false. Worker state now has an explicit
  `should_install_checkpoint` predicate that only returns true when the worker
  is checkpoint-blocked and the policy flag is enabled.
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
- 2026-06-09: Mapped broker not-owner failures into stable TCP protocol errors.
  Publish uses the ordinary `Error` frame and subscribe uses `SubscribeErr`, both
  with conflict status code `409`. The handler keeps the connection open so
  clients can retry, refresh topology, or continue other work. Subscription setup
  now uses a typed local error for duplicate subscriptions instead of `anyhow`.
- 2026-06-09: Added Stroma owner replication read APIs. Owners can read message
  and event records from a requested offset, with explicit record offsets,
  current epoch, and next offset. Reads reject non-owner queues. If the requested
  offset is older than the local log head, the API returns a checkpoint-required
  outcome instead of silently skipping forward. Other gaps are treated as
  corruption because replication must not hide missing owner log ranges.
- 2026-06-09: Added Stroma composition coverage for the first pull-replication
  path. A test now reads owner message/event batches, applies them to a follower,
  promotes the caught-up follower, and verifies delivery works after promotion.
  Adversarial coverage checks that message-only or event-only catch-up refuses
  promotion. This also fixed replicated ingest to advance the follower's applied
  event watermark as events are applied.
- 2026-06-09: Added a protocol-backed owner replication peer adapter. The
  adapter lives in `fibril-protocol`, owns one already-handshaken protocol
  connection, serializes replication requests on that connection, and implements
  the broker's `BrokerOwnerReplicationPeer` boundary. It supports owner record
  reads and owner state checkpoint export, converting wire records back into
  broker/Stroma record shapes. Protocol integration tests cover reading owner
  records and exporting checkpoints through the adapter. Remaining transport
  work is resolver wiring, connection/reconnect management, and typed protocol
  error mapping so not-owner and retryable failures are preserved without
  string parsing.
- 2026-06-09: Added typed replication request errors in the protocol helper and
  promoted the not-owner status code to a protocol-level constant. The
  protocol-backed owner peer now maps wire `ERR_NOT_OWNER` responses back into
  `BrokerError::NotOwner` with the requested queue identity instead of losing
  the condition as an opaque string. Focused protocol coverage verifies this
  through the same TCP handler path.
- 2026-06-09: Added the first static protocol owner-peer resolver. It maps
  coordination owner node ids to broker TCP addresses, opens a fresh protocol
  connection per resolve, performs Hello and optional Auth, then returns the
  protocol-backed `BrokerOwnerReplicationPeer`. Focused protocol tests cover
  successful owner reads through the resolver, missing owner id returning
  `None`, and auth-enabled resolver connections. This intentionally does not
  cache connections yet; connection reuse, reconnect/backoff, topology-watch
  refresh, and real coordination-backed address discovery remain follow-up
  transport work.
- 2026-06-09: Updated the static protocol owner-peer resolver to cache one lazy
  protocol peer per owner id. The historical note above describes the first
  resolver slice; the current behavior is connection reuse with lazy reconnect
  after transport failures.
- 2026-06-11: Added `fibril-coordination-ganglion` spike crate: a ganglion
  raft-backed implementation of the broker `Coordination` trait. A ganglion
  `RaftMetadataNode` replicates the coordination snapshot through real
  consensus; the crate maps losslessly between fibril and ganglion snapshot
  models (queue identity <-> namespaced resource identity, socket addrs <->
  endpoint strings, identical durability/epoch fields) and bridges ganglion's
  committed-snapshot watch into the `watch::Receiver<CoordinationSnapshot>`
  the trait serves. Reads stay sync; proposals are async and leader-only
  (`NotLeader`), with post-consensus stale-generation rejection. Tests cover
  lossless mapping roundtrip and the full propose -> commit -> watch ->
  owns_queue/owner_for/assignment_for path against a real raft node. Spike
  findings: assignment `epoch` already maps 1:1, so the remaining fencing work
  is controller-side epoch issuance (who increments, CAS semantics), not
  schema; raft's u64 node ids stay decoupled from fibril string node ids; the
  provider must be constructed inside a tokio runtime (watch forwarder task).
  This is a spike: not wired into the broker binary; etcd/static remain the
  v1 path per REPLICATION_PLANNING.md.
- 2026-06-12: F1 (provider contract suite) done. `fibril_broker::coordination`
  now exposes `contract_tests::assert_coordination_contract` (under cfg(test)
  or the new `provider-contract-tests` feature): one reusable assertion suite
  covering identity stability, empty-state queries, commit visibility, role
  query/snapshot consistency, epoch passthrough, ownership flips, pre-commit
  watch subscribers observing the latest committed value, and fresh
  subscribers starting at committed state. Both `StaticCoordination` (broker
  tests) and `GanglionCoordination` (coordination-ganglion tests, committing
  through real raft consensus with wait-for-visibility) pass the same suite.
  Ganglion-side prerequisites G1-G3 landed in the ganglion repo: epoch fencing
  rules + guarded CAS proposals (`plan_and_propose_guarded`), durability
  telemetry + serializable `RaftTopology` (the `GET /topology` JSON contract),
  and a scripted cluster playground. Next: F2 controller loop on the embedded
  provider.
- 2026-06-12: F2 (controller loop on the embedded provider) done.
  `GanglionCoordination::control_iteration` implements the planning-loop shape:
  raft-leadership gate (standbys return None and never write), read committed
  state, run the pure placement planner over a caller-supplied live-node set
  (liveness mechanisms stay above this layer), stamp fencing epochs via
  ganglion's rules (owner change bumps, follower churn holds), and propose
  through a guarded CAS write with bounded retries on generation mismatch.
  `ControlError` separates planning rejections from consensus failures.
  Choreography test on a 3-node raft cluster: standby no-op; initial
  assignment at epoch 1 observed by a follower provider's watch; owner removed
  from the live set -> next iteration moves ownership with epoch+1; follower
  watch converges on the failover. Next: F3 broker wiring behind a
  coordination config enum, then F4 fibrilctl topology + admin endpoint.
- 2026-06-12: F4 (topology visibility) + F5 (playgrounds) + tryout docs done.
  Admin: `GET /admin/api/topology` returns the committed coordination snapshot
  (sorted nodes + assignments with owner/followers/epoch, generation) plus an
  optional consensus-internals block via an opaque `RaftTopologyProvider`
  callback (admin crate stays backend-independent); attached with
  `AdminServer::with_coordination`/`with_raft_topology`; endpoint test covers
  bare and fully-wired responses. CLI: `fibrilctl admin topology` renders
  cluster/nodes/assignments/raft tables, `--json` for scripts. Server binary
  attaches a single-node StaticCoordination by default so standalone servers
  report themselves. Playgrounds: `scripts/coordination-playground.sh` (3
  providers + embedded raft controller; scripted kill/reassign shows the
  epoch fence: killed owner's partition moved at epoch+1, untouched partition
  kept its epoch) and `scripts/cluster-tryout.sh` (USER ASK: real processes,
  no in-process simulation - starts N actual fibril-server binaries on real
  ports, health-checks them, runs real fibrilctl topology against each, and
  machine-verifies the reported broker addresses; --keep leaves the cluster
  up for manual play). `COORDINATION_TRYOUT.md` documents the whole flow.
  Remaining from the plan: F3 config-driven provider selection (needs the
  ganglion wire transport to be meaningful across processes) and the admin
  topology diagram (consumes the same JSON).
- 2026-06-12: F3 done - REAL multi-process coordination clusters (user ask: no
  in-process simulation). Ganglion grew a TCP wire transport (frames =
  1-byte format tag + u32 length + body; msgpack default, JSON via
  GANGLION_WIRE_FORMAT=json; receivers decode both, so mixed-format clusters
  interoperate - the per-payload flag idea). fibril-config gained a
  [coordination] section (mode static|ganglion; node_id; ganglion.raft_node_id
  /listen/peers/bootstrap/data_dir) with FIBRIL_COORDINATION_* env overrides.
  fibril-server's composition root starts the embedded durable coordinator
  (start_durable_tcp), bootstraps membership once (re-initialize on restart is
  tolerated), and feeds the admin topology endpoint both blocks.
  `scripts/cluster-tryout.sh --ganglion` starts 3 actual fibril-server
  binaries on real ports, waits for the cross-process raft election, and
  asserts via real fibrilctl calls that ALL nodes report the same leader and
  voter set - confirmed: "shared cluster confirmed: leader=1 voters=[1,2,3]
  on all 3 nodes". COORDINATION_TRYOUT.md updated. Remaining: broker
  self-registration/liveness loop feeding controller live-node input, and the
  admin topology diagram.
- 2026-06-12: Broker self-registration + heartbeat liveness done (and wire
  format now flows config -> argument per user directive; no env reads in
  libraries). Ganglion grew merge commands (RegisterNode/DeregisterNode -
  cannot clobber concurrent updates, no CAS needed) and a ClientWrite RPC on
  the TCP wire so follower processes forward writes to the leader
  (NotLeader + leader hint when contacted node isn't it). The provider gained
  register_self (leader-local or forwarded via topology lookup),
  spawn_heartbeat (log-and-retry on coordination outages - never kills the
  broker), live_nodes(ttl) (heartbeat labels vs local clock; unlabeled nodes
  treated as static/live), HEARTBEAT_LABEL const. fibril-server spawns the
  heartbeat with broker+admin addresses; coordination.ganglion gained
  heartbeat_interval_ms (default 3000). cluster-tryout --ganglion now also
  asserts every node sees all N brokers in the shared node table - confirmed:
  3 real processes, full broker table identical everywhere, registrations from
  followers travel through the leader. ganglion FAILURE_MODES.md gained the
  startup/connectivity section (cannot reach peers, missing bootstrap node,
  wrong peer addresses, listener death, forwarded writes during
  leaderlessness) per user ask, with TTL-vs-election-gap guidance baked into
  the heartbeat design.
- 2026-06-12: Admin Topology page done. New /admin/topology page (nav-linked)
  renders the cluster live from GET /admin/api/topology: summary line, SVG
  diagram (brokers on a ring, owner->follower dashed edges per assignment,
  consensus leader/voters/applied in the center), and the assignments table
  with epochs; auto-refreshes every 3s. Verified serving (HTTP 200, diagram
  mounted) against a real 3-process ganglion cluster with all brokers
  registered. COORDINATION_TRYOUT.md points at the page.
- 2026-06-12: Failure-mode backlog worked down (user directive; replication
  data plane next). Ganglion: six new failure tests (frame-decoder garbage
  fuzz; lone-node startup/join-without-restart; corrupt snapshot + leftover
  .tmp; TCP listener-drop/rebind partition; injected-WAL-failure fail-stop
  with cluster survival; late-joiner snapshot transfer over TCP) - 63 green;
  FAILURE_MODES.md statuses updated + operator runbook added. Fibril:
  provider gained forwarder_alive() and coordination_healthy(); the topology
  raft block now carries "healthy" and "listener_serving" flags (broker
  health surfaces); cluster-tryout.sh gained --staggered (user ask: demo
  nodes joining over time) - shows leader:null/healthy:false alone, election
  on second node, third joining the running cluster, then the standard
  shared-cluster assertions. Remaining open failure items: asymmetric
  partition chaos, leader-on-minority end-to-end. NEXT: replication data
  plane integration (controller assignments consumed by broker queue
  ownership, Keratin epoch checks per the phasing).
- 2026-06-12: Added the R-phase integration plan to REPLICATION_PLANNING.md:
  connects the completed coordination plane (ganglion F-phases) to the
  existing replication data plane. R1 cluster queue catalogue (snapshot
  `resources` set + Register/DeregisterResource merge commands, broker declare
  forwarding); R2 leader-gated controller task in the server (live_nodes TTL,
  anti-churn no-op guard, controller observability block); R3 the ownership
  switch (GanglionCoordination as QueueOwnership + the existing supervised
  assignment watcher + CoordinationProtocolOwnerPeerResolver; two-broker e2e
  coordination-driven replication test; tryout/diagram payoff); R4 failover
  choreography over existing primitives (promotion-tails decision flagged);
  R5 publish-confirm enforcement design notes (offset reporting piggybacked
  on pulls); R6 client topology unchanged. Supersedes the etcd-shaped
  Medium-Term §4/§5 for the embedded path; etcd stays possible behind the
  same trait. Decisions flagged: resources-as-separate-set, and
  promote-to-local-tail vs quorum-tails under epoch fencing.
- 2026-06-12: R1 done (both sides). Ganglion: CoordinationSnapshot gained
  `resources` (cluster queue catalogue) and `attributes` (opaque replicated
  KV for e.g. runtime settings), both serde-default so existing WALs/snapshot
  files load unchanged; merge commands RegisterResource/DeregisterResource/
  SetAttribute/RemoveAttribute (idempotent; same-value set is a generation
  no-op; dereg does not touch assignments). Provider: register_queue/
  deregister_queue/registered_queues, cluster_attribute/set_cluster_attribute,
  all through a shared leader-or-forwarded merge path (register_self
  refactored onto it); control_iteration now preserves resources+attributes
  across its snapshot-replace writes. Test proves the loop: register queue ->
  visible; set attribute -> visible; controller iteration assigns the
  catalogue queue AND preserves catalogue+attributes; deregister empties.
  Catalogue ingestion strategy decided: a broker-side catalogue-sync loop
  (diff local engine queues vs catalogue each heartbeat tick, register
  missing) instead of hooking the declare handler - idempotent, and catches
  pre-existing on-disk queues after restart, which a declare hook would miss.
  R2b (runtime settings over attributes) planned in REPLICATION_PLANNING.md.
  Next: R2 controller task + catalogue sync in fibril-server.
- 2026-06-12: R2 done - the declare-to-assignment loop works across real
  processes. Provider grew spawn_controller (leader-gated loop: watch + tick
  wake-up, live_nodes(ttl) + registered_queues inputs, ControllerStatus cell
  for observability) and spawn_catalogue_sync (diff local engine queues vs
  catalogue, register missing; idempotent; covers on-disk queues after
  restart). control_iteration hardening: preserves the committed NODES map
  (planner output must not strip heartbeat labels - would have silently
  broken liveness) and an anti-churn guard (no-op plans never touch the raft
  log). Config: coordination.ganglion.{target_followers, controller_tick_ms,
  liveness_ttl_ms}. Server wires catalogue sync (engine queue_stats_snapshot)
  + controller; controller status rides the topology raft block. Loop test:
  assign at epoch 1, idle generations frozen, dead-broker failover with
  epoch+1. REAL BUG found by the tryout: ResourceIdentity-keyed assignment
  maps cannot be JSON map keys ("key must be a string") - never surfaced
  before because nothing had written a NON-EMPTY assignments map through the
  JSON WAL until the controller's first assignment. Fixed in ganglion by
  serializing assignments as a pair sequence (works in JSON and msgpack);
  WAL fixture bumped to v2 with the no-migration-needed argument recorded
  (v1 could only ever contain empty maps). cluster-tryout --ganglion now
  declares a queue via the real CLI and asserts the controller assignment
  (owner/follower/epoch) is identical on every node - passing. Next: R2b
  (runtime settings over attributes), then R3 (ownership switch).
- 2026-06-12: R2b done - runtime settings are cluster-replicated (user
  requirement). Ganglion grew CompareAndSetAttribute (deterministic in-apply
  CAS; rejection AttributeMismatch carries the actual value) so concurrent
  publishers serialize; WireFormat::from_env deleted (settings-discipline
  audit: no env reads outside the config crate anywhere; legacy replay-profile
  env constructors remain as explicitly named opt-ins on the legacy node).
  Provider: ClusterRuntimeSettings document (cluster_version independent of
  per-node store versions) under the fibril/runtime_settings attribute;
  publish_runtime_settings (bounded CAS loop); spawn_runtime_settings_sync
  (applies documents via the manager's versioned update; equal-settings
  short-circuit so the publishing node does not double-store; locked-field
  rejections log loudly and are marked seen). forward_command now maps wire
  rejections to errors on the forwarded path (parity with the local path).
  Admin PUT hook: settings_published channel on AdminServer feeds a publisher
  task in the composition root. Gate test: update stored on manager A,
  published, sync loop converges manager B; second publish bumps the cluster
  version via CAS. cluster-tryout --ganglion now PUTs settings on node-1 and
  asserts nodes 2 and 3 converge - passing. Tryout gained a fail-fast port
  guard after a debugging session was poisoned by a stale --keep cluster
  answering on the test ports. Next: R3 - the ownership switch.
- 2026-06-12: R3 done - the ownership switch is live. GanglionCoordination
  now implements broker QueueOwnership (cluster brokers serve only assigned
  queues; standalone keeps OwnAllQueues). fibril-server restructured: the
  embedded coordinator starts BEFORE broker construction so ownership is
  injected at Broker::new_with_ownership; in ganglion mode the server spawns
  spawn_assignment_watcher_with_follower_replication with
  CoordinationProtocolOwnerPeerResolver, so assignment transitions and
  supervised follower loops run coordination-driven. R3 gate test (protocol
  crate, passing first run): a controller-written assignment - no manual
  transitions anywhere - makes the supervised watcher start the follower
  worker, resolve the owner's broker address from the snapshot node table,
  replicate two published messages over real protocol TCP to CaughtUp, and
  checked promotion lands at exactly the owner checkpoint offsets. Full
  sweeps green (protocol 43, broker 121, coordination-ganglion 8);
  cluster-tryout passes in both modes. Next: R4 failover choreography
  (TTL-driven owner loss -> epoch+1 reassignment -> drain -> checked
  promotion with progress-aware candidate selection) + adversarial suite.
- 2026-06-12: R4 core done - automatic failover works end to end. Stroma
  (keratin repo) grew promote_queue_follower_to_local_tail: failover-path
  promotion at the follower's OWN tails (the dead owner cannot supply
  expected tails; the bumped assignment epoch fences its unreplicated
  suffix), keeping the events-applied gate (every locally recorded event must
  be applied before serving). The broker's PromoteFollowerToOwner transition
  arm is no longer deferred: it drains the follower worker (no promotion
  racing a mid-batch ingest), promotes at local tails, and on refusal leaves
  the queue a follower with a loud warn (explicit refusal over optimism).
  Never-materialized queues stay cold (Noop, same rule as BecomeOwner) - the
  old "defers without offsets" test pinned the now-replaced deferred behavior
  and was updated to pin cold-stays-cold instead. R4 gate test (first-run
  pass, stable x3): owner dies (drops from live set) -> controller reassigns
  with epoch+1 -> the follower's supervised watcher drains + promotes -> the
  promoted broker ACCEPTS A PUBLISH as owner, and its message log continues
  at exactly replicated-history+1 (nothing lost, nothing duplicated). Full
  sweeps green (broker 121, protocol 44). Remaining R4 items: progress-aware
  candidate selection via heartbeat tails (matters only for >1 follower),
  old-owner-returns demotion e2e, and the rest of the adversarial list.
- 2026-06-12: R4 tail largely done. Progress-aware failover candidate
  selection: followers advertise per-assignment applied tails in heartbeat
  labels (applied/<topic>/<part>[/<group>] = msg:event via
  spawn_heartbeat_with_labels; the server feeds them from
  sparse_queue_observability_report); control_iteration's failover path
  prefers the most caught-up LIVE committed follower by event tail when the
  owner is dead and ownership moves, keeping the displaced planner pick in
  the replica set. Advisory only - checked promotion remains the authority.
  Test: b-slow sorts first but c-fast (higher tails) wins ownership at
  epoch+1, b stays follower. Old-owner-returns adversarial e2e: the previous
  owner's own watcher sees the fenced assignment, tears down the owner
  runtime, demotes to follower, and new publishes on it FAIL (no silent
  stale writes); first-run pass. Promote-before-caught-up note: under the
  decided promote-to-local-tail policy, mid-catch-up promotion is by-design
  accepted at the drained local tails (the epoch fences the rest); the only
  refusal condition is recorded-but-unapplied events, which Stroma's gate
  covers in its own tests. Remaining R4 niceties: promotion-refusal
  retry/escalation path, generation-race-under-partition chaos, follower
  restart mid-checkpoint-transfer. Also noted in planning (user): admin UI
  node management + programmatic scale-up/down flows as post-R6 QoL.
- 2026-06-13: Data-plane epoch fencing wired (the split-brain last line).
  Stroma grew advance_queue_epoch (both logs, persisted in the Keratin
  manifest BEFORE use, monotonic), become_queue_owner_with_epoch /
  become_queue_follower_with_epoch, and promote_queue_follower_to_local_tail
  now takes the assignment epoch and persists it before serving as owner.
  Broker transition arms thread transition.next.epoch into every role change:
  BecomeOwner and PromoteFollowerToOwner fence as owner; BecomeFollower and
  DemoteOwnerToFollower fence the follower logs so stale-epoch replicated
  batches are rejected at the storage layer (Keratin already carried epochs
  in batches and StaleEpoch outcomes - the missing link was role transitions
  never advancing the log epochs from the assignment). New broker surface
  advance_replication_epoch (also the substrate for future manual-fence
  tooling). Gate test epoch_fenced_follower_rejects_stale_owner_batches: a
  follower fenced at epoch 2 rejects an epoch-0 owner's batches with
  StaleEpoch{current:2, attempted:0} on BOTH logs and applies the identical
  records once the owner advances - the planning doc's "even if coordination
  misbehaves" defense is now real. The fence immediately caught both
  coordination e2e tests using watcher-less harness owners (epoch 0 reads
  vs fenced followers) - fixed by applying what the owner's watcher does in
  production (advance to the assignment epoch). One watcher test updated to
  poll for the worker instead of insta-asserting after materialization
  (epoch persist materializes the queue earlier in the transition). All
  suites green: broker 122, protocol 45, provider 9, stroma 162;
  cluster-tryout --ganglion passing.
- 2026-06-13: R5 core done - publish-confirm durability enforcement is live.
  Design: followers now apply replicated batches DURABLY
  (KDurability::AfterFsync), so the offsets they pull from are honest durable
  watermarks; replication reads carry an optional reporter_node_id
  (serde-default, wire-compatible; old peers simply do not report), stamped
  automatically by CoordinationProtocolOwnerPeerResolver with the local node
  id. The owner-side handler records per-follower durable progress into a
  per-queue registry with waiter wake-up. The assignment watcher maintains an
  assignment cache (durability policy + replica set per local queue; removed
  on unassignment; absent for standalone brokers = local-durable confirms,
  unchanged behavior). confirm_sink_loop now gates the producer reply: local
  durable append first, then ReplicationConfirmGate::await_confirm resolves
  the assignment's durability_requirement (owner counts as one node) against
  reported follower progress, timing out with a descriptive
  which-condition-failed error after replication.confirm_timeout_ms - a NEW
  RUNTIME SETTING (default 5000ms, serde-default so persisted settings
  documents load unchanged; rides the replicated settings path from R2b).
  Gate tests: publish under ReplicaDurable{2} times out without any follower
  report and resolves once recorded progress passes the offset; the R3
  supervised e2e now also asserts the owner accumulated the follower's
  progress from real stamped reads over TCP. All suites green (broker 123,
  protocol 45+13, provider 9); cluster-tryout --ganglion passing. Remaining
  R5 tail: min_in_sync_replicas refusal knob, surfacing per-follower progress
  /lag in topology observability, and an e2e confirm-over-wire test with two
  real brokers under replica_durable policy.
- 2026-06-13: Settings classification pass over everything replication added.
  Findings and fixes: (1) replication.confirm_timeout_ms was runtime-only
  with no startup seed - runtime_seed.replication added to the config crate
  so first boot can set it like every other runtime section. (2) The
  follower pull intervals (caught_up_poll_ms / retry_poll_ms /
  checkpoint_retry_poll_ms) were hardcoded worker defaults that had escaped
  both settings channels; caught_up_poll_ms directly bounds replica-durable
  confirm latency. They are now runtime settings (replication section,
  cluster-replicated, seedable), flowing into BrokerConfig; the production
  watcher opts its workers into per-tick refresh via
  FollowerReplicationWorkerConfig.follow_runtime_settings (default false, so
  tests pinning explicit poll values keep exact behavior). (3) Boot-time
  ganglion knobs (raft_node_id, listen, peers, bootstrap, data_dir,
  wire_format) confirmed correctly boot-only. heartbeat_interval_ms /
  liveness_ttl_ms / controller_tick_ms stay boot-time deliberately (the
  settings-replication path rides ON coordination - chicken and egg), but
  config validation now rejects liveness_ttl < 2x heartbeat_interval, the
  flap-inducing misconfiguration. (4) target_followers is replication-factor
  POLICY misfiled as per-node boot config (only the controller's copy
  matters; divergent values = placement depends on who wins the controller
  race). Backlog: move it into replicated runtime settings together with the
  programmatic scale-up/down QoL work. Test debt logged: multi-node (5+)
  topology matrices, unreliable-infra testing (partitions, latency, drops -
  needs proxy-based fault injection in the tryout harness or in-process
  transport shims), deterministic-clock abstraction for simulation tests.
  Clock decision: NO cluster clock synchronization abstraction needed for
  correctness today - ownership/fencing is epoch+raft based, never
  wall-clock-lease based. Wall clocks cross nodes only in (a) liveness TTL
  checks (advisory; worst case an unnecessary failover, which epoch fencing
  makes safe) and (b) producer-set not_before delays (timing accuracy only).
  Revisit only if we add client-visible cross-node ordering or wall-clock
  leases.
- 2026-06-13: Scaling sanity test + design notes from review questions.
  Added placement_scales_to_large_clusters (broker coordination, 75 nodes /
  600 queues / target_followers=2): asserts full placement on live nodes with
  exact replica counts, owner load balanced within one queue, re-plan is a
  pure no-op (anti-churn stability), and a one-third mass node failure
  rebalances only orphaned queues while every surviving owner is preserved.
  This exercises the pure placement axis at scale cheaply. Open follow-up:
  drive control_iteration / candidate-selection / live_nodes registry at the
  same node counts (needs a multi-node raft harness, heavier - tracked with
  unreliable-infra testing). NOTE the realistic scaling axis: a 75-NODE
  cluster is 3-5 raft VOTERS + ~70 coordination participants (brokers that
  register/heartbeat and receive assignments), NOT 75 raft voters. Tests
  should scale the registry/placement/fan-out, not the voter count.

  WIRE FORMAT on-demand switching (review Q): receivers are already per-frame
  format-agnostic (tag byte -> decode either msgpack or json; mixed clusters
  already work). The ONLY fixed thing is each sender's outbound choice, taken
  from startup config at construction. "Transparent switching" therefore needs
  nothing on the receive path; it would only mean making a sender's outbound
  WireFormat a runtime-swappable cell (ArcSwap) instead of a constant. Verdict:
  the real goals (json-for-debug / msgpack-for-prod, flag-day-free migration)
  are ALREADY met by per-node config + mixed tolerance. Live per-node outbound
  toggling without restart is a small operator nicety, not a foundation need;
  cheap to add later as a runtime setting if an operator ever wants it.
  Not doing it now.

  CLOCK abstraction (review Q): re-confirmed NOT needed for correctness.
  For tests the honest benefit is moderate and CONCENTRATED in the
  coordination liveness/failover tests that today burn real wall-time on short
  TTLs (900ms etc.) + tokio::time::timeout polling - those could become
  instant+deterministic under tokio::time::pause() with an injectable clock.
  But it is a cross-cutting refactor (every unix_millis()/SystemTime::now() in
  coordination + broker), and current tests pass and aren't egregiously slow.
  Decision: defer, and do it TOGETHER with unreliable-infra/fault-injection
  testing (same files, same goal of deterministic time control). Not piling it
  onto the already-huge replication feature now.

  DECLARE / partition count (review Q, brainstorm - R6, not yet built):
  declare should register the QUEUE (topic + group + config), not hardwire a
  single partition. Direction to flesh out: (1) topic carries a
  partition_count in the resource catalogue (replicated attribute or resource
  field); declare creates the topic with N partitions, fanning out N
  QueueIdentity placement units to the controller. (2) Partition count is a
  topic property, not a queue-instance property - groups share the topic's
  partitioning. (3) Repartitioning (changing N later) is the hard part:
  message-key->partition routing means raising N remaps keys and breaks
  per-key ordering for in-flight data. Options to weigh next: fixed-at-create
  (simplest, ship first); grow-only with explicit rehash/migration job;
  consistent-hashing/virtual-nodes to soften remaps. Lowering N must drain+
  merge. Recommend: ship fixed-at-create partition_count for R6 multi-
  partition, treat live repartitioning as a separate later milestone.
- 2026-06-13: R5 min_in_sync_replicas (Kafka min.insync.replicas) landed.
  An in-sync-replica FLOOR that refuses replica-durable publishes fast when too
  few replicas are healthy, instead of accepting and hanging until the confirm
  timeout. Two new runtime settings (replication section, cluster-replicated +
  seedable like the rest): min_in_sync_replicas (default 1 = OFF, fully
  preserves prior behavior) and isr_timeout_ms (default 10s, freshness window).
  In-sync = owner (always) + assigned followers whose last progress report is
  within isr_timeout_ms; the progress cell now timestamps each report
  (std::time::Instant) so a silent follower drops out of ISR. The floor is
  enforced in ReplicationConfirmGate::await_confirm, only for replica-durable
  policies (required_followers > 0), in two stages before the durability wait:
  STATIC infeasibility (floor > assigned replica set -> can never satisfy ->
  refuse) and DYNAMIC shortfall (healthy count < floor -> refuse). New
  BrokerError::NotEnoughInSyncReplicas {topic, partition, in_sync, required}
  gives clients a distinct retæable signal vs a generic timeout. Tests
  (broker_tests, all fast = proving fail-fast not timeout, with a 60s confirm
  timeout in the refuse cases): floor exceeds replica set; no healthy follower;
  stale follower excluded (isr_timeout 0 makes even a just-recorded report
  stale); floor met -> admitted and resolves. Kafka parity notes: the floor
  only bites replica-durable producers (local-durable/acks=1 bypass it, by
  design); a cold-start window exists where the first publish before any
  follower has pulled is refused until ISR forms (followers pull every
  caught_up_poll_ms ~1s) - matches Kafka's "replica not yet in ISR". All
  suites green (broker 35 lib + 96 integ, protocol 45+13, provider 9, config
  8); cluster-tryout --ganglion passing. R5 tail remaining: per-follower
  lag/ISR in topology observability surfaces; two-real-broker confirm-over-
  wire e2e. Future: per-topic min_in_sync override (currently broker-wide).
- 2026-06-13: Owner-side replication-lag / ISR observability + conventions.
  Added an owner-side view to the broker observability report: for every queue
  THIS broker owns (filtered via the ownership trait over the assignment
  cache), per-follower {durable message/event next-offset, last-report age,
  in_sync} plus per-queue {durability policy, in_sync_replicas vs
  min_in_sync_replicas floor, below_floor flag} and a summary
  {owned_queue_count, below_floor_count}. in_sync uses the same freshness rule
  as the publish gate (reported within isr_timeout_ms); a never-reported
  follower shows age=None, in_sync=false. Surfaced through the admin debug
  queues endpoint (owned_replicas / owned_replica_summary cherry-picked
  alongside the existing replication_followers), so `fibrilctl admin queues`
  carries it. Test asserts the in-sync count, below-floor flag, fresh vs
  silent follower. NOTE this is the per-broker (owner's-own-followers) view;
  cross-broker aggregation into `fibrilctl topology` and the admin web diagram
  are a follow-up (the topology endpoint is coordination-level and would need
  to fan out to each broker's observability). Earlier same-day: scrubbed all
  roadmap phase labels (R3/R4/R5/R2b/F2) out of .rs files (kept in these
  planning/worklog docs); reworked the min-in-sync floor to a fail-fast
  PRECONDITION (a degraded cluster errors immediately rather than hanging
  every publish to the confirm timeout; the durability ack-count stays a
  bounded wait); locked time bounds into the ISR tests so a fail-fast that
  secretly waits fails loudly; removed expect() from the progress-cell locks
  (poison-recovering lock_followers helper).
  BACKLOG (general, not replication): a codebase-wide pass to remove
  pre-existing .unwrap()/.expect() from production code (allowed only in
  tests/benches/demos) - propagate via ? / error types, recover mutex poison
  via PoisonError::into_inner. New code already follows this.
- 2026-06-13: Two-broker confirm-over-wire e2e (R5 tail). New handler test
  replica_durable_confirm_resolves_over_wire_from_follower_progress drives the
  full durability contract between two REAL brokers over TCP: owner owns the
  queue with a ReplicaDurable{2} assignment cached; a second broker materializes
  the queue and runs the follower replication worker loop against the owner's
  protocol listener via a StaticProtocolOwnerPeerResolver configured
  with_reporter("follower-a") so its reads are stamped. A publish to the owner
  under the replica-durable policy resolves its confirm ONLY because the
  follower pulls the record over the wire and its stamped reads advance its
  durable progress past the offset in the owner's progress registry, satisfying
  the confirm gate; the test also asserts the owner recorded that wire-sourced
  progress. Brisk follower poll (50ms) keeps it fast; an 8s timeout bound makes
  a regression fail loudly rather than hang. Proves the confirm path end to end,
  not just at the in-process gate. This closes the core R5 durability story
  (enforcement + min-in-sync floor + per-broker lag/ISR observability + this
  wire e2e). Remaining R5-adjacent follow-up: cross-broker lag aggregation into
  `fibrilctl topology` and the admin web diagram.
- 2026-06-13: R5 closed; retention decided; R6 planned + scoped. R5 is fully
  done (confirm enforcement, min-in-sync floor, per-broker lag/ISR
  observability, two-broker confirm-over-wire e2e). Retention/truncation:
  DECIDED to rely on checkpoint-install fallback, no strict follower floor
  (see PLANNING "Truncation rules"; the chain is already covered by the
  stroma truncation->CheckpointRequired test + worker-installs-checkpoint test
  + checkpoint-install-composes-with-catch-up test). R6 agreed: client
  topology awareness FIRST, then multi-partition fixed-at-create, with live
  repartitioning as a forward-compat TARGET (partition_count + partitioning_
  version versioned in the catalogue; routing parameterized by the version;
  see PLANNING "R6 partitioning plan").
  R6 increment breakdown (from code exploration):
  1. Coordination: client-facing topology view (queue -> owner node_id +
     broker_addr, + partitioning_version) from committed snapshot + node
     registry. [foundation, self-contained]
  2. Protocol: new Topology request/response op; structured not-owner-redirect
     error carrying the current owner address + partitioning_version.
  3. Handler: inject a topology source into handle_connection (mirror of how
     admin gets with_coordination/with_raft_topology); answer Topology queries;
     enrich NotOwner errors with the owner hint.
  4. Client: topology cache + route publish/subscribe to owner + refresh on
     not-owner/stale + multi-connection management; keep partition selection
     out of the user API. Stamp partitioning_version on publishes.
  Current state observed: client is single-connection with NO topology
  awareness or not-owner handling; protocol has no topology/metadata op; the
  Topology response will carry partitioning_version from day one (constant
  until multi-partition lands) so the wire stays forward-compatible.

- 2026-06-13: R6 Phase B progress + detailed resume state (context-compaction safety).
  COMMITS so far this phase: B1 per-(topic,group) partitioning via CAS
  (declare_queue_partitioning/queue_partitioning, key fibril/partitioning/<topic>[/<group>],
  QueuePartitioning{partition_count,partitioning_version}); attribute-CAS
  create-once/idempotency test in ganglion; B2 declare fan-out (DeclareQueue.partition_count:
  Option<u32>, default_partition_count runtime setting through all channels,
  declare records partitioning + registers N catalogue entries via the injected
  QueueDeclareCoordinator hook [server.rs CoordinationDeclareCoordinator], standalone
  materializes N locally, reports effective count; catalogue_sync stays partition-0
  because metrics QueueKey lacks partition — declare registers the full set);
  server.rs refactor "c" (extracted runtime_seed_from_config + the two coordination
  bridges into crates/fibril/src/lib.rs with tests); B3 FOUNDATION (5a22ed0):
  Publish/PublishDelayed gained partition_key: Option<Vec<u8>> (serde-default),
  QueueTopologyEntry + coordination ClientQueueTopology gained partition_count
  (authoritative N read from partitioning metadata in client_topology()).

  DESIGN DECISIONS (settled with user): routing = explicit `partition` override
  -> else hash(partition_key)%N if key present -> else ROUND-ROBIN/sticky
  (default; minimal-setup-just-works, key is opt-in). partition_key is purely
  partition selection (Kafka-style), NOT a RabbitMQ routing key; client-side
  routing input. Type Vec<u8> (not bytes crate; small, hashed once, consistent
  with payload). Partitioner choosable + version-parameterized (consistent-hash
  is the future repartition-friendly upgrade, swappable under partitioning_version).
  Partition count: explicit-at-declare else default setting; gated auto-create
  on first publish is a deferred follow-on; NEVER load-based autoscale.
  Delivery stays OFFSET order (no publish-time sort); cross-partition
  time-merge is a deferred opt-in client-side best-effort option.

  NEXT — B3a (client routing), all in crates/client/src/lib.rs:
   1. Add `partition: u32` to the 4 Command::Publish* variants (enum ~line 1560)
      and use it in the engine frame-build (the `partition: 0` hardcodes ~line
      2034/2049/2063/2079) instead of 0.
   2. EngineHandle publish methods (publish_unconfirmed / publish_with_confirmation
      / publish_unconfirmed_delayed / publish_delayed_with_confirmation, ~2360-2500)
      take `partition: u32` and pass into the Command.
   3. TopologyCache: store partition_count per queue (topology replace() now has
      QueueTopologyEntry.partition_count); add `partition_count(topic, group)->u32`
      (default 1 when unknown).
   4. Partitioner fn: route(key: Option<&[u8]>, explicit: Option<u32>, count: u32,
      rr: &AtomicUsize)->u32. hash%N keyed / round-robin keyless / explicit override.
      Add a per-client (or per-queue) AtomicUsize round-robin counter in ClientShared.
   5. Publisher methods (~1116-1240): resolve count from cache, compute partition,
      pass to engine.publish_*; route via engine_for(topic, partition, group)
      (already partition-parameterized).
   6. Tests: partitioner unit test (round-robin spread, hash determinism, explicit
      override); routing integration via the mock broker (crates/client/tests/redirect.rs
      style) — keyless spreads across partitions, keyed sticks.
  THEN B3b: per-message key API (NewMessage.partition_key builder + internal
  message struct field; default publish() has no key -> round-robin). THEN B4:
  owner-side version fence (stamp partitioning_version on the publish wire; server
  rejects/redirects stale-version routing). THEN B5 (subset subscriptions +
  multi-owner fan-in), B6 (multi-partition tests).

  REFACTOR DEBT: server.rs "b" (extract coordination bootstrap + broker background
  spawns + admin wiring into fibril lib.rs) deferred to END of Phase B to avoid
  churn while B3-B5 edit server.rs. Keep new wiring placed cleanly per the
  fibril/ganglion separation (reusable->ganglion, glue->fibril).

- 2026-06-13: B3a DONE (5c41067). Client routes publishes by partition:
  `partition: u32` threaded through the 4 Command::Publish* variants, the 4
  EngineHandle publish_* methods, and the engine frame-build (was hardcoded 0);
  TopologyCache gained `counts: HashMap<(topic,group),u32>` + `partition_count()`
  (populated from QueueTopologyEntry.partition_count in replace()); ClientShared
  gained `round_robin: AtomicUsize` + `route_partition(topic,group,key)` using a
  stable inline FNV-1a (`fnv1a`) — explicit-not-yet -> hash%N if key -> round-robin
  over N; N from the cache (unknown => partition 0). All 5 Publisher publish
  methods compute the partition and route via engine_for(topic, partition, group).
  Client tests green (in-memory: empty topology => partition 0 => unchanged).
  REMAINING:
  - B3 test: partitioner unit test (fnv1a determinism; round-robin spread;
    explicit/key behavior) + a mock-broker routing integration test
    (crates/client/tests/redirect.rs style: keyless spreads across partitions,
    keyed sticks to one) — the mock would answer Op::Topology with
    partition_count>1 so the client's route_partition has N>1.
  - B3b: per-message key API. Add partition_key to NewMessage builder
    (.partition_key(impl Into<Vec<u8>>)) + the internal message struct; thread
    message.partition_key into route_partition(.., Some(key)) in the 5 Publisher
    methods (currently pass None). Default publish() = no key = round-robin.
    (Wire Publish.partition_key currently sent as None from the engine frame
    build; populate it from the command if we want server-side use, else leave
    client-side-only.)
  - B4: owner-side version fence — stamp partitioning_version on the publish
    wire (add field; client stamps the version it routed under from the cache);
    server rejects/redirects publishes carrying a stale partitioning_version.
  - B5: subset subscriptions + multi-owner fan-in. B6: multi-partition e2e tests.
  - Then server.rs refactor "b" (coordination bootstrap/spawns/admin wiring ->
    fibril lib.rs).
- 2026-06-13: B3b DONE (299e988) + B3 tests DONE. B3b added
  NewMessage.partition_key(impl Into<Vec<u8>>) builder + field, threaded
  message.partition_key.as_deref() into route_partition in all 5 Publisher
  methods, fnv1a_is_deterministic_and_distributes unit test. B3 routing
  integration test DONE (03fed36): crates/client/tests/redirect.rs mock gained
  self_partitions:(topic,count) (answers Op::Topology with N self-owned entries)
  + recorded_partitions recorder; keyless_publishes_spread_keyed_publishes_stick
  asserts keyless fan out (>1 distinct partition) and same-key sticks to one.
  Client tests green (22 unit + 6 redirect-integration + doctests).
  NEXT — B4: owner-side version fence. Add partitioning_version:u64 to the
  Publish/PublishDelayed wire (serde default 0). route_partition must also return
  the version it routed under (from TopologyCache OwnerEntry/counts) so the
  client stamps it; thread version through the 4 Command::Publish* variants +
  EngineHandle publish_* + engine frame-build. Server (protocol handler) compares
  the frame's partitioning_version against the queue's authoritative version; if
  stale -> Op::Redirect with the current owner/version so the client re-fetches
  topology and re-routes. Version 0 (default/unknown) should pass when the queue
  is single-partition v0 to keep the in-memory/non-cluster path unchanged.
- 2026-06-13: B4 DONE (6f7407d). Publish/PublishDelayed wire gained
  partitioning_version (serde default 0). Client: TopologyCache.counts now stores
  PartitioningEntry{count,version} (was bare u32); partition_count() ->
  partitioning(); route_partition returns Route{partition, partitioning_version}
  (version from the cache entry, 0 when unknown); threaded through the 4
  Command::Publish* variants + 4 EngineHandle publish_* methods + the engine
  frame-build (stamps partitioning_version on the wire). Server: handler
  fence_stale_partitioning() — if topology_source.owner_endpoint() reports a
  version higher than the publish's, emit Op::Redirect (current owner+version)
  and skip the publish; checked at the top of both Op::Publish and
  Op::PublishDelayed arms. Equal/greater client version (incl. standalone v0)
  proceeds. Cleaned two stale phase-label comments in client lib.rs while there.
  Tests: handler stale_partitioning_version_publish_is_fenced (server redirects +
  stamps current version); client publishes_carry_routed_partitioning_version
  (client stamps the routed version on every frame). Whole workspace green (33
  suites). Mechanical: partitioning_version:0 added to every Publish literal
  across protocol/client/admin/tui-example + benches + handler_tests.
  GAP for B5/B6: the server publish path still keys the broker publisher by
  (topic, group) and ignores pubreq.partition — multi-partition publishes
  currently collapse to one log server-side. Per-partition server routing (each
  (topic,partition,group) is its own Stroma log) is the B5/B6 work.
  NEXT — B5: subset-capable subscriptions + multi-owner subscription fan-in, plus
  the server-side per-partition publish routing the fence/topology already assume.
- 2026-06-13: B5 (publish half) DONE (b62b777). Server-side per-partition publish
  routing: broker.get_publisher hardcoded `let part: LogId = 0`, collapsing every
  publish into one log no matter the partition the client routed to and stamped.
  Broker was already partition-aware below it (QueueKey.part, ensure_queue_owner,
  engine.materialize all take partition), so: get_publisher gained a `partition:
  LogId` param; handler keys the publisher cache by (topic, u32, group) and passes
  pubreq.partition to get_publisher + the not-owner redirect. ~70 test/bench call
  sites pass partition 0 (single-partition path unchanged). Test
  publishes_route_to_independent_partition_logs (broker_tests) proves two
  partitions of one (topic,group) keep independent offset sequences + messages,
  read back via read_owner_replication_records per partition. Whole workspace
  green (33 suites).
  NEXT — B5 (subscribe half): subscribe() is still partition-0-only. Make
  subscribe partition-aware (each (topic,partition,group) is its own log/cursor),
  then client-side multi-owner subscription fan-in so one logical subscription
  consumes all partitions (subset-capable for consumer-group assignment later).
  Once subscribe is partition-aware, add a true multi-partition publish->subscribe
  e2e test (publish to p0/p1, subscribe to each, assert isolation) to replace the
  replication-read-based proxy used for the publish half.
- 2026-06-13: B5 subscribe half DONE (server 5a73990 + client fan-in 1711ddf).
  DECISION (memory subscription-fanin-model): transparent client-side fan-in,
  per-partition ordering only (ordering opt-ins later); default = consume ALL
  partitions, but built around an assignment set so a coverage-first
  consumer-group coordinator can later hand a consumer a SUBSET without changing
  the surface API. Fits fibril's identity (RabbitMQ-like transparency, not Kafka).
  SERVER (5a73990): wire Subscribe gained partition (serde default 0); SubKey ->
  (topic, partition, group) so one conn holds independent per-partition subs;
  broker.subscribe gained partition param (was hardcoded 0); Ack/Nack/reconcile
  key by partition (already on wire + SubState). ~45 test/bench subscribe sites +
  Subscribe literals updated to partition 0.
  CLIENT (1711ddf): sub_manual_ack/sub_auto_ack resolve partition_set, open one
  Subscribe per partition (per-partition redirect loop in
  subscribe_partition_{manual,auto}), merge per-partition mpsc receivers into one
  Subscription via Subscription::fan_in / AutoAckedSubscription::fan_in (single
  partition = no-extra-hop fast path; forwarder task per partition otherwise).
  Acks route via each InflightMessage's own settle channel, so the merge is
  ack-transparent. partition_set is CACHE-ONLY (default [0]; N>1 only when the
  topology cache is warm) — subscribe never fetches topology itself (avoids the
  A5.5-style harness hang + a per-subscribe round-trip). Test
  subscription_fans_in_all_partitions (redirect.rs mock now answers Subscribe
  with SubscribeOk + one tagged Deliver per partition). Workspace green (33).
  FOLLOW-UPS:
  - Pure-consumer transparency gap: a client that only subscribes (never
    fetch_topology / never publishes) has a cold cache -> fans in only partition
    0. Options: warm topology at connect (carefully — some mocks don't answer
    Op::Topology, would hang), or add partition_count to SubscribeOk so the
    client expands the fan-in after the first subscribe. Decide before B6.
  - Consumer-group coverage-first rebalancer feeds partition_set a subset.
  NEXT — B6: multi-partition publish->subscribe e2e (real broker via protocol
  listener: publish keyed+keyless across p0..pN, subscribe, assert per-key
  stickiness + full coverage). Then server.rs refactor "b" (coordination
  bootstrap/spawns/admin wiring -> fibril lib.rs).
- 2026-06-13: pure-consumer gap RESOLVED via connect-time topology warm
  (f2e1b80): ClientOptions.topology_warm_timeout_ms (default 5s, None disables,
  disable_topology_warm() builder); Client::connect does a bounded best-effort
  fetch_topology so the first publish spreads and the first subscription fans in
  over all partitions (cold cache funnels everything to p0 = consumer starvation
  + publish hot-spot). redirect.rs mock now answers empty Op::Topology by default
  so connect-warm is prompt; reconcile test opts out (scripts exact frames).
- 2026-06-14: Partition NEWTYPE pass DONE (keratin 81072a8 + fibril befcdcb).
  stroma-common crate (keratin) owns Partition/Offset (Topic/Group aliases);
  fibril-common crate owns DeliveryTag; LogId dropped -> Partition newtype
  end-to-end incl. wire (serde-transparent, no protocol change). `.id()` marks
  the engine/metrics u32 boundary. See memory newtypes-for-domain-integers.
  Offset + Topic/Group newtypes deferred to ONE combined pass at the END (user
  pref: don't touch the same files thrice; fold in Arc<str> for Topic/Group).
- 2026-06-14: B6 DONE (833fa49). multi_partition_publish_subscribe_is_isolated_e2e
  in handler_tests: real broker behind a protocol listener, publish one msg to
  each of partitions 0/1, subscribe to each, assert each gets only its own
  message with the right partition tag. Full server stack
  (handler->broker->per-partition Stroma log->delivery). Whole workspace green
  (35 suites).

  ===== PHASE B (multi-partition) FUNCTIONALLY COMPLETE =====
  Declare fan-out (N catalogue entries) + per-(topic,group) QueuePartitioning CAS;
  key->partition routing (round-robin default / hash(key)%N keyed / version-fenced);
  per-partition server publish logs; partition-aware subscribe + transparent
  client fan-in (assignment-set ready for groups); connect-time topology warm;
  e2e isolation test. Remaining within reach but separable:
  NEXT CANDIDATES (recalc; Phase 1 assignor DONE d91a23a):
  1. Consumer-group rebalancer Phase 2 — membership (live members via liveness/
     lease) -> recompute on join/leave/expire -> push assignments -> client
     narrows fan-in partition_set to its subset -> GRACEFUL DRAIN on revoke/leave
     (pause + settle in-flight before release; see consumer-partition-assignment-
     model). The assignor (Phase 1) + fan-in + partition_set are built for this.
     DECISION (2026-06-14): exclusive consumer-groups are OPT-IN; plain subscribe
     stays competing-consumers (current, unchanged). Phase 2a = single-owner.
     Phase 2a brick sequence:
       [x] assignor (Phase 1, d91a23a)
       [x] ConsumerGroupState membership->delta state machine (c28dd68)
       [x] ExclusiveConsumerGroups registry (broker-side, per group key) — DONE
       [x] wire opt-in: Subscribe.consumer_group: Option<String> (serde default)
           — None = competing (unchanged); Some(id) = exclusive cohort. DONE
           (b56618f); handler doesn't consult it yet (no-op).
       [x] broker delivery GATE: QueueLoopState.exclusive_assignee (AtomicU64,
           sentinel) + delivery-loop retain + Broker::set_exclusive_assignee
           (6659718).
       [x] broker subscribe/leave integration (Model A) — ExclusiveGroupRouter on
           Broker (Mutex): pure ExclusiveConsumerGroups registry + authoritative
           `cohort -> member(client_id) -> partition -> sub_id` map. On exclusive
           subscribe/unsubscribe/disconnect it recomputes the union of subscribed
           partitions + live members (ExclusiveConsumerGroups::reconcile) and sets
           each partition's gate to the assigned member's sub_id. Ramp-up/drain
           rule: if the assignee hasn't subscribed a partition yet, keep its
           existing gate (never reopen); cohort emptied -> reopen (None). Handler:
           install_subscription calls exclusive_group_join after subscribe;
           remove_subscription + cleanup_connection_state call exclusive_group_leave.
           SubState carries the cohort id. default_consumer_target plumbed through
           settings (config consumer_groups.default_target_per_consumer -> runtime
           ConsumerGroupRuntimeSettings -> BrokerConfig).
       [x] GRACEFUL DRAIN on revoke — free via the gate: a revoked member keeps its
           in-flight settleable, gets no new; the new assignee is gated in and
           receives new. (No explicit pause op; sufficient for single-owner 2a.)
       [x] client opt-in: SubscriptionBuilder::consumer_group(id) passes it on each
           per-partition Subscribe (fan-in unchanged).
       [x] e2e: exclusive_consumer_group_splits_partitions_and_fails_over_e2e
           (handler_tests, multi-connection listener): two members fan in to a
           2-partition queue, exclusively split, survivor takes over on disconnect.
     2a LIMITATIONS (revisit): (a) the gate is keyed (topic,partition,group) with a
     single assignee, so only ONE exclusive cohort per (topic,group) is supported
     (multiple cohort ids on the same queue collide / interfere; realistic model is
     one cohort per queue). (b) reconnect-reconcile restores subs as competing (the
     cohort id isn't carried in ReconcileSubscription) until the client re-subscribes.
     (c) NOT yet: assignment delivery to the client (the client still fans in to all
     partitions and relies on the gate; an Op::AssignmentChanged push + client
     fan-in narrowing is the optimization left). (d) stickiness across rebalances.
     Phase 2b (later): cross-broker coordinator (partitions on multiple owners).
  1b. Per-consumer target override (NEAR-TERM, easy, isolated): generalize the
     assignor's global target_per_consumer into per-member weights (weighted deal,
     still coverage-first soft-signal). Adjacent to / part of Phase 2. See
     load-aware-future-direction memory.
  2. server.rs refactor "b" (coordination bootstrap/spawns/admin wiring ->
     fibril lib.rs) — organizational, was deferred to end of Phase B (now).
  3. Gated auto-create-on-first-publish (deferred follow-on).
  4. Cross-broker lag aggregation into fibrilctl topology + admin diagram.
  5. Idempotent-producer dedup decision.
  6. (END) combined Offset + Topic/Group newtype pass.
  7. (END) DOCS: an explicit explainer of how the whole thing works together
     (replication + partitioning + consumer groups), and bring the
     implemented-surface docs page up to date with what's shipped.
  PLACEMENT (locked in, memory placement-spreads-partitions-first): cluster
  partition placement already spreads a queue's partitions across distinct nodes
  before reusing a node (small-cluster balance). Locked with a test (d91a23a).
  LOAD-AWARE (memory load-aware-future-direction; advisory, off-raft, trends):
   - EARLIER-ish: load metadata COLLECTION (per-node score + per-queue activity)
     — doubles as observability; collect+expose before anything rebalances on it.
   - FAR BACK: load-aware REBALANCE (act on load to move partitions) — very
     late, play safe, trends-not-spikes, generous hysteresis.
   - DEEP BACKLOG: client load-aware publish routing (keyless-only, P2C).
     P2C = "Power of Two Choices": to route a keyless publish, sample TWO random
     candidate partitions and send to the less-loaded of the two (by ready/
     backlog hint), instead of the globally least-loaded (which herds: every
     client stampedes the same hint, worsened by staleness) or pure random
     (poor balance). Result ("power of two random choices", Mitzenmacher/Azar):
     expected max load drops from ~log n/log log n (random) to ~log log n —
     exponential improvement — for O(1) work, no coordination, and tolerance to
     stale hints (the 2nd sample breaks herds). Used by nginx/HAProxy/gRPC
     least-request. Keyed publishes ignore it (stay hash(key)%N deterministic).

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

Rough status as of checkpoint 43, measured against a functional first version
with external coordination, pull replication, local failover mechanics, and
basic operator visibility:

- Keratin replication primitives: about 70%. Caller-assigned append,
  checkpoint/reset primitives, and role guard direction exist. Remaining work is
  mostly hardening, warnings/cleanup, and any later range/repair primitives that
  snapshot-based catch-up proves it needs.
- Stroma queue replication mechanics: about 65%. Queue roles, owner
  freeze/drain, follower ingest, state checkpoint export/install, checked
  promotion, and demotion exist. Remaining work is stronger adversarial tests,
  role-transition cleanup, possible module split, and more explicit
  consistency checks around snapshot/message catch-up.
- Broker ownership and assignment model: about 45%. Static coordination,
  assignment snapshots, transition planning, owner gates, not-owner protocol
  errors, and stable placement policy exist. Remaining work is real
  coordinator-backed watch/cache, owner fencing, controller loop, and promotion
  orchestration.
- Follower pull replication worker: about 55%. Worker state, catch-up tick,
  checkpoint policy, owner-peer boundary, loop scaffold, and loop supervision
  exist. Remaining work is protocol-backed owner peer, coordination-backed peer
  resolver, retry/connection management, and end-to-end replication tests over
  TCP.
- Replication transport: about 35%. Protocol frames and handler wiring exist
  for record reads, applies, and state checkpoint export/install. Remaining work
  is the client/peer implementation, real networked worker wiring, resume/error
  behavior, and larger transfer handling.
- Failover and promotion orchestration: about 20%. Local primitives exist, but
  the real sequence across coordinator leases, catch-up, fencing, promotion,
  and client reroute is still mostly unimplemented.
- Sharding and placement: about 25%. Deterministic placement policy exists and
  preserves stable assignments. Remaining work is queue partition topology,
  client partition routing, controller-managed balancing, partition scale-out,
  and eventual shrink/drain behavior.
- Publish-confirm replication durability: about 15%. The policy model exists,
  but follower accepted/durable acknowledgements and enforcement in the publish
  confirm path are still pending.
- Operator/admin visibility: about 30%. Queue roles and follower worker state
  are visible through broker observability surfaces. Remaining work is
  user-facing replication lag/role display, transition logs, refusal reasons,
  controller status, health signals, and a live topology view.
- Testing for replication and sharding: about 40%. Good bottom-up coverage
  exists for storage, Stroma roles, broker assignment, placement, worker ticks,
  and supervision. Remaining work is helper cleanup, adversarial race tests,
  TCP end-to-end tests, multi-broker tests, fault-injection tests, and eventually
  longer chaos/soak tests.

Overall for a decent first replication/sharding milestone: about 35-40%.
The foundation is no longer speculative, but the feature is not operationally
complete until real coordination, protocol-backed follower workers, failover
orchestration, and end-to-end tests are in place.

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

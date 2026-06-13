# Replication notes — async follower, manual leader assignment

## Scope and non-goals

- Async follower replication only for v1.
- Manual leader assignment via external coordination (etcd recommended).
- No automatic failover in v1.
- No read serving from followers — all queue ops are writes, so followers are pure standbys. *(Don't try to make followers serve reads. Queue semantics make this useless and adds coordination complexity for zero gain.)*
- Consensus / Raft is out of scope. Not implementing it; not building toward it in v1. *(Building your own consensus is a multi-year endeavor that breaks subtly for years after that. Use external coordination instead.)*
- No transactional writes across partitions.

## Decisions still to make

- **Coordination layer**: etcd, Consul, custom, or just config files for v1? Recommend etcd long-term, static config to start.
- **Metadata storage**: external etcd by default for HA. Self-hosting metadata via Fibril's own replicated log creates a chicken-egg bootstrap problem (need metadata to know who's leader for the metadata partition). The workload is tiny; etcd handles it directly. Revisit only after data-path replication is stable.
- **Replica set granularity**: per-partition or per-broker? Per-partition is more flexible, more complex. Per-broker is simpler. Pick now, hard to change later.
- **Number of replicas per partition**: configurable per topic, or global default?
- **Placement and balancing policy**: eventually the coordination layer needs more than "who is owner." It should also expose desired follower assignments and enough node capacity metadata for a balancer to choose where partition owners and followers live.
- **Partition scaling policy**: increasing partition count should be a deliberate queue/topic-level operation. Shrinking is harder because old partitions may still hold messages; the likely path is stop routing new publishes to retiring partitions, drain them, then remove them once empty.
- **Behavior when ISR shrinks**: configurable via `min_in_sync_replicas` knob. Default: keep serving with degraded ISR (availability bias). Operators tighten via config when they want durability bias. Below `min_in_sync_replicas`, leader refuses writes rather than acking unsafely.
- **Snapshot transfer format**: stream the existing snapshot blob with a wire-level frame and checksum. No new format.

## Principles inherited from queue/log design

These are non-negotiable rules that constrain everything below.

- **Followers accept offsets, never assign them.** Offset assignment is leader-only. Replicate `(payload, offset)` together.
- **Offsets are internal.** External API uses stable IDs / delivery tags. Replication shouldn't change this — offsets stay a replication-internal concept. Your current design already aligns; don't regress.
- **Event log is authoritative for "what happened."** Message log holds payloads at offsets referenced by events. Readers (delivery path) must enter via the event log, never scan the message log directly. This makes orphan payloads (failed-event-after-successful-msg) safe — wasted disk, not corruption. **Verify your current delivery code respects this** before adding replication; it's a precondition.
- **Metadata plane vs. data plane stay separate.** Metadata is rare and slow-changing; data is hot path. Don't conflate. etcd for metadata; your replication for data.
- **Epoch-based fencing.** Every leadership change increments an epoch number. Every replicated batch carries the current epoch. Followers reject batches from older epochs. This is what prevents split-brain at the data layer even when coordination misbehaves.
- **Epoch must be persisted before use.** If a leader accepts a write under epoch N but crashes before persisting "I am epoch N," recovery can re-elect at epoch N with different state. Epoch goes to durable storage *before* the first write that uses it.
- **Unclean leader election off by default.** Promoting a non-ISR replica means data loss. Make it a knob, off by default, enable only consciously.
- **Idempotent producers are required for `acks=all` retries to be coherent.** Without `(producer_id, sequence)` dedup, retries cause duplicates and the durability claim is meaningless. Either add dedup or explicitly accept and document "at-least-once with possible duplicates on retry."

## Keratin changes

- Add `append_replicated_batch(epoch, first_offset, records, durability) -> Result<AppendResult, ReplicationError>`.
- Enforce gap detection inside Keratin: refuse to apply if `first_offset != next_offset()`. Return structured error.
- Enforce epoch check: refuse to apply if `epoch < current_known_epoch`. Return structured error.
- Add `truncate_and_install_snapshot(snapshot_bytes, new_head, epoch)` for too-far-behind followers.
- Add a way to read records-with-offsets so the leader can ship them to followers (probably already exists via `reader.scan_from`).
- Keratin itself stays network-unaware. No channels, no async streams as a primitive. Just methods. *(Don't put coordination logic into Keratin. If you find yourself adding network or election logic here, push it up.)*
- Tests: out-of-order apply rejected, exact-fit apply succeeds, snapshot install discards and resumes correctly, stale-epoch apply rejected.

## Stroma changes

- Add `role: Owner | Follower | Frozen` per `QueueHandle` (not per-Stroma — a node may own some partitions and follow others).
- Add `epoch: u64` per `QueueHandle`, persisted alongside the snapshot/event-log metadata.
- Add `freeze()` method: refuse all owner writes until further notice. Use this during role transitions.
- Methods that must check role and reject on Follower:
  - `append_message`, `append_message_batch`
  - `ack_*`, `nack_*`, `mark_inflight_*`
  - `declare`
  - All public write paths
- Background tasks that must be owner-only:
  - Expiry worker (followers see expiry via replicated Nack events)
  - DLQ copy spawning (the DeadLetter event tells the follower a copy is happening; follower doesn't initiate one)
  - DLQ commit appends (same)
- Background tasks that run on both leaders and followers:
  - Periodic snapshot writes (followers want fast startup too)
  - Periodic msg-log and event-log truncation (followers GC too)
- State transitions: `become_follower(replicated_from_offset, epoch)`, `become_owner(new_epoch)`. Both must be idempotent. Both must persist epoch *before* doing role-specific work.

## Replication transport (outside Keratin and Stroma)

- Leader pushes events to followers, or followers pull? **Recommend pull**: follower controls its rate, easier backpressure, leader doesn't track per-follower state in the hot path.
- Protocol: gRPC, raw TCP, whatever fits your stack. Lowest-risk choice.
- Follower keeps a watermark: "I've applied up to event offset N for partition (tp, part, group) at epoch E."
- Leader exposes: "give me events from offset N at epoch E" and "give me snapshot at offset M plus events from M+1 at epoch E."
- Heartbeats: follower sends periodic "alive, at offset N, epoch E" so leader knows replication lag and ISR membership.

## Coordination, placement, and balancing

- External coordination issues ownership leases and fencing epochs. Fibril should not implement consensus itself.
- The broker should depend on a coordination/watch trait, not directly on etcd. The trait should expose current owner, whether this node owns a partition, desired follower assignments for this node, owner endpoints for followed partitions, and assignment change watches.
- etcd is the default HA backend. A normal HA deployment should run a small
  etcd cluster, typically three nodes, and point Fibril brokers at it.
- Static config remains useful for local tests and early manual operation.
- Consul, Postgres, Kubernetes leases, or a future self-hosted Fibril
  coordinator can implement the same trait later, but they are not the primary
  target now.
- Long term, Fibril may absorb more coordination complexity to reduce operator
  burden. That is deliberately a later project. The immediate priority is a
  correct HA path using a proven coordination substrate.
- Election is lease/assignment driven. If an owner disappears, coordination chooses a new owner from eligible followers according to policy; the broker then performs local promotion checks before serving client traffic.
- Cluster decisions should be made by one active controller at a time. Brokers can all be controller-capable, but only the broker holding a lease-backed controller key should compute and write placement changes.
- The controller is metadata-plane only. It is not special for client traffic and is not on the replication data path.
- Balancing is a separate policy layer over coordination. It should consider configurable rules such as target follower count, maximum owned partitions per node, maximum followed partitions per node, disk pressure, and replication lag.
- Do not make balancing part of Keratin or Stroma. Those layers only enforce local log and queue role correctness.
- Follower assignment and ownership assignment are related but different. A node can own some partitions and follow others at the same time.
- Prefer a stable controller lease holder by default. Do not rotate controllers for normal operation; needless controller churn creates assignment churn risk and makes logs harder to read.
- Later, allow voluntary controller lease handoff when the active controller is persistently less healthy than another eligible node. Candidate health can consider controller loop latency, etcd write latency, local load, command queue depth, disk pressure, and data-plane degradation.
- Controller handoff must be conservative: minimum tenure, cooldown after handoff, no handoff during a multi-step assignment change, and clear handoff reason logs/metadata.
- Controller handoff does not imply partition ownership failover. Ownership changes still go through assignment writes, fencing epochs, local freeze/drain, catch-up checks, and promotion checks.

### Coordination keyspace sketch

Names are illustrative. The important part is separating durable desired state,
ephemeral liveness, and observed progress.

- `/fibril/cluster/config`
  - durable cluster-level defaults: target follower count, placement limits,
    failover policy, unclean election policy, partition scaling policy
- `/fibril/nodes/{node_id}`
  - lease-backed liveness record
  - endpoints, advertised zones/racks if any, capacity hints, software version
- `/fibril/controller`
  - lease-backed active-controller record
  - controller node id, lease id, controller epoch, acquired timestamp
- `/fibril/queues/{queue_id}`
  - durable queue topology: partition count, desired replica policy, optional
    per-queue overrides
- `/fibril/assignments/{queue_id}/{partition}`
  - durable desired partition assignment
  - owner node id, follower node ids, assignment epoch, placement generation
- `/fibril/follower-status/{queue_id}/{partition}/{node_id}`
  - lease-backed or periodically updated follower status
  - applied message offset, applied event offset, follower epoch, lag,
    health/state, last update timestamp
- `/fibril/owner-status/{queue_id}/{partition}/{node_id}`
  - owner-reported status when useful
  - local tails, retained heads, serving state, drain/freeze state
- `/fibril/operations/{operation_id}`
  - optional durable operation record for multi-step moves
  - used for observability and recovery of in-progress assignment changes

Static config can model a subset of this for early tests: nodes, queue partition
count, owner assignment, follower assignment. It should still look like the same
snapshot shape the real coordinator exposes.

### Controller lease

- Every broker can be controller-capable.
- Brokers compete for `/fibril/controller` using a lease-backed compare-and-set.
- Only the active controller computes desired placement and writes assignment
  changes.
- Standby brokers watch metadata but do not write assignment decisions.
- Controller loss is handled by lease expiry. A standby acquires a new
  controller lease and reconstructs state from durable assignments plus live
  node/follower status.
- Controller identity is metadata-plane only. Clients do not route through it,
  and data replication does not depend on talking to it.

Controller lease records should include enough data for diagnostics:

- node id
- controller epoch/generation
- lease id or metadata backend revision
- acquired timestamp
- last successful control-loop timestamp
- optional handoff reason if voluntarily released

### Controller loop

The loop should be boring and testable:

1. Acquire or renew the controller lease.
2. Read/watch a metadata snapshot: live nodes, queue topology, assignments,
   follower status, owner status, and cluster policy.
3. Run pure planning functions:
   - detect invalid assignments
   - detect dead owners/followers
   - pick failover candidates
   - choose follower replacements
   - choose balancing moves
   - choose partition scaling moves
4. Convert the plan into small safe operations.
5. Write each operation through compare-and-set transactions.
6. Record operation outcome and metrics.
7. Wait for watch events or a bounded control-loop interval.

The smart parts should be pure functions over a snapshot of metadata. etcd
integration should mostly be read/watch/CAS plumbing.

### Assignment transitions

Ownership assignment is permission, not proof that the local queue is safe to
serve. A broker receiving an assignment still has to pass local checks.

Owner to follower:

1. Controller writes an operation intent or new assignment generation.
2. Current owner stops accepting new client traffic for the partition.
3. Owner freezes and drains accepted Stroma work.
4. Owner records local tails and switches local queue/logs to follower or
   non-owner state.
5. Assignment is updated only through CAS against the expected generation.

Follower to owner:

1. Controller chooses an eligible follower from follower status.
2. Controller writes a new owner assignment with an incremented fencing epoch.
3. Candidate broker catches up if needed.
4. Candidate runs Stroma promotion checks against expected message/event tails.
5. Candidate begins serving only after local promotion succeeds under the
   current assignment/epoch.

If local promotion fails, the broker must not serve traffic. It should publish a
refusal reason or status so the controller can pick another candidate or require
operator action.

### Failover policy

Failover is external metadata policy plus local safety checks.

- Clean failover: old owner is known stopped/frozen, candidate follower is
  caught up, promotion succeeds.
- Degraded failover: owner is gone, candidate is the best known follower but may
  need catch-up from retained logs or snapshot.
- Unclean failover: candidate is not known caught up and data loss is possible.
  This should be disabled by default and require explicit operator policy.

Candidate selection should consider:

- follower applied offsets and lag
- follower status freshness
- matching epoch/generation
- node liveness
- node health score
- zone/rack diversity if configured
- whether the candidate already owns too many partitions

The controller may decide intent, but Stroma promotion remains the final local
gate before serving.

### Balancing policy

Balancing should minimize movement. Moving a partition owner is expensive and
risky compared with assigning a new follower.

Inputs:

- target followers per partition
- maximum owned partitions per node
- maximum followed partitions per node
- node capacity hints
- disk pressure
- replication lag
- command queue depth or broker load
- software version compatibility
- zone/rack labels if configured

Outputs:

- add follower
- remove follower
- move follower
- move owner only when materially useful or required
- refuse to move if safety preconditions are not met

Balancing should be conservative:

- prefer filling missing followers before moving owners
- avoid moving hot partitions unless necessary
- use cooldowns after moves
- avoid cascading moves from one control-loop tick
- preserve enough followers before retiring an old follower

### Partition scaling

Growing partitions:

1. Update queue topology with a higher partition count.
2. Assign owners and followers for new partitions.
3. Clients refresh topology and start routing new publishes according to the
   partitioning policy.

Shrinking partitions:

1. Mark partitions as retiring.
2. Stop routing new publishes to retiring partitions.
3. Continue delivering/draining existing work.
4. Remove assignments only after the retiring partition is empty and safe to
   delete or archive.

Shrinking should be considered a later feature. It is much harder than growth
because queue partitions can contain live delayed, ready, inflight, and DLQ
state.

#### R6 partitioning plan (2026-06-13): fixed-at-create first, live repartitioning as a TARGET

First cut is fixed-at-create `partition_count`, but live repartitioning
(growing, later shrinking) is an explicit TARGET — so the first cut must be
forward-compatible, not a dead end. Bake these in NOW even though N won't change
live yet:

- **Partitioning is a VERSIONED property of the logical queue `(topic, group)`,
  never an immutable constant.** REVISED 2026-06-13: `group` is part of the
  queue identity (a name prefix) and each `(topic, group)` is an independent
  queue with its own message data, so partitioning is per-`(topic, group)`, NOT
  per-bare-topic. (The earlier "groups share the topic's partitioning" note is
  superseded — it predated confirming groups have separate logs.) Store
  `{ partition_count, partitioning_version }` as a CAS attribute keyed
  `fibril/partitioning/<topic>[/<group>]`. The first cut never bumps the
  version; the data model already supports it changing. (Implemented in B1:
  `declare_queue_partitioning(topic, group, count)` / `queue_partitioning`.)
- **Routing carries the partitioning version.** Key->partition routing is
  `route(key, partitioning_version) -> partition`. Clients learn the current
  `partitioning_version` with topology and stamp it on publishes. The owner can
  then FENCE traffic routed under a stale version (not-owner/redirect with the
  new topology) — this is the hook that makes a future live repartition safe.
  Mirror of the data-plane epoch-before-use principle, at the routing layer.
- **Routing function is swappable behind the version.** First cut can be
  `hash(key) % N`; a later version may switch to consistent-hashing / virtual
  nodes to minimize key remapping on resize. Because routing is
  version-parameterized, swapping it is not a wire break.
- **Assignment model already supports it.** Assignments are per
  `(topic, partition, group)`; adding/removing partitions is adding/removing
  placement units. Catalogue/resources represent partitions individually so the
  count can grow without a schema change.
- **Per-key ordering across a resize is the hard semantic, deferred with the
  feature** — but the version-fencing hook above is what will let the live
  migration (drain/freeze or dual-route affected keys under the version bump)
  be implemented without a protocol break. Do NOT design anything in the first
  cut that assumes N is constant for all time (e.g. no caching partition->owner
  without the version, no client that ignores `partitioning_version`).

Compatibility rule of thumb for the first cut: any code that maps key->partition
or caches partition topology MUST be parameterized by `partitioning_version`, so
that live repartitioning is later a matter of bumping the version + a migration
job, not a wire/format change.

### Health score and controller handoff

Health score is an input to placement and optional controller handoff. It should
not be a magic global truth.

Possible node health inputs:

- control-loop latency
- etcd read/write latency from that node
- broker command queue depth
- CPU/load
- memory pressure
- disk pressure
- replication apply lag
- owner/follower error rates
- data-plane degraded flag
- software version compatibility

Controller handoff rules:

- stable controller is the default
- no routine rotation
- voluntary handoff only after sustained degradation
- require a clearly healthier eligible candidate
- minimum tenure before handoff
- cooldown after handoff
- never hand off in the middle of a multi-step assignment operation
- write handoff reason for observability

Controller handoff changes only who computes metadata-plane assignments. It
does not by itself move any queue ownership.

### Broker watch behavior

Brokers watch assignment changes and apply local role transitions:

- assigned owner: catch up if needed, promote locally, serve only after local
  checks pass
- assigned follower: become follower, start pull replication from current owner
- assignment removed: stop follower worker or stop serving after safe demotion
- owner changed elsewhere: reject new client traffic and surface not-owner

Brokers should keep local status records current enough for the controller to
make decisions, but stale status should be treated conservatively.

### Client topology behavior

Clients eventually need a metadata/topology view separate from normal queue
operations:

- bootstrap from one or more broker addresses
- fetch topology for a queue
- route publish/subscribe operations to the owner of the relevant partition
- refresh topology on not-owner/stale-topology errors
- maintain multiple broker connections when one logical queue spans owners
- hide partition selection from normal user APIs

The client-facing contract stays topic/group oriented. Partition ownership is an
implementation and routing detail.

## Client side (out of scope for v1, but plan the wire protocol)

- Bootstrap list: clients get N broker addresses, try in order until one answers, refresh full topology from there. Survives any single broker failure.
- Client must handle "not leader" errors by refreshing metadata and retrying. Don't fail.
- For sharded queues, clients eventually need a topology view: which partitions exist, which broker owns each partition, and which broker endpoint to use.
- A client may need connections to multiple owners for one logical queue if that queue spans partitions owned by different brokers.
- Normal users still should not choose partitions directly. Client routing should pick a partition according to Fibril policy, while keeping offsets and partition internals out of the user API.
- Subscriptions may need one stream per owner/partition behind one logical user-facing subscription. That should be hidden by the client library where possible.
- Idempotent producer dedup table lives on the leader, keyed by `(producer_id, sequence)`. Must be replicated (or rebuildable from event log) so failover doesn't break dedup.

## Truncation rules

DECISION (2026-06-13): rely on checkpoint-install fallback for correctness;
do NOT build a strict follower-aware truncation floor. The earlier "Critical
floor" stance below is superseded — kept for context.

Rationale:
- Truncation never violates the durability contract. Confirmed data is already
  on the required followers BEFORE the confirm returned (the confirm gate
  waited); only unconfirmed/local-only data can be truncated, which the
  producer was never promised. Local safety (`safe_message_truncate_before` =
  lowest-not-acked etc.) already protects undelivered messages.
- A follower that falls behind the truncated log head recovers via the
  existing `CheckpointRequired` -> install snapshot -> resume post-snapshot
  event transfer path. So a strict floor would be a pure OPTIMIZATION (avoid
  occasional checkpoint transfers), not a correctness requirement — and it has
  a real cost (a slow/dead follower pins the log and leaks disk).
- Snapshot-loop risk is structurally bounded: the owner truncates events only
  up to its own `applied_upto` and serves the checkpoint AT that same
  `applied_upto`, so an installing follower lands exactly there and reads
  forward into events that, by construction, still exist. A loop needs a
  follower persistently slower than the snapshot cadence — pathological.
- Coverage is already piece-by-piece: stroma test (truncation -> owner read
  returns `CheckpointRequired`), follower worker test (reacts -> installs),
  and `replication_checkpoint_export_install_composes_with_catch_up`
  (install -> resume incremental -> CaughtUp -> promote).

Still holds:
- **Don't truncate acked/committed data**: local truncation floor already
  respects the commit/settle watermark, so confirmed data is never deleted.

Deferred OPTIONAL optimization (only if checkpoint churn is ever measured as a
problem): a soft retention floor = min over IN-SYNC followers' durable offsets
(stale followers drop out and don't pin, à la Kafka ISR), fed fresh from the
owner's progress registry. Any thresholds must be runtime-configurable per the
settings discipline.

### Superseded earlier stance (context only)
- ~~**Critical**: leader's truncation floor must include the minimum applied offset across all known followers.~~ (Superseded: checkpoint fallback handles this; floor is optional optimization.)
- Followers report their applied offset to the leader — this DOES exist now (stamped replication reads feed the owner's progress registry), and powers the ISR floor / observability, just not a hard truncation floor.
- Dead-follower handling: with no hard floor, a dead follower simply resyncs via checkpoint on return; it does not pin retention.

## Snapshot handling

- **Leader does the snapshot work** for shipping to followers.
- Followers can run their own periodic snapshots locally for their own fast startup. These are independent of any received snapshots.
- Snapshot transfer: stream the existing snapshot blob format. Add a wire-level checksum (the snapshot already has crc32c, but the wire transfer needs its own framing checksum).
- During snapshot transfer, leader keeps generating events. After follower installs, it re-requests from `snapshot_offset + 1`; usually the gap is small.
- If snapshot transfer fails partway, follower stays at its old offset and tries again. No partial-install state.
- **Snapshot-aware retention** (revised 2026-06-13): the leader does NOT block deletion on follower positions. It truncates per local safety; a follower that falls behind the new log head resyncs via checkpoint install + post-snapshot event transfer. See the Truncation rules DECISION above.

## Msg log and event log coordination

- **Both must be replicated.** Followers need msg payloads to serve them post-failover.
- Ordering: ship msg log records first, then event log records that reference them. Or interleave with care.
- Acceptable to briefly have event entries referencing msg offsets the follower hasn't received yet *during catch-up*, but not in steady state. Make this an invariant check on the follower side.
- Alternative: unified replication stream that carries both, with explicit ordering. Cleaner, more work.
- **On startup, verify event log → message log references resolve.** Fail loud if not. Don't try to self-heal mismatched logs.

## Caveats / things to watch out for

- **Split brain**: two nodes both think they're leader for the same partition. Defense: external coordination must fence the old leader (via lease expiry, kill signal, etc.) before promoting the new one. Stroma's `freeze()` is the local fence point. Epoch checks in Keratin are the last-line defense — even if coordination fails, an old leader's writes with a stale epoch get rejected by followers.
- **Followers running stale logic**: if leader has a newer schema/version of an event, follower must handle it or refuse to apply. Add a version check; refuse with loud error rather than silent corruption.
- **Followers that fall behind faster than they can catch up**: leader will hit retention or runs out of disk. Need a "give up on this follower" signal to higher coordination, plus metrics so this is visible before it's a crisis.
- **Network partitions**: follower can't reach leader. Follower should not auto-elect itself or do anything risky — just keep retrying. Election is external.
- **Time-based logic on followers**: expiry worker is leader-only specifically because followers can't make time-based decisions independently (they'd diverge from the leader). All time-based effects must flow through replicated events.
- **Clock skew between nodes**: don't trust follower clocks for anything authoritative. All deadlines come from the leader's clock, embedded in events.
- **Replication of in-flight delayed messages**: delayed heaps live in QueueInternalState and are reconstructed from event replay. As long as the EnqueueDelayed events are replicated, this works. But this depends on the snapshot fix (heaps in snapshots) — otherwise a follower restarting after applying a snapshot loses the delayed entries until replay re-adds them. Don't ship replication until that fix is in.
- **DLQ copies during follower role**: if a node is follower for partition A (source) and leader for partition B (DLQ target), only the leader of A spawns the copy. The follower of A applies the resulting DeadLetter event passively. Make sure the copy code is only on the leader path.
- **Restart recovery vs. replication catch-up**: when a node starts, it doesn't know if it's leader or follower for each partition until coordination tells it. Default to Follower (freeze writes), wait for coordination to say "you're leader for partition X at epoch N." Avoids accidentally serving writes during the moment before coordination kicks in.
- **Background tasks during role transition**: when a follower becomes leader, expiry worker must start *fresh* — don't run on stale state from before the transition. When a leader becomes follower, expiry worker must stop before applying any replicated events.
- **Replication lag as backpressure**: if followers are far behind, leader may want to slow its accept rate. Out of scope for v1 but think about where this hook goes (in `append_*` methods on Stroma).
- **Orphan payloads**: if event-log append fails after msg-log append succeeds, the payload is orphaned. This is *not* corruption as long as readers only enter via the event log. Verify this holds in your delivery code. Don't try to actively GC orphans; let retention sweep them incidentally.
- **Test scenarios that always reveal bugs**:
  - Leader dies mid-batch (followers have partial batch applied)
  - Follower restarts during snapshot transfer
  - Follower behind by exactly one snapshot boundary
  - New follower joining an active partition
  - Two followers, one fast one slow, leader truncation behavior
  - Network slow-loris (follower acks slowly but never disconnects)
  - Leader epoch change while a slow follower is mid-fetch
  - Coordination layer (etcd) becomes unreachable mid-operation

## Phasing

1. **Snapshot the delayed heaps** and the other Tier-1/2 bug fixes from the test plan. Replication on top of buggy state machine = replicated bugs. Get the local state machine bulletproof first.
2. Keratin replication primitives (`append_replicated_batch`, `truncate_and_install_snapshot`, epoch tracking) + tests. No network, no Stroma changes.
3. Stroma role flag + epoch + freeze + role-aware background tasks. No actual replication wired up; just verify writes are rejected on follower and role transitions are clean.
4. In-process replication test: spawn two Stroma instances, ship events between them via a fake transport (channel), verify follower state matches leader's.
5. Real network transport. Pull-based with heartbeats.
6. Snapshot transfer protocol.
7. Coordination integration (etcd or whatever you pick). Lease-based leader assignment with epoch issuance.
8. Idempotent producer dedup (if not already done; required before claiming `acks=all` is meaningful).
9. The durability/availability knobs: `min_in_sync_replicas`, unclean-leader-election toggle.
10. Operational stuff: metrics for replication lag, alerts for stuck followers, runbook for manual failover.

## What not to do in v1

- **Don't auto-failover.** Manual only. Build the failover *mechanism* but require a human to trigger it.
- **Don't replicate metadata via the same path as data.** External coordination. *(The chicken-egg trap and the operational complexity aren't worth it for a tiny workload that etcd handles trivially.)*
- **Don't try to support followers serving reads.** Queue semantics make this useless. *(For Kafka it makes sense — consumers reading old offsets — but your delivery model is broker-tracked. No analog.)*
- **Don't optimize replication throughput.** Correctness first, performance once it works. *(Premature optimization here means subtle ordering bugs that take weeks to find.)*
- **Don't try to make Keratin network-aware.** Keep the API local-and-deterministic. *(The moment Keratin starts knowing about peers, you've created a dependency cycle between your storage layer and your coordination layer that's painful to undo.)*
- **Don't build leader election.** Use lease-based assignment from coordination layer. *(Implementing leader election correctly is a research project. etcd already did it.)*
- **Don't expose offsets in client-facing APIs.** Use stable IDs / delivery tags. *(Once clients depend on offsets, they depend on replication-internal state, and every replication change is a breaking client change.)*
- **Don't try to actively GC orphan payloads.** Let retention sweep them. *(Active scan + compact is complexity for a problem that's not corruption; only justifiable if orphan rate is empirically high.)*
- **Don't enable unclean leader election by default.** *(Trades silent data loss for availability; the kind of knob operators turn on once during an incident and forget about.)*

## Things to question as you go

- *Am I about to put coordination logic into Keratin?* If yes, push it up to Stroma or higher.
- *Am I about to make followers do something the leader hasn't told them to?* If yes, that decision should flow through a replicated event instead.
- *Am I about to expose an offset in a client-facing API?* If yes, use a stable ID instead.
- *Am I solving a v1 problem or a v2 problem?* Be honest. Cross-region, follower reads, self-hosted metadata, transactional writes — all v2+.
- *Am I about to coordinate two replicas on every write?* The leader should decide alone; followers replicate the decision.
- *Am I about to truncate something that might still be referenced?* Check follower positions, snapshot lifecycle, and commit watermark before any delete.
- *Does this code path assume single-writer?* If yes, will it still hold when a follower is applying replicated events concurrently with snapshot installation?

## Success criteria

- [ ] Single-node still works (no regression from current state).
- [ ] Replication: messages survive single-replica failure with strict durability mode.
- [ ] Leader failover (manual): cluster recovers; no acked data lost.
- [ ] Idempotent producer (if in scope): retries don't duplicate.
- [ ] ISR shrinkage: writes proceed with degraded ISR, refuse below `min_in_sync_replicas`.
- [ ] Orphan handling: failed event appends leave orphans; retention cleans them; no consumer-visible effects.
- [ ] Recovery: broker restart replays event log, verifies message log references, fails loud on mismatch.
- [ ] Metadata: client survives bootstrap broker failure (tries next in list). *(Client work is out of scope for v1; success criterion is the broker-side protocol supports it.)*
- [ ] Replication lag is observable via metrics before it's a crisis.
- [ ] Manual failover runbook exists and has been exercised in a test environment.

## Ganglion coordination provider — integration plan (added 2026-06-12)

Status: `fibril-coordination-ganglion` spike exists and passes (raft-backed `Coordination`
trait, lossless snapshot mapping, sync reads via bridged watch, async leader-only proposals).
etcd/static remain the v1 default; this plan makes the embedded provider a real option without
changing that default. Ganglion-side prerequisites live in `ganglion/DESIGN.md` (phases G1–G3:
guarded CAS proposals + epoch stamping, telemetry + `RaftTopology`, cluster playground).

### F1 — Provider contract test suite

One reusable assertion suite run against every `Coordination` implementation (static, ganglion,
later etcd), so providers can't drift:

- `crates/broker/src/coordination.rs` gains `pub mod contract_tests` (cfg(any(test, feature =
  "provider-contract-tests"))) with `fn assert_coordination_contract(make: impl Fn() ->
  (Provider, UpdateHandle))` covering: initial snapshot visibility; watch sees updates in order
  with no loss of the latest value; `owns_queue`/`follows_queue`/`owner_for`/`assignment_for`
  consistency with the snapshot; node identity stability; behavior when a queue is absent.
- `UpdateHandle` abstracts "how a new snapshot gets committed" (static: `update_snapshot`;
  ganglion: `propose` + wait-for-watch).
- Gate: suite passes for `StaticCoordination` and `GanglionCoordination`.

### F2 — Controller loop on the embedded provider

With ganglion, the controller lease collapses into raft leadership: the raft leader IS the
active controller (no separate lease key, no second consensus). Keep the loop pure-function
shaped exactly as the "Controller loop" section above:

1. `is_leader()` gate (standbys watch only).
2. Read committed snapshot (sync, from watch).
3. Pure planning (existing `plan_local_assignment_transitions` + placement policy).
4. Epoch stamping via ganglion's pure `stamp_assignment_epochs` (owner change bumps, follower
   churn does not — matches the fencing principles section).
5. Propose via guarded CAS (`write_snapshot_guarded` / `plan_and_propose_guarded`); on
   generation mismatch, re-read and re-plan (another controller won — with raft this only
   happens across a leadership change, which is exactly when re-reading matters).
6. Brokers (including the controller) consume committed snapshots only via the watch.

Tests: controller loop drives owner failover on a 3-node in-process cluster (kill owner's
broker record, loop reassigns with epoch+1, watch delivers); two loops racing across a forced
leader change never produce interleaved/lost assignments (mirrors ganglion's G1 race test at
the fibril layer).

### F3 — Broker wiring behind config

- Config enum: `coordination = "static" | "ganglion" | (later) "etcd"`; ganglion variant takes
  `data_dir`, `raft_node_id: u64`, `peers: map<raft_id, addr>`, `bootstrap: bool` (initialize
  exactly once on first boot of the bootstrap node; all restarts never re-initialize).
- Broker constructor accepts `Arc<dyn Coordination>`; provider construction stays outside the
  broker core (composition root), so the broker itself gains no new deps.
- Node identity: fibril string node ids remain canonical in snapshots; raft u64 ids are
  transport-only and live in provider config.
- Out of scope here: wire transport for raft (in-process router covers single-process clusters
  and tests; multi-process needs ganglion's long-term gRPC transport item).

### F4 — Topology visibility (CLI now, admin diagram after)

- Admin endpoint `GET /topology` returns one JSON document: fibril coordination snapshot
  (nodes, assignments with owner/followers/epoch, generation) + optional `raft` block
  (ganglion `RaftTopology`: leader, voters, learners, last_applied) when the embedded provider
  is active.
- CLI: `fibril-cli topology` renders it — nodes table (id, addr, raft role if present), queue
  table (topic/partition/group → owner, followers, epoch), cluster line (generation, leader).
  Plain text first; `--json` passthrough for scripts.
- Admin page: same JSON feeds a diagram (nodes as boxes, owner edges solid / follower edges
  dashed, epoch + generation badges). Diagram work is last; the JSON contract above is the
  deliverable that unblocks it.

### F5 — Cluster playground

- `scripts/coordination-playground.sh`: builds a 3-broker single-process playground binary
  (example in `fibril-coordination-ganglion`) — brokers register NodeInfo, the controller loop
  assigns a demo queue, stdin commands (`status`, `kill <node>`, `restart <node>`, `move
  <queue> <owner>`) show reassignment + epoch bumps live via the watch.
- Non-interactive `--script` mode so a Jepsen-style scenario can smoke it in CI.

### Confidence test suite (cross-repo summary)

| Layer | Where | Status |
| --- | --- | --- |
| raft storage contracts (openraft suite) | ganglion | done |
| state-machine + WAL model fuzz, epoch rules | ganglion | done / G1 |
| cluster failover/partition/restart/membership | ganglion | done |
| bounded recovery + atomic persistence | ganglion | done |
| provider contract suite (static + ganglion) | fibril F1 | planned |
| controller loop + epoch failover choreography | fibril F2 | planned |
| replication data path (Keratin/Stroma epochs, pull transport) | fibril (existing phasing §) | in progress |
| "scenarios that always reveal bugs" matrix | fibril (existing caveats §) | tracked per scenario |
| playgrounds + scripted smoke | both (G3/F5) | planned |

The replication data path keeps its own phasing (earlier sections of this doc); this plan only
covers the coordination/metadata plane and its visibility. The two meet at F2 (controller writes
assignments; Stroma promotion checks consume epochs locally).

## Replication data-plane integration plan — R-phases (added 2026-06-12)

How the coordination plane (ganglion-backed; F-phases complete) connects to the existing
replication data plane (Stroma roles, follower workers, protocol owner peers — Medium-Term
§§1–3 largely done). This supersedes the etcd-shaped Medium-Term §4/§5 *for the embedded
coordinator path*: ganglion already provides the snapshot/watch cache (provider + contract
suite), CAS assignment writes (guarded generation writes), node registration with TTL
(heartbeat labels + `live_nodes(ttl)`), and the controller lease (raft leadership IS the
lease). etcd remains a possible alternative behind the same `Coordination` trait, unchanged.

Inventory of existing seams this plan composes (do not rebuild):
- `plan_local_assignment_transitions` — snapshot diff → local role intents.
- `spawn_assignment_watcher_with_follower_replication(coordination, resolver, cfg)` — applies
  transitions and supervises follower loops; `CoordinationProtocolOwnerPeerResolver` resolves
  owners from snapshots with peer caching.
- Owner demotion/freeze with release-inflight; StopFollower with tick-draining; checked
  promotion; checkpoint export/install over protocol v1.
- `GanglionCoordination::{control_iteration, live_nodes, register_self, propose}`.

### R1 — Cluster queue catalogue (the controller's missing input)

Problem: the controller needs the desired queue set, but queues are declared dynamically on
whichever broker a client hits. Declarations must become cluster-visible facts.

Design: the catalogue lives in the coordination snapshot, separate from assignments.

- Ganglion (schema + commands, mirror of node registration):
  - `CoordinationSnapshot.resources: BTreeSet<ResourceIdentity>` (new field, serde-default so
    existing WAL/snapshot files load — additive, no migration).
  - `MetadataRaftCommand::{RegisterResource { resource }, DeregisterResource { resource }}` —
    merge commands (no CAS), generation bump, same forwarded-write path brokers already use
    for heartbeats. `stamp_assignment_epochs`/CAS writes ignore `resources` except that
    snapshot-replace commands carry the field verbatim (controller writes preserve it).
- Fibril provider: `register_queue(&QueueIdentity)` / `deregister_queue` (forwarded like
  `register_self`); mapping uses the existing `fibril/queue` namespace.
- Broker wiring: on successful local `declare_queue` in ganglion mode, fire-and-retry
  `register_queue` (same never-crash policy as heartbeats). Followers receiving replicated
  DLQ/declare side effects do NOT register (owner-side only, mirrors "owner decides").
- Tests: ganglion — merge/dedup/dereg fuzz alongside the SM model (resources join the model);
  fibril — declare on a follower broker → resource visible in every node's snapshot.

DECISION NEEDED (small): `resources` as a separate set (recommended: catalogue ≠ placement,
deletion semantics stay clean) vs. encoding unassigned queues as empty assignments.

### R2 — Server controller task (leader-gated assignment loop)

- New task in fibril-server (ganglion mode only), spawned next to the heartbeat:
  - Wakes on coordination watch changes AND a bounded interval (config:
    `controller_tick_ms`, default 2000).
  - Each tick: `live = provider.live_nodes(ttl)` (config: `liveness_ttl_ms`, default
    3 × heartbeat interval — must exceed worst-case election + retry, FAILURE_MODES §4b.6);
    `queues` = snapshot catalogue (R1); run
    `control_iteration(DeterministicPartitionPlacement, queues, target_followers, live, retries)`.
  - `Ok(None)` (not leader) and planner `NoNodesForQueues` are normal idle outcomes, not errors.
  - Anti-churn guard: skip the proposal entirely when the planned snapshot equals the committed
    one modulo generation (placement policy is already stability-first; this makes no-op ticks
    free and keeps the raft log quiet).
- Config: `[coordination.controller] target_followers` (default 1), `controller_tick_ms`,
  `liveness_ttl_ms`.
- Observability: controller block in the topology JSON (`controller: { active: bool,
  last_plan_generation, last_error }`) — feeds the admin page and `fibrilctl topology`.
- Tests: 3 in-process providers — declare → assignment appears with epoch 1; standby never
  proposes; no-op ticks do not advance generation; owner drop from live set → epoch+1 move
  (already proven at provider level; re-assert through the server-shaped task fn).

### R3 — Brokers consume assignments (the ownership switch)

- In ganglion mode, fibril-server:
  - replaces `OwnAllQueues` with the coordination provider as the broker's `QueueOwnership`
    (gate exists; `StaticCoordination` already implements the trait — add the same impl for
    `GanglionCoordination`).
  - starts `spawn_assignment_watcher_with_follower_replication(provider,
    CoordinationProtocolOwnerPeerResolver::new(provider), worker_cfg)`.
- Standalone (`mode = "static"`) behavior is untouched.
- This is the moment cluster brokers refuse unowned traffic (`ERR_NOT_OWNER` already exists);
  client reroute remains R6.
- Tests (the confidence core; helpers per worklog item 48):
  - two-broker TCP cluster: declare on A → controller assigns A owner/B follower → publish N
    to A → B's follower worker catches up (protocol pull already proven; this proves the
    *coordination-driven* wiring end to end);
  - unowned broker rejects publish/subscribe with not-owner;
  - assignment refresh does not reset follower progress (re-assert through watcher path).
- Tryout: `cluster-tryout.sh --ganglion` gains a declare + publish step; `fibrilctl topology`
  and the admin diagram show real assignments with owner/follower edges.

### R4 — Failover orchestration (composes R2 + R3 + epochs)

Sequence on owner death (no new mechanisms — this phase is choreography + tests):
heartbeat TTL expires → controller tick reassigns (epoch+1) → watcher on the chosen broker:
stop follower worker (drain tick), checked promotion
against expected tails; on `CheckpointRequired`/not-caught-up, refuse promotion (worklog §6
risk: explicit refusal over optimism) and surface status; controller may pick another or wait.
Old owner returning: sees demotion intent (owner→follower transition path with
release-inflight); its stale writes die at the data plane via epoch checks.

- DECIDED (2026-06-12): promote-to-local-tail under the epoch fence. The chosen follower is
  promoted to its own applied tails; the epoch fence guarantees the dead owner's unreplicated
  suffix can never be partially visible later (its writes are rejected on return). Acked-data
  loss is bounded to exactly what the durability contract permitted: under `local_durable`
  the producer accepted single-copy durability; under `replica_*`/`majority_durable` (R5)
  confirmed writes are on the promoted follower by definition.
  CANDIDATE SELECTION REQUIREMENT (R4): with multiple followers the controller must prefer
  the most caught-up live follower — the dead owner cannot help a behind follower catch up,
  so promote-to-local-tail on the wrong candidate loses more than necessary. With
  `target_followers = 1` there is only one candidate and this is moot. Interim mechanism
  (lands with R4, before full R5): followers piggyback their per-assignment applied tails
  into the heartbeat labels they already send (e.g.
  `applied/<topic>/<part>[/group] = <msg_off>:<event_off>`); the failover path in the
  controller picks the live follower with the highest applied event offset for the moved
  partition. Labels are advisory/freshness-bounded — the promotion gate (checked promotion on
  the broker) remains the authority; a stale label at worst picks a slightly worse candidate,
  never an unsafe one.
  EXPLORATION FOR LATER (explicitly not a foundation crossroad — switching strategies only
  changes the promotion gate; storage formats, wire frames, coordination schema, and epoch
  fencing are unaffected): quorum-tails promotion — synchronously query the applied tails of
  all live replicas at failover time and promote the most advanced (or refuse below quorum).
  Strictly reduces loss for `local_durable` workloads versus label-freshness-bounded
  selection, at the cost of a read round to replicas during failover and a liveness
  dependency on them. Revisit when R5 progress reporting exists, since it provides exact
  replica-tail data on the owner path for free.
- Adversarial tests (REPLICATION_PLANNING "scenarios that always reveal bugs"): stale owner
  publish after fence; promote-before-caught-up refused; owner returns mid-failover;
  generation races (CAS already covers); partition during failover.

### R2b — Replicated runtime settings (user requirement, 2026-06-12)

Stroma runtime settings must be cluster-consistent. Mechanism: ganglion's generic
`attributes` KV (opaque values, merge `SetAttribute`, same-value no-op) carries the serialized
`RuntimeSettings` + version under one key (`fibril/runtime_settings`). Flow: admin PUT on any
broker → existing `RuntimeSettingsManager` versioned-update check locally → forward
`SetAttribute` through the provider (leader or forwarded write) → every broker's watch sees
the attribute change → applies it through the same versioned-update path (idempotent; version
conflict resolution already exists). Local persistence in the Stroma engine remains the
node-local cache/bootstrapping source; the attribute is the cluster truth in ganglion mode.
Tests: settings update on follower broker propagates to all; version conflicts surface as the
existing Conflict outcome; restart picks up cluster value over stale local.

### R5 — Replicated publish-confirm enforcement

Design notes only until R3/R4 are solid (Medium-Term §7 stands): follower workers already
track applied offsets; add owner-side progress reporting over the existing protocol peer
(piggyback on pull responses — the owner learns follower offsets from the requests
themselves: a pull at offset N proves application < N; an explicit ack frame confirms durable
N). Enforce `ReplicationDurabilityPolicy` in publish-confirm with per-policy timeouts.

### R6 — Client topology (unchanged from Medium-Term §8; after R4)

### Decision: one metadata raft group, not per-partition groups (2026-06-12)

Considered and rejected: a raft group per partition deciding its own ownership. Reasons:
assignment traffic scales with changes (declares/failovers/moves), not partitions×writes, so
one group is far from any ceiling; per-partition groups multiply election timers, heartbeats,
and WAL/membership management (only fixable with TiKV-style multi-raft batching) and reintroduce
the chicken-egg problem (something must still place each group's members); and the availability
benefit is moot here because data-plane serving already continues without coordination — owners
keep serving on their epoch, only ownership *changes* pause when the metadata quorum is lost.
Escape hatch if metadata scale ever demands it: shard the metadata group by partition ranges
behind the same `Coordination` trait — nothing decided now blocks that.

### Design note: ownership granularity and multi-region placement (2026-06-12)

Question raised: should ownership be finer than "a node owns a partition"? Clarification of
the current model: the assignment unit already IS the individual queue partition
`(topic, partition, group)` — there is no coupling of same-index partitions across queues, so
per-queue regional placement is mechanically possible today. Finer than one queue partition
(split ownership of a single partition) is rejected permanently: single-writer-per-partition
underpins offset assignment, ordering, and epoch fencing; scale within a hot queue by adding
partitions.

What multi-region optimization actually needs — placement INPUTS, all additive:
1. Region/zone labels flowing to the planner: ganglion `NodeInfo.labels` exists; fibril's
   mapping currently drops labels and `PlacementInput` lacks them. Surface both.
2. Per-queue placement hints (preferred region, zone-spread minimums): ride the `attributes`
   KV (`placement/<queue>`) or a future parallel hints map next to the catalogue — both
   serde-additive, no migration.
3. Activity signals (per-queue publish rates via heartbeat labels or metrics rollup) so the
   planner can follow real traffic.
Then a `region-aware` policy joins the `PlacementStrategy` registry beside
`deterministic`/`least-loaded`. Extends the existing zone/rack-diversity balancing inputs;
not scheduled before R3–R5 land.

### Noted for post-R6 (user, 2026-06-12): operator and scaling QoL

- Admin UI node management: add/remove/fence cluster members from the topology page (drives
  the existing membership + assignment APIs; the JSON contract already carries the state).
- Programmatic scale up/down: a first-class "join this cluster" / "drain and leave" flow
  (register -> learner -> voter -> rebalance in; drain assignments -> deregister -> remove
  voter out), exposed via fibrilctl and the admin API, so autoscalers can drive it.

### Execution order and gates

R1 (ganglion schema + catalogue) → R2 (controller task) → R3 (ownership switch + e2e test +
tryout/diagram payoff) → R4 (failover choreography + adversarial suite) → R5 → R6.
Each phase lands with its tests green plus one cluster-tryout assertion where applicable;
FAILURE_MODES.md gains entries for new modes (controller churn, promotion refusal loops,
catalogue divergence).

## R6 implementation breakdown (codebase-grounded, 2026-06-13)

Plan first; implement after sign-off. Order: Phase A (client topology
awareness) then Phase B (multi-partition, fixed-at-create + versioned). Each
item references the real code touchpoint. Gates per item-group: cargo fmt,
package test sweeps, cluster-tryout where applicable, worklog entry.

### Phase A — Client topology awareness (route to owners; redirect on not-owner)

- A1. [DONE, commit 2b70a41] Coordination client-facing topology view.
  `GanglionCoordination::client_topology() -> ClientTopology` (queue -> owner
  node_id + owner_endpoint + partitioning_version + generation), in
  crates/coordination-ganglion/src/lib.rs.
- A2. Protocol: client topology op. Add `Op::Topology` / `Op::TopologyOk`
  (crates/protocol/src/v1/mod.rs Op enum, currently no metadata op). Request:
  optional topic/group filter. Response: list of {topic, partition, group,
  owner_endpoint, partitioning_version} + generation — mirror of
  coordination::ClientTopology. Additive op; relies on existing negotiated
  protocol version.
- A3. Protocol: DEDICATED redirect frame (revised — was an ErrorMsg
  extension). Redirect is control flow, not an error: add `Op::Redirect` with a
  payload `{ topic, partition, group, owner_endpoint, partitioning_version }`.
  Reasons: it slots into the engine's existing opcode dispatch + request_id
  correlation; it carries structured topology without making every
  error-handling path inspect optional fields; and a redirect must be retried
  on a DIFFERENT connection, so it has to bubble up to the routing layer as a
  distinct outcome (not a generic error). `ERR_NOT_OWNER`(=409) stays as the
  terminal error when no redirect target is known.
- A4. Handler: topology source + redirect emission. `handle_connection`
  (crates/protocol/src/v1/handler.rs) gains an injected topology provider
  (Arc closure / trait), mirroring how admin gets `with_coordination` /
  `with_raft_topology` in server.rs. Answer `Op::Topology` from it; when the
  broker returns `BrokerError::NotOwner`, look up the current owner endpoint
  from the provider and emit an `Op::Redirect` for that request_id (fall back
  to `ERR_NOT_OWNER` if the owner is unknown). server.rs wires
  `coordination.client_topology()` as the source in ganglion mode; standalone
  returns a single-node topology (self owns all, no redirects).
- A5. Client: connection pool + topology cache + routing layer.
  crates/client/src/lib.rs is single-connection today (one `EngineSlot` to one
  `address`); the reshape is a layer ABOVE the engine — see "Client
  architecture notes" below. Settlement stickiness is already free (each
  engine's deliveries capture its own `cmd_tx`). Scope:
  - Bootstrap from N broker addresses; fetch topology via `Op::Topology`.
  - Topology cache: queue/partition -> owner endpoint + generation +
    partitioning_version.
  - Connection POOL keyed by owner endpoint, replacing the single EngineSlot.
    ONE engine task per connection (keep existing per-connection loop model;
    NOT a single multiplexed loop — see notes).
  - Route publish/subscribe to the partition owner's pooled engine (connect
    lazily).
  - On `Op::Redirect`: resolve the pending request as a redirect outcome that
    BUBBLES UP to the routing layer (engine can't retry cross-connection);
    routing layer refreshes topology, re-routes to the new owner's engine,
    retries (bounded; retry/refresh counts CONFIGURABLE per settings
    discipline).
  - HEAVY item: multi-owner subscription fan-out — a logical subscription over
    a multi-partition queue opens one Subscribe per owner-partition across
    several pooled connections and merges their deliveries into one
    user-facing stream; reconnect/reconcile becomes per-connection.
  - Keep partition selection out of the user API.
- A6. Tests: handler answers topology + emits redirect; client routes +
  follows redirect + refreshes; multi-owner subscription merges streams (unit +
  integration in handler_tests/client). cluster-tryout: a client routes across
  the 3-node cluster and follows a redirect after an ownership move.

#### Client architecture notes (first-attempt code approach)

Loop model: keep MANY loops, one engine task per connection (already the shape
of `start_engine`). A single multiplexed loop would need a re-armed `select!` /
`FuturesUnordered` over a changing stream set, become a serialization point, and
re-implement settle routing by hand. The pool is just `map<endpoint,
EngineHandle>`; each entry is the existing per-connection engine.

Concrete shape (first attempt, crates/client/src/lib.rs):
- `ClientShared`: replace `engine: Arc<EngineSlot>` (single) with
  `pool: RwLock<HashMap<SocketAddr, Arc<EngineSlot>>>` (each EngineSlot is the
  existing reconnectable single-connection holder) + `topology:
  RwLock<TopologyCache>` + `bootstrap: Vec<SocketAddr>`.
- `engine_for_operation()` -> `engine_for(endpoint)`: look up/insert the pooled
  EngineSlot for an owner endpoint, connecting (start_engine) on miss. Keep the
  per-endpoint reconnect logic exactly as today, just per pool entry.
- Routing helpers: `owner_endpoint(topic, partition, group) -> SocketAddr` from
  the topology cache; `ensure_topology()` fetches via `Op::Topology` from any
  live pooled/bootstrap connection and populates the cache.
- Publish edge (`Publisher`): compute partition (Phase B; partition 0 for now)
  -> owner_endpoint -> `engine_for(owner)` -> send Publish command as today.
- Redirect handling: the engine's response correlation (the `waiters` map)
  gains a `Redirect` resolution. Publish/subscribe command results become
  `Result<T, RoutingOutcome>` where `RoutingOutcome::Redirect{..}` is handled by
  the routing layer (refresh topology, re-route, retry up to a configurable
  bound). The engine itself never retries cross-connection.
- Subscribe edge (`Subscriber` -> `Subscription`): for each owned partition,
  resolve owner -> `engine_for` -> issue Subscribe on that connection; each
  per-connection Deliver feeds a SHARED mpsc behind one logical `Subscription`
  (the user drains one stream). Settlement: the existing per-delivery task that
  captured the delivering engine's `cmd_tx` already routes acks/nacks home — no
  change to `Message`/`InflightMessage`. Per-connection subscription registries
  + reconcile-on-reconnect stay per engine.
- New tunables (settings discipline): redirect retry bound, topology refresh
  cooldown/limit. Client-side, so they live in `ClientOptions`.
- DO NOT change the `Message`/`InflightMessage` settle path — verified the
  settle oneshot already binds to the delivering engine's `cmd_tx`.

### Phase B — Multi-partition (fixed-at-create, versioned for live-repartition compat)

- B1. Topic metadata. Store `{ partition_count, partitioning_version }` per
  topic in the replicated attribute-CAS store (SetAttribute/CompareAndSet
  already exist; the attribute store is the right home for topic-level
  metadata). `declare` sets it (default partition_count=1,
  partitioning_version=0). NEVER an immutable constant — versioned from day one.
- B2. Declare fan-out. `DeclareQueue` (mod.rs) gains `#[serde(default=1)]
  partition_count`. Handler arm (handler.rs ~1889) currently calls
  `declare_queue(topic, 0, group, meta)` — change to register N partition
  resources in the catalogue (register_queue per partition 0..N) and write the
  topic metadata attribute; the controller then assigns owners for all N.
- B3. Routing. `route(key, partitioning_version) -> partition`
  (hash(key) % partition_count under the version). Client computes the
  partition from the message key + topic metadata. First cut may also accept an
  explicit key; default policy is hash. Publishes STAMP partitioning_version.
- B4. Owner-side version fence. Owner rejects/redirects a publish stamped with
  a stale partitioning_version (NotOwner-style with fresh topology) — routing-
  layer mirror of data-plane epoch-before-use. This is the hook that makes a
  future live repartition safe without a wire break.
- B5. Subscribe across partitions. NOTE: `Subscribe` wire does NOT carry a
  partition today (only SubscribeOk/ReconcileSubscription do); subscribe is
  implicitly partition 0. Phase B adds an explicit partition to the Subscribe
  wire. Support a subscription targeting a
  partition SUBSET; a logical subscription fans out one stream per owned
  partition, hidden by the client. (Coverage-first consumer-group assignment is
  DEFERRED — see [[consumer-partition-assignment-model]] — but the subscribe
  model must not hardwire "all".)
- B6. Tests: multi-partition declare -> N catalogue entries + N assignments;
  client routes distinct keys to distinct partitions/owners; stale-version
  publish is fenced; a logical subscription covers all partitions.

### Phase C — Deferred (forward-compat hooks only; not built in R6 core)

- Consumer-group partition assignment (coverage-first; capacity-signal limits;
  under-provisioned alert/autoscale). [[consumer-partition-assignment-model]]
- Live repartitioning (bump partitioning_version + migration job; per-key
  ordering handled by drain/freeze or dual-route under the version).
  [[live-repartitioning-is-a-target]]
- Geo/locality-aware placement + consumer assignment via node labels
  (region/zone); follower/nearest-replica reads (topology may grow from "owner
  endpoint" to "owner + reachable replica endpoints" without a wire break).
- Per-partition consumer-presence + lag observability (cold-partition
  visibility); cross-broker replication-lag aggregation into `fibrilctl
  topology` + admin diagram.

### Production-soundness checks to apply throughout (not just guarantees)
- Coverage-first: never strand a partition (consumer side) and never leave a
  partition without a broker owner (controller already ensures owners).
- Fail-fast over hang for preconditions; bounded retries with CONFIGURABLE
  limits; visible degradation (redirect storms, refresh loops) surfaced not
  silent.
- Forward-compat: nothing assumes partition_count constant; routing/caches keyed
  by partitioning_version.

#### A5 detailed steps (stepwise, each compiles; behavior preserved until routing flips on)

Key seam: every client op is `self.shared.engine_for_operation().await?` then a
method call on the returned `EngineHandle` (publish_*, declare_queue, subscribe
via Command::*). Ack/nack stickiness is already free (the per-delivery task
captured the delivering engine's cmd_tx). Back-compat lever: standalone brokers
return an EMPTY topology (A4a), so "no owner in cache" must FALL BACK to the
single bootstrap connection => existing single-connection behavior is preserved
automatically.

- A5.1 ClientOptions: additive tunables (no behavior change). Add
  `bootstrap_addresses: Vec<SocketAddr>` (defaults to [connect address]),
  `max_redirects: u32` (default ~3), `topology_refresh_cooldown_ms` (anti-storm).
  Per settings-discipline these are client options (no server settings).
- A5.2 TopologyCache type (additive). `struct TopologyCache { generation: u64,
  by_queue: HashMap<(String, u32, Option<String>), OwnerEntry> }`,
  `OwnerEntry { endpoint: SocketAddr, partitioning_version: u64 }`. Methods:
  `lookup(topic, partition, group) -> Option<OwnerEntry>`, `replace(TopologyOk)`,
  `invalidate(topic, partition, group)`. Lives in `ClientShared` behind RwLock.
- A5.3 Pool, NO behavior change yet. Replace `engine: Arc<EngineSlot>` with
  `pool: RwLock<HashMap<SocketAddr, Arc<EngineSlot>>>` + keep a `bootstrap:
  Vec<SocketAddr>`. Add `engine_for_endpoint(addr) -> FibrilResult<Arc<EngineHandle>>`
  (get-or-create pool entry; on miss, start_engine to addr; reuse the existing
  per-entry reconnect logic). Re-point `engine_for_operation()` to return the
  bootstrap entry => all existing call sites + tests unchanged. COMMIT here:
  pure restructure, green.
- A5.4 Topology fetch. Add `Command::Topology { reply }` + engine arm that
  sends `Op::Topology` and resolves a waiter on `Op::TopologyOk`; add
  `EngineHandle::fetch_topology()` and `ClientShared::ensure_topology(topic)`
  (fetch from a bootstrap/any pooled engine if cache misses the queue or is past
  cooldown; replace cache). Additive; not wired into routing yet.
- A5.5 Routing resolver — FLIP routing on. Add
  `engine_for(topic, partition, group)`. DECISION (done): routing is REACTIVE,
  not eager-fetch-on-miss.
  (1) partition = 0 for now (Phase B routes by key);
  (2) cache lookup; HIT -> engine_for_endpoint(owner);
  (3) MISS -> engine_for_endpoint(bootstrap[0]) (no fetch on the hot path).
  The cache is populated by REDIRECTS (A5.6) and by explicit
  `Client::fetch_topology` warm-up. Rationale: eager fetch-on-miss added a
  topology round-trip to every first op (pointless in standalone, where
  topology is empty) AND hung/broke the in-memory unit harness (no responder
  for `Op::Topology`); the redirect path corrects a misroute precisely. So
  `ensure_topology` was dropped; `fetch_topology` stays for explicit warm-up
  (cooldown option reserved for a future warm-up loop).
  Swap the publish/declare/subscribe call sites from `engine_for_operation()`
  to `engine_for(...)` (declare routes to bootstrap; it is topic metadata).
  Standalone tests stay green via the step-4 fallback. COMMIT.
- A5.6 Redirect handling (confirmed/reply-bearing ops). Engine: handle
  `Op::Redirect` -> resolve that request's waiter with a Redirect OUTCOME
  (introduce `enum OpOutcome<T> { Ok(T), Redirect(Redirect) }` or a
  `Result<T, RoutingRedirect>` on the reply oneshots for PublishConfirmed,
  DeclareQueue, Subscribe). Routing layer (Publisher/Client methods): on
  Redirect -> `topology.replace_entry(redirect)` -> engine_for_endpoint(new
  owner) -> retry, bounded by `max_redirects`. Unconfirmed publish has no
  waiter: a Redirect for an unknown request_id invalidates the cached queue
  owner (next publish re-routes); the in-flight fire-and-forget message may be
  lost (documented best-effort). COMMIT.
- A5.7 Subscription fan-out (HEAVY). A logical `Subscription` resolves owner(s)
  for its partition(s) (1 partition now), issues Subscribe on each owner's
  engine, and merges per-connection deliveries into one user-facing mpsc.
  Per-connection subscription registry + reconcile-on-reconnect move to
  per-EngineSlot state (today the `subscriptions` map is global; make it
  per-endpoint). Settlement unchanged. On Redirect during subscribe, re-route
  that partition's Subscribe to the new owner. COMMIT.
- A5.8 Tests (with A6): route-to-owner from topology; redirect-follow re-routes
  and retries a confirmed publish; standalone empty-topology falls back to the
  single connection (existing tests remain green = the back-compat proof);
  multi-owner subscription merge. cluster-tryout: client follows a redirect
  after an ownership move across the 3-node cluster.

Risk notes: A5.3 and A5.5 are the "make storage/routing changes without behavior
change" steps that de-risk the rest. A5.6 (reply-type change to carry Redirect)
ripples through the Command reply oneshots — keep `OpOutcome` internal. A5.7 is
the one genuinely large piece (per-connection registry + merge); everything
before it is mechanical.

#### A9 — in-depth failure-mode pass (after A5/A6 wiring complete)

A dedicated correctness pass over the routing/redirect/pool machinery's failure
paths, each with a test (not just happy path). Cases to cover:
- Owner endpoint unreachable (connect fails): error surfaces cleanly; cached
  entry invalidated so the next op refetches topology; bounded retry honored.
- Redirect loop / ping-pong (A→B→A) and redirect storms: `max_redirects`
  caps it and returns a clear terminal error, never spins.
- Stale topology: cached owner is no longer the owner (NotOwner without a
  redirect target) → invalidate + refetch; with a redirect target → follow.
- Topology fetch fails (bootstrap down): publish/subscribe degrade to a clear
  error; cooldown prevents fetch storms; recovers when a broker returns.
- Mid-operation ownership move: in-flight confirmed publish gets redirected and
  retried on the new owner; unconfirmed publish best-effort (documented) +
  cache invalidated for subsequent ops.
- Connection drop on a pooled owner: per-endpoint reconnect (graceful, with
  resume) without disturbing other pool connections; subscriptions on that
  connection reconcile.
- Partial cluster outage: some owners reachable, some not — unaffected queues
  keep working; affected ones error/redirect without taking down the client.
- Shutdown races: shutdown during an in-flight redirect/fetch/reconnect.
- (Server side) topology source returns owner with unparseable/empty endpoint
  → treated as owner-unknown (terminal NotOwner), never a panic.
Plus the real round-trips deferred from earlier: `fetch_topology` against a
live server, and client reconnect-with-resume end to end.

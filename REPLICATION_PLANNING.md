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
- **Metadata storage**: external (etcd) vs. self-hosted replicated log. **Recommend external.** Self-hosting metadata via your own replicated log creates a chicken-egg bootstrap problem (need metadata to know who's leader for the metadata partition). The workload is tiny; etcd handles it trivially. Revisit only after data-path replication is stable.
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
- etcd, Consul, or another metadata store can implement that trait later. Static config remains useful for local tests and early manual operation.
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

- **Critical**: leader's truncation floor must include the minimum applied offset across all known followers for the partition. Truncating past a follower's applied position forces that follower into snapshot-resync mode unnecessarily.
- Followers report their applied offset to the leader (via heartbeat).
- If a follower is dead/missing, decision needed: wait forever (blocks GC, leaks disk) or drop it from the floor calculation after a timeout (forces it to resync on return). Default: timeout-then-drop, log loudly. Add metric for "events retained beyond leader's local floor due to follower lag."
- **Don't truncate logs that may have been acked**: once `acks=all` (or your equivalent) has returned to the producer, the data is committed; truncating it loses data. Truncation floor respects commit point as well as follower positions.

## Snapshot handling

- **Leader does the snapshot work** for shipping to followers.
- Followers can run their own periodic snapshots locally for their own fast startup. These are independent of any received snapshots.
- Snapshot transfer: stream the existing snapshot blob format. Add a wire-level checksum (the snapshot already has crc32c, but the wire transfer needs its own framing checksum).
- During snapshot transfer, leader keeps generating events. After follower installs, it re-requests from `snapshot_offset + 1`; usually the gap is small.
- If snapshot transfer fails partway, follower stays at its old offset and tries again. No partial-install state.
- **Snapshot-aware retention**: leader can't delete log segments until all followers needing them are either caught up or have grabbed a snapshot. Snapshot lifecycle ties into retention.

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

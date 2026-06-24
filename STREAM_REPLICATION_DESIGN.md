# Durable stream replication — transport design (#73c)

Status: DRAFT for review (precedes 73b/73c/73d implementation). Working doc;
archive when implemented (per the design-notes-home convention).

## Goal

Replicate a DURABLE Plexus stream partition from its owner to its followers so
the partition survives owner node loss, with caught-up-follower promotion on
failover. Gated strictly to the durable tier — ephemeral/speculative stay
owner-only (replication contradicts those tiers). Reuse the queue replication
subsystem; diverge only where streams genuinely differ.

## Key structural finding: streams are already two-log, like queues

| | Queue partition | Stream partition |
| --- | --- | --- |
| data log | message records | stream records |
| progress log | ack/nack events | cursor-commit events |
| durable state | both keratin logs | both keratin logs |

`commit_stream_cursor` is logged as a cursor-commit event (stroma.rs:3977) and
recovery replays it (stroma.rs:3204-3206). So the queue two-log replication
transport — `read_owner_replication_records(message_from, event_from)` plus the
two-log follower apply — maps directly onto streams as `(record_from,
cursor_event_from)`. This is why reuse is high.

## What reuses as-is (fibril)

- Follower worker orchestration: `run_follower_replication_worker_loop` shape
  (resolve owner peer, credit-based streaming tick vs one-shot, OwnerChanged /
  retry / shutdown handling).
- Owner peer resolution: `BrokerOwnerReplicationPeerResolver` (owner endpoint
  from topology — streams already carry owner endpoints from #67).
- Credit-based streaming transport pattern (`ReplicationStream*` ops).
- Checkpoint-install fallback for a far-behind follower.
- Durability requirement resolution (`ReplicationDurabilityPolicy`,
  `ReplicationDurabilityRequirement`) and replication-progress tracking.
- Applied-tail failover-candidate selection in `control_iteration` (heartbeat
  labels -> most caught-up live follower).
- Assignment-apply watcher (`spawn_assignment_watcher_with_follower_replication`).

## What is genuinely new

### Keratin / stroma (the substrate work)

1. `apply_stream_records(tp, part, Vec<(Offset, payload, headers)>, epoch)` —
   write records at the OWNER'S exact offsets (not assign-new), so follower and
   owner logs are byte-identical. (Owner read side already exists:
   `read_stream_records(from, max)` returns `(Offset, payload, headers)`.)
2. `apply_stream_cursor_commits(tp, part, Vec<(Offset, name, position)>, epoch)`
   — apply replicated cursor-commit events so a promoted follower resumes
   subscribers correctly. (Owner read side: a cursor-commit-event read by
   offset — partially present via the recovery replay path; needs an offset-
   ranged reader.)
3. `become_stream_follower` / `advance_stream_epoch` — role + epoch fence at the
   stream log layer (mirrors `become_replication_follower_with_epoch` /
   `advance_queue_epoch`), so stale-epoch stream replication is rejected at the
   storage layer (epoch-before-use).
4. Stream applied-offset query (for the follower's heartbeat applied-tail label
   and for the owner's durable-confirm gate).

### fibril

5. `plan_local_stream_transitions(node_id, prev_stream_assignments,
   next_stream_assignments)` — parallel to `plan_local_assignment_transitions`,
   over `stream_assignments`/`StreamAssignment`/`StreamIdentity`. The role/intent
   logic (Become/Promote/Demote/Stop) is identical; the apply is SIMPLER (no
   consumer leases, no offset release on demote).
6. Stream apply: BecomeOwner -> open channel (already via `route_stream` from
   coordination config); BecomeFollower -> `become_stream_follower` + spawn a
   stream follower-replication worker; Promote/Demote/Stop analogues.
7. A stream follower-replication worker (parallel runtime map keyed by
   `StreamIdentity`) reusing the loop logic; its tick fetches `(record,
   cursor-event)` batches from the owner and calls the new stroma apply.
8. Owner-side replication peer for streams: `read_owner_stream_replication_
   records(record_from, cursor_from, max…)` mirroring the queue peer method.
9. Protocol op for stream replication read (see decision below).
10. Durable-tier confirm: a durable stream publish confirms after the
    `ReplicationDurabilityPolicy` is met (N followers ack the record offset),
    reusing replication-progress tracking. `StreamConfig`/`StreamAssignment`
    carry the durability policy (today defaulted).
11. Failover: extend the `control_iteration` failover-candidate loop (currently
    skips streams via `to_fibril_queue` returning None) to also pick the most
    caught-up live stream follower from a stream applied-tail label.

## Decisions for review

1. **Protocol surface**: the stream replication-read transport frames are
   structurally identical to the queue ones (two logs of `(offset, payload,
   headers)`). Options:
   - (a) Parallel stream-specific replication op keyed by stream identity —
     consistent with the parallel-track decision used throughout #67/#73.
   - (b) Reuse the queue `ReplicationStream*` frames with a resource-kind tag.
   Recommend (a) for separation consistency; the codec is a near-copy of the
   queue one, so the churn is mechanical.

2. **Far-behind follower past the retained window**: streams have retention, so
   a brand-new replica cannot always replay from offset 0. Recommend: a fresh
   follower starts at the owner's current retained floor (checkpoint-install the
   retained window), not historical records — mirrors queue checkpoint-install.
   Accept that a just-added replica holds only the retained window, not dropped
   history (correct for a retention-bounded log).

3. **Cursor consistency**: replicate cursor-commit events in-band (the second
   log) so a promoted follower resumes subscribers at the right position.
   Recommend in-band (the two-log apply already covers it).

## Brick mapping

- 73b: `plan_local_stream_transitions` + stream apply (owner open / follower
  become + worker spawn). Depends on keratin `become_stream_follower`.
- 73c: the transport — keratin apply/fence (items 1-4) + fibril owner-read peer
  (8) + protocol op (9) + follower worker tick (7).
- 73d: durable confirm (10) + applied-tail failover (11).

## Follower worker: follow the queue precedent (reuse, don't fork)

Per the "follow queue precedents" directive, the stream follower worker REUSES the
queue follower-worker machinery rather than a bespoke loop:

- Represent a stream partition as `QueueIdentity{topic, partition, group: None}` at
  the worker layer (a topic is either a stream or a queue, so the runtime map keyed
  by `QueueIdentity` never collides). The `follower_replication_workers` map, tick
  guards, retry / OwnerChanged / shutdown lifecycle all reuse.
- Thread a resource kind (Queue | Stream) through
  `catch_up_replication_follower_from_owner` (+ `_with_checkpoint`) and the worker
  loop so the ONLY divergences are: the apply call
  (`apply_replicated_stream_batch` vs `apply_replicated_queue_batch`, replication.rs
  ~1605) and the peer op (stream-mode peer, already built).
- Make `BrokerOwnerReplicationPeerResolver` kind-aware so a stream assignment
  resolves a `.with_stream_mode()` peer.
- Resume offsets from `stream_replication_next_offsets` (already added).

This keeps the queue path byte-identical (add a branch, don't rewrite) and must be
tested on BOTH paths. It is an invasive refactor of shared data-integrity code, so
it is the careful final piece.

Status: 73c is DONE (committed, green end to end). Built: keratin substrate
(apply + epoch/role + next-offsets) plus a follower stream-marker materialize in
become_stream_follower_with_epoch; fibril engine seam; protocol
StreamReplicationRead op + owner handler; stream-mode remote peer;
ReplicationResourceKind{Queue,Stream} threaded through the shared catch-up +
worker loop + (owner,kind)-keyed resolver (queue path byte-identical, all queue
tests still green); apply_stream_assignment_transition + a stream branch in the
existing assignment watcher that spawns a stream-mode follower worker per followed
partition. End-to-end test:
stream_follower_catches_up_records_and_cursor_from_owner.

DEFERRED to 73d (failover): caught-up follower promotion (PromoteFollowerToOwner
currently keeps the partition a follower) and the durable-confirm timing. DEFERRED
to 73e: follower retention-config propagation (the follower opens with no local
retention policy; retention is mirrored via replication for now).

## Execution plan for the follower worker (next session, step by step)

All anchors are fibril `crates/broker/src/replication.rs` unless noted. The whole
transport plumbing below the worker is already committed (keratin substrate +
engine seam + `StreamReplicationRead` op + owner handler + stream-mode peer +
`stream_replication_next_offsets`). Build the worker bottom-up; commit per step.

Step W1 - resource kind + the apply branch (the ONLY data divergence):
- Add `pub enum ReplicationResourceKind { Queue, Stream }` (broker replication
  module).
- The divergence point is `apply_follower_replication_records` (~line 1513): its
  final call is `engine.apply_replicated_queue_batch(topic, partition.id(), group,
  messages, events)` (~line 1605). Add a `kind` param; when `Stream`, call
  `engine.apply_replicated_stream_batch(topic, partition.id(), messages, events)`
  (group is always None for streams). The records->batches conversion above is
  identical - keep it shared, branch only the final engine call.
- The checkpoint-required arms stay the same shape.

Step W2 - thread `kind` through catch-up + worker (no behavior change for Queue):
- `catch_up_replication_follower_from_owner` (+ `_with_checkpoint`): add `kind`,
  pass to `apply_follower_replication_records`.
- `run_follower_replication_worker_once` (~1919) and
  `run_follower_replication_worker_loop` (~1956): add `kind`, pass down. Queue
  callers pass `Queue` (mechanical).
- Keep the runtime map (`follower_replication_workers`, keyed by `QueueIdentity`)
  as-is: a stream is `QueueIdentity{topic, partition, group: None}` (a topic is
  either a stream or a queue, so no collision).

Step W3 - resolver kind-awareness:
- `BrokerOwnerReplicationPeerResolver` (trait ~line 80) `resolve_owner_peer`
  builds a `ProtocolOwnerReplicationPeer`. For a stream assignment it must build
  `...new_reconnecting(...).with_stream_mode()`. Simplest: pass `kind` to
  `resolve_owner_peer`, or carry it on the assignment wrapper. The endpoint lookup
  (assignment.owner -> node endpoint) is identical.
- The fibril resolver impl lives in `crates/fibril/src/lib.rs` (built from
  coordination) and the test resolver in `crates/broker/tests/broker_tests.rs`
  (~4061) and `crates/protocol/.../replication.rs`. Update each to honor `kind`.

Step W4 - spawn + offsets:
- `spawn_follower_replication_worker_loop` (~2157): accept `kind`; for streams
  spawn with a `PartitionAssignment` synthesized from the `StreamAssignment`
  (owner + followers + epoch, group None) + `kind = Stream`.
- The worker resumes from `engine.stream_replication_next_offsets(tp, part)` for
  streams (record_next, cursor_event_next) instead of the queue worker-state
  offsets. Wire this into how the worker seeds `message_next_offset` /
  `event_next_offset` for the Stream kind.

Step W5 - apply wiring + stream assignment watcher:
- Add `apply_stream_assignment_transition` consuming 73b
  `plan_local_stream_transitions` (broker/src/coordination.rs):
  - BecomeOwner -> `engine.become_stream_owner_with_epoch(tp, part, epoch)` +
    `route_stream` opens the channel (already materializes from coordination
    config).
  - BecomeFollower -> `engine.become_stream_follower_with_epoch(tp, part, epoch)`
    + `spawn_follower_replication_worker_loop(.., Stream)`.
  - DemoteOwnerToFollower / PromoteFollowerToOwner / FreezeOwner / StopFollower:
    mirror the queue arms in `apply_assignment_transition` (broker.rs ~4103),
    minus consumer-lease/offset-release (streams have none).
- Add a stream assignment watcher mirroring
  `spawn_assignment_watcher_with_follower_replication` (broker.rs ~3939) +
  `apply_assignment_snapshot_transitions_with_follower_replication` (~4013),
  driven off the coordination snapshot `stream_assignments` diff. Wire it in
  `crates/fibril/src/lib.rs` next to the queue watcher (cluster mode only).

Step W6 - test end to end:
- Mirror a queue replication integration test: two brokers, declare a durable
  stream RF>=1, publish to the owner, assert the follower's log + cursors match
  (records readable at owner offsets, cursor committed), then kill the owner and
  assert a caught-up follower is promoted and serves (the promotion path is 73d,
  so this step may assert up to follower-has-the-data; promotion lands in 73d).
- Keep all existing QUEUE replication tests green (the kind=Queue path must be
  byte-identical).

Then 73d (durable confirm via `ReplicationDurabilityPolicy`: ReplicaAccepted =
accept-after-write, ReplicaDurable/Majority = fsync-before-ack; caught-up-follower
promotion reusing the applied-tail selection + a `promote_stream_follower_*`
keratin fn) and 73e (RF setting + per-stream override + honest docs).

## Honest durability semantics

Owner-only durable survives process restart (local fsync), NOT node loss. Only
replicated durable (followers >= 1, met `ReplicationDurabilityPolicy`) survives
node loss. Do not advertise clustered durable streams as HA until 73c+73d land.

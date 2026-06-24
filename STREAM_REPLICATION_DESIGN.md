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

## Honest durability semantics

Owner-only durable survives process restart (local fsync), NOT node loss. Only
replicated durable (followers >= 1, met `ReplicationDurabilityPolicy`) survives
node loss. Do not advertise clustered durable streams as HA until 73c+73d land.

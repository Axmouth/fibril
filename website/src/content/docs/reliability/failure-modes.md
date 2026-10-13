---
title: Failure modes and operations
description: What survives which failure per durability tier, and the operator runbook for restarts, failover, repartition, and recovery.
---

This page is the operator-facing view of what happens when things go wrong: what
survives which failure, and what to do about it. It synthesizes the
[reliability semantics](/reliability/semantics/),
[replication](/reliability/replication/),
[recovery quarantine](/reliability/recovery-quarantine/), and
[reconnects](/reliability/reconnects/) pages into one incident-time
reference.

Failure semantics are version-specific. This page describes the current main
branch; consult the matching documentation for a deployed version.
Single-node durability is a real, tested part of the broker. The cluster path
(replication and failover) is experimental and needs more failure testing before
it is production-grade high availability.

## What survives what

The delivery foundation is **at-least-once**: a confirmed publish is durable per
the tier below, and a message is redelivered until it is settled (acked or
dead-lettered). Fibril does not claim exactly-once.

Two distinct failures matter, and they are not the same:

- **Process restart** - the broker process stops and starts again on the same
  machine, with its data directory intact.
- **Node loss** - the machine (or its disk) is gone, so the local data is not
  coming back.

| Tier | Survives process restart | Survives node loss | Notes |
| --- | --- | --- | --- |
| Queue, single owner (`local_durable`) | Yes (fsync per durable write) | No | The partition is unavailable while the node is down. The only mode for a standalone broker. |
| Queue, replicated (`replica_durable` / `majority_durable`) | Yes | Copies survive. Failover waits for recovery proof | `majority_durable` is the cluster default. Confirmation waits for the required replicas. The controller retains the old assignment while a pending recovery request awaits confirmed-history proof, and enrolled queues then recover automatically. Requires Ganglion coordination and assigned followers. A restarted follower of a queue with W >= 2 rejoins as a learner without a recovery fence. |
| Stream, `durable` owner-only | Yes (fsync before deliver/confirm) | No | Survives restart, not node loss. Records and cursors are on the one owner, so the partition waits for that owner to return. |
| Stream, `durable` replicated (`stream_replication_factor` >= 2) | Yes | Copies survive. Automatic failover paused | Record and cursor logs replicate. Replicated ownership changes share the pending-recovery barrier of queues, but automatic recovery does not resolve a stream request. See [operator decisions](#a-recovery-or-held-owner-needs-an-operator). |
| Stream, `speculative` | Partial | No | Delivers off the staged offset and defers the producer confirm until durable, so a confirmed record is durable, but unconfirmed records in flight at a crash can be lost. An owner-only partition waits for its owner to return. |
| Stream, `ephemeral` | Partial | No | Lowest latency, `AfterWrite` (no per-record fsync). Recent records can be lost on a crash. Intended for where freshness beats durability. Makes no durability promise, so the controller can move it to another node when the owner stops heartbeating. |

Cross-cutting guarantees that hold across a restart or a clean failover:

- **Assignment fencing.** Ownership changes bump an **epoch**. Nodes that have
  advanced to it reject older-epoch replication. Cluster-wide preservation of
  confirmed history during ownership changes remains an acceptance gate.
- **Per-partition ordering** is preserved; there is no global order across
  partitions.
- **Leased-but-unacked work survives a crash.** Inflight (offset, deadline) pairs
  are persisted, so a message that was leased to a consumer but not yet acked is
  not lost across a broker restart - it becomes deliverable again.
- **Streams resume from the durable cursor**, not a client-held offset, so a
  reconnecting or failed-over reader continues where its committed cursor left
  off.

## Operator runbook

### Planned restart or rolling upgrade

Use drain so in-flight work is not dropped:

1. `POST /admin/api/drain` (optionally `{ "grace_ms": 5000, "message": "upgrade" }`).
   The broker pushes a going-away notice to every connected client and, in
   coordinated mode, marks itself draining so the controller hands its
   partition ownership to caught-up followers. The call returns once
   ownership has moved or `connection.drain_handoff_timeout_ms` (default
   30s) elapses, reporting `owned_partitions_remaining` and
   `handoff_complete` so a restart script knows whether stopping now is
   gap-free.
2. Clients surface the notice (so apps can stop producing or finish
   in-flight work); ownership moves push topology updates that redirect
   traffic to the new owners while this broker keeps serving whatever it
   still owns.
3. Restart the broker once the drain call returns.

Reconnect grace (on by default, `connection.reconnect_grace_ms`, 5s) keeps a
returning client's session and inflight work intact across the brief break.
Drain uses the same recovery barrier as failover for replicated assignments.
Those assignments remain on the old owner while recovery is pending, so drain
can time out with `handoff_complete = false`. Preserve that owner's data and
inspect the pending recovery before stopping it. A draining node receives no new
placements.

On a broker that only follows a replicated queue (write requirement W of 2 or
more), a restart needs no recovery. The queue keeps serving from its owner and
the restarted follower rejoins as a learner. It does not count toward confirms
until it is re-admitted, so publishes wait, and can fail their confirm, while
fewer than W copies count. At most W-1 followers of a queue rejoin at once, so
in a rolling restart wait for the previous node's followers to be re-admitted
before restarting the next one. See
[restarted queue followers](/reliability/recovery-sealing/#restarted-queue-followers).

### An owner broker dies (cluster)

For enrolled queue histories, automatic ownership replacement uses the recovery
barrier until verified history and an installed write quorum are available:

- The controller persists the previous and proposed assignments, preserving the
  old replica set and replication source. The proposed owner cannot serve through
  that unapproved assignment.
- The recovery worker seals eligible survivors, verifies a complete source,
  transfers its state and activates the installed quorum. If its candidate becomes
  unavailable, the controller can choose another member of the fixed proposed set.
- Inspect `consensus.controller.pending_recoveries` in the admin topology
  response and retain every surviving replica's data. These records preserve the
  original proposal; candidate-replacement logs identify a subsequent handoff.
  Returning replicas can supply missing evidence. Serving resumes only after
  verified activation and exact local admission. Never withdraw a pending
  recovery for an enrolled queue to skip this proof. The withdraw command
  refuses it.
- Check the [admin queues page](/admin-dashboard/) for owner and in-sync
  replica status.

A `local_durable` single-owner partition has no follower to promote, so it is
**unavailable until the node returns** with its data dir. This is expected - that
mode trades node-loss survival for lower latency.

The same hold applies to any resource that only its owner is known to hold in
full: owner-only `speculative` and `durable` streams, and legacy queues (created
before history enrollment) whose write requirement is one copy. A follower or
another node may hold some of the data, but only the owner is known to hold every
confirmed write, so the controller keeps the assignment on the missing owner
instead of serving from a copy that could be missing confirmed data. `ephemeral`
streams make no durability promise and move to another node on liveness alone.

### Choosing the new owner

When an owner is gone or draining, the controller picks the replacement in this
order:

1. A live, non-draining follower. A follower that published an applied-tail
   label in its heartbeat beats one that did not. Labelled followers rank by
   their tail: queues by their log position, streams by record tail first and
   cursor-event tail second. Producer confirms count records, so a follower
   ahead on consumer progress but behind on records does not win. A lagging
   cursor-event log can at worst rewind cursors, which means redelivery.
2. An ordinary round-robin pick, only when no live follower remains. That node
   may have to copy the whole history before it can serve.

Equally ranked followers, including followers with no label, go to the one that
owns the fewest partitions, counting choices made earlier in the same pass, then
to the smallest node id. This spreads a failed node's partitions over its
followers instead of moving them all to one.

For a recovery, the proposed replica set keeps only nodes that already hold a
copy, as long as they still meet the old write requirement. A fresh node with no
copy would otherwise have to receive a full history before the new owner serves.
Normal placement adds followers later. When too few copy holders survive, a fresh
node still joins, because recovery never lowers the write requirement on liveness
alone.

### Whole-cluster restart

A newly elected controller waits before moving any owner. Until every
registered node has a fresh heartbeat, or
`coordination.ganglion.controller_startup_grace_ms` (default 30 seconds)
expires, it keeps existing owners and followers in place. It still fills empty
follower slots and places new partitions. So a node that boots a few seconds
late keeps its partitions. The trade-off is that a registered node that never
returns delays owner failover by up to the grace after every leader change.
`consensus.controller.startup_grace_waiting_for` in the admin topology response
lists the nodes the controller is still waiting for. Set the grace to `0` to
disable it. See [configuration](/configuration/).

### A recovery or held owner needs an operator

The controller lists what it cannot resolve on its own under
`consensus.controller.attention` in the admin topology response
(`fibrilctl admin topology`), and logs a warning when an entry appears. Each
entry names the resource, the reason and the suggested action. Two cases appear
there:

- **A pending recovery automatic recovery will not finish.** The recovery worker
  only recovers queues with an accepted history. A pending recovery for a stream
  or a legacy queue stays until an operator withdraws it.
- **A resource held on a missing owner.** See the hold described above.

Two operator commands resolve these. Both need Ganglion coordination and
admin access, and both are recorded in the activity feed.

```sh
# Remove a pending recovery so the controller plans the partition again.
fibrilctl admin recovery withdraw <topic> <partition> [--group <group>] [--stream]

# Let a held partition move off its missing owner. Gives up writes only that owner stored.
fibrilctl admin recovery accept-owner-loss <topic> <partition> [--group <group>] [--stream] --confirm-data-loss
```

The matching admin routes are `POST /admin/api/recovery/withdraw` and
`POST /admin/api/recovery/accept-owner-loss`, with a JSON body of `kind`
(`queue` or `stream`), `topic`, `partition` and an optional `group`.
Accept-owner-loss also needs `"confirm_data_loss": true` and is refused
without it.

**Withdraw** is refused when:

- the resource has no pending recovery,
- the pending recovery is for a queue with an accepted history, because its
  replicas may already be sealed for that request, or
- recovery already recorded a plan.

Use it for a stream once its owner serves again, or as the first step before
accepting owner loss.

**Accept owner loss** gives up every write that only the missing owner stored,
including confirmed ones. It is refused when:

- the resource has no assignment,
- the resource has an accepted history (it recovers from sealed replicas
  instead), or
- a recovery request is pending for it. Withdraw that first.

The decision covers only the owner and epoch the controller holds at that
moment. The next move uses it up. It is cleared without effect if the owner
comes back and serves again or the assignment changes, so a later failure needs
a fresh decision.

### Replica-durable publishes start failing fast

A `replica_durable` / `majority_durable` confirmed publish fails fast (rather
than blocking to the timeout) when fewer replicas are in sync than
`min_in_sync_replicas`. This is the ISR floor protecting you from acknowledging
writes that are not adequately replicated. Check follower health and lag on the
admin queues page; the partition keeps accepting writes again once enough
followers are back in sync. Set the floor to `1` to disable it.

### A damaged log is found on restart

If a power loss or hardware fault left a queue's event log unreadable, the broker
does not blindly replay it (which could rebuild wrong state or crash the whole
process). It isolates the partition per the `recovery.on_mismatch` policy and
surfaces a quarantine banner. Repair truncates the partition to its last valid
point via `POST /admin/api/quarantine/repair`; a follower then re-fetches the
dropped suffix on its next catch-up. See
[recovery quarantine](/reliability/recovery-quarantine/).

### The node refuses its data dir at startup

The node checks its data dir before opening anything. It refuses to start when
the dir uses an older Fibril layout, belongs to a different `coordination.node_id`,
or holds top-level entries that are not part of the layout. The error names the
problem. The Stroma storage root under `storage/` has its own layout version
and is refused the same way when it is older. Fix the config if the node id is
wrong. For an older layout there is no
migration yet: drain the node (move its partitions to other nodes or let
consumers empty its queues), then start it on a new, empty data dir. The `cache/` directory inside the data dir is disposable and can be
deleted while the node is stopped. See the
[data directory layout](/configuration/#startup-fields).

### On-disk data for a partition this node no longer owns

After a cold restart, a partition that was reassigned to another node while this
node was down is left on disk as **inert cold storage**: it is not served and not
materialized (serving is ownership-gated), so it cannot leak stale data. Its disk
is not reclaimed automatically yet - reclaiming it is a manual step for now.

### A node refuses a stream with "partition kind mismatch"

Older releases could leave a cluster node with a queue partition for a
Plexus stream topic, at
`storage/partitions/@none/<topic>/<partition>` under the data directory, with a
`kind` file that reads `queue`. The node then refuses the stream with
"partition kind mismatch: expected Stream, this partition is Queue", or with
"already declared as a queue", and can neither serve it as owner nor follow it.

The broker repairs this itself when it is safe. When it opens the stream, or
takes a follower role for it, it removes the queue partition only if all of
these hold:

- coordination declares the topic as a stream
- coordination has no queue declaration, partitioning, catalogue entry or
  assignment for the topic, under any group
- no queue for that partition is running on the node
- the queue never held a record: its log is still at offset 0, so no message,
  ack or declaration was ever written

It logs a warning naming the topic, partition and path, and the stream opens in
its place. A follower then copies the stream from its owner.

If any check fails, nothing is removed and the node logs an error once with the
reason. Repair it by hand only when the partition holds no data you need:

1. Stop the node.
2. Delete the directory named in the error,
   `storage/partitions/@none/<topic>/<partition>` under the data directory.
   Partition 0 is `0000000000`.
3. Start the node again.

### Growing or shrinking a queue's partitions

Live repartitioning (Ganglion mode) is driven from the admin topology page. A
grow adds partitions; a shrink drains the retiring partitions before retiring
them, and the cutover is fenced on cluster-wide client adoption of the new
routing, with publish version-fencing as the correctness backstop. Watch the
topology page through the transition.

### Tuning reconnect grace

`connection.reconnect_grace_ms` controls how long a dropped resumable client is
held before its subscriptions are cleaned up and its unsettled inflight is
requeued. It is on by default (5s). Lower it for faster redelivery on a genuinely
dead consumer; raise it for flakier networks; set `0` to disable and clean up
immediately on disconnect.

## See also

- [Reliability semantics](/reliability/semantics/) - the formal delivery
  and durability guarantees.
- [Replication](/reliability/replication/) - durability levels, in-sync
  replicas, and failover.
- [Recovery quarantine](/reliability/recovery-quarantine/) - damaged-log
  handling.
- [Reconnects](/reliability/reconnects/) - reconnect grace and resume.
- [Configuration](/configuration/) - the durability, replication, and
  connection settings referenced here.

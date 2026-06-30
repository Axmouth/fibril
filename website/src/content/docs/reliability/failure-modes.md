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

Failure semantics are version-specific. This page describes the current release;
treat it as the contract for the version you are running, not a forward promise.
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
| Queue, single owner (`local_durable`) | Yes (fsync per durable write) | No | The partition is unavailable while the node is down. This is the default and the only mode for a standalone broker. |
| Queue, replicated (`replica_durable` / `majority_durable`) | Yes | Yes | A confirmed publish waited for follower durability, so a caught-up follower is promoted on owner loss. Requires Ganglion coordination and assigned followers. |
| Stream, `durable` owner-only | Yes (fsync before deliver/confirm) | No | Survives restart, not node loss. Records and cursors are on the one owner. |
| Stream, `durable` replicated (`stream_replication_factor` >= 2) | Yes | Yes | Record and cursor logs replicate to followers; a caught-up follower is promoted on owner loss. |
| Stream, `speculative` | Partial | No | Delivers off the staged offset and defers the producer confirm until durable, so a confirmed record is durable, but unconfirmed records in flight at a crash can be lost. |
| Stream, `ephemeral` | Partial | No | Lowest latency, `AfterWrite` (no per-record fsync). Recent records can be lost on a crash. Intended for where freshness beats durability. |

Cross-cutting guarantees that hold across a restart or a clean failover:

- **No split-brain.** Ownership changes bump a fencing **epoch**. A stale former
  owner cannot keep serving or replicating: its writes and replication carrying
  an old epoch are rejected at the storage layer.
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
   The broker pushes a going-away notice to every connected client.
2. Clients surface it (so apps can stop producing or finish in-flight work) and,
   when the socket closes, reconnect - redirecting to the current owner.
3. Restart the broker.

Reconnect grace (on by default, `connection.reconnect_grace_ms`, 5s) keeps a
returning client's session and inflight work intact across the brief break. For
true zero-downtime in a cluster, graceful ownership handoff on drain (moving
ownership to a follower before the node goes down) is still pending; today a
single-owner `local_durable` partition is briefly unavailable across its
restart.

### An owner broker dies (cluster)

Failover is automatic when coordination is enabled and the partition has
followers:

- The controller reassigns the partition, bumps the epoch, and promotes a
  caught-up follower at its local durable tail.
- Producers and consumers are redirected to the new owner by the topology the
  broker pushes; the high-level clients re-resolve and reconnect.
- Check the [admin queues page](/admin-dashboard/) for owner and in-sync
  replica status.

A `local_durable` single-owner partition has no follower to promote, so it is
**unavailable until the node returns** with its data dir. This is expected - that
mode trades node-loss survival for lower latency.

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

### On-disk data for a partition this node no longer owns

After a cold restart, a partition that was reassigned to another node while this
node was down is left on disk as **inert cold storage**: it is not served and not
materialized (serving is ownership-gated), so it cannot leak stale data. Its disk
is not reclaimed automatically yet - reclaiming it is a manual step for now.

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

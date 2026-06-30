---
title: Replication
description: Replica-durable queues, follower catch-up, and failover in Fibril's
  experimental cluster mode.
slug: 0.2/reliability/replication
---

By default a queue partition lives on a single broker. That broker fsyncs every
durable write, so the data survives a process restart, but it does not survive
losing that node, and while the node is down the partition is unavailable.

Replication keeps copies of a partition on other brokers so a partition can
survive and keep serving when its owner fails.

This is experimental and only active in [Ganglion coordination
mode](/0.2/concepts/clustering/). Standalone brokers own every queue and do
not replicate.

## What Fibril does

When coordination is enabled and a partition is assigned followers, each
partition has one **owner** and one or more **followers**:

* **Followers catch up by pulling.** A follower worker reads the owner's durable
  message and event records over the protocol and applies them durably to its
  own log. If a follower falls too far behind the owner's retained log, it
  installs an owner checkpoint and resumes from there.
* **Failover promotes a caught-up follower.** When the controller sees the owner
  is gone, it reassigns the partition and bumps its fencing **epoch**. A follower
  is promoted at its local durable tail. The old owner cannot keep serving,
  because writes and replication carrying a stale epoch are rejected (no
  split-brain).
* **Replica-durable publishes wait for replicas.** When the assignment's
  durability policy requires more than the owner, a confirmed publish does not
  return until enough followers have reported the required progress, subject to a
  timeout and an in-sync floor.

## Durability levels

The assignment durability policy decides what a confirmed publish waits for. It
is set per cluster through `coordination.ganglion.assignment_durability` (see
[configuration](/0.2/configuration/)):

| Mode | A confirmed publish returns once... |
| --- | --- |
| `local_durable` | the owner has durably written the append (the default, same as a single node) |
| `replica_accepted` | N assigned nodes (including the owner) have accepted the append (weaker than fsync) |
| `replica_durable` | N assigned nodes (including the owner) have durably written the append |
| `majority_durable` | a durable majority of the assigned replica set has the append |

`N` includes the owner, so `replica_durable` with `N = 2` means the owner plus
one durable follower.

## In-sync replicas

A follower counts as in sync when it has reported durable progress recently
enough (`runtime_seed.replication.isr_timeout_ms`). Two runtime settings gate
replica-durable acceptance:

* `min_in_sync_replicas` is a floor. When fewer replicas are recently in sync
  than the floor, replica-durable publishes fail fast with a clear error instead
  of blocking until timeout. `1` disables the floor.
* `confirm_timeout_ms` bounds how long a replica-durable confirm waits before
  failing.

The follower read budget and poll intervals
(`runtime_seed.replication.*`) tune catch-up throughput versus idle confirm
latency. See the [configuration](/0.2/configuration/) replication settings.

## Activation and conditions

* Replication requires Ganglion coordination mode and a follower target
  (`coordination.ganglion.target_followers` greater than zero). It is a
  cluster-level placement decision, not a per-queue client option.
* A partition replicates only after the controller has assigned it followers.
* `local_durable` queues behave exactly like single-node queues even in a
  cluster. Replica-durable confirms only mean something when the durability
  policy requires more than the owner.
* Follower application is durable: a follower fsyncs before reporting the
  progress that owners count toward replica-durable confirms.

## Tradeoffs and limits

* Replica-durable confirms add latency: a publish waits for follower progress,
  bounded by the follower poll interval and the confirm timeout.
* This surface is experimental. Failover safety rests on assignment epochs plus
  local promotion gates, and more failure testing is needed before treating it
  as production-ready high availability.
* Cross-broker replication-lag aggregation into a single cluster view is still
  pending. A broker's own follower workers and their progress are visible on the
  [admin queues page](/0.2/admin-dashboard/).

## See also

* [Clustering](/0.2/concepts/clustering/) for ownership, epochs, and coordination modes.
* [Recovery quarantine](/0.2/reliability/recovery-quarantine/) for how a node handles a damaged log on restart.
* [Configuration](/0.2/configuration/) for the replication and durability settings.
* [Project status](/0.2/status/) and [implemented surface](/0.2/implemented-surface/) for what is wired.

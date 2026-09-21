---
title: Replication
description: Replica-durable queues, follower catch-up, and failover in Fibril's experimental cluster mode.
---

By default a queue partition lives on a single broker. That broker fsyncs every
durable write, so the data survives a process restart, but it does not survive
losing that node, and while the node is down the partition is unavailable.

Replication keeps copies of a partition on other brokers so a partition can
survive and keep serving when its owner fails.

This is experimental and only active in [Ganglion coordination
mode](/concepts/clustering/). Standalone brokers own every queue and do
not replicate.

## What Fibril does

When coordination is enabled and a partition is assigned followers, each
partition has one **owner** and one or more **followers**:

- **Followers catch up by pulling.** A follower worker reads the owner's durable
  message and event records over the protocol and applies them durably to its
  own log. If a follower falls too far behind the owner's retained log, it
  installs an owner checkpoint and resumes from there.
- **Replicated failover waits for recovery proof.** The controller records a
  proposed replacement and retains the previous assignment and replication
  source. Explicitly enrolled queue histories have an automatic worker that seals
  replicas, verifies a source, installs the recovered state and activates a new
  quorum. Ordinary declarations still await enrollment support; legacy histories
  have no automatic migration into this recovery path. Heartbeat tails can suggest a
  candidate but cannot authorize it to serve.
- **Replica-durable publishes wait for replicas.** When the assignment's
  durability policy requires more than the owner, a confirmed publish does not
  return until enough followers have reported the required progress, subject to a
  timeout and an in-sync floor. Each counted follower must have the queue payload
  batch and its exact enqueue-event frontier; the complete payload group is
  required when one enqueue record names several messages. Delivery visibility
  uses the same dependency proof.

## Durability levels

The assignment durability policy decides what a confirmed publish waits for. It
is set per cluster through `coordination.ganglion.assignment_durability` (see
[configuration](/configuration/)):

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

- `min_in_sync_replicas` is a floor. When fewer replicas are recently in sync
  than the floor, replica-durable publishes fail fast with a clear error instead
  of blocking until timeout. `1` disables the floor.
- `confirm_timeout_ms` bounds how long a replica-durable confirm waits before
  failing.

The follower read budget and poll intervals
(`runtime_seed.replication.*`) tune catch-up throughput versus idle confirm
latency. See the [configuration](/configuration/) replication settings.

## Activation and conditions

- Internal reads, writes, checkpoint operations and streaming controls require
  the `@node` principal authenticated on the current connection. Ordinary users
  cannot access them, including when a logical session resumes. Production peers
  use the configured cluster secret and authenticate again when reconnecting.

- Replication requires Ganglion coordination mode and a follower target
  (`coordination.ganglion.target_followers` greater than zero). It is a
  cluster-level placement decision, not a per-queue client option.
- A partition replicates only after the controller has assigned it followers.
- `local_durable` queues behave exactly like single-node queues even in a
  cluster. Replica-durable confirms only mean something when the durability
  policy requires more than the owner.
- Follower application is durable: message and event writes overlap, and both
  completions drain before queue state is applied and progress is reported.
  Failed or interrupted application blocks promotion until recovery or resync.
- Progress reports are tied to an assignment epoch and an ordered transport
  session. Replacement sessions and assignments invalidate earlier reports;
  resets replace both reported frontiers together.
- Run matching broker revisions across a cluster. Older reports without an
  assignment epoch remain readable but do not count toward replicated confirms.
  The client publish, delivery and acknowledgment frames are unchanged.

## Tradeoffs and limits

- Replica-durable confirms add latency: a publish waits for follower progress,
  bounded by the follower poll interval and the confirm timeout.
- This surface is experimental. A follower's local tails can omit a batch
  confirmed by the old owner and another follower. Recovery therefore requires
  accepted-history witnesses and fencing before selecting and installing a
  source. Enrolled queues retain their configured confirmation threshold after
  recovery; unsupported or insufficient evidence leaves the partition fenced.
  Checkpoint backfill is checked before promotion and after restart. Unix
  checkpoint installation uses a durable journal to resume interrupted log/state
  replacement before ordinary replay; completed retries preserve later backfill.
  Linux fault and process-kill tests cover this local path. Non-Unix checkpoint
  installation is unsupported pending durable metadata support.
- The controller persists pending recovery metadata for owner replacement and
  follower-set or durability-policy changes involving replicated confirmation.
  Existing healthy owners retain their active configuration until recovery can
  safely activate the proposed assignment. Recovery of enrolled queues requires
  a complete source and an available proposed owner; replacing an unavailable
  proposed owner and reconstructing crossed histories remain rollout gates.
  Pending requests appear in the admin topology's controller status. Do not
  discard a surviving replica or clear a request to bypass recovery proof.
- Cross-broker replication-lag aggregation into a single cluster view is still
  pending. A broker's own follower workers and their progress are visible on the
  [admin queues page](/admin-dashboard/).

## See also

- [Clustering](/concepts/clustering/) for ownership, epochs, and coordination modes.
- [Recovery sealing](/reliability/recovery-sealing/) for explicit seal authorization, retained identity and remaining recovery gates.
- [Recovery quarantine](/reliability/recovery-quarantine/) for how a node handles a damaged log on restart.
- [Configuration](/configuration/) for the replication and durability settings.
- [Project status](/status/) and [implemented surface](/implemented-surface/) for what is wired.

---
title: Clustering and coordination
description: How Fibril assigns partition ownership across brokers, fences stale owners, and moves ownership on failure.
---

A single broker is simple: it owns every queue and answers every request. To
spread queues across machines and to survive losing a machine, brokers need to
agree on who owns what.

Fibril's clustering is experimental. It adds a coordination layer that assigns
each queue partition to a broker, fences stale owners, and reassigns ownership
when a broker fails.

## Two modes

- **Standalone (default, `coordination.mode = "static"`).** One broker owns all
  queues. There is no coordination, no ownership negotiation, and no replication.
  This is the simplest deployment and needs no cluster configuration.
- **Coordinated (Ganglion mode).** Each broker runs an embedded coordinator and
  they form one consensus group. A controller assigns ownership, and clients are
  routed to the current owner of each partition. Enable it with `[coordination]
  mode = "ganglion"` (see [configuration](/latest/configuration/)).

The embedded coordinator currently uses the Raft consensus protocol under the
hood. You do not configure Raft directly beyond its timings: Fibril's surface
talks in terms of coordination, owners, followers, and a leader, and the
consensus protocol is an implementation detail of the coordinator.

## Try a cluster with Docker

The fastest way to see a cluster, in under a minute, with no clone and no build.
You need Docker with the Compose plugin:

```sh
curl -fsSL fibril.sh/tryout.sh | sh
```

This fetches a small Compose file and starts a three-broker coordinated cluster,
then prints an admin dashboard URL for each broker. The cluster seeds a few demo
queues (one grouped) and a Plexus stream on startup, so the topology page shows
partition ownership spread across the brokers right away instead of an empty
board. Open one and visit the topology page to watch ownership, followers, and
fencing epochs.

If you would rather not pipe a script into your shell, fetch the Compose file and
run it yourself:

```sh
curl -fsSL https://raw.githubusercontent.com/Axmouth/fibril/main/compose.cluster.example.yaml -o fibril-cluster.yaml
docker compose -f fibril-cluster.yaml up
```

Stop and remove everything, including the data volumes:

```sh
docker compose -f fibril-cluster.yaml down -v
```

## Watch traffic in the terminal visualizer

To see real wire traffic and partition routing rather than the topology page, the
visualizer tryout brings up one broker and drops you into a live terminal view,
again with no clone and no build. It needs Docker with the Compose plugin and a
real terminal (it is interactive):

```sh
curl -fsSL fibril.sh/viz.sh | sh
```

This animates publishes, confirms, deliveries, acks, pings and errors as moving
dots across partition lanes, with a metrics HUD. Press `q` to quit, which tears
the broker back down. It is a single standalone broker, so every partition is
owned by the one broker. For ownership spread across machines, use the cluster
tryout above and its topology page.

If you would rather not pipe a script into your shell, fetch the Compose file and
run the visualizer yourself (it needs `run`, not `up`, for a real terminal):

```sh
curl -fsSL https://raw.githubusercontent.com/Axmouth/fibril/main/compose.viz.example.yaml -o fibril-viz.yaml
docker compose -f fibril-viz.yaml run --rm viz
docker compose -f fibril-viz.yaml down -v   # stop the broker and remove its data
```

The richer interactive keys (kill and restart a consumer, nack, change the rate,
consumer-group cohorts, routing across a real cluster) are available when you run
the visualizer from source with `--viz`, described next.

## Try a cluster locally

You can also stand up the cluster from source instead of the published image, with
the tryout script. You will need the repository checked out and
a Rust toolchain, since the first run builds the broker from source. It starts several actual broker processes
(each with its own ports and data directory), forms one coordination group,
runs a few checks, and then holds the cluster open so you can explore it:

```bash
scripts/cluster-tryout.sh --ganglion --nodes 3 --hold
```

It prints an admin dashboard URL for each node. Open one and visit the topology
page to watch ownership, followers, and fencing epochs across the three brokers.
Press Ctrl-C to stop every broker it started and clean up.

A few variations are worth trying:

- `--staggered` starts the nodes one at a time, so you can watch the cluster form
  (no quorum, then an election, then the remaining members join).
- `--failover-verify` runs a confirmed producer and consumer against a survivor,
  kills the partition owner mid-load, and checks that no confirmed message is lost.
- `--repartition-smoke` grows and then shrinks a queue's partition count under
  live traffic.
- `--viz` starts a single standalone broker and drops you into a live terminal
  visualizer of real wire traffic and partition routing (publishes, confirms,
  deliveries, pings, errors as moving dots across partition lanes, with a metrics
  HUD). Interactive keys steer it live: `Tab` to focus a client, `space` to
  pause/resume it, `k` / `r` to kill / restart it (watch its unacked messages
  redeliver on reconnect), `n` to nack its next delivery (watch the requeue +
  redelivery), `[` / `]` to change the publish rate, `c` to toggle confirms, `g`
  to switch keyed vs round-robin routing, and `q` to quit (which tears the broker
  down). Pass `--consumer-group <name>` to the visualizer to make the clients an
  exclusive cohort: the broker divides the partitions across them, and killing a
  client moves its partitions to a surviving peer. Add `--ganglion --nodes 3` to
  run the visualizer against a real
  cluster: it fetches topology, connects to every broker, and routes each publish
  and subscribe to the partition's owner, with lanes colored by owning broker.
  Against a `--ganglion` cluster, `+` / `-` live-repartition the demo queue
  (double or halve its partition count) through the admin API: the lanes grow or
  drain on screen as the clients reconnect against the new layout. Live
  repartition is coordination-only, so the keys do nothing against a single
  standalone broker.

This needs `cargo`, plus `jq` and `curl` for the coordinated-mode checks. The
first run builds the broker and CLI, so it takes longer than later runs.

## What Fibril does in coordinated mode

- **Brokers register themselves.** Each broker publishes itself into a shared
  node table and keeps a heartbeat fresh. A broker whose heartbeat goes stale
  stops counting as live.
- **A controller assigns ownership.** The consensus leader acts as the
  controller. It assigns every partition an **owner** and, when followers are
  configured, a follower set, and it spreads a queue's partitions across distinct
  nodes before reusing a node.
- **Fencing epochs prevent split-brain.** Each assignment carries an epoch.
  Whenever ownership moves, the epoch is bumped. A broker that believes it is
  still the owner cannot keep serving once the epoch has advanced: writes and
  replication carrying a stale epoch are rejected.
- **Clients follow ownership.** A producer or consumer that contacts a
  non-owner is redirected to the current owner, and clients refresh their
  topology and retry. This is transparent in the Rust client.

Ownership, followers, and epochs are visible per partition on the admin
[topology page](/latest/admin-dashboard/) and through `fibrilctl topology`.

## Operator actions

In coordinated mode the topology page and admin API expose cluster operations:

- **Repartition** a queue (grow or shrink its partition count).
- **Add or remove a consensus voting member** to change the coordination quorum.

Change voting membership deliberately and one member at a time, since quorum
changes affect availability.

## Activation and conditions

- Standalone mode needs no coordination configuration and keeps single-node
  behavior.
- Coordinated mode is enabled per node with `[coordination] mode = "ganglion"`
  and its `[coordination.ganglion]` settings (consensus timings, follower target,
  assignment durability).
- The intended shape for large clusters is a small voting set with many brokers
  participating as registered coordination clients, not every broker becoming a
  voter.
- Ownership assignment and failover are gated by epochs plus each broker's local
  promotion checks, so a returning stale owner is demoted rather than allowed to
  double-serve.

## Tradeoffs and limits

- Clustering is experimental and needs more failure testing before it is
  production-ready high availability.
- Cross-broker aggregation of replication lag and in-sync state into one cluster
  view is still pending.
- Coordination outages do not take the broker down: assignment work retries, and
  brokers keep serving what they already own.

## See also

- [Replication](/latest/reliability/replication/) for follower catch-up and replica-durable confirms.
- [Recovery quarantine](/latest/reliability/recovery-quarantine/) for damaged-log handling on restart.
- [Partitioned queues](/latest/concepts/consumer-groups/) and [partition routing](/latest/development/partition-routing/) for how clients address partitions.
- [Configuration](/latest/configuration/) for coordination settings.

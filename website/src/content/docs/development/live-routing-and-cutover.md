---
title: Live routing and repartition cutover
description: How clients learn current routing, how the broker pushes topology changes, and how a repartition cutover is fenced on client adoption.
---

This is a development note. It records how routing stays current on clients and how
a live repartition is finalized safely, plus the assumptions each step relies on.

## The pieces

1. **Initial routing.** On connect a client fetches the cluster topology
   (`Op::Topology` -> `TopologyOk`) and fills its routing cache (per-partition
   owners + partition counts). Standalone brokers return an empty topology and the
   client routes to partition 0.

2. **Correctness backstop (always on).** Every publish carries the
   `partitioning_version` it routed under. An owner that receives traffic routed
   under a stale version rejects/redirects it (`Op::Redirect` names the current
   owner). This is what makes routing *correct* during any change; everything
   below only makes it *cleaner and quieter*.

3. **Live topology push.** When the routing *content* changes (owners, partition
   counts, partitioning versions) the broker pushes a `TopologyUpdate` (op 101,
   same body as `TopologyOk`) to each connection. The client applies it to its
   routing cache (replace + pool prune, generation-guarded against stale/out-of-order
   pushes) and acks the generation it now reflects (`TopologyUpdateAck`, op 102).
   The initial snapshot still comes from the client's `TopologyRequest`; the push
   carries the deltas. The trigger is the routing content, not the raw coordination
   generation. The generation bumps on any committed metadata change cluster-wide
   (any topic's declare, a transition marker, runtime settings, an unrelated
   failover), so triggering on it would re-push an identical topology to every
   client whenever anything anywhere changed. (Heartbeats and other label-only
   updates do not bump the generation - the state machine absorbs them silently -
   so this is not a per-heartbeat storm, but it is still far more often than a
   given client's routing actually changes.) Content-triggering pushes only when
   this client's routing changed. The pushed frame still carries the live
   generation, which is what the client acks.

4. **Adoption tracking.** The broker records, per connection, the highest
   generation that connection has acked (`TopologyAdoptionTracker`). It reports
   the lowest acked generation across its connections as a heartbeat label
   (`fibril/topology_adoption`). The controller takes the cluster-wide minimum
   (`global_topology_adoption`) as the fleet's adoption floor.

5. **Cutover fencing.** A live repartition (grow/shrink) writes a transition
   marker, bumps the partitioning version (the routing cutover), and drains the
   pre-cutover backlog of the old partitions. The controller finalizes the
   transition (retiring shrunk-away partitions and clearing the marker) only once
   the backlog has drained **and** the fleet has adopted the new routing (or an
   adoption timeout elapses).

## The two counters, and the bridge between them

There are two independent counters, and conflating them is the easy mistake:

- **Topology generation** is the coordination committed-snapshot generation. It is
  what `TopologyOk.generation` carries and what a client acks.
- **Partitioning version** is per `(topic, group)` and is what publishes are
  fenced on.

A repartition tracks the partitioning version, but a client acks the topology
generation. To relate them, the transition marker carries an
`adoption_generation`: the topology generation that reflects the new partitioning.
"The fleet adopted this repartition" then means "every node's acked generation has
reached `adoption_generation`."

`adoption_generation` is stamped **eagerly, right after the cutover** (the grow or
shrink stamps it once the version bump commits). It cannot be set at marker
creation because the generation only exists after that bump. Eager stamping is
what makes the gate work with content-gated pushes: the post-cutover push carries
a generation at least this high and the client acks it, but no further push fires
while the cutover drains, so a connection's acked generation stays at the cutover
value. Stamping that value (rather than a later, churned one) keeps the gate
satisfiable.

## The finalize gate

```
finalize = drained_complete && (adopted || adoption_timed_out)
adopted  = global_topology_adoption() >= marker.adoption_generation
```

`adopted` is false until `adoption_generation` is stamped and the cluster minimum
catches up. `adoption_timed_out` is measured from when the transition first became
drain-complete, bounded by `coordination.ganglion.repartition_adoption_timeout_ms`
(default 30s).

## Assumptions and invariants

- **Adoption fencing is a refinement, not correctness.** Step 2 (version-fencing +
  redirects) is what guarantees per-key ordering through a cutover. The adoption
  gate only reduces misroute churn and lets a shrink retire partitions cleanly
  once clients have stopped routing to them. Because of this it is safe to bound
  the wait with a timeout.
- **Only push-capable connections count.** A connection appears in the adoption
  tracker only after it acks at least once. A silent or pre-push client never
  drags the cluster minimum down; it is covered by the version-fence and the
  timeout. A connection's entry is dropped when it closes.
- **Only live nodes count.** `global_topology_adoption` reads adoption labels from
  nodes whose heartbeat is within the liveness TTL. A dead node's adoption label is
  frozen at its last value and represents departed clients (the clients connected
  to it are gone), so counting it would wrongly pin the cluster minimum down and
  stall every cutover on the timeout. Live nodes self-heal: each heartbeat replaces
  a node's whole label set, so a live node that loses all its clients drops its
  adoption label on its next beat.
- **No adoption signal means "not adopted yet."** If no connection anywhere has
  acked, `global_topology_adoption()` is `None` and the gate leans entirely on the
  timeout.
- **New and reconnecting clients converge for free.** They fetch the current
  topology on connect, so they already reflect the latest generation and count as
  adopted.
- **The push lands race-free.** Clients create their routing cache (and catalogue
  state) before the bootstrap connection starts, so a push the broker sends right
  after HELLO has somewhere to land. Wiring the apply target after construction
  would let the first push be acked without being applied (a false ack).
- **The push triggers on content, not the generation.** The generation advances on
  any committed metadata change cluster-wide (other topics, markers, runtime
  settings, unrelated failovers), not just this client's routing - and not on
  label-only updates like heartbeats, which the state machine absorbs without
  bumping it. Triggering on the routing *content* (queues + streams) pushes only
  when this client's routing actually changed. The pushed frame still carries the
  live generation for the client to ack.
- **The adoption gate is stamped eagerly, at the cutover.** Because pushes are
  content-gated, a connection stops re-acking once the cutover settles (no further
  content change for that queue), even though unrelated cluster activity keeps
  bumping the generation. So the marker's `adoption_generation` is captured at the
  cutover (the generation a post-cutover push carries), not lazily at drain-complete
  time (by which point the generation may have advanced past what clients acked,
  leaving the gate unsatisfiable until the timeout). A marker with no
  `adoption_generation` predates this fencing and is treated as ungated.

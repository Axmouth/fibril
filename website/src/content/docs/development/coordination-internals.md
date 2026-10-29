---
title: Coordination internals
description: Development notes on cohort assignment, generation fencing, placement, and why load data is never authority.
---

This is a development note. User-facing behavior lives in
[clustering](/concepts/clustering/) and
[consumer groups](/concepts/consumer-groups/). This page records how the
coordinator stays simple and still safe.

## The gate is the correctness backstop, the plan is advisory

Each partition has a delivery gate (an atomic assignee) that admits at most one
consumer, enforced locally on every owner, always. The cross-broker assignment
plan the controller computes and publishes is therefore advisory and
eventually-consistent: about one heartbeat of lag affects balance only, never
correctness. A departed member's partitions pause until the next plan rather than
ever being double-delivered.

This split is what lets the coordinator avoid global locks and strict consensus on
the hot path. The same principle holds for placement and routing: assignment
epochs, owner fences, and the per-partition gate are the correctness mechanisms,
and everything advisory falls back to them.

## Cohort plans are per-cohort, durable, and generation-fenced

The published assignment document carries a `generation`, bumped only when the
assignment content actually changes, so re-publishing a stable plan is a no-op.

- Per-cohort, not one shared counter. Each cohort document is its own
  authoritative version. A single cluster-wide counter would force re-stamping
  every cohort whenever any one changed, for no correctness gain.
- Durable in the document, not an in-memory counter. The generation is read back
  from the committed attribute before each publish, so it stays monotonic across a
  controller leader change.
- Owners fence stale plans. An owner ignores any plan older than the one it holds,
  so a late or out-of-order slice never overwrites a newer one. An equal
  generation is still re-resolved, because local subscriptions may have changed
  since the last apply.

## Leader changes are generation-stable

A fresh leader's controller would otherwise start with empty sticky state, so its
first plan could differ from the prior one even with unchanged membership,
causing a needless rebalance on every leader change. The controller seeds itself
from each cohort's published plan before planning, so the reconstructed plan
matches what is published, the generation does not bump, and owners see no change.

## Placement spreads a queue's partitions first

When assigning owners and followers, the controller spreads a queue's partitions
across distinct nodes before reusing a node. On small clusters this gives the most
balanced and failure-tolerant layout (a single node loss takes out the fewest of
a queue's partitions) rather than packing partitions onto one broker.

## Failover placement prefers live copies

When an owner is gone or draining, `choose_failover_owner` picks the
replacement from the live, non-draining followers of the committed assignment.
The ranking comes from each follower's advisory applied-tail heartbeat label
(`applied/<topic>/<partition>[/<group>]`):

- A follower with a label beats one without.
- A queue ranks by its log position.
- A stream ranks by its record tail first and its cursor-event tail second.
  Producer confirms count records, so a record gap can lose confirmed data,
  while an event gap only rewinds cursors and causes redelivery.
- Ties go to the follower that owns the fewest partitions, counting picks made
  earlier in the same pass, then to the smallest node id.

Only when no live follower remains does the planner's ordinary pick stand. For
a recovery, the proposed replica set keeps only nodes that already hold a copy,
as long as they still meet the old write requirement. A fresh node would put a
full history copy on the critical path, so normal placement adds it after
activation. When too few copy holders survive, a fresh node joins right away,
because promotion never lowers the write requirement on liveness alone.

Labels can be stale, so the pick is a hint. Recovery still proves the history
before the new owner serves. The user-facing rules are in
[choosing the new owner](/reliability/failure-modes/#choosing-the-new-owner).

## Startup grace holds owners after a leader change

A controller that just won leadership moves no owner until every node registered
in the committed snapshot has a fresh heartbeat, or until
`coordination.ganglion.controller_startup_grace_ms` (default 30000) passes. It
still fills empty follower slots and places new partitions. This stops a
whole-cluster restart from moving partitions away from a node that is still
booting. The grace restarts on every leadership win, and expired-heartbeat
warnings wait for it too, since labels go stale while the whole cluster is down.
The admin topology lists the nodes still awaited in
`consensus.controller.startup_grace_waiting_for`. See
[whole-cluster restart](/reliability/failure-modes/#whole-cluster-restart).

## Owner-only resources wait for their owner

A resource without an accepted (enrolled) history whose write requirement is one
copy has only its owner known to hold every confirmed write. This covers
owner-only `speculative` and `durable` streams and legacy queues. Promotion keeps
such an assignment on the missing owner instead of serving from a copy that may
lack confirmed data. `ephemeral` streams make no durability promise, so they
still move on liveness alone.

The controller lists these holds, and pending recoveries the automatic driver
will never finish (streams and legacy queues), as
`consensus.controller.attention`. Two guarded operator overrides resolve them:

- `fibrilctl admin recovery withdraw` removes a pending recovery that has no
  recorded plan. It is refused for queues with an accepted history, because
  their replicas may already be sealed for the request.
- `fibrilctl admin recovery accept-owner-loss --confirm-data-loss` records an
  acceptance bound to the exact owner and epoch. The next move spends it, and it
  is cleared if the owner serves again or the assignment changes. It is refused
  for accepted histories and while a recovery is pending.

Details and refusal rules are in
[a recovery or held owner needs an operator](/reliability/failure-modes/#a-recovery-or-held-owner-needs-an-operator).

## Eager failover and controller wakeups

By default a peer leaves placement when its broker heartbeat expires. Eager
failover is an opt-in runtime setting (`replication.eager_failover`, off by
default) that lets the active controller exclude a peer after repeated explicit
Raft connection failures that span `replication.eager_failover_grace_ms`
(default 1000, range 0 to 60000). A grace of zero also turns on idle Raft
connection monitoring on the active controller. Exclusion only starts the normal
placement and recovery path. See [eager failover](/reliability/replication/#eager-failover).

The controller loop wakes on committed snapshot changes, on transport failure
reports while eager failover is on, on its tick, and on Ganglion's
`watch_leader`. The leader wake matters when the killed node also led Raft: the
new leader acts on failures it saw while electing instead of sleeping up to a
full tick. See the
[controller wake note](/development/engineering-notes/#controller-wake-on-leadership).

## Restarted followers rejoin as learners

A follower process restart on a queue with W >= 2, while the owner keeps
running, requests no recovery. The accepted history marks the follower
suspended, and it rejoins through learner admission while the queue keeps
serving. Owner restarts, stream replica restarts and W = 1 queues still go
through recovery. See
[restarted queue followers](/reliability/recovery-sealing/#restarted-queue-followers).

## Process identity is published once per start

Each broker carries its process identity in the `fibril/history-process`
heartbeat label. Label-only node updates are version-silent in Ganglion: they
neither bump the snapshot generation nor wake watchers, so heartbeats stay cheap.
A restarted process's new identity could therefore stay invisible to watched
snapshots until some unrelated write. Histories built from such a stale view
would suspend the replica with nothing to re-admit it. So the first registration
of each process also writes the `fibril/node-process/<node>` attribute, which
bumps the generation and publishes a snapshot that carries the new label.

## Load data is never authority

Two load signals are kept separate and advisory:

- Node load (how good a broker is as an owner/follower candidate) belongs on the
  coordination heartbeat path as a compact report, not as high-frequency durable
  state. The controller writes durable assignment decisions, not every transient
  sample.
- Partition load (which partition keyless traffic should prefer) is a routing
  hint, partition-aware at the metrics surface.

Both are future direction beyond the current spread-first placement. The
correctness rule is fixed: missing or stale load data falls back to existing
placement and routing, and epochs, fences, and gates remain authoritative.

## Member identity is scoped to the trusted-client model

Cohort member ids are validated with a local guard: a malformed id is rejected and
one member identity is enforced per connection per broker (reconnect-safe). This
is sufficient under the current model, where broker connections have optional auth
and a client uses one connection per broker with a single shared member id. When
the broker port is exposed to untrusted or multi-tenant clients, this needs a
cluster-issued signed identity token so one tenant cannot spoof another's member
id. The per-partition gate still keeps delivery correct in the meantime.

## Local playgrounds

Two single-process playgrounds exercise consensus and assignment without
starting brokers.

The raft playground lives in the Ganglion repo and runs a durable openraft
cluster in one process:

```bash
scripts/cluster-playground.sh                  # 3 durable nodes, interactive
scripts/cluster-playground.sh --script "status; write 1; kill 2; status; quit"
```

Its commands are `status`, `write <gen>`, `kill <id>`, `restart <id>`, `add <id>`,
`remove <id>` and `quit`. Killing the leader shows re-election, and restarting a
node after a few hundred writes shows recovery loading the snapshot and replaying
only the short log tail.

The coordination playground in this repo runs three Fibril coordination
providers sharing one raft group, with the leader acting as controller:

```bash
scripts/coordination-playground.sh --script \
  "status; assign orders 2; kill broker-1; assign orders 2; status; quit"
```

In that run, the partition owned by `broker-1` moves and its epoch goes from 1
to 2, while the other partition keeps its owner and epoch, because only an owner
change fences.

## Large local clusters

`scripts/cluster-tryout.sh --nodes N --ganglion --summary --resource-summary`
starts N broker processes that are all raft voters and prints process count,
RSS and file descriptors before shutting down. Local runs of 15, 21 and 88 nodes
passed with stable leader agreement, queue declaration and runtime settings sync.
The 88-node run used about 6.5 GiB RSS across 88 brokers under
`ulimit -n 2048` (one workstation, 2026). That is a stress test, not a target
shape: a large cluster should keep a small voting set and run the other brokers
as registered members.

## See also

- [Clustering](/concepts/clustering/) and [consumer groups](/concepts/consumer-groups/) for the user-facing model.
- [Replication design](/development/replication-design/) for the durability side of coordination.

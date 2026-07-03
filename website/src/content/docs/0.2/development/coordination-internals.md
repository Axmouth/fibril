---
title: Coordination internals
description: Development notes on cohort assignment, generation fencing,
  placement, and why load data is never authority.
slug: 0.2/development/coordination-internals
---

This is a development note. User-facing behavior lives in
[clustering](/0.2/concepts/clustering/) and
[consumer groups](/0.2/concepts/consumer-groups/). This page records how the
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

* Per-cohort, not one shared counter. Each cohort document is its own
  authoritative version. A single cluster-wide counter would force re-stamping
  every cohort whenever any one changed, for no correctness gain.
* Durable in the document, not an in-memory counter. The generation is read back
  from the committed attribute before each publish, so it stays monotonic across a
  controller leader change.
* Owners fence stale plans. An owner ignores any plan older than the one it holds,
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

## Load data is never authority

Two load signals are kept separate and advisory:

* Node load (how good a broker is as an owner/follower candidate) belongs on the
  coordination heartbeat path as a compact report, not as high-frequency durable
  state. The controller writes durable assignment decisions, not every transient
  sample.
* Partition load (which partition keyless traffic should prefer) is a routing
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

## See also

* [Clustering](/0.2/concepts/clustering/) and [consumer groups](/0.2/concepts/consumer-groups/) for the user-facing model.
* [Replication design](/0.2/development/replication-design/) for the durability side of coordination.

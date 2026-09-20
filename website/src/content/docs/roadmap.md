---
title: Roadmap
description: Remaining work and acceptance criteria for Fibril.
---

Fibril's priorities are recovery safety, complete client semantics, compatibility,
and cluster operations. This page tracks remaining work. Current capabilities
and their limits are documented in [implemented surface](/implemented-surface/);
[project status](/status/) summarizes their maturity.

## Path to 1.0

The 1.0 acceptance criteria cover four areas:

1. **Cluster confidence:** replication, recovery and failover withstand
   deterministic fault tests, sustained chaos/soak runs, and multi-node operation.
2. **Compatibility:** stable client APIs, versioned wire and storage formats,
   a written support policy, and automated compatibility checks.
3. **Operational lifecycle:** predictable drain, restart, reconnect and upgrade
   behavior, with typed client outcomes and failure-recovery runbooks.
4. **Security:** authenticated and encrypted client, admin and inter-broker
   connections, with documented trust configuration and credential lifecycle.

The remaining work is grouped below by dependency and operational impact.

## Recovery safety

- Implement and validate Windows durable metadata replacement before enabling
  checkpoint installation and recovery seals on that platform.
- Require promotion to preserve the previously confirmed history, using current
  replica evidence and assignment fencing. Cover stale heartbeat tails and a
  lagging, internally consistent candidate that lacks a confirmed batch. See the
  [failover plan](/development/failover-plan/).
- Exercise failover during checkpoint catch-up through real brokers, and extend
  local error/cancellation/process-kill coverage to power-loss persistence tests.
- Define source authority, acknowledged-history preservation, bounded retries
  and operator alerts before enabling automatic repair of divergent replicas.

## Client lifecycle and compatibility

Complete lifecycle semantics before freezing the public APIs:

- Define stream `fail`, `retry` and delayed-retry behavior, including typed
  rejection of operations the stream cursor model cannot support.
- Ensure a resumed stream's cursor ACK waits for re-subscription or is retained
  for retry until the subscription is ready.
- Add focused coverage for automatic recreation continuity and fuller
  multi-broker exclusive-consumer-group scenarios.

The compatibility work follows this order:

1. Finish the Offset/Epoch and Topic/Group type review, including the proposed
   `Arc<str>` representation, while preserving existing wire bytes.
2. Define wire and durable-format versioning, migration and support policies.
3. Review and freeze the Rust, TypeScript, Python, Go and C# client APIs.
4. Enforce the policies with vector regeneration checks, baseline-client/new-broker
   tests, mixed-version rolling upgrades and storage fixtures. Select explicit
   supported baselines and expand fixture coverage to coordination storage, users
   and runtime settings.

## Cluster operations

- Add an opt-in eager failover policy through cluster runtime settings, after
  the promotion-safety gate is complete. Use explicit peer-failure signals,
  bounded probing and grace, with heartbeat expiry as fallback; measure recovery
  time, false reassignments and disruption under the
  [failover acceptance scenarios](/development/failover-plan/#acceptance-scenarios).
- Provide one-command node enrollment with short-lived invitations, trust
  verification and configuration exchange.
- Aggregate replication lag and in-sync status across brokers in the CLI and
  topology view.
- Add coordinated queue deletion, replicated purge, and node scale-up/scale-down
  workflows with explicit failure outcomes.
- Narrow exclusive-consumer subscriptions to their assigned partition subset.
- Define replicated policy for cluster-wide runtime locks while retaining local
  hardware settings such as storage paths and log tuning.
- Expand deployment, failover and rolling-upgrade runbooks.

## Performance and observability

- Profile client scheduling, decoding and delivery costs in Python and TypeScript.
- Track publish and delivery capacity, latency and memory across payload sizes,
  storage devices, partition counts and replication policies.
- Improve memory use under large backlogs, high inflight load and many idle queues.
- Improve bulk DLQ replay, message inspection and sparse-queue diagnostics.
- Extend the shared benchmark harness with stream workloads, portable cluster/fault
  provisioning and repeated workload matrices.
- Assess sending immutable staged batches to followers before owner fsync, after
  promotion can distinguish and preserve committed history. Keep local durability
  and complete replica dependencies as confirmation requirements, with bounded
  buffering and recovery rules for tentative suffixes.
- Add OpenTelemetry export.

## Longer-term options

These require a concrete use case or further design:

- Reclaim inflight ownership across broker restart and transfer sessions across
  nodes, with a defined startup grace window and redelivery policy.
- Per-topic authorization and tenancy controls.
- Ordering guarantees across a partitioned queue: compare per-key and queue-wide
  ordering, delivery versus processing order, retries and repartitioning, and the
  cost of sequencing or merging across partitions.
- Additional clients, such as Java.
- Richer bounded stream filters and client-side wildcard publishing.
- Queue expiration based on coordinated inactivity.

## Out of scope

Transactions and transactional publish/consume workflows are outside the broker's
scope. Content-routing scripts and SQL/stream processing belong in an external
layer that uses the broker's publish and subscribe APIs.

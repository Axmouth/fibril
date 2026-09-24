---
title: Roadmap
description: Remaining work and acceptance criteria for Fibril.
---

Fibril's priorities are recovery safety, complete client semantics, compatibility,
and cluster operations. This page tracks remaining work. Current capabilities
and their limits are documented in [implemented surface](/implemented-surface/);
[project status](/status/) summarizes their maturity.

## Immediate rounds

The next rounds prioritize bounded operational work, recovery latency and adoption
of the retained delivery experiment. Client and stream work can proceed as
separate rounds when their dependencies permit.

1. **Admin settings and recovery visibility.** Unify defaults/validation metadata
   and extend requested-versus-applied reporting beyond node-local preallocation.
   Add explicit remote-node edit routing and classify additional live storage
   controls. Show recovery stages and account for retained generations and staging
   data before adding automatic reclamation.
2. **Checkpoint policy and recovery overhead.** Measure the opt-in agreed
   checkpoint path under higher sustained rates and many partitions. Tune the
   implemented event/byte/age triggers, verification budgets and retention limits;
   extend diagnostics to physical retained bytes and per-stage costs. Evaluate
   remaining live-payload verification costs and silent-endpoint discovery across
   SDKs; explicit Rust EOF handling and per-replica seal/inspection overlap are implemented. See the
   [fast-recovery plan](/development/failover-plan/#fast-recovery).
3. **Speculative delivery adoption.** Reconcile the retained local-queue prototype
   with current recovery and ordered application. Close ACK-before-durability,
   crash/error, slow-consumer, memory-budget and expiry/fallback gaps, then repeat
   representative physical-storage measurements. Production configuration and
   confirmation/identity contracts are required before adoption. Replicated
   speculation and replication before owner fsync remain separate increments.
4. **Python and TypeScript performance.** Profile scheduling, decoding, delivery
   and flushing against shared broker workloads. Apply useful transport changes
   across SDKs where the same mechanism applies, retaining consistent semantics.
5. **Streams completeness.** Resolve fail/retry and reconnect-ACK behavior across
   clients, then add stream and consumer fan-out workloads to the shared harness.
6. **Many-queue resource use.** Measure adaptive staging with idle, intermittently
   active and busy queues under realistic memory pressure. Track retained recovery
   disk separately from active logs and assess reclamation only after visibility.

A smaller optional round can add a coherent moving dashboard scenario: backlog
builds, consumers catch up and a node recovers. Reuse production views and clearly
label the simulated state. Allocator/THP repeats remain evidence-driven follow-ups.

## Medium-term directions

These four directions follow the immediate rounds. They are exploration and
product targets, with implementation scope and acceptance refined individually.

### Ordering across partitioned queues

Explore useful per-key ordering while distributing customers or jobs across
partitions. Specify delivery versus processing order, retries, consumer ownership
and repartitioning. Evaluate queue-wide ordering separately, including its
sequencing and throughput costs.

### Scaling across partitions, nodes and drives

Map aggregate and per-partition capacity with replica count as an independent
axis. Spread owners and storage across nodes and physical drives, verify client
headroom, and measure latency, CPU, memory and durable behavior at offered rates
and saturation. Explore a million messages per second as a measurement target;
report sustainable operating points and exact hardware/configuration conditions.

### Low-latency service routing

Build a representative request → worker → response example with correlation,
deadlines, retries and idempotency. Define worker completion, response durability
and request acknowledgment ordering, including duplicate responses after failure.
Measure both queue legs and full round-trip latency. Integrate speculative
delivery only under its validated contract; pre-staging delivery and write
avoidance remain a further design requiring explicit ordering and crash semantics.

### Public/private endpoints and routing fallback

Design public entry brokers that can forward to owners on private endpoints,
while retaining direct owner routing where reachable. Define advertisement scope,
authentication, failure/retry outcomes, loop prevention and bounded forwarding.
Overlapping topology groups and arbitrary multi-hop routing remain later options
that require a concrete deployment use case.

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
- Extend accepted-history recovery coverage to packet-level asymmetric partitions,
  whole-process interruption during learner transfer and candidate replacement
  outside the fixed proposed replica set. Preserve confirmed history and fencing
  through every transition; see the [failover plan](/development/failover-plan/).
- Extend local error/cancellation/process-kill coverage to power-loss persistence
  tests, larger records/snapshots and authoritative stream-state recovery.
- Design composite reconstruction for crossed payload/event histories and safe
  reclamation of retained recovery data. Keep unresolved authoritative divergence
  fenced and visible to operators.

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

- Unify configuration defaults and validation metadata across startup seeds, the
  runtime API and the dashboard. Report requested versus locally applied settings,
  node/cluster scope and application boundaries; complete safe startup visibility
  for coordination and listener configuration.
- Extend node-local storage settings with explicit remote-node routing and classify
  fsync, batching and adaptive-buffer controls by their safe application boundary.
  Preserve in-flight durability requirements. Extend failure acceptance to full-disk
  persistence and process interruption during updates; report applied versions for
  settings beyond segment preallocation.
- Assess live updates for coordination heartbeat interval and liveness TTL,
  with coupled validation, safe adoption across nodes, controller propagation,
  admin visibility and rollback. Keep Raft election timing separate.
- Extend recovery acceptance to sustained high-rate and aged workloads, distinguishing
  retained history, live backlog and checkpoint age. Extend the internal matched
  RabbitMQ/JetStream history checks to more sustained traffic and failure modes.
  Add safe reclamation of retained recovery generations and measure shared-segment
  repair costs under storage pressure.
- Target common-case queue recovery within two seconds: measure release-build
  stage costs, remove avoidable waits, and tune agreed checkpoints with
  suffix-only comparison while preserving payload dependencies and quorum proof. See the
  [staged fast-recovery plan](/development/failover-plan/#fast-recovery).
- Extend eager failover acceptance to packet-level partitions, CPU/storage stalls,
  planned drains and durable streams. Measure detection, recovery time, false
  reassignments and healthy traffic disruption under the
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

## Dashboard demo and interactive documentation

Pin matching demo assets when archiving documentation versions. Extend inline
examples to additional dashboard pages where they clarify operational behavior.
Assess coherent moving scenarios for backlog growth, slow consumers and recovery.

Add approachable topology and sequence diagrams directly to the relevant concept,
replication and recovery pages. Cover partition placement and owners, metadata
leadership and controller planning, ordinary versus early replication, polling
versus push replication, local and replicated speculative delivery, and the full
failover/checkpoint recovery sequence. Distinguish eager failure detection from
early replication, and label implemented, experimental and proposed behavior.
Show delivery, consumer ACK, local/replica persistence and publisher-confirmation
boundaries explicitly. Pair overview diagrams with detailed recovery steps and
failure branches. Maintain shared diagram sources and visual conventions, with
readable static fallbacks and vector exports suitable for presentations; version
diagrams alongside the behavior documented on each page.

## Longer-term options

These require a concrete use case or further design:

- Reclaim inflight ownership across broker restart and transfer sessions across
  nodes, with a defined startup grace window and redelivery policy.
- Per-topic authorization and tenancy controls.
- Additional clients, such as Java.
- Richer bounded stream filters and client-side wildcard publishing.
- Queue expiration based on coordinated inactivity.

## Out of scope

Transactions and transactional publish/consume workflows are outside the broker's
scope. Content-routing scripts and SQL/stream processing belong in an external
layer that uses the broker's publish and subscribe APIs.

# Replication And Sharding Transition Safety Audit

This audit reviews the safety of moving a queue partition between owner,
follower, and frozen roles across Stroma, the broker, the protocol transport,
Ganglion-backed coordination, and clients.

Source context:

- [REPLICATION_WORKLOG.md](REPLICATION_WORKLOG.md)
- [REPLICATION_PLANNING.md](REPLICATION_PLANNING.md)
- Stroma role, freeze, ingest, checkpoint, and promotion APIs
- broker assignment watcher, follower worker, ownership gates, and topology
  routing
- Ganglion coordination provider, catalogue, controller, and topology surfaces

## Goal

Role changes should avoid becoming ordinary user-visible errors in the common
path. The coordination layer should stop or redirect new traffic first, the
broker should transition its local loops, Stroma should fence storage behavior,
and Keratin should reject stale epochs if anything races through anyway.

Errors still matter as hard backstops. They should be rare during a correct
transition, but they must remain explicit when stale owners, old clients, or
faulty coordination try to write after a role change.

## Current Safety Model

The current model is layered in the right direction.

- Ganglion is the current embedded coordination path. Older etcd-shaped notes
  remain historical grounding, but current implementation work routes through
  Ganglion-backed snapshots, watches, controller ticks, runtime settings, queue
  catalogue entries, and topology views.
- Coordination owns assignment intent: owner, followers, epoch, queue catalogue,
  partition count, and controller decisions.
- The broker owns routing and runtime loops. It rejects unowned publish and
  subscribe setup before queue materialization, emits not-owner redirects where
  topology is known, and applies assignment changes through local transition
  intents.
- The broker stops owner runtime before demotion or freeze, releases
  broker-tracked delivery offsets durably, closes stale owner handles, and starts
  follower workers only after the local role transition has succeeded.
- Stroma owns queue-local role enforcement. Owner operations acquire leases,
  freeze waits for accepted work to drain, follower ingest uses replicated
  append paths, promotion checks applied state and log tails, and demotion
  records local tails before switching to follower.
- Keratin stays generic. It provides caller-assigned append, checkpoint/reset
  primitives, local log roles, and epoch fencing without learning Fibril
  topology or broker ownership.
- Follower workers pull from owners over protocol, reuse owner-peer
  connections, handle checkpoint-required boundaries, and treat not-owner from
  the owner as a topology-change signal rather than a normal retry.
- Clients are no longer single-owner only. The Rust client has topology fetch,
  redirect handling, pooled owner connections, partition routing, topology warm,
  and subscription fan-in across partitions.

This is a substantially stronger state than the early replication plan assumed.

## Findings

### 1. Stroma Role Transitions Are Strong

Status: Addressed

Stroma has the right local primitives for safe transition:

- owner-operation leases cover accepted work
- freeze blocks new owner operations and waits for leases to drain
- follower ingest accepts replicated records but rejects public owner traffic
- checked promotion verifies message tail, event tail, and applied event state
- owner demotion freezes, drains, records tails, and changes queue plus log role
- local-tail failover promotion is fenced by epoch

Coverage exists for durable publish paths, ack/NACK paths, DLQ continuation,
follower ingest, checkpoint export/install, promotion refusal, promotion success,
and demotion behavior.

Follow-up:

- Add more adversarial role-change tests once the scenario harness exists,
  especially transition during active protocol catch-up and during checkpoint
  install.
- Split Stroma modules after behavior settles. This is maintainability work, not
  a correctness blocker.

### 2. Broker Loop Transition Is No Longer Just A Role Flag

Status: Addressed

The broker now accounts for the fact that changing Stroma role is not enough.
Owner runtime is canceled and removed before demotion or freeze, broker-tracked
delivery offsets are released through Stroma, stale owner handles close, and
follower runtime starts after the local follower role is installed.

StopFollower also drains an active follower tick before freezing follower state,
which avoids racing a mid-batch replicated ingest.

Follow-up:

- Keep broker loop transition tests close to real public behavior. The
  correctness concern is not just "role became follower", but "new owner traffic
  stopped, accepted work resolved, and stale handles cannot keep writing".
- Add a regression where StopFollower lands while a catch-up tick is active and
  prove freeze waits for the tick boundary.

### 3. Protocol Pull Replication Has Real Coverage

Status: Addressed

The protocol surface covers ordinary contiguous catch-up and checkpoint-required
catch-up. A follower can pull message and event records from an owner, apply
them, install an owner-exported state checkpoint when policy allows, resume
catch-up, and pass checked promotion.

The owner peer boundary is also the right shape. Follower workers depend on a
small `BrokerOwnerReplicationPeer` trait instead of concrete broker internals,
and the coordination-backed resolver maps assignment snapshots to protocol
peers.

Follow-up:

- Add longer-running supervised watcher coverage with real topology changes, not
  only run-once or protocol proof tests.
- Add harsher transport tests: dropped connections, owner endpoint changes,
  not-owner after cached peer reuse, and checkpoint transfer interruption.

### 4. Ganglion Coordination Is The Current Cluster Path

Status: Addressed

Ganglion now provides the practical coordination path: node registration,
catalogue resources, assignment snapshots, guarded attribute writes, runtime
settings replication, topology API data, controller ticks, queue partitioning,
and cohort assignment documents.

This means the old "use etcd first" plan is no longer the active short-term
path. Etcd can remain a possible backend behind the coordination trait, but the
implementation and audit should treat Ganglion as the current source of truth.

Follow-up:

- Keep Ganglion generic and keep Fibril domain behavior in the adapter or broker.
- Do not rewrite historical planning notes just to remove etcd references. New
  work should be appended as the current direction.

### 5. Failover Core Exists And Has A Passing Live Smoke

Status: Addressed

Automatic failover is no longer merely theoretical. The worklog records
TTL-driven owner loss, epoch-bumped reassignment, progress-aware candidate
selection, follower drain, local-tail promotion, stale-owner demotion, and
cluster tryout coverage.

The 3-node Ganglion tryout now covers the key public scenario: replica-durable
pre-failover publish confirms, owner kill, follower promotion, consumption of
pre-failover payloads through the new owner, and a post-failover publish/consume.

Two important safety fixes landed from that smoke:

- Follower catch-up progress is derived from local replicated append outcomes,
  not from owner-read offsets alone.
- Replicated events are not appended or applied in memory when the paired
  replicated message batch is rejected.
- A cold owner applies its cached assignment epoch during first lazy
  materialization.
- An overlapping stale local follower prefix is repaired through owner
  checkpoint install instead of retried blindly.

The remaining risk is production hardening, not basic architecture.

Open follow-up:

- promotion-refusal retry and escalation policy
- generation races during network partitions
- owner returns during failover
- follower restart during checkpoint transfer
- stale owner publish after fence across a real multi-node setup
- clear operator state when failover refuses to promote

### 6. Publish-Confirm Durability Is Mostly Implemented

Status: Addressed

Replica-durable confirm gating, follower durable progress, min-in-sync refusal,
ISR freshness, timeout errors, and two-broker wire end-to-end coverage are
already present according to the worklog.

This materially reduces the gap between "replication works" and "confirmed data
has an understandable durability contract".

Follow-up:

- Surface cross-broker lag and ISR state in topology and admin views.
- Consider per-topic overrides only after the current cluster-wide behavior is
  operationally clear.

### 7. Sharding And Client Topology Are Much Further Along Than Early Plans

Status: Addressed

The Rust client has topology awareness, redirect handling, pooled owner
connections, routing by partition, partitioning-version fencing, connect-time
topology warm, and subscription fan-in. Server-side publish and subscribe paths
also carry partition data.

This changes the estimate for the overall feature. Sharding is no longer mostly
future work.

Open follow-up:

- live repartitioning
- optional client narrowing for exclusive cohorts
- TypeScript client parity
- more redirect and topology fault tests
- operator visibility for partition placement and lag

### 8. Exclusive Cohorts Are Correctness-Safe, But Need The Multi-Node E2E

Status: Audited

The cohort gate is the correctness backstop, so the global plan can be advisory
and eventually consistent. Cross-broker coordinator plumbing is largely wired:
member identity, membership labels, plan computation, owner apply path,
assignment documents, provider methods, controller tick, heartbeat label wiring,
and owner watcher wiring.

The remaining blocker is testability. The multi-node coordinator e2e needs the
server bootstrap extracted from `server.rs` into reusable library code.

Open follow-up:

- extract cluster bootstrap enough to start two brokers in a test
- prove a cohort spanning owners is globally balanced
- prove consumer drop rebalances through the controller path
- later, add TypeScript cohort API parity

### 9. Operator Visibility Is Partial

Status: Audited

The system has admin topology API/page, CLI topology, sparse queue visibility,
replication follower reports, runtime settings, and queue inspection. This is
good enough for development and early tryouts.

It is not yet enough for confident incident operations.

Open follow-up:

- cross-broker lag aggregation
- ISR and replica progress in topology
- promotion refusal state
- controller status and last error surfaced clearly
- node management and fence/drain controls
- topology diagram enrichment
- runbooks for manual recovery and refused promotion

### 10. Multi-Node And Adversarial Testing Are The Main Gaps

Status: Audited

The code has many focused tests, protocol proofs, storage tests, and tryout
scripts. The remaining confidence gap is around scenarios that combine real
server bootstrap, real coordination, real protocol transport, and failure.

Addressed in this pass:

- live 3-node replica-durable owner-kill smoke
- stale append must not record ready state
- rejected replicated message append must not apply corresponding event state
- cold owner materialization must fence logs to the assignment epoch
- checkpoint-aware catch-up must repair an overlapping stale local prefix

Priority tests:

- multi-node cohort coordinator e2e
- owner death and return
- partition during failover
- cached owner endpoint changes while follower worker is running
- checkpoint-required catch-up interrupted and resumed
- stale topology redirect loop bounded by client settings
- publish-confirm timeout and min-in-sync refusal visible to the caller

## Recommendation

The next high-leverage implementation work is not a new storage primitive. It is
testability and operational hardening:

1. Extract cluster bootstrap from `server.rs` enough for integration tests.
2. Add the multi-node cohort coordinator e2e.
3. Add failover adversarial scenarios around stale owners, owner return, and
   promotion refusal.
4. Add cross-broker lag and ISR visibility to topology/admin surfaces.
5. Only then do larger cleanup passes such as module splitting and helper
   dedupe.

The current architecture is coherent. The main risk is untested composition
under real multi-node timing, not a missing conceptual layer.

## Coverage Decision For This Pass

No code regression was added in this audit pass. The audit changed the
classification of current risk after reviewing newer worklog entries and should
not pretend to verify behavior by adding a narrow test unrelated to the next
real blocker.

The next behavior change should include tests at the integration harness level,
because that is where the remaining transition risks live.

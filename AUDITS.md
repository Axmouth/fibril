# Fibril Audit Index

This is the root checklist for focused audits. Detailed findings should live in
their own audit documents so this file stays useful as a status board. The
detailed audit documents from the replication effort now live under
`archive/replication-sharding-plan/` (linked below). Open `Next:` items are still
worth a fresh pass.

## Status Meanings

| Status | Meaning |
| --- | --- |
| Pending | Worth auditing, but no focused pass has been recorded yet. |
| Audited | Findings are documented, but at least one item remains open. |
| Addressing | Fixes are actively being implemented. |
| Addressed | Findings are fixed, tested, and the audit document says so. |
| Deferred | Known issue or idea, intentionally not being worked now. |

## Checklist

- [ ] Hot-path performance (delivery, ack, publish)
  - Status: Addressing
  - Detail: [PERF_AUDIT_HOT_PATHS.md](PERF_AUDIT_HOT_PATHS.md)
  - Next: remaining keratin findings (reader fd cache, poll retries fast
    path, apply-completion clone) and the serial replica-confirm gate wait
    for cluster benchmarks. Fibril A, B, and C1 plus keratin K1
    (self-clocking group commit) are landed and measured, C2 was assessed
    low yield.
- [x] Plexus stream hot-path performance
  - Status: Addressed (no server-side knee found)
  - Detail: S section of [PERF_AUDIT_HOT_PATHS.md](PERF_AUDIT_HOT_PATHS.md).
    Headline: the apparent r16/r32 collapse was the single bench client
    process. Split across processes the server fans out 3.2M records/s (1KB,
    32 subscribers) at p50 1ms on ~7 cores, tiers indistinguishable. SF1
    (chain batching) was implemented, measured neutral-to-slightly-negative,
    and parked on branch perf-stream-batching. SF2-SF4 stay as deprioritized
    efficiency notes.
  - Next: the correctness spin-offs live elsewhere: lagged gap-skip (P3) and
    the concurrent-declare race in the task tracker.
- [x] Stream operational parity with queues
  - Status: Addressed
  - Detail: [STREAM_PARITY_AUDIT.md](STREAM_PARITY_AUDIT.md). All five gaps
    closed: lag gap-skip contiguity fix, idle eviction with an ephemeral
    flush dirty-gate, stream traffic in BrokerStats, shutdown cursor flush,
    and lag observability on the admin streams page.
  - Next: lag notification client events and lag policies ride the typed
    subscription-lifecycle surface (API freeze family).
- [x] Runtime settings through Ganglion
  - Status: Addressed
  - Detail: [RUNTIME_SETTINGS_GANGLION_AUDIT.md](archive/replication-sharding-plan/RUNTIME_SETTINGS_GANGLION_AUDIT.md)
  - Next: keep follow-up policy questions in the deferred immutable runtime
    policy item.
- [ ] Cluster-wide immutable runtime policy
  - Status: Deferred
  - Detail: not started
  - Next: decide whether Fibril needs replicated locks or policy for selected
    cluster runtime settings. Keep hardware-shaped startup settings local.
- [x] Single-node behavior
  - Status: Addressed
  - Detail: [SINGLE_NODE_GUARDRAIL_AUDIT.md](archive/replication-sharding-plan/SINGLE_NODE_GUARDRAIL_AUDIT.md)
  - Next: add a broader single-node scenario runner with the end-to-end harness.
- [ ] Replication and sharding transition safety
  - Status: Audited
  - Detail: [REPLICATION_TRANSITION_SAFETY_AUDIT.md](archive/replication-sharding-plan/REPLICATION_TRANSITION_SAFETY_AUDIT.md)
  - Next: extract cluster bootstrap for multi-node integration tests, then cover
    cohort coordination and failover adversarial scenarios.
- [ ] Ganglion integration boundary
  - Status: Audited
  - Detail: [GANGLION_INTEGRATION_BOUNDARY_AUDIT.md](archive/replication-sharding-plan/GANGLION_INTEGRATION_BOUNDARY_AUDIT.md)
  - Next: defer dedupe until a helper is clearly domain-neutral and reused.
- [ ] Admin operations surface
  - Status: Audited
  - Detail: [ADMIN_OPERATIONS_SURFACE_AUDIT.md](archive/replication-sharding-plan/ADMIN_OPERATIONS_SURFACE_AUDIT.md)
  - Next: make message inspection status filtering explicit, either with a
    bounded admin-side scan-fill loop or a response/UI note that filtering
    happens after a bounded page read.
- [ ] Client API parity
  - Status: Pending
  - Detail: not started
  - Next: audit Rust and TypeScript clients against the current protocol and docs.
- [ ] Performance-sensitive paths
  - Status: Audited
  - Detail: [PERFORMANCE_SENSITIVE_PATHS_AUDIT.md](archive/replication-sharding-plan/PERFORMANCE_SENSITIVE_PATHS_AUDIT.md)
  - Next: add cluster benchmark profiles for replica-durable confirms,
    follower catch-up, partitioned fan-in, and redirects.
- [ ] Lock usage and synchronization
  - Status: Audited
  - Detail: [LOCK_USAGE_AUDIT.md](archive/replication-sharding-plan/LOCK_USAGE_AUDIT.md)
  - Next: replace replica-durable per-offset follower-map checks with committed
    watermark progress, then add the concurrent duplicate-subscribe regression.
- [ ] User-facing documentation shape
  - Status: Audited
  - Detail: [USER_FACING_DOCUMENTATION_SHAPE_AUDIT.md](archive/replication-sharding-plan/USER_FACING_DOCUMENTATION_SHAPE_AUDIT.md)
  - Next: add a clustered queues and replication explainer, then keep TypeScript
    examples scoped to the single-owner API until the parity pass lands.
- [ ] Pipeline serialization (work inside serialized contexts)
  - Status: Audited
  - Detail: [PIPELINE_AUDIT.md](PIPELINE_AUDIT.md)
  - Next: ranked findings pending the contention-probe flamegraph; top
    candidates are actor drain-batching + delivery read starvation (D2),
    global confirm-path atomics (P1), and slow-consumer HOL blocking (D1).
- [x] Broker memory after queue eviction
  - Status: Addressed (no leak found)
  - Detail: [MEMORY_AUDIT.md](MEMORY_AUDIT.md)
  - Next: low-priority follow-up recorded there - identify the ~1 MB
    retained per materialized queue-partition if a large-queue-count
    deployment ever appears. Regression probe: scripts/memory-audit.sh.

## Single-Node Guardrail

Every clustered feature should preserve the standalone broker path.

- Ganglion should be optional unless clustered mode is explicitly enabled.
- Local runtime settings remain authoritative in standalone mode.
- Admin settings should not require a coordination leader in standalone mode.
- Single-node tests should cover the same public API paths changed for cluster
  behavior.
- Cluster-only failure states should not leak into normal single-node operation.

If a fix for a clustered path risks changing standalone behavior, add or update a
single-node regression test in the same change.

# Fibril Audit Index

This is the root checklist for focused audits. Detailed findings should live in
their own audit documents so this file stays useful as a status board.

## Status Meanings

| Status | Meaning |
| --- | --- |
| Pending | Worth auditing, but no focused pass has been recorded yet. |
| Audited | Findings are documented, but at least one item remains open. |
| Addressing | Fixes are actively being implemented. |
| Addressed | Findings are fixed, tested, and the audit document says so. |
| Deferred | Known issue or idea, intentionally not being worked now. |

## Checklist

- [x] Runtime settings through Ganglion
  - Status: Addressed
  - Detail: [RUNTIME_SETTINGS_GANGLION_AUDIT.md](RUNTIME_SETTINGS_GANGLION_AUDIT.md)
  - Next: keep follow-up policy questions in the deferred immutable runtime
    policy item.
- [ ] Cluster-wide immutable runtime policy
  - Status: Deferred
  - Detail: not started
  - Next: decide whether Fibril needs replicated locks or policy for selected
    cluster runtime settings. Keep hardware-shaped startup settings local.
- [x] Single-node behavior
  - Status: Addressed
  - Detail: [SINGLE_NODE_GUARDRAIL_AUDIT.md](SINGLE_NODE_GUARDRAIL_AUDIT.md)
  - Next: add a broader single-node scenario runner with the end-to-end harness.
- [ ] Replication and sharding transition safety
  - Status: Audited
  - Detail: [REPLICATION_TRANSITION_SAFETY_AUDIT.md](REPLICATION_TRANSITION_SAFETY_AUDIT.md)
  - Next: extract cluster bootstrap for multi-node integration tests, then cover
    cohort coordination and failover adversarial scenarios.
- [ ] Ganglion integration boundary
  - Status: Audited
  - Detail: [GANGLION_INTEGRATION_BOUNDARY_AUDIT.md](GANGLION_INTEGRATION_BOUNDARY_AUDIT.md)
  - Next: defer dedupe until a helper is clearly domain-neutral and reused.
- [ ] Admin operations surface
  - Status: Pending
  - Detail: not started
  - Next: audit settings, auth, message inspection, DLQ replay, and cluster
    topology from an operator perspective.
- [ ] Client API parity
  - Status: Pending
  - Detail: not started
  - Next: audit Rust and TypeScript clients against the current protocol and docs.
- [ ] Performance-sensitive paths
  - Status: Pending
  - Detail: see the docs optimization log for existing notes
  - Next: audit publish, delivery, replication, and metadata paths with benchmarks
    before and after changes.
- [ ] User-facing documentation shape
  - Status: Pending
  - Detail: not started
  - Next: audit that user docs explain behavior and operations, while internals
    stay in development docs.

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

# Design Notes

Curated design decisions and their rationale, kept so the user-facing docs can
draw on them later. This is not a running log (that is REPLICATION_WORKLOG.md)
and not a point-in-time audit (those are the *_AUDIT.md files). Add an entry when
a decision has a "why" worth explaining to a reader, not just a "what".

## Cohorts and cross-broker coordination

### The gate is the correctness backstop, the global plan is advisory

Each partition has a delivery gate (`QueueLoopState.exclusive_assignee`, an
atomic) that admits at most one consumer. This is enforced locally on every
owner, always. The cross-broker assignment plan the controller computes and
publishes is therefore advisory and eventually-consistent: about one heartbeat
of lag affects balance only, never correctness. A departed member's partitions
pause until the next plan rather than ever being double-delivered. This split is
what lets the coordinator be simple (no global locks, no strict consensus on the
hot path).

### Cohort plan generation is per-cohort and durable

The published assignment document carries a `generation`. The controller bumps
it only when the assignment content actually changes, so re-publishing a stable
plan is a same-value no-op.

- PER-COHORT, not one shared counter. Each cohort document is its own
  authoritative version. A single cluster-wide counter would force re-stamping
  every cohort's document whenever any one cohort changed (write amplification)
  for no correctness gain. A single "cluster is at generation N" number, if ever
  wanted, is a thin max-over-documents view on top.
- DURABLE in the document, not an in-memory counter. The generation is read back
  from the committed attribute before each publish, so it stays monotonic across
  a controller leader change (a fresh leader does not reset it to zero).

### Owners fence stale plans by generation

An owner ignores any plan older than the one it already holds, so a late or
out-of-order slice never overwrites a newer one. An equal generation is still
re-resolved, because local subscriptions may have changed since the last apply
(a member finally subscribing to its assigned partition needs its gate set).
`exclusive_assignment_generation` exposes the applied version so convergence is
observable (an owner sitting on an older generation has not caught up yet).

### Known limitation: leader-change rebalance churn

A new leader's controller starts with empty sticky state, so the first plan it
computes after a leader change can differ from the prior plan even with the same
membership. That is one rebalance (the gate keeps delivery correct throughout).
Seeding the new controller from the published plan would remove the churn. This
is tracked with the cooperative-incremental-rebalance follow-up.

## Forwarded writes (coordination)

The deterministic path is the mechanism, retries are only a safety net. A
forwarded write goes to the known leader and follows explicit leader hints on
redirect (`client_write_remote_with_hint`). The blind "no leader known yet"
retry count (`no_leader_retries`) defaults to a small non-zero value so a freshly
started or churning standby waits briefly for its leader view to converge instead
of failing instantly. Tests run it at zero to prove the deterministic forward and
hint-redirect path carries the write without leaning on retry-spam.

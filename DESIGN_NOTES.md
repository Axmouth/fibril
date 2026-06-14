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

### Leader changes are generation-stable (controller seeding)

A new leader's controller would otherwise start with empty sticky state, so its
first plan could differ from the prior one even with unchanged membership,
causing a needless rebalance on every leader change. The controller seeds itself
from each cohort's published plan before planning, so it keeps the existing
assignment. With the per-cohort generation, this makes a leader change
generation-stable: the reconstructed plan matches what is published, so the
generation does not bump and owners see no change.

### Member-id validation is scoped to the trusted-client model

Cohort member ids are validated with a local guard: the broker rejects a
malformed id and enforces one member identity per connection per broker
(reconnect-safe). This is sufficient under the current trust model, where broker
connections have optional auth (a post-auth client is a trusted principal), the
client uses one connection per broker with a single shared member id, and v4
uuids make accidental cross-client collision negligible.

Reconnect is already safe without a cross-connection takeover: a normal reconnect
resumes the session (the server keeps the LogicalConnection and its subscriptions
and just reattaches the transport), so no member re-registration happens and
nothing is clobbered. The reconcile path (server lost state, client re-pushes
subs) also cannot clobber, since the old registration is already gone. The only
residual race is a client abandoning its session and opening a fresh connection
that re-subscribes with the same member id while the old session's cleanup is
still pending. There the broker's whole-member `leave` could drop the new
connection's registration. Impact is bounded to a transient pause (the gate keeps
delivery correct, never double-delivers), so this is left as a follow-up: make
`leave` sub_id-scoped so a stale connection's cleanup only removes its own
registration. That reworks `leave`'s whole-member failover atomicity, so it is
its own brick.

REVISIT when the broker port is exposed to untrusted or multi-tenant clients. A
malicious authenticated tenant could otherwise spoof another tenant's member id
to hijack delivery, which the local guard does not prevent. The next steps then
are a cluster-issued signed token (HMAC over the id with a shared cluster secret,
verified locally on any broker, no per-subscribe round-trip) or full
coordinator-issued identity.

## Forwarded writes (coordination)

The deterministic path is the mechanism, retries are only a safety net. A
forwarded write goes to the known leader and follows explicit leader hints on
redirect (`client_write_remote_with_hint`). The blind "no leader known yet"
retry count (`no_leader_retries`) defaults to a small non-zero value so a freshly
started or churning standby waits briefly for its leader view to converge instead
of failing instantly. Tests run it at zero to prove the deterministic forward and
hint-redirect path carries the write without leaning on retry-spam.

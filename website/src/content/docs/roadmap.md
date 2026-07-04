---
title: Roadmap
description: Near-term work for Fibril.
---

Fibril is still early. Treat this page as the current checkpoint for what has
landed recently and what is still pending. The near-term focus is making the
existing semantics complete, observable, and usable before adding larger
distributed-system features.

For the reverse view, use [implemented surface](/implemented-surface/) to
check what is already wired and under what conditions.

## Releases and the path to 1.0

Fibril follows SemVer. While on 0.x the public API and wire protocol are not
frozen and can change between minor releases. Versions move one minor at a time,
with no vanity jumps: the number tracks the stability promise, not perceived
completeness. Per-feature maturity lives in [project status](/status/).

These docs live at the site root and track main. When a version is cut the whole
documentation set is frozen under a versioned path (for example `/0.2/`), so an
adopter on a release reads the surface, the
[implemented surface](/implemented-surface/) inventory, and the
[project status](/status/) exactly as that release shipped them while the root
moves ahead. A version picker switches between the current docs and any frozen
release.

1.0 is not a quality badge, it is a commitment: a stable API and wire protocol
plus confidence in the durability and replication semantics. It ships only when
all four of these hold.

1. Cluster confidence. Replication and failover are proven by deterministic
   simulation testing, a green chaos and soak suite, and at least one real
   multi-node deployment run.
2. API and wire-protocol freeze. The protocol is versioned with a back-compat
   policy and client public APIs are stable, including the Offset/Topic newtype
   and `Arc<str>` pass.
3. Operational lifecycle. Graceful drain and durable restart reconciliation,
   with a failure-semantics runbook, so restarts and upgrades are first-class.
4. Security baseline. TLS in transit and auth/authz beyond the static handler.

### 0.1

Durable single-node queues and Plexus streams, the Rust, TypeScript, and Python
clients, the admin dashboard, and an experimental cluster path (coordination,
replication, failover, live repartitioning). Single-node features are Available,
the cluster path is Experimental, and the API and wire protocol may still change.

### 0.2

Cluster confidence and operational hardening - **gate 1 (cluster confidence) is
met**:

- Deterministic simulation testing (turmoil): a multi-broker harness covering
  election, replication, failover, split-brain, lossy networks, durability floor,
  checkpoint install, and repartition cutover.
- A chaos and soak suite (crash recovery and sustained-load no-loss) and a real
  multi-node failover validation run.
- Graceful drain announcement so a planned restart is transparent to in-flight
  work (gate 3), resolved reconnect grace-policy defaults, and configurable
  replication read/connect timeouts.
- The versioning and release process itself: a shared repo version, a changelog,
  per-repo release scripts with an overlord, and version-tagged images.

The cluster path stays labeled Experimental until the remaining gates land.

### 0.3 (current)

The security release - **gate 4 (security baseline) is substantially met**:

- TLS in transit: the broker listener serves TLS from operator PEMs or
  per-deployment generated material, the admin dashboard serves HTTPS from the
  same material, and the Rust, TypeScript, and Python clients connect with
  OS-root, CA-file, or fingerprint-pin trust and a typed error taxonomy.
- Broker authentication against an argon2 user store, managed from the
  dashboard and `fibrilctl`, replicated across the cluster, with the built-in
  credentials restricted to loopback. Node-to-node connections authenticate
  with a cluster shared secret, separate from user accounts.
- A first-boot setup flow (TLS, admin user, and cluster secret) and a cluster
  setup guide, so both the entry-level and unattended paths are documented.

Mutual-TLS client authentication, per-topic authorization, TLS on inter-broker
connections, and certificate rotation are the remaining security depth, planned
for later minors.

### Toward 1.0 (later 0.x minors)

Each subsequent minor knocks off part of a gate, incrementally:

- The wire protocol is versioned with a back-compat policy and the client APIs
  are frozen, including the Offset/Topic newtype and `Arc<str>` pass (gate 2).
- Durable restart reconciliation finishes the operational lifecycle (gate 3).
- The remaining security depth (mTLS, per-topic authorization, inter-broker
  TLS, certificate rotation) completes gate 4.

### After 1.0 (parallel, non-gating)

These add reach and polish without gating 1.0, and several can proceed in
parallel before then:

- More first-party clients: Go, C#, Java.
- Observability exporters (Prometheus, OpenTelemetry).
- Continued performance refinement (async replication fsync, staging micro-opts).
- Optional symmetric conveniences such as wildcard publishers (client-side
  fan-out to every topic matching a pattern, mirroring wildcard subscribe), if a
  real need appears.

See [out of scope](#out-of-scope) for what stays deliberately outside the broker.

## Release history

Shipped changes are recorded in the repo changelog
([CHANGELOG.md](https://github.com/Axmouth/fibril/blob/main/CHANGELOG.md)),
which is also the body of each
[GitHub release](https://github.com/Axmouth/fibril/releases). This page
covers direction, not history. For the full wired surface and its
conditions, see [implemented surface](/implemented-surface/).

## Near term

The near, medium, and longer-term lists below are the detailed backlog that
feeds the milestones above.

- Refactor server bootstrap wiring into the `fibril` library enough to make
  multi-node coordination and cohort-controller tests stand up real brokers
  without going through `main`.
- Add fuller multi-broker client scenario tests for cross-broker exclusive
  consumer-group coordination and failure handling.
- Finish the combined Offset plus Topic/Group newtype pass after the current
  partition newtype pass, including the planned `Arc<str>` direction for
  Topic/Group.
- Improve client feedback for reconnect cases where reconciliation closes a
  subscription stream.
- Keep improving DLQ replay and message inspection workflows, especially bulk operations and clearer operator feedback.
- Add the next storage-level startup/runtime settings where they have clear operational value.
- Keep refining sparse-queue observability where it helps operators decide why a queue is loaded, idle, or not yet unloaded.
- Explore cluster-wide immutable runtime policy. Hardware-shaped startup
  settings such as storage paths and log tuning should stay local, while any
  cluster-wide runtime locks need a replicated policy rather than node-local
  boot config.
- Improve TCP protocol ergonomics and error behavior.
- Keep the Rust, TypeScript, and Python client APIs aligned as public-path
  behavior changes.

## Medium term

- Add cross-broker replication lag aggregation to `fibrilctl topology` and the
  admin topology diagram.
- Add programmatic node management and scale-up or scale-down flows around the
  Ganglion-backed cluster path.
- Add fault-injection and unreliable-infrastructure testing for partitions,
  latency, dropped traffic, and leadership churn.
- Add consumer-group assignment narrowing so clients can subscribe only to their
  assigned partition subset instead of relying only on the delivery gate.
- Continue improving runnable broker images and binaries, including `fibrilctl` in the server image.
- Add more production deployment guidance for the broker itself.
- Improve admin interface observability.
- Produce repeatable benchmark reports and keep the
  [optimization log](/development/optimization-log/) current as
  low-level performance work is investigated.
- Tighten memory behavior under large queue depth, high inflight load, and many idle queues.

## Longer term

- Hardening the experimental replication and clustering path (queues and streams)
  into production guidance and supported defaults.
- Durable broker restart reconciliation. This extends the live-process graceful
  reconnect (see [reconnection grace](/development/reconnection-grace/))
  across a broker process restart. A client whose connection is still within its
  grace window would reclaim its persisted session and inflight ownership
  transparently, using a startup grace window before normal redelivery resumes,
  so a restart is invisible to in-flight work rather than a disconnect. The same
  property makes rolling upgrades trivial. Current reconnect grace is live-process
  only. The partition side already has its counterpart: on cold restart a
  partition reassigned away while the node was down is retained as inert on-disk
  cold storage rather than served stale or discarded.
- Planned restart announcement (drain). Before a graceful shutdown or upgrade the
  broker would push a "going away" notice with a grace deadline, so clients stop
  issuing new work, settle in-flight, and enter their resume path proactively
  instead of after a dropped socket. Combined with durable restart reconciliation
  this turns a rolling upgrade into announce, drain, restart, resume.
- More complete client ecosystem. The Python client (async plus a blocking
  facade) has landed. The next targets are C#, Go, and Java.

## Out of scope

Transactions are not planned. Fibril is aiming for clear durable messaging semantics, not a clone of every broker feature, and transactional publish/consume workflows are intentionally out of scope.

Content-routing scripting and SQL/stream-processing are also out of scope for the broker core. Programmable routing, if ever pursued, belongs in a layer above Fibril (an external engine that drives declares and subscriptions, potentially its own runtime), not embedded in the broker hot path. Keeping that logic outside the broker preserves the simple, predictable core.

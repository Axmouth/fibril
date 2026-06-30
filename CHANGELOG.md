# Changelog

All notable changes to the Fibril repo (the broker, the Rust/TypeScript/Python
clients, the admin dashboard, and the CLI) are recorded here. Ganglion and
Keratin track their own changelogs in their own repos.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). Pre-1.0, minor
versions may still change the API and wire protocol; 1.0 commits to stability.

## [Unreleased]

## [0.2.0] - 2026-06-30

The cluster-confidence and operational-hardening release. The cluster path is
still labeled experimental, but it is now proven by a deterministic simulation
suite, a chaos/soak suite, and a real multi-node failover run.

### Added

- Deterministic simulation testing (turmoil): an in-process multi-broker harness
  with eleven scenarios - broker-in-sim smoke, follower catch-up, owner-partition
  failover, a real ganglion raft cluster over the simulated network,
  returning-old-owner split-brain, lossy-link catch-up, raft under message loss,
  replica-durable no-false-ack under partition, checkpoint install, and
  repartition-cutover under delayed acks. Built behind a `simulation` feature.
- Chaos and soak suite (`crates/broker/tests/soak.rs`): durable crash-recovery
  across restarts and sustained concurrent-load no-loss, scalable via
  `FIBRIL_SOAK_*` environment variables.
- Real multi-node validation: `scripts/cluster-tryout.sh --failover-verify`
  (identity-tagged zero-loss check under a live owner kill) and `--chaos`
  (repeated mixed faults under load, zero loss plus reconvergence).
- Planned-restart drain: a `GoingAway` notice the broker broadcasts and clients
  surface as an observable event, plus an admin drain endpoint.
- Configurable replication read-timeout slack and owner connection-setup timeout
  so a dropped response or SYN can no longer hang a follower on a dead
  connection.

### Changed

- Reconnect grace is on by default (5s), with the policy questions resolved.
- The ack-tracking window is a settled `RangeSet` rather than a bounded bitset.
- The injectable raft transport (a `RaftDialer` in Ganglion) lets the coordination
  layer run over real or simulated TCP without a Ganglion test dependency.

### Fixed

- Restart cold-start reconciliation treats orphaned on-disk partitions as inert
  cold storage rather than mis-materializing them.

[Unreleased]: https://github.com/axmouth/fibril/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/axmouth/fibril/releases/tag/v0.2.0

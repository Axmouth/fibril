# Changelog

All notable changes to the Fibril repo (the broker, the Rust/TypeScript/Python
clients, the admin dashboard, and the CLI) are recorded here. Ganglion and
Keratin track their own changelogs in their own repos.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). Pre-1.0, minor
versions may still change the API and wire protocol; 1.0 commits to stability.

## [Unreleased]

## [0.2.0] - 2026-07-03

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
- Idle stream channel eviction through the new `stream` runtime settings
  (disabled by default), with the ephemeral flush ticker gated to only sync
  after writes so idle ephemeral streams cost nothing.
- Stream observability: per-partition live subscription counts and lag
  recovery totals in stream stats and the admin streams page, and stream
  publish and delivery counted in the broker overview rates.
- A `min_fsync_interval_ms` storage setting, a group-commit cadence floor for
  storage where per-fsync cost dominates (consumer SATA class).

### Changed

- Reconnect grace is on by default (5s), with the policy questions resolved.
- The ack-tracking window is a settled `RangeSet` rather than a bounded bitset.
- The injectable raft transport (a `RaftDialer` in Ganglion) lets the coordination
  layer run over real or simulated TCP without a Ganglion test dependency.
- Delivery-path throughput: consumer flow-control credit is released when an
  ack is accepted rather than after its fsync, and broker-to-connection
  delivery moves per-consumer batches. Single-node 1KB saturation onset moved
  from roughly 350-400k/s to 500-600k/s with lower high-rate memory use.
- Publish confirm p50 dropped to a few milliseconds on fast storage with
  Keratin's self-clocking group commit (the storage-side detail lives in
  Keratin's own changelog).

### Fixed

- Restart cold-start reconciliation treats orphaned on-disk partitions as inert
  cold storage rather than mis-materializing them.
- A lagging connected stream subscriber could silently lose records: a full
  live buffer dropped individual records and auto-ack advanced the durable
  cursor past the gap. Stream delivery is now contiguous per subscriber on
  every tier, with watermark-based lag recovery from the ring or the log.
- Concurrent identical stream declares could fail with a spurious 500 (a
  shared temp file race in the storage kind marker, fixed in Keratin).
- Graceful shutdown flushes pending stream cursor commits, so a planned
  restart loses no committed consumer progress.

[Unreleased]: https://github.com/axmouth/fibril/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/axmouth/fibril/releases/tag/v0.2.0

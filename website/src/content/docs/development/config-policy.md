---
title: Configuration policy
description: How Fibril decides where a setting belongs between startup config, runtime settings and queue state.
---

This is a development note. It describes how Fibril should decide where a setting belongs before adding more one-off environment variables.

## Goal

Configuration should stay predictable as Fibril gains an admin UI and more runtime-tunable behavior.

The main risk to avoid is overlapping sources of truth where a setting can come from env, a file, admin state, and queue state with unclear precedence.

## Setting Classes

Fibril settings should be classified before implementation.

### Bootstrap Settings

Bootstrap settings are required before the broker can open or accept traffic.

Examples:

- data directory
- TCP bind address
- admin bind address
- storage/log implementation choice
- TLS and listener setup
- initial admin authentication source

These should come from a config file, environment variables, or CLI flags. They should not be edited through persisted runtime state while the process is running.

Changing a bootstrap setting should require restart unless there is a very clear reason to support live reload.

### Runtime Defaults

Runtime defaults affect behavior but can reasonably change while the broker is alive.

Examples:

- default inflight lease duration and expiry polling (`delivery`)
- idle queue cleanup timeout, sweep interval and publisher idle expiry
  (`idle_queue_cleanup`)
- reconnect grace, drain handoff wait and message size limits (`connection`)
- replication timeouts, in-sync replica minimum, eager failover and agreed
  checkpoints (`replication`)
- default partition count for new queues (`partitioning`)
- stream cursor commit batching and idle stream eviction (`stream`)
- the soft partitions-per-consumer target for exclusive groups
  (`consumer_groups`)
- forwarding limits for private partition owners (`routing`)

These live in persisted global state. The admin UI can edit them, and the broker can apply them without restart where the implementation supports it.

Some persisted runtime settings are not broker defaults. For example, the
current global dead-letter queue target is owned by storage-level state because
Stroma resolves the target when messages exhaust retries. It still follows the
same operator-facing rules: live update, persisted value after restart, and
explicit version checks for admin changes.

### Queue Settings

Queue settings apply to one queue or queue group.

Examples:

- per-queue retry policy
- per-queue dead-letter policy
- per-queue retention-like policy, if added
- per-queue delivery or expiry settings, if added

Queue settings should be changed through explicit queue commands, such as declaration/update commands, not by editing global config.

Global runtime defaults can provide starting values for new queues, but existing queues should not silently change unless the user makes a global policy that is explicitly defined to apply dynamically.

## Source of Truth

Use one owner per setting.

Bootstrap settings are owned by startup config.

Runtime defaults are owned by persisted global state. Startup config may provide initial values for first boot, but should not keep overriding persisted runtime values on every restart by default.

Queue settings are owned by queue state.

This keeps operator expectations simple:

- startup config decides how the process starts
- global state decides live broker defaults
- queue state decides queue-specific behavior

## First-Boot Defaults

Startup config can seed persisted global state only when no global state exists yet.

After global state exists, persisted global state wins for runtime defaults.

This avoids surprising admin UI behavior where a user edits a setting, restarts the process, and an env var silently changes it back.

If an operator wants a startup setting to be locked, make that explicit rather than implicit.

## Locked Runtime Settings

Some deployments may want configuration management to own a runtime default and prevent admin edits.

Fibril makes this explicit with the typed `[runtime_locks]` startup section:

```toml
[runtime_locks]
idle_queue_cleanup = true
```

A locked section keeps its boot seed on every start, and the admin API rejects
edits to it with HTTP 423 `setting_locked`. `idle_queue_cleanup` is the only
lockable section so far.

Do not make every environment variable an invisible override for persisted state. That creates hard-to-debug behavior.

## Global State Shape

Persisted global state lives in the Stroma global store, a versioned key-value store on its own log, separate from queue logs. Runtime settings are one versioned document in it, updated with an expected-version check. In cluster mode the coordination cluster settings document is the authority and each node keeps a local copy.

Properties the store has:

- Changes are appended to the global store log, and the store rebuilds its
  current values by replaying that log on open. It takes no snapshots and does
  not compact.
- Every key carries an explicit version, and admin updates check the expected
  version.
- The whole runtime settings document is written at once, so related settings
  change together.

Planned or partial:

- Readable change history. Old versions stay in the log, but nothing reads them
  back. The admin activity feed records settings changes in memory only and
  resets on restart. The durable trail is the broker log.
- Snapshots or compaction of the global store log.

Runtime settings are grouped into sections where atomic updates matter. The
document has these sections, shown here with a few of their fields at their
first-boot values. The full list and meanings are in
[configuration](/configuration/), where each one can be seeded under
`[runtime_seed.<section>]`.

```toml
[delivery]
inflight_ttl_ms = 30000
expiry_poll_min_ms = 15000

[idle_queue_cleanup]
enabled = false
evict_after_ms = 600000
sweep_interval_ms = 60000
# publisher_idle_timeout_ms is unset by default, so publishers on an open
# connection never expire.

[connection]
# The startup seed fills these two on first boot.
reconnect_grace_ms = 5000
resume_session_restart_ttl_ms = 60000
# drain_handoff_timeout_ms is unset by default, which waits up to 30000 ms.

[replication]
confirm_timeout_ms = 5000
min_in_sync_replicas = 1
eager_failover = false
eager_failover_grace_ms = 1000
agreed_checkpoint_interval_ms = 0

[partitioning]
default_partition_count = 1

[stream]
cursor_commit_window_us = 100
cursor_commit_max_batch = 1024
idle_evict_enabled = false

[consumer_groups]
# default_target_per_consumer is unset by default, which disables the signal.

[publish]
buffered_coalesce = "gap"
buffered_gap_us = 100
durable_coalesce = "window"
durable_gap_us = 100

[routing]
max_upstream_sessions = 4096
max_upstream_sessions_per_owner = 1024
forward_timeout_ms = 30000
upstream_idle_close_ms = 30000
```

## Naming Direction

Prefer user-facing names in docs and admin UI:

- "idle queue cleanup"
- "idle timeout"
- "publisher idle expiry"

Use implementation names only in internal APIs or development notes.

The current environment variable names are acceptable as temporary bootstrapping, but future config should avoid adding more bespoke env vars for runtime behavior.

## Implementation Policy

Before adding a setting, answer:

1. Is it needed before the process starts?
2. Can it safely change at runtime?
3. Is it global, queue-specific, or connection-specific?
4. Who owns it after restart?
5. Can admin UI edit it?
6. If config and persisted state both mention it, which one wins and is that visible?

If the answer is unclear, do not add an env var as the default path. Add a typed config field or a persisted runtime setting design first.

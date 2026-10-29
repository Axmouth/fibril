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
The `fibril-config` crate loads them into one finished `ServerConfig`, with
this precedence:

```txt
compiled defaults < TOML config file < environment variables < CLI arguments
```

The same precedence applies to first-boot runtime seeds, but environment
variables never override persisted runtime settings after they exist.

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

Locks are evaluated per node. The proposed
[configuration sources](/development/config-sources/) design replaces them with a
cluster-wide choice of who owns each document.

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

Runtime settings are grouped into sections where atomic updates matter, such as
`delivery`, `idle_queue_cleanup`, `connection`, `replication`, `partitioning`,
`stream`, `consumer_groups`, `publish`, `storage` and `routing`. The full list,
defaults and meanings are in [configuration](/configuration/), where each section
can be seeded under `[runtime_seed.<section>]`.

### Store interface

Stroma does not know what Fibril's settings mean. It stores opaque bytes with a
version, under a namespaced key (`fibril.runtime` / `settings` for runtime
settings), and offers compare-and-swap and a watch:

```rust
impl GlobalStore {
    pub async fn get(&self, key: &GlobalKey) -> Result<Option<GlobalValue>>;
    pub async fn put(
        &self,
        key: GlobalKey,
        value: Vec<u8>,
        expected_version: Option<u64>,
    ) -> Result<PutOutcome>; // Stored { version } or Conflict { current }
    pub async fn watch(&self, key: GlobalKey) -> Result<watch::Receiver<Option<GlobalValue>>>;
}
```

Broker and admin code own encoding and validation. Values are MessagePack
through Serde rather than JSON: compact, schema-friendly and off the hot path.

### Versioned updates

Every update names the version it was edited from. If the stored version still
matches, the update commits and the version goes up by one. Otherwise it returns
a conflict with the current document instead of overwriting, so two operators
cannot silently clobber each other.

The admin API reads and writes the whole document at once
(`GET` and `PUT /admin/api/runtime-settings`, the `PUT` body carrying
`expected_version` and `settings`). Whole-document writes keep atomicity clear
and avoid partial merge rules. Responses:

- `200` stored, with the new effective settings
- `400` validation failed
- `409` stale `expected_version`, with the current effective settings
- `423` the request changed a locked section
- `404` runtime settings are unavailable on this admin server

On a conflict the settings page warns that settings changed, shows the current
values beside the attempted ones, marks the fields the operator changed, and lets
them resubmit from the new version. It never merges silently.

### Live application

Readers never lock on the hot path. The write goes request, validation, encode,
compare-and-swap put, watchers notified, and components swap in a new immutable
snapshot. A setting that cannot apply live must be labelled restart-required
rather than pretend to take effect. Such settings should stay rare until a
controlled restart flow (drain, persist, restart, reconnect) exists.

Changing a global queue default affects new queues only. Existing queues never
inherit a later default change implicitly.

### Open questions

- A reset or repair flow for corrupted runtime settings.
- Whether the global store belongs in Stroma core or a separate metadata module.
- The admin auth model for mutations once read-only admin access exists.

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

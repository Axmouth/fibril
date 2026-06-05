---
title: Configuration design
description: Planned implementation shape for startup config and persisted runtime settings.
---

This is a design reference for implementing Fibril configuration. It builds on the [configuration policy](/latest/development/config-policy/).

## Goals

Fibril should have an operations model that is easy to predict:

- startup config decides how the process starts
- persisted runtime settings decide live broker defaults
- queue settings decide queue-specific behavior

The implementation should avoid hidden precedence, runtime env-var surprises, and higher-level broker concepts leaking into lower-level storage code.

## Startup Config

Startup config is loaded before the server opens storage or starts listeners.

The config file format is TOML.

Precedence:

```txt
compiled defaults < TOML config file < environment variables < CLI arguments
```

This precedence applies to bootstrap settings and first-boot runtime seeds. It does not mean env vars keep overriding persisted runtime settings after runtime state exists.

Examples of bootstrap settings:

- data directory
- broker TCP bind address
- admin bind address
- admin auth bootstrap
- config file path
- storage/log implementation options
- TLS/listener setup, when added

## Config Crate

Add a dedicated config crate, likely `fibril-config`.

Responsibilities:

- define the server config model
- load TOML
- apply env overrides
- apply CLI overrides
- validate values
- expose source metadata for diagnostics and admin display
- produce a finished config object for `crates/fibril/bin/server.rs`

The server binary should not keep hand-parsing env vars.

The crate can depend on lower-level crates for config structs where that does not create cycles. Lower-level crates should not depend on the server binary or admin crate.

Use `clap` for CLI parsing when CLI support is added. It is fine to implement TOML + env first, but the model should leave room for CLI overriding env.

## Startup Config Sketch

Example TOML shape:

```toml
[server]
data_dir = "server_data"

[broker.listener]
bind = "0.0.0.0:9876"

[admin.listener]
bind = "0.0.0.0:8081"

[runtime_seed.idle_queue_cleanup]
enabled = false
evict_after_ms = 600000
sweep_interval_ms = 60000
publisher_idle_timeout_ms = 600000

[runtime_locks]
idle_queue_cleanup = false
```

`runtime_seed` values seed persisted runtime settings only when no persisted runtime settings exist yet.

`runtime_locks` are bootstrap-owned. If a runtime setting is locked, admin mutation should reject changes and the admin UI should show the field as locked.

## Persisted Runtime Settings

Runtime settings are not owned by env vars after first boot.

Runtime settings should be persisted through a durable global store and exposed through broker/admin APIs.

Examples:

- idle queue cleanup behavior
- default inflight lease duration
- expiry polling defaults
- future global dead-letter defaults
- future defaults for newly declared queues

Runtime defaults should be grouped where atomic updates matter. Idle queue cleanup is one group:

```rust
pub struct IdleQueueCleanupSettings {
    pub enabled: bool,
    pub evict_after_ms: u64,
    pub sweep_interval_ms: u64,
    pub publisher_idle_timeout_ms: Option<u64>,
}
```

The broker can convert this to internal `BrokerConfig`/`ConnectionSettings` shapes.

## Global Store

Persisted runtime settings should live in Stroma or next to Stroma as a generic durable store.

Stroma should not learn broker-specific setting semantics. It should provide persistence, replay, snapshots, versioning, and compare-and-swap behavior for opaque values.

Possible API shape:

```rust
pub struct GlobalKey {
    pub namespace: String,
    pub key: String,
}

pub struct GlobalValue {
    pub version: u64,
    pub bytes: Vec<u8>,
}

pub enum PutOutcome {
    Stored { version: u64 },
    Conflict { current: GlobalValue },
}

pub trait GlobalStore {
    async fn get(&self, key: &GlobalKey) -> Result<Option<GlobalValue>>;

    async fn put(
        &self,
        key: &GlobalKey,
        value: Vec<u8>,
        expected_version: Option<u64>,
    ) -> Result<PutOutcome>;

    fn watch(&self, key: &GlobalKey) -> watch::Receiver<Option<GlobalValue>>;
}
```

Namespacing keeps broker-owned settings separate from other future metadata:

```txt
namespace = "fibril.runtime"
key = "idle_queue_cleanup"
```

## Encoding

Global store values should not use JSON.

Use MessagePack for now:

- compact enough
- schema-friendly through Serde
- already aligned with Fibril's internal encoding direction
- not on a hot path

Stroma stores bytes. Broker/admin code owns serialization and validation for Fibril runtime settings.

## Compare And Swap

Runtime setting updates should be versioned.

Admin update requests should include the version the user edited from:

```txt
expected_version = 12
```

If the stored version is still 12, the update commits and produces version 13.

If another change already produced version 13, the update returns a conflict instead of silently overwriting.

This protects operators from clobbering each other's changes.

## Admin Conflict UX

The first admin UI does not need a complex merge editor.

Good enough behavior:

1. User opens settings page at version 12.
2. Another user changes settings to version 13.
3. First user submits.
4. API returns conflict with current values and version 13.
5. UI shows a clear warning that settings changed meanwhile.
6. UI shows current values and the user's attempted values.
7. For fields the user changed, show the conflict near the field.
8. User can review and submit again from version 13.

Do not silently merge unless the merge rule is obvious and safe. For grouped settings, treating the group as one atomic object is acceptable.

## Live Application

Runtime readers should not lock on every hot-path operation.

Use immutable settings snapshots and atomic replacement, such as `ArcSwap` or a `watch` receiver feeding an `Arc` held by broker/protocol components.

The write path:

```txt
admin request
  -> validate request and expected version
  -> encode MessagePack
  -> GlobalStore CAS put
  -> notify watchers
  -> broker/protocol observe new immutable settings snapshot
```

Settings that cannot be live-applied should be marked restart-required and should not pretend to take effect immediately.

## Queue Defaults

Global queue defaults apply to new queues.

Existing queues should not dynamically inherit later global default changes. That behavior sounds convenient but creates operator surprises.

If dynamic inheritance is ever added, it should be explicit and named as such.

## Runtime Locks

Runtime locks are bootstrap-owned and should be visible.

If a setting group is locked:

- admin UI shows it as locked
- admin API rejects mutation with a clear error
- persisted runtime state still stores the current value
- config management can intentionally own that value

Do not make ordinary env vars hidden runtime overrides.

## First Implementation Slice

Implement in this order:

1. Add `fibril-config` with typed startup config, TOML parsing, env overlay, validation, and tests.
2. Wire `server.rs` through the finished config object while preserving current behavior.
3. Add config docs for TOML and env compatibility.
4. Add Stroma `GlobalStore` with MessagePack-ready byte values, versions, CAS, event log, snapshot, and tests.
5. Add broker runtime settings model and first-boot seeding.
6. Add live settings snapshots for idle queue cleanup.
7. Add admin read endpoint.
8. Add admin update endpoint with expected-version conflict handling.
9. Add admin UI for viewing/updating settings and conflict display.

The first slice should not change runtime behavior. It should make configuration explicit and ready for persisted runtime settings.

## Open Questions

- Exact config file discovery: default `fibril.toml`, `FIBRIL_CONFIG`, CLI `--config`, or all three.
- Whether runtime locks should be implemented with the first persisted settings slice or only reserved in the schema.
- Whether the global store belongs directly in Stroma core or in an adjacent Stroma metadata module.
- Whether a global store snapshot format should be shared with queue snapshots or kept fully separate.
- Exact admin auth model for mutation once read-only admin access exists.

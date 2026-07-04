---
title: Configuration design
description: Planned implementation shape for startup config and persisted runtime settings.
slug: 0.4/development/config-design
---

This is a design reference for Fibril configuration. It builds on the [configuration policy](/0.4/development/config-policy/) and tracks both implemented pieces and planned follow-up work.

## Goals

Fibril should have an operations model that is easy to predict:

* startup config decides how the process starts
* persisted runtime settings decide live broker defaults
* queue settings decide queue-specific behavior

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

* data directory
* broker TCP bind address
* admin bind address
* admin auth bootstrap
* config file path
* storage/log implementation options
* TLS/listener setup, when added

## Config Crate

Fibril has a dedicated `fibril-config` crate.

Responsibilities:

* define the server config model
* load TOML
* apply env overrides
* apply CLI overrides
* validate values
* expose source metadata for diagnostics and admin display
* produce a finished config object for `crates/fibril/bin/server.rs`

The server binary should not keep hand-parsing env vars.

The crate can depend on lower-level crates for config structs where that does not create cycles. Lower-level crates should not depend on the server binary or admin crate.

CLI parsing uses `clap`. TOML, environment, and CLI values are folded into one finished `ServerConfig`.

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

* idle queue cleanup behavior
* default inflight lease duration
* expiry polling defaults
* future global dead-letter defaults
* future defaults for newly declared queues

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

Persisted runtime settings live in Stroma's durable global store.

Stroma should not learn broker-specific setting semantics. It should provide persistence, replay, snapshots, versioning, and compare-and-swap behavior for opaque values.

Current API shape:

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
    Conflict { current: Option<GlobalValue> },
}

impl GlobalStore {
    pub async fn get(&self, key: &GlobalKey) -> Result<Option<GlobalValue>>;

    pub async fn put(
        &self,
        key: GlobalKey,
        value: Vec<u8>,
        expected_version: Option<u64>,
    ) -> Result<PutOutcome>;

    pub async fn watch(&self, key: GlobalKey) -> Result<watch::Receiver<Option<GlobalValue>>>;
}
```

Namespacing keeps broker-owned settings separate from other future metadata:

```txt
namespace = "fibril.runtime"
key = "settings"
```

## Encoding

Global store values should not use JSON.

Use MessagePack for now:

* compact enough
* schema-friendly through Serde
* already aligned with Fibril's internal encoding direction
* not on a hot path

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

The broker runtime settings manager now has the CAS primitive for this. It accepts an expected version and a complete `RuntimeSettings` document, validates it, writes through the global store, and returns either the stored effective settings or the current effective settings on conflict.

Locked groups are handled before persistence. If a group is locked by boot config, updates that try to change it are rejected. Updates to other groups preserve the hidden persisted value for the locked group instead of overwriting it with the boot-owned effective value.

## Admin API

The first admin API exposes the complete runtime settings document.

Read:

```http
GET /admin/api/runtime-settings
```

Response:

```json
{
  "version": 12,
  "settings": {
    "delivery": {
      "inflight_ttl_ms": 30000,
      "expiry_poll_min_ms": 15000,
      "expiry_batch_max": 8192,
      "delivery_poll_max_ms": 5000
    },
    "idle_queue_cleanup": {
      "enabled": false,
      "evict_after_ms": 600000,
      "sweep_interval_ms": 60000,
      "publisher_idle_timeout_ms": null
    }
  },
  "locks": {
    "idle_queue_cleanup": false
  }
}
```

Update:

```http
PUT /admin/api/runtime-settings
```

Request:

```json
{
  "expected_version": 12,
  "settings": {
    "delivery": {
      "inflight_ttl_ms": 30000,
      "expiry_poll_min_ms": 15000,
      "expiry_batch_max": 8192,
      "delivery_poll_max_ms": 5000
    },
    "idle_queue_cleanup": {
      "enabled": true,
      "evict_after_ms": 600000,
      "sweep_interval_ms": 60000,
      "publisher_idle_timeout_ms": 600000
    }
  }
}
```

Outcomes:

* `200 OK`: update stored and response contains the new effective settings.
* `400 Bad Request`: validation failed or the request tried to change a locked setting group.
* `409 Conflict`: `expected_version` is stale and the response contains the current effective settings.
* `404 Not Found`: runtime settings are unavailable for this admin server instance.
* `500 Internal Server Error`: storage, encoding, or unexpected update failure.

The API currently accepts and returns the whole settings document. That keeps atomicity clear and avoids partial merge rules until the admin UI has a more deliberate editing model.

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

If a future runtime setting genuinely needs restart, the admin surface should label it as restart-required. The long-term direction is a controlled restart flow: drain or close connections gracefully, persist state, restart, then let clients reconnect cleanly. Until that exists, restart-required settings should be rare and explicit.

## Queue Defaults

Global queue defaults apply to new queues.

Existing queues should not dynamically inherit later global default changes. That behavior sounds convenient but creates operator surprises.

If dynamic inheritance is ever added, it should be explicit and named as such.

## Runtime Locks

Runtime locks are bootstrap-owned and should be visible.

If a setting group is locked:

* admin UI shows it as locked
* admin API rejects mutation with a clear error
* persisted runtime state still stores the current value
* config management can intentionally own that value

Do not make ordinary env vars hidden runtime overrides.

## Implementation Status

Done:

* `fibril-config` typed startup config with TOML, env overlay, CLI overlay, validation, and tests
* `server.rs` loads a finished config object
* Stroma `GlobalStore` with opaque byte values, versions, CAS, watch support, and tests
* broker runtime settings model with MessagePack encoding
* first-boot seeding into global state
* boot-owned runtime locks
* manager-level compare-and-swap update with conflict reporting
* admin JSON API for reading runtime settings
* admin JSON API for updating runtime settings with expected-version conflict handling
* first admin UI for viewing/updating runtime settings, locked fields, and conflicts
* live broker runtime config snapshots with settings-change wakeups
* live publisher idle expiry updates for existing TCP connections

Useful follow-up work:

1. Add an operator-facing reset or repair flow for corrupted runtime settings.
2. Add more focused admin route/UI tests once the admin test harness is less ad hoc.

## Open Questions

* Whether the global store belongs directly in Stroma core or in an adjacent Stroma metadata module.
* Whether a global store snapshot format should be shared with queue snapshots or kept fully separate.
* Exact admin auth model for mutation once read-only admin access exists.

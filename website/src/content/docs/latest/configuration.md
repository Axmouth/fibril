---
title: Configuration
description: Configure Fibril startup behavior and persisted runtime settings.
---

Fibril has two configuration layers:

- **Startup config** decides how the server process starts.
- **Runtime settings** decide live broker behavior and are persisted after first boot.

That split matters. Startup config is for things the process needs before it can run, such as bind addresses and the data directory. Runtime settings are for behavior that can be changed while the broker is running, such as delivery timing and idle queue cleanup.

## Config File

The config file format is TOML. The repository includes `fibril.example.toml`:

```toml
[server]
data_dir = "server_data"

[broker.listener]
bind = "0.0.0.0:9876"

[admin.listener]
bind = "0.0.0.0:8081"

[storage.keratin]
fsync_interval_ms = 5

[storage.keratin.message_log]
segment_max_bytes = 268435456

[storage.keratin.event_log]
segment_max_bytes = 33554432

[runtime_seed.delivery]
inflight_ttl_ms = 30000
expiry_poll_min_ms = 15000
expiry_batch_max = 8192
delivery_poll_max_ms = 5000

[runtime_seed.idle_queue_cleanup]
enabled = false
evict_after_ms = 600000
sweep_interval_ms = 60000
# Set this for sparse workloads with long-lived publishing connections.
# publisher_idle_timeout_ms = 600000

[runtime_locks]
idle_queue_cleanup = false
```

Run with a config file:

```sh
cargo run --release --bin fibril-server -- --config fibril.toml
```

or:

```sh
FIBRIL_CONFIG=fibril.toml cargo run --release --bin fibril-server
```

## Precedence

Startup config is resolved in this order:

```txt
compiled defaults < TOML config file < environment variables < CLI arguments
```

This precedence applies to startup fields and first-boot runtime seeds. It does not mean environment variables keep overriding persisted runtime settings after runtime state exists.

## Startup Fields

These fields are read on process start.

| TOML field | Env var | CLI flag | Default |
| --- | --- | --- | --- |
| `server.data_dir` | `FIBRIL_DATA_DIR` | `--data-dir` | `server_data` |
| `broker.listener.bind` | `FIBRIL_BROKER_BIND` | `--broker-bind` | `0.0.0.0:9876` |
| `admin.listener.bind` | `FIBRIL_ADMIN_BIND` | `--admin-bind` | `0.0.0.0:8081` |
| `storage.keratin.fsync_interval_ms` | `FIBRIL_KERATIN_FSYNC_INTERVAL_MS` | `--keratin-fsync-interval-ms` | `5` |
| `storage.keratin.message_log.segment_max_bytes` | `FIBRIL_KERATIN_MESSAGE_LOG_SEGMENT_MAX_BYTES` | `--keratin-message-log-segment-max-bytes` | `268435456` |
| `storage.keratin.event_log.segment_max_bytes` | `FIBRIL_KERATIN_EVENT_LOG_SEGMENT_MAX_BYTES` | `--keratin-event-log-segment-max-bytes` | `33554432` |

Changing these generally requires restarting the server.

`storage.keratin.message_log.segment_max_bytes` and `storage.keratin.event_log.segment_max_bytes` are rollover thresholds. A segment rolls after an append crosses the configured size, so an individual segment can be slightly larger than this value.

## Runtime Seeds

`runtime_seed` values initialize the persisted runtime settings document when no runtime settings exist yet.

After runtime settings exist, the persisted values own these settings. You can edit them through the admin settings page or the admin runtime settings API.

### Delivery

| TOML field | Default | Meaning |
| --- | --- | --- |
| `runtime_seed.delivery.inflight_ttl_ms` | `30000` | How long a delivered message lease lasts before it can be retried. |
| `runtime_seed.delivery.expiry_poll_min_ms` | `15000` | Minimum sleep between expiry checks when no earlier expiry is known. |
| `runtime_seed.delivery.expiry_batch_max` | `8192` | Maximum expired messages to requeue in one expiry pass. Must be at least `1`. |
| `runtime_seed.delivery.delivery_poll_max_ms` | `5000` | Maximum idle poll delay for delivery loops. |

### Idle Queue Cleanup

| TOML field | Env/CLI compatibility | Default | Meaning |
| --- | --- | --- | --- |
| `runtime_seed.idle_queue_cleanup.enabled` | enabled implicitly by `FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS` or `--queue-idle-evict-after-ms` | `false` | Enables unloading idle queues from memory. |
| `runtime_seed.idle_queue_cleanup.evict_after_ms` | `FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS`, `--queue-idle-evict-after-ms` | `600000` | How long a queue must be idle before cleanup can unload it. |
| `runtime_seed.idle_queue_cleanup.sweep_interval_ms` | `FIBRIL_QUEUE_IDLE_SWEEP_INTERVAL_MS`, `--queue-idle-sweep-interval-ms` | `60000` | How often the cleanup worker checks tracked queues. Must be at least `1`. |
| `runtime_seed.idle_queue_cleanup.publisher_idle_timeout_ms` | `FIBRIL_PUBLISHER_CACHE_IDLE_TIMEOUT_MS`, `--publisher-idle-timeout-ms` | unset | Lets unused publishers stop keeping queues active while a connection remains open. |

For sparse workloads, enable publisher idle expiry alongside queue cleanup. Without it, a long-lived connection that published to a queue can keep that queue active until the connection closes.

See [many idle queues](/latest/concepts/many-idle-queues/) for the user-facing behavior.

## Runtime Locks

Runtime locks let startup config own a runtime setting group and prevent admin edits.

```toml
[runtime_locks]
idle_queue_cleanup = true
```

When `idle_queue_cleanup` is locked:

- startup config controls the effective idle queue cleanup settings
- the admin settings page shows the group as locked
- admin update attempts for that group are rejected
- updates to other runtime settings can still be saved

Use locks only when config management should intentionally own the setting. Do not use ordinary env vars as hidden runtime overrides.

## Admin Runtime Settings

The admin UI exposes a settings page at:

```txt
/admin/settings
```

The JSON API is:

```http
GET /admin/api/runtime-settings
PUT /admin/api/runtime-settings
```

Update requests include an `expected_version`. If another operator changed settings first, the API returns `409 Conflict` with the current settings instead of overwriting them.

## Other Persisted Runtime Settings

Some live settings are owned by storage-level state rather than the broker
runtime settings document. The global dead-letter queue target is the current
example.

The admin settings page also exposes:

```http
GET /admin/api/global-dlq
PUT /admin/api/global-dlq
```

This setting:

- applies live
- is persisted in Fibril's storage state
- survives restart
- uses `expected_version` and returns `409 Conflict` if another update wins
- is not seeded or overridden by TOML, environment variables, or CLI flags

See [dead lettering](/latest/reliability/dead-lettering/) for the setting
shape and current limitations.

## Validation

Current validation rules:

- `server.data_dir` must not be empty
- `storage.keratin.fsync_interval_ms` must be at least `1`
- `storage.keratin.message_log.segment_max_bytes` must be at least `1`
- `storage.keratin.event_log.segment_max_bytes` must be at least `1`
- `runtime_seed.delivery.expiry_batch_max` must be at least `1`
- `runtime_seed.idle_queue_cleanup.sweep_interval_ms` must be at least `1`

More validation will be added as more settings become user-facing.

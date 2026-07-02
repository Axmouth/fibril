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

[admin.auth]
enabled = false
username = "fibril"
# password = "change-me"

[storage.keratin]
fsync_interval_ms = 5
# Floor between storage commits while the fsync worker is idle. 0 self-clocks
# group commit on fsync completions, the best setting for fast storage (NVMe,
# tmpfs). On slow-fsync storage such as SATA SSDs a floor around the fsync
# interval (5) gives the drive breathing room between write barriers.
min_fsync_interval_ms = 0

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

[runtime_seed.connection]
# Reconnect grace is on by default (5000 ms). Set to 0 to disable it.
# reconnect_grace_ms = 5000

[runtime_seed.replication]
confirm_timeout_ms = 5000
caught_up_poll_ms = 1000
retry_poll_ms = 100
checkpoint_retry_poll_ms = 5000
max_messages_per_read = 256
max_events_per_read = 256
max_bytes_per_read = 8388608
max_iterations_per_tick = 8
min_in_sync_replicas = 1
isr_timeout_ms = 10000
read_timeout_slack_ms = 10000
owner_connect_timeout_ms = 5000

[runtime_seed.partitioning]
default_partition_count = 1

[runtime_seed.consumer_groups]
# Blank or omit to disable the under-provisioned signal.
# default_target_per_consumer = 4

[coordination.ganglion]
heartbeat_interval_ms = 3000
liveness_ttl_ms = 9000

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
| `broker.listener.advertise` | `FIBRIL_BROKER_ADVERTISE` | none | derived (see below) |
| `admin.listener.bind` | `FIBRIL_ADMIN_BIND` | `--admin-bind` | `0.0.0.0:8081` |
| `admin.auth.enabled` | `FIBRIL_ADMIN_AUTH_ENABLED` | `--admin-auth-enabled` | `false` |
| `admin.auth.username` | `FIBRIL_ADMIN_USERNAME` | `--admin-username` | `fibril` |
| `admin.auth.password` | `FIBRIL_ADMIN_PASSWORD` | `--admin-password` | unset |
| `storage.keratin.fsync_interval_ms` | `FIBRIL_KERATIN_FSYNC_INTERVAL_MS` | `--keratin-fsync-interval-ms` | `5` |
| `storage.keratin.min_fsync_interval_ms` | `FIBRIL_KERATIN_MIN_FSYNC_INTERVAL_MS` | `--keratin-min-fsync-interval-ms` | `0` |
| `storage.keratin.message_log.segment_max_bytes` | `FIBRIL_KERATIN_MESSAGE_LOG_SEGMENT_MAX_BYTES` | `--keratin-message-log-segment-max-bytes` | `268435456` |
| `storage.keratin.event_log.segment_max_bytes` | `FIBRIL_KERATIN_EVENT_LOG_SEGMENT_MAX_BYTES` | `--keratin-event-log-segment-max-bytes` | `33554432` |
| `coordination.mode` | `FIBRIL_COORDINATION_MODE` | none | `static` |
| `coordination.ganglion.heartbeat_interval_ms` | `FIBRIL_COORDINATION_HEARTBEAT_INTERVAL_MS` | none | `3000` |
| `coordination.ganglion.liveness_ttl_ms` | `FIBRIL_COORDINATION_LIVENESS_TTL_MS` | none | `9000` |
| `coordination.ganglion.target_followers` | none | none | `1` |
| `coordination.ganglion.stream_replication_factor` | none | none | `1` |
| `coordination.ganglion.repartition_adoption_timeout_ms` | none | none | `30000` |
| `coordination.ganglion.assignment_durability` | `FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY` | none | `local_durable` |
| `recovery.on_mismatch` | `FIBRIL_RECOVERY_ON_MISMATCH` | none | `quarantine` |

Changing these generally requires restarting the server.

`broker.listener.advertise` is the address (or addresses) the broker tells peers
and clients to reach it on, separate from `bind`. This matters when `bind` is not
itself dialable - the common case is binding `0.0.0.0` in a container, which a
peer cannot connect back to. Give it a routable `host:port` (a service name is
fine, it is resolved at connect time), or several comma-separated entries in
`FIBRIL_BROKER_ADVERTISE` in priority order. When unset it is derived in
`ganglion` mode from this node's coordination peer host plus the broker port, and
otherwise falls back to `bind`. Standalone single-broker deployments do not need
it (clients connect to the broker directly). Only the first entry is dialed today;
the rest are carried for forward compatibility.

`coordination.mode` is `static` for a standalone single-broker deployment (the
default) or `ganglion` to run the embedded coordinator and form a cluster. The
`coordination.ganglion.*` settings only apply in `ganglion` mode. See
[clustering](/concepts/clustering/) and
[replication](/reliability/replication/).

`coordination.ganglion.target_followers` is the desired follower count per queue
partition. `coordination.ganglion.stream_replication_factor` is the equivalent for
DURABLE Plexus stream partitions — tuned separately so stream and queue fault
tolerance can differ; only the durable tier replicates, the express tiers stay
owner-only. A value of one keeps a durable stream available across a single node
loss; zero makes durable streams owner-only (durable on disk, not HA). See
[Plexus streams](/concepts/plexus-streams/).
`coordination.ganglion.assignment_durability` is the default durability
policy for new assignments (`local_durable`, `replica_accepted`, `replica_durable`,
or `majority_durable`).

`recovery.on_mismatch` controls what happens when recovery finds a damaged queue
log: `quarantine` (default) isolates the partition, `refuse` reports not ready,
and `ignore` truncates to the last valid record. See
[recovery quarantine](/reliability/recovery-quarantine/).

`admin.auth.enabled = true` requires both `admin.auth.username` and `admin.auth.password`.
The admin password is intentionally not shown in the dashboard startup summary.

`storage.keratin.message_log.segment_max_bytes` and `storage.keratin.event_log.segment_max_bytes` are rollover thresholds. A segment rolls after an append crosses the configured size, so an individual segment can be slightly larger than this value.

`coordination.ganglion.heartbeat_interval_ms` controls how often a broker
renews its cluster liveness record. `coordination.ganglion.liveness_ttl_ms`
controls how long a broker can go without a fresh heartbeat before the cluster
considers it unavailable. The TTL must be at least twice the heartbeat interval.
For heavy replication benchmarks, a longer TTL can avoid false failover while
the node is under artificial load.

`coordination.ganglion.repartition_adoption_timeout_ms` bounds how long a live
repartition's finalize (retiring shrunk-away partitions and clearing the
transition marker) waits for clients to adopt the new routing once the backlog
has drained. Adoption is observed from client topology acks. The timeout keeps a
silent or stuck client from stalling a cutover forever; publish version-fencing
is the correctness backstop regardless. See
[live routing and cutover](/development/live-routing-and-cutover/).

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

See [many idle queues](/concepts/many-idle-queues/) for the user-facing behavior.

### Connections

| TOML field | Env/CLI compatibility | Default | Meaning |
| --- | --- | --- | --- |
| `runtime_seed.connection.reconnect_grace_ms` | `FIBRIL_RECONNECT_GRACE_MS`, `--reconnect-grace-ms` | `5000` | Keeps a disconnected resumable client alive for this long before cleaning up subscriptions and requeueing unsettled messages. On by default so a transient blip resumes transparently; set `0` to disable. |

Reconnect grace is disabled when unset. It only helps clients that use the resume identity handshake.

### Replication

These settings apply to the experimental cluster replication path.

| TOML field | Default | Meaning |
| --- | --- | --- |
| `runtime_seed.replication.confirm_timeout_ms` | `5000` | How long a replica-durable publish confirm can wait for enough durable follower progress. |
| `runtime_seed.replication.caught_up_poll_ms` | `1000` | Follower pull interval while already caught up with the owner. Lower values can reduce idle replica-durable confirm latency at the cost of more wakeups. |
| `runtime_seed.replication.retry_poll_ms` | `100` | Follower retry interval after a partial pull or transient replication error. |
| `runtime_seed.replication.checkpoint_retry_poll_ms` | `5000` | Follower retry interval while it needs an owner checkpoint before it can continue. |
| `runtime_seed.replication.max_messages_per_read` | `256` | Maximum message records a follower asks the owner for in one pull. |
| `runtime_seed.replication.max_events_per_read` | `256` | Maximum event records a follower asks the owner for in one pull. |
| `runtime_seed.replication.max_bytes_per_read` | `8388608` | Approximate byte budget for one owner replication response. One oversized message can exceed it so replication still makes progress. |
| `runtime_seed.replication.max_iterations_per_tick` | `8` | Maximum pull/apply iterations a follower performs before yielding. |
| `runtime_seed.replication.min_in_sync_replicas` | `1` | Minimum recently in-sync replicas required before accepting replica-durable publishes. `1` disables the floor. |
| `runtime_seed.replication.isr_timeout_ms` | `10000` | How recently a follower must report durable progress to count as in sync. |
| `runtime_seed.replication.read_timeout_slack_ms` | `10000` | Slack added to a follower read's long-poll window before the read is abandoned and the connection dropped, so a dropped owner response cannot hang the follower. A read waits `max_wait_ms + this`. |
| `runtime_seed.replication.owner_connect_timeout_ms` | `5000` | Upper bound on a follower establishing a connection to an owner (TCP connect plus the handshake) before it is abandoned and retried. |
| `runtime_seed.replication.stream_enabled` | `true` | Use credit-based streaming replication on followers. When disabled, followers fall back to polling pulls. |
| `runtime_seed.replication.stream_apply_linger_us` | `2000` | Microseconds a streaming follower gathers contiguous frames before one fsynced apply. Higher trades apply latency for fsync amortization. `0` is drain-only. |
| `runtime_seed.replication.stream_apply_max_merge_bytes` | `16777216` | Byte cap on a single coalesced streaming apply (peak memory versus fsync amortization). |
| `runtime_seed.replication.stream_buffer_batches` | `8` | In-flight batch buffer depth (credit window) for the streaming follower. Applied on the next stream. |

The read-budget settings are useful when tuning replica-durable throughput.
Small values reduce per-tick work, while larger values let a follower catch up
faster when the owner is receiving sustained traffic.

### Partitioning and Consumer Groups

| TOML field | Default | Meaning |
| --- | --- | --- |
| `runtime_seed.partitioning.default_partition_count` | `1` | Partition count for a new queue declared without an explicit count. |
| `runtime_seed.consumer_groups.default_target_per_consumer` | unset | Optional soft target for exclusive consumer groups. When set, cohorts above the target can be reported as under-provisioned without reducing coverage. |

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

See [dead lettering](/reliability/dead-lettering/) for the setting
shape and current limitations.

## Validation

Current validation rules:

- `server.data_dir` must not be empty
- when `admin.auth.enabled = true`, `admin.auth.username` and `admin.auth.password` must both be set
- `storage.keratin.fsync_interval_ms` must be at least `1`
- `storage.keratin.message_log.segment_max_bytes` must be at least `1`
- `storage.keratin.event_log.segment_max_bytes` must be at least `1`
- `coordination.ganglion.heartbeat_interval_ms` must be at least `1`
- `coordination.ganglion.liveness_ttl_ms` must be at least twice `coordination.ganglion.heartbeat_interval_ms`
- `runtime_seed.delivery.expiry_batch_max` must be at least `1`
- `runtime_seed.idle_queue_cleanup.sweep_interval_ms` must be at least `1`
- `runtime_seed.replication` poll intervals and worker limits must be at least `1`
- `runtime_seed.replication.min_in_sync_replicas` must be at least `1`
- `runtime_seed.replication.isr_timeout_ms` must be at least `1`
- `runtime_seed.partitioning.default_partition_count` must be at least `1`

More validation will be added as more settings become user-facing.

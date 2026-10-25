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

[broker]
# Private brokers are never given to clients. Public brokers forward their
# clients' operations to them (cluster mode only).
# visibility = "private"
# The address other brokers dial for replication and forwarding, when it
# differs from the client advertise address.
# peer_advertise = "broker-2.internal:9876"

[broker.listener]
bind = "0.0.0.0:9876"

[admin.listener]
bind = "0.0.0.0:8081"

[admin.auth]
enabled = false
username = "fibril"
# password = "change-me"

[setup]
mode = false

[tls]
enabled = false
# Supply your own PEM files:
# cert_path = "/etc/fibril/tls/server.pem"
# key_path = "/etc/fibril/tls/server.key"
# Or generate per-deployment material under <data_dir>/node/tls on first boot:
# auto_self_signed = true
# The admin dashboard serves HTTPS from the same material when TLS is
# enabled. Opt it out when a reverse proxy terminates TLS for the dashboard:
# admin_enabled = false
# Follower-to-owner replication follows `enabled` too. Opt it out when a
# service mesh or tunnel already encrypts inter-broker traffic:
# inter_broker = false
# CA that peer certificates chain to. Unset falls back to the generated
# <data_dir>/node/tls/ca.pem, then OS roots:
# peer_ca_path = "/etc/fibril/tls/ca.pem"

[storage.keratin]
fsync_interval_ms = 5
# Floor between storage commits while the fsync worker is idle. 0 self-clocks
# group commit on fsync completions, the best setting for fast storage (NVMe,
# tmpfs). On slow-fsync storage such as SATA SSDs a floor around the fsync
# interval (5) gives the drive breathing room between write barriers.
min_fsync_interval_ms = 0
# Writer and notification channels each have 64 * factor slots per log.
# 128 preserves the default 8192 slots; smaller values reduce eager allocation.
writer_buffer_factor = 128
adaptive_staging = true
staging_decay_secs = 10
staging_idle_release_secs = 60

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
# How stale a persisted session skeleton a restarted broker will honor, so a
# fast restart lets clients resume and reconcile (60000 ms). Set to 0 to
# disable restart resume.
# resume_session_restart_ttl_ms = 60000
# Largest accepted message payload, 1 KiB to 15 MiB (8 MiB by default).
# max_message_bytes = 8388608
# Log a warning for payloads above this size. Set to 0 to disable it.
# warn_message_bytes = 1048576

[runtime_seed.replication]
confirm_timeout_ms = 5000
caught_up_poll_ms = 1000
retry_poll_ms = 100
checkpoint_retry_poll_ms = 5000
max_messages_per_read = 2048
max_events_per_read = 2048
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
| `broker.visibility` | `FIBRIL_BROKER_VISIBILITY` | none | `public` |
| `broker.peer_advertise` | `FIBRIL_BROKER_PEER_ADVERTISE` | none | first client advertise address, then `bind` |
| `admin.listener.bind` | `FIBRIL_ADMIN_BIND` | `--admin-bind` | `0.0.0.0:8081` |
| `admin.listener.advertise` | `FIBRIL_ADMIN_ADVERTISE` | none | derived (see below) |
| `admin.auth.enabled` | `FIBRIL_ADMIN_AUTH_ENABLED` | `--admin-auth-enabled` | `false` |
| `admin.auth.username` | `FIBRIL_ADMIN_USERNAME` | `--admin-username` | `fibril` |
| `admin.auth.password` | `FIBRIL_ADMIN_PASSWORD` | `--admin-password` | unset |
| `admin.metrics_per_channel` | `FIBRIL_ADMIN_METRICS_PER_CHANNEL` | none | `true` |
| `tls.enabled` | `FIBRIL_TLS_ENABLED` | none | `false` |
| `tls.cert_path` | `FIBRIL_TLS_CERT_PATH` | none | unset |
| `tls.key_path` | `FIBRIL_TLS_KEY_PATH` | none | unset |
| `tls.auto_self_signed` | `FIBRIL_TLS_AUTO_SELF_SIGNED` | none | `false` |
| `tls.admin_enabled` | `FIBRIL_TLS_ADMIN_ENABLED` | none | follows `tls.enabled` |
| `tls.inter_broker` | `FIBRIL_TLS_INTER_BROKER` | none | follows `tls.enabled` |
| `tls.peer_ca_path` | `FIBRIL_TLS_PEER_CA_PATH` | none | generated CA, then OS roots |
| `tls.client_auth` | `FIBRIL_TLS_CLIENT_AUTH` | none | `off` |
| `tls.client_ca_path` | `FIBRIL_TLS_CLIENT_CA_PATH` | none | generated CA |
| `setup.mode` | `FIBRIL_SETUP_MODE` | none | `false` |
| `auth.allow_default_loopback` | none | none | `true` |
| `auth.seed_users` | `FIBRIL_AUTH_USERNAME` + `FIBRIL_AUTH_PASSWORD` (one entry) | none | empty |
| `coordination.secret_path` | `FIBRIL_CLUSTER_SECRET_PATH` (or `FIBRIL_CLUSTER_SECRET` for the value) | none | unset |
| `storage.keratin.fsync_interval_ms` | `FIBRIL_KERATIN_FSYNC_INTERVAL_MS` | `--keratin-fsync-interval-ms` | `5` |
| `storage.keratin.min_fsync_interval_ms` | `FIBRIL_KERATIN_MIN_FSYNC_INTERVAL_MS` | `--keratin-min-fsync-interval-ms` | `0` |
| `storage.keratin.writer_buffer_factor` | `FIBRIL_KERATIN_WRITER_BUFFER_FACTOR` | none | `128` |
| `storage.keratin.adaptive_staging` | `FIBRIL_KERATIN_ADAPTIVE_STAGING` | none | `true` |
| `storage.keratin.staging_decay_secs` | `FIBRIL_KERATIN_STAGING_DECAY_SECS` | none | `10` |
| `storage.keratin.staging_idle_release_secs` | `FIBRIL_KERATIN_STAGING_IDLE_RELEASE_SECS` | none | `60` |
| `storage.keratin.message_log.segment_max_bytes` | `FIBRIL_KERATIN_MESSAGE_LOG_SEGMENT_MAX_BYTES` | `--keratin-message-log-segment-max-bytes` | `268435456` |
| `storage.keratin.event_log.segment_max_bytes` | `FIBRIL_KERATIN_EVENT_LOG_SEGMENT_MAX_BYTES` | `--keratin-event-log-segment-max-bytes` | `33554432` |
| `coordination.mode` | `FIBRIL_COORDINATION_MODE` | none | `static` |
| `coordination.ganglion.heartbeat_interval_ms` | `FIBRIL_COORDINATION_HEARTBEAT_INTERVAL_MS` | none | `3000` |
| `coordination.ganglion.liveness_ttl_ms` | `FIBRIL_COORDINATION_LIVENESS_TTL_MS` | none | `9000` |
| `coordination.ganglion.target_followers` | none | none | `1` |
| `coordination.ganglion.stream_replication_factor` | none | none | `1` |
| `coordination.ganglion.repartition_adoption_timeout_ms` | none | none | `30000` |
| `coordination.ganglion.controller_startup_grace_ms` | none | none | `30000` |
| `coordination.ganglion.assignment_durability` | `FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY` | none | `majority_durable` |
| `recovery.on_mismatch` | `FIBRIL_RECOVERY_ON_MISMATCH` | none | `quarantine` |

Changing these generally requires restarting the server.

`server.data_dir` is the root of everything a node keeps on disk:

```text
<data_dir>/
  fibril-node     layout marker with the layout version and node id
  node/           setup_complete, config-overlay.toml, cluster.secret,
                  tls/ (generated material), tls-provided/ (setup uploads)
  coordination/   default Ganglion data dir
  storage/        Stroma storage root for queue and stream data
  cache/          disposable, safe to delete while the node is stopped
```

A fresh data dir is missing, empty, or holds only `lost+found`, and Fibril
creates the layout on first start. An existing data dir must carry a
`fibril-node` marker with a known layout version and the configured
`coordination.node_id`. Startup refuses a data dir with an older layout, one
that belongs to a different node, or one with unknown top-level entries. There
is no migration between layouts yet, so drain the node and start it on a new
data dir instead. `coordination.ganglion.data_dir` defaults to
`<data_dir>/coordination`, and Fibril keeps a marker there tied to the raft
node id, so a coordination dir from another node is refused too.

`storage/` carries its own `stroma-layout` marker with the Stroma storage layout
version, which is 3. That version is separate from the node layout version in
`fibril-node`, which is 2, so a data dir can pass the node check and still have
its storage refused. Startup refuses a storage root with an older Stroma layout,
with no migration. Storage layout 3 also changes the coordination records for
queue learner admissions and reseeds, so upgrading a cluster to it means
draining the queues, starting every node on a fresh data dir and fresh
coordination state, and recreating the queues.

To place files such as `cluster.secret` or a shared CA before the first
start, prepare the data dir with `fibrilctl node init` (it uses the same
config and node id as the server), copy the files into `<data_dir>/node/`,
then start the server. Copying files into an empty data dir without this
step makes it look foreign and startup refuses it.

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

`broker.peer_advertise` is the single `host:port` other brokers dial for
replication and forwarding. The cluster node table carries it, while clients
get the `broker.listener.advertise` list. Unset, peers dial the first client
advertise address, then `bind`. Set it when the client address is a load
balancer address shared by several brokers (a peer dialing it would land on
any of them) or is not reachable from inside the cluster. With TLS, the peer
host joins the generated certificate names, and operator certificates must
cover it.

`broker.visibility` is `public` (the default) or `private`. A private broker is
never given to clients: it publishes no client addresses, so topology answers
and redirects never name it. Public brokers serve the partitions it owns by
forwarding client operations to it over internal, node-authenticated
connections, see [private brokers](#private-brokers). Private needs
`coordination.mode = "ganglion"` and fails validation in standalone mode. A
private broker still accepts ordinary client connections from clients that can
reach it, serves the partitions it owns and redirects the rest to public
brokers.

`admin.listener.advertise` is the same idea for the dashboard: the address a
node registers with the cluster so the Cluster page's "open its admin" links
and the broker switcher can point a browser at it. When unset and the admin
bind host is unspecified (`0.0.0.0`), the node reuses the broker advertise
host with the admin port; a raw unspecified address is never linked (the
dashboard shows "no reachable admin address registered" instead). In the
Docker cluster example each node advertises its host-mapped admin port.

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
policy for new assignments (`majority_durable`, the default, `local_durable`,
`replica_durable` or `replica_buffered`). `majority_durable` keeps every
confirmed message on a majority of copies, so a queue survives losing one
machine of three. On a two-node cluster a majority is both nodes, so confirms
pause while either is down. `local_durable` waits only for the owner. `replica_buffered` keeps the node
count of `replica_durable` and makes new queues buffered unless their declare
names a mode. A buffered queue confirms once the required nodes have written a
message to the OS, before fsync, so power loss on all of them at once can lose
recent confirmed messages. `replica_accepted` is deprecated, logs a warning and
behaves as `replica_durable`, as it always has.

`recovery.on_mismatch` controls what happens when recovery finds a damaged queue
log: `quarantine` (default) isolates the partition, `refuse` reports not ready,
and `ignore` truncates to the last valid record. See
[recovery quarantine](/reliability/recovery-quarantine/).

`admin.auth.enabled = true` requires both `admin.auth.username` and `admin.auth.password`.
The admin password is intentionally not shown in the dashboard startup summary.

`admin.metrics_per_channel` controls whether the Prometheus `/metrics`
endpoint on the admin listener includes per-channel series (queue depth,
stream subscriptions, follower applied state) alongside the always-exported
node-level aggregates. See [monitoring](/deployment/monitoring/).

`tls.enabled = true` serves TLS on the broker listener and the admin dashboard
from one certificate. Supply PEM files with `tls.cert_path` and `tls.key_path`,
or set `tls.auto_self_signed = true` to generate a per-deployment CA and server
certificate under `<data_dir>/node/tls` on first boot. The CA fingerprint is printed
at startup so clients can pin or trust it - self-signed material a client does
not verify defeats passive snooping only. Fibril never ships certificates.
`tls.admin_enabled = false` keeps the dashboard on plain HTTP for deployments
where a reverse proxy terminates TLS in front of it. Certificate material is
read once at startup, so replacing it requires a restart (live reload and
rotation are planned).

`tls.inter_broker` covers follower-to-owner replication and the coordination
raft channel. It follows `tls.enabled`, which assumes a homogeneous cluster
whose certificates chain to one CA - see
[TLS across nodes](/deployment/cluster/#tls-across-nodes) for the shared-CA
lane and the rotation runbook. Set it to `false` when a service mesh or
tunnel already encrypts inter-broker traffic. `tls.peer_ca_path` names the CA
peers are verified against, falling back to the generated
`<data_dir>/node/tls/ca.pem` when present, then OS roots. The serving certificate
and key reload live via `fibrilctl admin reload-tls` or
`POST /admin/api/tls/reload`.

`tls.client_auth` turns client certificates into credentials: `request`
verifies a presented certificate (certless clients still connect and
password-auth, the migration lane), `require` rejects certless clients in the
handshake. A verified certificate whose identity (first DNS SAN, else CN)
names an existing user authenticates as that user with no password; the `@`
node namespace can never be claimed by certificate. `tls.client_ca_path`
names the CA client certificates chain to, falling back to the generated CA -
issue workload certificates from it with `fibrilctl cert issue <identity>`.
Brokers present their own certificate on inter-broker dials, so a cluster
converges at any `client_auth` setting.

The `auth` section governs broker authentication. The built-in default
credentials (`fibril`/`fibril`) are accepted from loopback connections only,
so local development works out of the box while remote access always requires
a real user. `auth.seed_users` creates users when the user store is empty
(first boot only, after which the persisted store owns the users), and the
`FIBRIL_AUTH_USERNAME`/`FIBRIL_AUTH_PASSWORD` pair seeds one user from the
environment. A real user named `fibril` replaces the built-in pair entirely.
Passwords are stored as argon2 hashes. With users configured and TLS off, the
server warns loudly: passwords travel in cleartext on non-loopback plaintext
connections.

The cluster shared secret authenticates node-to-node connections (replication)
as a node principal, never as a user account, and is required in `ganglion`
mode. Resolution order: `FIBRIL_CLUSTER_SECRET` (the value itself), then
`coordination.secret_path`, then `<data_dir>/node/cluster.secret` if present -
which is what `fibrilctl secret generate` writes. Every node holds the same
secret.

`setup.mode = true` arms first-boot setup: when the data dir holds no
`node/setup_complete` marker, the server serves only a setup page on
`127.0.0.1:<admin port>` and the broker listener stays down until the operator
chooses generated TLS material, supplies a certificate, or explicitly
continues without TLS. The choice is written to `<data_dir>/node/config-overlay.toml`,
which boot layers below explicit config, so anything set in the `tls` section
by file, environment, or CLI always wins over a setup choice. Completion
writes the marker and the broker starts in the same process. Deleting the
marker and booting with setup mode runs setup again. Deployments that
configure TLS explicitly never need setup mode, and with it armed they boot
straight through (the marker is written automatically).

Queues store payloads and state transitions in one event log. That log uses the
`storage.keratin.message_log` capacity and batching settings because it carries
message bodies. Its default durability is the stronger of the message and event
profiles. Plexus streams retain a payload log and a cursor-event log, configured
through the two respective profiles.

`storage.keratin.message_log.segment_max_bytes` and `storage.keratin.event_log.segment_max_bytes` are rollover thresholds. A segment rolls after an append crosses the configured size, so an individual segment can be slightly larger than this value.

`storage.keratin.writer_buffer_factor` sets the startup capacity of each storage
writer input and notification channel to `64 × factor` entries. Valid factors
are `1..=128`: `1` gives 64 entries, `16` gives 1,024, and the default `128`
gives 8,192. The channels allocate their slot arrays when a log opens, so smaller
values reduce the resident cost of materialized queues and streams and apply
backpressure earlier during bursts. A channel entry may own a batch; this
setting is a slot limit rather than a total memory limit. Measure throughput
and tail latency with representative payloads, storage and replication before
choosing a lower value. Changing it requires restart and applies to both the
message and event logs. Fsync pipeline depth remains controlled by
`storage.keratin.max_inflight_fsyncs`; caches, actor mailboxes and client buffers
have separate limits.

`storage.keratin.adaptive_staging = true` (the default) enables lazy staging allocations for
both message and event logs. Write buffers start empty with a 64 KiB reservation
floor; sparse-index buffers use a 4 KiB floor. They grow to fit batches, may halve
while empty every `staging_decay_secs` (default 10), and release fully after
`staging_idle_release_secs` without use (default 60). Decay preserves headroom
for recent batches and never discards staged records. For less frequent bursts,
longer retention delays reduce repeated allocation work.

Set `adaptive_staging = false` to retain eager 16 MiB write and 256 KiB index
reservations per log. These sizes and adaptive floors are initial reservations,
not maximum capacities. This startup-only setting changes allocation retention;
channel sizes, caches and durability policy remain independently configured.
Released capacity may remain resident in the allocator, so RSS can stay high or
even increase for some burst patterns. Evaluate memory, CPU and first-message
latency after idle when choosing retention delays or opting out.

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

`coordination.ganglion.controller_startup_grace_ms` keeps partition owners in
place after a node becomes the coordination leader. Until every node registered
in the committed metadata has a fresh heartbeat, or the grace expires, the
controller moves no existing owner or follower. It still fills empty follower
slots and places new partitions. This stops a whole-cluster restart from moving
partitions away from a node that boots a few seconds after the others. The cost
is that a registered node that never comes back delays owner failover by up to
this long after every leader change. `0` disables the grace. While it is active,
the controller status in the admin topology response lists the nodes it is
waiting for under `consensus.controller.startup_grace_waiting_for`.

## Private Brokers

A cluster can run some brokers private, for example behind a load balancer or
a Docker Swarm service that publishes one address in front of the public
brokers. Clients then only ever talk to public brokers:

- Every partition owned by a private broker is advertised with the client
  addresses of one live, non-draining public broker, chosen by rendezvous
  hashing of the partition, so every broker gives the same answer.
- A public broker that receives an operation for such a partition forwards it
  to the owner on an upstream session opened for that client session. The
  owner answers exactly as it would answer the client: a forwarded publish is
  confirmed only when the owner confirms, with the owner's offset.
- Everything a client does on a partition is forwarded: confirmed and
  fire-and-forget publishes, delayed publishes, queue subscriptions, acks and
  nacks, stream publishes (each durability tier confirms as it would on the
  owner), stream subscriptions with durable cursors, start positions and
  filters, cursor commits, and exclusive consumer groups. For a consumer
  group the public broker settles one member id per client session and sends
  it to every owner, and the owners' assignment changes reach the client.
- When the owner is lost after a request was sent, the client gets error
  `504`: the outcome is unknown and clients do not replay it. When nothing was
  sent (owner unreachable, a forwarding limit, ownership moving), the client
  gets the retryable `503`.
- When the link to an owner breaks, the public broker resumes its session
  there for up to its own `reconnect_grace_ms`. A resumed session keeps its
  subscriptions, and deliveries held by the client stay settleable. If the
  owner no longer has the session, its subscriptions close with `OwnerMoved`
  and client supervisors resubscribe.
- A client that reconnects to the same public broker within its reconnect
  grace resumes its session, forwarded subscriptions included. Reconcile
  policies apply to forwarded subscriptions as to local ones: `restore`
  resubscribes them on the owner.
- When a public broker restarts or dies, its clients reconnect (through the
  load balancer) and resubscribe. An owner releases the old sessions at once
  when the public broker's process closed its connections, and after its own
  reconnect grace when the machine or network failed, then redelivers what
  they held.
- With no live public broker, private-owned partitions show no endpoints,
  each broker logs a warning once a minute, and the dashboard shows it on the
  Cluster page and in its attention list.

Mark brokers private only after every broker runs a version that understands
the `fibril/visibility` label. An older broker would still hand out a private
broker's peer address to clients.

The `routing` runtime settings bound the forwarding work of a public broker.
They are shared by the cluster and adjustable live from the settings page.

| Runtime setting | Default | Meaning |
| --- | --- | --- |
| `routing.max_upstream_sessions` | `4096` | Upstream sessions this broker keeps open to owners, one per client session and owner. At the cap new forwarded operations get `503`. |
| `routing.max_upstream_sessions_per_owner` | `1024` | Upstream sessions to any one owner. At the cap that owner's partitions get `503`. |
| `routing.max_inflight_forward_bytes` | `268435456` | Publish payload bytes sent upstream and not answered yet. A publish waits up to the forward timeout, at most a few seconds, for room and then gets `503`. |
| `routing.forward_timeout_ms` | `30000` | How long a forwarded request waits for the owner's answer. Past it the client gets `504` and the upstream session is closed. |
| `routing.upstream_connect_timeout_ms` | unset | Upper bound on opening an upstream session. Unset uses `replication.owner_connect_timeout_ms`. On expiry the client gets `503`. |
| `routing.upstream_idle_close_ms` | `30000` | An upstream session with no subscription and no pending request closes after this long. |

Forwarded traffic shows in `/metrics` as the `fibril_forward_*` families on
public brokers, with `fibril_forward_upstream_resumes_total` counting resumed
and lost upstream sessions, and `fibril_forwarded_sessions` on owners. The
Cluster page shows each public broker's upstream sessions and each owner's
forwarded sessions, and the connections view marks a forwarded session as
"via node P for user U".

The `storage` runtime setting sets how often queue logs sync records they have
written but not synced. Only buffered queues leave such records.

| Runtime setting | Default | Meaning |
| --- | --- | --- |
| `storage.background_sync_interval_ms` | `100` | Target interval between syncs of a queue log's unsynced records. Slow disks can stretch it. `0` turns it off and leaves syncing to the operating system, with no limit on how long a confirmed buffered message stays unsynced. Applies to open queue logs at once. |
| `storage.loaded_fsync_spacing_ms` | `2` | Least time between fsync starts on a log while an earlier fsync of that log is still running, from 0 to 1000. A log with no fsync running syncs at once, so light traffic is unaffected. Under load it merges many small syncs into fewer, for less CPU and steadier tail latency. `0` turns it off. Applies to open logs at once. |

The `publish` runtime settings set how a queue's owner gathers publish
requests from its connections into one append, separately for buffered and
durable queues. They apply to the next batch on every queue.

| Runtime setting | Default | Meaning |
| --- | --- | --- |
| `publish.buffered_coalesce` | `gap` | `window` waits up to 250 µs after the previous append for a batch of 32. Tokio rounds that up to the next millisecond, so a lone request can wait about 1 ms, and it makes the fewest appends. `gap` stops waiting once no request arrived for the gap, with a cap that grows with the batch up to 1 ms. From about 10k/s per queue it lowers latency for up to about 70% more CPU per message, and below 1k/s both behave the same. Gap needs Linux. Other platforms use the window |
| `publish.buffered_gap_us` | `100` | Quiet time after the last request that ends a batch in gap mode, from 1 to 1000 |
| `publish.durable_coalesce` | `window` | The same choice for durable queues. The disk sync dominates their latency, so smaller batches mostly cost CPU |
| `publish.durable_gap_us` | `100` | Quiet time that ends a durable batch in gap mode, from 1 to 1000 |

Both modes wait only when the previous append was recent, so a request after
a quiet period goes out at once.

## Linux Memory Policy

The Linux broker uses mimalloc, which exposes a startup environment option for
transparent huge pages (THP). Consider disabling THP when resident memory is a
deployment constraint, especially with many materialized queues whose log
buffers are lightly used. Huge pages can make a sparsely touched allocation
consume substantially more resident RAM; disabling them can also reduce the
footprint of an active broker.

```bash
MIMALLOC_ALLOW_THP=0 ./fibril-server
```

Append the usual broker arguments. Set the same variable in a container's
environment or a systemd service's `Environment=MIMALLOC_ALLOW_THP=0` directive,
then restart or recreate the broker. This allocator setting is read at process
startup and has no Fibril TOML field or admin runtime setting. It disables THP
for the broker process and its descendants without changing the host policy
for other services. Remove the variable and restart to restore the allocator's
default behavior under the host's policy.

The tradeoff depends on workload: huge pages can improve address-translation
efficiency for large active working sets, while allocating and clearing larger
pages can add latency and consume extra memory.

:::caution[Validate latency and CPU]
Compare resident memory, CPU, confirmation latency and delivery latency for the
intended workload before changing the policy. Mirrored local tests found lower
RSS at paced and saturated rates, including three-node request/reply; retained
buffer throughput was similar, while adaptive-buffer saturation and light-load
CPU showed potential costs. Tail-latency effects varied between runs. Behavior
under memory pressure, across hosts and with large active working sets remains
unmeasured, and the production default is unchanged.
:::

After restart, `/proc/<broker-pid>/status` should report `THP_enabled: 0`, and
`AnonHugePages` in `/proc/<broker-pid>/smaps_rollup` should be zero. These checks
confirm that the process policy took effect, including in restricted containers.
THP policy is independent of the writer channel factor and idle queue cleanup;
each addresses a different part of the materialized-resource footprint. See
the [memory investigation notes](/development/engineering-notes/#transparent-huge-pages--deployment-option)
and the upstream [mimalloc options](https://github.com/microsoft/mimalloc#environment-options)
and [Linux THP documentation](https://docs.kernel.org/admin-guide/mm/transhuge.html).

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
| `runtime_seed.connection.resume_session_restart_ttl_ms` | `FIBRIL_RESUME_SESSION_RESTART_TTL_MS`, `--resume-session-restart-ttl-ms` | `60000` | How stale a persisted session skeleton a restarted broker will honor. Within this window a resume after a broker restart succeeds and reconciles instead of being rejected; past it (or `0`) it reports not-found. Independent of `reconnect_grace_ms`, which governs live-process dormancy. |
| `runtime_seed.connection.max_message_bytes` | none | `8388608` | Largest message payload the broker accepts, between `1024` (1 KiB) and `15728640` (15 MiB). A larger publish is rejected with error `413` and the connection stays open. The upper bound keeps one message plus its overhead inside a 16 MiB recovery page, see [message size limits](/reliability/recovery-sealing/#message-size-limits). Adjustable live from the settings page. |
| `runtime_seed.connection.warn_message_bytes` | none | `1048576` | Publishes with a payload above this size are accepted but logged as a warning, at most once per topic per minute on each broker. `0` disables the warning. Adjustable live from the settings page. |
| `connection.drain_handoff_timeout_ms` (runtime settings) | none | `30000` | Upper bound on how long a drain call waits for partition ownership to hand off to caught-up followers in coordinated mode. Only caps the wait: reactive failover stays the backstop either way. Adjustable live from the settings page. |

Reconnect grace is disabled when unset. It only helps clients that use the resume identity handshake.

### Replication

These settings apply to the experimental cluster replication path.

| TOML field | Default | Meaning |
| --- | --- | --- |
| `runtime_seed.replication.confirm_timeout_ms` | `5000` | How long a replica-durable publish confirm can wait for enough durable follower progress. |
| `runtime_seed.replication.agreed_checkpoint_interval_ms` | `0` | Opt-in agreed queue checkpoints; 0 disables new attempts, otherwise 1000–86400000 ms between periodic attempts. Work thresholds can start earlier, at most once per second. Existing attempts finish and the last accepted base/suffix remain retained. All admitted replicas must agree. See [checkpoint policy](/development/failover-plan/#4-agreed-checkpoints-and-suffix-comparison). |
| `runtime_seed.replication.agreed_checkpoint_max_events` | `0` | Start an enabled checkpoint earlier after this many events beyond its agreed cut; 0 disables the event trigger. |
| `runtime_seed.replication.agreed_checkpoint_max_bytes` | `0` | Start earlier after approximately this many local append content bytes since observing the last certificate; 0 disables. The counter resets on reopen and excludes record framing; the periodic interval remains the fallback. |
| `runtime_seed.replication.eager_failover` | `false` | Opt-in explicit peer-connection failure detection on the active metadata controller. Recovery proof is still required; see [eager failover](/reliability/replication/#eager-failover). |
| `runtime_seed.replication.early_replication` | `true` | Owners of history-bound queues send written records to followers before their own fsync. Publish confirms still wait for owner durability. Adjustable live from the settings page; see [owner I/O failure](/development/recovery-continuation/#early-replication-under-owner-io-failure). |
| `runtime_seed.replication.eager_failover_grace_ms` | `1000` | Failed-reconnect grace, 0–60000 ms. Zero enables idle-disconnect monitoring and immediate reconnect verification on the active controller. Positive values require failed contact across the interval; heartbeat expiry handles silent failures. |
| `runtime_seed.replication.caught_up_poll_ms` | `1000` | Caught-up pull interval and streaming owner long-poll timeout. Streaming reads wake when new work arrives, without waiting for the timeout. |
| `runtime_seed.replication.retry_poll_ms` | `100` | Follower retry interval after a partial pull or transient replication error. |
| `runtime_seed.replication.checkpoint_retry_poll_ms` | `5000` | Follower retry interval while it needs an owner checkpoint before it can continue. |
| `runtime_seed.replication.max_messages_per_read` | `2048` | Maximum message records per owner read, for pull and streaming. |
| `runtime_seed.replication.max_events_per_read` | `2048` | Maximum event records per owner read, for pull and streaming. |
| `runtime_seed.replication.max_bytes_per_read` | `8388608` | Approximate byte budget per owner log scan and initial byte credit for a new follower stream. An oversized record and accompanying events can make the response exceed this budget. |
| `runtime_seed.replication.max_iterations_per_tick` | `8` | Maximum pull/apply iterations a follower performs before yielding. |
| `runtime_seed.replication.min_in_sync_replicas` | `1` | Minimum recently in-sync replicas required before accepting replica-durable publishes. `1` disables the floor. |
| `runtime_seed.replication.isr_timeout_ms` | `10000` | How recently a follower must report durable progress to count as in sync. |
| `runtime_seed.replication.read_timeout_slack_ms` | `10000` | Slack added to a follower read's long-poll window before the read is abandoned and the connection dropped, so a dropped owner response cannot hang the follower. A read waits `max_wait_ms + this`. |
| `runtime_seed.replication.owner_connect_timeout_ms` | `5000` | Upper bound on a follower establishing a connection to an owner (TCP connect plus the handshake) before it is abandoned and retried. |
| `runtime_seed.replication.stream_enabled` | `true` | Use credit-based streaming replication on followers. When disabled, followers fall back to polling pulls. |
| `runtime_seed.replication.stream_apply_linger_us` | `2000` | Microseconds a streaming follower gathers contiguous frames before one fsynced apply. Higher trades apply latency for fsync amortization. `0` is drain-only. |
| `runtime_seed.replication.stream_apply_max_merge_bytes` | `16777216` | Byte threshold for a coalesced streaming apply. Whole frames can cross it. Trades peak memory and overlap against fsync amortization. |
| `runtime_seed.replication.stream_buffer_batches` | `8` | Queued frame count for the streaming follower, separate from byte credit. Applied on the next stream. |
| `runtime_seed.replication.stream_initial_credit_bytes` | `0` | Experimental. Bytes a follower lets the owner push before its first progress report, up to 134217728. Zero uses `max_bytes_per_read`. Applied on the next stream. |
| `runtime_seed.replication.stream_apply_fixed_deadline` | `false` | Experimental. Collect frames for the whole `stream_apply_linger_us` window from the first frame instead of stopping at the next arrival. Adjustable live. |
| `runtime_seed.replication.stream_apply_open_batch` | `0` | Experimental follower batching mode. `0` off, `1` stages frames as they arrive under one sync, `2` collects first then uses the same sync path (a measurement control), `3` is `1` plus draining already queued frames without a fresh wait. Adjustable live. |

Streaming owner reads adopt updated record and byte limits at the next batch.
The initial credit window changes when a new follower stream starts. Oversized
batches consume that window and retain any excess as debt until the follower
returns credit after durable application.

Read budgets trade batch size and memory use against throughput. Measure both
throughput and latency under the intended workload before increasing them.

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

## Node-local Storage Settings

The dashboard's **Node-local Storage** section edits the node serving that
page. Use that node's dashboard address; this endpoint does not forward edits to
another node. These overrides remain local even in a coordinated cluster.

```http
GET /admin/api/local-storage-settings
PUT /admin/api/local-storage-settings
```

A PUT contains the node identity and revision returned by GET:

```json
{
  "node_id": "broker-1",
  "expected_version": 0,
  "segment_preallocate_bytes": 1048576
}
```

`segment_preallocate_bytes` sets how much space message, event and internal
metadata logs keep ready ahead of their write cursor, 8 MiB by default. Keratin
reserves it and fills it with synced zeros in the background, so a durable write
into it syncs without waiting for a filesystem journal commit. Zero disables
preallocation; `null` removes the override
and follows `storage.keratin.segment_preallocate_bytes` from startup configuration.
The override is durably saved before publication and survives restart. Startup
file/environment/CLI changes remain the fallback until the override is reset.
A stale revision returns `409 Conflict`; a different node identity is rejected.

Existing segments keep their policy. Each log adopts the latest revision when
creating or reopening an active segment, including recovery-created logs. Saving
does not force rollover or resize current segments, so a quiet log can remain
pending indefinitely. Future logs use the latest revision immediately. The shared
configuration is sampled at these boundaries, without synchronization per record.

The response reports the accepted revision, requested and startup values, pending
log count, and each open log's adopted revision and effective allocation chunk.
Filesystem allocation is best-effort: failure emits a warning, reports the error
and effective zero, and retains normal extending writes. A later segment retries
allocation. Closed logs are absent from the inventory. Applied status describes
policy adoption, not a change to publish durability or an all-logs transaction.

Fsync cadence, batching and adaptive-buffer settings remain startup-only. This
endpoint has the same administrator authentication as other settings endpoints.

## NVMe idle latency on Linux

Bursty workloads can encounter slower storage synchronization after an idle
gap. NVMe autonomous power-state transitions (APST) are one possible cause.
Compare smooth traffic and bursts at the same average rate when investigating
publish-confirmation latency. A desktop's performance power profile can leave
the NVMe controller's separate latency policy unchanged.

A September 2026 diagnostic on a Samsung 970 EVO Plus 2TB with Linux and ext4
measured roughly 9 ms median `sync_data()` latency after one-second gaps.
Temporarily disabling APST produced 2.69–2.73 ms medians in two repetitions of
the same single-log workload. The test alternated approximately 106 and 212 kB
writes, with 16 rounds per repetition and the first four excluded. These are
isolated file-sync measurements on one machine. Application latency also includes
batching, other persistence operations and replication. Test the policy on the
target hardware before choosing a deployment default.

### Temporary controller test

Identify the controller backing the broker's data filesystem. For example,
`nvme0n1p2` belongs to controller `nvme0`. Device numbering can differ across
machines and boots.

```sh
lsblk -o NAME,TYPE,FSTYPE,MOUNTPOINTS,MODEL

# Replace nvme0 with the controller identified above.
nvme_controller=nvme0
nvme_policy="/sys/class/nvme/$nvme_controller/power/pm_qos_latency_tolerance_us"
nvme_original=$(cat "$nvme_policy")
printf 'Original latency tolerance: %s\n' "$nvme_original"

# Save the original value, then temporarily disable APST.
printf '0\n' | sudo tee "$nvme_policy"
```

The Linux NVMe driver interprets zero as disabling APST. The control requires
administrator access and may be absent when APST is unavailable. It applies to
the controller and therefore affects other workloads using that controller.
Higher idle power consumption and temperature are the principal tradeoffs,
including reduced battery life on portable systems. Fsync and flush guarantees
remain unchanged. This is a host setting, separate from Fibril's node-local
storage API, and the command does not install a persistent boot configuration.

Repeat the workload with everything else unchanged, then restore the saved value
in the same shell and repeat again:

```sh
printf '%s\n' "$nvme_original" | sudo tee "$nvme_policy"
```

Record the controller, filesystem, power profile and policy with benchmark
results. Compare both latency and sustained performance before making the
setting persistent through deployment tooling. Keep the original value available
if the shell is closed before restoration.

The [Linux NVMe driver](https://github.com/torvalds/linux/blob/master/drivers/nvme/host/core.c)
implements this in `nvme_set_latency_tolerance` and `nvme_configure_apst`.
The [PM QoS documentation](https://docs.kernel.org/power/pm_qos_interface.html)
describes the per-device latency control.

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
- when `tls.enabled = true`, exactly one certificate source must be configured: both `tls.cert_path` and `tls.key_path`, or `tls.auto_self_signed = true`
- `tls.admin_enabled = true` requires `tls.enabled = true`
- `storage.keratin.fsync_interval_ms` must be at least `1`
- `storage.keratin.writer_buffer_factor` must be in `1..=128`
- Staging decay must be positive; idle release must be at least the decay interval and representable on the monotonic clock
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
- `broker.visibility = "private"` requires `coordination.mode = "ganglion"`
- `broker.peer_advertise`, when set, must be a `host:port` address
- the `routing` runtime limits and timeouts must be at least `1`

More validation will be added as more settings become user-facing.

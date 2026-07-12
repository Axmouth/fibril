---
title: Monitoring with Prometheus
description: Scrape broker metrics from the admin listener in Prometheus text exposition format.
---

Every broker serves `GET /metrics` on its admin listener in the Prometheus
text exposition format. The endpoint sits behind the same protections as the
rest of the admin surface: when admin auth is enabled the scrape needs basic
auth, and when TLS covers the dashboard the scrape happens over HTTPS. There
is nothing separate to enable.

A scrape is cheap by design. It reads atomic counters and the same snapshot
views the dashboard's live stream serializes, never a delivery hot path, so
scraping every few seconds does not affect broker throughput.

## Naming and semantics

Metrics are named `fibril_<subsystem>_<name>`, with `_total` on counters.
Counters are process-lifetime monotonic and reset when the broker restarts,
which is standard Prometheus counter semantics - use `rate()` and friends
rather than reading raw values. Labels only ever carry channel identity
(topic, group, partition) or an outcome tag, never message data.

## What is exported

Node-level aggregates, always present:

- Broker message flow: `fibril_broker_published_total`,
  `fibril_broker_delivered_total`, `fibril_broker_acked_total`,
  `fibril_broker_redelivered_total`, `fibril_broker_expired_total`.
- Storage operations: `fibril_storage_reads_total`,
  `fibril_storage_writes_total`.
- Transport: `fibril_tcp_connections_total`, `fibril_tcp_bytes_in_total`,
  `fibril_tcp_bytes_out_total`, `fibril_tcp_errors_total`, and the open
  gauges `fibril_tcp_connections_open` and `fibril_subscriptions_open`.
- Session resume: `fibril_tcp_resume_new_total`,
  `fibril_tcp_resume_accepted_total`, `fibril_tcp_resume_rejected_total`,
  the reconnect-grace counters, and
  `fibril_tcp_reconcile_outcomes_total{outcome=...}` for how resumed
  subscriptions reconciled.
- Recovery health: `fibril_recovery_quarantined` (a gauge - anything above
  zero means a partition is parked awaiting
  [repair](/reliability/recovery-quarantine/)) and
  `fibril_recovery_quarantines_total`.
- Replication workers, in cluster mode: `fibril_replication_followers`,
  `fibril_replication_followers_caught_up`,
  `fibril_replication_followers_pending_retry`,
  `fibril_replication_followers_checkpoint_required`.

Per-channel series, on by default and controlled by
`admin.metrics_per_channel`:

- `fibril_queue_ready{topic,group,partition}` and
  `fibril_queue_inflight{topic,group,partition}` - queue depth and leased
  messages per partition.
- `fibril_stream_subscriptions{topic,partition}` and
  `fibril_stream_lag_evictions_total{topic,partition}` - live stream
  subscribers and how often a lagging subscriber went through lag recovery.
- `fibril_replication_follower_applied_messages{topic,group,partition}` and
  `fibril_replication_follower_applied_events{topic,group,partition}` - how
  far this node's follower workers have applied, per followed partition.

Per-channel series come from materialized channels only, so a deployment
with [many idle queues](/concepts/many-idle-queues/) exports series for the
active ones, not the whole catalogue. If the active channel count itself is
large enough to strain the scraper, set `admin.metrics_per_channel = false`
(or `FIBRIL_ADMIN_METRICS_PER_CHANNEL=false`) and the endpoint serves the
node-level aggregates alone.

## Example scrape config

A minimal `prometheus.yml` job for a plaintext deployment without admin
auth:

```yaml
scrape_configs:
  - job_name: fibril
    static_configs:
      - targets: ["broker-1:8081", "broker-2:8081"]
```

With admin auth and TLS (the dashboard serves HTTPS from the broker's TLS
material - point Prometheus at the CA, which for generated material is
`<data_dir>/tls/ca.pem`):

```yaml
scrape_configs:
  - job_name: fibril
    scheme: https
    basic_auth:
      username: fibril
      password_file: /etc/prometheus/fibril-admin-password
    tls_config:
      ca_file: /etc/prometheus/fibril-ca.pem
    static_configs:
      - targets: ["broker-1:8081", "broker-2:8081"]
```

In a cluster, scrape every broker: each node exports its own view (the
partitions it owns and follows), and aggregation across nodes is a query
concern.

## Health endpoints

`/healthz` and `/readyz` live on the same listener without auth. `/readyz`
reflects quarantine state and the configured recovery policy, so it is the
right probe for load balancers and orchestrators. See
[failure modes](/reliability/failure-modes/) for how they behave under
faults.

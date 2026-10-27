---
title: Monitoring presets and history
description: Design for ready-made Grafana dashboards and Prometheus alert rules, the metrics they need, and telemetry history that survives restarts.
---

This is a proposed design. Nothing here is implemented yet. Current exports
are described in [monitoring](/deployment/monitoring/). The
[MCP server](/development/mcp-design/) uses the history described here.

## Problem

Fibril exports Prometheus metrics, but an operator starts from nothing: no
dashboards, no alert rules and no guidance on which numbers matter. The
exports also miss what matters most when something goes wrong: latency
distributions, fsync timings, process resources, disk levels and attention
items.

Inside the broker, the dashboard's history covers the last 30 minutes in
memory, the activity feed holds 512 entries in memory, and both start empty
after a restart. That is exactly when history is most useful.

## Ready-made presets

The repository ships, under `deploy/monitoring/`:

- **Grafana dashboards** as JSON, importable or provisioned:
  - *Cluster overview*: nodes, health, attention items, publish and delivery
    rates, backlog, latency percentiles, resource levels.
  - *Queue detail*, selected by topic and group: backlog trend, consumers,
    redeliveries, dead letters, replication state.
  - *Node and storage*: CPU, memory, threads, disk free and levels, fsync
    latency, segment rolls and spares.
  - *Replication*: in-sync sets, follower lag, partitions below their
    in-sync minimum, failovers.
- **Prometheus alert rules** with a runbook link on each, pointing at the
  docs section that explains the condition.
- **A compose example** that runs a three-node cluster with Prometheus and
  Grafana provisioned with the dashboards and rules, for trying it locally.

### One source for alerts

Every attention rule is exported as a gauge,
`fibril_attention_items{kind, severity}`, counting raised items. A single
alert rule per severity then covers every attention rule, and adding a rule
in the broker adds its alert without editing YAML. Further alert rules exist
only for conditions the broker cannot judge by itself, such as a node that
stopped answering scrapes.

### Keeping presets correct

A test parses every dashboard and rule file and checks that each referenced
metric and label exists in the broker's exporter. Renaming a metric fails the
build until the presets follow. The presets are generated where practical,
for example the attention runbook links come from the attention rule list.

## Metrics to add

| Metric | Type | Labels |
| --- | --- | --- |
| `fibril_publish_confirm_seconds` | Histogram | `durability` |
| `fibril_delivery_ack_seconds` | Histogram | none |
| `fibril_storage_fsync_seconds` | Histogram | none |
| `fibril_storage_segment_rolls_total` | Counter | none |
| `fibril_storage_spare_bytes` | Gauge | `filesystem` |
| `fibril_disk_free_bytes`, `fibril_disk_total_bytes` | Gauge | `filesystem` |
| `fibril_resource_level` | Gauge, 0 to 3 for ok to reserve | `resource` |
| `fibril_publishes_limited_total` | Counter | `reason` |
| `fibril_attention_items` | Gauge | `kind`, `severity` |
| `fibril_replication_lag_records` | Gauge | `topic`, `group`, `partition` |
| `process_*` | Standard process metrics | none |

Histograms are node-wide, not per queue, so their cost does not grow with
queue count. Per-queue latency belongs to the dashboard and MCP, which can
compute it on demand.

## Persistent history

Each node keeps its own telemetry in a small Keratin log under its data
directory, separate from queue storage.

- **What:** the samples behind the dashboard history, the activity feed and
  attention item transitions.
- **Resolution:** 5 s samples for the last 6 hours, 1 minute averages for 7
  days. Older data is truncated. These are proposed defaults, as runtime
  settings.
- **Size:** capped at a proposed 256 MiB per node. The log drops its oldest
  segments at the cap or at the 7 day limit, whichever comes first.
- **Writes:** buffered, synced on a timer, never on the message path. A lost
  last few seconds of telemetry after a crash is acceptable.
- **Reads:** the dashboard and MCP read history for a window. Cluster views
  ask each node for its part and mark nodes that did not answer.

The log counts toward the node's disk use and is the first thing trimmed
below the soft disk level, after spare segments, as described in
[resource limits](/development/resource-limits-design/).

Prometheus remains the long-term store. The persistent history covers the
common case of looking back across a restart or over the last few days
without one.

## Attention notes

An attention item seen by one operator is often unknown to the rest of the
team. Any item can carry a note, such as "Alice: renewing the certificate",
and a state:

- **Investigating**, with the note and who set it.
- **Snoozed until** a given time, for known conditions such as planned
  maintenance. A snoozed item stays listed but sorts last and does not fire
  its alert until the time passes.

Notes are a small cluster document keyed by the item's rule and subject, so
every node, the dashboard and the [MCP server](/development/mcp-design/) see
the same notes. A note is cleared automatically when its item resolves, and
every change is recorded in the activity feed with who made it. The exported
`fibril_attention_items` gauge carries a `state` label so alert rules can skip
snoozed items.

## Implementation order

1. `fibril_attention_items`, process metrics and the disk gauges.
2. Latency and fsync histograms.
3. Grafana dashboards, alert rules, the metric reference test and the
   compose example.
4. The telemetry log with downsampling, then the dashboard and activity feed
   reading from it.
5. Attention notes and snoozing.
6. Resource level and limited publish metrics with resource limits.

## Open questions

- Whether the compose example should live in the repository or in a separate
  examples repository.
- Whether the activity feed should be cluster-wide, replicated through
  coordination, instead of per node.

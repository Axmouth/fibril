---
title: Admin Dashboard
description: Use the Fibril admin UI for operational inspection and runtime settings.
---

The admin dashboard is for operators. It shows broker state, active connections,
queues, runtime settings, and message inspection tools.

The UI uses the same dark/light visual language as the public site and vendors
only the small icon set it needs.

The dashboard is not meant to be a high-frequency monitoring feed. Use it to
answer specific operational questions, then rely on metrics and logs for
continuous monitoring.

When broker TLS is enabled (`tls.enabled = true`) the dashboard serves HTTPS
from the same certificate material. Set `tls.admin_enabled = false` to keep it
on plain HTTP behind a reverse proxy that terminates TLS. See
[configuration](/configuration/) for the `tls` section.

## Overview And Diagnostics

The overview page is intentionally curated. It shows broker throughput, process
resource use, reconnect outcomes, and a small set of storage and queue health
signals that help answer whether queue processing or persistence is backing up.

The diagnostics page shows lower-level storage and queue metrics, including
command-lane depths and timings, command-kind counters, append stats, snapshot
cost, and recovery counters. Use it when the overview suggests pressure and you
need the next level of detail.

## Auth State

When admin authentication is configured, dashboard pages redirect unauthenticated
requests to `/login`, and the header shows a logout action.

When authentication is disabled, pages are accessible directly and the header
shows `Auth disabled`. Treat that mode as local-only or otherwise protected by
network boundaries.

## Queues

The queues page lists known queues with ready, inflight, and settled offset
information. Use **Inspect messages** from a queue row when you want to inspect
that specific queue.

You can **create a queue** from the page (partition count, an optional
dead-letter policy, and an optional default message TTL) and **delete a queue**
from its row. Delete is single-node: it is refused while a partition still has
inflight work, and refused in cluster mode pending coordinated teardown. A
**hide-inactive** toggle and a **search** filter help when the list is long.

For sparse workloads, the page also shows whether each queue is currently loaded
in memory, only indexed on disk, or recently unloaded after being idle. It shows
active publisher/subscriber counts, idle time when known, last used time for the
current process, and the most recent idle-cleanup result or skip reason.

Partitioned queues are shown as one row per topic with their partition and
loaded counts. Expand a topic to see each partition's own state, and each row
shows the queue's dead-letter policy.

When this broker is replicating queues from their owners, the page also shows a
follower-replication section: which partitions this broker follows, each
follower's status (caught up, pending retry, or checkpoint required), how far it
has pulled, and when it last made progress. See
[replication](/reliability/replication/).

The message inspection link starts near the queue's settled offset by default so
you do not begin at offset `0` on large queues unless you choose to.

## Streams

The streams page lists the [Plexus](/concepts/plexus-streams/) stream
channels this broker is currently hosting, grouped by topic. Each partition row
shows its head and tail offsets, how many records are retained, its live
subscription count, and how often a subscriber overflowed its live buffer and
went through lag recovery. The topic heading shows the declared durability
tier and retention bound.

You can **create a stream** from the page: a topic, a partition count, a
durability tier, and optional retention bounds (records, bytes, or age). A topic
is one channel kind for its lifetime, so declaring a stream over an existing
queue (or the reverse) is refused.

## Message Inspection

Message inspection reads queue state and persisted message data on demand. Use
it for debugging and operations, not as a live polling view.

Inspecting a queue can load it into memory. If idle queue cleanup is enabled and
no publisher or subscriber keeps that queue active, cleanup can unload it again
after the idle window.

By default, inspection shows active queue state:

- ready messages
- inflight messages
- delayed messages
- pending DLQ messages

Enable **Include settled offsets** when you also need persisted records that are
no longer active in queue state. Use the status filter when you only care about
one status, such as pending DLQ messages.

Payload previews are optional. They are base64 over the admin API and shown as a
short preview in the table. Use the payload modal for a larger preview. Large
page sizes and large payload previews ask for confirmation because they can read
a lot of persisted data.

## DLQ Replay

When inspecting a DLQ queue, select specific offsets and use **Replay selected
to source**. Replay copies the payload and user headers back to the recorded
source queue.

Replay does not remove or acknowledge the DLQ message. The result table reports
which offsets were replayed and which were skipped.

## Runtime Settings

The settings page shows live broker settings and storage-owned runtime settings
that can be updated while the process is running. Locked settings are shown as
locked and cannot be edited through the dashboard. This includes the replication
and streaming-replication settings when running in a cluster.

Saving a settings group sends every field in that group, so values you do not
change are preserved. Updates use a version check: if the settings changed since
you loaded the page, the save is rejected and you are asked to reload and resubmit.

Global DLQ target changes are persisted in storage-owned state and survive
restart.

## Subscriptions and Cohorts

The subscriptions page lists active subscriptions. When exclusive consumer groups
([cohorts](/concepts/consumer-groups/)) are in use, it also shows this
broker's view of each cohort: the topic, group, and the members with their
per-consumer targets. Cohort assignment is broker-local runtime state, so this is
a per-node view rather than a single cluster-wide table.

## Topology

When the broker runs in coordinated (Ganglion) mode, the topology page shows the
cluster: registered brokers, per-partition ownership with fencing epochs and
followers, and the consensus block (leader and voters). See
[clustering](/concepts/clustering/).

The page also exposes two operator actions, each with a confirmation:

- **Repartition** a queue by setting its partition count.
- **Add or remove a consensus voting member.**

Use these deliberately. Repartitioning changes placement, and voting-membership
changes affect quorum and availability.

## Health And Quarantine

`/healthz` is a simple liveness check. `/readyz` reflects readiness, including
whether any partition is quarantined after a failed recovery.

If a partition was quarantined because its log failed recovery verification, a
banner appears across the dashboard. From the banner you can repair the affected
partition, which truncates its log to the last valid record and clears the
quarantine. See [recovery quarantine](/reliability/recovery-quarantine/).

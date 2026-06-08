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

## Overview And Diagnostics

The overview page is intentionally curated. It shows broker throughput, process
resource use, reconnect outcomes, and a small set of Stroma timing and health
signals that help answer whether storage or queue actors are backing up.

The diagnostics page shows the wider Stroma metrics snapshot: command lane
depths and timings, command-kind counters, event-log append stats, snapshot
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

For sparse workloads, the page also shows whether each queue is currently loaded
in memory, only indexed on disk, or recently unloaded after being idle. It shows
active publisher/subscriber counts, idle time when known, last used time for the
current process, and the most recent idle-cleanup result or skip reason.

The message inspection link starts near the queue's settled offset by default so
you do not begin at offset `0` on large queues unless you choose to.

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
locked and cannot be edited through the dashboard.

Global DLQ target changes are persisted in storage-owned state and survive
restart.

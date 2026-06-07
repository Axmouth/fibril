---
title: Admin Dashboard
description: Use the Fibril admin UI for operational inspection and runtime settings.
---

The admin dashboard is for operators. It shows broker state, active connections,
queues, runtime settings, and message inspection tools.

The dashboard is not meant to be a high-frequency monitoring feed. Use it to
answer specific operational questions, then rely on metrics and logs for
continuous monitoring.

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

The message inspection link starts near the queue's settled offset by default so
you do not begin at offset `0` on large queues unless you choose to.

## Message Inspection

Message inspection reads queue state and persisted message data on demand. Use
it for debugging and operations, not as a live polling view.

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

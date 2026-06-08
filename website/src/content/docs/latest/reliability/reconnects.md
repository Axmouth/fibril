---
title: Reconnects
description: What reconnect grace does and what clients can rely on today.
---

Reconnect grace is for short network breaks where the broker process is still
running and the client can reconnect with the same resume identity.

When grace is enabled, a disconnected client is not cleaned up immediately. The
broker keeps the logical connection dormant for the configured window. If the
client reconnects with the resume identity before the window expires, the broker
reattaches the socket to that logical connection.

## What It Helps With

Reconnect grace can preserve server-side subscriptions long enough for late
settle requests to arrive after a brief TCP break.

For example:

1. A client receives a message.
2. The socket breaks before the client sends `ack`.
3. The client reconnects before grace expires.
4. The client sends the `ack` against the resumed logical connection.

In that case, the broker can accept the settlement instead of immediately
returning the message for redelivery.

## What It Does Not Do Yet

Current clients store resume identity and explicit `reconnect()` sends it, but
they do not yet run an automatic reconnect loop that hides a socket break from
existing publisher and subscription handles.

Existing handles are still tied to the connection engine that created them. If
that engine exits, those handles can fail or end, even if the client later
connects again.

Reconnect grace is also not durable restart recovery. If the broker process
restarts, the in-memory dormant connection state is gone.

## Configuration

Reconnect grace is controlled by the runtime setting
`connection.reconnect_grace_ms`.

It is disabled when unset. It can be seeded on first boot:

```toml
[runtime_seed.connection]
reconnect_grace_ms = 30000
```

You can also seed it with:

- `FIBRIL_RECONNECT_GRACE_MS`
- `--reconnect-grace-ms`

After runtime settings exist, edit the value from the admin settings page or
the runtime settings API.

## Current Client Signal

Rust and TypeScript explicit reconnect calls return the broker handshake
outcome. Use it to tell whether the broker actually resumed the previous
logical connection or started a fresh one.

If the outcome is not `resumed`, treat old subscriptions and unsettled local
work as unsafe to continue without a fresh application-level decision.

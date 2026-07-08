---
title: Reconnects
description: What reconnect grace does and what clients can rely on today.
---

Reconnect grace is for short network breaks where the broker process is still
running and the client can reconnect with the same resume identity.

This page documents current user-facing behavior. For the protocol design and
broker internals behind it, see the [reconnection grace development
note](/development/reconnection-grace/).

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

All five clients (Rust, TypeScript, Python, Go, and C#) store the resume identity
and make one conservative automatic reconnect attempt before a new publish,
subscribe, or declare operation when the previous engine is already known to be
closed. A connection is *known to be closed* when a socket read or write fails,
the peer sends EOF, an expected heartbeat is missed, or the TLS layer reports a
fatal alert. Rust, TypeScript, and Python additionally expose an explicit
`reconnect()` you can call yourself (see the support matrix below); Go and C#
reconnect only automatically.

Publisher handles created from a client use the latest connection engine after
explicit or automatic reconnect. New subscriptions created after reconnect also
use the latest engine.

After a successful resume, the clients send the broker the
subscription metadata they still believe is active. The broker compares it with
the server-side logical connection and replies with a reconciliation result.
When the broker reports that a subscription should be kept, the client routes
new deliveries for that subscription back into the existing subscription stream.
If the broker keeps the subscription with a different server subscription id,
the clients remap the existing stream to that id.

The default reconciliation policy is conservative:

- Subscriptions present on both sides with matching topic, group, partition,
  prefetch, and ack mode are kept.
- Subscriptions reported by the client but missing on the server are closed on
  the client.
- Subscriptions with conflicting metadata are closed on the client.
- Subscriptions present on the server but missing from the client are dropped by
  the broker, because no client stream is listening for them.

The clients also expose an opt-in restore policy. With that policy, a
client-owned subscription that is missing server-side is recreated by the
broker, and the existing client stream continues with the broker's new
subscription id. Metadata mismatches are still treated as unsafe.

The clients do not replay operations that were already in flight when the socket
failed. This avoids silently duplicating confirmed publishes whose frame may
have reached the broker before the confirmation was lost.

If resume is not accepted, or the broker reports that the client and server
disagree about a subscription, treat that stream as unsafe and recreate the
subscription at the application level.

Today the clients report a closed subscription as end-of-stream on their normal
receive API, each in its idiomatic shape: Rust `recv()` returns `None`,
TypeScript returns `null` or ends async iteration, Python returns `None` (or
raises `StopAsyncIteration`), Go closes the delivery channel, and C# ends the
`IAsyncEnumerable`. None of them yet attach a specific reconciliation-close reason
to that stream. A typed subscription lifecycle that carries the termination reason
is planned as part of the client API freeze (see the [reconnection grace
note](/development/reconnection-grace/#remaining-reconciliation-work)).

Reconnect grace is also not durable restart recovery. If the broker process
restarts, the in-memory dormant connection state is gone.

## Configuration

Reconnect grace is controlled by the runtime setting
`connection.reconnect_grace_ms`.

It is on by default (5000 ms) in the server seed, so a transient client blip
resumes transparently. Set it to 0 to disable, or seed a different window on
first boot:

```toml
[runtime_seed.connection]
reconnect_grace_ms = 5000
```

You can also seed it with:

- `FIBRIL_RECONNECT_GRACE_MS`
- `--reconnect-grace-ms`

After runtime settings exist, edit the value from the admin settings page or
the runtime settings API.

## Current Client Signal

Most reconnect behavior is uniform across the clients; two capabilities are not
yet, so rather than name clients inline this is the support matrix:

| Capability | Rust | TypeScript | Python | Go | C# |
|---|---|---|---|---|---|
| Resume identity + one automatic reconnect attempt before an op | yes | yes | yes | yes | yes |
| Subscription reconciliation on reconnect (conservative default) | yes | yes | yes | yes | yes |
| Opt-in restore policy | yes | yes | yes | yes | yes |
| Closed subscription surfaces as end-of-stream (no typed reason yet) | yes | yes | yes | yes | yes |
| No replay of in-flight operations | yes | yes | yes | yes | yes |
| Explicit `reconnect()` returning the handshake outcome | yes | yes | yes | no | no |
| Disable automatic reconnect | yes | yes | yes | no | no |

Where an explicit `reconnect()` is available it returns the broker handshake
outcome. Use it to tell whether the broker actually resumed the previous logical
connection or started a fresh one. If the outcome is not `resumed`, treat old
subscriptions and unsettled local work as unsafe to continue without a fresh
application-level decision. Go and C# perform the same one-attempt automatic
reconnect but do not surface an explicit call or a disable knob today.

Automatic reconnect is intentionally small everywhere: one attempt before a new
operation, not an unbounded background loop.

## Operator Visibility

The admin overview page exposes reconnect and subscription reconciliation
counters since broker start. Use them to tell whether clients are resuming,
being rejected, entering grace, expiring grace, keeping subscriptions, restoring
subscriptions, or having subscriptions closed during reconciliation.

The TCP metrics log also includes the same totals. Reconciliation completion is
logged with the client id, connection id, policy, and per-action counts.

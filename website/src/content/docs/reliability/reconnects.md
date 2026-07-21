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

## Mental model

The TCP socket is the transient part. The logical connection is what actually
owns your subscriptions and unsettled work, and it can outlive a socket:

```
Application code
      |  publish  .  subscribe  .  settle (complete / fail / retry)
      v
Logical connection ...... keyed by a resume identity; the broker keeps it
      |                    dormant through a brief break (the grace window)
      |  owns -> subscriptions -> in-flight delivery tags
      v
TCP socket .............. transient: it can drop and be replaced. A reconnect
                          presents the resume identity and re-attaches the new
                          socket to the SAME logical connection, so the
                          subscriptions and unsettled work above it survive.
```

When the socket drops and the client reconnects in time, only the bottom layer
was replaced. When grace expires (or resume is rejected), the whole logical
connection is torn down and its subscriptions and in-flight work are cleaned up
and requeued.

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

## Reconciliation and Subscription Lifecycle

All five clients (Rust, TypeScript, Python, Go, and C#) store the resume identity
and make one conservative automatic reconnect attempt before a new publish,
subscribe, or declare operation when the previous engine is already known to be
closed. A connection is *known to be closed* when a socket read or write fails,
the peer sends EOF, an expected heartbeat is missed, or the TLS layer reports a
fatal alert. Every client also exposes an explicit `reconnect()` you can call
yourself and a knob to disable the automatic attempt (see the support matrix
below).

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

A closed subscription never ends silently: every client carries a typed reason
so you always know why a stream stopped. When the broker ends a subscription
(the topic was deleted, ownership moved away, the broker is draining) it sends a
typed close, and a reconcile verdict that closes a subscription carries its
reason too. Each client surfaces it in its idiomatic shape:

- **Rust** `recv()` yields a `SubEvent` (`Delivery(msg)` or `Closed(reason)`);
  the terminal event is fused, so code that left the loop can still call
  `sub.close_reason()`.
- **TypeScript** and **Python** throw a typed `SubscriptionClosedError` from the
  subscription's iterator (a clean user `close()` still ends iteration quietly).
- **Go** closes the `Deliveries` channel and exposes `CloseReason()`, read after
  the channel closes.
- **C#** completes the `Deliveries()` enumeration with a typed
  `SubscriptionClosedException`.

Supervised subscriptions (the default) ride through the reasons that are safe to
retry: an owner move or a broker-advised recreate re-subscribes automatically.
Terminal reasons (the topic was deleted, a server error) stop the supervisor and
surface. Auto-resubscribe on a recreate is on by default and can be turned off,
in which case a recreate surfaces as a typed close instead.

### Durable restart resume

Reconnect grace covers a live broker. A broker *restart* is covered separately:
the broker persists a small skeleton of each resumable session, so a fast
restart lets a client resume and reconcile its subscriptions instead of being
rejected. The handshake reports `resumed_after_restart` for this case. Messages
redeliver per at-least-once, and delivery tags held from before the restart are
stale (the client marks them so, and settling one returns a typed error). This
is bounded by `connection.resume_session_restart_ttl_ms` (default 60s, `0`
disables it) and is independent of reconnect grace.

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
| Typed subscription close reason on the receive surface | yes | yes | yes | yes | yes |
| Auto-resubscribe on a broker-advised recreate (opt-out) | yes | yes | yes | yes | yes |
| No replay of in-flight operations | yes | yes | yes | yes | yes |
| Explicit `reconnect()` returning the handshake outcome | yes | yes | yes | yes | yes |
| Disable automatic reconnect | yes | yes | yes | yes | yes |

The explicit `reconnect()` returns the broker handshake outcome. Use it to tell
whether the broker actually resumed the previous logical connection or started a
fresh one. If the outcome is not `resumed`, treat old subscriptions and unsettled
local work as unsafe to continue without a fresh application-level decision. The
method is `reconnect()` in Rust, TypeScript, and Python, `Reconnect(ctx)` in Go,
and `ReconnectAsync(ct)` in C#. Disabling the automatic attempt makes a closed
connection surface its close error before the next operation instead of silently
redialing (`disable_auto_reconnect()` / `autoReconnectAttempts(0)` in the earlier
clients, `DisableAutoReconnect` in Go, `AutoReconnect = false` in C#).

Automatic reconnect is intentionally small everywhere: one attempt before a new
operation, not an unbounded background loop.

## Operator Visibility

The admin overview page exposes reconnect and subscription reconciliation
counters since broker start. Use them to tell whether clients are resuming,
being rejected, entering grace, expiring grace, keeping subscriptions, restoring
subscriptions, or having subscriptions closed during reconciliation.

The TCP metrics log also includes the same totals. Reconciliation completion is
logged with the client id, connection id, policy, and per-action counts.

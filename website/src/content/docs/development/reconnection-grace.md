---
title: Reconnection grace
description: Development plan for client reconnection and inflight reconciliation.
---

This is a development note covering the protocol design and broker internals. The
current user-facing behavior and per-client support matrix live on the
[Reconnects](/reliability/reconnects/) page.

## Problem

Today a TCP connection gets a server-issued `client_id` during HELLO. That id is
useful, but it is only returned to the client. A later connection cannot yet say
"I am the same client coming back".

When a connection closes, the protocol handler unsubscribes its consumers. The
broker removes those subscriptions and requeues broker-tracked inflight
messages. That is correct for normal disconnects, but it gives no grace period
for a client that briefly loses its socket and reconnects with the same local
state.

The desired behavior is:

- client gets a server-issued identity
- client reconnects with that identity after a transient break
- broker can recognize the returning client
- late settle requests from the old connection can be accepted during a short
  grace window
- delivery resumes cleanly after reconciliation
- logs make it clear whether a client resumed, missed the grace window, or was
  treated as a fresh connection

## First Principles

The server should still issue the identity. Clients should not mint identities
that the broker trusts blindly.

The first version should be broker-memory scoped. It targets transient network
breaks while the broker process remains alive. Durable restart reconciliation is
a separate feature because it needs persisted client and subscription ownership,
and that metadata must be designed carefully to avoid bloating every message or
every hot-path state transition.

Grace is a reliability tool, not a replacement for at-least-once semantics. If
the client does not reconnect in time, messages should still become deliverable
again through the normal paths.

## Current State

Protocol:

- HELLO carries client name, client version, protocol version, and optionally a
  previously issued resume identity.
- HELLO OK returns protocol version, a server-issued `client_id`, resume token,
  owner id, and a resume outcome.
- The protocol has a subscription reconciliation metadata frame and result
  frame.

Broker:

- consumer handles contain `client_id`, `sub_id`, topic, group, and partition.
- inflight broker deliveries are tracked by delivery tag and include queue key,
  offset, and consumer id.
- unsubscribe removes the consumer and requeues matching inflight offsets.
- `next_sub_id` is process-local and starts from `1`.
- reconnect grace can keep a logical connection dormant instead of immediately
  unsubscribing it.
- after a successful resume, the broker can compare client-reported
  subscription metadata with the dormant logical connection.

Storage:

- Stroma owns queue state and durable inflight state.
- It does not currently persist client id or subscription id ownership for
  inflight messages in a way that can rebuild broker connection state after
  process restart.

Future topology:

- Fibril is not currently sharded or replicated.
- The design should still leave room for clients to hold multiple connections
  to different partition owners later.
- Resume identity should be scoped so one partition owner cannot accidentally
  claim another owner's live connection state.

## Implemented Resume Identity

Add explicit resume identity to the connection handshake.

HELLO can optionally include a previously issued `client_id`, owner id, and
resume token. HELLO OK reports whether the connection is new or resumed, and
returns the identity the client should keep for the next reconnect attempt.

The token matters because `client_id` alone is a public value once handed to the
client. A random opaque resume token issued by the server gives the reconnecting
client something harder to guess. For pre-0.1, this can be a simple random value
kept in broker memory. Later it can become persisted or signed if needed.

Current outcomes:

- `new`
- `resumed`
- `resume_not_found`
- `resume_rejected`

This work item does not move deliveries yet. It establishes the wire shape, server
identity registry, client API storage of the resume identity, and logs.

## Preferred Grace Model

On socket close, do not immediately destroy the logical connection if reconnect
grace is enabled. Move it into a dormant state.

The dormant state is the old logical connection waiting for proof that the same
client came back. If the client proves ownership before the deadline, resume or
reconcile. If the deadline expires, finalize the disconnect and run the same
cleanup that would have happened immediately today.

This model is clearer than "reconnect and recreate everything" because the
broker has one place to hold old subscription and inflight ownership until a
decision is made.

## Implemented Grace Window

The broker now has broker-side dormant logical connections when reconnect grace
is enabled for the TCP handler.

On eligible disconnect, instead of immediately requeueing all inflight messages,
the broker keeps a short-lived logical connection:

- `client_id`
- resume token
- subscriptions that were active
- broker delivery tags still associated with those subscriptions
- a replaceable transport sink for the currently attached socket

During the grace window, the old subscription state stays alive. Late
settlements using existing delivery tags work after a successful resume. If the
client does not return before the grace window expires, the broker runs normal
unsubscribe cleanup and requeues unsettled inflight messages.

Subscription delivery tasks no longer write directly to a single socket. They
send through the logical connection's current transport sink. When a socket is
gone, the sink is set to unavailable and delivery waits until either a resume
attaches a new sink or grace expiry aborts the subscription and requeues
leftovers.

Current limits:

- Reconnect grace is controlled by the live runtime setting
  `connection.reconnect_grace_ms`.
- The setting is seeded by `runtime_seed.connection.reconnect_grace_ms` when no
  persisted runtime settings document exists yet.
- Existing subscriptions are preserved server-side, and clients send
  subscription metadata after a successful resume.
- Existing Rust, TypeScript, Python, Go, and C# publisher handles use the latest engine
  after explicit or automatic reconnect. New subscriptions also use the latest
  engine.
- Active subscription streams continue when reconciliation returns `keep`.
- Conservative reconciliation drops server-only subscriptions and closes
  client-only or mismatched subscriptions client-side.
- The opt-in restore-client-subscriptions policy recreates client-owned
  subscriptions that are missing server-side and remaps the stream to the new
  server subscription id.
- Rust, TypeScript, Python, Go, and C# clients make one automatic reconnect attempt
  before a new operation when the old engine is already known closed.
- In-flight protocol requests from the old socket are not replayed.
- Admin overview counters and TCP metrics logs expose resume, grace, and
  reconciliation outcomes since broker start.
- Reconciliation completion logs include client id, connection id, policy, and
  action counts.

Durable restart recovery and in-flight delivery reconstruction can come later.

## Implemented Subscription Reconciliation Metadata

After a successful resume, Rust, TypeScript, Python, Go, and C# clients send a
lightweight subscription reconciliation frame. This frame tells the broker which
subscriptions the client still believes belong to the resumed logical
connection.

The current client frame includes:

- reconciliation policy
- topic
- group
- sub id
- partition
- auto-ack mode
- prefetch

The broker replies with one result per subscription:

- `keep`
- `close_client_side`
- `close_server_side`
- `recreate_client_side`

The result also carries the client and server view that caused the decision, plus
a short reason string. The broker currently uses this to report matched,
missing, or mismatched subscription metadata. Clients read the result after
resume. When the result says `keep`, they attach the existing subscription
stream to the new engine so later deliveries keep flowing to the same user-facing
receiver.

The current policy is conservative:

- If both sides have matching topic, group, partition, auto-ack mode, and
  prefetch, the result says to keep it. If only the server subscription id
  changed, clients remap to the server id.
- If the client reports a subscription the server no longer has, the result
  says to close it client-side.
- If both sides disagree on topic, group, partition, auto-ack mode, or prefetch,
  the result says to close it client-side.
- If the server has a subscription that the client did not report, the broker
  drops it and reports `close_server_side`.

Current Rust, TypeScript, Python, Go, and C# receive APIs do not carry a typed
reconciliation close reason. A closed subscription stream ends normally from the application
view, so a future client-surface pass should decide whether to change the
pre-0.1 receive API or add an explicit subscription status channel.

The opt-in restore-client-subscriptions policy differs only for client-only
subscriptions. The broker recreates the missing subscription, returns `keep`,
and includes the new server subscription id for the client to use.

This is enough to avoid silent disagreement without pretending to solve durable
restart recovery.

## Remaining Reconciliation Work

Active stream recovery handles the `keep` case. The remaining user-facing work
is clearer feedback when a stream cannot be kept, and a possible automatic
resubscribe path for safe cases where the broker says to recreate the
subscription.

Design decision (2026-06-29): both of those are facets of one model - a typed
subscription lifecycle whose termination reason travels WITH the stream, not on a
side channel. When a subscription ends, the receive surface itself yields the
reason (unsubscribed, client shutdown, reconciled-closed, recreated, disconnected,
broker error). A reason is intrinsically about that one subscription and the app
learns the stream ended at the receive point, so an out-of-band broadcast would
force the app to correlate `(topic, group, partition)` racily - the wrong shape.
The high-level client auto-handles the recoverable cases (auto-reconnect,
auto-resubscribe on recreate), so a typed termination is the escape hatch for the
cases it cannot recover. This changes the receive surface, so it is designed once
as part of the client API freeze rather than bolted on early: the reconcile
close-reason and the auto-resubscribe path are variants of this single lifecycle.
A separate ops firehose of reconcile decisions (metrics/observability) is a
distinct concern and, if added, stays a broadcast - it is not this control-flow
surface.

Inflight delivery reconciliation is also not done yet. A future version may need
additional frames or fields for:

- delivery tag
- offset
- local processing status
- local settle status
- lease deadline

If a server-to-client reconciliation view is added later, keep it separate from
the client-to-server frame so each direction has a clear owner and result.

## Durable Restart Scope

Broker restart reconciliation means restarting a broker process and letting
clients continue as if the TCP break was the only interruption. That is larger
than network-blip grace.

Current reconnect grace is intentionally live-process only. A broker restart
destroys the in-memory dormant connection state, so old resume identities are
not enough by themselves. A durable restart design needs its own persistence and
startup behavior.

To support it properly, storage likely needs to persist enough ownership
metadata to recover client, subscription, and inflight relationships:

- client id
- resume token or another proof that the reconnecting client owns that session
- subscription id or a stable subscription identity
- delivery tag or durable delivery sequence
- queue key and offset
- lease deadline
- ack mode and prefetch

There are two viable subscription-id directions:

- Persist subscription ids and initialize `next_sub_id` above the maximum
  recovered id.
- Treat subscription ids as process-local and use reconciliation to remap
  client streams to fresh server ids after recovery.

The second direction is simpler and matches the current remap behavior. It
still needs a stable way to identify the logical subscription, such as client
identity plus topic, group, partition, ack mode, and prefetch.

The inflight model is the hard part. State needs to answer which client or
session leased an offset, which logical subscription received it, which delivery
tag was issued, and whether a late ack or nack is valid after reconnect or
restart. Late settles should be idempotent, and stale settles from an old owner
epoch should be rejected or ignored predictably.

A durable restart implementation likely needs a startup grace mode:

1. Recover queues and persisted runtime state.
2. Accept reconnects from known clients for a bounded grace window.
3. Let clients send subscription reconciliation and late settle requests.
4. Keep reclaimed inflight work leased to the recovered client.
5. Release unreclaimed inflight work back to normal delivery after grace
   expires.

That metadata should not be attached blindly to every common message path if it
can be avoided. It should be sparse, compact, and only paid for by behavior that
needs restart reconciliation.

For the first implementation, keep durable restart out of scope and document
that grace only applies while the broker process remains alive.

## Planned Restart Announcement (Drain)

Grace today is reactive. The broker only learns a connection is gone when the
socket drops, then holds dormant state and waits for the client to return. A
planned restart can be cleaner if the broker announces it ahead of time instead
of vanishing.

Before a graceful shutdown or upgrade, the broker pushes a drain notice to its
connected clients: it is going away and will hold their sessions for a bounded
window. A client that receives it can stop issuing new operations on that
connection, let in-flight work settle, enter its resume path proactively rather
than after a dropped socket, and redirect to another owner where one exists in
cluster mode.

The broker then drains: stop accepting new work, let leases settle or hand them
off, flush durable state, and exit. On restart it comes back inside the durable
restart startup grace window above, and the announced clients resume against
their persisted sessions, so the restart is transparent to in-flight work.

This pairs with durable restart reconciliation to make rolling upgrades a
first-class operation: announce, drain, restart, resume, with no client-visible
interruption beyond a brief pause. It needs a server-initiated push frame (a
"going away" notice with a suggested grace deadline), client handling for that
frame, and broker drain sequencing on shutdown.

## Sharding and Replication Pressure

The first version should work in a single broker process, but avoid design
choices that make sharding awkward.

Design guardrails:

- Do not make one `client_id` mean "this broker owns all state for this client".
- Do not put all client inflight state behind one global client record that
  would later need cross-node locking.
- Keep dormant state keyed by owner scope plus client identity.
- Keep queue and partition identity in reconciliation data, even while
  partition is still internally `0`.
- Avoid protocol fields that assume a client has only one broker connection.

If partitions later have owners, a client may hold multiple connections at once,
one per owner. A resume identity should therefore be either:

- scoped to one broker or partition owner, or
- globally unique plus paired with the expected owner or partition set

For the network-blip implementation, server-issued `client_id` plus resume token is
enough. Later partition-aware clients can keep one resume identity per
connection or owner.

This is now implemented. Each broker mints an `owner_id` and rejects a resume
whose `owner_id` does not match, so one owner cannot claim another owner's
session even if a client presents the wrong identity. The Rust, TypeScript, and
Python clients store resume identity per connection and key their connection pool
by endpoint, so each owner connection carries that owner's own identity and a new
owner connection starts fresh.

## Logging

Add structured logs for:

- new client identity issued
- resume accepted
- resume rejected with reason
- reconciliation result counts by action
- client entered grace window
- grace expired
- late settle accepted during grace
- late settle rejected with reason
- inflight messages requeued after grace
- future durable restart grace started and ended
- future durable restart inflight work reclaimed or released

These logs should include client id, connection id, and counts. Avoid logging
payloads.

## Tests

Protocol tests:

- HELLO without resume returns a new identity
- HELLO with valid resume returns resumed
- HELLO with unknown or expired resume returns a clear outcome

Broker tests:

- disconnect without grace still requeues
- disconnect with grace keeps inflight without immediate requeue
  (implemented)
- dormant consumers are skipped by delivery
- late settle after resume during grace is accepted (implemented)
- grace expiry requeues remaining inflight messages (implemented)
- reconnect after expiry is treated as fresh

Client tests:

- Rust, TypeScript, Python, Go, and C# clients store resume identity after connect
  (implemented)
- reconnect sends resume identity when available (implemented)
- reconnect handles rejected resume by falling back to fresh state

## Resolved Policy Decisions

- Resume identity is always issued and the clients always send it on reconnect.
  Reconnect grace (keeping the logical connection dormant) is server-controlled
  by `connection.reconnect_grace_ms`. The broker library defaults it off (None ->
  immediate cleanup) to stay conservative, but the `fibril-server` product seed
  enables it by default at `DEFAULT_RECONNECT_GRACE_MS` (5s). Operators opt out
  by setting the value to 0. So resume is enabled by default at the product level
  and remains a single server-side knob, not a per-client one.
- Auto-ack subscriptions participate in grace. The dormant logical connection
  preserves all subscriptions regardless of ack mode. Auto-ack holds no inflight
  to retain, so grace simply preserves the subscription and avoids resubscribe
  churn on a transient blip - cheap and worth keeping uniform with manual-ack.
- Default grace window: 5s (`DEFAULT_RECONNECT_GRACE_MS`). Long enough to cover a
  transient network blip and the client's automatic reconnect, short enough that
  the redelivery delay for a genuinely dead consumer stays small. At-least-once
  holds either way - grace only delays redelivery, never drops it.
- Automatic resubscribe on `recreate_client_side` is a separate client-surface
  follow-up (see Remaining Reconciliation Work and task #103), not gated here.
- Late settles after the grace window: best-effort. Once grace expires the
  inflight is requeued, so a late ack/nack references a retired lease and is
  applied idempotently against storage (it does not double-settle a redelivered
  message). A dedicated rejection error code is deferred to the inflight delivery
  reconciliation work (task #104), which is where the per-delivery lease identity
  needed to reject precisely will live.

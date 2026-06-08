---
title: Reconnection grace
description: Development plan for client reconnection and inflight reconciliation.
---

This is a development note. User-facing behavior should be documented separately
once the feature exists.

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

- HELLO carries client name, client version, and protocol version.
- HELLO OK returns protocol version and a server-issued `client_id`.
- There is no resume field in HELLO.
- There is no reconciliation frame.

Broker:

- consumer handles contain `client_id`, `sub_id`, topic, group, and partition.
- inflight broker deliveries are tracked by delivery tag and include queue key,
  offset, and consumer id.
- unsubscribe removes the consumer and requeues matching inflight offsets.
- `next_sub_id` is process-local and starts from `1`.

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

## Implemented First Slice

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

This slice does not move deliveries yet. It establishes the wire shape, server
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
- Existing subscriptions are preserved server-side, but clients do not yet have
  an automatic reconnect loop that hides the socket break.
- In-flight protocol requests from the old socket are not replayed.

Transparent subscription restore and user-facing client automation can come
later.

## Proposed Third Slice

Add a reconciliation frame.

The client should be able to send what it believes is still inflight locally:

- topic
- group
- old sub id
- delivery tag
- offset

The broker replies with what it accepts:

- still valid
- already settled
- requeued
- unknown
- expired

This lets a client decide whether to keep processing, drop local inflight work,
or expect redelivery.

## Durable Restart Scope

Broker restart reconciliation means restarting a broker process and letting
clients continue as if the TCP break was the only interruption. That is larger
than network-blip grace.

To support it properly, storage likely needs to persist enough ownership
metadata to recover client and subscription relationships:

- client id
- subscription id or a stable subscription identity
- delivery tag or durable delivery sequence
- queue key and offset
- lease deadline

That metadata should not be attached blindly to every common message path if it
can be avoided. It should be sparse, compact, and only paid for by behavior that
needs restart reconciliation.

If subscription ids are persisted, `next_sub_id` must be initialized above the
maximum recovered id. If subscription ids remain process-local, reconciliation
should rely on client id plus queue and offset instead.

For the first implementation, keep durable restart out of scope and document
that grace only applies while the broker process remains alive.

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

For the network-blip slice, server-issued `client_id` plus resume token is
enough. Later partition-aware clients can keep one resume identity per
connection or owner.

## Logging

Add structured logs for:

- new client identity issued
- resume accepted
- resume rejected with reason
- client entered grace window
- grace expired
- late settle accepted during grace
- inflight messages requeued after grace

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

- Rust and TypeScript clients store resume identity after connect
  (implemented)
- reconnect sends resume identity when available (implemented)
- reconnect handles rejected resume by falling back to fresh state

## Open Questions

- Should resume be opt-in per client, enabled globally, or enabled by default?
- Should auto-ack subscriptions participate in grace, or only manual-ack
  subscriptions?
- Should the server restore subscriptions automatically, or should clients
  resubscribe after a successful resume?
- How long should the default grace window be?
- Should late settles after the grace window return a specific error code?

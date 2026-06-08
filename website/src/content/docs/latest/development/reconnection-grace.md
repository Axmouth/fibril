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

The first version should be broker-memory scoped. It helps transient TCP breaks
while the broker process remains alive. Durable restart reconciliation is a
separate feature because it needs persisted client and subscription ownership.

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

## Proposed First Slice

Add explicit resume identity to the connection handshake.

HELLO should optionally include a previously issued `client_id` plus a resume
token. HELLO OK should report whether the connection is new or resumed.

The token matters because `client_id` alone is a public value once handed to the
client. A random opaque resume token issued by the server gives the reconnecting
client something harder to guess. For pre-0.1, this can be a simple random value
kept in broker memory. Later it can become persisted or signed if needed.

Suggested outcomes:

- `new`
- `resumed`
- `resume_not_found`
- `resume_expired`
- `resume_rejected`

This slice does not need to move deliveries yet. It should establish the wire
shape, client API storage of the resume identity, and logs.

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

## Proposed Second Slice

Add broker-side dormant client records.

On eligible disconnect, instead of immediately requeueing all inflight messages,
the broker records a short-lived dormant client entry:

- `client_id`
- resume token
- disconnect deadline
- subscriptions that were active
- broker delivery tags and offsets still associated with those subscriptions

During the grace window, the broker should avoid normal redelivery of those
specific broker-tracked messages. Late settlements using existing delivery tags
should still work.

The broker also needs to pause old consumers while they are dormant. The current
protocol task owns the receiving side of each delivery channel. Once the socket
is gone, that receiver is not a usable delivery target. A dormant consumer should
not receive more deliveries until it is resumed or replaced.

If the client resumes, the broker can either:

- bind the returning connection to the existing client record and let it settle
  old tags, or
- re-create subscriptions and reconcile what remains

The first version should prefer the smaller behavior: accept late settles during
grace, pause old consumers, and let the client re-create subscriptions
explicitly. Transparent subscription restore can come later.

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

Broker restart reconciliation is larger.

To support it properly, storage likely needs to persist enough ownership
metadata to recover client and subscription relationships:

- client id
- subscription id or a stable subscription identity
- delivery tag or durable delivery sequence
- queue key and offset
- lease deadline

If subscription ids are persisted, `next_sub_id` must be initialized above the
maximum recovered id. If subscription ids remain process-local, reconciliation
should rely on client id plus queue and offset instead.

For the first implementation, keep durable restart out of scope and document
that grace only applies while the broker process remains alive.

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

- disconnect without resume still requeues
- disconnect with grace records inflight without immediate requeue
- dormant consumers are skipped by delivery
- late settle during grace is accepted
- grace expiry requeues remaining inflight messages
- reconnect after expiry is treated as fresh

Client tests:

- Rust and TypeScript clients store resume identity after connect
- reconnect sends resume identity when available
- reconnect handles rejected resume by falling back to fresh state

## Open Questions

- Should resume be opt-in per client, enabled globally, or enabled by default?
- Should auto-ack subscriptions participate in grace, or only manual-ack
  subscriptions?
- Should the server restore subscriptions automatically, or should clients
  resubscribe after a successful resume?
- How long should the default grace window be?
- Should late settles after the grace window return a specific error code?

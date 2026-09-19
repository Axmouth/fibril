---
title: Reconnection grace
description: Resume identity, logical connections, reconciliation and restart recovery.
---

Fibril separates the TCP socket from the logical connection that owns queue
subscriptions and unsettled deliveries. A logical connection can survive a short
socket interruption. Persisted session metadata also supports reconciliation
after a broker restart, with different settlement guarantees.

The [reconnect guide](/reliability/reconnects/) describes client-facing behavior.
[Implemented surface](/implemented-surface/#reconnects) records support and limits.

## Resume identity

HELLO accepts a previously issued client id, owner id and opaque resume token.
HELLO OK returns the identity and an outcome: `new`, `resumed`,
`resumed_after_restart`, `resume_not_found` or `resume_rejected`.

The broker validates the token and owner scope. The owner identity is persisted
locally, so a restarted broker can recognize its sessions. Session transfer to a
different broker requires further design.

## Live-process grace

On an eligible disconnect, the broker holds the logical connection dormant for
`connection.reconnect_grace_ms`. The server seed defaults to 5000 ms; 0 disables
the window. Runtime settings control subsequent changes.

The dormant connection retains queue subscriptions and their inflight delivery
tags. Subscription tasks send through a replaceable transport sink. While no
socket is attached, delivery waits for resume or cleanup. A successful live
resume attaches the replacement sink and permits settlement of held deliveries.

When grace expires, normal unsubscribe cleanup runs and unsettled queue messages
become eligible for redelivery. At-least-once semantics apply throughout.

## Subscription reconciliation

Clients report topic, group, subscription id, partition, auto-ACK mode and
prefetch. The broker compares that metadata with the logical connection and
returns `keep`, `close_client_side`, `close_server_side` or
`recreate_client_side`, including the relevant metadata and reason.

The conservative policy keeps matching subscriptions, closes client-side
mismatches or missing subscriptions, and drops subscriptions present only on the
server. The opt-in restore policy recreates missing client-owned subscriptions
and returns the replacement server id. Clients remap kept subscriptions to that
id when necessary.

Rust, TypeScript, Python, Go and C# expose typed terminal reasons through their
receive APIs. Subscription supervisors retry supported ownership changes and
broker-advised recreation. Automatic recreation is enabled by default for
supported safe cases and can be disabled; terminal errors surface to the
application. Automatic recreation of auto-ACK subscriptions remains outside this
continuity contract.

Publisher handles and new subscriptions use the current connection engine.
In-flight requests on a failed connection fail without automatic replay, because
a publish may have reached the broker before its confirmation was lost.

## Resume after broker restart

The broker stores session skeletons in its local global store: owner identity,
client identity, resume token, queue-subscription metadata and freshness data.
Session changes update this document outside the per-delivery hot path. Writes
are best-effort; a failed persistence operation is logged and can prevent a later
resume.

`connection.resume_session_restart_ttl_ms` bounds the accepted age of persisted
sessions. The default is 60000 ms; 0 disables restart resume. This setting is
independent of live-process grace.

A valid persisted session produces `resumed_after_restart` and supports
subscription reconciliation. Authentication is established on the new
connection. Queue messages remain subject to recovery and redelivery; old
inflight ownership and delivery tags are not restored.

## Held-delivery settlement

Each client delivery carries the incarnation of the connection engine that
received it. Settlement checks that incarnation against the current binding and
sends through the current engine only when the binding is valid.

A live `resumed` outcome preserves the incarnation. Other outcomes, including
`resumed_after_restart`, replace it. Held deliveries from an earlier incarnation
return a typed stale-delivery error without sending a settlement frame. The
reader captures its incarnation when the engine is bound, so a late delivery
from an old reader cannot acquire a newer engine's identity.

A successful local ACK write alone does not confirm durable settlement. Queue
redelivery and stream cursor replay retain their documented at-least-once
semantics.

## Stream limits

Stream subscriptions use durable cursor commits and are re-established by their
supervisors. A held delivery's cursor ACK can currently arrive before resumed
re-subscription completes, leaving the cursor unchanged. Stream NACK-family
operations also currently have no broker effect. The
[roadmap](/roadmap/#client-lifecycle-and-compatibility) tracks explicit stream
settlement semantics and reliable settlement across this reconnect interval.

## Operational signals

TCP logs and admin counters expose resume, grace and reconciliation outcomes.
Reconciliation logs include client id, connection id, policy and action counts.
Planned broker drain sends `GoingAway`; ownership handoff and client reconnection
follow the [admin workflow](/admin-dashboard/).

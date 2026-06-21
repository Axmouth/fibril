---
title: Replication design
description: Development notes on Fibril's follower-pull replication, durability invariants, and the streaming catch-up model.
---

This is a development note. User-facing behavior lives in
[replication](/latest/reliability/replication/). This page records the internal
invariants and why they were chosen.

## Follower progress is local-append progress

An owner read is only a proposal from the follower's point of view. A follower
advances its durable replication cursor only after its own local append path
accepts the owner's batch, never from the owner-returned offsets alone.

This keeps the durability contract honest: a replica-durable confirm waits on
what followers have accepted durably, not on what the owner attempted to send. It
also keeps gaps, overlaps, stale epochs, and future prefix-validation failures
meaningful, because a follower cannot report itself caught up without actually
holding the corresponding local durable records.

## Replicated events cannot outrun replicated messages

Queue state is derived from events, but a ready/delivered/settled event is
meaningless unless the payload it references is present and accepted in the
message log. Replication therefore applies strictly in order:

1. append replicated messages,
2. append replicated events only if the message append was accepted,
3. mutate in-memory queue state only if the event append was accepted.

This prevents a follower from holding in-memory state that claims "offset N is
ready" while payload N is missing, stale, or from a different owner history. A
rejected message batch skips the paired event batch for that pull, and the
follower repairs or retries through a safe path. This invariant is enforced at
recovery and at promotion, not on the live apply path: catch-up legitimately runs
with events transiently ahead of their messages, so a live hard-fail would be
wrong.

## Overlap is a checkpoint repair

Keratin's replicated append rejects partial overlap by default, which is correct:
"I already have offsets 0..10" does not prove those bytes are identical to a new
owner's 0..10, and accepting a suffix over an unproven prefix would silently bless
divergence.

So the broker's checkpoint-aware catch-up treats overlap or a gap as a repair
signal: install the owner's queue-state checkpoint, reset local follower logs to
the checkpoint continuation, then resume pulling. `AlreadyPresent` stays a pure
idempotence outcome that advances only over the owner-returned range.

A future improvement is prefix hash/CRC validation in Keratin: once an existing
prefix is proven byte-identical, a suffix append can be safe without a checkpoint
reset.

## Replica-durable visibility: consumers see only committed data

Durability (when a publish confirms) and visibility (when a consumer may receive
a message) must agree. A replica-durable queue gates delivery on the
committed-replicated watermark, the Kafka high-watermark model: a delivery lease
is only allowed for offsets below the highest offset durable on the required
number of replicas.

The watermark is the `(nodes - 1)`-th largest follower durable `message_next`
(the owner is always durable locally), maintained from the same follower progress
reports that drive the publish-confirm gate, so durability and visibility stay
consistent by construction. It is exposed to the delivery loop as one per-queue
atomic ceiling, so the hot path stays lock-free.

Local-durable queues are never gated: with the owner alone defining durability
there is no committed watermark to wait on, so single-node delivery is unchanged.
The consequence on a replica-durable queue is that delivery latency is bounded
below by commit latency. That is correct: consumers trade a few milliseconds for
never acting on data that could vanish on failover.

## Streaming catch-up is credit-based

Strict request-then-response per batch left the follower idle on the wire while
applying and idle applying while fetching. Once batch sizes made fsync cheap,
that serialization (an owner-read round trip plus an apply, repeated per tick)
became the dominant cluster latency.

The streaming model is a continuous stream with credit-based flow control. The
follower opens a stream and grants the owner a byte budget. The owner pushes
offset-ordered batches down one connection and pauses when credit runs low. The
follower reads into a bounded in-order buffer and applies concurrently, so the
next batch arrives while the current one is applied. TCP already gives in-order,
reliable single-connection delivery, so ordering needs no per-batch round trips.
Credit refills at a low watermark (not at zero) so the pipe never drains. The
applier still runs sequentially through the unchanged durable apply path, so the
failover-safety ordering is untouched: only fetching is overlapped.

## See also

- [Replication](/latest/reliability/replication/) for the user-facing model.
- [Clustering](/latest/concepts/clustering/) for ownership and failover.
- [Recovery quarantine](/latest/reliability/recovery-quarantine/) for damaged-log handling.

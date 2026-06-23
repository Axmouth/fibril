---
title: Plexus streams
description: Fan-out stream channels where every consumer sees every record.
---

A **Plexus stream** is a channel type beside the work queue. Where a queue
delivers each message once (consumed means gone), a stream delivers **every
record to every consumer**. Pick the type when you declare the channel:
`declare_queue(...)` for a work queue, `declare_plexus(...)` for a stream.

Use a stream for fan-out: event broadcast, change feeds, notifications, an audit
tail, or anything where many independent readers each need the full sequence.

## Cursors: where a consumer reads from

A stream subscription tracks position with a **cursor**. There are two modes:

- **Durable (recommended).** Give the subscription a name. The broker remembers
  the cursor for that name, advances it as you ack, and resumes it after a
  restart or reconnect, anywhere in the cluster. No offset bookkeeping on your
  side. A fresh name starts at the earliest retained record so it cannot silently
  miss data. Different names are independent fan-out consumers, each tracking its
  own position. The same name is single-active (last commit wins).
- **Ephemeral.** No name. You choose a start position and the broker keeps no
  state for you, so you are at the mercy of retention. Good for a live tail or an
  ad-hoc replay.

Start positions for an ephemeral subscription: latest (only new records),
earliest (oldest retained), N records back from the tail, or the first record at
or after a wall-clock time.

Note that a record's offset is an internal storage detail, not a stable identity
to build application logic on. Resume with a durable cursor name rather than by
remembering offsets. The client deliberately does not let you start a
subscription at a raw offset.

## Filtering

A subscription can carry a **header filter**: an AND of `header == pattern`
clauses, where the value pattern may contain `*` wildcards (`eu-*`). It is
deliberately tiny — no regex, no OR, no nesting. A record is delivered only when
it carries every named header and each matches. Filtering is stream-only.

## Partitioning: fan-in, not work-sharing

Partitioning a stream buys write throughput, storage spread, and per-key ordering
(records with the same partition key stay ordered). It does **not** divide work
among consumers. A stream subscription reads **all** partitions and fans them in
client-side, so post-filter you see every matching record. The same durable name
keeps an independent cursor per partition.

This is the opposite of a queue's exclusive consumer cohort, which splits
partitions across members. If you want within-group work distribution, use the
work queue — that is its job.

## Retention

A stream keeps records until a retention bound is crossed, then drops whole
sealed log segments. Bound it by age, total bytes, or record count (any
combination). Retention wins over a slow cursor: a consumer that lags past
the retained window is clamped forward and flagged rather than holding storage
forever.

## Durability tiers

Each stream picks a durability tier at declare time. They trade latency for
durability:

- **durable** (default) — persist and fsync (and replicate when configured)
  before delivering and confirming the producer. Survives power loss. Highest
  latency.
- **speculative** — deliver the instant the offset is assigned, persist (fsync)
  in the background, and defer the producer confirm until the record is durable.
  Readers see it sooner, the producer still gets a real durability confirmation.
  Speculative deliveries carry a `fibril.speculative` header so a consumer knows
  the record may not be durable yet.
- **ephemeral** — deliver at staging and confirm immediately, persisting without
  an fsync (the record goes to the OS but is not flushed to disk). Lowest
  latency, weakest guarantee: a process crash is survivable, a power loss can
  drop the newest ephemeral records.

All three are log-backed, so durable cursors, replay, and retention work on any
tier.

## Acks advance the cursor

For a durable subscription, settling a record (manual ack, or server-side
auto-ack) advances the cursor past it — at-least-once delivery with no offset
math. Publishing to a stream uses the ordinary publish path; the broker routes by
channel kind, so the same producer works for queues and streams.

See [Client usage](/latest/clients/) for the stream API in Rust, TypeScript, and
Python.

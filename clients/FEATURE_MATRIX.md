# Client feature matrix

What each Fibril client supports, so parity gaps are visible at a glance and new
clients have a checklist to work against. The Rust client (`crates/client`) is
the reference implementation, and other clients aim to match its behavior, not
necessarily its internal structure (e.g. the TS and Python clients mirror the
routing design with plain event-loop constructs rather than locks and atomics).

`ARCHITECTURE.md` covers the shared layering, invariants, and porting lessons.

Legend:

- **done** — works end to end.
- **client-ready** — the client side is complete, waiting on a server feature to
  take effect end to end.
- **deferred** — a known gap, intentionally postponed. The note says what
  unblocks it.
- **no** — not implemented.
- **n/a** — not applicable.

Superscript numbers refer to the Notes under the tables.

## Core protocol

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Custom binary wire format (v1) | done | done | done <sup>6</sup> |
| Handshake + compliance marker | done | done | done |
| Username/password auth | done | done | done |
| Heartbeat ping/pong + timeout | done | done | done |
| Typed wire parse errors | done | done <sup>1</sup> | done <sup>1</sup> |

## Publish

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Unconfirmed publish | done | done | done |
| Confirmed publish (awaits offset) | done | done | done |
| Pipelined confirmation handle | done | done | done |
| Delayed publish | done | done | done |
| Raw bytes publish | done | done | done |
| Content types (msgpack/json/text/raw/custom) | done | done | done |
| Custom headers | done | done | done |
| Message TTL: `expiring` publisher | done | done | done <sup>7</sup> |
| Message TTL: queue `default_message_ttl` on declare | done | done | done <sup>7</sup> |

## Consume

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Subscribe manual ack | done | done | done |
| Subscribe auto ack | done | done | done <sup>8</sup> |
| Ack / Nack / retry-after | done | done | done |
| Prefetch / backpressure | done | done | done |
| Declare queue + DLQ policies | done | done | done |
| Multi-partition subscribe fan-in | done | done | done |
| Live-repartition partition pickup (consumer grow) | done | done <sup>2</sup> | done <sup>2</sup> |

## Plexus (fan-out streams)

Every consumer of a stream sees every record (vs a queue, where a message is
consumed once). A stream subscription reads ALL partitions and fans them in, and
the same durable name tracks an independent cursor per partition.

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Declare plexus (partitions, durability, retention) | done | done | done |
| Publish to a stream (reuses Publish, routed by kind) | done | done | done |
| Stream subscribe manual ack (cursor advance) | done | done | done |
| Stream subscribe auto ack (server-settled cursor) | done | done | done |
| Durable named cursor (resume/advance) | done | done | done |
| Ephemeral start position (latest/earliest/offset/n-back/by-time) | done | done | done |
| Header filter (AND of `header == pattern`, `*` glob) | done | done | done |
| Client-side fan-in across all partitions | done | done | done |
| Failover resubscribe + live-grow pickup | done | done | done |
| Durability tiers honored end to end (express lane) | done <sup>11</sup> | done <sup>11</sup> | done <sup>11</sup> |

## Reconnect and resume

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Resume identity on Hello | done | done | done |
| Auto-reconnect before an operation | done | done | done |
| Reconcile subscriptions on reconnect | done | done | done |
| Owner-restart (reconcile on any reconnect, not just resumed) | done | done | done |

## Cluster routing

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Topology fetch + cache | done | done | done |
| Per-endpoint connection pool | done | done | done |
| Owner routing per (topic, partition) | done | done | done |
| Partition-key routing (FNV-1a, broker-exact) | done | done | done |
| Keyless round-robin spread | done | done | done |
| Follow owner redirects (bounded) | done | done | done |
| Transient failover publish retry + backoff | done | done | done |
| Topology refresh on transient failure (throttled) | done | done | done |
| Subscription supervisor (re-subscribe on owner drop + graceful move) | done | done | done |

## Reliability and groups

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Retry classification (is_retryable / retry_advice) | done | done | done |
| Reserved-namespace header validation | done | done | done |
| ReliablePublisher helper | done | done | done |
| Producer-id dedup headers | done | done <sup>3</sup> | done <sup>3</sup> |
| Exclusive consumer groups | done | done | done |
| Cohort member id mint/carry | done | done | done |
| Assignment events stream (AssignmentChanged) | done | done <sup>4</sup> | done <sup>4</sup> |

## Language ergonomics

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Async API | done | done | done |
| Blocking/sync API | done | n/a | done <sup>9</sup> |
| Delay / TTL units | seconds or Duration | milliseconds | seconds or timedelta |

## Tooling

| Feature | Rust | TypeScript | Python |
| --- | --- | --- | --- |
| Examples | done | done | done |
| Examples-as-light-tests runner | done | done | deferred <sup>10</sup> |
| Real-broker integration smoke | done | done <sup>5</sup> | deferred <sup>5</sup> |

## Notes

1. Decode raises a typed `WireError` (with a `kind` discriminant:
   `unexpected_eof`, `invalid_magic`, `trailing_bytes`, `invalid_uuid`,
   `unknown_content_type`, `unknown_tag`), mirroring the Rust typed wire errors so
   callers branch on kind rather than parse messages.
2. The broker supports live repartition (grow/shrink) in cluster mode
   (`fibrilctl repartition`, `/admin/api/repartition`, broker transition
   machinery). A consumer must re-fan-in to pick up partitions added by a grow.
   The clients do this via a supervised poll that refreshes topology and
   subscribes new partitions. Shrink (retiring a partition) is handled by the
   per-partition supervisor, not an explicit drop.
3. The ReliablePublisher retries until durably confirmed (at-least-once) and
   stamps `fibril.client.producer_id`/`_seq` on every message. Those headers reach
   the consumer, so effectively-once has a client-only path (consumer-side dedup
   on (producer_id, seq)). Broker-side dedup is the only server-gated part.
4. The broker pushes `AssignmentChanged` to exclusive-cohort members. The Rust
   client exposes them via `assignment_events()`, the TS client via
   `client.onAssignmentChange(handler)`, and the Python client via
   `client.on_assignment_change(handler)`. Exclusivity is enforced by the
   per-partition gate regardless, so this is observability/narrowing, not
   correctness.
5. CI runs the examples against the published single-node broker image for Rust
   and TS. A multi-node ganglion cluster smoke for a real cross-owner redirect is
   still pending. The Python client's wire correctness is pinned by the shared
   cross-client vectors (note 6). Wiring its examples into CI against a live broker
   is still pending.
6. The Python wire codec is cross-checked byte-for-byte against the shared
   `clients/wire_vectors.json`, the same fixture the Rust protocol crate pins its
   encoders to (`crates/protocol/tests/wire_vectors.rs`), so all three
   implementations agree on the bytes.
7. Message TTL and the queue default TTL are seconds-native in Python (`expiring`
   takes a float of seconds or a `timedelta`). The wire field stays milliseconds.
8. Auto-ack settles server-side (`auto_ack: true` on the wire), matching the Rust
   client. The subscription yields plain messages with nothing to ack.
9. The Python blocking client (`fibril.blocking.BlockingClient`) is a thin facade
   over the async core on a background event-loop thread, not a second
   implementation.
10. The Python client has unit and fake-broker integration tests. A runnable
    examples-as-tests job like the TS client's is not wired up yet.
11. The express lane is wired end to end. Durable persists and fsyncs before
    delivering and confirming. Speculative delivers off the staged offset, fsyncs
    in the background, and defers the producer confirm until durable (deliveries
    carry a server-owned `fibril.speculative` header). Ephemeral delivers at
    staging, confirms immediately, and persists without an fsync (AfterWrite). All
    tiers stay log-backed, so cursors and replay work on each.

See the repo-root `FOLLOWUPS.md` "Clients" section for the brick-by-brick plan
behind these rows.

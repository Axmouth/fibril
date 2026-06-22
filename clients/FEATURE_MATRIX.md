# Client feature matrix

What each Fibril client supports, so parity gaps are visible at a glance and new
clients have a checklist to work against. The Rust client (`crates/client`) is
the reference implementation, and other clients aim to match its behavior, not
necessarily its internal structure (e.g. the TS client mirrors the routing
design with plain single-threaded constructs rather than locks and atomics).

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

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Custom binary wire format (v1) | done | done |
| Handshake + compliance marker | done | done |
| Username/password auth | done | done |
| Heartbeat ping/pong + timeout | done | done |
| Typed wire parse errors | done | done <sup>1</sup> |

## Publish

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Unconfirmed publish | done | done |
| Confirmed publish (awaits offset) | done | done |
| Pipelined confirmation handle | done | done |
| Delayed publish | done | done |
| Raw bytes publish | done | done |
| Content types (msgpack/json/text/raw/custom) | done | done |
| Custom headers | done | done |
| Message TTL: `expiring` publisher | done | done |
| Message TTL: queue `default_message_ttl` on declare | done | done |

## Consume

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Subscribe manual ack | done | done |
| Subscribe auto ack | done | done |
| Ack / Nack / retry-after | done | done |
| Prefetch / backpressure | done | done |
| Declare queue + DLQ policies | done | done |
| Multi-partition subscribe fan-in | done | done |
| Live-repartition partition pickup (consumer grow) | done | done <sup>2</sup> |

## Reconnect and resume

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Resume identity on Hello | done | done |
| Auto-reconnect before an operation | done | done |
| Reconcile subscriptions on reconnect | done | done |
| Owner-restart (reconcile on any reconnect, not just resumed) | done | done |

## Cluster routing

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Topology fetch + cache | done | done |
| Per-endpoint connection pool | done | done |
| Owner routing per (topic, partition) | done | done |
| Partition-key routing (FNV-1a, broker-exact) | done | done |
| Keyless round-robin spread | done | done |
| Follow owner redirects (bounded) | done | done |
| Transient failover publish retry + backoff | done | done |
| Topology refresh on transient failure (throttled) | done | done |
| Subscription supervisor (re-subscribe on owner drop + graceful move) | done | done |

## Reliability and groups

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Retry classification (is_retryable / retry_advice) | done | done |
| Reserved-namespace header validation | done | done |
| ReliablePublisher helper | done | done |
| Producer-id dedup headers | done | done <sup>3</sup> |
| Exclusive consumer groups | done | done |
| Cohort member id mint/carry | done | done |
| Assignment events stream (AssignmentChanged) | done | done |

## Tooling

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Examples | done | done |
| Examples-as-light-tests runner | done | done |
| Real-broker integration smoke | done | done <sup>5</sup> |

## Notes

1. Decode throws a typed `WireError` (with a `kind` discriminant:
   `unexpected_eof`, `invalid_magic`, `trailing_bytes`, `invalid_uuid`,
   `unknown_content_type`, `unknown_tag`), mirroring the Rust typed wire errors so
   callers branch on kind rather than parse messages.
2. The broker supports live repartition (grow/shrink) in cluster mode
   (`fibrilctl repartition`, `/admin/api/repartition`, broker transition
   machinery). A consumer must re-fan-in to pick up partitions added by a grow.
   Both clients now do this via a supervised poll that refreshes topology and
   subscribes new partitions. Shrink (retiring a partition) is handled by the
   per-partition supervisor, not an explicit drop, in both clients.
3. The ReliablePublisher already works as a reliability helper: it retries until
   durably confirmed (at-least-once), which is most of the value, and stamps
   `fibril.client.producer_id`/`_seq` on every message. Those headers reach the
   consumer, so effectively-once has a client-only path: a consumer-side dedup
   helper that skips already-seen (producer_id, seq). That is actionable client
   work needing no server change. Broker-side dedup (dropping dups at publish) is
   the alternative and is the only server-gated part.
4. The broker pushes `AssignmentChanged` to exclusive-cohort members
   (`handler.rs` spawn_assignment_forwarder). The Rust client exposes them via
   `assignment_events()`; the TS client exposes them via
   `client.onAssignmentChange(handler)`. Exclusivity is enforced by the
   per-partition gate regardless, so this is observability/narrowing, not
   correctness.
5. CI runs the examples against the published single-node broker image. A
   multi-node ganglion cluster smoke for a real cross-owner redirect is still
   pending.

See the repo-root `FOLLOWUPS.md` "Clients" section for the brick-by-brick plan
behind these rows.

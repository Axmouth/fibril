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
| Typed wire parse errors | done | deferred <sup>1</sup> |

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

## Consume

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Subscribe manual ack | done | done |
| Subscribe auto ack | done | done |
| Ack / Nack / retry-after | done | done |
| Prefetch / backpressure | done | done |
| Declare queue + DLQ policies | done | done |
| Multi-partition subscribe fan-in | done | done <sup>2</sup> |

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
| Producer-id dedup headers | done | client-ready <sup>3</sup> |
| Exclusive consumer groups | done | done |
| Cohort member id mint/carry | done | done |
| Assignment events stream (AssignmentChanged) | done | deferred <sup>4</sup> |

## Tooling

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Examples | done | done |
| Examples-as-light-tests runner | done | done |
| Real-broker integration smoke | done | done <sup>5</sup> |

## Notes

1. Decode throws on malformed frames, but a typed parse-error taxonomy (matching
   the Rust typed wire errors) is not ported yet.
2. The partition set is fixed at subscribe time. Picking up partitions added by a
   live grow is deferred, and pairs with live repartitioning (counts are fixed at
   queue creation today).
3. The client sends `fibril.client.producer_id`/`_seq` on every reliable publish.
   End-to-end dedup needs the broker to key on them, which is not implemented yet,
   so this stays at-least-once for now. Unblocks when broker-side dedup lands.
4. The Rust client carries dormant assignment-events plumbing. The TS client does
   not port it because the broker does not emit `AssignmentChanged` today (both
   references in `broker.rs` are comments). Exclusivity is enforced by the broker
   per-partition gate regardless. Unblocks when the broker emits the op.
5. CI runs the examples against the published single-node broker image. A
   multi-node ganglion cluster smoke for a real cross-owner redirect is still
   pending.

See the repo-root `FOLLOWUPS.md` "Clients" section for the brick-by-brick plan
behind these rows.

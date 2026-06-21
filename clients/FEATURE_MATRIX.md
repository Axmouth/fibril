# Client feature matrix

What each Fibril client supports, so parity gaps are visible at a glance and new
clients have a checklist to work against. The Rust client (`crates/client`) is
the reference implementation; other clients aim to match its behavior, not
necessarily its internal structure (e.g. the TS client mirrors the routing
design with plain single-threaded constructs rather than locks and atomics).

Legend: done, partial (scope noted), no (planned), n/a (not applicable).

## Core protocol

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Custom binary wire format (v1) | done | done |
| Handshake + compliance marker | done | done |
| Username/password auth | done | done |
| Heartbeat ping/pong + timeout | done | done |
| Typed wire parse errors | done | partial (decode throws, not yet a typed error taxonomy) |

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
| Multi-partition subscribe fan-in | done | no (single-partition; subscribe routes to the partition-0 owner) |

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
| Owner routing per (topic, partition) | done | done (publish path) |
| Partition-key routing (FNV-1a, broker-exact) | done | done |
| Keyless round-robin spread | done | done |
| Follow owner redirects (bounded) | done | done |
| Transient failover publish retry + backoff | done | done |
| Topology refresh on transient failure (throttled) | done | done |
| Subscription supervisor (re-subscribe on owner drop) | done | partial (single-partition; stream-close trigger, not graceful-move) |

## Reliability and groups

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Retry classification (is_retryable / retry_advice) | done | partial (isTransientError only) |
| Reserved-namespace header validation | done | no |
| ReliablePublisher helper | done | no |
| Producer-id dedup headers | done | no |
| Exclusive consumer groups | done | no |
| Assignment events / cohort member id | done | no |

## Tooling

| Feature | Rust | TypeScript |
| --- | --- | --- |
| Examples | done | partial (hello/demo; examples-as-light-tests runner planned) |
| Real-broker integration smoke | done | partial (standalone validated; cluster smoke planned) |

See `FOLLOWUPS.md` (repo root, "Clients" section) for the brick-by-brick plan
behind the TypeScript "no"/"partial" rows.

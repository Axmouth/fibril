# Client feature matrix

What each Fibril client supports, so parity gaps are visible at a glance and new
clients have a checklist to work against. The Rust client (`crates/client`) is
the reference implementation, and other clients aim to match its behavior, not
necessarily its internal structure (e.g. the TS and Python clients mirror the
routing design with plain event-loop constructs rather than locks and atomics,
the Go client mirrors the reference actor design with a goroutine and channels,
and the C# client mirrors it with a System.Threading.Channels actor and
async/await).

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

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Custom binary wire format (v1) | done | done | done <sup>6</sup> | done <sup>6</sup> | done <sup>6</sup> |
| Handshake + compliance marker | done | done | done | done | done |
| Username/password auth | done | done | done | done | done |
| TLS connect (OS roots / CA file / fingerprint pin) | done | done | done <sup>12</sup> | done | done |
| Typed TLS error taxonomy (426 mismatch / trust / config) | done | done | done | done | done |
| Client certificate (mTLS) + typed required-cert error | done | done | done <sup>13</sup> | done <sup>13</sup> | done <sup>13</sup> |
| Heartbeat ping/pong + timeout | done | done | done | done | done |
| Typed wire parse errors | done | done <sup>1</sup> | done <sup>1</sup> | done <sup>1</sup> | done <sup>1</sup> |

## Publish

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Unconfirmed publish | done | done | done | done | done |
| Confirmed publish (awaits offset) | done | done | done | done | done |
| Confirmation handle (publish_with_confirmation) | done | done | done | done | done |
| Delayed publish with confirmation handle | done | done | done | done | done |
| Delayed publish | done | done | done | done | done |
| Raw bytes publish | done | done | done | done | done |
| Content types (msgpack/json/text/raw/custom) | done | done | done | done <sup>15</sup> | done <sup>15</sup> |
| Custom headers | done | done | done | done | done |
| Message TTL: `expiring` publisher | done | done | done <sup>7</sup> | done | done |
| Message TTL: queue `default_message_ttl` on declare | done | done | done <sup>7</sup> | done | done |

## Consume

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Subscribe manual ack | done | done | done | done | done |
| Subscribe auto ack | done | done | done <sup>8</sup> | done <sup>8</sup> | done <sup>8</sup> |
| Ack / Nack / retry-after | done | done | done | done | done |
| Prefetch / backpressure | done | done | done | done | done |
| Declare queue + DLQ policies | done | done | done | done | done |
| Multi-partition subscribe fan-in | done | done | done | done | done |
| Live-repartition partition pickup (consumer grow) | done | done <sup>2</sup> | done <sup>2</sup> | done <sup>2</sup> | done <sup>2</sup> |

## Plexus (fan-out streams)

Every consumer of a stream sees every record (vs a queue, where a message is
consumed once). A stream subscription reads ALL partitions and fans them in, and
the same durable name tracks an independent cursor per partition.

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Declare plexus (partitions, durability, retention) | done | done | done | done | done |
| Publish to a stream (reuses Publish, routed by kind) | done | done | done | done | done |
| Stream subscribe manual ack (cursor advance) | done | done | done | done | done |
| Stream subscribe auto ack (server-settled cursor) | done | done | done | done | done |
| Durable named cursor (resume/advance) | done | done | done | done | done |
| Ephemeral start position (latest/earliest/offset/n-back/by-time) | done | done | done | done | done |
| Header filter (AND of `header == pattern`, `*` glob) | done | done | done | done | done |
| Client-side fan-in across all partitions | done | done | done | done | done |
| Failover resubscribe + live-grow pickup | done | done | done | done | done |
| Durability tiers honored end to end (express lane) | done <sup>11</sup> | done <sup>11</sup> | done <sup>11</sup> | done <sup>11</sup> | done <sup>11</sup> |

## Reconnect and resume

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Resume identity on Hello | done | done | done | done | done |
| Auto-reconnect before an operation | done | done | done | done | done |
| Reconcile subscriptions on reconnect | done | done | done | done | done |
| Owner-restart (reconcile on any reconnect, not just resumed) | done | done | done | done | done |
| Opt-in restore reconcile policy | done | done | done | done | done |
| Explicit `reconnect()` returning the handshake outcome | done | done | done | no | no |
| Disable automatic reconnect | done | done | done | no | no |
| Typed subscription close reason on the receive surface | done | done | done | done | no |
| Auto-resubscribe on a recreate verdict (opt-out) | done | done | done | done | no |

## Cluster routing

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Topology fetch + cache | done | done | done | done | done |
| Ignore stale (older-generation) topology pushes <sup>19</sup> | done | done | done | done | done |
| Per-endpoint connection pool | done | done | done | done | done |
| Prune pooled connections on topology apply <sup>20</sup> | done | done | done | done | done |
| Owner routing per (topic, partition) | done | done | done | done | done |
| Partition-key routing (FNV-1a, broker-exact) | done | done | done | done | done |
| Keyless round-robin spread | done | done | done | done | done |
| Follow owner redirects (bounded) | done | done | done | done | done |
| Transient failover publish retry + backoff | done | done | done | done | done |
| Topology refresh on transient failure (throttled) | done | done | done | done | done |
| Subscription supervisor (re-subscribe on owner drop + graceful move) | done | done | done | done | done |

## Discovery

Discovery is an opt-in surface (a routing/discovery view over the client) that
fans in across channels by a topic glob, driven by the cluster topology.

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Catalogue snapshot (declared queues + streams) | done | done | done | done | done |
| Catalogue change events | done | done | done | done | done |
| Pattern subscribe over queues (glob + auto-pickup) | done | done | done | done <sup>16</sup> | done <sup>16</sup> |
| Pattern subscribe over streams (glob + auto-pickup) | done | done | done | done <sup>16</sup> | done <sup>16</sup> |

## Reliability and groups

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Retry classification (is_retryable / retry_advice) | done | done | done | done | done |
| Bounded confirmed publish (timeout / deadline) <sup>21</sup> | done | done | done | done | done |
| Reserved-namespace header validation | done | done | done | done | done |
| ReliablePublisher helper | done | done | done | done | done |
| Producer-id dedup headers | done | done <sup>3</sup> | done <sup>3</sup> | done <sup>3</sup> | done <sup>3</sup> |
| Exclusive consumer groups | done | done | done | done | done |
| Exclusive cohort shorthand (`exclusive()`) | done | done | done | done <sup>17</sup> | done <sup>17</sup> |
| Cohort member id mint/carry | done | done | done | done | done |
| Assignment events stream (AssignmentChanged) | done | done <sup>4</sup> | done <sup>4</sup> | done <sup>4</sup> | done <sup>4</sup> |

## Language ergonomics

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Async API | done | done | done | n/a <sup>14</sup> | done |
| Blocking/sync API | done | n/a | done <sup>9</sup> | done <sup>14</sup> | n/a <sup>18</sup> |
| Delay / TTL units | seconds or Duration | milliseconds | seconds or timedelta | seconds or Duration | TimeSpan |

## Tooling

| Feature | Rust | TypeScript | Python | Go | C# |
| --- | --- | --- | --- | --- | --- |
| Examples | done | done | done | done | done |
| Examples-as-light-tests runner | done | done | deferred <sup>10</sup> | done | done |
| Real-broker integration smoke | done | done <sup>5</sup> | deferred <sup>5</sup> | done <sup>5</sup> | done <sup>5</sup> |

## Notes

1. Decode raises a typed `WireError` (with a `kind` discriminant:
   `unexpected_eof`, `invalid_magic`, `trailing_bytes`, `invalid_uuid`,
   `unknown_content_type`, `unknown_tag`), mirroring the Rust typed wire errors so
   callers branch on kind rather than parse messages. The Go client exposes the
   same discriminants on `WireError.Kind`; the C# client raises a `WireException`
   with a `WireErrorKind`.
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
   `client.onAssignmentChange(handler)`, the Python client via
   `client.on_assignment_change(handler)`, the Go client via the
   `OnAssignmentChanged` callback on `ClientOptions`, and the C# client via the
   `OnAssignmentChanged` callback on `ClientOptions`. Exclusivity is enforced by
   the per-partition gate regardless, so this is observability/narrowing, not
   correctness.
5. CI runs the examples against the published single-node broker image for Rust,
   TS, Go, and C# (each has a `run-all.sh` that self-validates every example
   against a real broker, wired into its client CI workflow with `--network host`
   so the client connects over real loopback). A multi-node ganglion cluster smoke
   for a real cross-owner redirect is still pending across clients. The Python
   client's wire correctness is pinned by the shared cross-client vectors (note 6);
   wiring its examples into CI is pending.
6. The Python, Go, and C# wire codecs are cross-checked byte-for-byte against the
   shared `clients/wire_vectors.json`, the same fixture the Rust protocol crate
   pins its encoders to (`crates/protocol/tests/wire_vectors.rs`), so all five
   implementations agree on the bytes.
7. Message TTL and the queue default TTL are seconds-native in Python (`expiring`
   takes a float of seconds or a `timedelta`). The wire field stays milliseconds.
8. Auto-ack settles server-side (`auto_ack: true` on the wire), matching the Rust
   client. The subscription yields plain messages with nothing to ack.
9. The Python blocking client (`fibril.blocking.BlockingClient`) is a thin facade
   over the async core on a background event-loop thread, not a second
   implementation.
10. The Python client has unit and fake-broker integration tests. A runnable
    examples-as-tests job like the TS and Go clients' is not wired up yet.
11. The express lane is wired end to end. Durable persists and fsyncs before
    delivering and confirming. Speculative delivers off the staged offset, fsyncs
    in the background, and defers the producer confirm until durable (deliveries
    carry a server-owned `fibril.speculative` header). Ephemeral delivers at
    staging, confirms immediately, and persists without an fsync (AfterWrite). All
    tiers stay log-backed, so cursors and replay work on each.
12. The Python fingerprint pin checks the whole presented chain on Python 3.13
    and later. Older Pythons only expose the leaf certificate to the check, so
    pin the server certificate there or trust the CA via ``ca_path``.
13. Under TLS 1.3 the broker's certificate-required alert arrives after the
    handshake completes, and the runtime can flatten it to a clean EOF (asyncio in
    Python, the TLS stack in Go), so the client attributes a certless TLS connect
    that ends with no HELLO reply to the certificate requirement.
14. Go has no async/await split: the client API is a set of ordinary blocking,
    goroutine-safe calls, and deliveries arrive on channels. So the "blocking"
    row is the native model and the "async" row is n/a rather than a gap.
15. Text, JSON, and raw have builders (`Text`/`JSON`/`Raw`). Rust, TypeScript, and
    Python also serialize a value straight to msgpack for you, and keep that encoder
    optional so a build that only publishes JSON, text, or raw needs no msgpack
    library: Rust behind the default-on `msgpack` Cargo feature, TypeScript as an
    optional `@msgpack/msgpack` peer dependency, Python as the `msgpack` extra. Go
    and C# never serialize msgpack for you. Their `Msgpack` and `Custom` builders
    tag already-encoded bytes with the content type, so they need no MessagePack
    dependency at all.
16. Discovery is reached via a routing view (`Client.Routing()` in Go and
    `Client.Routing` in C#). `SubscribePattern` and `SubscribeStreamPattern` take
    an options struct/record rather than the fluent builder the other clients use,
    but the surface and behavior match. The catalogue is empty against a
    single-node broker (which advertises no topology), so the pattern example is a
    demo and the behavior is covered by a fake-broker test.
17. Go's and C#'s exclusive shorthand is `SubscribeTopicExclusive` (a whole-topic
    fan-in joining the default cohort), with `SubscribeTopicCohort` for a named
    cohort. All five clients join the same default cohort id "default".
18. The C# client is async-native (Task-based), like the TS client, so it exposes
    no separate blocking facade. `CancellationToken` provides cancellation on every
    network call.
19. Topology apply is generation-gated: a pushed or fetched snapshot older than the
    cache already holds is ignored, so an out-of-order or duplicate push from a
    bounced broker cannot regress routing. The client still acks the current
    generation so the broker can fence a repartition cutover.
20. On every topology apply (fetch or push), pooled connections to endpoints that no
    longer own any partition are dropped and closed, so a failed-over owner's
    connection does not linger until shutdown.
21. A confirmed publish to an owner that never answers must terminate rather than
    hang. Rust and Python bound it with a client-level `publish_timeout_ms` option;
    Go and C# with the per-call `context`/`CancellationToken`; TypeScript with a
    retry-state deadline.

See the repo-root `FOLLOWUPS.md` "Clients" section for the brick-by-brick plan
behind these rows.

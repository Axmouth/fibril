# Fibril client architecture

A reference for building and maintaining Fibril clients in any language. The Rust
client (`crates/client`) is the canonical implementation and `clients/typescript`
is the second one. This captures the shared shape, the invariants every client
must hold, and the lessons from porting, so the next client (Python) starts from
the design rather than rediscovering it. See `FEATURE_MATRIX.md` for the
per-client status grid and the repo-root `FOLLOWUPS.md` "Clients" section for the
brick-by-brick plan.

## Layers

A client is built in layers, each depending only on the one below. The
TypeScript module names are given as a concrete example, but the layering is
language-agnostic.

1. **Wire codec** (`wire.ts`) - byte-exact encode/decode of each frame body in
   the broker's custom binary format (`crates/protocol/src/v1/wire.rs`). Big
   endian, length-prefixed strings and bytes, 16-byte UUIDs, a one-byte tag for
   options, a 4-byte ASCII magic per op. This is the only layer that touches
   bytes and the only one that must match the broker exactly.
2. **Frame header + stream framing** (`codec.ts`) - the fixed 20-byte frame
   header (payload length, version, opcode, flags, request id) and the logic to
   split a TCP byte stream into frames. Unchanged across the msgpack-to-binary
   move (only bodies changed).
3. **Op adapter** (`frames.ts`) - maps the engine's idiomatic message structs to
   and from the wire codec, in both directions for every opcode, so the in-process
   test broker speaks the same format as the real one. Keeps the wire field order
   in exactly one place.
4. **Engine** (`engine.ts`) - one TCP connection. Owns the handshake, auth,
   heartbeats, the request-id-to-waiter map, the per-subscription delivery
   queues, and the read/write loops. Exposes a command-submit interface and
   surfaces typed results and errors. Knows nothing about routing or topology.
5. **Client + routing** (`client.ts`) - the connection pool (one engine per
   endpoint), the topology cache, partition routing, redirect following, and the
   reconnect policy. This is where cluster-awareness lives.
6. **Handles** (`publisher.ts`, `subscription.ts`, `message.ts`) - the public
   API: publishers (including the reliable variant), subscriptions (with the
   failover supervisor and partition fan-in), and message encode/decode.

## Invariants every client must hold

- **Wire format is byte-exact.** Field order, widths, option tags, and magics
  must match `wire.rs`. Pin it with round-trip tests and byte-layout assertions,
  and cross-check against a real broker early (before building higher layers on
  a possibly-wrong codec).
- **Partition key hashing is FNV-1a (64-bit), byte-for-byte.** Offset basis
  `0xcbf29ce484222325`, prime `0x100000001b3`, mod the partition count. A given
  key must land on the same partition across every client and the broker, so
  per-key ordering holds. Pin it with the canonical FNV-1a vectors.
- **Routing is cache-only and reactive.** The client never fetches topology on
  the hot path. It routes from a cache warmed by explicit topology fetches and
  point-updated by redirects. A cold or standalone cache routes to partition 0.
  A misroute is corrected by the broker's redirect, not by a pre-flight lookup.
- **The compliance marker is preserved.** The handshake carries the AI policy
  compliance string unchanged. Do not alter or drop it.
- **Auto-ack settles server-side.** An auto-ack subscription sets `auto_ack:
  true` on the wire so the broker settles each delivery as it sends it; the
  client yields plain messages with nothing to ack (matches the Rust reference).
  Client-side auto-ack (leave the flag false, ack after yielding) is a valid
  alternative the protocol allows, but the clients do not use it.
- **The reserved header namespace is enforced.** User code cannot set
  `fibril.*` or `stroma.*` headers. The library-owned `fibril.client.*` carve-out
  (producer dedup ids) is set only by the client itself.

## Concurrency: mirror the shape, not the primitives

The Rust client uses `ArcSwap`, `RwLock`, atomics, and `OnceLock` because it runs
many tasks across threads. Those are accidental complexity for a single-threaded
runtime. In an event-loop language, mirror the Rust *structure and behavior* and
drop the synchronization machinery: the topology cache is a plain object, the
connection pool a plain map, the round-robin cursor a plain integer, the cohort
member id a plain nullable field. Do not port mutexes into a runtime that has no
data races.

Two practical rules that fall out of this:

- **Any background loop or timer must not keep the process alive on its own.** In
  Node this means `unref()` on intervals. The supervisor and owner-checks are
  background concerns, not reasons to keep the program running.
- **Bound every retry loop** (publish failover, re-subscribe backoff) and make it
  stop on client shutdown, or a test or a clean exit will hang.

## Continuity model (the key design decision)

There are two mechanisms that keep a subscription alive across a disconnect, and
they must be mutually exclusive per subscription or they fight each other:

- **Reconcile on reconnect.** On an intentional reconnect the engine preserves
  its delivery queues and sends a reconcile request listing its subscriptions.
  The broker restores them (keeping in-flight leases) or reports them gone.
  Reconcile fires on ANY reconnect that has subscriptions, not only a resumed
  session, because an owner that bounces in place reconnects into a fresh session
  that has forgotten them.
- **The subscription supervisor.** A supervised subscription reads from a merged
  queue fed by the current owner's per-connection delivery queue. When that
  stream closes (owner death or restart) or the topology owner moves, it
  re-subscribes fresh to the current owner with backoff. It also fans in over all
  partitions the topology knows.

The decision: a supervised subscription owns its continuity via fresh
re-subscribe, so it stays OUT of the reconcile registry. A non-supervised one
uses reconcile. Mixing them double-subscribes and deadlocks the handshake on a
reconcile the supervisor also triggers. Pick one per subscription.

Consequence for in-flight messages: a delivered-but-unsettled message binds to
the engine it arrived on. Across a failover its settle goes to the dead
connection and fails, and the broker redelivers on lease expiry. That is correct
at-least-once behavior and needs no special handling. New messages arrive tagged
with the new engine.

## Routing model

- One engine per endpoint, pooled and keyed by `host:port`. The bootstrap
  connection is the default route.
- Publish: choose a partition (key hash or round-robin over the count from the
  cache), resolve the owner from the cache, send on that owner's pooled
  connection, falling back to bootstrap on a cache miss.
- Confirmed publishes follow owner redirects (bounded) and retry across a
  transient owner failover (refresh topology, jittered backoff, deadline,
  not-found fast-fail). Fire-and-forget and pipelined publishes route but do not
  auto-follow, because there is no reply to carry the redirect.
- A full topology refresh prunes pooled connections to endpoints that no longer
  own anything.

## Reliability model

- `retryAdvice`/`isRetryable` answer "should the caller re-issue this op": yes for
  transport failures, redirects, topology conflicts (409), and 5xx. No for
  not-found (404), invalid (400), and local request errors.
- `ReliablePublisher` stamps a stable producer id and a monotonic sequence under
  `fibril.client.*` and retries until durably confirmed, re-sending the same
  sequence. It is at-least-once today and becomes effectively-once with no API
  change once the broker dedups on those keys.

## Lessons learned (porting)

- **Read the whole Rust subsystem for a feature before writing it.** Where this
  was done (routing) it landed in one clean pass. Where it was not (the
  subscription supervisor, started single-partition before seeing the fan-in and
  owner-check shape) it took several reshapes. The cheapest unit of work is "one
  fully-understood subsystem", not "one small diff".
- **Validate the wire format against a real broker before building on it.** A
  single wrong byte invalidates every layer above. Catching it at layer one is
  free. Catching it at the end is not.
- **The fake in-process broker should use the real codec.** Then integration
  tests exercise the actual bytes, and an op the broker does not handle (an
  unanswered topology fetch, a missing reconcile reply) surfaces as a precise
  hang rather than a wrong value.
- **Decide the continuity ownership early** (reconcile vs supervisor). It is the
  one place the layers genuinely interact, and getting it wrong is a deadlock,
  not a wrong answer.
- **Mine the reference client's tests.** Many client behaviors (pool pruning,
  retry classification, reconcile-on-resume-rejected, producer-id stamping) have
  a direct test in the Rust client that ports cleanly.

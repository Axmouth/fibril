# Go client plan

The Go client (`clients/go`) ports the reference client to full feature parity.
Read `../ARCHITECTURE.md` (layering, invariants, porting lessons) and
`../FEATURE_MATRIX.md` (the parity checklist) first - this doc records the
Go-specific decisions.

## Decisions

- **Module** `github.com/Axmouth/fibril/clients/go`, package `fibril`, Go 1.23.
  Lives in the monorepo alongside the other clients; the shared wire vectors
  (`../wire_vectors.json`) and error-guide vectors (`../error_guides.json`) are
  read from `..`.
- **Concurrency: mirror the reference client, not the event-loop shape.** Go's
  goroutines + channels + `select` map almost 1:1 onto the reference client's
  concurrency (it happens to be Rust: `tokio::spawn` + `mpsc` + `tokio::select!`),
  so the engine uses a command channel, a read goroutine, and a
  request-id -> reply-channel map like the reference - closer than the asyncio/TS
  "drop the sync primitives" port. The
  ARCHITECTURE invariants (byte-exact wire, FNV-1a, cache-only routing,
  compliance marker, server-side auto-ack, reserved headers) are language
  agnostic and hold unchanged.
- **Wire codec idioms:**
  - Option fields are Go pointers (`*string`, `*uint64`, `*UUID`), `nil` = none.
  - `UUID` is `[16]byte` (value type, echoed opaque). `Headers` is
    `map[string]string`. `ContentType` is `{Kind ContentKind; Custom string}`.
  - The reader carries a **sticky error**: a failed read sets `r.err`, later
    reads are no-ops, and the error surfaces at `finish()` (plus a trailing-byte
    check). Keeps decoders readable (no per-field error check) while staying safe.
  - Encoders never fail (append to a growable buffer) and return `[]byte`.

## Layer map (ARCHITECTURE's 6 layers -> Go files)

1. `wire.go` - byte-exact codec primitives (writer/reader), field types
   (`UUID`, `ContentType`, `Headers`), FNV-1a. **DONE**, pinned by
   `wire_test.go` against `../wire_vectors.json` + canonical FNV vectors.
2. `messages.go` + `messages_ext.go` - op structs <-> wire, both directions.
   **DONE for ALL 33 shared vectors** (handshake, auth, error, publish family,
   ack/nack, subscribe(+ok), deliver, declare queue/plexus(+ok), topology
   (ok/req/update/ack), reconcile_client, redirect, assignment, going_away,
   subscribe_stream). `wire_test.go` also guards that every vector has a case.
3. `codec.go` - 20-byte frame header + opcode table + buffer-level
   `TryDecodeFrame`. **DONE** (`codec_test.go`: byte layout, round-trip,
   partial/back-to-back framing).
4. `engine.go` + `delivery.go` - one connection, actor-shaped (one run goroutine
   owns all state + is sole writer; a read goroutine feeds it frames; methods use
   a command channel + per-request reply channel; no locks). **DONE** for a single
   connection: HELLO/AUTH handshake (compliance + version checks),
   confirmed/unconfirmed publish, declare-queue, topology, subscribe -> per-sub
   delivery channel (buffered to prefetch), DELIVER routing, ack/nack (fire-and-
   forget, reuse the delivery request id), Ping/Pong heartbeat + timeout, clean
   shutdown. Race-clean vs a net.Pipe fake broker (`engine_test.go`,
   `delivery_test.go`) and validated live end-to-end via `examples/smoke`
   (declare -> subscribe -> publish -> receive -> ack). Reconnect/resume lives in
   the client layer next.
5. `client.go` - pool keyed by host:port, topology cache, FNV partition routing
   (round-robin for keyless), bounded redirect-follow. **Core DONE**: Dial,
   Publish/PublishConfirmed (routed + redirect-follow), Subscribe (routed +
   redirect-follow), DeclareQueue, FetchTopology, Shutdown; deliveries settle via
   the delivery (it carries its engine). Now also: **multi-partition fan-in**
   (`SubscribeTopic`, merges all partitions) and **bootstrap reconnect/resume**
   (reconnects on op with the resume identity; publish/subscribe retry on
   transient). Race-clean tests (redirect, FNV routing, fan-in, reconnect via a
   real TCP fake broker) + live via `examples/smoke`. **TODO**: topology-push
   handling, supervised (failover) subscriptions (re-subscribe a dropped
   partition).
6. `publisher.go` / `subscription.go` / `message.go` - ergonomic handles
   (reliable publisher, message encode/decode). TODO.

Plus `errors.go` (typed `WireError` taxonomy; the `FibrilError` hierarchy grows
as higher layers land).

## Status / next bricks

1. **DONE:** wire codec + **all 33 message ops** + FNV, green against the shared
   vectors (byte-exact encode, decode round-trip, canonical FNV, coverage guard).
   The entire byte surface - the highest-risk part - is proven identical to the
   broker.
2. **NEXT:** frame codec (`codec.go`) - the 20-byte header (u32 len, u16 version,
   u16 opcode, u32 flags, u64 request_id) + TCP stream framing. See
   `../python/src/fibril/codec.py`. Opcodes live in the protocol; port the op
   number table.
3. Then the engine (one connection, reference-style: command channel + read goroutine
   + reply-channel map + heartbeat). Cross-check against a REAL broker here
   before building higher, per ARCHITECTURE.
4. Then client + routing (pool, topology cache, redirect follow, reconnect), then
   the public handles (publisher/subscription/message).
5. Add a Go column to `../FEATURE_MATRIX.md` once there is a usable client
   surface to track.

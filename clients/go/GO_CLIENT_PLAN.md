# Go client plan

The Go client (`clients/go`) ports the Rust reference to full feature parity.
Read `../ARCHITECTURE.md` (layering, invariants, porting lessons) and
`../FEATURE_MATRIX.md` (the parity checklist) first - this doc records the
Go-specific decisions.

## Decisions

- **Module** `github.com/Axmouth/fibril/clients/go`, package `fibril`, Go 1.23.
  Lives in the monorepo alongside the other clients; the shared wire vectors
  (`../wire_vectors.json`) and error-guide vectors (`../error_guides.json`) are
  read from `..`.
- **Concurrency: mirror the Rust reference, not the event-loop shape.** Go's
  goroutines + channels + `select` map almost 1:1 onto the Rust client's
  `tokio::spawn` + `mpsc` + `tokio::select!`, so the engine uses a command
  channel, a read goroutine, and a request-id -> reply-channel map like the
  reference - closer than the asyncio/TS "drop the sync primitives" port. The
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
2. `messages.go` - op structs <-> wire, both directions. **Core set DONE**
   (17 ops: hello, hello_ok, auth, error, publish, publish_delayed, publish_ok,
   ack, nack, subscribe, subscribe_ok, deliver). Remaining ops are the next brick.
3. `codec.go` - 20-byte frame header + TCP stream framing. TODO.
4. `engine.go` - one connection: handshake/auth/heartbeat, request-id->reply
   channel, per-subscription delivery, read/write goroutines. TODO.
5. `client.go` - pool keyed by host:port, topology cache, partition routing,
   bounded redirect-follow, reconnect. TODO.
6. `publisher.go` / `subscription.go` / `message.go` - public API. TODO.

Plus `errors.go` (typed `WireError` taxonomy; the `FibrilError` hierarchy grows
as higher layers land).

## Status / next bricks

1. **DONE:** wire codec + core message ops + FNV, all green against the shared
   vectors (byte-exact, round-trip, canonical FNV).
2. Remaining wire ops: declare (+ok), declare_plexus (+ok), topology_req/ok,
   topology_update (+ack), reconcile_client, redirect, assignment, going_away,
   subscribe_stream (needs the stream-start + filter types). Add each with its
   shared vector.
3. Frame codec (`codec.go`), then the engine (one connection), then client +
   routing, then the public handles. Cross-check against a real broker early
   (before building higher layers on the codec), per ARCHITECTURE.
4. Add a Go column to `../FEATURE_MATRIX.md` once there is a usable client
   surface to track.

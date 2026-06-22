# Python client plan

The Python client (`clients/python`) ports the Rust reference and TypeScript
second client to full feature parity. Read `ARCHITECTURE.md` (layering,
invariants, continuity model, porting lessons) and `FEATURE_MATRIX.md` (the
parity checklist) first - this doc only records the Python-specific decisions.

## Decisions (2026-06-23)

- **Async model: asyncio core + thin blocking facade.** The real client is
  written once on `asyncio` (which is Python's event loop, so ARCHITECTURE's
  "mirror the event-loop shape, drop the sync primitives" guidance ports 1:1 from
  the TS client). The blocking client is a facade: it runs the async client on a
  dedicated background event-loop thread and bridges each call via
  `asyncio.run_coroutine_threadsafe(...).result()`, with a queue bridge so a sync
  `for msg in sub:` drains the async subscription. Rationale: do NOT write a
  second full implementation - one source of truth for the hard parts (routing,
  redirects, continuity/supervisor, reconnect); async is also the more efficient
  core. Heavier deps than a pure-sync client is an accepted trade.
- **Minimum Python 3.11** - chosen on a DEBUGGING-RISK basis, not just features:
  the two trickiest parts (the supervisor/fan-in task lifecycles and the blocking
  facade's cross-thread event-loop bridge) are exactly where modern asyncio
  removes footguns. `asyncio.TaskGroup` gives structured cancellation + exception
  propagation (avoids orphaned-task / swallowed-exception / shutdown-deadlock
  rabbit holes); `asyncio.Runner` + cleaner cancellation de-risk the facade.
  3.9/3.10 are EOL / near-EOL so the reach below 3.11 is nearly gone.
  - KEEP IT 3.10-PORTABLE: isolate the few 3.11-only bits behind small internal
    helpers so a future 3.10 backport is localized, not a sweep. Mainly: wrap
    `TaskGroup` usage in one nursery-style helper; prefer `asyncio.wait_for` over
    bare `asyncio.timeout` patterns where trivial; avoid gratuitous 3.11-only
    syntax. (A 3.10 backport is a future item; 3.9 is not a target - it forces
    `from __future__ import annotations` everywhere plus older-asyncio quirks.)
- **Build straight to full parity** - do not stop at a single-broker milestone.
  Still build bottom-up (wire first); the goal is the whole FEATURE_MATRIX.

## Layer map (ARCHITECTURE's 6 layers -> Python modules)

1. `wire.py` - byte-exact codec vs `crates/protocol/src/v1/wire.rs` (hand-rolled
   with `struct`, big-endian, length-prefixed, 16-byte uuids, 1-byte option tags,
   4-byte magics). The only layer that touches bytes. Pin with round-trip +
   byte-layout tests AND the FNV-1a 64-bit canonical vectors (partition hashing
   must match byte-for-byte: basis `0xcbf29ce484222325`, prime `0x100000001b3`).
2. `codec.py` - 20-byte frame header + TCP stream framing.
3. `frames.py` - idiomatic dataclasses <-> wire, both directions for every op, so
   the in-process fake broker speaks real bytes.
4. `engine.py` - one connection: handshake/auth/heartbeat, request-id->Future
   map, per-subscription `asyncio.Queue` delivery, read/write loops. No locks.
5. `client.py` - pool keyed by host:port, topology cache, partition routing,
   bounded redirect-follow, reconnect policy. Plain dict/int fields.
6. `publisher.py` / `subscription.py` / `message.py` - public API: publisher +
   `ReliablePublisher` + `expiring`, supervised fan-in subscription (with
   live-grow partition pickup), message encode/decode.

Plus `errors.py` (typed `WireError` taxonomy + the FibrilError hierarchy) and a
blocking facade module (`blocking.py` or a `sync` subpackage).

## API shape (Pythonic)

- `async with await Client.connect("host:port") as client: ...`
- Subscriptions are async iterators: `async for msg in sub: await msg.complete()`.
- `await client.publisher("t").publish(obj)`; `.reliable()`, `.expiring(ttl)`.
- TTL/delay take `float` SECONDS or a `datetime.timedelta` (Python is
  seconds-native; this matches Rust's seconds-or-Duration, and differs from the
  TS client's milliseconds - each client follows its own language convention).
- Blocking surface mirrors it without async: `with Client.connect(...)`,
  `for msg in sub:`.

## Choices to pin

- Payloads: `msgpack` (PyPI) for the msgpack content type; JSON/text/raw builtin.
  Wire bodies are hand-rolled, not msgpack.
- Full type hints + `py.typed`; pyright/mypy clean.
- `pyproject.toml`; package name `fibril`.
- Tests: a fake in-process broker using the REAL Python codec (an unhandled op
  surfaces as a precise hang, not a wrong value), and mine the Rust/TS tests
  (pool pruning, retry classification, reconcile-on-resume-rejected, producer-id
  stamping, FNV vectors, grow-pickup, assignment events). Cross-check the codec
  against a live broker before building higher layers.

## Build order (bottom-up; goal is full parity, not a single-broker stop)

Start a FRESH session for the implementation - it is a large single push. Steps:

0. Scaffold `clients/python/`: `pyproject.toml` (package `fibril`, requires-python
   `>=3.11`, dep `msgpack`), `src/fibril/` layout, `py.typed`, a test runner
   (`pytest` + `pytest-asyncio`), and CI wiring mirroring the TS client's
   examples-as-tests job if cheap.
1. `wire.py` + tests: byte-exact codec for every op body. FIRST cross-check a few
   encodings against the running broker (or against bytes captured from the
   Rust/TS round-trip tests) before going further. Include the FNV-1a vectors.
2. `codec.py` (frame header + framing) + `errors.py` (WireError taxonomy +
   hierarchy).
3. `frames.py` adapter + an in-process fake broker (real codec) for tests.
4. `engine.py` (one connection, asyncio) + single-connection publish/consume.
5. `client.py` routing/pool/topology/redirect/reconnect.
6. `publisher.py` / `subscription.py` / `message.py` (incl. ReliablePublisher,
   expiring, supervised fan-in with live-grow pickup, exclusive groups +
   assignment events).
7. Blocking facade (background loop thread + `run_coroutine_threadsafe` + a queue
   bridge for sync subscription iteration).
8. Update `FEATURE_MATRIX.md` (add a Python column), `implemented-surface.md`
   (Client Surface), and the docs per the docs-currency directive.

## Parity target

Everything in `FEATURE_MATRIX.md` that is `done` for Rust + TS: core protocol,
publish (incl. delayed + message TTL `expiring` + queue `default_message_ttl`),
consume (manual/auto ack, retry-after, prefetch, declare+DLQ, multi-partition
fan-in incl. live-grow pickup), reconnect/resume, cluster routing (topology
cache, pool, partition-key + round-robin, redirect-follow, transient-failover
retry, subscription supervisor), reliability (retry classification, reserved
headers, ReliablePublisher + producer-id dedup, exclusive consumer groups +
cohort member id, assignment-events stream), typed `WireError`.

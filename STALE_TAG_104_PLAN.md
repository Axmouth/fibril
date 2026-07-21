# #104 stale-tag marking: plan and acceptance criteria

Branch: `feature/stale-tag-104`. The long-term-correct settlement model, no
bandaid: a delivery's settlement is keyed by the durable
`(topic, group, partition, tag)` and routed to whatever engine is currently
live for the connection, not to a per-delivery channel tied to the receiving
engine. Each delivery carries the connection incarnation it arrived on; a
non-resumed reconnect (or a broker restart reported as resumed-after-restart)
bumps the incarnation, so a held delivery settles to a typed stale error, while
a resumed reconnect keeps the incarnation and the delivery still settles
through the new engine.

## Design (as built for the Rust reference)

- `SettleContext` (one per connection slot, outlives every engine incarnation):
  one atomically-swapped `EngineBinding { incarnation, tx }` + a monotonic
  `next_incarnation` allocator. Pairing the incarnation with the sender in one
  swapped value means a settle observes both from a single snapshot.
  - `bind(fresh_session, tx) -> u64` on every (re)connect points settlement at the
    live engine and RETURNS the incarnation this engine must stamp. A non-resumed
    reconnect (`fresh_session = true`) allocates a fresh incarnation so held
    deliveries from the replaced session go stale; a resumed reconnect reuses the
    current one. The reader loop captures the returned incarnation ONCE and stamps
    it on every delivery (never a live re-read), so a late delivery on a superseded
    engine stays stale.
  - `settle(incarnation, cmd)`: loads the current binding once; returns
    `StaleDelivery` if the incarnation moved on, else sends `cmd` to the live
    engine.
- `Command::Ack`/`Nack` carry `(topic, group, partition, delivery_tag,
  request_id)` and build the frame directly - settlement no longer depends on
  the client sub id, so a reconnect that re-keyed the subscription still routes
  correctly.
- `InflightMessage` carries `topic, group, partition, incarnation, settle:
  Arc<SettleContext>`. The per-delivery oneshot + spawned settle task are gone.
- A fresh incarnation is allocated on any reconnect whose outcome is not
  `Resumed` (New / ResumeRejected / ResumeNotFound / ResumedAfterRestart).

## Acceptance criteria

Each applies to EVERY client (Rust done, then TS, Python, Go, C#).

- A1. Settling a delivery held across a NON-resumed reconnect (or a broker
  restart) returns a typed `StaleDelivery` error and sends no settle frame.
  The message redelivers on the current subscription per at-least-once.
- A2. Settling a delivery held across a RESUMED reconnect (within grace)
  succeeds: the settle routes to the current engine and the broker accepts it
  (the tag is still valid on the same session). No StaleDelivery.
- A3. Settlement is keyed by `(topic, group, partition, tag)`, not the client
  sub id, so a reconnect that re-keyed the subscription (Keep with a new server
  sub id) still settles correctly.
- A4. The typed stale error is distinct from a transport `BrokenPipe`: a caller
  can tell "the tag is dead, the message will redeliver" from "the connection
  is down, retry". `StaleDelivery` classifies as do-not-retry.
- A5. Auto-ack subscriptions are unaffected (settled server-side; no client
  settle path).
- A6. No regression: existing settle behavior on a live connection (ack, fail,
  retry, retry_after with a deadline) is byte-for-byte the same on the wire.
- A7. Each client has tests proving A1 and A2 at least at the unit level, plus
  one integration test over real wire proving A1 across a reconnect.

## Verification (Rust reference)

- A1: `settling_a_stale_delivery_returns_stale_error` (unit: a fresh rebind
  bumps the incarnation, the held delivery keeps its own stamp and goes stale),
  `held_delivery_becomes_stale_across_a_non_resumed_reconnect` (integration,
  real TCP mock + reconnect).
- A2: `settling_within_the_same_incarnation_routes_to_current_engine` (unit:
  a resumed rebind keeps the incarnation - asserted equal - and the ack routes
  to the new engine).
- A3: covered by A2's routing (settlement carries topic/partition, not sub id)
  and the reconnect integration test.
- A4: `StaleDelivery` is a distinct `FibrilError` variant, classified
  `DoNotRetry` in `retry_advice`.
- A6: `retry_after_sends_delayed_nack` asserts the `Command::Nack` deadline;
  the full client suite (redirect/reliability integration) passes unchanged.

## Client parity plan (TS, Python, Go, C#)

Scoped from a full map of each client's settle + reconnect path. The wire ack/nack
is ALREADY keyed by `(topic, group, partition, tag)` with no sub id in every
client, so no protocol change is needed. The shared gap: each client pins a
delivery to the concrete engine it arrived on, so a delivery settled after a
reconnect targets the dead old engine (silent drop or generic BrokenPipe). None
act on `ResumeOutcome`, none have an incarnation, none have a StaleDelivery error.

Uniform model to add in each client (mirrors the Rust reference):
- A persistent per-endpoint settle context (current engine binding + a monotonic
  incarnation allocator) that outlives every engine. `bind(fresh_session, engine)`
  returns the incarnation the engine must stamp; a non-resumed reconnect
  (`ResumeOutcome != Resumed`) allocates a fresh one, a resumed reconnect reuses
  the current one.
- Each engine stores its bound incarnation as an immutable field and stamps it on
  every delivery it hands out (NOT a live re-read - this is the bug the Rust
  adversarial review caught).
- The delivery carries `(topic, group, partition, tag)` + its incarnation + a
  reference to the settle context. Settle loads the current binding as one
  snapshot: incarnation mismatch -> typed StaleDelivery (no frame sent), else route
  to the live engine.
- A new typed StaleDelivery error, classified do-not-retry.

Per-client placement (the persistent context's home):
- TS: DONE. Added a `SettleContext` (engine.ts) held by the bootstrap `EngineSlot`
  and by each `PooledConnection`; `Engine.start` reserves the incarnation from the
  resume outcome and binds the context, the deliver loop stamps that captured
  incarnation + topic/group/partition on `InternalInflight`, and `InflightMessage`
  routes settles through the context. New `StaleDeliveryError` (do-not-retry).
  Tests: `a manual delivery held across a non-resumed reconnect settles stale` (A1),
  `... resumed reconnect settles to the current engine` (A2/A3, asserts the ack
  lands on the second socket), plus the retryAdvice case (A4). Full TS suite green.
- Python: DONE. Added a `SettleContext` (engine.py) held by the bootstrap `Client`
  and by each `_PooledConnection`; `Engine.start` reserves the incarnation from the
  resume outcome and binds the context, `_on_deliver` stamps that captured
  incarnation + topic/group/partition onto `Inflight`, `Engine.ack`/`nack` build
  from the durable coords, and `InflightMessage` routes settles through the context
  via `current_or_stale`. New `StaleDeliveryError` (do-not-retry). Tests in
  test_engine.py: held-across-non-resumed goes stale (A1), held-across-resumed
  routes to the current engine with the origin engine dropped (A2/A3), retry_advice
  classification (A4). Full suite green, mypy clean on src.
- Go: DONE. Added a `settleContext` (engine.go, atomic.Pointer binding) held
  per-endpoint by the `Client` (a `settle` map beside `reconcile`) and threaded via
  an unexported `EngineOptions.settle`; standalone `Connect` gets a fresh one.
  `startEngine` reserves the incarnation from the resume outcome and binds before
  starting goroutines, `handleDeliver` stamps the context + incarnation on the
  `Delivery` (which already carried topic/group/partition), and Complete/Fail/Retry/
  RetryAfter route via `currentOrStale`. New `StaleDeliveryError` (do-not-retry).
  Tests in delivery_test.go: A1 stale, A2/A3 route-to-current (origin dropped), A4
  classification. go vet + full suite green.
- C#: no slot - `Client._bootstrap` + `_pool`; the per-endpoint `ReconcileRegistry`
  is the natural home. `Delivery` (readonly struct) already carries the tuple; add
  incarnation + context ref and change the settle methods from void so they can
  surface StaleDelivery.

Acceptance criteria A1-A7 apply per client; each needs A1 + A2 at unit level and
one reconnect integration test proving A1 (matching the Rust reference tests).

## Multi-angle review

### /simplify (reuse, simplification, efficiency, altitude) - DONE

Applied:
- Efficiency/simplification (3 agents): the per-settle body clone was a
  regression. Folded `settle_with`/`as_message`/`ack_command`/`nack_command`
  into one consuming `settle(self, Settlement)` that MOVES headers/content_type/
  payload into the returned `Message` and topic/group into the command - zero
  clones on the settle path.
- Delivery path: destructure the owned `SubState` and MOVE topic/group into the
  `InflightMessage` instead of cloning.
- Dead code: removed the now-unused `let shutdown_acks = shutdown.clone();`
  (its consumer was the deleted per-delivery task).
- Altitude: consolidated the bind+invalidate transition into `start_engine` as
  one ordered operation (invalidate-if-not-Resumed, then bind) BEFORE the reader
  loop can deliver - removed the separate, later `invalidate()` in
  `reconnect_once`. This fixes the awkward bind-then-invalidate ordering and
  keeps the current-tx bound before any delivery.

Accepted (not changed):
- Two pointers to the live engine (SettleContext.current_tx vs
  EngineSlot.engine): they serve different consumers - the reader loop (which
  cannot see the slot) reads current_tx; publish/subscribe ops use
  slot.current(). Unifying would require the reader loop to reach the slot, a
  larger change with no functional gain. Documented as intentional.
- The delivery-path removal of the per-delivery `tokio::spawn` + oneshot is a
  net hot-path WIN (confirmed by the efficiency lens).

### /code-review (adversarial / correctness) - DONE

Two adversarial agents traced the diff (races/ordering and semantics/edge cases).
Both independently flagged the same primary bug.

Fixed:
- **Shared-context stamping race (CONFIRMED by both agents).** Deliveries stamped
  the incarnation with a LIVE read of the shared `SettleContext` at delivery time.
  On an explicit reconnect the old engine's reader loop stays alive until after
  the new engine binds and bumps the incarnation, so a late delivery on the old
  socket would read the NEW incarnation and settle as fresh - acking a tag dead on
  the replaced session (silent duplicate + false Ok), the exact failure #104 is
  meant to prevent. Fix: `SettleContext` now holds one atomically-swapped
  `EngineBinding { incarnation, tx }`; `bind` returns the incarnation, and each
  engine's reader loop captures it ONCE and stamps that fixed value. A late
  delivery on a superseded engine keeps its own (now stale) stamp.
- **TOCTOU in `settle` (CONFIRMED, secondary).** The old `settle` read the
  incarnation and loaded the tx as two separate atomics, so a concurrent reconnect
  between them could route a stale delivery to the new engine. Folding both into
  one `EngineBinding` value means `settle` reads a single consistent snapshot -
  the check and the route can no longer disagree.

Verified safe (no change needed):
- First-connect incarnation, `current == None` window, ack-without-sub-id after a
  re-keyed subscription, double-settle (InflightMessage is not Clone and settle
  consumes self), auto-ack path, Arc lifetimes (a held InflightMessage does not
  pin a dead engine).
- Queue redelivery after StaleDelivery: on disconnect the broker drains and
  unsubscribes the old sub (releasing leases), and a non-resumed reconnect
  re-subscribes via reconcile, so the still-inflight message redelivers.
- `retry_after` uses an absolute deadline computed at settle time, so a long hold
  then retry across a resumed reconnect yields `now + delay`, not a stale absolute.

Out of #104 scope (preexisting, recorded as follow-ups in FOLLOWUPS.md):
- Nack-family settles (`fail`/`retry`/`retry_after`) on a Plexus STREAM manual
  subscription are a broker no-op: `Op::Nack` looks up only queue subs, so the
  frame is ignored and the client still returns Ok. This is a stream settle-model
  gap independent of staleness (a stream's cursor model has no requeue), not a
  regression from this diff. #104's own guarantees still hold for streams: a
  StaleDelivery record was never acked, so it replays from the durable cursor.
- A stream ack sent after a RESUMED reconnect can be dropped if the fan-in
  supervisor has not yet re-subscribed; the cursor is not committed so the record
  replays. At-least-once holds; the resumed-settleability guarantee is best-effort
  for streams. Preexisting timing, recorded as a follow-up.

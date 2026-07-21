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
  `current_tx: ArcSwapOption<Sender<Command>>` + `incarnation: AtomicU64`.
  - `bind(tx)` on every (re)connect points settlement at the live engine.
  - `invalidate()` bumps the incarnation on a non-resumed reconnect.
  - `settle(incarnation, cmd)`: returns `StaleDelivery` if the incarnation moved
    on, else sends `cmd` to the current engine.
- `Command::Ack`/`Nack` carry `(topic, group, partition, delivery_tag,
  request_id)` and build the frame directly - settlement no longer depends on
  the client sub id, so a reconnect that re-keyed the subscription still routes
  correctly.
- `InflightMessage` carries `topic, group, partition, incarnation, settle:
  Arc<SettleContext>`. The per-delivery oneshot + spawned settle task are gone.
- The slot bumps the incarnation on any reconnect whose outcome is not
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

- A1: `settling_a_stale_delivery_returns_stale_error` (unit),
  `held_delivery_becomes_stale_across_a_non_resumed_reconnect` (integration,
  real TCP mock + reconnect).
- A2: `settling_within_the_same_incarnation_routes_to_current_engine` (unit:
  rebind to a new engine, same incarnation, ack routes to the new engine).
- A3: covered by A2's routing (settlement carries topic/partition, not sub id)
  and the reconnect integration test.
- A4: `StaleDelivery` is a distinct `FibrilError` variant, classified
  `DoNotRetry` in `retry_advice`.
- A6: `retry_after_sends_delayed_nack` asserts the `Command::Nack` deadline;
  the full client suite (redirect/reliability integration) passes unchanged.

## Multi-angle review

Run `/simplify` and `/code-review` on the branch; review through efficiency,
altitude, and adversarial lenses before replicating to the other clients. Record
findings and resolutions here.

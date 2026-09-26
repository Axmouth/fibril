# Client lifecycle completion

Complete stream settlement semantics and reconnect continuity coverage before
freezing the client APIs. Current resume, closure and staleness behavior is
specified in [implemented surface](website/src/content/docs/implemented-surface.md#reconnects)
and the [reconnect guide](website/src/content/docs/reliability/reconnects.md).

## Stream settlement semantics

A manual stream NACK currently has no broker effect even when the client call
returns successfully. Define a supported contract across all five clients:

- Decide whether `fail`, `retry` and `retry_after` are rejected with a typed
  unsupported-operation error or have explicit cursor/redelivery semantics.
- Specify ordering, cursor advancement, durable-name ownership and interaction
  with retention before adding stream redelivery behavior.
- Ensure unsupported operations cannot appear to succeed silently.

Acceptance checks must cover Rust, TypeScript, Python, Go and C#, including
retry classification and the absence of unintended cursor advancement.

## Settlement after resumed stream reconnect

A held delivery's cursor ACK can arrive before the subscription supervisor has
re-established the stream subscription. Define a bounded wait or retained-commit
mechanism so a valid settle reaches the new subscription. Specify behavior on
terminal closure, timeout and a subsequent non-resumed reconnect.

Acceptance checks:

- Hold a delivery across a resumed reconnect and settle before re-subscription.
- Verify the durable cursor advances after re-subscription and survives restart.
- Verify a stale incarnation sends no settle frame.
- Exercise repeated reconnects and shutdown without unbounded buffering or hangs.

## Automatic recreation continuity

Add the focused auto-resubscribe-on counterpart to the existing opt-out close
scenario. Force a safe `RecreateClientSide` verdict and verify that:

- the same application subscription continues receiving;
- the supervisor binds to the replacement server subscription;
- unsettled queue messages remain eligible for redelivery;
- stale held deliveries cannot settle against a new incarnation;
- terminal errors still surface once, and shutdown terminates the supervisor.

Exercise the behavior across all five clients using their typed receive APIs.
Keep queue and stream scenarios distinct because their settlement models differ.

## Further scope

Cross-node session transfer, preservation of inflight ownership across process
restart, and automatic recreation of auto-ack subscriptions require separate
semantics and are outside this completion pass.

Historical protocol decisions and validation records are in
[Reconnect design](archive/implementation-notes/RECONNECT_DESIGN.md) and
[stale-delivery settlement](archive/implementation-notes/STALE_TAG_104_PLAN.md).

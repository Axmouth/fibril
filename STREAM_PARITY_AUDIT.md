# Stream Operational Parity Audit (2026-07-03)

Plexus streams landed feature-first, so this audit sweeps the operational
machinery queues accumulated (eviction, metrics, lifecycle, recovery) and
checks what streams share, what they miss, and what does not apply to them by
design. Companion perf findings live in the S section of
PERF_AUDIT_HOT_PATHS.md.

## Parity table

| Area | Queues | Streams | Verdict |
| --- | --- | --- | --- |
| Idle eviction | `QueueActivity` leases, eviction worker, `queue_idle_evict_after_ms` | fixed: activity signal plus sweep worker via the `stream` runtime settings, plus a dirty-gate on the ephemeral flush ticker | FIXED (P1) |
| Broker traffic metrics | `published_many` / `delivered_many` / `redelivered_many` into `BrokerStats` | fixed: publish and delivery paths count into `BrokerStats` | FIXED (P2) |
| Lag safety under overload | credit-based backpressure, no loss | fixed: eviction plus watermark re-attach keeps delivery contiguous, auto-ack can only settle delivered records | FIXED (P3, 168b44d) |
| Graceful shutdown | waits for pending settles to drain | fixed: graceful shutdown flushes pending cursor commits per channel | FIXED (P4) |
| Lag observability | n/a (credit model) | fixed: per-partition live subscription count and lag recovery counter in stream stats and the admin streams page | FIXED (P5) |
| Reconnect grace (transport swap) | delivery re-targets the live transport | same, `send_to_current_transport` with the transport watch | OK |
| Resubscribe across full reconnect | broker-side reconcile registry | client-side by design: fan-in supervisor plus durable-cursor resume, deliberately not in the reconcile registry | OK by design |
| Cold start and failover reconciliation | #101 machinery | `apply_stream_assignment_transition`, lazy materialization from coordination config, promote-to-local-tail | OK |
| Recovery quarantine | stroma quarantine map | shared, keyed (topic, partition, group) with `PartitionKind` awareness | OK |
| Drain notice (planned restart) | GoingAway broadcast | connection-level, covers stream subscribers | OK |
| Runtime settings | queue tunables live-read | cursor-commit window/batch plus replication stream settings, live-read per window | OK |
| Admin surface | queues page + debug | streams page, streams_debug, declare from UI | OK |
| Retention | TTL + DLQ (consume-delete model) | retention worker (age/bytes/records) in stroma with a background sweep | OK (different model by design) |
| DLQ, per-message TTL, settle drain | queue concepts | not applicable, records are retention-bound log entries | N/A |

## Gap details

### P1: no idle eviction for stream channels

`Broker.streams` is a bare DashMap. Each `StreamChannel::open` spawns the
fan-out actor, ingest task, drain task, cursor committer, and (ephemeral
tier) a 5ms flush ticker, and none of it is ever torn down while the process
lives. Queues evict after `queue_idle_evict_after_ms` with activity leases
tracking live publishers/consumers.

The teardown path already exists and is sound: dropping the `StreamChannel`
closes ingest, which closes the drain, which drops the last actor sender, and
the cursor committer flushes pending commits on channel close. Lazy
rematerialization also exists (`route_stream` reopens from coordination
config). What is missing is only the activity signal (last publish time plus
live subscriber count) and a sweep that removes idle entries, mirroring the
queue worker. The ephemeral flush ticker makes idle channels an active CPU
cost, not just memory, so this matters more per-channel than for queues.

### P2: stream traffic invisible in broker metrics

Stream publishes and fan-out deliveries bypass `BrokerStats` entirely, so
admin rates, dashboards, and anything built on the counters see zero traffic
from a stream-only workload. Fix is counter calls at the ingest drain
(published) and the delivery task (delivered), batched amounts where SF1
lands batches.

### P3: lagged subscriber gap-skip (correctness) - FIXED (168b44d)

When a subscriber's live channel is full the fan-out drops the record for
that subscriber and sets `lagged`, on the argument that a durable consumer
re-reads the gap from its cursor after a reconnect. Two holes: nothing
triggers that reconnect (the subscriber stays registered and keeps receiving
later records), and auto-ack settles each later delivered record, advancing
the durable cursor past the dropped offsets. Result: a connected subscriber
that briefly lags silently loses records, on every tier, durable included.

Fixed as designed: the fan-out evicts a slow subscriber (contiguous
undelivered suffix instead of holes) and the subscription driver re-attaches
from its delivery watermark, replaying the suffix from ring or log before
rejoining live. Regression test forces a full live channel and asserts
exactly-once in-order recovery. Client lag notification events and the
skip-to-latest / close policies are deferred to the typed
subscription-lifecycle surface. A `lag_evictions` counter now exists on the
fan-out for P5 to surface.

### P4: shutdown does not flush pending cursor commits

`shutdown_graceful` waits for queue settles but never calls
`flush_cursor_commits` on open stream channels, so up to one microbatch
window of cursor advances is lost on a planned restart. At-least-once safe
(the consumer re-reads a small window) but easy to close for parity with the
queue settle drain.

### P5: lag not observable

`is_lagged` has no production caller. Once P3 gives lag a real recovery
path, streams_debug should surface per-subscriber lag state so an operator
can see slow consumers, since lag is the stream-side analog of a growing
queue backlog.

## Suggested order

All five gaps are closed: P3 (168b44d), then P1, P2, P4, and P5 in one parity
pass. The lag notification client events and lag policies remain deferred to
the typed subscription-lifecycle surface.

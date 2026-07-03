# Adaptive Tunables Registry

Living inventory of the points where a fixed tunable could instead derive its
operating point from measured behavior, per the adaptive direction and the
window budget rule (a coalesce window should stay at or under about 10
percent of expected overall latency, 20 percent as the stretch bound).
Current reference numbers: end-to-end publish-to-deliver p50 is 2-4ms and
durable confirm p50 is 4-5ms on fast storage (1KB, single node).

The goal state for sensitive dials is ADAPTIVE: the value derives at RUNTIME
from a measured signal, held in an atomic read at use-time and written by a
slow observer task with hysteresis and clamps, with config providing bounds
and a pin override. A static constant change is only a sensitivity probe: if
moving the constant moves nothing, the dial is insensitive and gets a plain
constant with no controller.

Status: OPEN (not started), PROBED (sensitivity measured, controller pending
or declined), ADAPTIVE (runtime-derived), N/A (assessed, no action needed).

| Site | Where | Current | Should key off | Budget share now | Ease | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Settle coalesce window | broker.rs spawn_settle_loop | 100us, 64 items | probed 500us vs 100us: identical everywhere, the window is not on the measured critical path at standard prefetch (confirms ride the publish pipeline, credit release decoupled from delivery) | none measurable | trivial | PROBED, controller declined |
| Publisher sink window | broker.rs publisher_sink | 250us, 256 items | observed confirm p50, though the settle probe suggests low sensitivity here too | 6-12 percent, at budget | trivial | OPEN, deprioritized |
| Connection writer flush | handler.rs writer task | 32 frames high-prio, 128/1MB/5ms | frame arrival rate | load-gated, sparse flushes immediately | easy | N/A for now |
| Keratin batch linger | keratin config.rs, writer.rs | adaptive 0-5ms by batch fill | already adaptive | load-gated | done | ADAPTIVE |
| Keratin fsync idle floor | keratin writer.rs commit_when_idle | 0 (self-clock) | EMA of fsync duration | n/a, cadence policy | moderate | OPEN |
| Startup timer-floor probe | new util, feeds all windows | none | measured sleep granularity | fixes Windows oversleep class | moderate | OPEN |
| Stream cursor commit linger | replication_stream_apply_linger_us | fixed setting | cursor commit round-trip | stream path | moderate | OPEN |
| Replication decode offload | protocol replication read apply | spawn_blocking by judgment | EMA of per-byte decode cost vs scheduler tick budget | cluster path | harder | OPEN |
| Follower caught-up poll | replication worker config | 1000ms | owner notify or long-poll instead | idle replica-durable confirm latency | harder | OPEN |
| Delivery poll batch size | delivery loop total_cap | credit-derived | already demand-driven | n/a | done | ADAPTIVE |

Experiments land with before/after numbers in the optimization log. The
guardrails for anything promoted to ADAPTIVE: hysteresis on slow signals,
bounded ranges with a config pin override, and the derived value visible in
the debug surface.

## Dispatch congestion model

Microbatching has two separate jobs. Under load, batches form from backlog
(the drain loop fills toward MAX_BATCH with no waiting), so the max batch
size is what guards actor dispatch congestion. The coalesce window only
pre-forms batches at low load, where congestion does not exist, so windows
trade latency and nothing else.

Rough sizing arithmetic: a queue actor is serial with a measured command cost
around 0.3-5us, so its ceiling is roughly 200k-1M commands per second per
queue. A path dispatching per item (batch factor 1) congests at a few hundred
thousand messages per second, which matches where the pre-batching delivery
knee sat (350-400k/s). With batch factor B the threshold scales to about
B x 200k-1M/s, so B of 8 or more pushes actor dispatch past every other
limit. Current settle (64) and publish (256) max batches have three orders of
magnitude of headroom at 150k/s.

Cap sizing notes: raising the settle max batch has no measurable upside while
actor dispatch holds ~1000x headroom, and backlog already self-sizes batches
below the cap, so caps stay static safety bounds. The actionable gap is that
the publish sink cap is count-only (256) while publish batches carry
payloads: at 1MB payloads that is a 256MB staged burst into one engine call.
Add a byte budget alongside the count cap (a few MB), the same fix the
replication read path needed when count limits allowed giant responses.

# Adaptive Tunables Registry

Living inventory of the points where a fixed tunable could instead derive its
operating point from measured behavior, per the adaptive direction and the
window budget rule (a coalesce window should stay at or under about 10
percent of expected overall latency, 20 percent as the stretch bound).
Current reference numbers: end-to-end publish-to-deliver p50 is 2-4ms and
durable confirm p50 is 4-5ms on fast storage (1KB, single node).

Status: OPEN (not started), TUNED (constant adjusted per the budget rule),
ADAPTIVE (derives from measurement), N/A (assessed, no action needed).

| Site | Where | Current | Should key off | Budget share now | Ease | Status |
| --- | --- | --- | --- | --- | --- | --- |
| Settle coalesce window | broker.rs spawn_settle_loop | 500us, 64 items | observed delivery p50 | 12-25 percent, over budget | trivial | OPEN |
| Publisher sink window | broker.rs publisher_sink | 250us, 256 items | observed confirm p50 | 6-12 percent, at budget | trivial | OPEN |
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

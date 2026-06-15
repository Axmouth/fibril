# Performance-Sensitive Paths Audit

Status: Audited

Date: 2026-06-14

Scope: publish, delivery, follower replication, replica-durable confirms,
cohort coordination, and client routing surfaces affected by the current
replication and sharding branch.

## Summary

The current hot paths are mostly shaped sensibly:

- publish batching has already been measured and tuned
- delivery-loop changes have prior benchmark evidence
- runtime settings are read through cheap snapshots
- cohort assignment is a subscription/control-plane concern, not a per-message
  delivery decision
- replica progress and assignment state use atomics, `DashMap`, and small locked
  cells rather than broad global locks

The main open risk is not an obvious code smell. It is missing measurement for
the new clustered paths. Replica-durable confirms, follower pull cadence,
multi-queue replication, and exclusive cohort fan-in now need benchmark cases
before we call their performance acceptable.

## Findings

### 1. Replica-durable confirm latency is probably tied to follower poll cadence

Severity: High for cluster latency, low for standalone.

The owner confirm path waits until enough followers report durable progress past
the published offset. That is the right durability contract, but follower
progress currently advances through the pull worker loop. When a follower is
caught up, the worker records `next_delay_ms = caught_up_poll_ms`, whose default
is `1000ms`.

Impact:

- Standalone or local-durable publish is unaffected.
- Replica-durable confirmed publish can add up to the caught-up poll interval
  for the first message after an idle period.
- High-throughput runs may hide this because the follower is already polling
  frequently while behind.

Relevant code:

- `crates/broker/src/broker.rs`: `ReplicationConfirmGate::await_confirm`
- `crates/broker/src/broker.rs`: `FollowerReplicationWorkerState::record_catch_up`
- `crates/broker/src/broker.rs`: `run_follower_replication_worker_loop`

Recommendation:

- Add a benchmark case with one owner and one follower, replica-durable confirms
  enabled, and low offered load.
- Measure confirmed publish p50/p95/p99 with `caught_up_poll_ms` at `1000`,
  `100`, and a lower test value.
- If the expected 1s idle penalty appears, design owner notification or a
  long-poll style replication read with a cooldown. Do not just lower the
  default blindly.

Update, 2026-06-16:

- The live tryout benchmark now emits broker replication timing from the admin
  queue debug surface.
- In a 3-node Ganglion `replica_durable:2` publish-only run at 50k/s, 1 KiB
  payloads, and confirm window 1024, client-observed confirm latency was
  p50/p95/p99/max = 204/243/244/246ms.
- The owner-side `replica_confirm_wait` metric, which measures only the wait
  after local owner append until follower durable progress is observed, averaged
  about 0.033ms over 144,447 samples. That means this run's visible latency was
  not mostly the post-append replica wait.
- The larger signal was follower work: owner read averaged about 1.0ms,
  follower owner-read await averaged about 6.3ms, follower apply averaged about
  19.2ms, and whole follower ticks averaged about 552ms.

Revised interpretation: caught-up poll cadence is still a low-load/idle risk to
test, but the current 50k/s latency shape is more likely dominated by local
append completion, client confirm-window backlog, follower tick batching, and
follower durable apply cost. The next benchmark should separate local append
completion latency from replica-gate wait before changing the poll cadence
again.

### 2. Cluster-mode benchmark coverage is behind the implementation

Severity: High for confidence.

The current benchmark scripts are useful for standalone throughput, payload
size, latency, missing messages, and RSS. They do not yet isolate the branch's
new paths:

- not-owner redirect and retry
- multi-partition keyed publish routing
- multi-partition subscription fan-in
- follower pull catch-up
- replica-durable confirms
- exclusive cohort gate and assignment push

Impact:

- Optimizing standalone publish and delivery can miss regressions in cluster
  mode.
- Replica durability may look correct in tests but have unacceptable latency
  or backlog behavior.
- Follower replication may pass correctness tests while creating too much
  connection, lock, or scheduler pressure at queue counts that matter.

Recommendation:

- Add a small cluster benchmark profile to `scripts/bench-matrix.sh`.
- Keep it short and diagnostic at first:
  `replica-confirm-low-rate`, `replica-confirm-knee`,
  `partitioned-fan-in`, and `redirect-publish`.
- Record broker logs separately, as the current scripts already do.

### 3. Per-owner protocol replication peers serialize requests

Severity: Medium for many followed queues, low for the current first version.

`CoordinationProtocolOwnerPeerResolver` caches one protocol peer per owner id.
`ProtocolOwnerReplicationPeer` serializes requests over one connection through a
`Mutex<Option<Conn>>`.

That is a good conservative starting point because it keeps the protocol simple,
limits connection count, and avoids multiplexing bugs. It can become a
bottleneck when one follower owns many follower workers pulling from the same
owner.

Relevant code:

- `crates/protocol/src/v1/replication.rs`:
  `CoordinationProtocolOwnerPeerResolver`
- `crates/protocol/src/v1/replication.rs`: `ProtocolOwnerReplicationPeer`

Recommendation:

- Keep the design for now.
- Add a many-queue follower catch-up benchmark before changing it.
- Only consider per-queue peers or request multiplexing if the benchmark shows
  serialization is the limiting factor.

### 4. Cohort routing uses a mutex, but currently on control paths

Severity: Low.

The exclusive-group router is behind a mutex. The important detail is where it
is used: subscribe, unsubscribe, membership snapshots, and coordinator plan
application. Delivery itself reads the resolved `exclusive_assignee` atomic on
the queue loop state.

Impact:

- Per-message delivery does not take the cohort mutex.
- Large membership churn or frequent heartbeat-label snapshots can contend with
  subscribe/leave, but this is not a normal message hot path.

Relevant code:

- `crates/broker/src/broker.rs`: `ExclusiveGroupRouter`
- `crates/broker/src/broker.rs`: `Broker::local_cohort_membership`
- `crates/broker/src/broker.rs`: `QueueLoopState::exclusive_assignee`

Recommendation:

- Do not optimize this now.
- Revisit only after membership-churn or large-cohort tests show contention.
- The next correctness cleanup, `sub_id`-scoped leave, is more important than
  changing the lock shape.

### 5. Delivery loop allocation is known, but not yet proven worth changing

Severity: Medium for standalone throughput, low for immediate work.

The delivery loop still rebuilds a consumer vector, scans capacity, clones queue
keys, records tag maps, and constructs delivery messages. This is the same class
of cost already noted in the optimization log.

Prior measurement matters: one attempted reduction of per-message metric and
activity work did not improve the end-to-end path and was reverted.

Recommendation:

- Keep the current shape until a targeted benchmark points at this path.
- If revisited, start with a microbench for consumer selection and delivery
  record construction.
- Avoid fairness or backpressure changes unless the benchmark includes tail
  latency and redelivery behavior.

### 6. Topic/group allocation remains a broad cleanup, not a quick win

Severity: Low now, potentially medium for many queues.

The code still clones topic and group strings in client routing, broker records,
coordination snapshots, and admin reports. This is a real cleanup area, but
interner-style fixes can add locks and lifetime complexity.

Recommendation:

- Fold this into the planned Topic/Group newtype pass.
- Prefer validated types and `Arc<str>` where ownership crosses async tasks or
  maps.
- Benchmark both single hot queue and many sparse queues before claiming a
  performance win.

## What Not To Change Yet

- Do not raise publish batch windows without measuring low-rate latency.
- Do not replace the cohort mutex before membership-churn tests justify it.
- Do not add production percentile histograms on hot paths until the overhead is
  measured.
- Do not optimize replication transport throughput before replica-durable
  confirm latency is measured.

## Recommended Next Work

1. Add a cluster benchmark profile for replica-durable confirmed publish.
2. Add a many-queue follower catch-up benchmark to test the one-peer-per-owner
   design.
3. Add a partitioned fan-in benchmark using the Rust client.
4. Add a short note to benchmark output that says whether durability is local or
   replica-durable.
5. Keep `website/src/content/docs/latest/development/optimization-log.md`
   updated with each result.

## Replica-Durable Throughput Notes

2026-06-15 checks against a 3-node Ganglion-backed tryout cluster found a
repeatable replica-durable ceiling for 1 KiB confirmed publishes:

- `confirm_window=2048`, target 25k/s: about 10.9k/s, p99 about 18 ms.
- `confirm_window=8192`, target 50k/s: about 14.9k/s, p99 about 17 ms.
- `confirm_window=16384`, target 100k/s: lower measured rate and multi-second
  publish-to-deliver latency, while server-receive-to-deliver stayed around
  13-21 ms. This is client/window backlog, not useful throughput.

The current follower worker defaults make that ceiling plausible:

- `max_messages_per_read = 256`
- `max_iterations_per_tick = 8`
- `retry_poll_ms = 100`
- `caught_up_poll_ms = 1000`

Gross lagging capacity is roughly:

`256 messages/read * 8 reads/tick / 0.1s = 20,480 messages/s`

The measured 10-15k/s range is consistent once protocol round trips, durable
replicated appends, event reads, scheduling overhead, and client confirms are
included. This suggests the next throughput work should first target follower
replication worker budget and wakeup behavior, not Keratin write throughput.

Correctness/perf fixes from the same pass:

- Owner replication reads were synchronous Keratin scans inside async Stroma
  methods. They now run in `spawn_blocking`, matching the normal delivery read
  path and preventing replication polling from starving Tokio timers.
- `safe_message_truncate_before()` could return `u64::MAX` when no message
  offsets were retained in state. That sentinel reached Keratin truncation and
  leaked into replica progress. It now clamps to the settled frontier.
- `scripts/cluster-tryout.sh` now checks owner and follower replica cursors after
  steady benchmarks and rejects `u64::MAX` as an invalid cursor.

Next candidate changes:

- Expose replication read budget settings with configuration discipline instead
  of hard-coded worker defaults.
- Add follower wakeups or long-poll-style owner reads so caught-up followers do
  not depend only on `caught_up_poll_ms`.
- Consider a combined Stroma owner replication read that performs message and
  event scans in one blocking task. The current patch uses two blocking scans
  because the public Stroma API already exposes separate methods.
- Add a benchmark variant that records follower loop status, records-per-tick,
  and delay decisions alongside publish latency.

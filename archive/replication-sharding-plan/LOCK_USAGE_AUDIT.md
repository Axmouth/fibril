# Lock Usage Audit

Status: Audited

Date: 2026-06-16

Scope: mutexes, rwlocks, `DashMap`, connection serialization, and related
synchronization on publish, delivery, replication, client reconnect, runtime
settings, coordination, Stroma, and Keratin paths.

## Summary

Most locks found in this pass are either cold-path coordination locks or
intentional guards around state that is not per-message. The main risks are not
"there is a mutex" in isolation. The real risks are:

- locks on replica-durable confirmation progress
- request serialization in the protocol owner-replication peer
- global `DashMap` delivery-tag indexes that are touched on delivery/settle and
  scanned on unsubscribe
- connection subscription state that has a correctness-sensitive check/await/put
  shape
- optional replication cache locking if that cache ever becomes default

This audit should stay separate from the broad performance-sensitive paths audit.
It is a synchronization inventory and replacement plan, not a benchmark result.

## Findings

### 1. Replica progress uses a mutex-protected follower map on the confirm path

Severity: High for replica-durable throughput and latency.

Current shape:

- `Broker::replication_progress` is a `DashMap<QueueKey, Arc<ReplicationProgressCell>>`.
- `ReplicationProgressCell` contains a `std::sync::Mutex<HashMap<String, FollowerProgress>>`
  plus a `Notify`.
- `ReplicationConfirmGate::await_confirm` locks the follower map to check ISR
  freshness and again in a loop to count followers past the requested offset.
- Follower progress updates also lock the same map and notify waiters.

Why this matters:

- This is directly on replica-durable publish confirmation.
- Per-offset waits repeatedly scan follower progress.
- It pushes the design toward per-offset thinking instead of monotonic committed
  watermark progress.

Recommendation:

- Keep the follower membership/progress map for control-plane detail, ISR
  freshness, and observability.
- Add cheap per-queue atomic committed watermarks for the data path:
  `committed_message_next` and `committed_event_next` or equivalent.
- On follower progress update, recompute the required committed watermark once,
  store it atomically, and notify waiters.
- Publish confirm should first read the atomic committed message watermark. Only
  the slow/degraded path should inspect the follower map.
- Delivery visibility, if replica-durable requires "safe before visible", should
  use the same committed watermark instead of consulting per-follower maps.

This is the most plausible lock removal with a real design payoff.

### 2. Protocol owner-replication peer serializes requests over one connection

Severity: High for replication transport throughput, medium for correctness risk
if changed carelessly.

Current shape:

- `ProtocolOwnerReplicationPeer` has `conn: Mutex<Option<Conn>>`.
- It also has `request_lock: Mutex<()>`.
- `read_owner_replication_records` and `export_owner_state_checkpoint` hold
  `request_lock` across send and response receive.
- The connection is taken from `conn`, used, then restored.

Why this matters:

- The lock is intentional: the current protocol connection expects one active
  request/response flow at a time for this peer.
- Removing the mutex without changing the transport shape would create response
  matching and concurrent sink/read hazards.
- Keeping it means one follower-to-owner peer cannot pipeline replication reads
  or checkpoint requests over the same connection.

Recommendation:

- Do not simply remove this lock.
- Treat it as a transport design limit.
- Future options:
  - a dedicated peer actor with an internal request queue
  - request/response multiplexing by request id over one connection
  - separate message and event/progress streams
  - more than one owner peer connection when justified by benchmark data

This aligns with the performance audit's "transport shape" item.

### 3. Delivery-tag indexes use global `DashMap`s on delivery and settle paths

Severity: Medium to high under high inflight counts.

Current shape:

- `records_by_tags: DashMap<DeliveryTag, TagRecord>`
- `tags_by_key_offset: DashMap<(QueueKey, Offset), DeliveryTag>`
- These are touched while issuing deliveries and processing settles.
- `unsubscribe` scans `records_by_tags` globally to find entries for one
  consumer and queue.

Why this matters:

- `DashMap` avoids a single global mutex, but it still has shard locks and per-entry
  overhead.
- A global scan on unsubscribe can become expensive when many queues or
  consumers have large inflight sets.
- This is not the first replication bottleneck, but it is on normal broker data
  paths.

Recommendation:

- Keep for now unless benchmarks show settle/delivery map pressure.
- Future shape to consider:
  - per-queue or per-consumer inflight indexes owned by the queue loop
  - direct `consumer -> tags` index to make unsubscribe O(consumer inflight)
    instead of O(global inflight)
  - batch-oriented removal on settle to reduce repeated shard traffic

This can be measured with a high-prefetch, many-consumer benchmark.

### 4. Protocol logical connection state has a check/await/insert subscription pattern

Severity: Medium for correctness, low to medium for performance.

Current shape:

- `LogicalConnection.state` is a `tokio::sync::Mutex<ConnState>`.
- Subscription install checks `state.subs.contains_key(...)`, releases the lock,
  awaits `broker.subscribe(...)`, then inserts the new subscription under the
  lock.

Why this matters:

- The mutex is not inherently a hot data-path lock. Delivery runs through spawned
  tasks and the transport watch channel.
- The check/await/insert shape can admit races if a client sends concurrent
  duplicate subscribe frames for the same key on one logical connection.
- Holding the mutex across `broker.subscribe` would be worse, so the right fix
  is not "make the lock bigger".

Recommendation:

- Replace the pattern with a reservation state:
  - acquire lock
  - insert `Installing` placeholder or return `AlreadySubscribed`
  - release lock
  - await broker subscribe
  - replace placeholder with `Installed`, or remove on failure
- Add a concurrent duplicate-subscribe regression test.

This is a correctness hardening item more than a performance optimization.

### 5. Queue activity uses a plain mutex, but its hot operation is atomic

Severity: Low.

Current shape:

- `QueueActivity` uses `Mutex<QueueActivityState>` for publisher/subscriber
  lease counts and idle timestamps.
- `last_used_ms` is an `AtomicU64`.
- The frequent `touch()` path only stores the atomic timestamp.

Why this matters:

- Lease creation/drop is not per-message.
- Idle cleanup and admin snapshots can tolerate this lock.

Recommendation:

- Keep the mutex.
- If publisher creation ever becomes a per-message path, fix that caller rather
  than replacing this lock prematurely.

### 6. Follower worker runtime state uses an async mutex once per tick

Severity: Low to medium.

Current shape:

- `FollowerReplicationWorkerRuntime.state` is an `AsyncMutex<FollowerReplicationWorkerState>`.
- The worker locks it to compute catch-up options, then later records the tick
  outcome and reads `next_delay_ms`.

Why this matters:

- This is not per replicated record. It is per follower tick.
- It is less important than the protocol peer serialization and committed
  watermark work.

Recommendation:

- Keep for now.
- If follower tick scheduling remains a bottleneck after watermark work, split
  frequently-read fields such as `message_next`, `event_next`, and
  `next_delay_ms` into atomics and keep the mutex for richer debug state.

### 7. Exclusive cohort and repartition locks are control-plane locks

Severity: Low.

Current shape:

- `exclusive_groups: Mutex<ExclusiveGroupRouter>`
- `repartition_transitions: Mutex<HashMap<...>>`

Why this matters:

- These locks are held during subscribe/unsubscribe, assignment application,
  repartition planning, or heartbeat/report generation.
- They are not per-message delivery locks.
- Current repartition code snapshots or clones state under the mutex, drops the
  guard, then performs async engine calls. It does not intentionally hold the
  sync repartition mutex across `.await`.

Recommendation:

- Keep for now.
- Treat sync mutex guards across `.await` as banned. If future repartition work
  needs async I/O, snapshot the required state first, release the guard, await,
  then reacquire briefly to apply the result.
- If exclusive cohorts become high-churn, split by cohort key or use a
  `DashMap<CohortKey, CohortState>`.
- If repartition transitions grow complex, move them into a small dedicated
  state object with tests rather than replacing the mutex by reflex.

### 8. Stroma replication cache mutex is acceptable only while the cache is opt-in

Severity: Low now, high if enabled by default without redesign.

Current shape:

- `Stroma.replication_cache` is `Option<Arc<Mutex<RecentReplicationCache>>>`.
- Owner message/event insertions and cache reads lock this cache.
- The cache is currently disabled by default because the local benchmark did not
  justify default clone and lock overhead.

Why this matters:

- If enabled globally, this mutex becomes a shared hot lock across all queues.
- A future useful cache likely needs per-queue structure plus global eviction.

Recommendation:

- Keep disabled by default.
- If revisited, design as:
  - per-queue suffix state for cheap range lookup
  - separate global byte-budget/eviction structure
  - metrics for hit rate, miss rate, retained bytes, and lock wait if needed

### 9. Keratin segment mapping uses an rwlock, but it is not the current bottleneck

Severity: Low.

Current shape:

- Keratin `segment_mapping` is a `parking_lot::RwLock<BTreeMap<u64, PathBuf>>`.
- Readers lock it to find the segment for an offset or the next segment.
- Writers update it on segment rollover, truncate, or reset.

Why this matters:

- This is a reasonable local-log metadata lock.
- It is not obviously per-record in sequential scans.
- Replacing it without a measured reader bottleneck risks complexity for little
  payoff.

Recommendation:

- Keep.
- If raw Keratin replication APIs become the next performance target, benchmark
  segment lookup cost as part of that work.

### 10. Settings, metrics, coordination, admin, and client locks are mostly fine

Severity: Low.

Examples:

- Runtime settings load issue mutex is cold.
- Static coordination snapshot `RwLock` is not a data-plane lock.
- Metrics `System` `RwLock` guards system refresh/readback, not per-message
  broker flow.
- Client `EngineSlot.reconnect_lock` intentionally serializes reconnect attempts.
- Client engine/pool `RwLock`s are short-lived handle lookups or replacements.
- Admin test mocks use mutexes only in tests.

Recommendation:

- Keep.
- Do not spend performance budget here unless a benchmark names one of these
  locks.

## Priority List

1. Replace replica-durable per-offset follower-map checks with committed
   watermark progress.
2. Decide the replica-durable delivery visibility contract and reuse the same
   committed watermark there if strong visibility is required.
3. Add a concurrent duplicate-subscribe test and reservation state for
   `LogicalConnection.state`.
4. Treat `ProtocolOwnerReplicationPeer::request_lock` as a transport limit and
   plan pipelining or stream separation instead of removing it locally.
5. Add a high-inflight benchmark before changing broker delivery-tag maps.
6. Keep the Stroma replication cache opt-in until it has per-queue structure or
   strong benchmark evidence.

## Non-Goals

- Do not replace cold mutexes with lock-free structures for style.
- Do not remove request serialization from the protocol owner peer without a
  protocol/transport change.
- Do not optimize Keratin segment mapping until raw replication benchmarks point
  at it.
- Do not hold async mutexes across broker/storage calls to avoid a race. Use
  reservation states instead.
- Do not hold sync mutex guards across `.await`. This is banned even if it
  compiles in a local future, because it can block executor workers, hide
  deadlocks, and make later `Send` requirements fail in surprising places.

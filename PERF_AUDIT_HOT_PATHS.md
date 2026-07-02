# Hot-Path Performance Audit (2026-07)

Scope: the per-message paths — publish -> append -> confirm, poll -> deliver ->
wire, and ack -> settle -> credit release — across fibril (broker, protocol
handler) and the stroma/keratin substrate. The lens is structural cost, not just
micro-optimization: blocking that could happen async, work done N times that
could be done once, and per-message overhead that could batch.

Method note: every change below is a hypothesis until measured. Use
`scripts/bench-matrix.sh` per the optimization-log discipline (baseline +
confirmed for settle/confirm changes, throughput-1k for delivery-loop changes),
median of at least 3 short runs, same machine and settings before/after.

Status legend per finding: OPEN / DONE (with commit) / DROPPED (with reason).

## A. Blocking that could happen async

### A1. Ack durability gates consumer flow-control credit — OPEN

Where: `crates/broker/src/broker.rs` `handle_settle_batch` (~3376-3465),
completion built at 3386-3406; `stroma/core/src/stroma.rs` `ApplyThenComplete`
(~299) fires the broker completion only after the event-log append is durable
(keratin group-commit fsync) and applied.

Current chain: ack frame -> settle coalesce (64/500us) -> `engine.ack_batch`
-> event-log append -> fsync -> apply in-memory -> broker completion ->
`dec_inflight` + `qs.wake()`.

Problem: a prefetch-bound consumer cannot receive its next message until its
previous ack is on disk. Per-consumer throughput is capped at roughly
prefetch / (coalesce + group-commit latency), and ack fsync latency lands
directly on the delivery cycle.

Recommended: release the credit at settle-accept time — after the engine call
enqueues the append (offsets validated, batch grouped), not in the completion.
Move `dec_inflight` (as one `fetch_sub(count)`) and `qs.wake()` to right after
the `ack_batch`/`nack_batch` call returns. Keep in the completion: the
`pending_settles` decrement (drain-wait must track real completion), metrics,
and the replication wake (followers need durable progress, not intent).

Why it is safe: an un-fsynced ack lost in a crash means the message is
redelivered — exactly the at-least-once contract. No double delivery from the
early wake either: the acked offset stays marked inflight in stroma until the
ack event applies, so `poll_ready_and_mark` cannot re-lease it; the freed
credit only lets other ready messages flow. Note the current code does not
release credit at all on a failed append (the `else` branch only wakes), so the
consumer leaks a credit slot on failure today — early release also fixes that.

Impact estimate: the largest structural latency lever found. Biggest effect at
low prefetch (credit-bound consumers); at prefetch 16k the bench may show
little — measure with a low-prefetch case too (PREFETCH=8/64 run of the
confirmed scenario) to demonstrate the mechanism.

### A2. Publish confirm path pays for a channel nobody reads — OPEN

Where: `crates/broker/src/broker.rs` confirm_sink_loop (~2180-2222), the
`confirm_tx.send(offset)` at ~2186; `ConfirmStream` returned by
`get_publisher`; `crates/protocol/src/v1/handler.rs` ~3574-3585 spawns a task
per cached publisher that receives those offsets and discards them (the old
"confirms are handled elsewhere" TODO).

Problem: one bounded-channel send + recv + task wakeup per confirmed publish,
for nothing. Worse than waste: the channel is bounded, and the send happens in
`confirm_sink_loop` BEFORE the producer reply — if the drain task ever stalls,
producer confirms stall behind it. A latent liveness hazard plus steady
overhead.

Recommended: delete the offset send, the `ConfirmStream` return from
`get_publisher`, and the handler drain task. Check every `get_publisher`
caller (handler Publish and PublishDelayed arms, tests, benches).

Also in this loop: `confirm_gate.await_confirm(key, offset)` is awaited
serially per message (~2194). Offsets are monotonic per partition, so when a
batch flushes, waiting once on the batch's max offset would collapse N gate
waits into 1. Worth doing while in the file, but measure separately if easy —
the gate only engages in replica-durable mode.

Impact estimate: modest steady-state CPU/wakeup reduction on every confirmed
publish (one channel hop of three removed); removes a real stall hazard. The
serial-gate batching matters only for replica-durable clusters.

### A3. Per-connection writer ticks every 2ms even when idle — OPEN

Where: `crates/protocol/src/v1/handler.rs` writer task, `ticker` at ~2162,
select arm at ~2224.

Problem: the interval only exists to flush a sub-5ms tail of unflushed frames,
but it fires forever, so every idle connection costs ~500 wakeups/s.

Recommended: gate the arm with `, if non_flushed_messages > 0`. After an idle
stretch the first enabled tick fires immediately (missed-tick burst), which
just runs the flush check — the correct behavior.

Why safe: when `non_flushed_messages == 0` there is nothing the tick could
flush; all other paths (32-frame high-prio threshold, 128/1MB/5ms post-frame
check) are driven by actual frames.

Impact estimate: no throughput change expected; a many-idle-connections
CPU/wakeup win. Regression check: protocol tests + TS example suite + a bench
smoke to confirm flush latency is unchanged.

## B. Work done N times that could be done once

### B1. Delivery pump clones owned data per message — OPEN

Where: `crates/protocol/src/v1/handler.rs` ~1174-1191.

`msg` is owned and dropped at the end of each loop iteration, yet the code
clones `payload` (full memcpy per delivery), the `headers` map, `topic`, and
`group` twice (`msg.group` duplicates `msg.message.group`). Move the fields
into the `Deliver` instead. Impact: removes one full payload copy plus several
allocations per delivered message — likely the best cheap win for 1KB+
payload delivery throughput.

### B2. Ack frame handling locks twice and sends per tag — OPEN

Where: `crates/protocol/src/v1/handler.rs` Op::Ack arm ~3351-3402 (Nack arm
mirrors it).

Per ack frame: `logical.state.lock().await` is taken twice (stream check, then
queue-sub lookup), and a batch ack of N tags does N x `fetch_add(1)` + N x
`settler.send().await`. Recommended: one lock acquisition resolving both, then
`fetch_add(n)` once and one batched send (extend `SettleRequest` to carry the
tag batch, or `Vec<SettleRequest>` per send — the settle loop already consumes
batches). Impact: removes an async-mutex acquisition and N-1 channel ops per
batch ack; matters for clients that ack in windows.

### B3. Reverse tag map maintained hot, read cold — OPEN

Where: `crates/broker/src/broker.rs` `tags_by_key_offset` (1389), insert per
delivery (~3646), remove per settle (~3333); readers are only
`take_delivery_offsets_for_queue` (owner teardown ~2261), `unsubscribe`
(~3193), and the 15s expiry worker (~3891).

Recommended: drop the map; on the cold paths scan `records_by_tags` (it holds
key + offset + consumer per tag, so the reverse lookup is a filter). Impact:
one sharded-map insert + remove (with key clones) removed per message
lifecycle, paid for by O(inflight) scans on rare paths.

### B4. Delivery loop per-message overhead — OPEN

Where: `crates/broker/src/broker.rs` ~3612-3691.

- `qs.wake()` after every delivered message (3683) self-notifies the loop that
  is already running; one wake after the send loop (or none — the inner loop
  re-polls anyway) suffices. Also `wake()` on the no-capacity break (3630) is
  redundant for the same reason: the settle path wakes on credit return.
- `metrics.delivered()` per message (3677) while `redelivered_many` right below
  is already batched — use `delivered_many(delivered)` after the loop.
- `qs.activity.touch()` per message (3680) — clock read + atomic store; once
  per poll batch is enough for idle-tracking granularity.
- `next_tag_epoch.fetch_add(SeqCst)` (3634) — a unique-id counter needs only
  `Relaxed`.

Impact: shaves several atomics + a clock read per message; shows up at the
throughput-1k knee, not at low rate.

### B5. Publish arm checks streams before the publisher cache — OPEN

Where: `crates/protocol/src/v1/handler.rs` ~3489-3548; `is_stream`
(`crates/broker/src/broker.rs:1750`) allocates a `String` per call to build its
lookup key.

Every queue publish pays `is_stream` + `stream_declared_in_coordination`
before the local `publishers` HashMap is consulted. A topic that has a cached
publisher is definitionally a queue — check the cache first, and make
`is_stream` non-allocating (borrowed-key lookup or a keyed-by-parts map).
The cache key tuple also clones topic + group per frame even on hits; a
raw-entry/borrowed lookup avoids it. Impact: turns the steady-state publish
dispatch into one local map hit; removes 2-3 allocations + 2 shared-map reads
per publish.

### B6. `send_to_current_transport` re-borrows per frame — OPEN

Where: `crates/protocol/src/v1/handler.rs` ~855-870.

Per delivered frame it takes the watch borrow and clones the `FrameSink`
(two refcount ops + a lock window). Cache the sink across sends; refresh only
on send failure or `changed()`. Impact: micro, but free.

## C. Larger design candidates (measure before committing to)

### C1. Micro-batch the broker -> pump delivery hop — OPEN

Delivery sends one `DeliverableMessage` per `mpsc` send per message
(broker.rs ~3672); batching a `Vec` per consumer per poll batch amortizes the
channel and task wakeups (same shape as the stream cursor microbatch, #83).
This is the optimization log's "delivery loop allocation and scanning"
candidate. Needs care with fairness and the per-consumer credit checks.

### C2. Payload copy count end to end — OPEN

A payload is copied ~3x on delivery: stroma read -> `StoredMessage(Vec<u8>)`
-> `Deliver` (B1 removes this one) -> msgpack encode into the frame -> framed
write buffer. `Bytes`/`Arc<[u8]>` end-to-end or out-of-band payload framing
would cut more, but that is a protocol/type change — pair it with the
optimization log's "protocol encode and header layout" experiment and the
existing encode microbench.

## Positive findings (already the right shape — leave alone)

- Settle loop batches with a 64/500us coalesce window anchored correctly
  (broker.rs ~3256-3300).
- Publisher sink batches 256/250us with the window anchored to the previous
  flush, so a lone message after quiet never waits (broker.rs ~2056-2105).
- Connection writer batches flushes (32 high-prio frames, or 128 frames / 1MB
  / 5ms) and flushes immediately when sparse (handler.rs ~2158-2247).
- `poll_ready` fetches messages in contiguous-range batches off the async path
  (stroma.rs ~4718+), and delivery leases are marked in-memory only — no fsync
  on the delivery path.
- Keratin appends group-commit via the linger batcher, so ack/publish fsyncs
  amortize under load.
- Broker config reads are `ArcSwap::load_full` snapshots, once per poll batch.
- Publisher handles are cached per connection keyed by (topic, partition,
  group) — no shared-lock lookup per publish after the first.

## Execution order (agreed with the user, 2026-07-03)

1. A1 and A2 with before/after benchmarks each (baseline + confirmed
   scenarios; add a low-prefetch confirmed run for A1's mechanism).
2. A3 as a free win + regression tests (protocol tests, TS examples, bench
   smoke).
3. B items in safe groups, benching per group: group 1 = B1 + B4 (delivery
   path), group 2 = B2 (+ whatever of the dec_inflight loop A1 left), group 3 =
   B3 + B5 + B6 (maps + publish arm).
4. C1/C2 only with dedicated benchmarks, separately.

# Hot-Path Performance Audit (2026-07)

Scope: the per-message paths (publish to append to confirm, poll to deliver to
wire, and ack to settle to credit release) across fibril (broker, protocol
handler) and the stroma/keratin substrate. The lens is structural cost, not
just micro-optimization: blocking that could happen async, work done N times
that could be done once, and per-message overhead that could batch.

Method note: every change below is a hypothesis until measured. Use
`scripts/bench-matrix.sh` per the optimization-log discipline (baseline plus
confirmed for settle and confirm changes, throughput-1k for delivery-loop
changes), median of at least 3 short runs, same machine and settings before
and after.

Status legend per finding: OPEN, DONE (with commit), or DROPPED (with reason).

## A. Blocking that could happen async

### A1. Ack durability gates consumer flow-control credit. DONE (df89e33)

Where: `crates/broker/src/broker.rs` `handle_settle_batch` (~3376-3465) with
the completion built at 3386-3406. In `stroma/core/src/stroma.rs`,
`ApplyThenComplete` (~299) fires the broker completion only after the
event-log append is durable (keratin group-commit fsync) and applied.

Old chain: ack frame, settle coalesce (64/500us), `engine.ack_batch`,
event-log append, fsync, apply in-memory, broker completion, and only then
`dec_inflight` plus `qs.wake()`.

Problem: a prefetch-bound consumer could not receive its next message until
its previous ack was on disk. Per-consumer throughput was capped at roughly
prefetch / (coalesce + group-commit latency), and ack fsync latency landed
directly on the delivery cycle.

Change: release the credit at settle-accept time, after the settle batch is
validated and grouped, not in the completion. `dec_inflight` became one
`fetch_sub(count)` and the queue wake happens right away. The completion keeps
the `pending_settles` decrement (the drain-wait must track real completion),
metrics, and the replication wake (followers need durable progress, not
intent).

Why it is safe: an un-fsynced ack lost in a crash means the message is
redelivered, exactly the at-least-once contract. No double delivery from the
early wake either, because the acked offset stays marked inflight in stroma
until the ack event applies, so `poll_ready_and_mark` cannot re-lease it. The
freed credit only lets other ready messages flow. The old code also never
released credit on a failed append, so a consumer leaked a credit slot on
failure. Early release fixes that too.

Impact estimate: the largest structural latency lever found. Biggest effect at
low prefetch (credit-bound consumers). At prefetch 16k the standard bench may
show little, so the measurement includes a PREFETCH=64 confirmed run to
demonstrate the mechanism.

### A2. Publish confirm path pays for a channel nobody reads. DONE (86fdbe7)

Where: `crates/broker/src/broker.rs` confirm_sink_loop (~2180-2222) with the
`confirm_tx.send(offset)` at ~2186, the `ConfirmStream` returned by
`get_publisher`, and the task in `crates/protocol/src/v1/handler.rs`
(~3574-3585) that received those offsets and discarded them.

Problem: one bounded-channel send, recv, and task wakeup per confirmed
publish, for nothing. Worse than waste: the channel was bounded and the send
happened in `confirm_sink_loop` before the producer reply, so a stalled drain
task would have stalled producer confirms behind it. A latent liveness hazard
plus steady overhead.

Change: deleted the offset send, the `ConfirmStream` type, and the drain
tasks. `get_publisher` returns just the `PublisherHandle`. Confirm delivery
stays on the per-publish oneshot.

Measured (3x baseline plus confirmed at 50k/150k, 1KB): latency-neutral, all
percentiles within run-to-run noise. Landed as overhead and hazard removal per
the acceptance rules, not claimed as a latency win.

Still open from this finding: `confirm_gate.await_confirm` is awaited serially
per message (~2194). Offsets are monotonic per partition, so waiting once on
the batch's max offset would collapse N gate waits into 1. Only engages in
replica-durable mode.

### A3. Per-connection writer ticks every 2ms even when idle. OPEN

Where: `crates/protocol/src/v1/handler.rs` writer task, `ticker` at ~2162,
select arm at ~2224.

Problem: the interval only exists to flush a sub-5ms tail of unflushed frames,
but it fires forever, so every idle connection costs ~500 wakeups per second.

Recommended: gate the arm with `if non_flushed_messages > 0`. After an idle
stretch the first enabled tick fires immediately (missed-tick burst), which
just runs the flush check, the correct behavior.

Why safe: when `non_flushed_messages == 0` there is nothing the tick could
flush. All other paths (32-frame high-prio threshold, the 128-frame, 1MB, 5ms
post-frame check) are driven by actual frames.

Impact estimate: no throughput change expected, a many-idle-connections CPU
and wakeup win. Regression check: protocol tests, TS example suite, and a
bench smoke to confirm flush latency is unchanged.

## B. Work done N times that could be done once

### B1. Delivery pump clones owned data per message. OPEN

Where: `crates/protocol/src/v1/handler.rs` ~1174-1191.

`msg` is owned and dropped at the end of each loop iteration, yet the code
clones `payload` (a full memcpy per delivery), the `headers` map, `topic`, and
`group` twice (`msg.group` duplicates `msg.message.group`). Move the fields
into the `Deliver` instead. Impact: removes one full payload copy plus several
allocations per delivered message, likely the best cheap win for 1KB-plus
payload delivery throughput.

### B2. Ack frame handling locks twice and sends per tag. OPEN

Where: `crates/protocol/src/v1/handler.rs` Op::Ack arm ~3351-3402 (the Nack
arm mirrors it).

Per ack frame the per-connection async mutex is taken twice (stream check,
then queue-sub lookup), and a batch ack of N tags does N times `fetch_add(1)`
plus N times `settler.send().await`. Recommended: one lock acquisition
resolving both, then `fetch_add(n)` once and one batched send (extend
`SettleRequest` to carry the tag batch, or send a `Vec<SettleRequest>`, since
the settle loop already consumes batches). Impact: removes an async-mutex
acquisition and N-1 channel ops per batch ack, which matters for clients that
ack in windows.

### B3. Reverse tag map maintained hot, read cold. OPEN

Where: `crates/broker/src/broker.rs` `tags_by_key_offset` (1389), with an
insert per delivery (~3646) and a remove per settle (~3333). Its only readers
are `take_delivery_offsets_for_queue` (owner teardown ~2261), `unsubscribe`
(~3193), and the 15s expiry worker (~3891).

Recommended: drop the map and scan `records_by_tags` on those cold paths (it
holds key, offset, and consumer per tag, so the reverse lookup is a filter).
Impact: one sharded-map insert plus remove (with key clones) removed per
message lifecycle, paid for by O(inflight) scans on rare paths.

### B4. Delivery loop per-message overhead. OPEN

Where: `crates/broker/src/broker.rs` ~3612-3691.

- `qs.wake()` after every delivered message (3683) self-notifies the loop that
  is already running. One wake after the send loop suffices, and even that is
  arguably redundant since the inner loop re-polls anyway. The `wake()` on the
  no-capacity break (3630) is redundant the same way, because the settle path
  wakes on credit return.
- `metrics.delivered()` per message (3677) while `redelivered_many` right
  below is already batched. Use `delivered_many(delivered)` after the loop.
- `qs.activity.touch()` per message (3680) costs a clock read plus an atomic
  store. Once per poll batch is enough for idle-tracking granularity.
- `next_tag_epoch.fetch_add(SeqCst)` (3634) is a unique-id counter and needs
  only `Relaxed`.

Impact: shaves several atomics plus a clock read per message. Shows up at the
throughput-1k knee, not at low rate.

### B5. Publish arm checks streams before the publisher cache. OPEN

Where: `crates/protocol/src/v1/handler.rs` ~3489-3548, and `is_stream`
(`crates/broker/src/broker.rs:1750`) which allocates a `String` per call to
build its lookup key.

Every queue publish pays `is_stream` plus `stream_declared_in_coordination`
before the local `publishers` HashMap is consulted. A topic that has a cached
publisher is definitionally a queue, so check the cache first, and make
`is_stream` non-allocating (borrowed-key lookup or a map keyed by parts). The
cache key tuple also clones topic and group per frame even on hits, which a
raw-entry or borrowed lookup avoids. Impact: turns the steady-state publish
dispatch into one local map hit and removes 2-3 allocations plus 2 shared-map
reads per publish.

### B6. `send_to_current_transport` re-borrows per frame. OPEN

Where: `crates/protocol/src/v1/handler.rs` ~855-870.

Per delivered frame it takes the watch borrow and clones the `FrameSink` (two
refcount ops plus a lock window). Cache the sink across sends and refresh only
on send failure or `changed()`. Impact: micro, but free.

## C. Larger design candidates (measure before committing to)

### C1. Micro-batch the broker to pump delivery hop. OPEN

Delivery sends one `DeliverableMessage` per `mpsc` send per message
(broker.rs ~3672). Batching a `Vec` per consumer per poll batch amortizes the
channel and task wakeups (same shape as the stream cursor microbatch, #83).
This is the optimization log's "delivery loop allocation and scanning"
candidate. Needs care with fairness and the per-consumer credit checks.

### C2. Payload copy count end to end. OPEN

A payload is copied about 3 times on delivery: the stroma read into
`StoredMessage(Vec<u8>)`, the clone into `Deliver` (B1 removes this one), the
msgpack encode into the frame, and the framed write buffer. `Bytes` or
`Arc<[u8]>` end to end, or out-of-band payload framing, would cut more, but
that is a protocol and type change. Pair it with the optimization log's
"protocol encode and header layout" experiment and the existing encode
microbench.

## Positive findings (already the right shape, leave alone)

- Settle loop batches with a 64-request, 500us coalesce window anchored
  correctly (broker.rs ~3256-3300).
- Publisher sink batches 256/250us with the window anchored to the previous
  flush, so a lone message after quiet never waits (broker.rs ~2056-2105).
- Connection writer batches flushes (32 high-prio frames, or 128 frames, 1MB,
  or 5ms) and flushes immediately when sparse (handler.rs ~2158-2247).
- `poll_ready` fetches messages in contiguous-range batches off the async path
  (stroma.rs ~4718 and on), and delivery leases are marked in-memory only, so
  there is no fsync on the delivery path.
- Keratin appends group-commit via the linger batcher, so ack and publish
  fsyncs amortize under load.
- Broker config reads are `ArcSwap::load_full` snapshots, once per poll batch.
- Publisher handles are cached per connection keyed by (topic, partition,
  group), so there is no shared-lock lookup per publish after the first.

## Execution order (agreed with the user, 2026-07-03)

1. A1 and A2 with before and after benchmarks each (baseline plus confirmed
   scenarios, and a low-prefetch confirmed run for A1's mechanism).
2. A3 as a free win plus regression tests (protocol tests, TS examples, bench
   smoke).
3. B items in safe groups, benching per group: group 1 is B1 plus B4 (delivery
   path), group 2 is B2 (plus whatever of the dec_inflight loop A1 left),
   group 3 is B3, B5, and B6 (maps plus the publish arm).
4. C1 and C2 only with dedicated benchmarks, separately.
5. Knee check: run throughput-1k (250k to 500k) before vs after the batch to
   see whether the latency knee moved (user request, 2026-07-03).

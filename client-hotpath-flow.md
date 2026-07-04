# Client hot-path flow (publish + delivery)

Working analysis for the single-client throughput audit. Traces the happy path a
message takes through the Rust client, marks every task boundary / channel /
await, and calls out the blocking or serializing spots. Line references are into
`crates/client/src/lib.rs` unless noted.

## Tasks and channels

The client is three kinds of moving part:

- **User task(s)** - whoever calls `publisher.publish(...)` or `sub.recv()`.
- **Engine task** - one per connection, spawned by `Client::connect`. Owns the
  socket via `Framed<Stream, ProtoCodec>` and runs one `tokio::select!` loop
  (around :3888) over: heartbeat tick, flush tick, shutdown, the outbound
  command channel (`cmd_rx`), and inbound frames (`framed.next()`).
- **Per-delivery ack tasks** - manual-ack only, one `tokio::spawn` per delivered
  message (see Path B).

Channels:

- `cmd_tx -> cmd_rx`: user -> engine, `mpsc::channel::<Command>(8192)` (:3784).
- per-publish confirm: `oneshot`, engine -> user, one per confirmed publish.
- per-subscription delivery: engine -> consumer, `mpsc::channel(prefetch * subs)`
  (:1490 etc.), so ~16k deep at the bench's prefetch.
- per-delivery settle: `oneshot`, consumer -> ack task.

The engine is a SINGLE task handling BOTH directions. Everything on one
connection is serialized through it.

## Path A - publish (happy path)

Unconfirmed (`publish`) and confirmed (`publish_confirmed` /
`publish_with_confirmation`) share the outbound half.

```
user task                         engine task (select loop)              socket
---------                         -------------------------              ------
publish(msg)
  into_message()      ~22ns
  route()             ~83ns
  engine_for().await  ~156ns  (topology load + 2 awaits, RwLock+Arc)
  publish_unconfirmed/confirmed:
    build Command
    cmd_tx.send().await  --[cmd channel 8192]-->  recv (drained in a batch)
                                                   [confirmed] waiters.insert(req_id, oneshot)
                                                   wire::encode_publish  (alloc frame + copy payload)
                                                   framed.feed().await   (copy into write buffer)
                                                   flush at 128 frames / 1MB / 250us --> write() --> broker
```

Confirm return (confirmed only):

```
broker --> PublishOk frame --> framed.next() (engine)
                                 wire::decode_publish_ok
                                 waiters.remove(req_id)
                                 oneshot.send(Ok(offset))   (non-blocking)  --[oneshot]--> user's confirm future resolves
```

### Blocking / serializing spots on Path A

1. **cmd_tx.send().await backpressure.** When the engine cannot keep the 8192
   channel drained, the writer blocks here (measured ~1561ns under saturation).
   The channel is large, so this is NOT a capacity problem - it means the
   engine is the slower stage.
2. **One engine task, both directions.** For confirmed publish every message is
   two frames through the same task (outbound Publish + inbound PublishOk) plus
   a `waiters` HashMap insert/remove and a oneshot. The engine is the
   serialization point.
3. **[FIXED] tokio-util Framed 8KB flush boundary.** `Framed::new` defaults
   `backpressure_boundary` to `INITIAL_CAPACITY = 8KB`, so `feed()` forced a
   socket write every ~8 frames (1KB), defeating the intended 128-frame batch.
   Raised to `PUBLISH_FLUSH_BYTES` on this branch (~+14% alone).
4. **Per-frame copies in encode + feed.** `encode_publish` allocates a frame
   buffer and copies the payload (`put_bytes`); `feed` copies it again into the
   Framed write buffer. Two copies of the payload per message on the engine, the
   CPU-bound stage.

### Measured (single client, 1KB, saturated, this branch)

- Rate ~179k/s unconfirmed. CLIENT is CPU-bound at ~1 core (~104%); BROKER at
  ~57% with headroom. So the ceiling is client-side, and specifically the
  engine: writer + engine total ~1 core means they SERIALIZE (a true parallel
  pipeline would show ~200%), with the writer blocked on send while the engine
  is the ~1-core bottleneck doing encode + feed.
- Confirm latency dominated by the in-flight window (Little's law: window/rate),
  not by batching. Batch-drain did not change it.

## Path B - delivery (happy path)

```
broker --> Deliver frame --> framed.next() (engine)
                               wire::decode_deliver
                               subs.get(sub_id)
                               [auto] tx.send(Message).await   --[sub channel ~16k]--> sub.recv() (consumer)
                               [manual] tx.send(InflightMessage{settle: oneshot}).await --> consumer
                                        tokio::spawn(ack task) per message
```

Manual ack settle:

```
consumer: msg.ack()  --[settle oneshot]--> ack task
                                             cmd_tx.send(Command::Ack).await  --[cmd channel]--> engine --> encode --> socket --> broker
```

### Blocking / serializing spots on Path B

1. **tx.send(msg).await on the engine is a LATENT head-of-line risk, not a
   steady-state stall** (:4145 manual, :4207 auto). The engine awaits pushing
   each delivered message into the per-subscription channel, so IF that channel
   fills it stalls the entire engine (every other subscription, all confirms,
   all outbound publishes) and TCP-backpressures the whole connection. BUT the
   channel is sized `prefetch * partitions` and the broker gates delivery on
   `inflight < prefetch` (broker.rs:576), so it only ever has the credit window
   in flight - the channel always has room and the send does NOT block in the
   happy path. The real per-subscription backpressure is the broker credit
   window, not this channel filling. So this is a robustness edge (fires only if
   credit exceeds channel capacity, a burst outruns sizing, or a bug), worth a
   `try_send` that fails fast on a genuine violation rather than silently
   stalling - but it is NOT a throughput bottleneck.
2. **tokio::spawn per delivered message (manual ack).** One task allocation +
   schedule per message just to await the settle oneshot and forward an Ack
   command. Per-message task churn on the delivery hot path.
3. **Ack round-trips the cmd channel** like any publish, through the same single
   engine task (shares the Path A serialization).

## Candidate optimizations (ranked, not yet done)

- **Split the engine into read-half and write-half tasks (the ~180k lever).**
  One task does read (`framed.next` + dispatch) AND write (encode + feed +
  flush), so the socket write does NOT interleave with the rest and the client
  runs at ~1 core (writer blocked, engine the bottleneck). Splitting outbound
  (drain cmd_rx -> encode -> write) from inbound (read -> dispatch) into two
  tasks lets writing overlap reading and uses a second core - the structural
  path toward 200-300k. Shared state (`waiters`, `subs`) has to be divided or
  shared cheaply; this is the biggest change but the real throughput win.
- **Copies / memory.** `Frame.payload` is ALREADY `Bytes` (cheap clone), so the
  frame itself is not re-copied. Two payload copies remain: `put_bytes` in
  `encode_publish` (payload -> frame-body buffer) and `dst.extend_from_slice` in
  `ProtoCodec::encode` (frame body -> Framed write buffer, frame.rs:69).
  Eliminating them fully needs vectored writes (write header + payload `Bytes`
  separately instead of concatenating), which means bypassing the `Framed`
  Encoder - INTRUSIVE for only ~4% CPU, but it also lowers transient memory, so
  worth it only if the write-half rework touches this path anyway. Memory note:
  the raised backpressure boundary lets the write buffer reach ~128KB per active
  connection (the code flushes at 128 frames; the 1MB is a cap rarely hit) vs
  the old 8KB - a deliberate throughput/memory trade, modest per connection but
  it scales with connection count.
- **Non-blocking delivery (Path B) is robustness, not throughput.** The sub
  channel is sized to the broker credit window (prefetch * partitions) and the
  broker gates on inflight < prefetch, so it does not fill in the happy path.
  `try_send` (fail-fast on the impossible overflow) removes the latent
  head-of-line risk at zero happy-path cost. Prefetch is fixed at subscribe, so
  the bound is static - no dynamic resize.
- **Drop the per-delivery spawn (Path B).** Handle settle inline or via one
  long-lived ack-forwarder task per subscription instead of a spawn per message
  (memory + per-message task churn).
- **Increasing the cmd channel (8192) will NOT help** the publish ceiling - the
  engine is CPU-bound, not channel-capacity-bound.

## Correctness notes (batch-drain)

- Ordering: `try_recv` drains FIFO, so batching preserves command order.
- Failure scope: every exit in the drained command arm is a `_or_die!` macro
  that sets `fatal_error` and breaks; those are CONNECTION-level failures (encode
  or socket-write errors). One such error fails the whole connection whether
  commands are processed one-per-select or in a batch, so batching does not fail
  MORE than the unbatched path. TODO: a test that pins this (fatal mid-batch
  fails cleanly, remaining commands surface a connection error, no hang).

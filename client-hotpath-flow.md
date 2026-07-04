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

## Engine read/write split - concrete plan

Goal: writing interleaves with reading (two tasks, two cores) so a single
client moves from ~180k toward 200-300k. Every step is proved on the bench
before the next; the branch merges only when the whole matrix is green.

Verified facts the plan builds on:
- `MaybeTlsStream` is `Plain(TcpStream) | Tls(Box<TlsStream<TcpStream>>)`.
  Plain splits zero-cost via `TcpStream::into_split()`. TLS needs
  `tokio::io::split()` (BiLock per poll - rustls shares one state machine, so
  TLS keeps some read/write coupling; measure plain first, accept a smaller TLS
  win).
- `FramedWrite::set_backpressure_boundary` exists (framed_write.rs:156), so the
  raised-boundary fix carries over.
- `dashmap` is already a client dependency (waiter-map option).

### Step 0 - bench prerequisite (do first, independently useful)

Add an unthrottled mode to steady_c (`--rate-per-sec 0` = no interval tick, a
plain loop), so saturation runs stop measuring the rate limiter. Also add a
`mixed` single-client scenario (1 producer + 1 consumer, same client) - the
split's interleaving claim is strongest there. Record the pre-split baselines
with it: publish-only, consume-only, mixed, each 3 runs.

### Step 1 - split the stream and the codec halves

`MaybeTlsStream::into_halves() -> (ReadHalf, WriteHalf)` (enum halves wrapping
`OwnedReadHalf`/`OwnedWriteHalf` for plain, `tokio::io::ReadHalf/WriteHalf` for
TLS). Replace `Framed<S, ProtoCodec>` with `FramedRead<ReadHalf, ProtoCodec>` +
`FramedWrite<WriteHalf, ProtoCodec>` (same codec; decode state lives only in
FramedRead, encode buffer only in FramedWrite - they were already disjoint).
Set the backpressure boundary on the FramedWrite. No task split yet: keep one
loop selecting over both halves. Prove: bench unchanged (this step must be
neutral) and the full client test suite green.

### Step 2 - two tasks, ownership divided

- WRITE task owns: `FramedWrite`, `cmd_rx` (with the proven batch-drain),
  `next_req` allocation, encode + feed + the flush ticker/thresholds, the
  heartbeat PING ticker, and the heartbeat-timeout check.
- READ task owns: `FramedRead`, `subs` (installed on SubscribeOk, removed on
  drop), delivery dispatch, reply dispatch, topology/assignment pushes, and
  updates `last_seen`.
- Shared state, minimal by design:
  - `waiters`: DashMap<u64, Waiter> (write task inserts before feeding a
    confirmed publish, read task removes on reply). Sharded map because two
    tasks now touch it ~150k/s each; a plain Mutex<HashMap> is the fallback if
    DashMap shows anything odd - measure both if in doubt.
    Insert-before-feed preserves the no-lost-reply invariant: the reply cannot
    arrive before the frame is written.
  - `last_seen`: Arc<AtomicU64> (millis relative to engine start; read task
    stores, write task compares on its heartbeat tick).
  - a `CancellationToken` (or Notify pair): whichever task hits a fatal error
    records it (OnceLock<FibrilError>), cancels the token; both tasks exit on
    cancellation.
- A small supervisor (the code that today spawns the engine) awaits both task
  handles, then runs the existing teardown exactly once: fail all waiters
  (drain the shared map), clear subs via the registry, notify shutdown. The
  reconnect/resume path calls the same spawn, so it inherits the split
  unchanged.
Prove: full test suite + tls_client + auth_client + redirect tests green,
then bench - expect client CPU > 1 core and publish-only saturate to move.
This step is the win-or-shelve gate: if mixed/publish saturate does not clear
~220k on this hardware, stop and reassess before piling on.

### Step 3 - settle path simplification (rides the split)

Manual-ack settle today: consumer -> oneshot -> per-delivery spawned task ->
cmd channel. Give `InflightMessage` a `cmd_tx` clone and have
`complete()/fail()/retry()` send `Command::Ack/Nack` directly - deletes the
per-delivery `tokio::spawn` and the oneshot (memory + churn win). The engine
shutdown must still fail in-hand settles: senders observe the closed channel
(BrokenPipe), same surface as today. Prove: settle tests + soak, bench
consume-drain (expect the consumer side to gain most here).

### Step 4 - delivery hardening (robustness, zero happy-path cost)

Replace the read task's `tx.send(msg).await` into the sub channel with
`try_send`. The channel is sized to the broker credit window
(prefetch x partitions, fixed at subscribe) so Full is a credit violation:
treat it as a fatal protocol error naming the invariant (never silent drop -
at-least-once redelivers after inflight TTL, but the error must be loud).
Prove: delivery tests + a new test that overfills a shrunken channel and
asserts the loud failure, not a hang.

### Step 5 - optional, only if step 2 pays: vectored write

Bypass the Encoder copy for large payloads: write header and payload `Bytes`
separately (write_vectored on the write half) instead of extend_from_slice
into the write buffer. Saves the second payload copy (~4% CPU at 1KB, more at
larger payloads) and transient memory. Intrusive - only do it inside the
already-reworked write task, and only if the numbers justify it.

### Step 6 - the full matrix before any merge decision

- Throughput: publish-only unconfirmed + confirmed(w=1024, 4096), consume-only,
  mixed, at 1 client and 10 clients, 1KB (plus one 16KB spot-check).
- Latency guardrail: confirmed publish p50/p99 at a FIXED offered rate (50k)
  must be within noise of main - the split must not add latency at moderate
  load.
- CPU split sanity: writer/read/write tasks together should exceed 1 core
  under saturation, broker headroom unchanged.
- Correctness: fatal-mid-batch (kill the socket during a burst: every pending
  confirm fails with Disconnection, no hang), publish order preserved within a
  partition under the batch-drain, heartbeat timeout still fires (server
  accepts then goes silent), reconnect + resume identity survives the split,
  clean shutdown mid-traffic. Existing suites: client unit + integration,
  handler_tests, error_guides, TS/Python untouched but rerun for safety.
- Memory: RSS per idle connection and under load vs main (the write buffer may
  hold up to PUBLISH_FLUSH_BYTES transiently; confirm steady-state is not
  higher than main by more than that).

### Follow-on (after Rust lands): the other clients

- Measure TS and Python single-client ceilings with the same
  saturate/mixed/latency matrix (their benches may need the same no-tick fix).
- Check each for the same two flaws found here: a per-few-frames forced socket
  flush (TS: how socket.write chunks; Python: per-message drain() awaits), and
  a serialized read/write structure (both are single-threaded runtimes - the
  interleaving story differs, but a Python writer that awaits drain() per
  message is the same bug shape as the 8KB boundary).
- Port what applies; wording of guides stays per error_guides.json, numbers
  land next to the FEATURE_MATRIX per-client perf bar.

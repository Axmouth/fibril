# Pipeline audit: work inside serialized contexts (DRAFT, 2026-07-18)

Goal: per-partition ordering serializes the ORDER, not the work. This audit
walks the hot paths and flags work that runs inside a serialized context
(the queue actor, a single loop, a shared lock) without needing to, so the
stages of one partition's flow can interleave across successive messages.
Feeds the throughput/latency assessment in FOLLOWUPS (the million target).

STATUS: queue-actor section complete. Delivery-path and publish-confirm
sections pending (exploration in flight when this draft was written).

## Queue actor (stroma/core/src/state.rs) - findings

The actor loop (one task per partition) processes ONE command per
`recv().await` iteration; `process_command` is fully synchronous. Reads
(next_deliverable, inspections) travel the same channel as writes on
higher-priority lanes - they preempt at command boundaries but still
serialize behind in-progress work.

Ranked findings:

1. NO COMMAND BATCHING. One wakeup per command despite bounded 5-lane
   mpsc. A drain loop (recv one, then try_recv up to N more before
   yielding) would amortize wakeups and let the actor chew a rush in one
   scheduling quantum. Cheap, measurable, low risk.
2. SETTLE-PATH O(n log n): `recompute_hint_if_needed()` (state.rs ~3641)
   can pop stale heap heads repeatedly and, when drained, REBUILDS the
   whole expiry heap from the inflight map - inside the actor, and PER
   ELEMENT in AckMany/NackMany. Fix candidates: run once per batch (not
   per element), or an incremental structure. Likely the heaviest hidden
   cost on the ack path.
3. READS BEHIND WRITES: delivery's `next_deliverable` queues behind
   publishes in the same actor. If the deliver-path contention probe
   confirms starvation here, candidates: a read-optimized snapshot the
   delivery loop can consult without the actor (ready-range watermark
   published via atomics), or strict read-preemption inside a drain batch.
4. PER-COMMAND INSTRUMENTATION: two Instant::now + histogram observes +
   depth atomics on EVERY command (process ~1546/1967, wait ~836). The
   histogram/depth atomics are shared across producers - false-sharing
   candidate. Consider sampling (1-in-N) or per-lane padding.
5. Actor-side oneshot `r.send(())` acks on every hot command - not needed
   for actor progress. `*Many` result Vecs (NackMany ~3153) allocate on
   the actor thread.
6. Notify wakeups from inside the actor on TTL/delay paths (~3419/3459);
   the settle-path notifies already live handle-side.

Clean bill: no IO, no encoding, no locks inside the hot commands.

## Congestion visibility (user direction 2026-07-18)

Two layers, the second being the real ask:

1. Stroma actor lanes: per-lane cmd_queue_depth atomics + cmd_wait_latency
   histograms already exist (~837/890) - surface via exporter + a
   diagnostics panel if not already visible.
2. KERATIN PIPELINE STAGES (the ask): the writer -> fsync worker ->
   manifest/completion chain has bounded inter-stage buffers (the
   writer/fsync bounded channel was the publish-wedge deadlock site -
   keratin 3cb1218 added the hard cap + ensure_fsync_slot gate). Add
   per-stage saturation metrics: current depth vs capacity (y/N), the
   fraction of submissions made while the buffer was FULL ("batches while
   maxed x%"), and average fill. A downstream stage that blocks (fsync
   stalls, notify backlog) then shows as its buffer pinned at N with
   upstream stages' full-fraction climbing - the bottleneck reads
   directly off the graph instead of needing a gdb session. Export via
   the existing keratin stats plumbing (the IO stats line already tracks
   per-stage TIME - this adds per-stage QUEUE state) and feed /metrics.

## Delivery path (broker.rs spawn_delivery_loop ~3649 + handler.rs pump ~1270 + writer ~2460)

Already well-pipelined in shape: three stages - per-partition delivery
loop (lease + assign) -> per-subscription pump (frame encode) ->
per-connection writer (socket, coalesced flush) - with frame encoding
deliberately OFF the shared per-partition loop, batched channel handoffs,
and a drain-until-stall inner loop. No per-message Mutex on the hot path.

Findings:

D1. HEAD-OF-LINE BLOCKING ACROSS CONSUMERS: the per-partition loop awaits
    `c.tx.send(batch)` per consumer (broker.rs ~3888) into a capacity-4
    channel. One slow consumer with a full channel stalls the WHOLE
    partition's delivery to every other consumer. Candidate: try_send +
    skip (leave messages leased for redelivery or reassign within the
    poll), or size the channel by prefetch.
D2. STORAGE POLL COMPETES IN THE ACTOR (cross-path with the queue-actor
    findings): `poll_ready` (broker.rs ~3778) is one actor command that
    queues behind in-progress publish commands, one command per wakeup,
    no drain batching. Under a publish flood the delivery read starves at
    command granularity - the most concrete mechanism yet for the
    measured "publish drowns delivery". Pairs with actor finding 1/3.
D3. THREE PAYLOAD COPIES userspace: log -> owned Vec (poll_ready,
    queue_engine.rs ~920), Vec -> frame BytesMut (encode_deliver,
    wire.rs ~270), frame Bytes -> socket write buffer (ProtoCodec,
    frame.rs ~69). Moves elsewhere. Bytes-backed payloads end-to-end
    would drop one to two copies - real CPU/byte at high rates.
D4. Per-message shared touches in the loop: records_by_tags DashMap
    insert, tag-epoch fetch_add, consumer inflight atomics - all
    per-partition-scoped, acceptable; revisit only if the flamegraph
    names them.

## Publish-confirm path (broker.rs publisher_sink ~2226 + confirm_sink_loop ~2350 + replication.rs await_confirm ~1111)

The sink batches well (drain up to 256, 250us coalesce window only inside
a burst, one storage submit + one metrics bump per batch). The confirm
side is where per-message costs cluster.

Findings:

P1. BROKER-GLOBAL ATOMICS PER CONFIRMED MESSAGE:
    `replication_timing.replication_wakes.fetch_add` runs unconditionally
    per completed message (broker.rs ~2356), plus a 3-atomic timer trio
    when replication is required (replication.rs ~568-585). Cross-core
    contention / false sharing candidate at 1M/s across all partitions -
    the "shared counters" classic. Fix: per-core/per-partition shards or
    sampling.
P2. PER-MESSAGE WAKES AND LOOKUPS ON CONFIRM: `wake_with_replication()`
    (a Notify) per message, and confirm-gate global DashMap get (+ entry
    when replicated) per message (replication.rs ~1116/1137). Batchable:
    the confirm sink processes completions in order and could coalesce
    wakes/lookups per drained run.
P3. TWO ONESHOT ALLOCATIONS PER MESSAGE (append completion + client
    reply, broker.rs ~237/~2297). Amortizable with batch-completion
    structures if the flamegraph shows allocator pressure.
P4. PublishOk rides the LOW-priority frame lane behind ALL deliver/ping
    frames with no force-flush of its own (handler.rs ~2517-2534) -
    deliberate, but it inflates publisher-observed confirm latency under
    delivery load. Revisit when chasing latency SLOs.
P5. Confirm relay awaits completions strictly one-at-a-time
    (broker.rs ~2351) - order-correct and cheap while completions arrive
    in order, but every per-message cost in P1/P2 sits on this single
    serialized task per publisher.

## Cross-path ranking (what to try first, each behind a bench)

1. Actor drain-batching + D2 read starvation (one change unlocks both:
   drain N commands per wakeup with read-priority inside the batch).
2. P1 global-atomic sharding (cheap, classic, measurable with the
   contention probe flamegraph).
3. D1 slow-consumer HOL isolation.
4. Actor settle-path recompute_hint once-per-batch.
5. D3 payload copy reduction (bigger surgery, biggest CPU/byte win).

## Method note

Findings become individually benchable follow-ups - never change the hot
path blind. The contention probe (FOLLOWUPS) decides which finding gets
implemented first; the e2e_c latency decomposition measures each fix.

# Design Notes

Curated design decisions and their rationale, kept so the user-facing docs can
draw on them later. This is not a running log (that is REPLICATION_WORKLOG.md)
and not a point-in-time audit (those are the *_AUDIT.md files). Add an entry when
a decision has a "why" worth explaining to a reader, not just a "what".

## Cohorts and cross-broker coordination

### The gate is the correctness backstop, the global plan is advisory

Each partition has a delivery gate (`QueueLoopState.exclusive_assignee`, an
atomic) that admits at most one consumer. This is enforced locally on every
owner, always. The cross-broker assignment plan the controller computes and
publishes is therefore advisory and eventually-consistent: about one heartbeat
of lag affects balance only, never correctness. A departed member's partitions
pause until the next plan rather than ever being double-delivered. This split is
what lets the coordinator be simple (no global locks, no strict consensus on the
hot path).

### Cohort plan generation is per-cohort and durable

The published assignment document carries a `generation`. The controller bumps
it only when the assignment content actually changes, so re-publishing a stable
plan is a same-value no-op.

- PER-COHORT, not one shared counter. Each cohort document is its own
  authoritative version. A single cluster-wide counter would force re-stamping
  every cohort's document whenever any one cohort changed (write amplification)
  for no correctness gain. A single "cluster is at generation N" number, if ever
  wanted, is a thin max-over-documents view on top.
- DURABLE in the document, not an in-memory counter. The generation is read back
  from the committed attribute before each publish, so it stays monotonic across
  a controller leader change (a fresh leader does not reset it to zero).

### Owners fence stale plans by generation

An owner ignores any plan older than the one it already holds, so a late or
out-of-order slice never overwrites a newer one. An equal generation is still
re-resolved, because local subscriptions may have changed since the last apply
(a member finally subscribing to its assigned partition needs its gate set).
`exclusive_assignment_generation` exposes the applied version so convergence is
observable (an owner sitting on an older generation has not caught up yet).

### Leader changes are generation-stable (controller seeding)

A new leader's controller would otherwise start with empty sticky state, so its
first plan could differ from the prior one even with unchanged membership,
causing a needless rebalance on every leader change. The controller seeds itself
from each cohort's published plan before planning, so it keeps the existing
assignment. With the per-cohort generation, this makes a leader change
generation-stable: the reconstructed plan matches what is published, so the
generation does not bump and owners see no change.

### Member-id validation is scoped to the trusted-client model

Cohort member ids are validated with a local guard: the broker rejects a
malformed id and enforces one member identity per connection per broker
(reconnect-safe). This is sufficient under the current trust model, where broker
connections have optional auth (a post-auth client is a trusted principal), the
client uses one connection per broker with a single shared member id, and v4
uuids make accidental cross-client collision negligible.

Reconnect is already safe without a cross-connection takeover: a normal reconnect
resumes the session (the server keeps the LogicalConnection and its subscriptions
and just reattaches the transport), so no member re-registration happens and
nothing is clobbered. The reconcile path (server lost state, client re-pushes
subs) also cannot clobber, since the old registration is already gone. The only
residual race is a client abandoning its session and opening a fresh connection
that re-subscribes with the same member id while the old session's cleanup is
still pending. There the broker's whole-member `leave` could drop the new
connection's registration. Impact is bounded to a transient pause (the gate keeps
delivery correct, never double-delivers), so this is left as a follow-up: make
`leave` sub_id-scoped so a stale connection's cleanup only removes its own
registration. That reworks `leave`'s whole-member failover atomicity, so it is
its own brick.

REVISIT when the broker port is exposed to untrusted or multi-tenant clients. A
malicious authenticated tenant could otherwise spoof another tenant's member id
to hijack delivery, which the local guard does not prevent. The next steps then
are a cluster-issued signed token (HMAC over the id with a shared cluster secret,
verified locally on any broker, no per-subscribe round-trip) or full
coordinator-issued identity.

## Load-aware placement and partition routing

Two related signals should stay separate:

- Node load: "how good is this broker as an owner/follower candidate?"
- Partition load: "which partition of this queue should keyless traffic prefer?"

Node load is a cluster-placement hint. It belongs on the coordination heartbeat
path as a compact, advisory report, not as high-frequency durable state. The
controller should write durable assignment decisions, not every transient load
sample. A useful report carries both a score and the dimensions that explain it:
CPU/RSS pressure, total ready/inflight work, owned/followed partition counts,
connection counts, replication lag pressure, and an observed-at timestamp. A
single score is useful for sorting, but the raw dimensions are needed for
operators and tests.

Partition load is a routing hint. It should be partition-aware at the metrics
surface first; topic+group queue stats are not enough once sharding is active.
The first useful fields are `ready_count`, `inflight_count`, and later maybe
`oldest_ready_age_ms` per `(topic, partition, group)`. These hints can be shown
in admin/topology and optionally returned in topic-scoped topology responses.
Avoid putting every queue partition's live counters into committed coordination
labels by default; for larger clusters that turns a lossy routing hint into
metadata churn.

Publishing and consuming use different policies:

- Keyed publish keeps stable hash routing by default. This preserves per-key
  affinity and ordering expectations.
- Keyless publish may use an advisory policy: round-robin, sticky, least-backlog,
  or power-of-two least-backlog. Do not always pick the globally emptiest
  partition; stale hints and herd behavior can make that unstable.
- Consuming should not blindly prefer empty partitions. For drain throughput it
  should bias toward backlog. For fairness or tail latency it can reserve pulls
  for sparse partitions. This is a separate policy from publish routing.

Initial implementation order:

1. Make queue stats partition-aware and surface them in admin/topology.
2. Publish compact node load reports through the existing heartbeat-label path.
3. Add optional partition load hints to topology for requested topic/group
   snapshots, with TTL/staleness handling.
4. Use node load as a tie-breaker for new owner/follower placement first.
5. Add explicit keyless publish routing policies behind config/runtime settings.
6. Consider consumer scheduling policy after we have evidence from partitioned
   workloads.

Correctness rule: load data is never authority. Missing or stale load data must
fall back to the existing routing/placement behavior. Assignment epochs, owner
fences, and queue gates remain the correctness mechanisms.

## Live repartitioning (changing partition_count on an existing queue)

Easier than for a log: fibril is a work queue (consumed = gone), so there is no
historical log to migrate or rewrite. Only undelivered messages and per-key
ordering need handling across the change. v1 is GROW only, and only by an INTEGER
MULTIPLE (N_new = k * N_old, typically doubling). Shrink-by-factor and
arbitrary-N are deferred (see escape hatch below).

FIRST, THE NON-FEATURE: you can already over-provision. Declare a high partition
count up front and the spread-first placement planner distributes them across
nodes, no repartitioning ever needed (the Redis-slots / Cassandra-vnodes stance).
Live repartitioning is the escape hatch for queues that did not over-provision.

THE GUARANTEE AT RISK: per-key order. A key K routes to P_old = hash(K) % N_old
before and P_new = hash(K) % N_new after. When P_old != P_new, K's pre-cutover
messages sit in P_old while its post-cutover messages go to P_new, so P_new must
not deliver K before P_old has drained K.

WHY INTEGER-MULTIPLE GROWTH MAKES THIS CHEAP (the core trick): with modulo
hashing and N_new = k * N_old, every new partition sources from exactly one old
partition: source(p) = p % N_old. A new partition p (>= N_old) only ever receives
keys that were in old partition (p % N_old). Old partitions (< N_old) keep only
keys that STAY (P_old == P_new), so their order is preserved naturally by offset
order and they need NO gate. So the ordering barrier is a pure partition-identity
rule, with no per-message key storage and no whole-queue pause:

    new partition p may deliver only once old partition (p % N_old) has drained
    its pre-cutover (v_old) backlog. old partitions deliver throughout.

Example (N_old=4 -> N_new=8): new partition 5 only holds keys with h%8=5, all of
which had h%4=1, so partition 5 simply waits on partition 1 draining.

TWO VERSIONS:
- Routing version (partitioning_version, already exists): bumped immediately on
  repartition. Producers re-route to N_new at once via the existing
  fence_stale_partitioning redirect (a stale-version publish is redirected, the
  client refreshes topology and re-routes).
- Per-partition drain gate (new): each old partition's owner captures a cutover
  boundary offset when it first observes v_new (messages below it are v_old), and
  reports "old partition r drained" once its v_old backlog (ready + inflight
  below the boundary) is empty. The controller aggregates a drained set into a
  transition doc. A new partition p's owner reads the doc and ungates once
  (p % N_old) is drained. When all old partitions drain, the transition clears.
  This reuses the cohort controller pattern (heartbeat report -> leader aggregate
  -> published doc -> owner reads).

FLOW (grow):
1. Admin repartition(topic, group, N_new): validate N_new is a multiple of and
   greater than current; CAS the partitioning doc to (N_new, version+1).
2. Controller places owners/followers for the added partitions (spread-first).
3. Producers cut over to N_new via the version fence.
4. Old-partition owners drain v_old and report per-partition drain.
5. Each new partition ungates when its single source old partition drains.
6. Cohorts re-plan over N_new (generation + controller seeding absorb the churn).

PRECEDENTS: Elasticsearch _split requires the target to be a multiple of the
source and _shrink a factor of it, the same constraint for the same reason.
Linear / extendible hashing in DB indexes double the address space the same way.
Kafka and Pulsar allow arbitrary growth but silently break key ordering across
the remap and cannot shrink, so gated integer-multiple grow is strictly stronger
on correctness.

SHRINK (planned next): shrink-by-factor (N -> N/k) is the mirror. Surviving
partition r absorbs old partitions {r, r+N_new, r+2*N_new, ...} (those p with
p % N_new = r), still a pure partition-identity gate, no key storage. More
involved than grow in three concrete ways:

1. OFFSET-GATED hold, not whole-partition. A surviving partition keeps existing
   AND must deliver its own pre-cutover backlog (offset < boundary_r) so it can
   drain, while holding its post-cutover messages (offset >= boundary_r) until
   the merge is safe. So the grow gate (whole-partition delivery_held bool) does
   NOT apply, holding a surviving partition entirely would deadlock its own
   drain. Replace/augment with a `hold_above_offset` gate (an AtomicU64 boundary,
   sentinel = no hold): the delivery loop delivers messages below it and stops at
   the first message at/above it. This UNIFIES with grow, a new partition is just
   hold_above_offset = 0 (hold everything), so both directions share one gate.
2. MULTI-SOURCE drain. A surviving partition r lifts its hold only once ALL its
   merge sources {r, r+N_new, ...} have drained their pre-cutover backlog (r's own
   drain is settled_until(r) >= boundary_r; the others via the same drain label).
3. REMOVED partitions start FULL. Partitions >= N_new must stay consumable until
   their backlog drains and only then retire (grow's new partitions start empty).
   Client side: a shrinking consumer must KEEP consuming a removed partition until
   it is retired, then drop it (the broker closing the retired partition's stream
   is the natural signal). This is why auto-resub (below) is the foundation for
   both directions.

CHOREOGRAPHY (correctness ordering, the part to get right when wiring):
- Grow tolerates a ~1-tick lag between the version bump and a new partition being
  held, because new partitions start empty with no consumers (and the subscribe
  path starts them held). Shrink does NOT: a surviving partition is already live
  with consumers, so if the bump routes moved keys to it before it is held, it
  could deliver a moved key ahead of that key draining from its old partition.
- So shrink must: (1) write the marker, (2) wait until survivors are actually
  held (hold_above set) on their owners, (3) only THEN bump the version. A short
  grace (a couple of watcher ticks) between marker and bump is the pragmatic v1;
  a stricter version has owners ack "held" before the leader bumps.
- Survivor hold sequence: on seeing the marker, a survivor first sets
  hold_above_offset = 0 (hold everything) so nothing slips through, then the next
  drain refresh captures its boundary (= next_offset at that point) and relaxes
  hold_above to that boundary so it can deliver and drain its own pre-cutover
  backlog. Lift (hold_above = None) once ALL its merge sources have drained.

REMAINING WIRING (shrink, after the gate which is DONE): a shrink_queue trigger
(factor validation, marker-first with the grace before the version bump, no new
registration), direction-aware apply_repartition_transition /
refresh_repartition_drain / apply_repartition_drained in the broker (grow holds
p >= n_old whole; shrink holds p < n_new above-boundary and treats every p < n_old
as a drain source), and removed-partition retirement/deregistration deferred
(safe to leave them draining). The watcher and the drain label/aggregation are
already direction-agnostic.

## Client auto-resubscribe on partition-count change

The client builds its fan-in ONCE at subscribe time from the topology cache
(partition_set), so a running consumer never picks up partitions added by a grow
(or sheds ones removed by a shrink). Auto-resub makes the fan-in DYNAMIC: a
manager task watches the topology partition_count and, on growth, subscribes to
the newly appeared partitions and merges their streams into the same output; on
shrink, a removed partition's stream ends naturally when the broker retires it,
so the forwarder just exits. The manager periodically refreshes topology (a pure
consumer otherwise has no reason to refresh, so it would never notice a grow) and
stops when the subscription is dropped (its output sender's closed() resolves).
This is the foundation both grow and shrink need to be usable end to end.

ESCAPE HATCH (not a one-way door): the integer-multiple gate leaves no persistent
artifact, it changes no storage format, message schema, or hash function. After a
grow the queue is indistinguishable from one created at N_new. The strong
consequence: with both grow-by-multiple and shrink-by-factor, ANY target T is
reachable from N in at most two cheap steps, because gcd(N, T) divides both,
shrink to gcd(N, T) (a factor of N) then grow to T (a multiple of gcd). Worst
case N -> 1 -> T. So arbitrary-N never needs a per-key gate or a message-schema
change, it decomposes into the two cheap partition-identity operations. The only
cost is that the intermediate count (gcd, possibly small) transiently funnels
traffic through fewer partitions, so a big awkward jump (e.g. 6 -> 8 via 2) has a
real transition cost while doubling stays a single clean step. The one standing
commitment is keeping modulo hashing (which we already use); only switching to
consistent hashing would invalidate the fast path, and that would force a
repartitioning redesign regardless.

## Replication safety

### Follower progress is local-append progress

Owner reads are only proposals from the follower's point of view. A follower can
advance its durable replication cursor only after its own local append path
accepts the owner batch. This matters for gaps, overlaps, stale epochs, and
future prefix-validation failures. If the broker advanced from owner-read
offsets alone, a follower could report itself caught up without actually having
the corresponding local durable records.

This also keeps the durability contract honest. Replica-durable confirm waits on
what followers have accepted durably, not on what the owner attempted to send.

### Replicated events cannot outrun replicated messages

Stroma queue state is derived from events, but a ready/delivered/settled event is
not meaningful unless the payload it references is present and accepted in the
message log. Replication therefore applies in this order:

1. append replicated messages
2. append replicated events only if the message append was accepted
3. mutate in-memory queue state only if the event append was accepted

This prevents a follower from having in-memory state that says "offset N is
ready" while payload N is missing, stale, or from a different owner history. It
is intentionally conservative. A rejected message batch means the paired event
batch is skipped for that pull and the follower must repair or retry through a
safe path.

### Overlap is a checkpoint repair until Keratin can validate prefixes

Keratin's default replicated append mode rejects partial overlap. That is the
right default because "I already have offsets 0..10" does not prove that those
bytes are identical to the new owner's offsets 0..10. Accepting a suffix over an
unknown prefix would silently bless divergence.

The broker's checkpoint-aware catch-up treats overlap or gap as a repair signal:
install the owner's queue-state checkpoint, reset local follower logs to the
checkpoint continuation, then resume pulling records. `AlreadyPresent` remains
an idempotence outcome only. It advances over the owner-returned range, not to
the follower's full local tail.

Future improvement: Keratin can expose prefix hash/CRC validation for overlap.
Once the existing prefix is proven byte-identical, a suffix append can be safe
without checkpoint reset.

### Future replication cache shape

The first record-owned replication cache is intentionally opt-in because it did
not help the SSD/local benchmark path enough to justify default hot-path cost.
If we revisit it, the better shape is not a single fragile suffix per queue.

The cache should keep recent offsets indexed per queue and stream, so an owner
replication read can cheaply answer "do I have a contiguous range from this
offset?" even when inserts complete out of order. Reads should still only return
contiguous records. Gaps remain misses until filled.

Eviction should be global, not per-queue. The memory budget belongs to Stroma as
a whole, while each queue keeps enough local indexing to answer reads quickly.
When over budget, evict the oldest retained record globally, effectively taking
the lowest/oldest offsets from the queue that owns the oldest cached records.
That lets hot queues retain their newest replication tail while cold queues age
out naturally.

This is a replication performance cache only. It must never become the source
of truth for durability or follower progress. Cache population happens after
the durable owner operation completes, and follower progress still advances only
after the follower applies records locally.

### Replica-durable visibility: consumers see only committed data

Durability (when a publish is confirmed) and visibility (when a consumer may
receive a message) must agree. Before this, a replica-durable queue delivered a
message to a consumer as soon as it was locally enqueued on the owner, before any
follower had it. A consumer could then lease and ack a message that a promoted
follower never received, while the producer, still unconfirmed, retried and
caused duplicate work.

So a replica-durable queue gates delivery on the committed-replicated watermark:
delivery leases only offsets below the highest offset durable on the required
number of replicas (the Kafka high-watermark model). The watermark is the
(nodes - 1)-th largest follower durable `message_next`, since the owner is always
durable locally. It is maintained from the same follower progress reports that
drive the publish-confirm gate, so the two stay consistent by construction, and
it is exposed to the delivery loop as a single per-queue atomic ceiling so the
hot path stays lock-free.

Local-durable queues are never gated. There is no committed watermark to wait on
when the owner alone defines durability, so single-node and local-durable
delivery are unchanged. The mechanism is an exclusive deliverable ceiling on the
storage poll (`poll_ready` upper bound), reused from the same primitive a shrink
uses to hold delivery above a boundary.

A consequence worth stating: on a replica-durable queue delivery latency is now
bounded below by commit latency. That is correct, not a regression. Consumers
trade a few milliseconds of delivery delay for never acting on data that could
vanish on failover.

## Streaming replication (credit-based)

Replication catch-up was strict request then response per batch. A follower sent
a read, waited, applied it, then sent the next read. It was idle on the wire
while applying and idle applying while fetching. Once batch sizes made the fsync
cost small, this serialization (an owner-read round trip plus an apply, repeated
several times per tick) became the dominant cluster latency, not durability.

The model is a continuous stream with credit-based flow control. The follower
opens a stream and grants the owner a send budget in bytes. The owner pushes
offset-ordered record batches down one connection, decrementing the budget, and
pauses a stream when the remaining credit is below the next batch. The follower
runs a reader (socket into a bounded in-order buffer) and an applier (buffer into
the log) concurrently, so the next batch arrives while the current one is being
applied. TCP already guarantees in-order, reliable delivery on one connection, so
ordering needs no per-batch round trips. The applier still applies sequentially
through the unchanged durable apply path, so the failover-safety ordering (events
never outrun accepted messages) is untouched. Only fetching is overlapped.

Credit is refilled at a low watermark, not at zero. When the follower has applied
and freed about half the granted budget it sends more credit, so the pipe never
drains and there is no stop and wait. Credit is per stream so a slow partition
cannot starve others sharing a connection. The unit is bytes because that bounds
follower memory regardless of payload size.

Progress and credit travel on the same follower-to-owner frame. As the applier
durably applies, it reports its durable `message_next` / `event_next` and adds
credit in one message. The progress half feeds the existing follower-progress
path, so the confirm gate and the replica-durable visibility watermark keep
working with no change. Confirms therefore become asynchronous: there is no read
request to attach them to, they ride the stream.

Gaps and checkpoints use a rewind. If an apply needs a checkpoint (an offset gap
or an epoch fence), the follower tells the owner to reset the stream to an offset,
or it runs the existing checkpoint export and install (kept as request/response
because it is rare) and then resets the stream to the new offset. A stale owner is
fenced by the epoch carried on each batch, and a not-owner signal makes the
follower re-resolve the owner and re-subscribe, exactly as the pull path does
today.

This is additive on the wire. The transport already has server-to-client push,
a frame id usable as a multiplexed stream id, and the raw batch payload codec, so
streaming adds message types and owner/follower tasks rather than a transport
rewrite. The owner gains a per-stream sender holding a cursor and remaining
credit, woken by a new publish or a credit refill. The old pull path stays as a
fallback until the stream path is proven, including under failover.

## Dependency boundaries

### Future: a single top-level ganglion crate to depend on

Fibril currently reaches into two ganglion sub-crates (`ganglion-core` and
`ganglion-openraft`), so it depends on ganglion internals rather than a stable
surface. The target is a single top-level `ganglion` umbrella crate that
re-exports the public surface fibril needs, so the fibril side depends on just
`ganglion` (and the workspace [patch] redirects that one crate to the local
checkout). This is a ganglion-repo refactor, do it opportunistically. It mirrors
the clustering-module-separation intent: depend on cohesive public surfaces, not
internals.

## Forwarded writes (coordination)

The deterministic path is the mechanism, retries are only a safety net. A
forwarded write goes to the known leader and follows explicit leader hints on
redirect (`client_write_remote_with_hint`). The blind "no leader known yet"
retry count (`no_leader_retries`) defaults to a small non-zero value so a freshly
started or churning standby waits briefly for its leader view to converge instead
of failing instantly. Tests run it at zero to prove the deterministic forward and
hint-redirect path carries the write without leaning on retry-spam.

## Settings tiering (admin panel) — basic / advanced / expert

We are settings-disciplined: every tunable is configurable (no magic numbers).
But that means a LOT of knobs (replication has confirm/poll/read-budget/stream
linger/merge-bytes/retry-backoff/... and each subsystem adds more), and dumping
them all into the admin panel buries the handful that operators actually touch.

Decision: settings carry a visibility TIER, and the admin panel shows tiers
progressively rather than all at once.

- **Basic** — the few an operator changes routinely (e.g. durability defaults,
  partition count default, inflight TTL). Shown by default.
- **Advanced** — tuning knobs for a specific workload (poll intervals, read
  budgets, stream apply linger). Behind an "Advanced" expander per category.
- **Expert** — internal/rarely-correct-to-touch dials (retry backoff bounds,
  merge-byte caps, jitter). Behind a further "Expert" reveal; changing them is
  a "you'd better know why" action.

UI shape: collapsible per-category sections (Replication, Delivery, Partitioning,
...), each expandable, and within a category the Advanced/Expert knobs are hidden
until revealed. So the default view is small; depth is opt-in.

Implementation hook: tag each setting with its tier in the settings schema
(the same place the runtime-settings document is defined), so the panel renders
tiers from metadata instead of hard-coding which knob goes where. This keeps the
"every knob is configurable" guarantee while keeping the surface approachable.
New knobs (e.g. the client publish retry backoff bounds) default to Expert.

## Client failover retry (publish across an owner transition)

When an owner dies, the partition is briefly unreachable: the old owner is gone
and the cluster has not committed the reassignment yet. Without retry, every
confirmed publish in that window fails, so the app sees an error burst instead of
a short latency bump (a failover-under-load run showed ~150k publish errors).

Model (client-side):

- **Transient = transport failure** (`Disconnection`/`BrokenPipe`/`Eof`) to the
  routed owner. Structured server errors (incl. terminal not-owner) are NOT
  transient and stay fail-fast. A redirect is separate control flow (follow it).
- On a transient failure the client **refreshes topology from any reachable node**
  (`refresh_topology_throttled`: seeds + pooled endpoints, cooldown-gated). The
  assignment table is coordination-replicated, so any one live node returns the
  authoritative current owner — you only need to reach one, not a quorum. The
  client never picks a new owner itself; only the controller assigns one.
- It then **retries with bounded backoff** (exp + jitter) until `publish_timeout_ms`
  (a `ClientOptions` knob; default 30s; `0` = fail fast = old behavior). `engine_for`
  re-resolves the owner each attempt, so once the controller reassigns, the retry
  lands on the new owner. Net effect: a publish during failover blocks for the
  reassignment window then succeeds, instead of erroring.

"No owner yet" is **derived, not a distinct wire code**: the committed assignment
keeps naming the (dead) old owner during the gap (observed: still old owner at
t+6s post-kill), so there is no clean "owner = none" signal to read. Instead the
client distinguishes **transitioning vs gone** from its own topology cache, which
already separates the two:

- `counts` (partitioning per `(topic, group)`) is populated for every *declared*
  topic, even when the owner is currently unresolved.
- `by_queue` (owner endpoint per partition) is populated only when an owner is
  known.

So after a *successful* refresh against a populated cluster view: topic absent
from `counts` ⇒ **not declared / gone ⇒ fail fast** (do not burn the whole
budget); topic present but no `by_queue` owner ⇒ **transitioning ⇒ keep retrying**.
The "populated view + successful refresh" guard avoids a standalone/cold cache
(empty topology) being misread as not-found.

Pieces: (1) `publish_confirmed` transient retry + `publish_timeout_ms`. (2) the
declared-vs-gone fail-fast above (client-side via the cache split; no protocol
change). (3) the pipelined `publish_with_confirmation` path, which must preserve
in-flight ordering under the confirm window across a retry — the careful one.

### Consumer-side failover (and why it does not clash with reconnect-to-same)

The producer retry has a read-side mirror: a consumer must keep consuming a
partition across an owner failover, not stall. The subtlety is that two desirable
behaviors look like they conflict but operate at different layers:

- **Connection-level reconnect-to-same-endpoint** (engine): a TCP blip where the
  owner is still the owner. The engine reconnects the same endpoint and reconciles
  every subscription on it. The stream "survives reconnects" - cheap, no churn.
- **Subscription-level migrate-on-owner-change** (supervisor): the owner actually
  moved (failover). The subscription must re-resolve and re-subscribe to the new
  node.

Because "stream survives reconnects" means a failover does NOT close the
per-partition stream, a supervisor that only watches the stream never fires. So
failover is detected via TOPOLOGY instead: each supervised partition captures the
owner endpoint it bound to, periodically refreshes topology (throttled), and
migrates only when the committed owner endpoint actually changes
(`current.is_some() && current != bound`). A blip leaves the owner unchanged (or
briefly unknown) so it does not trigger migration - the engine handles it. They
are disjoint triggers at disjoint layers, so there is no compromise: blips →
reconnect-to-same, failover → migrate. Re-subscribe also degrades gracefully
(`engine_for` re-resolves the current owner, same or new), so it is always
correct.

Deferred: a "first wins" race (reconnect-to-same vs re-resolve) in the ambiguous
stream-close window would cut recovery latency, but blips never close the stream
and failover latency is dominated by reassignment, so it is not worth the
double-subscribe/cancel complexity yet. Minor wart: the dead owner's connection
slot lingers in the pool after a failover (the engine keeps retrying it) - harmless,
a future pool-prune.

## Idempotent producer (effectively-once producer->broker) — roadmap

The client failover retry is at-least-once: a confirmed-publish whose confirm
errors mid-failover has an UNKNOWN outcome, so re-publishing can duplicate. Today
the caller decides via `FibrilError::is_retryable()` / `retry_advice()` (the manual
path - it knows which ids failed and can re-publish, tolerating duplicates). The
automatic path is an idempotent producer:

- Each producer gets a stable **producer-id**; each message a **monotonic
  sequence** per (producer-id, partition).
- The owner keeps a small **dedup window** of recently-seen (producer-id, seq)
  and drops a re-send it already has, returning the original outcome. The window
  travels with replication (so a failover owner still dedups) and is bounded
  (seq high-watermark per producer + a recent ring).
- The client then **auto-retries unconfirmed publishes** safely: no duplicate, and
  order preserved (re-send in seq order). This is the Kafka idempotent-producer
  model; it turns the producer->broker hop into effectively-once.

`failover_verify` is the proof harness: with this, `unconfirmed_delivered` and
`duplicate deliveries` go to ~0 across a failover. Scope: a producer-id/seq field
on the publish wire op + an owner-side dedup map (replicated) + client auto-retry.
Until then, `is_retryable()` + caller retry is the documented, honest stopgap.

Helper layering (opt-in, addable before the broker side lands): a client-side
`ReliablePublisher` wrapper that (a) stamps a producer-id + monotonic sequence
header on each message and (b) retries-until-confirmed using `is_retryable()`.
With no broker dedup it is at-least-once-no-miss (every message eventually
confirmed; duplicates possible - documented); once the owner-side dedup map lands
the SAME helper becomes effectively-once with NO API change, because the ids are
already on the wire. So the whole "zero-missed" pipeline (ids + retry) can ship as
an opt-in helper now and get wired to real dedup later. Pair it with copy-paste
DOC EXAMPLES (the `match ... is_retryable()` pattern, ReliablePublisher usage,
later the idempotent-producer config) - reliability patterns belong in docs, not
rediscovered per user.

## Plexus — a fan-out / stream consumption mode (future direction)

"Plexus" (anatomical: a branching nerve/vessel network that fans one signal to
many) is the working name for a JetStream-like fan-out / message-bus capability:
each consumer gets EVERY message (pub/sub + persistent streams), versus the
work-queue core where each message goes to one consumer (consumed=gone).

### Distribution: a mode within Fibril, NOT a separate product

Precedent is unanimous - nobody ships fan-out as a separate product: Pulsar
(subscription type over one BookKeeper log), Kafka (consumer groups over the log),
NATS (JetStream is a mode inside the server), Redis/RabbitMQ (streams added as a
type within the same broker). "The thing everyone connects to" REQUIRES one
cluster, one client, one operational surface. A separate product would
re-implement the hardened part (replication/coordination/log) and drift.

So Plexus is a per-topic (or per-subscription, Pulsar-style) MODE on the same
cluster/binary/client, selected by a topic-type / subscription-type knob.

User-facing framing: the lightest, most intuitive surface is a CHANNEL TYPE - a
"plexus channel" is just a channel/topic declared with fan-out/stream semantics,
sitting next to ordinary work-queue channels on the same cluster and client. Not a
separate subsystem to deploy or connect to - one cluster, and a channel is either a
queue or a plexus. `declare(name, type: queue | plexus)` (or `channel(name).plexus()`)
is the whole user-visible footprint. This keeps "increase usability by adding modes"
genuinely simple: the user picks a channel type, the smart client + server do the
rest. Matches the precedent (Pulsar subscription types, NATS subjects) and the
"just works" philosophy - depth is opt-in per channel, the default surface stays
small.

Naming (decided): **Fibril queues** (the default work queue) and **Plexus
channels** (fan-out). The queue keeps its plain name - it is the default, so it
does not need a special codename; `plexus` is the one themed type name, used for
the fan-out channel and the "queue vs plexus" contrast in docs. (Shortlist once
considered for a queue codename, not adopted: synapse - apt but overused; bouton -
neural/distinctive; bolus - consumed=gone; conduit - rejected, off the organic
theme. Left here only as a record.)

### The enabling refactor: substrate vs consumption semantics

Fibril is already log-backed (keratin-log) with the work-queue semantics layered
on as events. The move is to make that layering explicit:

- SHARED SUBSTRATE (the hardened 80%, already trending toward a clustering module):
  keratin-log + partitioning + replication + cohorts + coordination (ganglion) +
  cursor plumbing + the client failover/fan-in machinery.
- PLUGGABLE CONSUMPTION ENGINES over it, via the existing stroma `QueueEngine` seam:
    - WorkQueueEngine (today): events -> lease/ack/consumed-gone.
    - StreamEngine (Plexus, new): retention policy + per-consumer DURABLE CURSORS +
      fan-out delivery + subjects/wildcards.
  Broker routes by topic-type to the right engine. New crate (e.g. stroma-stream /
  plexus) beside stroma-core, both on the substrate - not a fork, not client-only.

The physical log already retains (for replication/recovery); work-queue "removal"
is logical state, so a retained-stream mode coexists at the substrate level.

### What Plexus adds vs reuses

REUSES (no rebuild): replicated partitioned log, coordination, partitioning,
failover, per-partition ordering, client fan-in + producer/consumer failover retry.
ADDS: (1) retention mode (time/size/count) instead of consume-removal - starting
point is the existing retention-via-checkpoint; (2) per-consumer durable cursor (the
core new primitive; stored + replicated like assignments); (3) fan-out = N cursors
over one log (no per-subscriber copy, cheaper than RabbitMQ); (4) subject hierarchy
+ wildcard routing; (5) the bus surface (smart client exposing both modes).
HARDER/LATER: two modes coexisting without product confusion (clear topic-type
semantics - "no core fan-out" was deliberate, so this is a scoped expansion);
exactly-once (overlaps the idempotent-producer work); JetStream extras (KV/object
store, mirrors/sources) as features on top of streams.

### Why this shape

One thing to connect to (the goal), zero duplication of the hard parts, client
reuse, one cluster to operate. The mode selector (topic/subscription type) is the
intuitive user knob - Pulsar shows users grok "pick your subscription type" - and
fits the "just works" smart-client philosophy. Verdict: high usability upside, and
a layer rather than a rewrite, because the substrate already supports
log-with-many-cursors.

## queue_handle vs destroy_partition: optimistic retry, with serialization as the escape hatch (2026-06-20)

Stroma's `queue_handle` lazily (re)creates a partition's logs on first access.
`destroy_partition` tombstones the slot, renames the partition dir aside, and
deletes it. A concurrent `queue_handle` whose lazy init is mid-flight can race a
destroy: the dir/log is pulled out from under the init, surfacing as
`Io("No such file")`, `Io("File exists")`, or `Io("Keratin already open ...")`.
This was a real (if cold-path) function-level race - it surfaced as a ~1-in-5
flaky test (concurrent_destroyers_and_materializers), not just test noise.

CHOSEN FIX (optimistic retry): on init failure `queue_handle` re-checks whether a
destroy swapped its slot (stale -> re-acquire and retry, unbounded since destroy
churn is finite) and, if the slot is still current, retries a bounded number of
times for the brief window where a just-destroyed sibling incarnation has not yet
released its log/dir. A CAS does NOT apply here - the contended resource is the
filesystem (dir + Keratin log), not an in-memory word; the registry slot-map
already uses compare_and_swap correctly. So the real design axis is "optimistic
retry" vs "explicit serialization". Retry wins for now because destroy_partition
is a cold repartition-retirement path, so a little spin/yield under contention is
fine, and the fix is ~20 local lines.

FUTURE ESCAPE HATCH (correctness-by-construction, if churn ever gets hot or the
bounded retry proves insufficient): serialize init against destroy. Mark a slot
"initializing" (a flag + Notify on QueueSlot); have destroy_partition WAIT for the
displaced slot's in-flight init to finish (then shut it down) before trashing the
dir. That closes the window by construction - no no-such-file, no already-open, no
retry/spin at all - at the cost of more slot state and more teardown edge cases.
Reach for this if destroy/create contention becomes a hot path or the retry bound
ever has to grow.

## Stroma queue-handle lifecycle: a real concurrency-safety issue (found 2026-06-20)

A pre-existing flaky test (concurrent_destroyers_and_materializers_stay_consistent)
turned out to pin a genuine design weakness in Stroma's per-partition handle
lifecycle, surfaced by amplifying the test (destroyers + materializers + queue_handle
"reader" victims, run in rounds). Symptoms, in order of discovery:
  1. queue_handle's lazy init races destroy_partition: the dir/log gets trashed mid
     init -> Io("No such file") / Io("File exists").
  2. Io("Keratin already open ..."): a NEW incarnation's init opens a flock
     (root/.keratin.lock) still held by a prior incarnation's handle.
  3. At extreme scale the test HANGS (a stuck await / deadlock), not just errors.

ROOT CAUSE (the architectural part): `QueueHandle` is a STRONG handle that owns the
Keratin logs (and thus the flock). Anyone holding a clone - the returned handle, the
spawned periodic_snapshot task - pins those logs open. So destroy/evict can never
truly reclaim a partition while a holder exists; a racing build can be orphaned (its
slot swapped out after it published) and then nobody shuts it down, leaking the flock
forever -> permanent "already open". Keratin makes this worse: shutdown() releases the
flock only AFTER a writer-task ack (ms-scale under load), and Keratin::Drop early
-returns if shutdown_started is set - so a shutdown() that errors after setting the
flag but before unlocking leaks the lock permanently (Drop won't retry it), and
destroy does `shutdown().await.map_err(..)?` so it bails before renaming, leaving a
locked root behind.

Patches tried (all partial; left UNCOMMITTED): (a) optimistic retry on stale slot;
(b) Dekker-style begin_init/wait_init_done serialization so destroy waits for an in
-flight build - but the registry CAS is ArcSwap Acquire/AcqRel, not SeqCst, so the
double-check is not airtight and destroy can still miss a build; (c) self-cleanup
(builder shuts down its own handle if its slot went stale) - reduces orphans but does
not fix the shutdown-latency / shutdown-error-leaks-lock windows. None make it safe by
construction; it stays whack-a-mole.

PROPER FIX (converged design - handle as a re-resolved ticket, safe by construction):
  - The log-owning handle lives ONLY in the registry slot; never hand out a strong,
    storable clone. `queue_handle` returns a lightweight TICKET = { Arc<ArcSwap<
    Registry>> + key (tp,part,group) }. Each op re-resolves the current live handle
    from the registry by key, so a stale ticket resolves to the current incarnation or
    "gone" - it can never pin a dead incarnation's logs. Destroy/recreate just swaps
    what's under the key.
  - Non-leakable resolved handle, two API options (offer both):
      * `resolve() -> Option<Handle>` via Weak upgrade - ergonomic, no closure; a
        stored Weak does not pin, upgrade fails after destroy.
      * `with(|h| ...)` closure form - the borrow can't escape the op (strict mode).
  - Per-op cost is just an ArcSwap load + hash lookup (cheap; cacheable within a
    batch). This is a real refactor: QueueHandle is used across all Stroma ops and via
    the stroma_core API (broker/replication), so it touches many call sites.
  INTERIM (smaller, deterministic) mitigation if the full refactor is deferred: make
  the flock release reliable + always-called - (1) ensure Keratin releases the lock
  even when shutdown errors (unlock independent of the writer-ack, and let Drop release
  if shutdown didn't); (2) ensure every displaced incarnation is shut down (destroy /
  evict / self-clean) without `?`-bailing before the unlock; (3) consider a Keratin
  `open` that WAITS for a prior holder's release with a tri-state outcome
  (nothing-to-wait / already-released / waited) and a timeout that surfaces a real
  hang instead of spin-retrying. That removes the latency races and the permanent leak
  without the full ticket refactor, but is not safe-by-construction against future
  handle-pinning.

REPRODUCER: the amplified test (destroyers + materializers + queue_handle readers that
must never spuriously fail, run in rounds) at "mouse" scale (~8 rounds x 24 tasks, 4
threads) reproduces the error in round 0; at "bear" scale (~40 x 96, 8 threads) it
also exposes the hang. Keep both as graduated trip-wires (one parameterized helper,
two entry points) once the engine is fixed.

### RESOLVED (2026-06-20): per-key lifecycle mutex
Root cause confirmed by tracing: after a destroy renames a partition dir, two builds
ran concurrently and both create_dir_all + opened the same path, colliding on the
.keratin.lock flock (and at bear scale escalating to deadlock). There was no in-memory
serialization of the Keratin open/close lifecycle - only the advisory, fail-fast flock.
Fix (keratin 7dd6c18): a per-partition-key lifecycle mutex (Stroma.lifecycle_locks)
acquired only on the cold open/close paths (queue_handle BUILD slow-path, destroy,
evict); the materialized fast path and all hot publish/consume paths are untouched.
mouse 10/10 + bear 3/3 deterministic; full stroma-core suite green. The broader
ticket/re-resolve redesign (orphan-pin leak) remains a separate, lower-urgency item.

### RESOLVED (2026-06-20): ticket/re-resolve redesign (the orphan-pin leak)
QueueHandle is now a TICKET, not the state: the log-owning `QueueHandleInner` lives only
in the registry slot (sole strong owner); a handed-out `QueueHandle` holds `{registry,
key, Weak<Inner>}`. `resolve()` upgrades the Weak, or on a rotated/gone incarnation
re-looks-up the slot by key (transparent failover, else `ActorGone`). It returns a
`Resolved<'a>` guard that is lifetime-bound to the ticket, so a resolved handle CANNOT be
moved into a `'static` task or stashed in a longer-lived field - parking a strong handle
and pinning the logs forever is now a COMPILE error, not a latent leak. The control task
and periodic_snapshot task hold the ticket (Weak) and resolve per use, so they never pin;
an orphaned task self-exits when `resolve()` fails. Contained to stroma-core (broker/
fibril/ganglion never touch QueueHandle). Bench: resolve() ~3.9 ns/op (one Weak upgrade,
done once per batch on hot paths -> amortized ~0/record); handing out a ticket is cheaper
than the old ~25-Arc clone-bundle. Full keratin workspace 285 passed; fibril builds across
the boundary. Flow diagrams + lessons-learned + the evict/destroy teardown-contract
assessment: keratin/stroma/core/src/STROMA_HANDLE_REDESIGN.md.

Teardown-contract note (from this work): evict/destroy guard only on inflight_len (leased
deliveries), NOT on the owner-operation lease, and force-shutdown the logs. They are safe
only on a QUIESCED partition - the caller must freeze + drain owner operations first
(freeze_queue_for_transition), which destroy already documents. The ticket change does not
regress this (it was already unprotected if the contract is violated) and degrades more
gracefully (ActorGone, no flock leak). Optional belt-and-suspenders: drain owner ops inside
evict/destroy so the contract is self-enforced. Rematerialize-on-completion deferred.

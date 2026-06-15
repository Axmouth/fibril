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

SHRINK (later): shrink-by-factor (N -> N/k) is the mirror. Surviving partition r
absorbs old partitions {r, r+N_new, r+2*N_new, ...} (those p with p % N_new = r),
still a pure partition-identity gate, no key storage. It is modestly more
involved than grow: the removed partitions (indexes >= N_new) start FULL, so they
must be kept consumable until their v_old backlog drains and only then retired
(grow's new partitions start empty), and a surviving partition gates on a GROUP
of sources rather than one. A follow-on after grow, not free with it.

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

## Forwarded writes (coordination)

The deterministic path is the mechanism, retries are only a safety net. A
forwarded write goes to the known leader and follows explicit leader hints on
redirect (`client_write_remote_with_hint`). The blind "no leader known yet"
retry count (`no_leader_retries`) defaults to a small non-zero value so a freshly
started or churning standby waits briefly for its leader view to converge instead
of failing instantly. Tests run it at zero to prove the deterministic forward and
hint-redirect path carries the write without leaning on retry-spam.

---
title: Failover plan
description: Remaining recovery proofs and failure-detection acceptance scenarios.
---

This plan covers the remaining rollout gates for queue recovery and failure
detection. Implemented sealing, verified transfer, durable generation
installation, exact quorum activation and bounded recovery are documented in
[recovery sealing](/reliability/recovery-sealing/).

## Remaining recovery work

1. Extend process-isolation and membership-change tests through candidate
   handoff, including replacement outside the fixed proposed replica set. Extend
   background learner coverage to packet-level asymmetric partitions and whole-
   process interruption during checkpoint transfer. Controlled stale-owner
   isolation, concurrent recovery/admission and repeated broker/metadata reopening
   with incomplete payload backfill are covered by authenticated tests.
2. Add composite reconstruction for crossed queue log tails and authoritative
   stream-state recovery. Keep unsupported or contradictory histories fenced.
3. Extend transfer support beyond the current 16 MiB record/page and snapshot
   bounds. Measure full-history rescans, checkpoint/hash CPU, peak memory and
   recovery latency on representative storage and replication configurations.
4. Add safe reclamation of retained generations and stages. Extend implemented
   node-level disk accounting to per-queue attribution.
5. Design [resumable recovery phases, storage-health policy and cooperative owner
   handoff](/development/recovery-continuation/). Add actual write/fsync-failure
   injection before adopting early replication or fault-triggered relinquishment.

Fresh cluster queues enroll automatically on every platform. The worker prepares
their write quorum, activates exact instances and retries local admission
independently on all replicas. Owner-process replacement before activation renews
the preparation. Activated queues use verified history recovery and retain their
configured confirmation threshold. Owner-only durability still requires its
surviving storage: the controller holds such a resource on its owner until the
owner returns or an operator accepts the loss. Pending plans retry with bounded
backoff when reachable evidence is insufficient. Authoritative divergence requires
investigation.

Placement feeds recovery with a live follower as the candidate, ranked by its
applied-tail label, and proposes only copy holders while they still meet the old
write requirement. A new controller waits for registered nodes during its startup
grace before moving owners. Pending recoveries that the worker never resolves, and
owner-only holds, are listed as controller attention with guarded withdraw and
accept-owner-loss overrides. See
[failure modes](/reliability/failure-modes/#choosing-the-new-owner).

## Fast recovery

### Target and scope

Target **two seconds or less from owner process loss to resumed useful service**
for an enrolled replicated queue when the metadata leader and a sufficient data
quorum survive. Measure first valid delivery and first new replica-durable publish
confirmation independently. A ready flag alone does not meet the target. This is
an engineering target for the common case, with no current latency guarantee.

Measure simultaneous owner/metadata-leader loss separately, including election
latency. Silent partitions, quorum loss, missing payloads, damaged storage and
conflicting histories have separate availability limits. Initial implementation
and measurement focus on three-replica, majority-durable queues. Other supported
confirmation policies require their own quorum-intersection proof. Stream recovery
and speculative/early-replication histories remain separate work.

### 1. Establish a comparable baseline

Use the same release binary, storage, configuration, queue contents and client
reconnect policy for heartbeat and eager runs. Measure both a continuously
reconnecting client and broker readiness, keeping their times distinct. Separate
owner-only failure from combined owner/metadata-leader failure. Compare with the
older first-delivery benchmark only after matching its method and build profile.

Capture monotonic stage timings for failure observation, placement proposal,
worker dispatch, seal/witness collection, source inspection, plan commit, transfer,
installation, receipt publication/visibility, activation and local admission.
The recovery driver emits payload-free `fibril::recovery_timing` events for each
attempt and its witness, inspection, comparison, plan, transfer, installation,
activation and admission stages. Fields include transition, resource, epoch, peer,
sequence, monotonic microseconds and success/error/cancellation. Target preparation
contains child stages, so do not sum parent and child durations. Enable the target at
`info` when using a restrictive log filter. No per-record timing events are emitted.
The same instrumentation feeds the [Cluster dashboard](/admin-dashboard/#topology)
with bounded, process-local worker timelines. Failure detection and client
reconnection require separate observations and are excluded from worker totals.

Record RPC count, connection/handshake time, metadata waits, fsync time, bytes
read/copied, retries and polling delay, keyed by resource and recovery transition.
Keep payloads out of diagnostics and bound retained trace data.

Exit criterion: account for the dominant elapsed time, including why an activation
attempt can lack quorum receipts after installation calls returned. Distinguish
replication lag in the local metadata view from missing receipts or failed work.
Recovery and local admission wake on committed metadata changes, with a
one-second fallback and 25 ms burst coalescing. Replica seal/inspection pipelines, private stage preparation and installation
use at most two concurrent operations. Each inspection can start as soon as its
replica seals, while source selection still waits for all collected evidence. All
stage reads finish before any installation can replace a source. Completed-stage
source reads serialize. Local storage admission also wakes deferred assignment
transitions so follower reporting need not wait for the periodic retry.

### Current recovery screen

The 24 September release screen used three replicas on one host, SATA ext4,
majority-durable confirmations, 1 KiB payloads and a one-second eager grace.
Only the queue owner was killed. The metadata leader survived. With agreed
checkpoints and 100k settled messages plus one outstanding message, a matched
baseline delivered the first fresh-client message in 4.15 seconds. The wakeup,
concurrency and admission changes measured 2.00 and 2.02 seconds. Owner readiness
was sampled at 3.31 seconds before and 2.10 to 2.11 seconds after. Polling can observe
readiness after a client has already received its message.

The 100k outstanding-message case measured 5.51 seconds ready / 6.45 seconds first
fresh delivery. This workload still verifies live payloads. Every successful gate
checked all expected confirmed IDs, new durable work and old-owner rejoin.

Warm 500/s traffic passed age-, event- and byte-triggered checkpoint gates, with
owner readiness at 1.71 to 1.91 seconds. The original subscriber received the new
post-recovery probe at 4.94 to 4.96 seconds. A separate instrumented run spent
3.93 seconds refreshing client topology and 19 ms resubscribing. That run exposed
a Rust receive loop bug, where a `Some(frame)` select pattern disabled the receive
arm on clean EOF. The fixed loop closes pending work immediately on EOF and treats
transport read errors as retryable. Equivalent pending-request EOF tests pass in all five SDKs. With the
same immediate-retry server, the original subscriber received fresh work at 1.06
seconds. Silent-endpoint discovery remains a separate case.

A subsequent zero-grace idle-monitor and per-replica seal/inspection screen measured
0.51 to 1.13 seconds to sampled readiness for small backlogs, 0.82 to 1.95 seconds to
fresh-subscription delivery, and 1.06 seconds for fresh work on the original
subscriber under warm 500/s traffic. With 100k outstanding messages, readiness was
4.50 seconds and first fresh delivery 5.45 seconds. All gates checked confirmed IDs,
new durable work and old-owner rejoin. These small screens establish useful leads,
not a p95 latency bound.

### Initial transport result

A release-build three-replica, majority-durable SATA test with one confirmed 1 KiB
message isolated repeated small metadata RPC delays. Enabling `TCP_NODELAY` on
Ganglion's real TCP transport reduced the recovery attempt from 6.75 to 6.96 seconds
to 0.81 to 1.22 seconds across two control and three changed runs, including a final
run with the connection-lifecycle repair. Owner readiness moved
from 8.8 to 9.3 seconds after the kill to 2.87 to 3.14 seconds. The fresh-subscription
probe's first delivery moved from 9.5 to 11.2 seconds to 3.73 to 4.00 seconds. Its two-second
attempt timeout, routing and reconnect costs remain part of that measurement.
These are small single-host acceptance measurements, not a failover guarantee.

Every successful case checked confirmed-message recovery, new durable publication
and convergence after old-owner restart. The initial pending-subscription failure is resolved by fallback discovery and
retrying temporary recovery responses. The mixed-traffic gate below covers the
original subscriber. Continuously retrying publishers, heavier traffic and combined
owner/metadata-leader loss still need matched release-build measurements. A first
debug-build screen of the combined loss is in
[owner and metadata leader loss](#owner-and-metadata-leader-loss).

### Longer retained histories

A subsequent matched SATA check with 10,000 outstanding 1 KiB messages recovered
all confirmed IDs, accepted new durable work and converged after owner restart.
Two runs reached owner readiness in 15.12 to 15.22 seconds and fresh-client delivery
in 15.86 to 15.97 seconds. In the first run, 11.27 seconds of the 13.14-second recovery
attempt went into source inspection, comparison and reinspection. With 100,000 outstanding messages, or 100,000 settled messages plus
one outstanding message, inspection repeatedly exceeded its ten-second deadline
and the queue remained fenced throughout the 90-second observation window. The
settled case retained history from offset zero, so it does not establish compacted
checkpoint behavior. The saved pre-transport control also timed out at 10,000
messages, so this scale limit predates the transport improvement.

Automatic inspection uses pages of up to 4096 records / 16 MiB, within the
existing wire limits. These runs kept the same total history budgets and deadlines. Keratin
buffers sequential frozen reads and reuses bounded record scratch space. In the
same SATA profiles, the combined changes reached readiness/delivery in 4.29/5.21
seconds for 10k outstanding messages, 50.72/51.68 seconds for 100k outstanding,
and 46.87/47.70 seconds for 100k settled plus one outstanding. Every case verified
all expected IDs, new durable work and old-owner restart/convergence. These are
individual acceptance runs, with no claim of a history-independent failover bound.
Larger pages alone improved 10k but still timed out at 100k.

Automatic inspection traverses each retained log sequentially, validates its
sealed digest before completing it, and constructs recovery evidence only after
full receiver verification. It reuses authenticated connections and the selected
artifact within one attempt. Strict diagnostic reads and target copying retain
complete verification on every page. See [implementation details](/development/recovery-internals/#bounded-sequential-inspection).

Before retained-data reuse, the sequential-inspection SATA results with the same
three-replica majority-durable profile were:

| Retained workload | Owner ready | First fresh delivery |
| --- | ---: | ---: |
| 10k outstanding, 1 KiB | 3.90 s | 4.80 s |
| 100k outstanding, 1 KiB | 24.11 s | 24.99 s |
| 100k settled + one outstanding, 1 KiB | 18.97 s | 19.83 s |
| 200k outstanding, 64 bytes | 15.82 s | 16.77 s |

Every case preserved expected IDs, accepted new durable work and converged after
old-owner restart. The 200k case uses smaller payloads to remain within the
unchanged total inspection byte budget. These counts describe retained history,
not an offered rate of 100k or 200k messages/s. Copying retained data accounts for
14.56 seconds in the 100k-backlog run, including settled payloads when still
retained. Live backlog alone does not bound recovery cost. A final-binary repeat of the
100k/1KiB case passed at 24.57 seconds ready / 25.47 seconds delivery.

### Mixed traffic across owner loss

A 30-second warm-up at 500 offered 1 KiB messages/s with an active ACKing consumer
exercises snapshots and ACK history before owner SIGKILL. Clients can use
explicit fallback discovery addresses, and the original subscriber successfully
reattaches through temporary recovery responses. With sequential inspection,
owner readiness was 5.02 seconds and the original subscriber received a newly
published probe at 5.51 seconds. All 17,034 journaled confirmed IDs were delivered
and replicas converged after restart. Buffered postkill deliveries are excluded
from the resumed-service timing. In-flight publisher requests can still fail on
connection loss, so applications must handle unknown outcomes. A final-binary repeat
passed at 6.05 seconds ready / 6.11 seconds for the original subscriber's fresh
probe, preserving all 17,030 observed confirmed IDs.

Extend these gates to sustained 100k/200k offered workloads, recording achieved
throughput, retained log heads/bytes, checkpoint age and live backlog separately.
Extend the internal matched RabbitMQ/JetStream 10k/100k backlog and settled-history
checks to sustained traffic and additional failure modes. Keep durability,
placement and client concurrency explicit. Process loss does not test power loss.

### Owner and metadata leader loss

On 11 and 12 October 2026, a debug-build screen killed the node that both owned
the queue and led Raft. It used three local nodes and one confirmed message, and
measured kill to activation:

| Settings | Kill to activation |
| --- | ---: |
| Defaults (9 s liveness TTL, eager failover off, election 1.5 to 3 s) | 9.7 s |
| Eager failover, 1 s grace | 6.8 s |
| Eager failover, 0 grace | 5.7 s |
| Eager 0 grace, Raft heartbeat 100 ms, election 0.5 to 1 s | 3.7 s (two runs) |
| Same, with the controller waking on leader change | 1.9 and 2.1 s |
| Eager 0 grace, default Raft timings, with the leader wake | 4.9 and 5.3 s |

The leader wake lets a newly elected controller act on transport failures it saw
during the election instead of sleeping up to a full 2 s tick. See the
[controller wake note](/development/engineering-notes/#controller-wake-on-leadership).

Leader loss is slow mostly because of the election itself. After the last
heartbeat, openraft waits for the leader lease plus an election timeout before a
follower calls an election, and it ties the lease to `election_timeout_max`.
With Fibril's default election timeout of 1.5 to 3 s, a new leader exists only
4.5 to 6 s after the old one stops. Tighter Raft timings shorten this, but false
elections under fsync stalls, slow disks and CPU load need measuring before the
defaults change. These are small debug-build screens. Release-build runs with
repeated trials are still needed.

### 2. Reduce overhead with the existing recovery proof

Evaluate changes individually against that baseline:

- Extend inspection connection reuse to other recovery operations if measured costs
  justify it. Reconnect,
  cancellation, stale replies and per-operation deadlines must preserve identity
  checks and bounded resource use.
- Per-replica seal/inspection overlap and one-page prefetch during target append
  are implemented. A SATA release ABBA test copying a missing 64 MiB suffix
  measured 0.95 to 1.13 seconds copying before and 0.75 to 0.76 seconds after. Complete
  attempts measured 1.85 to 2.08 and 1.70 seconds. Two active targets can each retain
  two 16 MiB pages, plus wire/storage allocations. Compatible retained-data reuse
  avoids this copy path entirely.
- Keep separate Finish and Install operations for now. A completed write quorum
  and the all-source-reads barrier preserve retry sources before replacement.
  Combining the operations cannot eliminate their verification or durability work.
  A warm SATA trace spent 17 to 19 ms in Finish and 162 to 168 ms in Install, so the
  removable network overhead is only part of the smaller stage.
- Reproduce missing local visibility of successful installation receipts before
  adding a dedicated exact-receipt wait. Such a wait must preserve current plan,
  process, deadline and activation checks. It cannot repair genuinely absent
  receipts. Existing committed-change wakeups already shorten ordinary retries.
- Keep admission after committed activation. The two admission calls totaled
  about 11 ms in the same trace. A prequeued waiter adds lifecycle and stale-plan
  handling without removing the authority check. Revisit on higher-RTT networks.
- Resume completed stages and cache verified evidence within its exact immutable
  transition where safe, retaining all invalidation checks.
- Explicit EOF handling is repaired in Rust and covered across all five SDKs.
  Test silent-endpoint discovery separately before adding bounded or staggered
  attempts. Preserve topology generation checks, cancellation and cleanup.

Adopt each change only after correctness checks and matched timing runs. Do not
shorten safety deadlines or weaken confirmation thresholds to meet the target.
Reassess the remaining delay before introducing new recovery metadata or formats.

### 3. Extend compatible retained-data reuse

Matching sealed payloads can be shared into recovery staging and installed
generations after full CRC and digest verification. Each generation keeps private
metadata and a private writable tail. Mutations of shared segments first create
private copies. Exact selected state, fresh writer identity and installed-quorum
activation remain required. See [compatible retained-data reuse](/development/recovery-internals/#compatible-retained-data-reuse).

In the matched three-replica SATA 100k-backlog checks, owner readiness improved
from a fresh 23.99-second baseline to 9.31 to 10.34 seconds. Fresh first delivery was
10.14 to 11.19 seconds. The 100k-settled-plus-one case reached readiness/delivery in
9.17/10.98 seconds, and the warm-traffic case reached readiness/original-subscriber
fresh-probe delivery in 4.84/4.98 seconds. These checks preserved confirmed IDs and
verified old-owner restart. They do not establish a general latency bound.

Remaining work includes safe generation reclamation, shared-file repair latency
under pressure and wider compatibility when retained bounds differ. Promotion
within an existing generation is a separate design option. Current reuse
preserves the durable generation-installation protocol.

### 4. Agreed checkpoints and suffix comparison

An opt-in queue worker establishes a common applied boundary during
healthy operation. `replication.agreed_checkpoint_interval_ms` defaults to zero.
Positive values start periodic attempts on materialized queues. Each admitted
replica pins a separate durable replay base before the owner chooses the common
cut. Local replay and disk verification reconstruct the same normalized state,
retained payload range and live-payload identities while later writes continue.

Each pin describes the queue's one log: an epoch, the applied boundary, the
payload floor and the log tail. The coordinator asks for a cut that covers the
largest pinned log tail and checks that the cut covers every pin's applied
boundary, log tail and payload floor. A cut carries `next` and `payload_head`.
Proposal and evidence digests use `-v2` tags, and the checkpoint status reports
`next`.

Every admitted replica, including the owner, publishes its own durable capsule
receipt through the existing consensus metadata channel. One guarded certificate
binds exact history, membership, process/storage identities and content. A learner
joining the accepted set invalidates an in-progress agreement even when the base
activation identifier stays unchanged. Contradictory reports stop that attempt.

Local installation persists a covering restart snapshot and accepted retention
record before advancing the logical log head. Interruption can leave extra
retained data. The next checkpoint waits for installation acknowledgements from
all participants. Recovery prefers the accepted capsule, verifies all retained
payloads and the later event suffix, and retains the existing source selection,
quorum and activation checks. Missing compatible capsules use ordinary verified
snapshot recovery. Corrupt or contradictory material grants no shortcut.

The first policy limits one candidate per queue and one local build at a time,
with record, byte and elapsed-work budgets. An uncommitted attempt expires after
two minutes. Old live messages remain retained, and a large live backlog still costs
payload verification. Disabling the interval stops new attempts and allows an
existing attempt to finish. The last accepted checkpoint and its suffix remain
pinned. An unavailable admitted replica can delay replacement and increase retained
disk. Retention limits, administrative abandonment and physical disk accounting need
further operational tuning before considering a default-on policy.

Current tests cover unequal replay bases, zero, compaction, bounded physical reads,
interruption at each storage publication boundary, unanimous agreement, partial
local installation, repeated metadata/broker restarts and confirmed suffix recovery.
Further acceptance should cover high offered rates, many partitions, checkpoint/hash
CPU, memory, retained disk, metadata traffic and healthy publish/delivery latency.
Incremental digests require a separate proof of what they cover.

### Validation and adoption

Start with a small repeated release-build screen, then run at least 30 trials per
primary failure/configuration case before assessing the two-second target. Report
median, p95, maximum, failures and false reassignments. The initial performance
gate is p95 at or below two seconds in the stated common-case workload. Publish
slow cases rather than excluding them. Include SATA/NVMe, empty and nonempty
queues, retained backlogs, compacted histories and concurrent traffic. Report
healthy throughput, tail latency, CPU, memory and retained disk alongside failover.

At each protocol change, cover confirmed data present only on a surviving quorum
member, offset zero, unequal checkpoints, payload/event tail mismatch, delayed
activation, acknowledgements, stale owners still reachable by clients, candidate
loss, lost replies, restart at durable transition boundaries and repeated failover.
Checkpoint work additionally needs crashes around receipt/certificate publication
and compaction racing boundary replacement. Tests must reject stale authority and
verify all confirmed identities survive. Legitimate redelivery is recorded
separately from loss.

Keep adoption incremental: overhead changes first, generation reuse after its
proof, periodic boundaries only if their measured benefit justifies their steady
state cost. Use opt-in rollout for new authority/format paths until fault acceptance
passes. Document any format break explicitly and preserve a safe recovery fallback.

## Existing experimental queues

Migration of existing queue histories is outside the supported scope. This is a
breaking change for adoption of the new recovery path. Drain existing queues
using their compatible broker revision, retire them and recreate them with a fresh enrolled history. Disposable
test data can be recreated directly. Retained data receives no automatic conversion
or deletion. Unsupported histories remain fenced during recovery.

## State digests at a recovery boundary

Exact queue checkpoint capture and bounded comparison from different checkpoint
starts are implemented in [recovery sealing](/reliability/recovery-sealing/#queue-state-at-an-exact-boundary).
New resource declarations have a consensus incarnation ID bound into pending
recovery transitions. An explicit local storage primitive persists incarnation,
history and writer-session IDs for pristine storage, and blocks ordinary access
after restart while retaining recovery sealing. Ordinary cluster queue
creation uses this protocol. Consensus preparation fixes history/session IDs and
withholds serving until activation. Before activation, a new owner process or
placement can renew preparation with the same origin IDs and fresh quorum receipts.
Local preparation leaves admission closed. Prepared quorum receipts persist under
the current decision and require the owner and configured write threshold.
Authenticated remote preparation rechecks the exact decision through consensus.
Explicit initial activation admits the exact prepared storage/process instances
and carries their identity on live replication. Version-two seals bind retained
evidence to that accepted storage history and original replica instance. Owner
process replacement requests recovery even without a placement change, as does
any replica replacement on a stream or a W = 1 queue. A restarted follower of a
queue with W >= 2 is suspended and
[rejoins as a learner](/reliability/recovery-sealing/#restarted-queue-followers)
while the queue keeps serving. Recovered activation
establishes the next exact accepted history and supports repeated recoveries.
Existing resources require recreation to adopt this path. The catalogue ID alone
supplies no history authority.
A trusted, quorum-installed checkpoint can replace older history, so indefinite
retention of settled payloads is not required.

Owner-local leases have a separate comparison projection. Newly activated delayed
work moves to ready through an ordered event carrying an explicit clock
boundary and bounded work count. Replay uses that recorded boundary and consumes
the same timers, preserving retry and TTL state. Older histories can lack these
transitions and are unsupported by this recovery path. Complete stream-state proof
is also pending.

Measure checkpoint replay/hash CPU, peak memory and recovery delay before adding
periodic state hashes or larger recovery fan-out. The ordered-application benchmark
showed competitive throughput and higher memory use at saturation. Allocation
retention and pending-task memory require separate investigation.

## Failure-detection rollout

Opt-in eager detection includes immediate reconnect verification and zero-grace
idle-connection monitoring on the active metadata controller. See
[replication](/reliability/replication/#eager-failover) for settings and safeguards.
Four single-host SATA owner-SIGKILL screens recorded suspicion 26 to 34 ms after the
injection timestamp, including the container kill overhead. Metadata leadership
survived. These measurements do not describe silent partitions or restoration of
service: verified history, fencing and installed-quorum activation remain required.

Remaining work includes packet-level partitions, longer CPU/storage stalls,
planned drains, repeated membership changes and durable-stream acceptance.
Measure detection separately from election, recovery activation and restored
publish/delivery service. Faster suspicion alone does not establish a shorter
end-to-end outage.

## Acceptance scenarios

Covered: confirm on A and B, leave C behind, then lose A. In
`majority_failover_preserves_old_assignment_until_recovery_proof`
(`crates/coordination-ganglion/src/lib.rs`), stale heartbeat labels rank the
lagging C first, but C gets only a pending recovery that needs two old witnesses
and cannot serve or start a new history. In
`recovery_copies_multiple_pages_to_a_lagging_checkpoint_replica`
(`crates/fibril/tests/recovery_driver.rs`), the suffix confirmed on A and B is
missing from C. After A stops, B becomes owner, recovery copies the suffix into
C's stage, and every confirmed ID is delivered after activation.
`recovery_keeps_downloaded_source_after_source_loss_and_candidate_restart` covers
the lagging copy as the candidate, which finishes its copy after losing its
source. These end-to-end tests run with agreed checkpoints enabled.

Open:

- Exercise process kill, process pause, silent packet loss and an old owner
  isolated from coordination while still reachable by clients.
- Exercise brief disconnections, CPU and storage stalls, planned drain,
  controller-leader loss, repeated failovers and membership changes.
- Change the runtime policy during a pending suspicion. Restart the controller
  or candidate at each transition boundary and verify recovery of the decision.
- Compare heartbeat and eager modes for detection, safe promotion and restored
  publish/delivery latency, alongside false reassignments, retries and healthy
  traffic disruption. Test queues and durable streams.

Adoption requires preserved confirmed history and bounded recovery behavior in
addition to faster crash detection. Default timing values follow measurements.


### Checkpoint work policy and diagnostics

Periodic attempts can also be triggered by accumulated event count or approximate
local append bytes. The interval remains the opt-in switch. Thresholds default to
zero, starts remain at least one second apart, and queue-specific jitter staggers
periodic attempts. One local bounded build still runs at a time. Event counts use
the accepted cut. Byte counts rebase on observing a new certificate, epoch change
or counter reset. Neither hint authorizes retention or recovery decisions.

The Cluster page and topology API expose accepted checkpoint age, proof and install
counts, failure reason, and the cut, local suffix and retained records of the
queue log. The API
returns at most 128 records and reports truncation. These logical ranges do not
measure physical disk allocation. Byte/age scheduling is implemented. Retained-disk
limits, many-queue pressure tests and safe generation reclamation remain follow-ups.

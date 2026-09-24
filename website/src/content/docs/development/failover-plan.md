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
2. Add composite reconstruction for crossed payload/event tails and authoritative
   stream-state recovery. Keep unsupported or contradictory histories fenced.
3. Extend transfer support beyond the current 16 MiB record/page and snapshot
   bounds. Measure full-history rescans, checkpoint/hash CPU, peak memory and
   recovery latency on representative storage and replication configurations.
4. Add safe reclamation and separate disk accounting for retained generations and
   stages. Establish durable metadata support before enabling the installation
   protocol on non-Unix platforms.

Fresh Unix cluster queues enroll automatically. The worker prepares their write
quorum, activates exact instances and retries local admission independently on
all replicas. Owner-process replacement before activation renews the preparation;
activated queues use verified history recovery and retain their configured
confirmation threshold. Owner-only durability still requires its
surviving storage. Pending plans retry with bounded backoff when reachable evidence
is insufficient; authoritative divergence requires investigation.

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
and measurement focus on three-replica, majority-durable queues; other supported
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
contains child stages; do not sum parent and child durations. Enable the target at
`info` when using a restrictive log filter. No per-record timing events are emitted.

Record RPC count, connection/handshake time, metadata waits, fsync time, bytes
read/copied, retries and polling delay, keyed by resource and recovery transition.
Keep payloads out of diagnostics and bound retained trace data.

Exit criterion: account for the dominant elapsed time, including why an activation
attempt can lack quorum receipts after installation calls returned. Distinguish
replication lag in the local metadata view from missing receipts or failed work.
Recovery and local admission now wake on committed metadata changes, with a
one-second fallback and 25 ms burst coalescing. Replica seal/inspection pipelines, private stage preparation and installation
use at most two concurrent operations; each inspection can start as soon as its
replica seals, while source selection still waits for all collected evidence. All
stage reads finish before any installation can replace a source; completed-stage
source reads serialize. Local storage admission also wakes deferred assignment
transitions so follower reporting need not wait for the periodic retry.

### Current recovery screen

The 24 September release screen used three replicas on one host, SATA ext4,
majority-durable confirmations, 1 KiB payloads and a one-second eager grace.
Only the queue owner was killed; the metadata leader survived. With agreed
checkpoints and 100k settled messages plus one outstanding message, a matched
baseline delivered the first fresh-client message in 4.15 seconds; the wakeup,
concurrency and admission changes measured 2.00 and 2.02 seconds. Owner readiness
was sampled at 3.31 seconds before and 2.10–2.11 seconds after; polling can observe
readiness after a client has already received its message.

The 100k outstanding-message case measured 5.51 seconds ready / 6.45 seconds first
fresh delivery. This workload still verifies live payloads. Every successful gate
checked all expected confirmed IDs, new durable work and old-owner rejoin.

Warm 500/s traffic passed age-, event- and byte-triggered checkpoint gates, with
owner readiness at 1.71–1.91 seconds. The original subscriber received the new
post-recovery probe at 4.94–4.96 seconds. A separate instrumented run spent
3.93 seconds refreshing client topology and 19 ms resubscribing. The Rust receive
loop ignored clean EOF because a `Some(frame)` select pattern disabled the receive
arm; it now closes pending work immediately and treats transport read errors as
retryable. Equivalent pending-request EOF tests pass in all five SDKs. With the
same immediate-retry server, the original subscriber received fresh work at 1.06
seconds; silent-endpoint discovery remains a separate case.

A subsequent zero-grace idle-monitor and per-replica seal/inspection screen measured
0.51–1.13 seconds to sampled readiness for small backlogs, 0.82–1.95 seconds to
fresh-subscription delivery, and 1.06 seconds for fresh work on the original
subscriber under warm 500/s traffic. With 100k outstanding messages, readiness was
4.50 seconds and first fresh delivery 5.45 seconds. All gates checked confirmed IDs,
new durable work and old-owner rejoin. These small screens establish useful leads,
not a p95 latency bound.

### Initial transport result

A release-build three-replica, majority-durable SATA test with one confirmed 1 KiB
message isolated repeated small metadata RPC delays. Enabling `TCP_NODELAY` on
Ganglion's real TCP transport reduced the recovery attempt from 6.75–6.96 seconds
to 0.81–1.22 seconds across two control and three changed runs, including a final
run with the connection-lifecycle repair. Owner readiness moved
from 8.8–9.3 seconds after the kill to 2.87–3.14 seconds. The fresh-subscription
probe's first delivery moved from 9.5–11.2 seconds to 3.73–4.00 seconds; its two-second
attempt timeout, routing and reconnect costs remain part of that measurement.
These are small single-host acceptance measurements, not a failover guarantee.

Every successful case checked confirmed-message recovery, new durable publication
and convergence after old-owner restart. The initial pending-subscription failure is resolved by fallback discovery and
retrying temporary recovery responses; the mixed-traffic gate below covers the
original subscriber. Continuously retrying publishers, heavier traffic and combined
owner/metadata-leader loss still need matched measurements.

### Longer retained histories

A subsequent matched SATA check with 10,000 outstanding 1 KiB messages recovered
all confirmed IDs, accepted new durable work and converged after owner restart.
Two runs reached owner readiness in 15.12–15.22 seconds and fresh-client delivery
in 15.86–15.97 seconds. In the first run, 11.27 seconds of the 13.14-second recovery
attempt went into source inspection, comparison and reinspection. With 100,000 outstanding messages, or 100,000 settled messages plus
one outstanding message, inspection repeatedly exceeded its ten-second deadline
and the queue remained fenced throughout the 90-second observation window. The
settled case retained history from offset zero; it does not establish compacted
checkpoint behavior. The saved pre-transport control also timed out at 10,000
messages, so this scale limit predates the transport improvement.

Automatic inspection now uses pages of up to 4096 records / 16 MiB, within the
existing wire limits; total history budgets and deadlines are unchanged. Keratin
buffers sequential frozen reads and reuses bounded record scratch space. In the
same SATA profiles, the combined changes reached readiness/delivery in 4.29/5.21
seconds for 10k outstanding messages, 50.72/51.68 seconds for 100k outstanding,
and 46.87/47.70 seconds for 100k settled plus one outstanding. Every case verified
all expected IDs, new durable work and old-owner restart/convergence. These are
individual acceptance runs, with no claim of a history-independent failover bound.
Larger pages alone improved 10k but still timed out at 100k.

Automatic inspection now traverses each retained log sequentially, validates its
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
exercises snapshots and ACK history before owner SIGKILL. Clients can now use
explicit fallback discovery addresses; the original subscriber successfully
reattaches through temporary recovery responses. With sequential inspection,
owner readiness was 5.02 seconds and the original subscriber received a newly
published probe at 5.51 seconds. All 17,034 journaled confirmed IDs were delivered
and replicas converged after restart. Buffered postkill deliveries are excluded
from the resumed-service timing. In-flight publisher requests can still fail on
connection loss; applications must handle unknown outcomes. A final-binary repeat
passed at 6.05 seconds ready / 6.11 seconds for the original subscriber's fresh
probe, preserving all 17,030 observed confirmed IDs.

Extend these gates to sustained 100k/200k offered workloads, recording achieved
throughput, retained log heads/bytes, checkpoint age and live backlog separately.
Extend the internal matched RabbitMQ/JetStream 10k/100k backlog and settled-history
checks to sustained traffic and additional failure modes. Keep durability,
placement and client concurrency explicit; process loss does not test power loss.

### 2. Reduce overhead with the existing recovery proof

Evaluate changes individually against that baseline:

- Extend inspection connection reuse to other recovery operations if measured costs
  justify it. Reconnect,
  cancellation, stale replies and per-operation deadlines must preserve identity
  checks and bounded resource use.
- Per-replica seal/inspection overlap and one-page prefetch during target append
  are implemented. A SATA release ABBA test copying a missing 64 MiB suffix
  measured 0.95–1.13 seconds copying before and 0.75–0.76 seconds after; complete
  attempts measured 1.85–2.08 and 1.70 seconds. Two active targets can each retain
  two 16 MiB pages, plus wire/storage allocations. Compatible retained-data reuse
  avoids this copy path entirely.
- Keep separate Finish and Install operations for now. A completed write quorum
  and the all-source-reads barrier preserve retry sources before replacement;
  combining the operations cannot eliminate their verification or durability work.
  A warm SATA trace spent 17–19 ms in Finish and 162–168 ms in Install, so the
  removable network overhead is only part of the smaller stage.
- Reproduce missing local visibility of successful installation receipts before
  adding a dedicated exact-receipt wait. Such a wait must preserve current plan,
  process, deadline and activation checks; it cannot repair genuinely absent
  receipts. Existing committed-change wakeups already shorten ordinary retries.
- Keep admission after committed activation. The two admission calls totaled
  about 11 ms in the same trace; a prequeued waiter adds lifecycle and stale-plan
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

Matching sealed payloads can now be shared into recovery staging and installed
generations after full CRC and digest verification. Each generation keeps private
metadata and a private writable tail; mutations of shared segments first create
private copies. Exact selected state, fresh writer identity and installed-quorum
activation remain required. See [compatible retained-data reuse](/development/recovery-internals/#compatible-retained-data-reuse).

In the matched three-replica SATA 100k-backlog checks, owner readiness improved
from a fresh 23.99-second baseline to 9.31–10.34 seconds. Fresh first delivery was
10.14–11.19 seconds. The 100k-settled-plus-one case reached readiness/delivery in
9.17/10.98 seconds; the warm-traffic case reached readiness/original-subscriber
fresh-probe delivery in 4.84/4.98 seconds. These checks preserved confirmed IDs and
verified old-owner restart; they do not establish a general latency bound.

Remaining work includes safe generation reclamation, shared-file repair latency
under pressure and wider compatibility when retained bounds differ. Promotion
within an existing generation is a separate design option; current reuse
preserves the durable generation-installation protocol.

### 4. Agreed checkpoints and suffix comparison

An opt-in Unix queue worker now establishes a common applied boundary during
healthy operation. `replication.agreed_checkpoint_interval_ms` defaults to zero;
positive values start periodic attempts on materialized queues. Each admitted
replica pins a separate durable replay base before the owner chooses the common
cut. Local replay and disk verification reconstruct the same normalized state,
retained payload range and live-payload identities while later writes continue.

Every admitted replica, including the owner, publishes its own durable capsule
receipt through the existing consensus metadata channel. One guarded certificate
binds exact history, membership, process/storage identities and content. A learner
joining the accepted set invalidates an in-progress agreement even when the base
activation identifier stays unchanged. Contradictory reports stop that attempt.

Local installation persists a covering restart snapshot and accepted retention
record before advancing either logical log head. Interruption can leave extra
retained data. The next checkpoint waits for installation acknowledgements from
all participants. Recovery prefers the accepted capsule, verifies all retained
payloads and the later event suffix, and retains the existing source selection,
quorum and activation checks. Missing compatible capsules use ordinary verified
snapshot recovery; corrupt or contradictory material grants no shortcut.

The first policy limits one candidate per queue and one local build at a time,
with record, byte and elapsed-work budgets. An uncommitted attempt expires after
two minutes. Old live messages remain retained; a large live backlog still costs
payload verification. Disabling the interval stops new attempts and allows an
existing attempt to finish; the last accepted checkpoint and its suffix remain
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
median, p95, maximum, failures and false reassignments; the initial performance
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
verify all confirmed identities survive; legitimate redelivery is recorded
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
or deletion; unsupported histories remain fenced during recovery.

## State digests at a recovery boundary

Exact queue checkpoint capture and bounded comparison from different checkpoint
starts are implemented in [recovery sealing](/reliability/recovery-sealing/#queue-state-at-an-exact-boundary).
New resource declarations have a consensus incarnation ID bound into pending
recovery transitions. An explicit local storage primitive persists incarnation,
history and writer-session IDs for pristine storage, and blocks ordinary access
after restart while retaining recovery sealing. Ordinary Unix cluster queue
creation uses this protocol. Consensus preparation fixes history/session IDs and
withholds serving until activation. Before activation, a new owner process or
placement can renew preparation with the same origin IDs and fresh quorum receipts.
Local preparation leaves admission closed. Prepared quorum receipts persist under
the current decision and require the owner and configured write threshold.
Authenticated remote preparation rechecks the exact decision through consensus.
Explicit initial activation now admits the exact prepared storage/process instances
and carries their identity on live replication. Version-two seals now bind retained
evidence to that accepted storage history and original replica instance; process
replacement requests recovery even without a placement change. Recovered activation
establishes the next exact accepted history and supports repeated recoveries.
Existing resources require recreation to adopt this path; the catalogue ID alone
supplies no history authority.
A trusted, quorum-installed checkpoint can replace older history; indefinite
retention of settled payloads is not required.

Owner-local leases have a separate comparison projection. Newly activated delayed
work now moves to ready through an ordered event carrying an explicit clock
boundary and bounded work count. Replay uses that recorded boundary and consumes
the same timers, preserving retry and TTL state. Older histories can lack these
transitions and are unsupported by this recovery path; complete stream-state proof
is also pending.

Measure checkpoint replay/hash CPU, peak memory and recovery delay before adding
periodic state hashes or larger recovery fan-out. The ordered-application benchmark
showed competitive throughput and higher memory use at saturation; allocation
retention and pending-task memory require separate investigation.

## Failure-detection rollout

Opt-in eager detection includes immediate reconnect verification and zero-grace
idle-connection monitoring on the active metadata controller. See
[replication](/reliability/replication/#eager-failover) for settings and safeguards.
Four single-host SATA owner-SIGKILL screens recorded suspicion 26–34 ms after the
injection timestamp; that includes the container kill overhead. Metadata leadership
survived. These measurements do not describe silent partitions or restoration of
service: verified history, fencing and installed-quorum activation remain required.

Remaining work includes packet-level partitions, longer CPU/storage stalls,
planned drains, repeated membership changes and durable-stream acceptance.
Measure detection separately from election, recovery activation and restored
publish/delivery service. Faster suspicion alone does not establish a shorter
end-to-end outage.

## Acceptance scenarios

- Confirm on A+B, leave C behind, then lose A with stale heartbeat labels;
  recover the confirmed history before C serves or select B safely.
- Exercise process kill, process pause, silent packet loss and an old owner
  isolated from coordination while still reachable by clients.
- Exercise brief disconnections, CPU and storage stalls, planned drain,
  controller-leader loss, repeated failovers and membership changes.
- Change the runtime policy during a pending suspicion; restart the controller
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
the accepted cut; byte counts rebase on observing a new certificate, epoch change
or counter reset. Neither hint authorizes retention or recovery decisions.

The Cluster page and topology API expose accepted checkpoint age, proof and install
counts, failure reason, local event suffix and retained message ranges. The API
returns at most 128 records and reports truncation. These logical ranges do not
measure physical disk allocation. Byte/age scheduling is implemented; retained-disk
limits, many-queue pressure tests and safe generation reclamation remain follow-ups.

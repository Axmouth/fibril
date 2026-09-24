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
The current one-second worker poll, serial RPCs, fresh connections and whole-attempt
retries are investigation leads; their individual costs are not yet measured.

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

- Wake recovery and admission on relevant committed metadata changes, with bounded
  periodic retry as a fallback. Prevent lost wakeups and tight retry loops.
- Extend inspection connection reuse to other recovery operations if measured costs
  justify it. Reconnect,
  cancellation, stale replies and per-operation deadlines must preserve identity
  checks and bounded resource use.
- Overlap independent witness/inspection and target-installation work with bounded
  concurrency. Keep dependent mutations ordered and preserve the required witness
  set; an early reply alone cannot establish that source selection is complete.
- Wait for the exact required committed receipts when local metadata visibility is
  behind, avoiding a repeat of successful installation. Preserve deadlines and
  retry behavior for genuinely missing evidence.
- Resume completed stages and cache verified evidence within its exact immutable
  transition where safe, retaining all invalidation checks.

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

### 4. Assess agreed checkpoints and suffix comparison

If history inspection remains significant, establish periodic agreed recovery
boundaries during healthy operation. An agreed boundary represents a durable,
fully applied cut under a specific accepted history and replica configuration.
It must cover queue state, message/event dependencies and live-payload identity,
with the existing explicit treatment of owner-local leases and delayed activation.
A shared boundary is additional recovery evidence; it does not change the normal
publish-confirm contract.

Agreement evidence validation now binds exact admitted membership, exclusive
boundaries and content identities, rejects stale metadata, requires every admitted
replica and prevents a conflicting report from being overwritten by a later reply.
This foundation does not persist checkpoint material or certificates and cannot
authorize compaction or recovery shortcuts. Durable runtime integration remains:

- Capture an exact cut while later writes continue. Avoid hashing moving actor
  state against unrelated log positions or stalling the actor for full encoding.
- Initially obtain durable receipts from every currently admitted replica, including
  the owner, then commit the boundary certificate through coordination. A missing
  replica delays new checkpoints while ordinary service continues. Reject stale
  configuration, history, storage or writer identities; learner admission also
  changes eligibility even if the activation identifier remains unchanged.
- Retain a usable checkpoint and required suffix records/live payloads until a
  replacement boundary is safely committed. Define retention across compaction,
  membership changes, process replacement and interrupted checkpoint publication.
- During failover, validate surviving evidence against the boundary and compare
  the subsequent suffix. Preserve messages confirmed after that boundary and
  resolve unconfirmed suffixes using the same history rules as full recovery.
- Fall back safely when no compatible boundary or adequate survivor evidence
  exists. Incomplete, stale or corrupted certificates cannot authorize promotion.

Start with time/byte-triggered, coalesced background work and at most one pending
boundary per partition. Measure checkpoint/hash CPU, memory, retained disk,
metadata traffic and publish/delivery latency for idle queues, busy queues and
many partitions before selecting defaults. Incremental digests require a separate
proof of what they cover; a compact digest is not a replacement for recoverable
state or quorum authority.

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

Opt-in eager detection is implemented through cluster runtime settings; see
[replication](/reliability/replication/#eager-failover). Repeated explicit Raft
transport errors and failed reconnects can remove a peer from placement after a
grace period. Heartbeat expiry remains the default and handles silent failures.
Recovery proof still gates authority to serve.

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

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
and convergence after old-owner restart. A separate subscription begun during
recovery remained pending despite ready work in two diagnostic runs. Isolate that
client/server transition before claiming uninterrupted recovery for existing
subscriptions. A continuously attempting publisher, larger histories, healthy
traffic, and combined owner/metadata-leader loss still need matched measurements.

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

Inspection currently requests 256 records per page and verifies both complete
retained logs on every page read. Keep the larger-history cases as availability
gates while assessing larger bounded pages, verified progress reuse and connection
reuse. Separate scan/verification work from RPC setup and authorization costs;
preserve content, identity and quorum checks throughout. Larger queues also need
explicit acceptance for the total page, record and byte limits.

### 2. Reduce overhead with the existing recovery proof

Evaluate changes individually against that baseline:

- Wake recovery and admission on relevant committed metadata changes, with bounded
  periodic retry as a fallback. Prevent lost wakeups and tight retry loops.
- Reuse authenticated peer connections across recovery operations. Reconnect,
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

### 3. Prove promotion using existing storage

Design a path that can reuse a compatible replica's current generation without
copying and reinstalling it. Fresh fencing and intersecting survivor evidence must
establish that the proposed owner contains the required authoritative history,
including all confirmed payload/event dependencies and completely applied state.
Equal offsets, matching current state hashes or an old checkpoint alone cannot
establish this authority. Replicas may have different valid suffixes; define which
ones can be reconciled and when reconstruction remains necessary.

Specify the durable transition before implementation: exact incarnation/history,
configuration, writer and storage identities; new write authority; quorum receipts;
crash/retry behavior; and how existing seals interact with generation reuse.
Retaining the same files must not reopen an old writer or invalidate evidence.
Define safe fallback at every stage, including a crash after local preparation but
before quorum activation. Reuse must be idempotent and preserve the current
confirmation threshold; unsupported evidence stays fenced.

Exit criterion: a reviewed state transition and deterministic fault tests prove
that the common compatible-history case can activate without full generation
replacement. Retain verified reconstruction for cases that need repair.

### 4. Assess agreed checkpoints and suffix comparison

If history inspection remains significant, establish periodic agreed recovery
boundaries during healthy operation. An agreed boundary represents a durable,
fully applied cut under a specific accepted history and replica configuration.
It must cover queue state, message/event dependencies and live-payload identity,
with the existing explicit treatment of owner-local leases and delayed activation.
A shared boundary is additional recovery evidence; it does not change the normal
publish-confirm contract.

Design and measure the following before choosing a cadence or format:

- Capture an exact cut while later writes continue. Avoid hashing moving actor
  state against unrelated log positions or stalling the actor for full encoding.
- Obtain durable receipts from a set sufficient for the configured policy and
  future recovery intersection, then commit the boundary certificate through
  coordination. Reject stale configuration, history, storage or writer identities.
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

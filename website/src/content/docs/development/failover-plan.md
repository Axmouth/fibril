---
title: Failover plan
description: Planned promotion evidence, eager failure detection and acceptance scenarios.
---

This plan covers preservation of confirmed history during ownership changes and
an optional faster failure detector. The controller now persists pending recovery
requests and retains the previous assignment for changes involving replicated
confirmation. Automatic seal dispatch, evidence collection, recovery and activation remain
implementation work; replicated failover currently pauses at this barrier.
Current behavior and limitations are in [replication](/reliability/replication/).

## Promotion safety

Before serving, a new owner must establish that its recovered history preserves
previously confirmed work. Heartbeat tails provide candidate-selection hints;
local log completeness alone cannot establish this property.

The proposed handshake persists a pending transition with the previous replica
set, durability policy and new epoch. It fences old-epoch writes, obtains fresh
durable evidence from enough previous replicas to intersect every possible
confirmation quorum, validates compatible histories and payload/event
dependencies, then recovers the candidate before activating ownership.
Insufficient evidence keeps the partition unavailable with bounded retries and
an explicit reason. When missing replicas return and provide sufficient compatible
evidence, recovery should select a source, catch up an eligible owner and activate
automatically. Operator intervention is reserved for irrecoverable corruption,
incompatible authoritative histories or unresolved evidence loss.

The protocol must cover repeated failovers, controller and candidate restarts,
and membership changes. Initial implementation should establish the proof for
`majority_durable`; fixed-count policies require their own recovery thresholds.
Owner-only durability cannot guarantee recovery from another node. Compatible-history selection must establish ancestry and confirmed dependencies
across different retained ranges and checkpoint boundaries.

## Remaining recovery work

The implemented [recovery seal receiver](/reliability/recovery-sealing/) binds
explicit requests to committed transitions and persists exact retained-content
identity. Read-only sealed-source pages, bounded retained-history comparison and
recoverable local checkpoint installation are also available. Automatic recovery
requires the following pieces:

1. Build automatic dispatch with bounded backoff and restartable, coalesced
   progress on the explicit seal transport and witness admission primitives.
   Preserve the old replica set and witness threshold.
2. Establish compatible history from the collected reports, including compacted
   prefixes and payload/event/checkpoint dependencies. Exact content fingerprints
   and the completed seal count alone cannot establish ancestry or authority.
   Define installed/activated lineage, checkpoint relationships and a trusted
   baseline for existing resource incarnations; a newly advanced fence cannot
   establish that baseline.
3. Transfer the selected history and durably install it on the new write quorum,
   including the candidate, before committing activation. Resume interrupted
   phases and isolate the old owner through repeated failovers. Bound aggregate
   transfer buffering and total verification work, and support records larger than the
   current page limit before enabling automatic bulk transfer.

Automatic dispatch remains disabled until installation and activation can finish
safely. Linux tests cover local seals and interrupted checkpoint replacement;
other platforms need durable metadata support before these operations are enabled.

## State digests at a recovery boundary

Exact queue checkpoint capture and bounded comparison from different checkpoint
starts are implemented in [recovery sealing](/reliability/recovery-sealing/#queue-state-at-an-exact-boundary).
New resource declarations have a consensus incarnation ID bound into pending
recovery transitions. An explicit local storage primitive persists incarnation,
history and writer-session IDs for pristine storage, and blocks ordinary access
after restart while retaining recovery sealing. It is not enabled by ordinary
broker creation. Remaining work must authorize those history/session IDs through
consensus, bind snapshots and recovery installation to that authority, and readmit
writers after recovery. Existing resources still need a verified baseline; the
catalogue ID alone supplies no history authority.
A trusted, quorum-installed checkpoint can replace older history; indefinite
retention of settled payloads is not required.

Owner-local leases have a separate comparison projection. Delayed work can move
to ready state under a local clock without a log event, so a full timer-state
comparison still needs a defined common-time projection or equivalent replay
semantics. Preserve retry, TTL and DLQ decisions when defining that projection.
Complete stream-state proof is also pending.

Measure checkpoint replay/hash CPU, peak memory and recovery delay before adding
periodic state hashes or larger recovery fan-out. The ordered-application benchmark
showed competitive throughput and higher memory use at saturation; allocation
retention and pending-task memory require separate investigation.

## Eager failure detection

After promotion safety is established, add an opt-in cluster runtime policy for
eager detection. Proposed names are `failover.mode = heartbeat | eager` and a
bounded probe/grace duration; these are design names, not available settings.
Keep heartbeat detection as the default and as the fallback for silent failures.

Unexpected peer transport failure starts a coordinated probe/reconnect attempt.
Persistent suspicion can initiate reassignment after the grace period, through
the same safe promotion handshake. Coalesce signals per node, bound retries and
distinguish planned drain from unexpected failure. Client disconnects and normal
replication-stream restarts must not themselves evict a broker.

Runtime revisions must reach the active controller and define how a changed
grace applies to existing suspicions. Ganglion Raft election timers remain
separate from this broker-membership policy. Record the signal, probe outcome,
elapsed detection and recovery time, assignment epoch, promotion refusal and
owner changes without payloads.

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

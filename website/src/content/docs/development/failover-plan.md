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
identity. Recoverable local checkpoint installation is also available. Automatic
recovery requires the following pieces:

1. Dispatch seals with bounded backoff and restartable, coalesced progress for
   each persisted transition. Preserve the old replica set and witness threshold.
2. Collect fresh, distinct old-replica reports and establish compatible history,
   including compacted prefixes and payload/event/checkpoint dependencies.
   Exact content fingerprints alone cannot establish ancestry or authority.
3. Transfer the selected history and durably install it on the new write quorum,
   including the candidate, before committing activation. Resume interrupted
   phases and isolate the old owner through repeated failovers.
4. Require node authentication on the older replication read, apply, checkpoint
   and stream handlers. These currently check general authentication; the new
   recovery-seal handler checks the node principal on each physical connection.

Automatic dispatch remains disabled until installation and activation can finish
safely. Linux tests cover local seals and interrupted checkpoint replacement;
other platforms need durable metadata support before these operations are enabled.

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

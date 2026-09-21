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

Evaluate a versioned canonical state digest after deterministic replay to an
explicit exclusive event frontier. Bind it to the resource incarnation, installed
lineage and required payload coverage/content identity. Equal state at that
boundary can support checkpoint comparison; different histories can converge to
the same state, so lineage remains part of the proof.

Begin with isolated replay of sealed evidence during recovery. Canonical encoding
must cover ready/settled state, inflight deadlines, retries, delayed work, TTLs,
pending dead letters and persisted policy, sorting unordered collections and
excluding local timing metadata and derived caches. Current snapshot bytes and
the limited canonical debug view are insufficient for this purpose. Streams need
their own complete state schema.

Measure replay time, hashing/sorting CPU, peak memory and recovery delay before
adding periodic live-owner captures. A live capture must return state and the
actual durable applied frontier under an explicit application fence across actor
priority lanes. Hash an immutable capture outside the actor; avoid adding a hash
wait to every publish/confirmation batch. Requests for an already-passed boundary
require a retained capture or replay, and a hash cannot retroactively establish
that an arbitrary live state represented the requested offset.

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

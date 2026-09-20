---
title: Failover plan
description: Planned promotion evidence, eager failure detection and acceptance scenarios.
---

This plan covers preservation of confirmed history during ownership changes and
an optional faster failure detector. The controller now persists pending recovery
requests and retains the previous assignment for changes involving replicated
confirmation. Broker-driven sealing, evidence collection, recovery and activation remain
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
Owner-only durability cannot guarantee recovery from another node. Transition
records and history identity require a concrete storage/protocol design before
this handshake can be considered implemented.

Pending requests retain the complete previous and proposed replica sets, policies,
the requested generation and the old recovery-witness threshold. They survive
metadata restart and appear under `consensus.controller.pending_recoveries` in
the admin topology response. Heartbeat changes cannot replace an outstanding
request, and ordinary followers keep their existing source. Existing healthy
owners can continue under their unchanged active assignment; requests do not
yet seal their logs or establish a recovery certificate.

## Local seal foundation

Keratin provides a local recovery seal with a durable transition identity and
fencing epoch. It drains accepted operations, preserves both logs and prevents
ordinary promotion, replay repair, snapshot replacement and compaction from
changing the sealed source. Identical requests can resume after interruption or
restart. The returned offsets describe local retained data, including potentially
unconfirmed or incomplete records; they do not establish authoritative history.

The primitive is validated on Linux and is not called by automatic broker
failover. Other platforms currently reject sealing before changing storage.
Authenticated request dispatch, retained data-history identity, compatible-history
selection, new-write-quorum installation and activation remain required. The completed
controller must coalesce retries per transition and resume work after restarts.
Local checkpoint interruption recovery is documented in
[implemented surface](/implemented-surface/#experimental-cluster-and-replication-surface);
it does not supply the coordinated history proof or activation decision.

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

---
title: Failover plan
description: Planned promotion evidence, eager failure detection and acceptance scenarios.
---

This plan covers the remaining rollout gates for queue recovery and an optional
faster failure detector. Implemented sealing, verified transfer, durable generation
installation, exact quorum activation and bounded recovery are documented in
[recovery sealing](/reliability/recovery-sealing/).

## Remaining recovery work

1. Complete ordinary initial-enrollment orchestration and safe recovery of an
   initial preparation whose owner process is replaced before activation. Existing
   queues need a verified baseline; local file absence cannot establish an empty
   origin.
2. Support replacement of an unavailable candidate during a persisted pending
   transition, preserving its witness proof and any completed stages. Extend
   process-isolation and membership-change tests through that handoff.
3. Add composite reconstruction for crossed payload/event tails and authoritative
   stream-state recovery. Keep unsupported or contradictory histories fenced.
4. Extend transfer support beyond the current 16 MiB record/page and snapshot
   bounds. Measure full-history rescans, checkpoint/hash CPU, peak memory and
   recovery latency on representative storage and replication configurations.
5. Add safe reclamation and separate disk accounting for retained generations and
   stages. Establish durable metadata support before enabling the installation
   protocol on non-Unix platforms.

The current worker applies to explicitly enrolled queue histories and retains the
configured confirmation threshold. Owner-only durability still requires its
surviving storage. Pending plans retry with bounded backoff when reachable evidence
is insufficient; authoritative divergence requires investigation.

## State digests at a recovery boundary

Exact queue checkpoint capture and bounded comparison from different checkpoint
starts are implemented in [recovery sealing](/reliability/recovery-sealing/#queue-state-at-an-exact-boundary).
New resource declarations have a consensus incarnation ID bound into pending
recovery transitions. An explicit local storage primitive persists incarnation,
history and writer-session IDs for pristine storage, and blocks ordinary access
after restart while retaining recovery sealing. It is not enabled by ordinary
broker creation. Explicit consensus preparation now fixes history/session IDs to
an owner instance and assignment, and local preparation leaves admission closed.
Explicit creation-time enrollment now withholds ordinary serving while preserving
placement. Prepared quorum receipts persist under the immutable decision and
require the owner and fixed write threshold. Authenticated remote preparation rechecks the exact decision through consensus.
Explicit initial activation now admits the exact prepared storage/process instances
and carries their identity on live replication. Version-two seals now bind retained
evidence to that accepted storage history and original replica instance; process
replacement requests recovery even without a placement change. Recovered activation
establishes the next exact accepted history and supports repeated recoveries.
Existing resources still need a verified baseline; the catalogue ID alone supplies
no history authority.
A trusted, quorum-installed checkpoint can replace older history; indefinite
retention of settled payloads is not required.

Owner-local leases have a separate comparison projection. Newly activated delayed
work now moves to ready through an ordered event carrying an explicit clock
boundary and bounded work count. Replay uses that recorded boundary and consumes
the same timers, preserving retry and TTL state. Older histories can lack these
transitions and still require baseline validation; complete stream-state proof
is also pending.

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

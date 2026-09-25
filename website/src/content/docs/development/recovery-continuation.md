---
title: Recovery continuation and owner handoff
description: Proposed resumable recovery phases, storage-health policy and cooperative ownership transfer.
---

This design note records proposed work. Resumable inspection, storage-health-driven
relinquishment and cooperative owner handoff are not implemented capabilities.
Early replication and speculative delivery remain separate unmerged experiments.
Current behavior is documented in [failover and recovery](/reliability/recovery-sealing/).
The [failover plan](/development/failover-plan/) tracks the wider acceptance work.

## Current foundation and gaps

Recovery already persists an immutable selected-source plan, resumes staged
transfers and can use a completed transferred source. Source inspection, replay
and pairwise comparison happen before that plan exists. Exceeding their work
budgets can therefore leave an otherwise valid history repeatedly fenced without
retaining useful inspection progress between attempts.

The queue confirmation path waits for owner completion and the configured number
of followers covering the payload and enqueue-event dependency. Confirmation
normally times out after five seconds. Follower worker errors are logged and
retried at the configured interval, normally 100 ms. These defaults are runtime
settings. The optional in-sync floor measures recent reports, which does not
establish that storage can persist new records. Its check occurs in the confirmation
path after local completion. A rejection, timeout or I/O error does not establish
that a message was never stored.

A general policy connecting persistent storage failures to owner relinquishment
or follower replacement remains to be designed. Process liveness, replication
progress and storage health need separate signals.

## Resumable recovery phases

Make recovery progress through a sequence of bounded phases:

1. Collect authorized seals and identify the exact histories under inspection.
2. Inspect, hash and replay chunks, saving verified progress.
3. Compare histories and select a source using complete evidence.
4. Persist the authoritative recovery plan, transfer and install its state.
5. Activate and admit the required exact replica instances.

Each phase needs a retry policy, work window and explicit completion condition.
Attempt deadlines should yield to a later continuation where safe. Memory, page
size, concurrency, staging disk and cumulative resource use still need budgets.
A work-window limit should not become a permanent maximum recoverable history size.
Resource exhaustion should expose the limiting resource and required intervention.
Contradictory evidence must remain a blocking failure.

### Progress and authority

A preliminary work journal records progress before source selection. It grants
no installation, membership or serving authority. Bind each continuation to the
resource incarnation, pending transition, exact sealed source identity, relevant
storage identity, algorithm version and verified offset boundaries. Recheck
current authority before resuming. A changed source or transition invalidates
incompatible progress.

Persist the data needed to resume the actual computation. An offset alone cannot
resume a hash or reconstruct partially replayed queue state. Define durable replay
artifacts and verifiable chunk commitments, or another validated continuation
format. Chunk snapshot transport and bound decoded state as well as input pages.
Retain final end-to-end verification before declaring a phase complete.

Specify atomic progress publication, restart behavior, source loss, candidate
replacement and safe cleanup. Keep large work artifacts out of coordination
metadata. Determine which compact receipts require consensus and which artifacts
can remain local and reconstructible. Cross-node reuse must verify their identity
and completeness.

### Acceptance

Cross the current byte, record, replay-operation and snapshot limits with valid
histories. Interrupt each phase before and after progress publication, then restart
the worker or replace the candidate. Verify continued progress without repeatedly
starting from zero, bounded resident memory and preservation of every confirmed
message. Corrupt or substitute a continuation and ensure it cannot supply recovery
authority. Record bytes reread, saved work, progress age and the precise stopping
reason in the recovery timeline.

## Storage-health policy

Classify transport interruption, temporary resource pressure, disk exhaustion,
permission failures, corruption and uncertain write/fsync outcomes separately.
Failed writes must not advance durable progress. A serious durability failure
should close affected writer admission immediately while its outcome is assessed.
The threshold for requesting relocation can differ from the threshold for stopping
unsafe writes.

Use bounded retries with backoff, rate-limited diagnostics and observable last
success, consecutive failures, lag and affected storage scope. Determine whether a
fault affects one partition, one device or the whole node. Keep unhealthy storage
ineligible for new ownership until repair and verified readmission. Define restart
behavior and cooldown so a repaired-looking process cannot immediately regain
ownership and repeat the failure.

A healthy majority can continue without a failed follower when the configured
confirmation policy permits it. A requirement for three durable copies still needs
three copies. Repair or replace the failed replica through verified learner
catch-up and authoritative membership changes. Preserve acknowledged history and
the configured requirement throughout the transition. A requested policy change
needs its own safe application boundary and cannot retroactively authorize an
in-flight publish under weaker requirements.

Expose whether progress is limited by missing quorum, a storage fault, resource
pressure or invalid evidence. If all copies share a failing device, or no eligible
replacement has sufficient evidence, relocation cannot restore availability.

## Cooperative owner handoff

The queue owner can recommend a successor to the metadata controller. The owner
knows follower progress, while the controller owns assignment decisions. A proposal
should carry evidence for a specific handoff boundary. A recent heartbeat or a
large reported offset alone cannot authorize the successor.

The intended sequence is:

1. Prepare a candidate while the current owner continues serving.
2. Close admission for state-changing work and account for already accepted
   publishes, settlements, timer transitions and other mutations. Establish a
   final payload/event boundary with complete dependencies.
3. Obtain fresh evidence that the candidate and required replica set durably cover
   the handoff boundary and the accepted history. Fix how uncertain completions
   participate in that proof.
4. Commit the transition through coordination, fence the old writer and activate
   the verified replacement with the required exact-instance admission checks.
5. Keep the former owner as a follower or quarantine it according to storage health.

This is a proposed optimization of the proof path. Planned maintenance and load
rebalancing are the best initial cases because the owner can usually finish writes
and provide a clean final boundary. A slow but functioning owner may do the same.
A failed write/fsync can make its own final boundary uncertain, requiring ordinary
recovery from surviving evidence. A dead or isolated owner uses crash recovery.

Potential savings are failure-detection delay and some history discovery or
comparison work. Measure the final write pause and restored publish/delivery
latency separately. There is no latency claim until a prototype is validated.

### Interrupted handoffs

Use an idempotent handoff identity bound to the resource, epoch, owner instance,
candidate and boundary. Define each durable transition and whether cancellation
is still legal there. Test lost requests and replies, owner/candidate/controller
restart, missing metadata quorum, concurrent placement changes and a candidate
that fails while catching up. The old owner must never resume writes under stale
authority, even if it missed the activation response or restarted.

A failed fast path should fall back to the existing recovery protocol while
preserving history and fencing. Verify that the fallback actually accepts every
new intermediate state. Do not treat that compatibility as automatic. The protocol
must preserve the old confirmation contract and install the new write quorum.
Changing ownership alone cannot satisfy an all-replicas policy with a failed copy.

## Early replication under owner I/O failure

A follower may persist payloads and enqueue events before the owner's fsync
completes. If the owner then reports a storage error, follower success cannot
replace the owner completion required by the existing durable confirmation
contract. Ordinary delivery must retain its durability gate. Speculative delivery
needs its separate explicit contract.

The owner may request relinquishment, but this does not prove whether the uncertain
write persisted or authorize deleting its follower copies. Specify permissible
outcomes for an unconfirmed suffix. Recovery must preserve all previously confirmed
records, validate lineage and dependencies, and prevent conflicting offset reuse.
Client errors remain potentially ambiguous, so retries can produce duplicates.

A native experimental screen covers 48 schedules across segment write errors,
partial writes, fsync errors and abrupt process exit. It exercises offset zero and
one confirmed record, both log completion orders, and follower persistence before
or after an owner error. Failed publishes did not produce success receipts or
become owner-deliverable, partial records stayed out of the tentative cache, and
confirmed prefixes survived reopen. Hooks inject file-operation errors through
real writer paths and remain outside ordinary builds.

Complete unconfirmed bytes can become ready after an event-log fsync error and
reopen. The payload-error path appends a compensating cancellation. These outcomes
require callers to treat an error receipt as ambiguous.

Before adoption, extend this screen through admitted multi-node recovery and
old-owner rejoin under majority and all-copy confirmation. Verify retry cursors,
recovery source selection and divergent unconfirmed suffixes. Add index, manifest,
rollover and directory-sync failures. Owner relinquishment, unhealthy successors,
lost replies and missing metadata quorum need their own schedules. The native
screen does not establish these cluster outcomes or power-loss persistence.

## Implementation order and open decisions

1. Extend the native storage-failure screen to broker confirmation policies,
   recovery source selection and old-owner rejoin.
2. Define typed failure/progress states and expose them through recovery diagnostics.
3. Implement resumable inspection and replay incrementally, then comparison and
   chunked snapshots. Validate restart at every durable boundary.
4. Design the cooperative handoff proof and implement planned handoff first.
5. Add fault-triggered relinquishment and follower replacement using the validated
   health classification, handoff and recovery paths.

Decide how long transient faults may retry, which failures require manual repair,
how node eligibility survives restart, and which settings are operator-configurable.
Define final-boundary evidence, unconfirmed-suffix handling, metadata record lifetime
and cleanup ownership before selecting a wire format. Keep the broader partition,
stream recovery, power-loss and membership gates in the failover plan.

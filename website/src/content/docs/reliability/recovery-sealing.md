---
title: Recovery sealing
description: Explicit recovery authorization, durable local evidence and remaining activation gates.
---

Recovery sealing freezes a replica's retained records for inspection. The broker
has an explicit internal receiver; the placement controller does not dispatch
seals automatically. Replicated reassignment remains paused at the pending
recovery barrier until compatible-history selection, installation and activation
are implemented.

## Request authority

The internal `RecoverySeal` request (opcode 88) carries the queue or stream
identity, a digest of the complete pending transition and its proposed fencing
epoch. The receiver requires the authenticated `@node` principal on the current
physical transport, including when ordinary client authentication is disabled.
Resuming a session does not inherit this control privilege. An approved client
certificate or a fresh node authentication can establish it.

Ganglion verifies the previous active assignment, old replica membership, old and
new confirmation requirements, witness threshold and exact transition digest.
A same-value compare-and-set through consensus supplies a fresh committed view
without changing the metadata generation. A disconnected replica cannot authorize
sealing solely from stale local metadata. Standalone ownership providers reject
this operation. Noncanonical group aliases are rejected.

One seal operation runs per broker. Identical concurrent requests share its
result; other requests receive a busy error and can retry. Authorization times
out after ten seconds. Admitted storage work survives caller cancellation, and
identical retries retain the original seal. There is no public unseal operation.

`RecoverySealOk` (opcode 89) includes the local replica identity supplied by
coordination, transition digest, fence, retained bounds and content fingerprints.
The codecs use `RSL1` and `RSO1` version markers and reject trailing or truncated
data. These internal messages are experimental and require matching broker
revisions. Ordinary client frames are unchanged.

## Resource incarnation

New catalogue declarations receive a versioned incarnation ID in the same
consensus command that registers the queue or stream. Concurrent declarations
retain the first ID. Deletion removes the ID with the catalogue entry and checks
the observed value, so a delayed deletion cannot remove an identified replacement.
Recreation after the old assignment is retired receives a new ID.

Existing catalogue entries and resources with retiring assignments receive no
new ID. Their origin remains unverified. Pending recovery transitions include an
available incarnation ID in their digest; a changed, missing or malformed ID
invalidates seal authorization. Legacy transitions without an ID retain their
previous encoding.

This ID identifies a catalogue lifetime. Source selection also requires accepted
history, writer-session authority and an authoritative checkpoint. The local
storage primitive below supplies an explicit binding; automatic enrollment,
recovery readmission and legacy baseline establishment remain pending. The new consensus commands require matching
metadata-node binaries; mixed-version rollout is not supported for this change.

## Local storage history binding

The explicit `initialize_empty_storage_history` library operation binds new local
storage to a resource incarnation, accepted-history ID and writer-session ID.
The caller must first obtain an authorized initial-history decision from
coordination. Ordinary broker creation does not invoke this operation yet.

Initialization requires previously nonexistent partition directories and takes
both log locks before persisting a checksummed receipt. The receipt and directory
entries are synchronized before local admission is published. Identical retries
within the same storage instance are accepted; conflicting IDs and existing
storage, including empty legacy logs, are refused. Admitted work survives caller
cancellation.

A private storage-instance token prevents a reopened store from inheriting writer
permission. Ordinary access fails with `HistoryAdmissionRequired` before log
opening or checkpoint recovery, including at the same assignment epoch. Explicit
recovery sealing can still read and fence the retained logs. Ordinary checkpoint
replacement of bound storage is blocked pending an installation protocol that
carries the selected history and its authority.

This primitive covers local binding and restart admission. The initial preparation
protocol below allocates history/session IDs through consensus. Persisted quorum
installation, activation, legacy baseline validation and recovery readmission
remain pending. Loss of the entire binding
and log directory must be handled by that consensus protocol; local file absence
cannot prove a new resource. Unbound resources retain their existing behavior.
Linux tests include a SIGKILL restart; they do not simulate power loss. Older
binaries do not enforce this receipt, so bound stores require compatible binaries.

## Initial history preparation

`register_initial_history_resource` explicitly enrolls a new queue or stream
partition at catalogue creation. Its version-two incarnation is recorded in the
same consensus operation. Ordinary owner, follower and client-topology projections
withhold enrolled assignments; the placement controller retains them for replica
preparation. Existing or retiring resources cannot acquire a fresh origin from
empty local directories. Ordinary declarations retain their existing behavior.

Enrolled deletion atomically retains a retired incarnation marker, keeping the
assignment out of serving until placement removes it. Recreation after retirement
gets a new ID, and a stale conditional deletion cannot erase the replacement.
Malformed incarnation metadata also withholds the affected serving route.

Explicit coordination APIs persist an immutable `Preparing` decision for an
enrolled incarnation and its current assignment. The decision fixes the owner
instance, history ID, writer-session ID and required write count. Repeated calls
from that owner instance retain the committed IDs. A replacement provider using
the same node name and assignment epoch must enter recovery readmission.

Before preparing local storage, a generation-and-attribute guarded consensus
update rechecks the decision. This merges one attribute and preserves advisory
heartbeat updates. Assigned replicas can then persist an empty local baseline
without opening ordinary writer admission. Preparation receipts identify the
exact decision, reporting node, provider instance and storage instance. Existing
state, snapshots, admitted histories and sealed replicas cannot be recertified
as empty preparations.

The explicit receipt collector admits reports from the contacted replica, counts
identical retries once, requires the owner alongside the write-count threshold,
and blocks on contradictory process or storage-instance reports. The original
owner can persist the exact prepared quorum through a generation-and-attribute
guarded consensus update. Identical retries preserve the record; conflicting
receipts cannot overwrite it. The record survives metadata restart and remains
historical evidence: activation still needs fresh authority for the recorded
processes and storage instances. Prepared quorum evidence leaves serving and
storage admission closed.

These operations are not used by ordinary broker startup. Authenticated remote
preparation, writer activation, identity on live replication, recovery installation
and readmission remain pending. Existing resources require a verified baseline.
Matching broker and metadata binaries are required; older brokers do not enforce
the enrollment projection and the retirement command requires updated metadata
nodes. Mixed-version enrollment and downgrade are unsupported.

## Explicit witness collection

The explicit transport opens a fresh authenticated connection to the selected
replica, checks its returned identity against that target, and enforces a deadline
covering connection setup and the reply. Timeout or cancellation can leave an
admitted remote seal running; the caller can retry the same command.

A transition-bound collection admits at most one report per previous replica.
Identical retries count once. Stale transitions, wrong identities, unsupported
history versions and inconsistent bounds are refused. The previous replica set
and its witness threshold remain fixed even when the proposed configuration is
smaller. Contradictory replies from the same sealed replica block the collection
and produce a payload-free error log.

Reaching the old seal threshold yields `AwaitingHistoryValidation`. Different
fingerprints remain available for comparison; equal fingerprints also require
dependency and authority checks. The collection exposes no activation operation.
Reports are currently held in memory and must be recollected after restart from
the durable seals. Ordinary metadata changes preserve a matching transition;
replacement or removal of that transition invalidates the collection when checked
against the updated snapshot. An in-memory snapshot check alone supplies no fresh
consensus authority for activation.

These are explicit library operations. Automatic fan-out, retry scheduling,
automatic source selection and selected-history installation are not enabled.

## Retained content identity

After draining accepted work and fencing both logs, storage computes a versioned
BLAKE3 identity over the resource, retained bounds, message records, event records
and snapshot bytes. Record offsets, flags, headers and payloads participate;
replica-local append timestamps and promise epochs are excluded. Diagnostics
contain identities and offsets without message payloads.

A checksummed `recovery.history` receipt binds that identity to the seal request.
The file and its directory entries are synchronized before returning evidence.
Identical retries scan the frozen records from disk and compare the receipt.
Changed or corrupt data withholds evidence and leaves the source sealed. Older
seals without a receipt acquire one when explicitly retried.

The fingerprint establishes equality of exact retained contents. It does not
establish ancestry, a confirmed prefix, compatibility across compaction boundaries
or authority to promote. Replicas can have different fingerprints while sharing
a valid history, for example when their retained ranges or snapshots differ.
Future witness selection must prove those relationships separately.

## Explicit sealed-source reads

`RecoveryRead` (105) and `RecoveryReadOk` (106) expose message records, event
records and raw snapshot-envelope bytes from an already completed seal. The
`RRD1` and `RRO1` codecs bind pages to the exact transition, fence and retained
history ID. The receiver requires node authentication and fresh consensus
authorization of the pending transition; the requester checks replica identity,
contiguous offsets, retained bounds, progress and page budgets under a whole-call
deadline. Ordinary clients cannot access these controls.

Storage reads the files directly without opening ordinary queue actors or writer
threads. Cold reads acquire both existing log locks; live reads retain the frozen
log owners. Lifecycle, snapshot and admission guards remain held until disk work
finishes, including after caller cancellation. Missing receipts, stale requests,
pending checkpoint installation, corruption and changed content withhold the page
and leave the source sealed. Reads do not create receipts, repair files or unseal
replicas.

Every page verifies both complete retained logs and the complete snapshot against
the receipt, capturing its returned bytes during that scan. CRCs and contiguous
record offsets are checked without cache fallback or record resynchronization.
Queue, stream, empty-range, nonzero-head, cold-restart, cancellation and real TCP
authorization tests cover this path. The data is available for subsequent history
comparison; reads do not establish ancestry, dependency completeness or promotion
authority.

## Explicit retained-history inspection

`inspect_recovery_pair` reads two distinct sealed witnesses through authenticated
recovery control. It compares canonical records at shared message and event
offsets, tolerates different page boundaries and retained ranges, and verifies
each complete transferred log against its sealed digest. Results distinguish
matching overlap, the first divergent offset with content IDs, and no shared
records. Empty overlap supplies no compatibility evidence. Inspection retains at
most one page of unmatched record IDs and produces payload-free diagnostics.

Event-reference inspection checks all offsets in a recognized event, including
whole enqueue batches and event zero. It reports references outside that replica's
retained payload range and marks cancellation, reset, embedded snapshot, unknown
encoding and stream-state cases for further interpretation. A reference below the
retained head can be legitimate compaction; a later cancellation can eliminate a
missing enqueue. These findings describe unresolved dependencies and do not
establish corruption or authorize deleting an event suffix.

Every result lists remaining common-origin/installed-lineage and state/dependency
proofs. Nonzero retained heads and snapshot receipts add explicit compaction and
checkpoint requirements. The basic inspection reports snapshot digests; the checkpoint replay variant
also interprets exact snapshot state. The inspector never combines independent maximum log tails,
chooses a source, installs state or activates an owner.

Default inspection limits are 256 records and 1 MiB per page, 1,024 pages,
1,000,000 records and 256 MiB of canonical record bytes across both replicas.
Callers supply a whole-operation deadline. Budget exhaustion, malformed pages,
transferred-digest mismatch, timeout and source loss return an error without a
partial success report. Dependency decoding accepts at most 65,536 entries in an
event batch; larger or unfamiliar encodings leave an explicit semantic gap.
These limits bound one inspection, while aggregate scheduling remains future
work. Source reads still rescan retained data per page.

## Queue state at an exact boundary

`inspect_recovery_pair_with_queue_replay` optionally reconstructs both queues at
one common, exclusive `event_next` boundary, including the empty boundary zero.
It applies sealed events in log order to isolated state and verifies both complete
transferred logs, including records after that boundary. It requires message and
event histories retained from zero; compacted origins need a proven checkpoint.
Existing snapshot bytes are not treated as an authoritative baseline.

The versioned canonical digest includes ready and settled ranges, inflight
deadlines, delayed enqueues and retries, retry counts, TTL deadlines, pending DLQ
targets and persisted policy. Unordered collections are sorted and delayed-entry
multiplicity is preserved. Snapshot timestamps, wakeup objects and derived
expiry caches are excluded. Reports carry the actual replay boundary, required
payload frontier, event-prefix digest and verified message-history identity.
Different bodies can produce equal settled state, so state equality still requires
payload and lineage proof.

An enqueue referencing a missing payload must be cancelled by the selected
boundary. Missing non-enqueue dependencies, unsupported reset/snapshot/stream
semantics, unknown encoding, offset overflow and exhausted budgets fail the
replay. The default semantic-operation budget is one million per replica,
counting every batch entry. Hashing/replay runs on blocking workers with two
process-wide CPU slots held through caller cancellation; ordinary publishing and
actor scheduling are unchanged. Automatic source selection remains pending.

`inspect_recovery_pair_with_checkpoints` also accepts independently captured
version-two queue snapshots. It downloads and verifies each raw envelope against
its resource-bound seal, checks its exclusive boundary and state metadata, then
replays the retained suffix to a common target. Legacy inclusive checkpoints are
not accepted as exact replay evidence. The configured snapshot cap is at most
16 MiB per replica, and decoded blob bytes count against the replay work budget.

After reconstruction it streams the retained message records and hashes the
exact offsets, headers and bodies of live messages. Missing live payloads fail
inspection. Different retention of settled payloads can therefore produce equal
live-payload evidence despite different complete-log digests. Snapshot bytes and
input suffix identities remain in the report.

The report includes both the complete state digest and a projection that releases
owner-local leases to ready state. Retry counts, delays, TTL and pending DLQ state
remain significant. Timer-driven transitions can still cause legitimate state
differences and need further interpretation. Snapshot equality and live-payload
equality supply comparison evidence; resource incarnation, accepted recovery
history and quorum installation remain prerequisites for automatic activation.

## Cost and limits

Hashing scans all retained records on each explicit seal or completed retry in
chunks of 64 records. Each sealed-read page also scans all retained data, so
transferring many pages currently repeats that I/O. Returned pages are capped at
16 MiB and 4,096 records; log budgets include 18 bytes of wire metadata per record.
The strict disk reader caps a single record allocation at 64 MiB, snapshot reads
use 64 KiB chunks, and recovery metadata is capped at 64 KiB. Segment enumeration
uses memory proportional to segment count. One sealed-read operation per storage
instance is admitted at a time; concurrent calls receive a retryable busy error.
A record that cannot fit the requested page is refused rather than split.

There is no per-message hashing or normal-traffic metadata round introduced by
this path. Recovery I/O and latency are not benchmarked. The page limits govern
this recovery API's output and disk-read allocations. The shared frame decoder
rejects oversized recovery-read requests and replies from their frame headers,
before accumulating their bodies.

Linux storage, fault, cancellation and real TCP authorization tests cover this
increment. Non-Unix sealing rejects before mutation pending durable metadata
support. Process tests do not establish hardware power-loss behavior.

The cluster currently uses a shared node credential and trusts its peers. The
returned replica ID is coordinator-derived, rather than a cryptographic identity
proof against a malicious peer. Replication read, apply, checkpoint and streaming controls require the same
node authentication on the current transport. Remaining work is tracked in the
[failover plan](/development/failover-plan/).

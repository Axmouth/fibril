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
source comparison and recovery transfer are not enabled.

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

## Cost and limits

Hashing scans all retained records on each explicit seal or completed retry in
chunks of 64 records. There is no per-message hashing or normal-traffic metadata
round introduced by this path. Recovery I/O and latency are not benchmarked.

Linux storage, fault, cancellation and real TCP authorization tests cover this
increment. Non-Unix sealing rejects before mutation pending durable metadata
support. Process tests do not establish hardware power-loss behavior.

The cluster currently uses a shared node credential and trusts its peers. The
returned replica ID is coordinator-derived, rather than a cryptographic identity
proof against a malicious peer. Replication read, apply, checkpoint and streaming controls require the same
node authentication on the current transport. Remaining work is tracked in the
[failover plan](/development/failover-plan/).

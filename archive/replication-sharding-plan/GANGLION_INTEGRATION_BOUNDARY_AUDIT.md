# Ganglion Integration Boundary Audit

This audit records where the Fibril and Ganglion boundary currently sits, what
should stay Fibril-specific, and what may become generic Ganglion code later.

## Goal

Ganglion should stay a neutral coordination substrate. Fibril should use it for
replicated metadata and ownership decisions without teaching Ganglion about
queues, brokers, message logs, runtime settings schemas, DLQ behavior, or client
protocol routing.

## Current Boundary

### Fibril-Owned

These concepts are domain behavior and should stay in Fibril:

- queue identity as topic, partition, optional group
- broker ownership errors and routing behavior
- Stroma/Keratin role transitions and replication safety checks
- runtime settings schema and validation
- queue partitioning declarations and repartitioning semantics
- exclusive consumer group membership and assignment meaning
- client-facing topology response shape
- admin operator UX and API error contracts
- sparse queue, DLQ, message inspection, and broker metrics behavior

### Ganglion-Owned

These concepts are generic coordination mechanics and should stay in Ganglion:

- resource identities with namespace, name, partition, optional group
- node metadata and labels
- committed coordination snapshots
- owner/follower assignments and epoch fencing
- resource catalogue state
- opaque attribute storage with guarded updates
- membership and Raft transport
- committed snapshot watches
- generic placement primitives
- topology and coordination storage telemetry

### Adapter-Owned

`crates/coordination-ganglion` is currently the Fibril adapter over Ganglion. It
is allowed to depend on both Fibril and Ganglion because its job is translating
between the two models.

Adapter responsibilities that look correct:

- mapping `QueueIdentity` to `ganglion_core::ResourceIdentity`
- mapping Fibril coordination snapshots to Ganglion snapshots and back
- preserving Ganglion fields that Fibril planners do not own
- encoding Fibril runtime settings as an opaque Ganglion attribute
- encoding queue partitioning as an opaque Ganglion attribute
- shaping Ganglion assignments into Fibril client topology
- publishing Fibril cohort membership and assignment documents as attributes
- adding Fibril-specific liveness labels such as applied replication tails

## Findings

### 1. Runtime Settings Are Correctly Fibril-Owned

Status: Addressed

The runtime settings document is stored in Ganglion as an opaque attribute, but
the schema and validation stay in Fibril. This is the right split: Ganglion
should offer durable replicated attributes, not understand broker settings.

Follow-up:

- If cluster-wide immutable runtime policy is added later, it should be another
  Fibril-owned replicated document unless the policy model proves generic.

### 2. Queue Partitioning Is Fibril-Owned

Status: Addressed

Queue partition counts and future repartitioning semantics are broker behavior.
Ganglion stores the document and assigns generic resources, but it should not own
the meaning of partition counts or live queue migration.

Follow-up:

- Keep repartitioning design in Fibril/Stroma/Broker docs.
- Only generic resource assignment primitives should move to Ganglion.

### 3. Forwarded Writes And CAS Retry Helpers Are Generic Candidates

Status: Audited

`coordination-ganglion` currently owns leader-or-forwarded command submission and
repeated guarded-attribute update loops. The mechanics are not Fibril-specific.

Do not move this immediately. The current methods still carry adapter details
such as Fibril error mapping and document validation. A good future extraction
would look like a Ganglion helper for:

- submit locally if leader, otherwise forward to the current leader
- compare-and-set an opaque attribute with bounded retries
- report stale actual value cleanly

Follow-up:

- Move only after the helper shape is used by at least two Fibril documents or a
  second consumer.

### 4. Heartbeat And Catalogue Sync Are Mixed

Status: Audited

The high-level pattern is generic: periodically publish node labels and register
local resources. The current implementation is still Fibril-shaped because label
contents include broker applied tails and exclusive group membership, and local
queue discovery comes from Fibril storage.

Follow-up:

- Keep current code in the adapter.
- Later, Ganglion may provide generic loop scaffolding that accepts callbacks for
  labels and resource discovery.

### 5. Controller Loop Shape Is Generic, Planner Input Is Fibril-Facing

Status: Audited

Ganglion already has generic placement primitives. Fibril still keeps a
Fibril-facing coordination trait and planner model so the broker does not depend
directly on Ganglion as its only coordination backend.

That duplication is acceptable for now. Removing it too early could couple the
broker to Ganglion and make alternate backends harder.

Follow-up:

- Keep the Fibril coordination trait as the broker-facing abstraction.
- Revisit planner dedupe only after the Ganglion provider API is stable and the
  broker coordination trait has stopped moving.

### 6. The Adapter File Is Too Large, But The Boundary Is Reasonable

Status: Audited

`crates/coordination-ganglion/src/lib.rs` contains mapping, runtime settings,
partitioning, cohort attributes, heartbeat/catalogue loops, controller logic,
and tests. This is a maintainability issue, not a boundary issue.

Follow-up:

- Split into modules after the cluster path settles:
  - `mapping`
  - `attributes`
  - `runtime_settings`
  - `partitioning`
  - `cohorts`
  - `controller`
  - `provider`
  - `tests`

## Current Recommendation

Do not dedupe policy code yet.

The next safe work is to keep the boundary documented while continuing feature
hardening. Move code into Ganglion only when it is clearly domain-neutral,
stable, and useful outside Fibril.

## Regression Coverage

Existing relevant coverage:

- Ganglion provider contract test exercises the same Fibril coordination
  contract as static coordination.
- Runtime settings through Ganglion tests cover replicated settings documents,
  stale version conflicts, invalid documents, and invalid settings rejection.
- Single-node guardrail tests cover local settings behavior without a Ganglion
  cluster store.
- Ganglion surface inventory documents generic coordination behavior and
  consumer-owned boundaries.

No new code test is needed for this audit pass because the findings are boundary
classification and future extraction guidance rather than behavior changes.

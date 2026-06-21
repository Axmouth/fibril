# User-Facing Documentation Shape Audit

Status: Audited

Date: 2026-06-15

Scope: docs under `website/src/content/docs/latest`, with focus on whether
normal reader paths explain behavior and operations rather than implementation
mechanics.

## Summary

The docs now have a mostly workable split:

- concepts and reliability pages explain queue behavior
- configuration describes startup config vs runtime settings clearly
- admin dashboard docs are operator-oriented
- development docs hold most of the internals
- implemented surface works as a detailed reverse roadmap

The biggest remaining issue is clustered operation. Partitioning, ownership,
replication, failover, replica-durable confirms, and exclusive consumer groups
are real enough to mention, but the user-facing story is still scattered across
status, roadmap, implemented surface, consumer groups, and development notes.

## Findings

### 1. Clustered operation needs one user-facing explainer

Severity: High for discoverability.

Current cluster-related behavior is documented in fragments:

- `status` says partition ownership and replication are experimental.
- `roadmap` lists recently landed coordination and replication pieces.
- `implemented-surface` gives detailed inventory.
- `consumer-groups` explains the exclusive consumption model.
- development pages explain design policy and internals.

That is enough for maintainers, but not enough for an operator or application
developer asking:

- What problem does clustering solve today?
- What is safe to try?
- What config is required?
- What happens when an owner dies?
- What does replica-durable confirm mean operationally?
- Which client can use it?
- What is still not production-ready?

Recommendation:

- Add a user-facing page, likely under `concepts` or a new `operations` section:
  `Clustered queues and replication`.
- Use this shape:
  1. why this exists: spread partitions and survive a broker loss
  2. model: one owner serves a partition, followers keep durable copies
  3. client routing: clients talk to current owners, redirects refresh stale views
  4. durability: local durable vs replica-durable confirms
  5. failure behavior: promotion, stale-owner fencing, current limits
  6. operational state: topology page, CLI, logs, metrics
  7. current caveats: experimental branch, TS parity not ready, no live repartitioning

Keep implementation names such as Ganglion, epochs, and ISR out of the main
explanation unless they are directly shown in an operator screen.

### 2. Client docs need to keep parity visible

Severity: High until TypeScript catches up with the branch.

The client usage page says Rust and TypeScript cover the same core broker
surface, which is true for the single-owner queue API. It does not make the
cluster/cohort gap visible early enough.

The TypeScript client is not updated for the large topology/cohort branch yet.
That is a valid implementation choice, but the docs should make it explicit
where a reader expects it.

This audit added a short note near the top of `clients.mdx`.

Recommendation:

- Keep examples for both clients where they are actually equivalent.
- Avoid adding partial TS cluster examples until the TS client mirrors the Rust
  routing logic.

### 3. Status and roadmap are good entry points, but the status language should stay user-facing

Severity: Medium.

The status page is the right quick answer for feature maturity. It should avoid
terms that require implementation knowledge unless the term is already part of
the user contract.

This audit cleaned up a few examples:

- `Ganglion-backed coordination` became embedded coordination.
- `ISR checks` became in-sync checks.
- raw log wording in durable queues became durable message storage and queue
  state.

Recommendation:

- Keep future status rows in user terms first.
- Put component names in implemented surface or development docs.
- Mention the component only when the user must configure or operate it.

### 4. Reliability pages are mostly user-shaped now

Severity: Low.

`core-model`, `retries-delays`, `reconnects`, and `semantics` mostly explain
what the user can rely on. This audit removed direct references to storage
components from the normal retry and semantics paths.

Recommendation:

- Keep reliability pages behavior-first.
- Link to development docs for storage/recovery internals only when a reader
  needs the mechanism.
- Add more failure examples only if they help an application author decide how
  to handle duplicate delivery, reconnect, or delayed retry.

### 5. Admin docs are operator-facing, but diagnostics should stay clearly secondary

Severity: Low.

The admin dashboard page has the right framing: use the dashboard for specific
operational questions, not high-frequency monitoring. The diagnostics page can
show lower-level terms, but the overview should stay curated.

This audit reworded the overview and diagnostics text so it talks about storage
and queue health instead of queue actors and component names.

Recommendation:

- Keep the overview page focused on decisions: is the broker healthy, are queues
  active, is cleanup working, are reconnects happening, is replication lagging.
- Keep lower-level timing/counter terms on diagnostics pages, with short labels
  that make sense in the UI.

### 6. Implemented surface is useful, but should not become the main docs

Severity: Medium.

`implemented-surface` is intentionally detailed. It answers "is this wired and
where?". That is valuable, but it is not a replacement for user docs. It already
contains internal terms because it is partly a maintainer inventory.

Recommendation:

- Keep `implemented-surface` as a reverse roadmap.
- Link to it from user pages for precise status.
- Do not make readers go there to understand the basic behavior of clustering,
  DLQ replay, runtime settings, or reconnects.

## Quick Fixes Done In This Pass

- Removed internal storage/component names from admin overview wording.
- Reworded reliability semantics around durable records instead of Keratin.
- Reworded retry/delay behavior around broker-visible effects instead of
  Stroma internals.
- Reworded status rows for durable queues, ownership, and replication.
- Reworded the benchmark interpretation to avoid implementation jargon.
- Expanded `development/docs-writing.md` with section-specific guidance.
- Added a TypeScript cluster/cohort parity note to the client usage page.

## Recommended Next Work

1. Add the clustered queues and replication user-facing explainer.
2. Keep TypeScript examples limited to the currently implemented single-owner
   queue API until the cohesive parity pass lands.
3. Audit `implemented-surface` separately after the cluster explainer lands, so
   it can link to user docs instead of carrying all context itself.
4. Add a short "operator workflow" page later if topology, replication lag, and
   failover controls keep growing.

# Runtime Settings Ganglion Audit

This tracks the review findings for Fibril runtime settings replicated through
Ganglion. The current findings are addressed. Keep this file as the rationale
for the authority model and regression coverage.

## Goal

Runtime settings should have one clear source of truth in clustered mode.

- In standalone mode, the local Stroma settings store remains authoritative.
- In Ganglion mode, the Ganglion cluster document should be authoritative.
- Boot locks are standalone-only for runtime settings. In Ganglion mode, the
  cluster document is the runtime settings authority.
- Admin writes should either commit cluster-wide or report that they did not.
- Version conflicts should be meaningful across the active authority.

## Audit Items

### 1. Admin Update Reports Local Success Before Cluster Commit

Status: Addressed

Severity: High

Current behavior:

- `crates/admin/src/routes.rs` updates the local `RuntimeSettingsManager`.
- It then sends the effective settings to a background publisher hook.
- `crates/fibril/bin/server.rs` logs publish failure and drops the update.

Impact:

An admin update can return `200 OK` and affect only one broker. If the Ganglion
publish fails because there is no leader, a transient transport problem, or a
CAS retry failure, the sync loop does not repair the missed publish because the
cluster document was never updated.

Preferred fix:

In Ganglion mode, make the admin write path commit through Ganglion first. Only
return success once the cluster document is committed. The local manager can then
be updated immediately from the committed document or by the normal sync loop.

Fallback fix:

Persist pending cluster publishes with retry and expose a pending state to
operators. This is more complex and less intuitive than cluster-first writes.

Tests to add:

- Admin update fails or reports pending when the Ganglion publish cannot commit.
- A successful admin update on one broker appears on another broker.
- Two brokers racing updates produce a real conflict or deterministic cluster
  ordering.

Resolution:

- Ganglion-mode admin writes use a cluster settings store.
- Successful writes return only after the cluster document is committed.
- The local runtime settings manager is updated from the committed document.
- Cluster conflicts are reported from the cluster document version.

### 2. Boot Locks Must Not Be Used For Cluster Runtime Settings

Status: Addressed

Severity: High

Current behavior:

- Runtime settings locks are node-local boot configuration.
- Cluster runtime settings are replicated through Ganglion.

Impact:

A node-local lock is not a safe cluster authority. If the leader or the receiving
admin endpoint rejects or rewrites settings based on its own boot locks, cluster
behavior depends on which node handled the request.

Preferred fix:

Reject runtime locks when Ganglion coordination is enabled. If cluster-wide
immutability is needed later, make it a replicated cluster policy document rather
than a node-local boot lock.

Tests to add:

- Config validation rejects `runtime_locks` with Ganglion mode.
- Standalone mode still supports runtime locks.

Resolution:

- Startup config validation rejects `runtime_locks` when Ganglion coordination
  is enabled.
- Standalone mode keeps local runtime locks.
- Cluster-wide immutable runtime policy is deferred as a separate replicated
  policy question.

### 3. Runtime Settings Validation Is Split

Status: Addressed

Severity: Medium

Current behavior:

- `RuntimeSettings::validate()` checks only `delivery.expiry_batch_max` and
  `idle_queue_cleanup.sweep_interval_ms`.
- Startup config validation checks extra runtime fields such as replication poll
  intervals, ISR settings, and default partition count.

Impact:

Direct runtime updates can store values that startup config would reject. In
Ganglion mode, invalid settings can become cluster-wide.

Preferred fix:

Move every runtime invariant into `RuntimeSettings::validate()`. Startup config
validation should call that method for `runtime_seed` instead of duplicating the
rules.

Tests to add:

- Runtime update rejects zero replication poll intervals.
- Runtime update rejects zero `min_in_sync_replicas`.
- Runtime update rejects zero default partition count.
- Startup config still rejects the same invalid values.

Resolution:

- `RuntimeSettings::validate()` now owns runtime invariants.
- Startup config validation checks the same runtime seed rules.
- Runtime update tests cover the replication and partitioning zero-value cases.

### 4. Cluster Publish Boundary Does Not Validate Settings

Status: Addressed

Severity: Medium

Current behavior:

`GanglionCoordinationProvider::publish_runtime_settings` serializes and publishes
settings without validating them.

Impact:

The public publishing boundary can accept an invalid document if a caller bypasses
the normal admin update path.

Preferred fix:

Validate settings inside `publish_runtime_settings`, or make the unchecked method
private and expose a checked method. Invalid settings should map to a clear
coordination error.

Tests to add:

- Direct cluster publish rejects invalid settings.

Resolution:

- Ganglion runtime settings publish/update paths validate before writing the
  cluster document.
- Invalid runtime settings map to a coordination config error.

### 5. Corrupt Cluster Runtime Settings Attribute Is Silently Ignored

Status: Addressed

Severity: Low

Current behavior:

The Ganglion sync task parses the runtime settings attribute with `.ok()` and
treats bad JSON as no document.

Impact:

Cluster metadata corruption or an incompatible document shape can go unnoticed on
nodes that are only syncing.

Preferred fix:

Log the parse failure with enough context and avoid noisy repeated logs. Later,
surface this through admin health or coordination diagnostics.

Tests to add:

- Bad cluster runtime settings attribute does not crash sync.
- Bad cluster runtime settings attribute is observable through a log or health
  surface.

Resolution:

- Direct runtime settings document reads now return an error for malformed
  cluster metadata.
- The sync task logs malformed documents without repeatedly logging the same bad
  value.
- A broader health surface remains a later observability improvement, not a
  blocker for the authority fix.

### 6. Expected Version Is Local, Not Cluster-Wide

Status: Addressed

Severity: Medium

Current behavior:

The admin API uses the local Stroma settings version for optimistic concurrency.
In clustered mode, two operators updating two different brokers can both pass
local version checks before the background Ganglion CAS serializes their updates.

Impact:

The current user-facing conflict contract is local, not cluster-wide. The docs say
settings return `409 Conflict` when another operator changed settings first, but
that is not guaranteed across brokers.

Preferred fix:

In Ganglion mode, base admin write concurrency on the Ganglion document version.
This likely falls out naturally if item 1 switches the admin path to cluster-first
writes.

Tests to add:

- Two brokers with stale cluster versions produce a conflict or a clearly
  documented last-writer-wins outcome.

Resolution:

- Ganglion-mode admin responses expose the cluster document version.
- Ganglion-mode admin writes use the cluster document version for optimistic
  concurrency.
- Stale cluster versions return `409 Conflict`.

## Resolution Summary

The chosen model is:

- Standalone mode uses the local Stroma runtime settings manager.
- Ganglion mode uses a replicated cluster document as the runtime settings
  authority.
- Local boot locks are not valid cluster runtime policy.
- Hardware-shaped startup settings remain local.
- Any future immutable cluster runtime policy should be a replicated policy
  document, not node-local boot config.

## Regression Coverage

- Broker runtime settings validation rejects invalid replication and partition
  values.
- Config validation rejects Ganglion mode combined with runtime locks.
- Config runtime seed validation matches runtime settings rules.
- Admin runtime settings routes use cluster-first writes when a cluster store is
  installed.
- Ganglion runtime settings updates reject invalid values, detect stale cluster
  versions, replicate across managers, and report malformed cluster metadata.

## Deferred

Cluster-wide immutable runtime policy remains intentionally undecided. If Fibril
needs it, it should be designed as a replicated cluster policy with clear
operator behavior. It should not reuse node-local boot locks.

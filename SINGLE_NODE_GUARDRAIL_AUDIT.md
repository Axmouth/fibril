# Single-Node Guardrail Audit

This audit tracks the rule that clustered features must not make the standalone
broker path harder to run or reason about.

## Goal

Standalone mode should keep working without Ganglion, a coordination leader, or
cluster runtime settings authority.

- The default coordination mode is static.
- Local runtime settings remain authoritative when no cluster settings store is
  installed.
- Node-local runtime locks are valid in static mode.
- Ganglion-only failure states should not appear in normal standalone admin
  operations.
- Cluster wiring should stay optional until `coordination.mode = "ganglion"` is
  selected.

## Findings

### 1. Static Mode Is Still The Default

Status: Addressed

The config default remains `coordination.mode = "static"`. The server only
constructs Ganglion coordination, Raft transport, runtime settings cluster store,
replication loops, and controller tasks when Ganglion mode is selected.

Coverage:

- Existing config tests cover defaults.
- Existing `runtime_seed_from_config` tests cover boot seed mapping without
  coordination.

### 2. Standalone Runtime Settings Stay Local

Status: Addressed

When the admin server has no `RuntimeSettingsClusterStore`, `GET` and `PUT`
runtime settings use the local `RuntimeSettingsManager`.

Coverage:

- Admin runtime settings `GET` returns the local Stroma-backed settings.
- Admin runtime settings `PUT` updates local settings without a cluster store.
- Local stale-version conflicts still return `409 Conflict`.
- Local runtime locks still return `423 Locked`.

### 3. Runtime Locks Are Standalone-Only

Status: Addressed

Node-local runtime locks are accepted in static mode and rejected in Ganglion
mode. This prevents a cluster from depending on which node handled an admin
request, while preserving the standalone operator use case.

Coverage:

- Config validation accepts `runtime_locks` in explicit static mode.
- Config validation rejects `runtime_locks` in Ganglion mode.

### 4. Topology Has A Standalone Shape

Status: Addressed

Standalone admin topology can expose a static single-node coordination view
without exposing Ganglion or Raft internals.

Coverage:

- Existing admin topology tests cover the no-coordination path.
- Existing admin topology tests cover a static coordination provider without a
  Raft block.

## Deferred

A broader single-node scenario runner would still be useful. The ideal version
starts a real broker with default config, exercises admin settings, queue
declare, publish, subscribe, message inspection, and shutdown, then repeats with
auth enabled. That is larger than this guardrail pass and belongs with broader
end-to-end test harness work.

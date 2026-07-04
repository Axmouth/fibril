---
title: Configuration policy
description: Proposed direction for Fibril startup config, runtime settings, and
  persisted global state.
slug: 0.4/development/config-policy
---

This is a development note. It describes how Fibril should decide where a setting belongs before adding more one-off environment variables.

## Goal

Configuration should stay predictable as Fibril gains an admin UI and more runtime-tunable behavior.

The main risk to avoid is overlapping sources of truth where a setting can come from env, a file, admin state, and queue state with unclear precedence.

## Setting Classes

Fibril settings should be classified before implementation.

### Bootstrap Settings

Bootstrap settings are required before the broker can open or accept traffic.

Examples:

* data directory
* TCP bind address
* admin bind address
* storage/log implementation choice
* TLS and listener setup
* initial admin authentication source

These should come from a config file, environment variables, or CLI flags. They should not be edited through persisted runtime state while the process is running.

Changing a bootstrap setting should require restart unless there is a very clear reason to support live reload.

### Runtime Defaults

Runtime defaults affect behavior but can reasonably change while the broker is alive.

Examples:

* default inflight lease duration
* expiry polling defaults
* idle queue cleanup timeout
* idle queue cleanup sweep interval
* publisher idle expiry
* future queue declaration defaults

These are candidates for persisted global state. The admin UI can edit them, and the broker can apply them without restart where the implementation supports it.

Some persisted runtime settings are not broker defaults. For example, the
current global dead-letter queue target is owned by storage-level state because
Stroma resolves the target when messages exhaust retries. It still follows the
same operator-facing rules: live update, persisted value after restart, and
explicit version checks for admin changes.

### Queue Settings

Queue settings apply to one queue or queue group.

Examples:

* per-queue retry policy
* per-queue dead-letter policy
* per-queue retention-like policy, if added
* per-queue delivery or expiry settings, if added

Queue settings should be changed through explicit queue commands, such as declaration/update commands, not by editing global config.

Global runtime defaults can provide starting values for new queues, but existing queues should not silently change unless the user makes a global policy that is explicitly defined to apply dynamically.

## Source of Truth

Use one owner per setting.

Bootstrap settings are owned by startup config.

Runtime defaults are owned by persisted global state once that state exists. Startup config may provide initial values for first boot, but should not keep overriding persisted runtime values on every restart by default.

Queue settings are owned by queue state.

This keeps operator expectations simple:

* startup config decides how the process starts
* global state decides live broker defaults
* queue state decides queue-specific behavior

## First-Boot Defaults

Startup config can seed persisted global state only when no global state exists yet.

After global state exists, persisted global state wins for runtime defaults.

This avoids surprising admin UI behavior where a user edits a setting, restarts the process, and an env var silently changes it back.

If an operator wants a startup setting to be locked, make that explicit rather than implicit.

## Locked Runtime Settings

Some deployments may want configuration management to own a runtime default and prevent admin edits.

If Fibril supports this, it should be explicit:

```txt
runtime_settings_locked = ["idle_queue_cleanup", "default_dead_letter_policy"]
```

or equivalent typed config.

A locked setting should be visible as locked in the admin UI and rejected by the runtime API with a clear error.

Do not make every environment variable an invisible override for persisted state. That creates hard-to-debug behavior.

## Global State Shape

Persisted global state should have its own event log and snapshot, separate from queue event logs.

Useful properties:

* append-only changes
* snapshot and replay like queue state
* explicit versioning
* audit-friendly history for admin changes
* atomic updates for related settings

Runtime settings should be grouped where atomic updates matter. For example, idle queue cleanup settings are related and should be updated together:

```txt
idle_queue_cleanup = {
  enabled = true
  evict_after_ms = 600000
  sweep_interval_ms = 60000
  publisher_idle_timeout_ms = 600000
}
```

## Naming Direction

Prefer user-facing names in docs and admin UI:

* "idle queue cleanup"
* "idle timeout"
* "publisher idle expiry"

Use implementation names only in internal APIs or development notes.

The current environment variable names are acceptable as temporary bootstrapping, but future config should avoid adding more bespoke env vars for runtime behavior.

## Implementation Policy

Before adding a setting, answer:

1. Is it needed before the process starts?
2. Can it safely change at runtime?
3. Is it global, queue-specific, or connection-specific?
4. Who owns it after restart?
5. Can admin UI edit it?
6. If config and persisted state both mention it, which one wins and is that visible?

If the answer is unclear, do not add an env var as the default path. Add a typed config field or a persisted runtime setting design first.

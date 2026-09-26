# Queue declaration and replica history

Historical implementation record. Current contracts live in the [documentation site](../../website/src/content/docs/implemented-surface.md).

A clustered queue declaration must reach the assigned owner before that owner
accepts publications using the declared settings. The broker receiving the
request may be an owner, a follower, or neither.

## The failure sequence

1. A client declares a queue through broker A. Coordination records its partition
   count and catalogues the partitions for placement.
2. The old protocol and admin handlers also call local storage declaration on A.
   Before placement arrives, storage creates an owner handle and writes a durable
   `Declare` event at event-log offset 0.
3. Placement assigns ownership to B and makes A a follower. B can create its own
   log on first publication, starting with an enqueue event at offset 0. Custom
   settings written only on A are absent from B.
4. A receives B's event prefix. Its local `Declare` and B's enqueue occupy the
   same offset but differ in content. Keratin correctly rejects this as
   `replicated overlap mismatch at offset 0`.
5. Later checkpoint recovery can replace the divergent prefix. That explains
   why successful eventual catch-up does not make independent declaration writes
   safe, or prove the requested queue settings were honored before recovery.

This is independent of targeted routing: both routing paths exhibited the
warning. Storage's prefix comparison is a safety check and remains unchanged.
A storage regression reproduces this exact conflicting event sequence.

## Corrected declaration path

Both protocol and admin declarations commit the settings through coordination
before cataloguing the partitions. Settings are keyed by logical queue and group;
partial redeclarations merge only supplied fields through compare-and-set, so
omitting TTL or dead-letter settings does not reset them. A conflicting partition
count fails before updating settings.

The receiver creates no local storage as a side effect of clustered declaration.
Only the assigned owner appends the `Declare` event, under the same per-queue
materialization/retirement guard used by publishers. First publications therefore
see the committed settings. Followers obtain the event through ordinary
replication, and newly grown partitions inherit the logical queue's settings.

The assignment watcher also reconciles setting updates on existing owners.
Declaration success in cluster mode acknowledges committed metadata; applying
updates to already-running owners and followers converges asynchronously. Cold
legacy queues without coordinated settings retain their existing log settings.
Standalone declarations retain their direct local behavior.

An applied-settings cache avoids writing duplicate declaration events on every
reconciliation. It is invalidated on cold materialization, role transitions, and
retirement. Settings reconciliation must not override a refused follower
promotion: existing storage must already permit an owner write.

The committed-state lock is used only to look up and clone the declaration's raw
metadata string. JSON parsing and storage operations occur after releasing it.
No conflicting log records are silently skipped or rewritten by this change.

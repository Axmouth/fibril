---
title: Recovery quarantine
description: How Fibril detects and isolates a damaged queue log on restart instead of corrupting state or crashing the broker.
---

When a broker restarts, it replays each queue's durable event log to rebuild
queue state. If a power loss or a hardware fault left that log damaged, blindly
replaying it could rebuild wrong state or panic the whole broker over one bad
partition.

Recovery quarantine detects a damaged log during replay and isolates the
affected partition so the rest of the broker stays up.

## What Fibril does

During recovery, Fibril verifies the event log before trusting it:

- **Reference check.** Queue payloads live in enqueue records. Later state changes
  must reference earlier offsets in that same log. A forward reference is invalid.
- **Decode check.** Every event record must decode (including its CRC). A record
  that fails to decode is treated as corruption.

For an unbound queue, recovery handles the first bad record according to the
`recovery.on_mismatch` startup setting:

| Policy | Behavior |
| --- | --- |
| `quarantine` (default) | Park only that partition. Its operations return an error, and the rest of the broker keeps serving. |
| `refuse` | Treat the mismatch as fatal for readiness: the node reports not ready. |
| `ignore` | Automatically truncate the log to the last valid record and continue. |

A quarantined partition is surfaced clearly: a banner in the [admin
dashboard](/admin-dashboard/), the `/readyz` health endpoint, and a
`recovery.quarantined` metric.

## Repair

Repairing a quarantined partition truncates its event log to the last valid
record, dropping the damaged suffix, and clears the quarantine. Trigger it from
the admin banner or the repair endpoint.

Accepted replicated histories reject local suffix truncation. Recovery preserves
the evidence, selects a compatible source and installs a verified generation
through the coordinated recovery protocol. An ordinary follower retry cannot
authorize discarding or replacing an accepted history.

## What local repair discards

An unbound queue can be explicitly repaired by truncating at the first invalid
record. This also discards every later record, including any otherwise readable
payloads and state changes. Skipping an invalid record would lose a transition
while applying its successors, so replay stops at the damaged boundary.

The former two-log lost-payload suffix rule does not apply to this storage format.
A queue enqueue and its payload are stored together. A forward reference is
corruption and follows the configured policy.

## Conditions and limits

- The default `quarantine` keeps the broker available: one bad partition does not
  take down the others.
- `ignore` discards the bad suffix automatically for an unbound queue. Use it only when losing the
  unrecoverable tail without an operator step is acceptable.
- `refuse` is evaluated lazily today: a mismatch is detected when the partition
  is first used after restart rather than eagerly at boot. An eager whole-disk
  recovery at boot is a tracked follow-up.

## See also

- [Reliability semantics](/reliability/semantics/) for the durability model.
- [Replication](/reliability/replication/) for coordinated history repair and follower catch-up.
- [Configuration](/configuration/) for the `recovery.on_mismatch` setting.

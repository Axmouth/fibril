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

- **Reference check.** Every replayed event references a message offset. Recovery
  checks that offset against the message log's durable tail. A reference past the
  durable tail is a dangling forward reference.
- **Decode check.** Every event record must decode (including its CRC). A record
  that fails to decode is treated as corruption.

When recovery finds the first bad record, it acts according to the
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

In a replicated cluster, repair is safe to combine with replication: truncating
to the last valid record drops the bad suffix, and the partition's follower
replication re-fetches the dropped records from the owner on its next catch-up.

## Why truncate-to-valid is a complete repair

Events are always written after the messages they reference. So a dangling
forward reference can only appear as a lost tail: a crash that durably recorded
events whose messages did not survive. Truncating back to the last valid record
removes exactly that unbacked suffix.

A corrupt event record is the genuine mid-log failure rather than a lost tail,
but the safe repair is the same: truncate at the bad record. Skipping it would
silently drop a state transition, so recovery stops there instead.

## Conditions and limits

- The default `quarantine` keeps the broker available: one bad partition does not
  take down the others.
- `ignore` discards the bad suffix automatically. Use it only when losing the
  unrecoverable tail without an operator step is acceptable.
- `refuse` is evaluated lazily today: a mismatch is detected when the partition
  is first used after restart rather than eagerly at boot. An eager whole-disk
  recovery at boot is a tracked follow-up.

## See also

- [Reliability semantics](/reliability/semantics/) for the durability model.
- [Replication](/reliability/replication/) for how followers re-fetch a repaired suffix.
- [Configuration](/configuration/) for the `recovery.on_mismatch` setting.

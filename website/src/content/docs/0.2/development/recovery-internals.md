---
title: Recovery quarantine internals
description: Development notes on why a damaged queue log is repaired by
  truncate-to-valid, and where the consistency check belongs.
slug: 0.2/development/recovery-internals
---

This is a development note. User-facing behavior lives in
[recovery quarantine](/0.2/reliability/recovery-quarantine/). This page
records the reasoning behind the design.

The planning requirement was explicit: on recovery, verify that event references
resolve, fail loud, and do not silently self-heal.

## Detection

Recovery replays the event log lazily, per partition, on first use. During the
scan each event's highest referenced message offset is compared to the message
log's durable tail (`next_offset`). The first event whose reference is at or past
the tail is the truncation point: the scan stops there and returns the valid
prefix plus the mismatch (event-log offset and dangling message offset). Every
record must also decode, including its CRC.

## Why truncate-to-valid is the only shape

A mid-log dangling reference is impossible by construction. An `Enqueue{off}`
event is written only after its message is durable (message fsync, then event),
so the event log can reference a missing message only if it got ahead and a crash
lost the message tail. That always leaves a contiguous suffix of dangling events,
never a hole in the middle.

So truncating back to the last valid record drops exactly the unbacked suffix. An
earlier "surgically drop scattered dangling events" idea was discarded because
there is nothing scattered to drop. This is also the precise prerequisite for
parallel-fsync, where that suffix gap first becomes possible.

A corrupt record (CRC or decode failure) is the genuinely mid-log failure rather
than a lost tail, but the safe repair is the same: truncate at the bad record.
Skipping it and continuing the scan would silently drop a state transition and
leave inconsistent state, so recovery stops there too. Both kinds share one
human-readable reason on the quarantine record.

## Repair and re-replication

Truncate-to-valid drops the event-log suffix from the first bad record
(destructive reset to a checkpoint, briefly assuming the follower role).

On a follower this is not data loss: dropping the suffix lowers the local
`next_offset`, and the existing follower replication worker re-fetches exactly
that dropped suffix from the owner on its next catch-up tick. The worker retries
through the quarantine, so once an operator clears it the partition catches up on
its own, with no new code. An owner or single node has no peer to re-fetch from,
so the suffix is genuinely lost there, and the admin banner says so.

## Where the invariant belongs

The steady-state follower invariant "events never reference unreceived messages"
is enforced at recovery (the persisted-log scan) and at promotion (refuse a
partial replica), but not on the live apply path. Catch-up legitimately runs with
events transiently ahead of their messages, so a live hard-fail is wrong: it
broke a promotion test that intentionally exercises partial replication.
Fail-fast belongs only where consistency is actually required.

## Policy and blast radius

The default `quarantine` policy parks only the affected partition, so the blast
radius is one queue while the broker keeps serving everything else. `refuse`
escalates to readiness so a strict deployment cannot miss it, and `ignore`
auto-applies the truncate with a loud warning for operators who accept the
possible loss. An eager opt-in whole-disk recovery at boot (a tracked follow-up)
would make `refuse` a literal refuse-to-start.

## See also

* [Recovery quarantine](/0.2/reliability/recovery-quarantine/) for the operator view.
* [Replication design](/0.2/development/replication-design/) for the ordering invariants this builds on.

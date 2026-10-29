---
title: Follower loss without fencing
description: How a queue keeps serving when one of its followers is lost, by holding the lost copy in place instead of starting a recovery.
---

This note records the follower loss hold, implemented in October 2026 as the
runtime setting `replication.follower_loss_hold_ms`, 5 minutes by default. It is
the first step toward the lost-follower case of
[live rebalancing](/development/rebalancing-design/). A follower lost for longer
than the hold is still replaced through recovery.

## Problem

When a broker that is only a follower of a queue dies, the queue stops
serving for several seconds and fails the publishes that were in flight, even
though the owner and the other followers are healthy and could keep confirming.

Measured on three brokers sharing one NVMe drive, 3 copies under
`majority_durable`, 1,000 msg/s per queue, October 2026: queues that lost only a
follower paused for 3 to 15 s and failed 2,500 to 12,000 publishes each with an
unknown outcome. Nothing confirmed was lost. The pause and the failures come
from the coordination path below, not from the confirm rule.

## What happened before

1. Placement drops the dead follower from the proposed follower list once the
   node leaves the live set, through eager failover or liveness expiry.
2. For an activated queue, `retain_unproven_assignments` treats any follower list
   change as a recovery. It keeps the committed assignment and writes a pending
   recovery whose proposed epoch is the old epoch plus one.
3. While a recovery is pending, the accepted history of the queue fails to
   validate, so the queue's assignment drops out of the brokers' view. The owner
   stops owning it, and every publish waiting for a confirm fails with
   `assignment changed after publication admission`.
4. The recovery driver seals the old replicas, the healthy owner included,
   inspects them, installs on the proposed members and activates the new
   epoch. Each broker runs one recovery seal at a time and retries back off from
   1 s to 30 s, so several queues recovering at once wait on each other.

The confirm rule does not need any of this. Under `majority_durable` with 3
copies the write requirement W is 2, so the owner and one live follower confirm
on their own. A dead follower simply never reports progress.

## The hold

When placement drops a follower of a queue that is down, the controller keeps
the queue's current assignment instead of starting a recovery, as long as the
queue still meets its write requirement without that follower.

- **Condition.** The queue is activated and enrolled, W is at least 2, the owner
  is unchanged, and the followers left still number at least W - 1. Every
  dropped follower has stopped heartbeating, through liveness expiry or eager
  failover, less than the hold ago. A follower that is alive but leaving, for
  example from a draining node, does not qualify.
- **Effect.** The assignment, epoch and activation stay the same and no pending
  recovery is written, so the owner keeps owning the queue and publishes in
  flight keep waiting for a confirm the remaining followers provide. The lost
  copy never reports, so it is never waited for.
- **Retention.** A follower that has not reported for 60 s stops protecting the
  owner's log, so the owner keeps truncating while the copy is away. A copy that
  falls too far behind reseeds when it returns.
- **Return.** A follower back with a new process is suspended and rejoins as a
  learner, the existing path for restarted followers. One back with the same
  process, after a network fault, resumes streaming from where it stopped.
- **After the hold.** Placement replaces the lost follower through recovery as
  before. A hold of 0 does that at once.
- **Otherwise.** W = 1 queues, streams and owner changes take the recovery path
  unchanged.

Restoring the full number of copies without a fence is the second step, the
learner-based replacement in [live rebalancing](/development/rebalancing-design/).

## Why it is safe

The two quantities recovery depends on do not change.

- **The eligible set and the recovery threshold.** The held follower keeps its
  slot in the eligible set E. Recovery requires R = E - W + 1 seals from that
  set, and R + W > E means every possible confirm set shares a replica with
  every seal set. The hold changes neither E nor W, so a later recovery still
  sees every confirmed record.
- **The confirm rule.** W comes from the unchanged assignment, so the hold
  cannot lower it. A confirm still needs W copies durable, now drawn from the
  remaining ones. If they cannot supply it, confirms wait, as before.
- **No new epoch.** The owner, its epoch and its activation stay the same, so
  `preserves_replication_contract` holds and no session or publication needs to
  be fenced.
- **False suspicion.** If eager failover suspects a follower that is in fact
  alive, the assignment does not change and the follower keeps streaming and
  counting. Only a recovery after the hold would act on the suspicion.
- **A second failure.** If the owner is lost while a follower is held, the
  recovery threshold is computed over the same E as before. With 3 copies, the
  owner and one follower lost leaves one copy, short of R = 2, so the queue waits
  for a copy to return instead of electing an owner without proof. That is the
  same outcome as losing two of three copies before the hold existed.

## Where it lives

- `lost_followers_held` in `crates/coordination-ganglion/src/promotion.rs`
  decides whether a proposal only drops lost followers within the hold, and
  `retain_unproven_assignments` then keeps the current assignment without a
  pending recovery.
- The controller passes a predicate built from the live set and each node's
  heartbeat label, and reads the hold from the runtime settings every tick.

## Tests

- Unit tests in `history_activation.rs`: a lost follower within the hold keeps
  the assignment with no pending recovery and the queue still projected, a
  proposed replacement waits for the hold, past the hold a recovery is
  requested, losing both followers falls to the write requirement rule, and a
  W = 1 queue still recovers.
- Cluster bench, three brokers on one NVMe, 3 copies `majority_durable`, 3 queues
  at 1k msg/s, tuned failover. Killing an owner that is not the controller:
  queues that only lost a follower went from a 3.4-12.2 s pause and up to 12,000
  unknown outcomes to 0.2 s and none, and the queue that lost its owner from
  8.0-10.3 s to 4.3 s. Killing the controller: follower-only queues 0.2 s, the
  controller's own queue 6.4 s.

## Open questions

- Whether the one-at-a-time recovery seal per broker should be widened, since it
  also slows owner recoveries when several queues fail over at once.
- What the default hold should be once the learner-based replacement exists and
  replacing a copy no longer fences the queue.

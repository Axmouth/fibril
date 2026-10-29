---
title: Follower loss without fencing
description: Design for keeping a queue serving when one of its followers is lost, by suspending the lost copy instead of starting a recovery.
---

This is a proposed design. It covers the first step toward the lost-follower
case of [live rebalancing](/development/rebalancing-design/), and reuses the
suspension that restarted followers already get, described in
[restarted queue followers](/reliability/recovery-sealing/#restarted-queue-followers).

## Problem

When a broker that is only a follower of a queue dies, the queue stops
serving for several seconds and fails the publishes that were in flight, even
though the owner and the other followers are healthy and could keep confirming.

Measured on three brokers sharing one NVMe drive, 3 copies under
`majority_durable`, 1,000 msg/s per queue, October 2026: queues that lost only a
follower paused for 3 to 15 s and failed 2,500 to 12,000 publishes each with an
unknown outcome. Nothing confirmed was lost. The pause and the failures come
from the coordination path below, not from the confirm rule.

## What happens today

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

## Proposal

When a follower of a queue is lost, the controller suspends it instead of
changing the assignment, as long as the queue still meets its write
requirement without it.

- **Condition.** The queue is activated and enrolled, W is at least 2, the owner
  is live, and the live, unsuspended followers still number at least W - 1. This
  is the same scope as `followers_rejoin_without_recovery`, which already lets a
  restarted follower rejoin without recovery.
- **Action.** The controller writes the lost follower into the queue's suspended
  set with a guarded write, and leaves `followers`, the epoch and the activation
  unchanged. No pending recovery is created.
- **Effect.** The owner keeps owning the queue. Publishes in flight keep waiting
  for a confirm that the remaining followers provide. The suspended copy stops
  counting toward confirms and toward the in-sync set.
- **Return.** A suspended follower comes back through the existing paths: a new
  process rejoins as a learner, and the same process returning after a network
  fault is re-admitted by a reseed. Both verify catch-up before it counts again.
- **Otherwise.** When the condition does not hold, for example W = 1, a stream,
  or too few live followers left, the current recovery path runs unchanged.

Restoring the full number of copies with a new node is the second step, the
learner-based replacement in [live rebalancing](/development/rebalancing-design/).

## Why it is safe

The two quantities recovery depends on do not change.

- **The eligible set and the recovery threshold.** A suspended replica keeps its
  slot in the eligible set E. Recovery requires R = E - W + 1 seals from that
  set, and R + W > E means every possible confirm set shares a replica with
  every seal set. Suspension changes neither E nor W, so a later recovery still
  sees every confirmed record.
- **The confirm rule.** W comes from the unchanged assignment, so suspension
  cannot lower it. A confirm still needs W copies durable, now drawn from the
  remaining ones. If they cannot supply it, confirms wait, as they do today.
- **No new epoch.** The owner, its epoch and its activation stay the same, so
  `preserves_replication_contract` holds and no session or publication needs to
  be fenced.
- **False suspicion.** If eager failover suspects a follower that is in fact
  alive, the follower stops counting and must re-admit by reseed. That costs
  catch-up traffic and briefly one fewer counting copy, and never risks a
  confirm.
- **A second failure.** If the owner is lost while a follower is suspended, the
  recovery threshold is computed over the same E as before. With 3 copies, the
  owner and one follower lost leaves one copy, short of R = 2, so the queue waits
  for a copy to return instead of electing an owner without proof. That is the
  same outcome as losing two of three copies today.

## Changes

1. A controller rule that suspends a lost follower under the condition above,
   recorded in the replicated attributes beside the restarted-follower suspension
   and validated with the same guarded writes.
2. `retain_unproven_assignments` passes the unchanged assignment through when the
   only difference from the proposal is a suspended follower.
3. Placement stops proposing a follower list without the lost node while it is
   suspended, so the controller does not keep asking for a recovery.
4. Reseed re-admits a suspended follower whose process did not change.

## Test plan

- Simulation: kill a follower of a 3-copy queue under load. No publish fails,
  the epoch is unchanged, and confirms continue.
- Simulation: kill the owner while a follower is suspended. Recovery waits for a
  second copy and never activates with fewer than R seals.
- Simulation: a follower falsely suspected by eager failover re-admits by reseed
  and counts again.
- Cluster bench: the owner-only and controller kills of the nightly suite show no
  pause and no unknown outcomes on queues that lost only a follower.
- W = 1 queues and streams still take the recovery path.

## Open questions

- Whether the one-at-a-time recovery seal per broker should be widened, since it
  also slows owner recoveries when several queues fail over at once.
- How long a suspended follower may stay out before placement replaces it, once
  the learner-based replacement exists.

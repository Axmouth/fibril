# Remaining-follower failover regression

With an owner and two data followers, losing the owner promotes one follower while
the other remains a follower. The remaining node must replace its replication
worker's source even though its local role has not changed.

The planner already emitted `RefreshFollower` for this assignment change. The
broker treated it as a no-op, and the worker retained its startup assignment.
Coordination could therefore report the new owner at epoch 2 while the worker
continued retrying the dead owner at epoch 1. This stalled replica-durable
publication after promotion. The same bug affected queue and stream followers.

## Simplified behavior

```text
Before:
    start worker(owner=B, cursors=current)

    on assignment update:
        if role changes:
            apply role transition
        else if still follower:
            do nothing

    # B dies; A is promoted; this node stays a follower.
    # Its worker still contacts B forever.

After:
    on assignment update:
        if still follower and (owner changed or epoch changed):
            stop old worker and drain its active work
            save message cursor and event cursor
            persist the new epoch fence
            create a fresh worker runtime with saved progress
            start worker with the new assignment

    # The remaining follower contacts A from its existing cursors.
```

Changes only to follower membership or durability policy retain the worker when
its source owner and epoch are unchanged. Promotion safety checks, durable append
requirements and cursor/checkpoint validation remain in place. Worker draining
registers its idle notification before checking active work so the final tick's
notification cannot be missed at the check/wait boundary.

## Regression coverage

- Queue and stream watcher tests hold an old-owner read open, update the owner and
  epoch while retaining the follower role, and require the new-owner read to start.
  Both fail on the previous implementation and pass with the correction.
- The progress test starts with nonzero message/event cursors, changes owner and
  epoch, verifies preserved progress, and rejects old-epoch message/event input.
- The worker-drain test requires waiting for the final active tick and refusing
  new ticks after stopping starts.
- `scripts/failover-remaining-follower.sh` exercises real three-process owner
  SIGKILL with two data followers, preallocated segments and `replica_durable:2`.
  At least 1,000 publications begun after the owner exits must confirm, alongside
  identity coverage for every confirmed publication and no phantoms.

The verifier's optional `--recovery-marker` file is created only after the killed
owner exits. A publication counts toward recovery only if the marker exists
before that publication begins and its durable confirmation subsequently succeeds.
Pre-crash publications finishing afterward do not satisfy the recovery threshold.
The cluster harness enables this through `FAILOVER_VERIFY_RECOVERY_MIN`.

```bash
cargo test --release -p fibril-broker --lib --test broker_tests
bash scripts/failover-remaining-follower.sh
```

The earlier single-follower failover scenario covered follower-to-owner promotion,
not the remaining follower's source change. Short identity checks could also pass
with only pre-crash confirmations. The new scenario requires post-exit progress
and has an external runtime bound so a stalled producer cannot wait indefinitely.

## Validation

All 113 broker unit and 132 broker integration tests pass. Two verifier CLI tests
confirm that the original invocation remains valid and an explicit recovery
threshold requires an owner-exit marker. Four corrected experimental-build crash
checks and the permanent scenario on a normal build pass with zero lost confirmed
IDs and zero phantoms. The normal-build run confirms 8,021 publications begun
after owner exit. These WSL results exercise real processes and sockets, while
native Linux and host-power-loss validation remain distinct checks.

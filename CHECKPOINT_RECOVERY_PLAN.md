# Checkpoint recovery: reproduced gaps and repair requirements

Investigation checkpoint: 2026-09-19, Fibril `59d47be`, Keratin `11ef6a9`.
This records failures and a proposed repair direction, not an implemented fix.

The old `RefreshFollower` no-op bug is separate: `3fc4253` already retargets a
remaining follower when its owner or epoch changes. The failures here concern
local storage consistency after it has selected the correct replication source.

## Reproductions

Both tests ran in an isolated Keratin source copy. No fault injection or failing
test was added to production. The normal checkpoint/backfill/caught-up-promotion
test passes in that same copy.

### Local-tail promotion before checkpoint message backfill

1. An owner durably publishes messages at offsets 0 and 1 and exports state.
2. A follower installs that state. Its ready set now references both messages,
   its event continuation is 2, and its applied event position is 1. Its message
   log is reset to continuation 0; message transfer is a subsequent step.
3. Before transferring any message bodies, invoke
   `promote_queue_follower_to_local_tail(..., epoch = 1)`.
4. It returns `Promoted { message_next_offset: 0, event_next_offset: 2,
   applied_event_offset: Some(1) }` instead of refusing promotion.

The existing regression tests `promote_queue_follower_if_caught_up` against the
owner's expected tails and correctly refuses until message continuation 2.
The broker's `PromoteFollowerToOwner` transition uses the local-tail API because
it cannot query a dead owner. Checking applied events alone misses references
already installed through the checkpoint snapshot.

This reproduces the storage API failure used by failover. A full cluster test
must still establish the timing and assignment-selection conditions that expose
it through the broker; no end-to-end message-loss claim follows from this test.

### Failed install followed by reopen

1. Prepare a recipient with a valid durable message and a snapshot containing
   that ready offset. Prepare an owner checkpoint containing two messages.
2. Begin checkpoint installation on the recipient in follower role.
3. Inject a test-only error immediately after the message-log reset completes,
   before the event-log reset or new state/snapshot installation.
4. Shut down and reopen the same storage directory, then materialize the queue.
5. Recovery returns an owner handle with `message_next=0`, `event_next=1`, and
   `ready=1`: the old snapshot references a body the reset removed.

This is a deterministic error-boundary/reopen test, not a SIGKILL or power-loss
campaign. The source is an isolated copy; the injected error only applies to the
unique test topic. The safety assertion permits refusal to materialize or a
consistent recovered generation, but fails on a healthy handle missing the
previous generation's payloads.

## Repair direction

- Before the first destructive operation, persist and sync an installation
  record containing enough checkpoint data, epoch/identity, and message coverage
  requirements to resume or explicitly refuse recovery. Include directory
  synchronization in the durability ordering for new/renamed metadata.
- Serialize installation with promotion and competing mutation paths. A role
  check at entry is insufficient. Cancellation and failed I/O must leave a
  recoverable record and must not restore serving eligibility prematurely.
- On reopen, resolve pending installation before ordinary snapshot/event replay
  or exposure as an owner. An interrupted replacement must not be mistaken for
  an ordinary unconfirmed publish tail that can be discarded.
- Preserve a backfill requirement after metadata installation finishes. Verify
  required message coverage before owner promotion or durability credit; check
  snapshot-held references as well as subsequently applied events. This must
  survive restart. Do not equate an applied event cursor with available bodies.
- Revalidate authority if the assignment epoch changes. Keep general divergent
  history repair disabled until authoritative-history selection, bounded retries
  and alerts have their own policy and tests.

The exact marker format and how to represent required message coverage still
need design. Existing owner export includes both the message backfill start and
owner tail; the install struct currently retains only the start. A plain volatile
"installing" flag would not fix reopen or the post-install backfill interval.

## Acceptance checks for the implementation

- Both reproductions become passing regressions against production code.
- Error, cancellation and restart at each boundary: intent persistence, each log
  reset, state install, snapshot publication, and installation-record cleanup.
- Promotion refuses before full required backfill, including after reopen, and
  succeeds after verified catch-up. Include ready, inflight, delayed, pending-DLQ
  and settled-history cases when deriving coverage from queue state.
- Epoch changes, repeated installs, concurrent promotion, failed metadata I/O,
  and existing pre-marker data directories have explicit safe outcomes.
- A broker-level failover test exercises the local-tail path during checkpoint
  catch-up, then real process-kill tests validate persistent recovery ordering.

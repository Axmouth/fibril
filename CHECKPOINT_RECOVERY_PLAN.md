# Checkpoint recovery: behavior, regressions and remaining gates

Checkpoint recovery must preserve a consistent relationship between message
bodies, event history and queue state across errors, cancellation and restart.
Promotion requires the message coverage referenced by the installed state.

The original reproductions below used Fibril `59d47be` and Keratin `11ef6a9`.
Payload-coverage promotion guards and recoverable local checkpoint installation
are now implemented. Full coordinated failover proof remains pending.

## Original reproductions

Both original tests ran in an isolated Keratin source copy. Production regression
tests now cover these boundaries, with fault hooks compiled only for tests.

### Local-tail promotion before checkpoint message backfill

1. An owner durably publishes messages at offsets 0 and 1 and exports state.
2. A follower installs that state. Its ready set now references both messages,
   its event continuation is 2, and its applied event position is 1. Its message
   log is reset to continuation 0; message transfer is a subsequent step.
3. Before transferring any message bodies, invoke
   `promote_queue_follower_to_local_tail(..., epoch = 1)`.
4. It returns `Promoted { message_next_offset: 0, event_next_offset: 2,
   applied_event_offset: Some(1) }` instead of refusing promotion.

At the time, the existing regression checked `promote_queue_follower_if_caught_up`
against the owner's expected tails and correctly refused until continuation 2.
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

## Implemented local recovery

On Unix, installation persists a checksummed record containing the complete
checkpoint, queue identity, both expected epochs and continuation offsets before
resetting either log. Installation holds the partition lifecycle and follower
application locks; caller cancellation leaves its task running to completion.
Failures keep the partition gated and the record available for retry.

Ordinary access retries an interrupted live installation. On restart, the record
is resolved before normal log opening or snapshot/event replay. Recovery locks
both log roots and checks both persisted epochs before any deletion, so it can
rebuild partially removed or incomplete segment files without accepting a newer
assignment's history. Missing, damaged or mismatched authority metadata causes
an explicit error.

The installed snapshot is synced before a durable completion receipt replaces
the pending record. An identical completed request returns without resetting
later backfill; restart also preserves that backfill. Snapshot generations and
teardown fencing reject stale blocking snapshot writes prepared before an
installation or by an earlier queue incarnation.

Queue-state references retain the required message coverage across restart.
Owner activation and promotion wait for that coverage, including ready,
inflight, delayed, pending-DLQ and settled history. This requirement does not
prove the old owner's complete confirmed history or replace source validation.
The current install request carries the backfill start rather than a certified
owner tail; coordinated recovery still needs a validated history/dependency
manifest.

Linux validation covers each installation boundary through injected errors,
caller cancellation, restart and child-process SIGKILL. Other cases cover
partial segment files, repeated interruptions, offset zero, corrupted or
misaddressed records, concurrent maintenance exclusion and idempotent retry
after backfill. SIGKILL coverage does not establish storage power-loss behavior.

New installation and recovery of installation records return `Unsupported` on
non-Unix platforms before destructive changes. Ordinary pre-marker data remains
readable through its existing recovery path. Windows metadata durability requires
an implementation and platform validation before enabling this installation path.

## Remaining acceptance gates

- Authenticated checkpoint source selection bound to the committed transition,
  retained data-history identity and compatible authoritative evidence.
- New-write-quorum installation and activation, preserving the old confirmation
  requirement through repeated failures and membership changes.
- Broker-level failover during checkpoint catch-up, including controller and
  candidate restart, old-owner isolation and unavailable witnesses returning.
- Windows durable metadata support and platform-specific crash testing.
- Power-loss testing of filesystem/device persistence ordering.

The implementation sequence is maintained in the current
[failover plan](website/src/content/docs/development/failover-plan.md).

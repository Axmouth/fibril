---
title: Optimization and bug notes
description: Short records of measured optimizations, discovered bugs and unresolved failures.
---

Each entry records the mechanism, result or correctness effect, and a reference
for further detail. Measurements describe the stated workload and hardware.
Current capabilities are in [implemented surface](/implemented-surface/);
remaining work is in the [roadmap](/roadmap/).

## Adoption — September 2026

### Resource recreation and recovery identity

New queue and stream declarations now register their incarnation ID atomically, so concurrent declarations retain one identity and recreation after retirement receives a new one. Conditional deletion protects identified replacements from a delayed old delete, and recovery seal authorization rejects changed or missing incarnation metadata. Consensus snapshot restart and transition tests cover these cases; storage lineage and writer admission remain in the [failover plan](/development/failover-plan/).

### Ordered application and exact checkpoints

Queue events now finish actor application in log order, allowing snapshots to capture state with its actual exclusive event boundary. Interrupted application cannot be cleared by changing roles, and verified repeated follower batches skip already-applied events so NACK retries do not increment twice. Tests cover event zero, partial batches, cancelled captures, failed writes and restart; [checkpoint internals](/development/recovery-internals/) describes the boundary.

### Comparing different checkpoint starts

Explicit sealed inspection now verifies each snapshot and replays its event suffix to a common target, then compares live payload identities separately from state. Owner-local leases have a separate normalized digest; retry counts, delays, TTL and DLQ state retain their meaning. Authenticated TCP tests cover unequal checkpoint starts, while source authority and automatic activation remain tracked in the [failover plan](/development/failover-plan/).

### Checkpoint rejection and cancelled captures

Malformed queue checkpoints could panic on invalid ranges or a truncated custom-DLQ group, and a rejected load could partially replace existing actor state. Decoding now validates into isolated state before replacement, and a pause acquired for checkpoint export is released even when its caller is cancelled while draining active work. Owner capture also serializes with role changes and recovery sealing; malformed-field, cancellation and real broker checkpoint tests cover these paths.

### Restartable suffix repair preserves valid events

The recovery helper used a whole-log checkpoint reset when it intended to remove only a bad suffix, leaving the valid prefix in memory but removing its durable records. Suffix repair now journals the cut, preserves earlier records and resumes before normal log opening; repeated restarts, I/O faults and six SIGKILL boundaries cover the fix. A dangling enqueue also no longer masks later corruption or unexplained missing settlement payloads.

### Canonical queue state from sealed replay

Explicit inspection can reconstruct fully retained queue histories at a common exclusive event frontier and hash their complete canonical state. Payload and input-history digests remain separate, while operation budgets and bounded CPU workers keep this work outside live actor scheduling. Equal state supports comparison but still requires resource lineage and checkpoint authority ([recovery sealing](/reliability/recovery-sealing/#queue-state-at-an-exact-boundary)).

### Sealed-history comparison and dependency diagnostics

Explicit pair inspection now compares shared offsets across differently retained histories and verifies the complete transferred log digests, retaining only a page of unmatched record IDs. Whole-event reference checks expose incomplete payload batches and preserve explicit replay/checkpoint gaps; timeout, source loss and exhausted budgets discard partial results. Matching overlap remains subject to common-origin and state proof before source selection ([recovery sealing](/reliability/recovery-sealing/)).

### Read-only access to sealed evidence

Recovery can now read bounded pages from live or restarted sealed replicas without opening queue actors, repairing files or lifting the seal. Every page verifies the complete retained logs and snapshot, while authentication, transition checks and cancellation-safe locks preserve the recovery boundary. Full rescans are deliberately confined to explicit recovery calls; compatible-history proof and efficient bulk installation remain pending ([recovery sealing](/reliability/recovery-sealing/)).

### Transition-bound recovery witnesses

Explicit seal calls now use fresh authenticated connections with whole-call deadlines, and witness admission binds each reply to the contacted old replica and exact pending transition. Duplicate retries count once, a smaller proposed configuration cannot lower the old threshold, and contradictory evidence from one sealed replica blocks the collection. Threshold completion remains `AwaitingHistoryValidation`; automatic dispatch, compatible-history proof and activation are still pending.

### Replication authorization boundary

Internal replication handlers accepted ordinary authenticated clients, and unauthenticated clients when authentication was disabled; a real TCP regression reproduced a read of retained payloads. All replication and recovery controls now require the node principal on the physical connection before decoding or changing state. Tests cover refused reads/writes, every control opcode, resumed sessions, authenticated catch-up/checkpoints, TLS, streaming confirms and reauthentication after transport loss.

### Authenticated recovery requests and retained history

Explicit seal requests now require node authentication on the current transport and consensus authorization of the exact pending transition, with concurrent identical requests sharing one operation. Storage persists a fingerprint of the frozen records and snapshot, independent of fencing epochs and replica-local append times; retry reads bypass the cache so disk changes cannot hide behind cached contents. Authentication, cancellation, stale-transition and changed-record regressions cover this increment; automatic dispatch and compatible-history selection remain pending ([recovery sealing](/reliability/recovery-sealing/)).

### Recoverable follower checkpoint installation

A crash between the two log resets could leave old queue state referring to removed message bodies. A durable installation journal now resumes replacement before ordinary recovery, and a completion receipt prevents retries from erasing later backfill; snapshot fencing rejects stale writes after installation or eviction. Linux fault, cancellation and six-boundary SIGKILL tests cover [Keratin 76f1469](https://github.com/Axmouth/keratin/commit/76f1469); coordinated history selection and activation remain pending.

### Manifest replacement crash window

Manifest storage removed the old file before renaming its replacement, allowing a failed rename or crash to lose the persisted epoch. Direct replacement now preserves the previous manifest on rename failure and synchronizes both affected directories on Unix; an injected-failure regression covers [Keratin 3435e3d](https://github.com/Axmouth/keratin/commit/3435e3d). Windows metadata durability still requires separate implementation and testing.

### Replication wakeups independent of confirmation waits

Each locally durable queue batch now wakes replication after its payload/enqueue dependency is registered. A previous publication waiting for remote confirmation no longer delays that notification; the notification runs once per batch and skips a cancelled owner runtime. A regression parks a follower beyond batch A, leaves A unconfirmed, and verifies that durable batch B wakes the follower while both confirmations still wait for replica progress.

### Confirmed history during follower promotion — unresolved

A real-storage diagnostic confirmed a batch on owner A and follower B, then successfully promoted empty follower C after A stopped. Placement uses advisory heartbeat tails, while promotion establishes local completeness; neither establishes that the selected candidate contains every previously confirmed batch. Preserving that history across assignment changes is a recovery gate for clustered HA and for replication before owner durability.

### Durable local recovery seals

A persisted seal keeps queue and stream evidence intact across restart, eviction and interrupted fencing of the two logs. The storage primitive blocks ordinary reopening and cleanup and supports identical-request retries; it remains disconnected from automatic failover until history validation, installation and activation are implemented. Failure tests cover caller cancellation, a failed writer, partial fencing, damaged markers and startup suffix repair in [Keratin 617e1e8](https://github.com/Axmouth/keratin/commit/617e1e8).

### Epoch persistence retries

An epoch update changed the in-memory manifest before its disk write succeeded, allowing a retry to return success without retrying the failed write. The update now publishes the new in-memory epoch only after persistence succeeds; a filesystem fault test verifies repeated failure followed by successful durable retry in [Keratin 71b2f56](https://github.com/Axmouth/keratin/commit/71b2f56).

### Pending recovery before replicated assignment replacement

The controller retains the previous assignment and persists a proposed replacement when ownership, replica membership or policy changes affect replicated confirmation. The request survives metadata restart and is exposed in controller status, preventing followers from switching to an unproven source. Fresh sealing, recovery and activation remain pending, so this first barrier pauses replicated failover while preserving its recovery evidence.

### Complete durable cache tails

The log reader now accepts a cache hit that contains every record up to the captured durable frontier, even when the requested batch is larger. Incomplete coverage, decode failures and offset discontinuities fall back to the file reader; rollover, eviction and reopen parity are covered by regression tests. The initial three-node SATA/NVMe screen found overlapping throughput and latency ranges, so this change carries no end-to-end speedup claim; see [Keratin dd7943e](https://github.com/Axmouth/keratin/commit/dd7943e).

### Queue replication dependencies and follower persistence

Queue confirmation and delivery visibility require the same counted follower to cover the payload batch and its exact enqueue-event frontier, including the whole payload group referenced by an indivisible enqueue record. Progress is fenced by assignment epoch and transport session, and replacing an assignment clears its previous proof. Follower message and event writes overlap; both completions drain before state application, while an interrupted or failed apply blocks promotion until recovery or checkpoint resync.

### Checkpoint backfill and owner admission

Promotion and direct owner activation check the payload frontier required by ready, delayed, inflight, settled and dead-letter state; recovery keeps a checkpoint with missing payload backfill in the follower role. Client admission cannot perform the watcher’s follower-to-owner transition. Recovery also replays retained event zero when a legacy snapshot’s inclusive zero could denote an empty checkpoint; interrupted replacement of both logs and checkpoint state is covered by the recoverable installation journal described above.

### Durable publication application order

Consecutive batches could reach queue state in reverse order when the earlier durability continuation was delayed. Keratin now reserves an application turn under the append-order lock, allowing persistence to overlap while preserving state submission order; failed or abandoned turns fail the chain closed until recovery. A deterministic overtaking test and the Stroma regression suite cover [Keratin 0944dd6](https://github.com/Axmouth/keratin/commit/0944dd6).

### Settlement ownership and ignored requests

An ACK from a different consumer could remove the rightful consumer's delivery tag, and ignored or duplicate requests could leave settlement-drain accounting nonzero. Ownership validation and removal now share the same map lock, while ignored requests release their accounting without changing consumer credit. The ordinary-broker regression covers wrong-consumer ACK/NACK/reject requests, duplicate ACKs and graceful drain in [Fibril d64577d](https://github.com/Axmouth/fibril/commit/d64577d).

### Ordered benchmark confirmations

The Rust confirmation handle implements `Future`, permitting direct polling without a per-confirmation wrapper. The steady workload offers `--ordered-confirmations` for FIFO polling on an ordered single-partition workload; the default and cross-broker harness retain concurrent completion collection. This reduces benchmark bookkeeping and does not change broker confirmation guarantees or optimize the other SDKs' transports.

### Reproducible comparison tooling

The [shared Rust harness](https://github.com/Axmouth/fibril/tree/main/benchmarks/comparison) records message identity, offered load, completion latency and resource samples, with isolated single-node Docker provisioning. Existing-server modes also cover pipelined request/reply and multiple connections; portable cluster/fault orchestration remains pending. Raw competitor measurements remain separate from public capacity claims.

## Optimizations — September 2026

### Transparent huge pages — deployment option

Allocation tracing found that each materialized queue reserves two 16 MiB write
buffers and two 256 KiB index buffers, whose resident cost can grow substantially
under THP even when lightly used. Disabling THP only for the diagnostic broker
reduced RSS, while separate publish/delivery comparisons found mixed latency
effects at paced and saturated rates. The existing
[allocator startup option](/configuration/#linux-memory-policy) is a candidate
for memory-constrained deployments, especially lightly used materialized logs;
latency-sensitive deployments require workload-specific validation, and
production defaults are unchanged.

### Storage writer channel sizing — configurable, defaults unchanged

Crossbeam writer and notification channels allocate their full slot arrays when
each log opens. In a local 32-queue probe, reducing their capacities from 8,192
to 64 slots reduced post-declaration broker RSS from about 347 MiB to 248 MiB;
two short 1 KiB publish/delivery runs per storage type kept throughput within
roughly 1% of the default mean on tmpfs and SATA. The startup
[writer buffer factor](/configuration/) exposes this tradeoff while retaining
the existing default; shrinking the separate async command pipelines caused
substantial throughput losses in the same investigation.

### Targeted reads for queue routing — adopted

Queue routing reads the committed metadata it needs through a targeted path,
reducing repeated construction and traversal of the cluster view. Native
replication and live-repartition checks covered routing equivalence and stale
partition versions; the implementation is in [fibril 94cc1e8](https://github.com/Axmouth/fibril/commit/94cc1e8de25d9e6819347ba86582d888d9af4427).

### Rust ACK write coalescing — adopted

The client buffers individual ACK frames through a bounded command drain and
flushes the tail immediately when that drain ends, reducing socket writes while
preserving frame order and settlement history. In localhost tmpfs delivery-only
runs with 128-byte payloads, four readers, prefetch 16384 and 20 million preloaded
messages, mean throughput rose from about 529k to 1.03M messages/s across two runs
per variant. [fibril e713600](https://github.com/Axmouth/fibril/commit/e713600697be21ef73afa662b889128304bf5e20) includes ordering, boundary,
small-window and failed-flush coverage.

### Python and TypeScript publish buffering — adopted

Pipelined confirmed publishes share bounded socket writes while retaining their
individual frames and confirmation results. Both clients expose count, byte and
time limits plus an immediate-write option; Python's ordinary awaited publish
keeps its immediate-send path. See [fibril e713600](https://github.com/Axmouth/fibril/commit/e713600697be21ef73afa662b889128304bf5e20).

### Prompt socket-tail flushing — adopted

The broker flushes buffered output when both outgoing queues drain, retaining
its existing busy-batch bounds and durability gates. In same-host SATA ext4
runs with 1 KiB messages and one outstanding confirmation, mean per-run median
confirmation latency fell from about 6.0 ms to 1.4 ms; the higher-concurrency Rust
screen showed roughly 3–6% more broker CPU per message and small throughput
changes. [fibril dc878c9](https://github.com/Axmouth/fibril/commit/dc878c98d2f51f5a5da3954a470e3db7f5559c35) includes writer and replication-gate tests.

### Larger delivery cache — experiment retained, defaults unchanged

Increasing the log tail cache from 64 MiB to 4 GiB raised cache hits to 100% in
the measured tmpfs window, while delivery stayed around 419–422k messages/s and
broker RSS rose from about 0.44 to 4.87 GiB. Faster reads shifted waiting toward
consumer-channel sends, so the experiment did not justify a larger default.

### Settlement-history removal — not adopted

Isolated ACK experiments reduced queue-state work by avoiding full settled
history, but the broker comparisons did not establish a useful delivery gain.
Full range-compressed settlement history remains in place; later component
isolation identified client ACK writes as a stronger delivery limit.

Benchmark methods and earlier detailed experiments are in the
[optimization log](/development/optimization-log/).

## Bugs and diagnostics — September 2026

### Remaining follower kept its previous owner — fixed

After an owner change, a node that stayed a follower could receive a no-op
assignment transition and keep pulling from the previous owner. The planner now
refreshes that follower when its source or epoch changes, and the worker retains
its continuation offsets while retargeting: [fibril 3fc4253](https://github.com/Axmouth/fibril/commit/3fc4253d3e5b0d837c815055915c64cf2ca7f2e8).

### Retired partitions reopened during cleanup — fixed

Late cleanup or a stale reconnect could recreate a partition after shrink had
deleted it. Workers now stop before deletion, storage cleanup avoids creating
missing partitions, and a local retirement fence rejects stale access until a
fresh assignment admits the partition again: [fibril 8edb75f](https://github.com/Axmouth/fibril/commit/8edb75febc5669f70bb34bea65d2df1e653a37ec) and
[keratin 470d9e4](https://github.com/Axmouth/keratin/commit/470d9e423968e9b40121773a5b2d13c8d1af7ea8).

### Confirmation waiter missed the final progress update — fixed

A follower could report sufficient durable progress between the confirmation
waiter's progress check and notification registration, leaving the waiter asleep
until timeout. Registration now precedes the check, with a paused-time test that
forces the old race: [fibril 7f40f92](https://github.com/Axmouth/fibril/commit/7f40f929dd2137c440f33dfe30073e54c3971cc6).

### Independent declarations produced conflicting event histories — fixed

A broker receiving a declaration could append its own event before placement,
leaving a different record at an offset later supplied by the assigned owner.
Declarations now carry settings through coordination and only assigned owners
append them; followers receive those events through replication:
[fibril 990765e](https://github.com/Axmouth/fibril/commit/990765e77e09ce04ae504622b870803c26e1af89) and [keratin 0b75fd5](https://github.com/Axmouth/keratin/commit/0b75fd55879a7fda0592fc1f31baa770ab24fb33).

### Checkpoint reset discarded source epochs — fixed

Checkpoint installation dropped the source epochs before destructive log resets,
allowing a stale checkpoint to pass an already-advanced local fence. Both epochs
now reach storage, are validated before reset submission, and are checked again
by each writer in command order: [fibril dc878c9](https://github.com/Axmouth/fibril/commit/dc878c98d2f51f5a5da3954a470e3db7f5559c35) and
[keratin 11ef6a9](https://github.com/Axmouth/keratin/commit/11ef6a9455dc9a992a234c8d5ca8b4e9951899cd).

### Replication conflicts lacked preceding context — diagnostics added

Overlap reports now include bounded control history, offsets and effective
record identities, with payloads and headers excluded. An isolated reintroduction
of the declaration bug produced a report showing the local declaration and the
incoming enqueue at event offset zero; see the
[diagnostic format](https://github.com/Axmouth/fibril/blob/main/REPLICATION_OVERLAP_DIAGNOSTICS.md)
and [keratin 11ef6a9](https://github.com/Axmouth/keratin/commit/11ef6a9455dc9a992a234c8d5ca8b4e9951899cd).

### Interrupted checkpoints and early promotion — open

Isolated storage tests reproduced reopening after a message-log reset with
snapshot references to missing bodies, and local-tail promotion before
checkpoint-required message backfill completed. The
[recovery plan](https://github.com/Axmouth/fibril/blob/main/CHECKPOINT_RECOVERY_PLAN.md)
tracks consistent installation and promotion eligibility through restart; repair
implementation and broker-level process-kill validation remain pending. Evidence
and test scenarios are recorded in [fibril b14a91a](https://github.com/Axmouth/fibril/commit/b14a91a9690f543df7977f3da871d6de110c199e).

## Earlier storage finding

### Writer/fsync-worker deadlock under saturated storage — fixed

Scheduled commits could exceed the inflight limit and block the writer while
the fsync worker was blocked returning completions, producing a circular wait.
Every fsync handoff now waits for capacity by draining completions first; the
stress reproduction and fix are recorded in [keratin 3cb1218](https://github.com/Axmouth/keratin/commit/3cb121821e99aabdac35e73477b0b26a38789bd6).

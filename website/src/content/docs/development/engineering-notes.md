---
title: Optimization and bug notes
description: Short records of measured optimizations, discovered bugs and unresolved failures.
---

Each entry records the mechanism, result or correctness effect, and a reference
for further detail. Measurements describe the stated workload and hardware.
Current capabilities are in [implemented surface](/implemented-surface/);
remaining work is in the [roadmap](/roadmap/).

## Optimizations — September 2026

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

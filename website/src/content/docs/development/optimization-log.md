---
title: Optimization log
description: Benchmark-first notes for low-level Fibril performance work.
---

This page records benchmark methods and detailed optimization experiments.
Short summaries of recent optimizations and correctness findings are in
[optimization and bug notes](/development/engineering-notes/). Historical entries
retain the measurements and hypotheses from their recorded dates.

Define the cost to reduce, establish a baseline, compare before and after, and
record the result, including experiments that did not help.

## Current measurement surface

Fibril has these end-to-end benchmarks:

- `scripts/bench-steady-c.sh` measures controlled-rate publish and delivery,
  latency percentiles, missing messages, publish errors, confirmation errors,
  server RSS, and queue snapshots. `steady_c --rate-per-sec 0` publishes as
  fast as the client allows, so a saturation run measures the client and
  broker rather than the rate limiter.
- `scripts/bench-matrix.sh` runs repeatable steady-state scenarios and writes
  one results file plus one log file per case.
- `scripts/bench-e2e-c.sh` measures burst-style ingress and egress saturation.
- `scripts/bench-stream.sh` runs the stream tier and fan-out matrix. Its
  subscribe-only mode lets readers run in separate processes next to a
  publish-only process, so fan-out numbers are not capped by one client
  process. `clients/bench-plexus-fanout.sh` does the same for the other SDKs.
- `scripts/memory-audit.sh` runs repeated load, drain and eviction cycles and
  reports settled RSS per cycle. `--check` fails on cycle-over-cycle growth.
- `benchmarks/comparison` is a shared Rust workload for Fibril, NATS JetStream
  and RabbitMQ with resource sampling, settlement checks, saturation mode and
  `--burst-pattern` for bursty offered load.
- `benches/bin/failover_verify.rs` measures failover with confirmed-ID checks.
- `scripts/bench-results-table.sh` turns steady benchmark result files into a
  Markdown table.

`BENCHMARKING.md` in the repository root is the cookbook for these harnesses,
including the reference machine and the ceilings each one hits. The harnesses
do not print server CPU per message yet. Sample `/proc/<pid>/stat` over the
measured window when CPU matters.

Microbenchmarks are still few. `crates/protocol/benches/encode.rs` is a Criterion
encode/decode bench for publish frames, and `crates/broker/benches/stream_filter.rs`
covers stream filters. Before changing serialization, headers, batching, or
queue-handle lookup behavior, add a targeted microbench or an explicit
end-to-end benchmark case that isolates the expected effect.

For storage scheduling work, the `writer-stage-trace` feature records Keratin
writer batches, commit-to-fsync links and durable publication as JSON Lines that
convert to a browser timeline. It adds measurable CPU and memory, so keep traced
and untraced numbers in separate tables.

## Baseline protocol

Before changing a low-level path, record:

- Fibril, Keratin and Ganglion commits, and whether the Stroma source is local
  or git-sourced.
- Rust version, build mode and enabled Cargo features.
- Machine, CPU governor, kernel, disk type, filesystem and mount, NVMe APST
  policy, and `ulimit -n`.
- Any experimental runtime settings and `KERATIN_*` environment switches.
- Benchmark command, full environment variables, and output directory.
- Median result across at least three runs when the run is short enough.
- p50, p95, p99, max latency, missing messages, errors, server RSS avg and
  peak, and queue end state.
- Runtime warnings and errors from the benchmark log.

Use the same machine, same data directory class, same payload size, same
durability settings, same client count, same prefetch, and same benchmark
duration when comparing before and after. Label every comparison as solo or
replicated, with replica count, confirmation durability, drive layout, payload
size and traffic shape. Never mix tmpfs and physical storage results in one
table.

## Standard baseline commands

Quick smoke:

```sh
OUT_DIR=/tmp/fibril-opt-smoke scripts/bench-matrix.sh smoke
```

Short 1KB baseline:

```sh
OUT_DIR=/tmp/fibril-opt-baseline WARMUP_SECS=2 DURATION_SECS=5 \
  scripts/bench-matrix.sh baseline confirmed
```

Latency knee check:

```sh
OUT_DIR=/tmp/fibril-opt-throughput WARMUP_SECS=2 DURATION_SECS=5 \
  scripts/bench-matrix.sh throughput-1k
```

Payload-size check:

```sh
OUT_DIR=/tmp/fibril-opt-payload WARMUP_SECS=2 DURATION_SECS=5 \
  scripts/bench-matrix.sh payload
```

For noisy low-level work, prefer running one scenario three times over running
many scenarios once. A one-run improvement is a clue, not a conclusion. Run
paired comparisons in ABBA order (control, candidate, candidate, control) with
the same binary where the candidate is a setting, so host drift shows up as
disagreement instead of a false win.

## Acceptance rules

Use these as defaults unless the experiment says otherwise:

- A change aimed at throughput should improve measured throughput or the
  latency knee by at least 10% without increasing p95 or p99 latency in the
  normal-load cases.
- A change aimed at latency should improve p95 or p99 by at least 10% without
  lowering sustainable throughput.
- A change aimed at memory should reduce server RSS peak or steady RSS by at
  least 10% without adding missing messages, errors, or a clear latency
  regression.
- A change that adds hot-path instrumentation must show that the instrumentation
  overhead is small enough to keep enabled.
- A change that moves bottlenecks between CPU, storage, and memory must say
  which bottleneck moved and why.
- A performance change must not weaken a confirmation, seal or durability rule,
  and must pass the correctness suites before it is measured.

Small cleanups can still land without a visible benchmark win, but they should
not be described as optimizations unless the numbers support it. A change that
helps one workload shape but costs another can land as an off-by-default
runtime setting, labelled experimental, with its measured tradeoff recorded
here. It becomes a default only after a matched comparison against the default.

## Earlier candidates

These candidates were listed in June 2026. Their outcomes:

| Candidate | Outcome |
| --- | --- |
| Delivery loop allocation and scanning | Landed in July. Per-message work was cut and the broker-to-pump hop was batched, which moved the 1KB knee from 350-400k/s to 500-600k/s. See the 2026-07-03 rows. |
| Publish sink batch shape | The 250µs window anchored to the previous flush landed in June. September measurements showed per-connection sinks fragment enqueue batches when publishers use many connections. A shared per-queue publisher sink is an open CPU candidate. |
| Protocol encode and header layout | Closed. The positional codec costs under 1µs per 1KB message and is not a bottleneck. |
| Keratin writer pipelining | Landed in parts: adaptive fsync fusion and parallel message/event fsyncs in July, then stage-then-commit scheduling as an opt-in in September. Single-log queues later removed the second queue log. |
| Read-ahead for deliverable payloads | Landed as the Keratin tail cache in July (64 MiB default). A 4 GiB cache in September did not raise delivery throughput. |
| Topic and group representation | Not tried. |
| Replica-durable confirm latency | Superseded by credit-based push replication and long-poll catch-up. The July re-bench held 100k/s with p50 8ms. |
| Multi-queue follower catch-up | Not measured. Covered by the partitioned scaling track below. |
| Cluster routing and fan-in | Targeted committed-state routing reads landed in September with 21% less broker CPU per confirmation. A client fan-in benchmark is still unwritten. |

## Open directions

The current performance plan starts with a fresh baseline on current main,
because single-log queues, the storage failure latch, learner rejoin and the
stream backfill fixes changed the hot paths after most numbers below were
taken. Every candidate lands behind a flag first and is measured in ABBA order.
Its tracks:

- Memory: per-partition idle and active cost, writer channel slot defaults,
  tail cache and fan-out ring share of RSS, and payloads pinned by long-lived
  delayed or dead-lettered messages.
- CPU: a shared per-queue publisher sink, per-batch confirm handling instead of
  per-message dependency waits, profiles at saturation, and the Rust client
  read/write engine split. After the October fixes, most remaining CPU at 10k/s
  is threads waking each other: Tokio worker parking, the I/O driver, Keratin
  writer threads waiting on their channel, and syscalls. Candidates are
  completing a committed group's waiters in one batch, less spinning before
  the writer sleeps, a short grouping window for buffered applies, and keeping
  a partition's tasks and storage threads together, for example with
  `tokio_util::task::LocalPoolHandle::spawn_pinned_by_idx` by partition.
- Latency: a per-stage breakdown of replicated confirms, sending notifications
  before the periodic manifest store, background preparation of the next segment
  at rollover, write syscalls per confirm, and honest written versus durable
  progress so periodic sync can be evaluated.
- Throughput: rerun independent replication credit on single-log queues,
  payload copy counts at 4 KiB and 1 MiB, and replication re-encode cost.
- Recovery and failover time: raft timing defaults and leader lease, whether
  eager failover with a small grace should become the default, a bounded
  committed-view wait instead of the 1 s activation backoff, cheaper transfer
  authorization, fewer directory fsyncs in recovery caches, and concurrent
  recoveries per node.
- Partitioned scaling: sweep partitions per node from 1 to thousands on 1, 3
  and 5 nodes, separate idle from active partitions, and name the cause of the
  knee.

## Experiment record

Add entries here when an optimization is actually tried. Commits are Fibril
unless marked as Keratin or Ganglion.

| Date | Commit | Experiment | Expected effect | Benchmark | Result | Decision |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-06-08 | investigation | Establish benchmark-first optimization process | Avoid unmeasured low-level changes | Existing benchmark inventory | No runtime change | Keep this log updated |
| 2026-06-08 | local experiment | Batch delivery metrics and queue activity touch per poll batch | Reduce per-message time reads, atomics, and queue wakes in the delivery loop | `throughput-1k`, 250k-500k/s, 2s warmup, 5s steady | Neutral at 250k/s, worse p95/p99 and RSS at 350k/s, roughly neutral to worse above that | Reverted. The existing per-message updates stay until a better-targeted change proves itself |
| 2026-06-08 | local experiment | Wire protocol encode/decode Criterion bench and add content-type/header/payload-size cases | Make protocol/header optimization measurable before changing wire layout | `cargo bench -p fibril-protocol --bench encode -- --sample-size 20` | 1KB encode+decode about 6.3-6.7µs. Three small user headers were only slightly slower than no headers. 64KB encode+decode about 373µs | Keep the bench. Header layout is not the first obvious bottleneck for current end-to-end runs |
| 2026-06-08 | local experiment | Reduce publish sink small-batch coalesce window from 1ms to 250µs | Keep useful batching while reducing artificial wait in normal and near-saturation runs | `baseline`, `confirmed`, and `throughput-1k`, 2s warmup, 5s steady | Low-rate 50k/150k was neutral. High-rate p95 improved from 471ms to 385ms at 350k/s, 1598ms to 1533ms at 400k/s, and 3304ms to 3267ms at 500k/s. RSS was also lower in high-rate runs | Keep 250µs. It improves the knee without increasing the window |
| 2026-06-08 | local experiment | Remove publish sink small-batch wait entirely | Avoid any coalescing delay and rely only on immediate channel draining | `throughput-1k`, 2s warmup, 5s steady | Good at 250k/s, but worse than 250µs at 350k/s and 400k/s. Similar at 500k/s | Reverted. Some short coalescing still helps around the backlog knee |
| 2026-06-08 | local experiment | Start the publish sink coalesce window at the previous flush instead of after the next message arrives | Avoid artificial sparse-message latency while keeping burst batching when traffic is already flowing | `baseline`, `confirmed`, and `throughput-1k`, 2s warmup, 5s steady | Low-rate 50k/150k stayed neutral. High-rate p95 improved from the 250µs always-wait run: 385ms to 337ms at 350k/s, 1533ms to 1418ms at 400k/s, and 3267ms to 3152ms at 500k/s | Keep. This gives the sparse case immediate flush behavior and improves the measured saturation knee |
| 2026-06-15 | tooling | Let the steady benchmark target an external broker/admin pair and reuse `cluster-tryout.sh` for live cluster-routed runs | Make cluster-routed and later replica-durable confirm runs comparable without changing the benchmark client shape | `cargo check -p fibril-benches --bin steady_c --locked`, shell syntax checks, 3-node Ganglion tryout smoke at 500/s | The tryout-backed run delivered 500 measured messages with 0 missing and p99 publish-to-deliver latency of 15ms. A later audit showed the follower had not applied the measured range, so these are cluster-routed numbers, not replica-durable numbers. External targets report RSS as unavailable because the wrapper does not own the server process | Keep. Next step is an operator or test harness path that can require follower catch-up and replica-durable confirmation |
| 2026-06-15 | tuning | Add replica-durable latency split and follower read-budget runtime settings | Distinguish client/confirm backlog from broker delivery time, then test whether follower batch limits were the replica-durable throughput ceiling | 3-node Ganglion tryout, `replica_durable:2`, 1 KiB payload, 50k/s target, 50k confirm window, heartbeat 1s, liveness TTL 30s, follower budgets 1024 messages/read, 1024 events/read, 16 iterations/tick | Measured about 45.6k/s with zero missing and zero errors. Follower tail reached benchmark writes and owner/follower cursors matched. publish-to-server-receive p99 was about 3.9s, while server-receive-to-deliver p99 was about 141ms | Keep the knobs. The first ceiling was partly follower-read-budget limited. The next question is confirm-window and client-side backpressure behavior |
| 2026-06-15 | investigation | Add temporary protocol codec timing logs gated by `FIBRIL_PROTOCOL_CODEC_TIMING` | Check whether large-payload replicated runs are starving the server, the benchmark client, or both during encode/decode/frame-copy work | 3-node Ganglion tryout, `replica_durable:2`, 1 MiB payloads, explicit codec timing enabled | The probe showed large encode/decode costs on both sides, and especially expensive follower-side decode of large `ReplicationReadOk` frames. Moving large replication response decode to `spawn_blocking` protects Tokio workers but does not reduce deserialization time | Refactor or remove the logs before merge. If this signal remains useful, convert it to sampled or aggregated metrics rather than per-frame logs. Real speedup likely needs a different replication wire shape or dedicated decode/apply pipeline |
| 2026-06-15 | fix | Add byte-aware owner replication read caps | Stop large-payload followers from requesting giant single `ReplicationReadOk` responses when record-count limits allow too much payload data | 256 KiB replica-durable run exposed responses growing into tens or hundreds of MiB, with follower decode taking seconds and publish confirms timing out | With `max_bytes_per_read = 8388608`, the same 256 KiB, 170/s, confirm-window 100 run completed with 0 publish errors, 0 confirm errors, 0 missing messages, p99 publish-to-deliver about 69ms, and matching owner/follower cursors | Keep. Longer-term transport may split payload and event streams so size limits are natural rather than envelope-level |
| 2026-06-15 | investigation | Sweep confirm windows for high-rate 1 KiB replica-durable publishes | Separate sustainable low-latency throughput from throughput achieved by allowing a large confirmation backlog | 3-node Ganglion tryout, `replica_durable:2`, 1 KiB payload, 50k/s target, confirm windows 5k, 7.5k, 10k, 20k, 50k | 5k-7.5k windows stayed low latency at about 30k-32k/s with p99 publish-to-deliver 17ms. 10k reached about 34.8k/s but p99 rose to 841ms. 20k-50k reached about 38.5k-46.9k/s but p99 stayed around 2.8s. Raising event/message read limits at the 7.5k window did not improve throughput | Keep the split latency metrics. The next target is owner ingest/confirm scheduling, client confirm pacing, or storage isolation, not just larger follower read batches |
| 2026-06-15 | investigation | Compare tmpfs-backed and disk-backed local cluster data dirs | Check whether the current replica-durable low-latency ceiling is mostly physical disk contention | Same 3-node tryout, 1 KiB payload, 50k/s target, 7.5k confirm window. Default `/tmp` is tmpfs. Disk comparison used `CLUSTER_TRYOUT_RUN_ROOT` under the repo filesystem | Tmpfs reached about 31.8k/s with p99 publish-to-deliver 17ms. Disk-backed reached about 28.2k/s with p99 28ms. Storage placement matters, but tmpfs still has the same broad ceiling shape | Keep the script run-root override. Prioritize scheduler, protocol, confirm pacing, and single-partition owner-path analysis before assuming this is only disk throughput |
| 2026-06-15 | investigation | Split replica-durable publish-only and consume-drain paths, then inspect protocol codec logs | Find whether the current ceiling is publish replication, consume/ack, client windowing, roundtrips, or serialization | 3-node Ganglion tryout on tmpfs, `replica_durable:2`, 1 KiB payload, 50k/s publish-only target. Tuned follower budgets: 4096 messages/read, 4096 events/read, 16 MiB/read, 64 iterations/tick. Larger-batch variant used 16k records/read and 64 MiB/read | Consume-drain reached about 105k/s after preload, so consume/ack was not the bottleneck. Publish-only stayed around 27k/s. Lowering retry poll to 1ms did not help. Larger batches barely changed throughput but raised confirm latency from about 360ms p50 to about 1.36s p50. Codec logs showed owner `ReplicationReadOk` encode around 13-16ms for ~4.8 MiB frames, while follower decode took about 117-130ms. Larger ~19 MiB frames decoded in roughly 460-590ms | Current primary bottleneck is follower-side bulk `ReplicationReadOk` MessagePack decode, not physical disk or simple roundtrip count. Next useful work is a replication-specific wire shape or a bounded decode/apply pipeline |
| 2026-06-16 | investigation | Add broker replication timing metrics to the cluster tryout output | Separate client-observed confirm latency from owner replica-gate wait, owner read cost, follower owner-read wait, follower apply cost, and whole follower tick time | 3-node Ganglion tryout, `replica_durable:2`, 1 KiB payload, 50k/s publish-only target, confirm window 1024, 4096 messages/events per read, 16 MiB/read | Actual rate was about 49k/s with confirm p50/p95/p99/max = 204/243/244/246ms. Owner `replica_confirm_wait` averaged about 0.033ms over 144k samples, while owner reads averaged about 1ms, follower owner-read await averaged about 6.3ms, follower apply averaged about 19.2ms, and whole follower ticks averaged about 552ms | Keep the metrics. This run says the visible latency is not mostly waiting after local append for follower progress. Next isolate local append completion latency and follower tick batching before changing durability semantics or poll cadence |
| 2026-07-03 | df89e33 | Release consumer credit at settle accept instead of after the settle fsync | Remove the ack group-commit latency from the consumer flow-control cycle | `confirmed` at 50k/150k with PREFETCH=64 and default 16384, 3 runs each side | At prefetch 64 and 150k/s, publish-to-deliver collapsed from p50 ~800ms / p99 ~1290ms to p50 10ms / p99 14ms, matching the prefetch-16k profile. Default-prefetch runs unchanged | Keep. The credit-bound regime is gone and at-least-once semantics are unchanged |
| 2026-07-03 | 86fdbe7 | Remove the per-publish ConfirmStream offset send and drain tasks | Remove one channel hop and task wakeup per confirmed publish plus a bounded-channel stall hazard | `baseline` plus `confirmed` at 50k/150k, 3 runs each side | Latency-neutral, all percentiles within run noise | Keep as overhead and hazard removal, not claimed as a latency win |
| 2026-07-03 | 3179dae, 150a65a, b7ef762 | Delivery pump field moves, per-batch delivery bookkeeping, batched ack settle path, reverse tag map removal, publish-arm cache-first ordering, cached transport sink | Cut per-message allocations, atomics, map ops, and channel awaits on the delivery and settle paths | `baseline`, `confirmed`, `throughput-1k` (250k-500k), 2 runs per step | Sub-knee scenarios neutral, and the 1KB knee did not move (still between 350k/s and 400k/s). This supersedes the reverted 2026-06-08 metrics-batching trial: bundled with the payload move and wake-path changes it showed no 350k/s regression | Keep. The knee is set elsewhere, most likely the engine pipeline or the per-message broker-to-pump channel hop |
| 2026-07-03 | 9d1efb9 | Batch the broker-to-pump delivery hop (one Vec per consumer per poll batch) | Amortize the per-message channel send and task wakeup on the delivery path | `baseline`, `confirmed`, `throughput-1k`, low-prefetch confirmed, plus 600k/700k rate probes | This hop was the knee. 400k/s went from p50 ~750ms to 16ms and 500k/s from p50 ~2.4s to 26-27ms at full rate with zero missing. Saturation onset moved from 350-400k/s to 500-600k/s and high-rate RSS roughly halved. Sub-knee and low-prefetch unchanged | Keep. The next ceiling sits past 500k/s. A codec re-measure puts 1KB encode at ~0.8us and decode at ~0.11us, so the likely remaining bottleneck is the engine pipeline and scheduling, not the wire codec |
| 2026-07-03 | investigation | Compare tmpfs and SATA-SSD (ext4, ~92% full) data dirs on the post-audit build | Check how much of the measured single-node profile is storage-independent | `confirmed` plus `throughput-1k`, TMPDIR routed to the SSD, 2 runs | On the SSD the sustainable 1KB rate is roughly 250k-330k/s (400k+ targets fall short) with zero missing, vs 500k+ on tmpfs, and confirmed p50 ranged 15-36ms in the better run. Run-to-run variance on the SSD was large (the worse run showed 80-120ms p50 at low rates), consistent with a nearly full drive throttling writes. The pre-batching delivery-hop knee (350-400k/s) sat above what this disk sustains, so that win shows on fast storage | Keep both profiles in mind: tmpfs measures the architecture ceiling, this SSD measures this drive. Disk-backed numbers on healthier storage are worth collecting before publishing throughput claims |
| 2026-07-03 | keratin dda8555 | Self-clock keratin group commit when the fsync worker is idle | Stop durability acks from waiting out the fsync interval tick when no fsync is in flight | `baseline`, `confirmed`, `throughput-1k` on tmpfs, plus back-to-back disk A/B on the SATA SSD | Confirm p50 fell from 11ms to 4-5ms and publish-to-deliver p50 to 2-4ms sub-knee. 400k/s went from p50 16ms to 3-4ms and 500k/s from 27ms to 8-9ms at full rate, zero missing. The disk A/B was within drive variance with the new policy slightly ahead. A min_fsync_interval_ms floor (default 0) exists for storage where fsync rate is expensive, exposed through fibril config | Keep. The end-to-end p50 floor is no longer interval-bound |
| 2026-07-03 | local experiment | Settle coalesce window sensitivity probe, 500us vs 100us | Check whether the settle window gates end-to-end latency before building a runtime-adaptive controller for it | `confirmed` plus `throughput-1k`, 2 runs each side | Identical within noise everywhere. The settle window is not on the measured critical path at standard prefetch, so the earlier budget-share arithmetic overstated it | Keep 100us (consistent with the window budget rule, measured neutral). No controller for this dial. The first runtime-adaptive controller target moves to the keratin fsync idle floor, which is known sensitive across storage classes |
| 2026-07-03 | measurement | Re-bench replica-durable with the streaming transport and self-clocking commits | Check whether the June pull-path numbers still describe the cluster path | 3-node tryout, `replica_durable:2`, 1KB, 100k/s steady target, tmpfs | Held 100k/s with zero missing, publish-to-deliver p50 8ms / p95 14ms / p99 117ms under the replica-committed visibility gate. Follower ticks were zero (streaming path), follower apply avg 0.58ms with rare ~175ms stalls behind the p99 tail, replica gate wait avg 0.1ms. June reference was ~45-49k/s with ~204ms p50 confirm | The June cluster verdict is obsolete. Next: a confirmed-publish run for the producer path and a look at the rare follower-apply stalls behind the p99 |
| 2026-07-03 | investigation | Stream fan-out baseline plus knee attribution (bench-stream matrix, split-client runs) | Establish the stream tier x fan-out profile and locate the apparent r16/r32 collapse | `bench-stream` mixed at r1/r8/r16/r32 across tiers on tmpfs, then a new subscribe-only mode with readers split across processes | Tier made no difference anywhere (durable = speculative = ephemeral, cursor microbatch and confirmed publish clean at p50 1-3ms). Mixed-mode r16 at 100k/s collapsed to 745k/s p50 521ms, but split across processes the same load delivered 1.6M/s p50 1ms, and 32 readers delivered 3.2M/s p50 1ms at ~7 server cores. The collapse was the single bench client process | The server fan-out path has no measured knee up to 3.2M/s. Keep the subscribe-only mode for honest fan-out numbers. A concurrent-declare 500 race was found incidentally and filed |
| 2026-07-03 | branch perf-stream-batching | Batch the stream fan-out delivery chain end to end (PublishBatch command, per-subscriber batch send, batched backfill/forwarding, high-water auto-ack settle) | Amortize per-record dispatch per subscriber, the C1 analog for streams | Boundary points r8 at 200k, r16 at 60k/100k, r32 at 20k/100k both tiers, then split-client 3.2M/s runs with server CPU accounting | No throughput or latency change at any point. Server CPU slightly higher on the split 3.2M/s run (147 vs 132 CPU seconds): ingest batches rarely exceed one record at these rates, so the chain moved batches of one and paid a Vec per hop. The server was not the constraint anywhere | Parked unmerged. Revisit only if a real workload saturates the fan-out actor or delivery tasks. The subscribe-only bench mode was cherry-picked to main |
| 2026-07-03 | measurement | User-centric scenario battery: idle footprint, low-load latency, many readers at low traffic | Characterize the profile most deployments actually live in, not the saturation knee | Idle 30s windows with /proc CPU deltas, bench-stream mixed at 100 readers x 1k/s and 4 durable readers x 100/s confirmed, steady_c confirmed at 1k/s and 10k/s, tmpfs | Idle bare broker 0.0% CPU and 20MiB RSS. 4 durable stream readers at 100/s: sub-ms p50 delivery and confirm, ~3% of one core, 29MiB. 100 fan-out readers at 1k/s (100k del/s): p50 4ms, ~0.6 core, 57MiB. Confirmed queue at 1k-10k/s: p50 3-4ms confirm, ~28MiB. One wart: an idle ephemeral stream burns ~1.1% of a core because its 5ms flush ticker runs with nothing dirty | Publish these next to the saturation numbers in the benchmarks.md overhaul. Idle-gate or evict the ephemeral flush ticker (folded into the stream idle-eviction task) |
| 2026-07-03 | 29cba0d | Run the per-connection writer's 2ms flush tick only while frames are buffered | Remove about 500 wakeups per second per idle connection | Protocol tests, TypeScript examples, bench smoke | No throughput change expected or claimed | Keep as an idle CPU and wakeup fix |
| 2026-07-04 | d3e5578, 578df4c | Profile the Rust client publish path, then drain up to a bounded number of engine commands per wakeup | Find the single-client publish ceiling | In-code segment timers, tight-loop preload, 1KB | `publish()` costs about 554ns in total. The 100k/s figure came from steady_c pacing every publish through an interval tick. The tight loop reached about 148k/s unconfirmed and 150k/s confirmed, with the send to the engine task blocking under saturation | Keep batch drain. The saturate mode in steady_c came from this finding |
| 2026-07-05 | measurement | Single-client Rust engine ceiling | Decide whether the client or broker limits one connection | `steady_c` saturate mode, 1KB, one client | Publish-only about 173k/s unconfirmed and 159k/s confirmed (window 1024). The client sat at about one core while the broker used about 57% | Splitting the engine into read and write tasks is open, with a win-or-shelve gate of about 220k/s |
| 2026-07-05 | a02add1 | Coalesce Go client writes per drained command batch | One socket write per batch instead of per frame | Interleaved A/B, release broker on tmpfs | Publish 134k to 783k/s (about 5.8x). Manual-ack delivery 93k to 424k/s | Keep |
| 2026-07-05 | e6ad995, 74861ff, eb51411, f86a699 | Coalesce fire-and-forget writes and ACKs in the Python and TypeScript clients, with the window anchored to the last flush | Fewer send syscalls per message | Client publish and consume benches | Manual-ack consume about 1.6x (Python about 41k to 66k msgs/s). The changelog records roughly doubled single-client publish | Keep as a public client option |
| 2026-07-07 | Keratin 90368c3 | In-memory tail-read cache for durable records | Keep tail-following delivery from reading a segment under writeback | Mixed publish and delivery on one partition, NVMe and tmpfs | Mixed delivery about 232k to 394k msgs/s on NVMe. The delivery loss from concurrent writeback fell from about 48% to about 11% | Keep, 64 MiB default |
| 2026-07-07 | Keratin 7c0102d | Adaptive fsync fusion: pipeline several small commits and coalesce them into one fdatasync | Lift small-batch durable throughput without raising latency | Keratin append throughput, NVMe, 128B | Small-batch durable append 269k to 924k/s (about 3.4x) with no latency change. Large commits keep one fsync in flight | Keep. Tunables exposed through Fibril config in 4a91e2d |
| 2026-07-07 | Keratin d98b3a6, Fibril d2cf39e | Overlap the message-log and event-log fsyncs of a publish | Cut durable publish latency | Single-node durable publish | Roughly halved durable publish-to-confirm latency | Kept until single-log queues removed the second queue log in September |
| 2026-07-07 | Keratin bce0547, Fibril 07c449b | Optional segment preallocation ahead of the write cursor | Avoid the block-allocation flush of an extending write | fdatasync on a consumer NVMe | About 2.4ms to about 0.7ms per in-place fdatasync | Keep as an off-by-default setting. September burst screens found no isolated end-to-end gain at 1 MiB or 32 MiB, and 64 MiB was mixed on NVMe and worse for tmpfs throughput |
| 2026-07-07 | 5e8f7c6 | Cap each delivery poll by a node-local byte budget (1 MB default) | Keep `poll_ready` out of its memory-bandwidth-bound large-batch regime | End-to-end delivery | Matched prior throughput. The deliver and ACK path is the binding constraint | Keep as a guard against very large prefetch |
| 2026-07-16 | b348d25 | Broker memory audit across load, drain and eviction cycles | Find leaks or per-message retention | 6.4M messages across 32 queues, four repeated cycles | Eviction returns over 95% of the working set. Nothing is retained per message and eager mimalloc purge changed nothing. The residual is about 60 MB of one-time warm cost plus about 1 MB per queue partition ever materialized | Keep `scripts/memory-audit.sh` as a regression probe |
| 2026-09-17 | 94cc1e8 | Targeted committed-state reads for queue routing | Less cluster-view construction per publish | 250k/s offered, SATA, six runs of 45s in mixed order | 21.0% less broker CPU per confirmation (25.85µs to 20.42µs). p99 had no consistent direction | Keep. Always on since ec17a7c |
| 2026-09-19 | e713600 | Rust client ACK write coalescing through a bounded command drain | Fewer socket writes on the delivery path | Localhost tmpfs, delivery only, 128-byte payloads, four readers, prefetch 16384, 20 million preloaded, two runs per variant | Mean throughput about 529k to 1.03M msgs/s | Keep |
| 2026-09-19 | dc878c9 | Flush buffered broker output when both outgoing queues drain | Stop small confirms from waiting for a batch threshold | Same-host SATA ext4, 1 KiB, one outstanding confirmation | Mean per-run median confirmation latency about 6.0ms to 1.4ms. Roughly 3-6% more broker CPU per message at higher concurrency | Keep |
| 2026-09-19 | 32f03bb, Keratin a0bb46c | Smaller storage writer channels | Lower RSS per open log | 32-queue probe, short 1 KiB runs on tmpfs and SATA | 64 slots instead of 8,192 cut post-declaration RSS from about 347 MiB to 248 MiB, with throughput within about 1% | Configurable `writer_buffer_factor`, default unchanged |
| 2026-09-20 | branch experiment/speculative-delivery | Deliver before durability with an explicit speculative contract | Lower delivery tail on physical storage | SATA, 64 KiB at 1000/s, one queue | Early delivery with durable-or-processed confirm cut confirm and delivery p99 in both passes. Tmpfs saturation was about 8-10% lower | Kept on its experiment branch, not merged |
| 2026-09-21 | 1a20365, Keratin e42f302 | Early replication: send written records to followers before the owner's fsync | Overlap owner and follower persistence | Three replicas, majority durable, 1 KiB, smooth 100/s and 200/100 bursts at 100/s, ABBA | Matched-cache median intended-to-delivery 10-13ms to 8-9ms smooth and 40ms to 27-28ms for bursts. CPU per message rose about 26-35% and saturation results varied | Opt-in runtime setting `replication.early_replication` since ec17a7c, off by default. Persistent follower faults, divergent suffixes, lost responses and metadata quorum loss still need acceptance |
| 2026-09-22 | c5ea3c1 | Adaptive storage staging with lazy growth and idle release | Lower idle RSS after bursts | Broker comparisons, allocation tracing | Comparable latency, workload-dependent CPU and RSS | Enabled by default in Fibril with `adaptive_staging = false` as the opt-out |
| 2026-09-22 | investigation | Larger tail cache, 64 MiB to 4 GiB | Raise delivery throughput | tmpfs delivery | Cache hits reached 100% while delivery stayed about 419-422k msgs/s and RSS rose from about 0.44 to 4.87 GiB | Default unchanged |
| 2026-09-23 to 09-24 | a8297a0, 4b9cc91, 48f55ca, dd5530c, ca05e7b, 593b9f2, 76b9a25, Keratin 4b8b193, b5f1d4e | Recovery speedups: bounded pages with buffered scans, artifact and connection reuse, sequential inspection, retained segment reuse, seal/inspection overlap, payload prefetch and committed-metadata wakeups | Shorter recovery to readiness | Three replicas on SATA, 10k and 100k backlogs and settled histories | 10k readiness 15.12-15.22s to 4.29s. 100k readiness 50.72s to 24.11s with sequential inspection. Segment reuse cut a 23.99s gate to 9.31-10.34s. Prefetch cut copying from 0.95-1.13s to 0.75-0.76s. Wakeups cut settled-history fresh delivery from 4.15s to 2.00-2.02s | Keep. Details in the engineering notes |
| 2026-09-23 | Ganglion bcb45b8, Fibril 25b9455 | `TCP_NODELAY` on Ganglion Raft and forwarded metadata connections | Remove Nagle delays from small authorization round trips | Single host, three replicas, SATA, one confirmed 1 KiB message | Recovery attempt 6.75-6.96s to 0.81-1.22s | Keep |
| 2026-09-26 | Keratin d78a87c | Wake only the ordered applications that can proceed | Fix the ten-connection saturation stall | Historical five-million-message workload, solo and three-copy majority | The workload passes in both modes | Keep. One-connection samples vary, so no small performance claim |
| 2026-09-26 | Keratin 0343503, Fibril 18db4b7 | Stop owner replication reads after the byte-bounded prefix and keep a pending owner read across progress updates | Avoid discarded scans | Three-node tmpfs at 4 KiB | Lower CPU use and higher throughput | Keep |
| 2026-09-26 | branch d794be6 | Skip follower linger once queued frames merge to at least 1 MiB | Better saturation without hurting paced latency | Physical and tmpfs saturation | Physical saturation about 4.4% lower. Tmpfs gained about 6.1% throughput with about 4.4% less CPU per message | Abandoned, archived |
| 2026-09-26 | branch replication-actor-batch | Batch follower actor application | Higher replicated throughput | Split-drive saturation and paced runs | About 9.5% better split-drive saturation, worse paced latency | Not merged, archived |
| 2026-09-27 | investigation | One versus ten publisher connections at fixed total credit | Attribute replicated CPU per message | Replicated saturation and 50k/s offered | CPU per message about 27 to 47µs at saturation and 33.5 to 65.2µs at 50k/s. Mean enqueue batch size fell from 114 to 17 at saturation, and enqueue records were about 75-87% of the extra events | Led to the shared per-queue publisher sink candidate |
| 2026-09-27 | 57b875b, later ec17a7c | Follower push credit set apart from the per-read byte budget | Higher replicated throughput | Repeated physical saturation at 1 KiB and 4 KiB | No gain at 1 KiB. 16 MiB credit with 4 MiB merge improved 4 KiB saturation by about one third, with higher CPU per message and RSS | Off-by-default runtime setting `replication.stream_initial_credit_bytes`. Retest on single-log queues |
| 2026-09-27 | investigation | Burst timeline and CPU audit | Attribute low-rate CPU and burst latency | Replicated 1 KiB, smooth 100/s and 200/100 bursts | Smooth traffic used about 0.40 broker cores against about 0.014 for bursts at the same average rate. A synchronous periodic manifest store took about 9-12ms at fsync completion. Writers at the fsync pipeline cap spun through at least 957,600 zero-wait iterations | Writer fix landed as Keratin 561cbb1 (2026-10-07), which blocks the writer while the pipeline is full |
| 2026-09-27 | Keratin cb675ff | Background periodic manifest persistence | Remove the manifest store from the fsync completion path | Replicated 1 KiB at 100/s, same binary OFF/ON | Burst delivery medians 44.0-44.5ms to 31.8-32.7ms, smooth 9.6-11.2ms to 8.7-8.9ms, small CPU increase, burst p99 mixed | Off by default, enabled with `KERATIN_BACKGROUND_MANIFEST=1` |
| 2026-09-27 | Keratin 2ca48e6 | Close a pending append batch when writer input drains | Cut burst latency | 8 KiB bursts of 2,000/1,000 at an average 1,000/s, and 1 KiB at 100/s | 8 KiB median delivery 86-87ms to 72-73ms with about 11% more CPU and 18% more fsync jobs. No early closures at 1 KiB | Off by default, enabled with `KERATIN_FLUSH_ON_DRAIN=1` |
| 2026-09-27 | Keratin 7e56154 | Stage-then-commit: stage input immediately in bounded chunks and decide fsync submission after staging | Fewer fsyncs and lower CPU without adaptive linger | Replicated bursts and saturation, 1 KiB and 8 KiB | 8 KiB bursts 88-90ms to 74-77ms. 1 KiB saturation 117-124k/s to 121-132k/s with about 9% lower CPU per confirmed message. 8 KiB saturation overlapped at about 19-21k/s with about 6% lower CPU | Off by default, enabled with `KERATIN_STAGE_THEN_COMMIT=1`. Keratin CI runs the suites in this mode |
| 2026-09-27 | investigation | Standalone sync isolation and NVMe APST | Explain slow syncs after idle gaps | Dependency-free Rust probe on a Samsung 970 EVO Plus, ext4 | Intermittent single-writer segment sync was about 9ms against 2.62ms when continuous. With APST disabled it was 2.69-2.73ms, and four-log payload sync fell from 11.54ms to 5.32-5.41ms. Manifest stores stayed about 9ms | Deployment guidance only, no code change. A RabbitMQ quorum queue showed the same 8.72-8.88ms first follower sync after idle |
| 2026-09-27 | investigation | Follower coalescing window of 2000, 1000, 500 and 0µs | Lower burst latency | Replicated 1 KiB bursts and smooth runs, early replication on | Mean burst delivery 15.74, 16.98, 15.73 and 15.39ms. Zero raised CPU and fsync jobs on smooth traffic | No default change |
| 2026-09-27 | 4fbc7cb | Fixed follower collection deadline from the first frame | Catch trailing burst fragments in one sync | Replicated 1 KiB at 100/s, bursts and smooth | Whole-burst completion p50 16.22-18.00ms to 13.61-14.00ms and about 13% fewer fsync jobs. Smooth p50 regressed about 0.76ms | Off-by-default runtime setting `replication.stream_apply_fixed_deadline` |
| 2026-09-27 | 44094ab, ef76f61, Keratin 5e43417 | Stage arriving follower frames inside one open durability batch | Fewer follower sync rounds | Replicated 1 KiB, bursts and smooth, with a collect-first control | Burst median about 11% better, but the collect-first control using the same sync path removed that advantage. Smooth median about 7% better with about 4% extra CPU | Off-by-default runtime setting `replication.stream_apply_open_batch` modes 1 and 2 |
| 2026-09-27 | 36939dd | Drain follower frames already queued when an apply completes, without a fresh wait | Close follower sync gaps | Low-rate ABBA and short saturation ABBA | Payload sync gaps fell from about 2.4-3.2ms to 0.5-0.6ms. No consistent low-rate latency win, saturation about 1.6% better amid large variation | Off-by-default runtime setting `replication.stream_apply_open_batch` mode 3 |
| 2026-09-27 | Keratin 7a06491 | Delay the first owner commit after an idle period | One owner sync per burst | Replicated bursts, smooth and saturation | A 3ms window cut burst fsync jobs about 17% but isolated p50 rose from 8.07 to 10.65ms. Saturation averaged 130.8k against 124.9k confirms/s with large variation | Off by default, `KERATIN_IDLE_COMMIT_WINDOW_US` |
| 2026-09-27 | Keratin 6cfa5e0 | Shared batch timeline tracing across writers | Attribute storage scheduling | Small traced workloads | About 0.2 extra CPU-seconds and 7 MiB peak RSS from collection | Keep behind the `writer-stage-trace` feature |
| 2026-09-28 | Keratin 77b514c, Fibril 886f239 | Single-log queues: enqueue records carry their payloads | Simpler durable invariant and less per-queue storage work | Matched ABBA, solo and three replicas, NVMe, SATA and tmpfs, 19.5 million validated publications | NVMe below-saturation delivery improved in the burst and 8 KiB smooth shapes with lower peak memory. Physical saturation ranges overlapped widely. Tmpfs saturation lost about 3.5%. Failover readiness for a 50k live backlog rose from 2.52-2.73s to 2.91s, and for settled 50k history fell from 0.89-0.96s to 0.69s | Adopted for the simpler storage design and below-saturation gains. Breaking, fresh storage required |
| 2026-09-28 | branch aa72fb2, Keratin cef6c35 | AfterFlush: complete appends after an OS write without fsync | Measure weak-durability latency and CPU | Storage-only append latency, then three NVMe brokers at 100/s and 10k/s with early replication | Storage-only median append 2.535ms with fsync against 14.72µs without, which reflects different guarantees. The broker needed written-cache reads to progress at all. At 10k/s the weak arm had a repeatable tail and CPU regression. A 100ms background sync cut p99 from 96.6-110.2ms to 11.2-11.7ms | Keratin primitive landed as an opt-in API (Keratin 3de5c7b). The broker mode is not merged and needs written versus durable progress reporting first |
| 2026-09-28 | 3be4ac8, Keratin 456ec15 | Borrowed payload decoding during recovery inspection | Lower inspection CPU | Isolated inspection | About 32% less CPU at 128 B, 13% at 1 KiB and 7% at 8 KiB | Keep. Not a failover time claim |
| 2026-10-11 | 4426dbe, Ganglion d45e682 | Wake the controller loop on raft leader change | Act on known transport failures without waiting for the next tick | Debug builds, three local nodes, killed node owned the queue and led raft, eager failover at zero grace | With raft heartbeat 100ms and election 0.5-1s, kill to activation fell from 3.7s (two runs) to 1.9 and 2.1s. Default raft timings gave 4.9 and 5.3s, mostly election and detection | Keep. Raft timing defaults are open |
| 2026-10-13 | 555f7108, Ganglion a12ce6f | Cache each queue's accepted history by committed Raft log id | Stop rebuilding it from JSON on every replication read and progress report | Three nodes on one NVMe disk, 1 KiB, 10k/s, ABBA, `perf` with frame pointers | Broker CPU per million messages 99 to 58 durable, 212 to 83 buffered. Also about 3x less CPU at 100/s | Keep |
| 2026-10-13 | Keratin d54b9af | Serve tail cache hits for owner replication and delivery payload reads inline instead of through the blocking pool | Fewer cross-thread wakeups per message | Same setup, plus 8 queues at 2k/s each | 10k/s: 57 to 50 durable, 84 to 72 buffered, context switches per message down about a third. 8 queues: 166 to 150 durable, 437 to 350 buffered. Latency unchanged | Keep |
| 2026-10-13 | none, environment only | Fewer Tokio workers (`TOKIO_WORKER_THREADS`), and each broker pinned to 4 cores with `taskset` | Less park and unpark churn | Same setups, plus saturation and 100/s | 4 workers: 5-9% less CPU. Pinned: 17-25% less CPU at 10k/s and saturation, no throughput or low-rate latency loss, but 15-17% higher durable latency with 8 queues | Not adopted. Capping cores trades latency under load, prefer keeping related work on the same thread |
| 2026-10-13 | branch `placement-experiment`, Keratin branch `placement-experiment` | Run a partition's queue actor and its publisher, confirm, settle and delivery loops on one `LocalPoolHandle` thread, picked by partition | Fewer cross-thread wakeups without capping cores | Three nodes, 1 KiB, 10k/s, ABBA | Context switches per message rose from 1.7 to 2.1 durable and 3.2 to 4.1 buffered, CPU slightly up. Connection tasks stay on the shared runtime, so every publish and delivery crossed runtimes | Not merged. Placement only pays if connection I/O moves with the partition, which means a shard-per-core design |

## Lessons so far

- End-to-end benchmarks are good enough to catch large regressions, but they do
  not isolate small serialization or allocation changes.
- Low-rate steady runs are as important as saturation runs because batching can
  improve peak throughput while making normal latency worse.
- Traffic shape matters as much as average rate. Smooth 100/s used about 0.40
  broker cores where 200/100 bursts at the same average used about 0.014, and
  many experiments helped bursts while costing smooth traffic or the reverse.
- Per-message work only shows up where it is the bottleneck. Batching delivery
  metrics alone was neutral or worse in June, while batching the broker-to-pump
  channel hop moved the 1KB knee. Find the knee before trimming around it.
- For 1KB publish frames, the wire codec costs under 1µs and does not explain
  any measured knee.
- For internal replication reads, MessagePack was the wrong shape. Replacing the
  hot replication response with a raw binary frame carrying fixed metadata, raw
  record bytes and explicit offsets removed a large nested decode and re-encode
  cost from follower catch-up.
- Record-count limits are not enough for replication batches. Payload size can
  dominate, so byte caps must be part of the protocol and must rewrite progress
  metadata to the actual returned frontier.
- The publish sink benefits from a much shorter coalescing window than the
  original 1ms. Removing the wait entirely lost useful batching around the
  350k-400k/s knee. Measuring the window from the previous flush gives sparse
  traffic immediate sends while preserving useful batching under load.
- Connection layout changes batching. Ten publisher connections at the same
  total credit nearly doubled replicated CPU per message because each connection
  forms its own small enqueue batches.
- Storage latency after idle gaps can come from the drive's power management
  rather than the broker. Reproduce a storage cost with a standalone probe before
  changing broker scheduling, and record the APST policy with every result.
- Large payload and saturation results are hardware-specific. tmpfs measures the
  code ceiling and a physical drive measures that drive.
- Weaker completion modes are not free wins. AfterWrite and AfterFlush stalled
  because replication and delivery read only durable records. A weak mode needs
  honest written versus durable progress before its numbers mean anything.
- For replica-durable runs, report publish-to-server-receive and
  server-receive-to-deliver separately. Client confirm backlog can look like
  broker latency.
- Short paired runs often cross. Single-log queues, the owner startup window and
  several follower batching modes showed gains in one order and not the other.
  ABBA order with the same binary is the minimum for a claim.
- Correctness bugs can look like performance limits. The ten-connection
  saturation stall was a wake-up storm in ordered application, and a writer at
  the fsync pipeline cap spent a full core polling.
- Experiments switched by Cargo features and per-process environment variables
  are missing from normal builds and can disagree across nodes. Broker-level
  experiments that land become cluster runtime settings, off by default.
- Failover time after losing an owner that also leads raft is mostly raft
  election and failure detection. Seal, source selection and activation took
  about 0.3s of a 4.9-5.3s outage with default raft timings.
- Local multi-node benchmarks are useful stress tests, but they are not a full
  deployment model. They share CPU, page cache, and one drive, while also hiding
  real network cost.
- Temporary timing probes are acceptable during an audit, but they should not
  remain as ad hoc logs. Promote useful signals to cheap aggregated metrics or
  sampled diagnostics, then remove the raw per-frame logging.
- Operator metrics should stay cheap and useful. Percentile-heavy measurement
  belongs in benchmark tools unless there is a clear production need.
- A fan-out benchmark can knee inside its own client process long before the
  server. The stream r16/r32 collapse reproduced consistently, looked like a
  server congestion knee, and disappeared entirely once readers ran in
  separate processes (745k/s at p50 521ms became 3.2M/s at p50 1ms for the
  same offered load). Split or scale the load generator before trusting any
  saturation number, and treat identical goodput across very different
  offered loads as a client-side smell.

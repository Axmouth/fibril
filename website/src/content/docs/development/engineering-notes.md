---
title: Optimization and bug notes
description: Short records of measured optimizations, discovered bugs and unresolved failures.
---

Each entry records the mechanism, result or correctness effect, and a reference
for further detail. Measurements describe the stated workload and hardware.
Current capabilities are in [implemented surface](/implemented-surface/).
Remaining work is in the [roadmap](/roadmap/). Benchmark methods and the full
experiment table are in the [optimization log](/development/optimization-log/).

## Controller wake on leadership

A new raft leader slept until its next controller tick (2 s by default) before
acting on transport failures it had already seen. The controller loop also wakes
on Ganglion's leader change watch. With the killed node owning the queue and
leading raft, eager failover at zero grace and raft heartbeat 100 ms with election
0.5 to 1 s, kill to activation fell from 3.7 s to 1.9 and 2.1 s in debug builds on
three local nodes. With default raft timings it measured 4.9 and 5.3 s, mostly
election and failure detection. Fibril 4426dbe and Ganglion d45e682.

## Replication experiments as runtime settings

Early replication and three follower batching experiments were switched by Cargo
features and per-process environment variables, so normal builds lacked them and
nodes could disagree. They are cluster runtime settings. Early replication
(`replication.early_replication`) has been on by default since October 2026,
see [owner I/O failure](/development/recovery-continuation/#early-replication-under-owner-io-failure).
The batching experiments stay off by default and listed as experimental on the
Settings page: `replication.stream_apply_fixed_deadline`,
`replication.stream_apply_open_batch` and
`replication.stream_initial_credit_bytes`. Targeted queue routing is always
on. Fibril ec17a7c. Measurements for each are in the
[optimization log](/development/optimization-log/#experiment-record).

## Stream backfill on the Speculative and Ephemeral tiers

The Speculative and Ephemeral tiers put records in the fan-out ring when they are
staged, while log backfill reads durable records only. With more than a ring of
records in flight, a subscriber attaching or recovering from lag got a short log
read at the durable frontier and jumped to the ring, so the records in between
were never delivered and cursor commits moved past them. Backfill waits for the
durable frontier and joins the ring only after the whole log range is delivered,
and a by-time start at the live edge does not skip staged records. A caught-up
stream follower's long-poll also slept out its full `max_wait_ms` because stream
writes never fired the queue wakeup, slowing replica-durable stream confirms. It
wakes on the stream durability signal. Fibril commits 54378cd and 519b100, with
the Keratin side in its changelog.

## Restarted queue followers rejoin as learners

A queue follower whose process restarted put the queue into a pending recovery
with unchanged placement, closing serving until seal, inspection and install
finished. The restarted follower is suspended in the accepted history and
rejoins through learner admission while the queue keeps serving, with W
unchanged, its eligible-set slot kept and at most W-1 rejoins at once so a
recovery can still collect E-W+1 seals. Owner restarts, streams and W = 1 queues
still recover. Fibril commits c50b276, b62a00b and b7ae136, with a live
three-node test. See
[restarted queue followers](/reliability/recovery-sealing/#restarted-queue-followers).

## Failover placement and owner-only holds

Owner moves used liveness and a round-robin pick, so a whole-cluster restart could
move partitions off a late-booting node, a fresh node could become the recovery
driver and copy the whole history, a stream follower ahead only on cursor events
could win and drop confirmed records, and one follower took every partition of a
failed peer. The controller holds owner-only durable resources for their owner,
waits a startup grace for registered nodes, prefers live copy holders ranked by
record tail and spreads ties by owned partitions, with guarded operator overrides
for held owners and stuck recoveries. Controller unit and handler tests cover each
case, in commits 7fa8d2d through 247908e. See
[failure modes](/reliability/failure-modes/#choosing-the-new-owner).

## Single-lane queue replication

Queue replication carried a message lane that was always empty at offset zero,
and the broker guessed each partition's kind when reading progress, so a cold
stream misread as a queue could let cursor-event progress release record
confirms. Queues replicate one log with their own wire frames, progress is typed
by resource kind and checked against the partition's own kind, and recovery
evidence, checkpoints and stages describe one queue log. This is a breaking
change: Stroma storage layout 3, fresh coordination state and matching brokers.
Fibril commits a0be3a5 through b1123fa, with the Keratin side in its changelog.
See [replication design](/development/replication-design/#queues-replicate-one-log).

## Resumable queue recovery

Queue recovery persists exact inspection pages and completed phases, transfers
large snapshots in chunks and stages history under per-step and free-space bounds.
Downloaded source pages remain usable after source loss, with full target
verification and the existing activation quorum still required.
[Maintained acceptance cases](/reliability/recovery-sealing/#large-history-acceptance)
cross the former byte, record and snapshot limits and exercise old-owner rejoin.
Borrowed payload decoding cut isolated inspection CPU by about 32% at 128 B,
13% at 1 KiB and 7% at 8 KiB.

## Fragmented TTL replay

Enqueuing messages with distinct TTL deadlines scanned every existing deadline
to decide whether to wake the expiry worker, making reconstruction quadratic.
A coalesced notification removes that per-enqueue scan while expiry selection
still computes the exact minimum. An isolated 40,000-deadline release probe
changed from about 943 ms to 7 ms, with deadline ordering and wakeup tests.

## Single-log queue storage

Queue enqueues carry their payloads in the same ordered log as state changes,
removing the cross-log publish dependency and lost-payload suffix repair.
Recovery preserves older live payloads across snapshot boundaries and installs a
verified event prefix before admitting the new history. NVMe delivery below
saturation improved in the tested burst and 8 KiB smooth shapes with lower peak
memory, tmpfs saturation lost about 3.5%, and SATA failover readiness with a 50k
live backlog rose from 2.52-2.73 s to 2.91 s. The adoption includes crash-boundary, learner, reseed
and owner-loss coverage in
[Keratin 77b514c](https://github.com/Axmouth/keratin/commit/77b514c) and
[Fibril 886f239](https://github.com/Axmouth/fibril/commit/886f239), with a breaking
fresh-storage requirement and unchanged stream storage.

## Storage writer experiments

Three scheduling experiments from September were measured again on current
main in October 2026 and removed. Closing a batch when input drains made no
difference at 100 msg/s or 1k msg/s. Stage-then-commit, which stages input at
once and decides fsync submission afterwards, used 2.2 times the CPU per
message at 1k msg/s and 10k msg/s and raised 1k msg/s p99 from 16-17 ms to
25-31 ms, with saturation unchanged and 8 KiB bursts mixed. The idle commit
window only worked in that mode. Background periodic manifests started as an
experiment and are always on since October, when they took durable p99 at
10k msg/s from 45-47 ms to 28-33 ms. Loaded fsync spacing is the
`storage.loaded_fsync_spacing_ms` runtime setting.
`KDurability::AfterFlush` is an opt-in Keratin completion mode with no broker
setting, because a weak queue mode first needs separate written and durable
progress. Results are in the [optimization log](/development/optimization-log/).

## Writer polling at the fsync pipeline cap

When the fsync pipeline was full, the writer still woke at the expired fsync
deadline and polled its channels with a zero timeout until a completion arrived,
using a full core for the rest of a slow fsync. A September trace counted at least
957,600 such iterations. The writer blocks while the pipeline is full, since only
a completion can unblock a commit. Keratin 561cbb1.

## NVMe idle persistence latency

A standalone Linux/ext4 experiment reproduced slower syncs after idle gaps on a
Samsung 970 EVO Plus, independently of the broker. Disabling NVMe APST reduced
single-writer idle sync medians from about 9 ms to 2.69-2.73 ms in two
repetitions, while periodic manifest persistence remained a separate cost. The
[deployment procedure](/configuration/#nvme-idle-latency-on-linux) explains
the measured workload, power tradeoff and reversible controller setting.

## Bounded replication reads and preserved progress

Owner replication reads stop after the useful byte-bounded prefix and one
lookahead record, preserving snapshot deferral and ordered event gating.
Follower progress keeps a pending owner read alive, avoiding discarded blocking
scans, while reset and stop still cancel it. A three-node tmpfs comparison at
4 KiB measured lower CPU use and higher throughput, with cache, disk, oversized
record and snapshot-boundary regression coverage.

## Streaming settings and oversized credit

Owner streams adopt runtime read limits at the next batch. An oversized batch
keeps its credit debt until durable application returns those bytes, so repeated
oversized records cannot grow the window through saturating subtraction. Tests
cover live settings adoption and repeated oversized records, while the
[configuration reference](/configuration/#replication) separates queued frame
count from outstanding byte credit.

## Follower retention and non-destructive repair

Snapshot compaction could overtake admitted followers during sustained publishing,
and the checkpoint fallback correctly refused to replace their accepted history.
Owner compaction protects live follower cursors, and exhausted history starts
consensus-authorized repair in a separate generation while preserving the
original recovery evidence. Storage crash-boundary tests and a three-node repair
test cover the switch, continued majority confirms and stale-session rejection.

## Agreed queue recovery checkpoints

Opt-in background agreement creates durable same-cut queue snapshots across all
admitted replicas and advances retention only after consensus publication. Recovery
verifies the surviving snapshot and suffix while preserving live payload checks.
Large live backlogs still require payload reads. Crash, partial-installation and
confirmed-suffix tests cover the publication and recovery boundaries.

## Adoption, September 2026

### Runtime installation observations

The Settings page compares saved values with the configurations installed by the
local broker and connection updater. Independent cluster and cache revision
counters are kept separate, and startup installs the latest subscribed snapshot
before waiting for changes. Existing operations can retain earlier snapshots, so
installation does not imply adoption by every worker.

### Recovery budget diagnostics

Inspection, replay and staging failures carry numeric limits, accepted work and
refused work into the bounded recovery timeline. An unchanged retry is identified
as insufficient for these fixed limits, while unknown errors remain unclassified.
This adds observability without changing source proof, budgets or activation
requirements.

### Dashboard page cleanup

Boosted navigation releases page interaction listeners, timers, visibility
callbacks and event streams. Disposed polling and queued stream callbacks cannot
restart polling or restore an old page's scroll position.

### Ordered application wake-up contention

Normal ordered-application progress wakes only eligible offset ranges, while
reset and failure still wake invalidated operations. This removes the wake-up
storm that stalled the historical ten-connection workload, with
[benchmark results](/benchmarks/#historical-two-process-workload-reproduction)
and storage, checkpoint and recovery regressions covering the change (Keratin
d78a87c).

### Owner storage-error acceptance screen

Early replication, then an isolated experiment on the two-log queue format,
passed 48 schedules covering both logs, partial writes, fsync errors, follower
timing and abrupt process exit. No failed publish produced a success receipt or
became owner-deliverable, and confirmed prefixes survived reopen. Eight TCP
cluster cases preserved majority and three-copy requirements through owner
failure, recovery and readmission. On single-log queues the cluster owner I/O
failure test runs with the `replication.early_replication` setting. The remaining
adoption gates are tracked in the [recovery continuation plan](/development/recovery-continuation/#early-replication-under-owner-io-failure).

### Dashboard request lifetimes

Async page requests check their original page lifetime before updating controls,
including delayed response bodies and error handlers. Boosted navigation accepts
only the latest request and shares pending script loads, preventing older responses
or initializers from replacing a newer page. Regression tests complete requests out
of order and cover revisiting Settings while a save is pending.

### Observer error isolated from the dashboard

The intermittent `MutationObserver.observe` exception reproduces in the embedded
browser on script-free iframe controls with both lazy and eager loading. Those
controls contain no Fibril assets or application observer calls, isolating the
failure from the shipped dashboard. Tracing also identified observer initialization
in the browser automation layer, where further investigation belongs.

### Bounded recovery worker timelines

The recovery stage guards populate the Cluster dashboard with process-local
attempts, peer timings, outcomes and overlapping work, using the same renderer in
the docs demo. Retention is bounded to 32 attempts and 256 stages per attempt,
with omission counts and restart scope visible. Failure detection and client
reconnect remain separate measurements ([dashboard details](/admin-dashboard/#topology)).

### Prompt explicit-disconnect detection

Zero-grace eager mode monitors idle Raft connections on the active controller and
immediately verifies explicit connection loss. Successful RPCs clear suspicion
and silent failures retain heartbeat expiry. Four same-host, three-replica SATA
process-kill screens recorded suspicion 26-34 ms after injection, with
majority-durable history checks and old-owner rejoin passing. Recovery remains
separately fenced and validated. The default policy stays disabled with a
one-second grace ([settings and scope](/reliability/replication/#eager-failover)).

### Recovery wakeups, bounded concurrency and checkpoint cadence

Recovery wakes on committed metadata, and completed local admission wakes
assignment retries so follower durability reporting can resume promptly. At most
two replicas are prepared concurrently, with all source reads completed before
installation. The settled-history SATA screen improved fresh delivery from 4.15 s
to 2.00-2.02 s. Checkpoint event and append-byte triggers complement the periodic
policy, with shared dashboard age and retention diagnostics. See the
[current recovery screen](/development/failover-plan/#current-recovery-screen).

### Rust client EOF detection

The receive loop matched only `Some(frame)`, disabling its read branch on EOF
while heartbeat and command branches stayed live. Explicit EOF handling closes
the engine and fails pending requests immediately. Read-side transport errors
remain retryable, and all five SDKs pass a pending-topology EOF check. In the
single-host SATA warm-traffic screen, the original subscriber's fresh delivery
moved from roughly 5 s to 1.06 s with the same immediate-mode server.

### Prefetching recovery payload pages

A copying target reads one page ahead while appending the current page. Both
operations finish before progress is accepted, and installation still waits for
every source reader. A same-host SATA release ABBA test with a missing 64 MiB
suffix measured copying at 0.95-1.13 seconds sequentially and 0.75-0.76 seconds
with prefetch. Complete attempts fell from 1.85-2.08 to 1.70 seconds. The tradeoff
is one additional page of at most 16 MiB per active copying target, with
concurrency capped at two.

### Overlapping seal and inspection

Each of at most two replica pipelines starts inspection immediately after its own
seal succeeds. Source selection waits for all collected evidence, and every source
read finishes before installation can replace storage. Repeated recovery,
checkpoint, learner-restart and three-replica release gates cover those barriers.

### Reusing compatible retained segments

Recovery targets can reuse their exact sealed payload range after full CRC and
digest checks, and installation shares the completed stage's closed segments with
a private writable tail. The 100k-message, three-replica SATA readiness gate
improved from a fresh 23.99-second baseline to 9.31-10.34 seconds, and the
14.48-second payload-transfer phase disappeared for matching replicas. Repair
privatizes shared files before mutation, and the manifest format used for shared
segments fences older binaries ([storage and recovery boundaries](/development/recovery-internals/#compatible-retained-data-reuse)).

### Sequential sealed-history inspection

Inspection reads each frozen log once per traversal, checks record CRCs and the
complete sealed digest, and creates evidence only after full receiver
verification. Bounded sessions preserve authorization, cancellation ownership and
deadlines. Strict target-copy reads are unchanged. Single-run three-replica SATA
readiness improved from 50.72 to 24.11 seconds for 100k outstanding 1 KiB messages
and from 46.87 to 18.97 seconds for 100k settled plus one outstanding.
Retained-data copying was the main cost after this increment ([internals](/development/recovery-internals/#bounded-sequential-inspection)).

### Reusing authenticated inspection connections

Recovery inspection reuses one authenticated connection per source within a
bounded inspection, and each page still receives fresh authorization and identity
validation. Requests own their socket while in flight, so timeout, cancellation and
invalid replies discard it before a retry. The 100k-backlog SATA gate passed at
42.17 seconds after artifact reuse measured 42.50 seconds, a difference too small
to establish a speedup from these single runs.

### Reusing the selected recovery artifact

The recovery driver keeps the selected replay artifact through plan persistence
and verifies it against the committed plan before use, avoiding a second
reconstruction in the same attempt. Completed target snapshots remain preferred,
and resumed attempts reconstruct the source when no local artifact exists. The
100k-backlog SATA gate passed at 42.50 seconds to readiness, compared with 50.72
seconds before this change. Repeated recovery and restart tests also passed.

### Subscriber discovery after owner loss

A subscriber connected only to its queue owner stayed pending after that owner
died because topology advertised no surviving address. All five clients accept
fallback discovery addresses and retry temporary recovery responses during
supervised reattachment. A three-replica SATA warm-traffic test delivered fresh
probes on the original subscription 8.13 seconds after SIGKILL, with all 16,959
confirmed IDs preserved and replicas converged after restart.

### Bounded recovery pages and buffered history scans

Recovery inspection uses the existing 4096-record and 16 MiB page limits, and
Keratin buffers frozen reads while reusing bounded record scratch space. On the
three-replica SATA acceptance host, 10k-backlog readiness fell from 15.12-15.22 s
to 4.29 s. Previously timing-out 100k backlog and settled-history cases completed
in 50.72 s and 46.87 s with full ID, durable-write and restart/convergence checks.
Full-history verification and deadlines are unchanged. Later artifact, connection
and sequential-inspection improvements are recorded above.

### Raft sockets after cancellation and peer restart

A stopped embedded Raft core could keep answering fatal errors on a cached socket,
preventing retries from reaching its replacement. Requests own their socket until
a complete response arrives, and cancellation, failed exchanges and fatal replies
retire it. Two deterministic regressions and the repeated learner-restart
acceptance test cover the repair ([Ganglion 7e96cf0](https://github.com/Axmouth/ganglion/commit/7e96cf0)).

### Metadata TCP delays during recovery

Small recovery operations repeatedly spent around 170 ms waiting on quorum-backed
metadata authorization. Enabling `TCP_NODELAY` on Ganglion's accepted and dialed
TCP connections and forwarded writes reduced the measured recovery attempt from
6.75-6.96 s to 0.81-1.22 s across two controls and three changed single-host,
three-replica SATA runs with one confirmed 1 KiB message (Ganglion bcb45b8).
Durability and recovery authority checks are unchanged. Larger histories, loaded
clusters and pending-subscription recovery remain separate acceptance work.

### Node-local segment preallocation

Storage preallocation can be changed from the serving node's dashboard, with
durable overrides and revision checks. Logs sample the shared policy at segment
creation or reopen, keep existing segments unchanged, and report pending adoption
or filesystem allocation fallback. Tests cover concurrent edits, caller
cancellation, restart and reset, failed persistence, node isolation, rollover and
recovery staging.

### Dashboard settings preservation

Saving an unrelated runtime setting rebuilt an incomplete document, resetting
omitted connection, replication and Plexus stream settings to deserialization
defaults. The form edits a copy of the full loaded document, exposes every
runtime field and retains the version check. Regression tests cover preservation,
reloads, optional values, locks and duration conversion. A template coverage test
compares controls against the serialized Rust settings model so newly added
fields cannot silently miss the dashboard.

### Recovery during transient liveness loss

A metadata election could temporarily leave only one broker marked live, causing
majority placement to propose a lower confirmation threshold and then a second
recovery as heartbeats returned. The controller holds that proposal until it can
preserve the existing write requirement. Confirmed process-loss checks exercise
recovery onto two survivors. The fix and regression are in [5c6557d](https://github.com/Axmouth/fibril/commit/5c6557d).

### Replication after checkpoint admission

A recovered or newly admitted follower could start its ordinary replication worker
at zero despite having installed a checkpoint beyond the retained event head.
Accepted-history workers resume from the admitted storage's complete, idle
applied boundary, with current identity rechecked before adoption. A learner
regression confirms a new publication using that follower after checkpoint
admission. See [5c6557d](https://github.com/Axmouth/fibril/commit/5c6557d).

### Eager peer-failure detection

The active metadata controller can use repeated explicit Raft transport failures
and a reconnect grace to begin recovery before heartbeat expiry. The policy is
off by default and runtime configurable. Successful contact or a fresh heartbeat
resets suspicion, while recovery proof still gates serving. Process-kill,
brief-pause and stale-owner fencing checks cover the initial path. Packet-level
partitions and broader disruption measurements remain in the
[failover plan](/development/failover-plan/).

### Adaptive storage staging

Log staging can grow on demand, shed empty capacity and release idle allocations
through the existing writer loop. Broker measurements found comparable latency
and workload-dependent CPU and RSS effects. Capacity tracing and forced collection
on storage writer threads linked much of the idle RSS excess after bursts to
allocator retention after staging capacity was released. Fibril enables the
policy by default, with `adaptive_staging = false` as the retained-buffer opt-out.
Configuration and retention details are in [configuration](/configuration/) and
Keratin's `docs/ADAPTIVE_STAGING.md`.

### Follower transition before storage admission

Initial follower setup could fail while history admission was pending, while the
routing cache made subsequent reconciliation treat the role change as complete.
The broker retains that rejected transition's predecessor and retries against
current metadata after local admission. Removed assignments cancel the pending
work. A regression holds metadata unchanged across admission, and a second checks
assignment withdrawal.

### Recovery admission races and stale owners

Authenticated tests race learner admission against recovery and verify the
resulting witness requirement at the unchanged write threshold. An owner whose
metadata runtime has stopped cannot confirm further writes after its eligible
follower is sealed. Repeated learner broker and metadata restarts also preserve
incomplete checkpoint backfill and reject stale receipts. These tests complement
native process-kill coverage and leave packet-level partitions and whole-process
interruption during transfer as separate acceptance work.

### OpenRaft empty-read panic

An empty replication read panicked an OpenRaft 0.9.24 background task, sometimes
while the surrounding test still passed. The 0.9.25 heartbeat fallback also fails
at index zero. Ganglion carries a narrow retry patch that reports no progress for
unread data and retires old streams on step-down, with subprocess checks for
panics, eventual replication and cancellation. Dependency provenance and removal
conditions are recorded in Ganglion's `vendor/openraft/PATCHES.md`. The scheduling
cause of the original intermittent empty read remains unconfirmed.

### Background learner catch-up

Assigned replicas excluded from an activated quorum can copy and backfill the
current history while the owner continues confirming writes. A durable, fully
applied cut and complete payload dependencies gate exact-instance admission.
Unchanged sessions keep their progress and the next recovery uses the enlarged
witness set. Restart and process-kill tests cover checkpoint backfill and
publication of a new local generation with old evidence retained.

### Learner materialization role

Storage materialized a queue with an owner role before coordination adjusted it.
Learner generations retain a follower marker across eviction and restart,
preventing background timer maintenance from treating a returning copy as an
owner during catch-up. Explicit promotion remains a separate operation.

### Recovery candidate handoff

A persisted recovery could stall indefinitely after its proposed owner
disappeared, even with sufficient surviving evidence and completed transfers. The
controller selects another available member of the fixed replica set without
changing seals, staged data or quorum requirements, and activation binds the
selected owner through fresh consensus. Three-node tests cover a stopped
candidate, preserved staged data, stale-candidate rejection and resumed
replicated confirms.

### Checkpoint reset and outstanding fsync

A checkpoint reset could return before older fsync completions were consumed,
allowing one to restore a stale durable frontier after the cut. The writer drains
those completions before replacing files and resetting state. A queued-fsync
regression reproduced a frontier of one after reset to zero and verifies the
reset remains at zero.

### Empty fsync boundary

An empty fsync job used the same inclusive offset as a job covering message zero.
When its completion updated the manifest, it could invent a one-record tail, and
completion after a later append could also overstate durability. Fsync jobs and
the log's internal durable boundary use exclusive counts, with a deterministic
regression for both sequences (Keratin `d8902a7`).

### Automatic queue enrollment and interrupted preparation

Fresh cluster queues prepare and activate their initial write quorum
automatically. A guarded renewal preserves origin IDs across owner replacement
before activation, while durable storage intent makes interrupted pristine
preparation resumable. Activated histories stay on the normal recovery path.
Tests cover preparation SIGKILL boundaries, full broker and metadata restarts, and
independent follower admission after owner shutdown.

### Recovery installation and quorum activation

Recovery builds a verified storage generation and atomically publishes its route
while retaining the old sealed source. Exact process and storage receipts gate
new-quorum activation and local admission. Bounded authenticated transfer can
continue from a completed stage after source loss. Linux crash tests, resumed
majority confirmations and consecutive recoveries cover the installed path.

### Installed snapshot materialization

The first in-process installation attempt registered the replacement as a new
queue, so the actor skipped its saved snapshot despite retaining the payload log.
Installation publishes an existing-storage registry entry under lifecycle
serialization. An end-to-end regression performs two recoveries, resumes
publishing and verifies delivery of every confirmed message.

### Resumable recovery staging

Selected queue state and payloads are staged in a separate native log while the
old sealed source stays readable. Exact page retries, immutable metadata and
sequential disk verification preserve the selected baseline through restart, and
four SIGKILL boundaries and authenticated transfer are covered. A fresh consensus
plan check authorizes staging, with replacement and writer admission as separate
gates.

### Persisted queue recovery intent

A verified source selection is fixed in consensus before replacement storage work
begins. The plan binds the exact state, witnesses, continuation offsets and new
history and session IDs. Retries retain the first decision while replicas remain
fenced. Regressions cover zero boundaries, substituted evidence, stale transitions
and consensus retries across three metadata nodes.

### Verified recovery source proposals

Sealed queue replay produces deterministic snapshot artifacts that release old
leases while preserving logical state and verified live-payload identity. Explicit
source selection requires accepted origin authority, the old witness threshold,
complete source coverage and consistent comparisons. Tests cover partial replay,
divergence, crossed tails, stale transitions, older timer semantics and a single
intersecting witness over authenticated TCP.

### Durable delayed-message activation

An owner could activate a delayed publish locally, deliver it and record a NACK,
while restart or follower replay missed the activation and lost the retry
increment. Due timers move to ready through a bounded, ordered event that consumes
their heap entries, and an elapsed deadline at broker admission becomes an
ordinary enqueue. Regressions cover restart, follower application, different
checkpoint starts, idle polls and retry deadlines. Delayed retry also removes
follower-ready state before waiting.

### Sealed history authority and process replacement

Version-two seals retain the original durable storage receipt, allowing recovery
to check the exact accepted history and replica instance after restart. Recovery
witness thresholds use the eligible write set while confirmations retain the
configured write threshold. A changed registered process requests recovery even
when placement is unchanged, unless a restarted queue follower can rejoin as a
learner. Three-node authenticated TCP tests cover sealed receipt admission
after majority-confirmed replication.

### Log recovery buffer boundary

The recovery scanner counted bytes from the next read position instead of the
start of a buffered partial record, overstating the valid end when records crossed
read chunks. Keratin `d45757f` corrects the boundary, and tests cover
chunk-spanning records, padding and append after repeated reopen. Bound histories
also refuse local repair that could discard records and reuse offsets under an
unchanged writer session.

### Exact initial activation and live replication

Explicit activation admits only the prepared provider and storage instances, while
live replication carries the accepted history through pull and streaming paths. A
three-node TCP regression verifies majority confirmation, stale-identity rejection
and replacement-storage fencing.

### Replication stream rejection and cancellation

A rejected stream-start request left the follower waiting because the transport
ignored ordinary error responses. The reader reports that rejection, and both
transport tasks abort when their parent is dropped. TCP tests verify prompt exit
and socket closure.

### Authenticated initial preparation

Remote preparation verifies the exact target and committed decision through
consensus before creating a non-writable storage baseline. Requests use fresh node
authentication, bounded frames and deadlines, and identical calls share one broker
operation that survives caller cancellation. Real three-node TCP tests cover
majority receipt collection, retries, stale decisions, wrong targets and reopened
storage ([initial preparation](/reliability/recovery-sealing/#initial-history-preparation)).

### Creation-time enrollment and durable preparation evidence

Explicit enrollment blocks ordinary serving from catalogue creation while
preserving replica placement, and preparation requires that enrollment before it
can certify an empty origin. Deletion retains a retired marker so the remaining
assignment cannot accidentally regain legacy serving. Exact quorum receipts
persist across metadata restart without activating writers. Regressions cover
placement stability, retirement and recreation, conflicting receipts and owner
replacement ([initial preparation](/reliability/recovery-sealing/#initial-history-preparation)).

### Consensus preparation without writer admission

Initial-history decisions persist fixed history and session IDs bound to the owner
instance and exact assignment. Replica preparation leaves storage non-writable.
Receipt collection deduplicates replies, requires the owner and blocks
contradictory replica identities. A durable metadata restart regression covers
rejection of the old owner grant ([initial preparation](/reliability/recovery-sealing/#initial-history-preparation)).

### Storage history and writer restart

An explicit initialization primitive persists incarnation, history and
writer-session IDs before admitting pristine storage. Reopening that store blocks
ordinary access even at the same assignment epoch, while recovery sealing remains
available. Conflicting IDs and ordinary checkpoint replacement cannot relabel it.
Cancellation, corruption and Linux SIGKILL regressions cover the boundary
([storage binding](/reliability/recovery-sealing/#local-storage-history-binding)).

### Resource recreation and recovery identity

New queue and stream declarations register their incarnation ID atomically, so
concurrent declarations retain one identity and recreation after retirement
receives a new one. Conditional deletion protects identified replacements from a
delayed old delete, and recovery seal authorization rejects changed or missing
incarnation metadata. Consensus snapshot restart and transition tests cover these
cases.

### Ordered application and exact checkpoints

Queue events finish actor application in log order, allowing snapshots to capture
state with its actual exclusive event boundary. Interrupted application cannot be
cleared by changing roles, and verified repeated follower batches skip
already-applied events so NACK retries do not increment twice. Tests cover event
zero, partial batches, cancelled captures, failed writes and restart.
[Checkpoint internals](/development/recovery-internals/) describes the boundary.

### Comparing different checkpoint starts

Explicit sealed inspection verifies each snapshot and replays its event suffix to
a common target, then compares live payload identities separately from state.
Owner-local leases have a separate normalized digest, and retry counts, delays,
TTL and DLQ state retain their meaning. Authenticated TCP tests cover unequal
checkpoint starts.

### Checkpoint rejection and cancelled captures

Malformed queue checkpoints could panic on invalid ranges or a truncated
custom-DLQ group, and a rejected load could partially replace existing actor
state. Decoding validates into isolated state before replacement, and a pause
acquired for checkpoint export is released even when its caller is cancelled
while draining active work. Owner capture also serializes with role changes and
recovery sealing. Malformed-field, cancellation and real broker checkpoint tests
cover these paths.

### Restartable suffix repair preserves valid events

The recovery helper used a whole-log checkpoint reset when it intended to remove
only a bad suffix, leaving the valid prefix in memory but removing its durable
records. Suffix repair journals the cut, preserves earlier records and resumes
before normal log opening. Repeated restarts, I/O faults and six SIGKILL boundaries
cover the fix. A dangling enqueue also cannot mask later corruption or
unexplained missing settlement payloads.

### Canonical queue state from sealed replay

Explicit inspection can reconstruct fully retained queue histories at a common
exclusive event frontier and hash their complete canonical state. Payload and
input-history digests remain separate, while operation budgets and bounded CPU
workers keep this work outside live actor scheduling. Equal state supports
comparison but still requires resource lineage and checkpoint authority
([recovery sealing](/reliability/recovery-sealing/#queue-state-at-an-exact-boundary)).

### Sealed-history comparison and dependency diagnostics

Explicit pair inspection compares shared offsets across differently retained
histories and verifies the complete transferred log digests, retaining only a page
of unmatched record IDs. Whole-event reference checks expose incomplete payload
references and preserve explicit replay and checkpoint gaps. Timeout, source loss
and exhausted budgets discard partial results. Matching overlap remains subject to
common-origin and state proof before source selection
([recovery sealing](/reliability/recovery-sealing/)).

### Read-only access to sealed evidence

Recovery can read bounded pages from live or restarted sealed replicas without
opening queue actors, repairing files or lifting the seal. Every page verifies the
retained log and snapshot, while authentication, transition checks and
cancellation-safe locks preserve the recovery boundary. Full rescans are
deliberately confined to explicit recovery calls
([recovery sealing](/reliability/recovery-sealing/)).

### Transition-bound recovery witnesses

Explicit seal calls use fresh authenticated connections with whole-call deadlines,
and witness admission binds each reply to the contacted old replica and exact
pending transition. Duplicate retries count once, a smaller proposed configuration
cannot lower the old threshold, and contradictory evidence from one sealed replica
blocks the collection.

### Replication authorization boundary

Internal replication handlers accepted ordinary authenticated clients, and
unauthenticated clients when authentication was disabled. A real TCP regression
reproduced a read of retained payloads. All replication and recovery controls
require the node principal on the physical connection before decoding or changing
state. Tests cover refused reads and writes, every control opcode, resumed
sessions, authenticated catch-up and checkpoints, TLS, streaming confirms and
reauthentication after transport loss.

### Authenticated recovery requests and retained history

Explicit seal requests require node authentication on the current transport and
consensus authorization of the exact pending transition, with concurrent identical
requests sharing one operation. Storage persists a fingerprint of the frozen
records and snapshot, independent of fencing epochs and replica-local append
times. Retry reads bypass the cache so disk changes cannot hide behind cached
contents. Authentication, cancellation, stale-transition and changed-record
regressions cover this increment ([recovery sealing](/reliability/recovery-sealing/)).

### Recoverable follower checkpoint installation

In the two-log queue format, a crash between the two log resets could leave old
queue state referring to removed message bodies. A durable installation journal
resumes replacement before ordinary recovery, and a completion receipt prevents
retries from erasing later backfill. Snapshot fencing rejects stale writes after
installation or eviction. Linux fault, cancellation and six-boundary SIGKILL tests
cover [Keratin 76f1469](https://github.com/Axmouth/keratin/commit/76f1469).
Queue checkpoints install only through the authorized learner path.

### Manifest replacement crash window

Manifest storage removed the old file before renaming its replacement, allowing a
failed rename or crash to lose the persisted epoch. Direct replacement preserves
the previous manifest on rename failure and synchronizes both affected directories
on Unix. An injected-failure regression covers [Keratin 3435e3d](https://github.com/Axmouth/keratin/commit/3435e3d).
Windows metadata durability still requires separate implementation and testing.

### Replication wakeups independent of confirmation waits

Each locally durable queue batch wakes replication after its publication
dependency is registered. A previous publication waiting for remote confirmation
does not delay that notification. The notification runs once per batch and skips
a cancelled owner runtime. A regression parks a follower beyond batch A, leaves A
unconfirmed, and verifies that durable batch B wakes the follower while both
confirmations still wait for replica progress.

### Confirmed history during follower promotion (fixed for queues)

A real-storage diagnostic confirmed a batch on owner A and follower B, then
promoted empty follower C after A stopped. Placement used advisory heartbeat
tails and promotion checked only local completeness, so neither proved that C
held every confirmed batch. Replicated queue assignment changes go through sealed
recovery: the old assignment stays fenced until seals from enough old replicas
intersect every confirmed write quorum and a verified source is installed.
Stream-state recovery remains a separate gate.

### Durable local recovery seals

A persisted seal keeps queue and stream evidence intact across restart, eviction
and interrupted fencing of their logs. The storage primitive blocks ordinary
reopening and cleanup and supports identical-request retries. Failure tests cover
caller cancellation, a failed writer, partial fencing, damaged markers and startup
suffix repair in [Keratin 617e1e8](https://github.com/Axmouth/keratin/commit/617e1e8).

### Epoch persistence retries

An epoch update changed the in-memory manifest before its disk write succeeded,
allowing a retry to return success without retrying the failed write. The update
publishes the new in-memory epoch only after persistence succeeds. A filesystem
fault test verifies repeated failure followed by successful durable retry in
[Keratin 71b2f56](https://github.com/Axmouth/keratin/commit/71b2f56).

### Pending recovery before replicated assignment replacement

The controller retains the previous assignment and persists a proposed
replacement when ownership, replica membership or policy changes affect
replicated confirmation. The request survives metadata restart and is exposed in
controller status, preventing followers from switching to an unproven source
until sealing, recovery and activation complete.

### Complete durable cache tails

The log reader accepts a cache hit that contains every record up to the captured
durable frontier, even when the requested batch is larger. Incomplete coverage,
decode failures and offset discontinuities fall back to the file reader. Rollover,
eviction and reopen parity are covered by regression tests. The initial three-node
SATA/NVMe screen found overlapping throughput and latency ranges, so this change
carries no end-to-end speedup claim. See [Keratin dd7943e](https://github.com/Axmouth/keratin/commit/dd7943e).

### Queue replication dependencies and follower persistence

Queue confirmation and delivery visibility require a counted follower to cover the
queue log through the publication's complete enqueue boundary. Progress is fenced
by assignment epoch and transport session, and replacing an assignment clears its
previous proof. Stream followers drain both their record and cursor-event
writes before applying state. An interrupted or
failed apply blocks promotion until recovery or checkpoint resync.

### Checkpoint backfill and owner admission

Promotion and direct owner activation check the payload frontier required by
ready, delayed, inflight, settled and dead-letter state. Recovery keeps a
checkpoint with missing payload backfill in the follower role. Client admission
cannot perform the watcher's follower-to-owner transition. Recovery also replays
retained event zero when a legacy snapshot's inclusive zero could denote an empty
checkpoint. Interrupted replacement of the log and checkpoint state is covered by
the recoverable installation journal described above.

### Durable publication application order

Consecutive batches could reach queue state in reverse order when the earlier
durability continuation was delayed. Keratin reserves an application turn under
the append-order lock, allowing persistence to overlap while preserving state
submission order. Failed or abandoned turns fail the chain closed until recovery.
A deterministic overtaking test and the Stroma regression suite cover
[Keratin 0944dd6](https://github.com/Axmouth/keratin/commit/0944dd6).

### Settlement ownership and ignored requests

An ACK from a different consumer could remove the rightful consumer's delivery
tag, and ignored or duplicate requests could leave settlement-drain accounting
nonzero. Ownership validation and removal share the same map lock, while ignored
requests release their accounting without changing consumer credit. The
ordinary-broker regression covers wrong-consumer ACK/NACK/reject requests,
duplicate ACKs and graceful drain in [Fibril d64577d](https://github.com/Axmouth/fibril/commit/d64577d).

### Ordered benchmark confirmations

The Rust confirmation handle implements `Future`, permitting direct polling
without a per-confirmation wrapper. The steady workload offers
`--ordered-confirmations` for FIFO polling on an ordered single-partition
workload. The default and cross-broker harness retain concurrent completion
collection. This reduces benchmark bookkeeping and does not change broker
confirmation guarantees or optimize the other SDKs' transports.

### Reproducible comparison tooling

The [shared Rust harness](../../../../../benchmarks/comparison)
records message identity, offered load, completion latency and resource samples,
with isolated single-node Docker provisioning and repeatable burst arrivals.
Existing-server modes also cover pipelined request/reply and multiple
connections. Portable cluster and fault orchestration remains pending. Raw
competitor measurements remain separate from public capacity claims.

## Optimizations, September 2026

### Transparent huge pages (deployment option)

Allocation tracing found that retained log staging reserves two 16 MiB write
buffers and two 256 KiB index buffers per materialized queue, and their resident
cost can grow substantially under THP even when lightly used. Mirrored local
tests of the [allocator startup option](/configuration/#linux-memory-policy) found
lower RSS with THP disabled across paced, saturated and three-node request/reply
workloads, with workload-dependent CPU and latency costs. The option remains a
candidate for memory-sensitive deployments. Production defaults are unchanged,
and memory pressure and large active working sets require further validation.

### Storage writer channel sizing (configurable, defaults unchanged)

Crossbeam writer and notification channels allocate their full slot arrays when
each log opens. In a local 32-queue probe, reducing their capacities from 8,192
to 64 slots reduced post-declaration broker RSS from about 347 MiB to 248 MiB.
Two short 1 KiB publish/delivery runs per storage type kept throughput within
roughly 1% of the default mean on tmpfs and SATA. The startup
[writer buffer factor](/configuration/) exposes this tradeoff while retaining
the existing default. Shrinking the separate async command pipelines caused
substantial throughput losses in the same investigation.

### Targeted reads for queue routing (adopted)

Queue routing reads the committed metadata it needs through a targeted path,
reducing repeated construction and traversal of the cluster view. Six 45-second
SATA runs at 250k/s offered used 21.0% less broker CPU per confirmation, with no
consistent p99 direction. Native replication and live-repartition checks covered
routing equivalence and stale partition versions. The implementation is in
[fibril 94cc1e8](https://github.com/Axmouth/fibril/commit/94cc1e8de25d9e6819347ba86582d888d9af4427).

### Rust ACK write coalescing (adopted)

The client buffers individual ACK frames through a bounded command drain and
flushes the tail immediately when that drain ends, reducing socket writes while
preserving frame order and settlement history. In localhost tmpfs delivery-only
runs with 128-byte payloads, four readers, prefetch 16384 and 20 million preloaded
messages, mean throughput rose from about 529k to 1.03M messages/s across two runs
per variant. [fibril e713600](https://github.com/Axmouth/fibril/commit/e713600697be21ef73afa662b889128304bf5e20) includes ordering, boundary,
small-window and failed-flush coverage.

### Python and TypeScript publish buffering (adopted)

Pipelined confirmed publishes share bounded socket writes while retaining their
individual frames and confirmation results. Both clients expose count, byte and
time limits plus an immediate-write option. Python's ordinary awaited publish
keeps its immediate-send path. See [fibril e713600](https://github.com/Axmouth/fibril/commit/e713600697be21ef73afa662b889128304bf5e20).

### Prompt socket-tail flushing (adopted)

The broker flushes buffered output when both outgoing queues drain, retaining
its existing busy-batch bounds and durability gates. In same-host SATA ext4
runs with 1 KiB messages and one outstanding confirmation, mean per-run median
confirmation latency fell from about 6.0 ms to 1.4 ms. The higher-concurrency
Rust screen showed roughly 3-6% more broker CPU per message and small throughput
changes. [fibril dc878c9](https://github.com/Axmouth/fibril/commit/dc878c98d2f51f5a5da3954a470e3db7f5559c35) includes writer and replication-gate tests.

### Larger delivery cache (experiment retained, defaults unchanged)

Increasing the log tail cache from 64 MiB to 4 GiB raised cache hits to 100% in
the measured tmpfs window, while delivery stayed around 419-422k messages/s and
broker RSS rose from about 0.44 to 4.87 GiB. Faster reads shifted waiting toward
consumer-channel sends, so the experiment did not justify a larger default.

### Settlement-history removal (not adopted)

Isolated ACK experiments reduced queue-state work by avoiding full settled
history, but the broker comparisons did not establish a useful delivery gain.
Full range-compressed settlement history remains in place. Later component
isolation identified client ACK writes as a stronger delivery limit.

## Bugs and diagnostics, September 2026

### Remaining follower kept its previous owner (fixed)

After an owner change, a node that stayed a follower could receive a no-op
assignment transition and keep pulling from the previous owner. The planner
refreshes that follower when its source or epoch changes, and the worker retains
its continuation offsets while retargeting: [fibril 3fc4253](https://github.com/Axmouth/fibril/commit/3fc4253d3e5b0d837c815055915c64cf2ca7f2e8).

### Retired partitions reopened during cleanup (fixed)

Late cleanup or a stale reconnect could recreate a partition after shrink had
deleted it. Workers stop before deletion, storage cleanup avoids creating missing
partitions, and a local retirement fence rejects stale access until a fresh
assignment admits the partition again: [fibril 8edb75f](https://github.com/Axmouth/fibril/commit/8edb75febc5669f70bb34bea65d2df1e653a37ec) and
[keratin 470d9e4](https://github.com/Axmouth/keratin/commit/470d9e423968e9b40121773a5b2d13c8d1af7ea8).

### Confirmation waiter missed the final progress update (fixed)

A follower could report sufficient durable progress between the confirmation
waiter's progress check and notification registration, leaving the waiter asleep
until timeout. Registration precedes the check, with a paused-time test that
forces the old race: [fibril 7f40f92](https://github.com/Axmouth/fibril/commit/7f40f929dd2137c440f33dfe30073e54c3971cc6).

### Independent declarations produced conflicting event histories (fixed)

A broker receiving a declaration could append its own event before placement,
leaving a different record at an offset later supplied by the assigned owner.
Declarations carry settings through coordination and only assigned owners append
them. Followers receive those events through replication:
[fibril 990765e](https://github.com/Axmouth/fibril/commit/990765e77e09ce04ae504622b870803c26e1af89) and [keratin 0b75fd5](https://github.com/Axmouth/keratin/commit/0b75fd55879a7fda0592fc1f31baa770ab24fb33).

### Checkpoint reset discarded source epochs (fixed)

Checkpoint installation dropped the source epochs before destructive log resets,
allowing a stale checkpoint to pass an already-advanced local fence. Source epochs
reach storage, are validated before reset submission, and are checked again by
each writer in command order: [fibril dc878c9](https://github.com/Axmouth/fibril/commit/dc878c98d2f51f5a5da3954a470e3db7f5559c35) and
[keratin 11ef6a9](https://github.com/Axmouth/keratin/commit/11ef6a9455dc9a992a234c8d5ca8b4e9951899cd).

### Replication conflicts lacked preceding context (diagnostics added)

Overlap reports include bounded control history, offsets and effective record
identities, with payloads and headers excluded. An isolated reintroduction of the
declaration bug produced a report showing the local declaration and the incoming
enqueue at event offset zero. See the
[diagnostic format](/development/replication-design/#overlap-diagnostics)
and [keratin 11ef6a9](https://github.com/Axmouth/keratin/commit/11ef6a9455dc9a992a234c8d5ca8b4e9951899cd).

### Interrupted checkpoints and early promotion (fixed)

Isolated storage tests reproduced reopening after a message-log reset with
snapshot references to missing bodies, and local-tail promotion before
checkpoint-required message backfill completed. Evidence and test scenarios are
recorded in [fibril b14a91a](https://github.com/Axmouth/fibril/commit/b14a91a9690f543df7977f3da871d6de110c199e).
The recoverable installation journal and the payload-frontier checks on promotion
described above close both cases.

## Earlier storage finding

### Writer/fsync-worker deadlock under saturated storage (fixed)

Scheduled commits could exceed the inflight limit and block the writer while
the fsync worker was blocked returning completions, producing a circular wait.
Every fsync handoff waits for capacity by draining completions first. The stress
reproduction and fix are recorded in [keratin 3cb1218](https://github.com/Axmouth/keratin/commit/3cb121821e99aabdac35e73477b0b26a38789bd6).

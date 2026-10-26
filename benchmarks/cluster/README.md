# Cluster benchmarks

Runs Fibril on fresh local brokers, a standalone broker or a three-node
Ganglion cluster, and measures queue throughput, latency, CPU, syscalls and
context switches per message. The workload is the shared comparison client in
`benchmarks/comparison`, run once per queue against that queue's owner. Every
run validates message counts and settlement, so a fast but lossy run fails.

These are the tools behind the reference matrix in `BENCHMARKING.md` and the
experiments in the [optimization log](../../website/src/content/docs/development/optimization-log.md).

## Setup

Linux with Python 3 and `findmnt`. Build the broker and the client:

```sh
cargo build --release --bin fibril-server
(cd benchmarks/comparison && cargo build --release)
```

Use a dedicated filesystem for `--storage`, ideally the drive you care about,
with several GB free. On consumer NVMe, turn off autonomous power state
transitions or slow syncs after idle gaps will dominate latency:

```sh
echo 0 | sudo tee /sys/class/nvme/nvme0/power/pm_qos_latency_tolerance_us
```

Pass `--nvme nvme0` to `matrix.py` and it refuses to run while that is not 0.

## Run a suite

```sh
benchmarks/cluster/matrix.py reference --storage /mnt/nvme/fibril-bench --tag main --nvme nvme0
benchmarks/cluster/report.py main-reference
```

For an A/B, give each arm its own tag and line them up with
`benchmarks/cluster/compare.py TAG_A TAG_B`, which prints the median of each
case's repeats per tag.

Results go to `benchmarks/cluster/results/<tag>-<suite>-<shape>-<arm>-<run>`.
A rerun skips cases that passed and keeps failed attempts beside the new run.
Flags `matrix.py` does not know go to every `bench.py` run, so
`--linger-us 0`, `--broker-cpusets '0-3;4-7;8-11'` or
`--node-env KERATIN_EXPERIMENT_SOMETHING=1` turn a suite into an experiment,
and `--publish buffered_coalesce=window` sets a publish runtime setting.
`--replication stream_initial_credit_bytes=33554432` seeds a replication
setting at first boot, for settings read when a stream starts.

| Suite | What it compares |
| --- | --- |
| `reference` | The reference matrix on the shipped defaults: standalone and three-node saturation, 10k/s, 100/s and bursts, plus `replica_durable:3` and buffered standalone |
| `modes` | Durable against buffered queues, ABBA, at 100/s, bursts, 10k/s and saturation |
| `lowrate` | 100/s and 200/100 bursts, three runs per mode, for latency |
| `early` | Early replication off against on, durable queues |
| `cadence` | Background sync interval for buffered queues |
| `cpu` | Durable and buffered at 100/s, 10k/s and saturation, for CPU and wakeup work |
| `peak` | Saturation only, four runs per mode |
| `manyq` | Eight queues at 2k/s each, owners spread over the nodes |
| `publishers` | One queue fed by 1, 4 and 16 publisher connections at the same total rate and credit |
| `fanin` | The same with 1 and 16 connections only |
| `idle` | 0, 200 and 1000 quiet queues against the Keratin writer buffer factor, with one durable queue at 10k/s |
| `coalesce` | Publish batching, window against gap, for durable and buffered queues from 10/s to saturation. 10/s runs 5 minutes and 100/s 30 seconds |

Single runs use `bench.py` directly. `bench.py --help` lists every flag.

```sh
benchmarks/cluster/bench.py --output results/one --storage /mnt/nvme/fibril-bench \
  --rate 10000 --durability buffered
```

## Reading results

`summary.json` in each run directory carries a label (nodes, policy, queue
mode, queues, payload, rate, client credit and any non-default setting) and:

- `delivered_per_sec` and `latency_ms`, intended send time to delivery, per queue
- `cpu_seconds_per_million`, broker CPU summed over every node
- `switches_per_message`, voluntary and involuntary context switches over every
  node, the best single sign of handoff cost between threads
- `idle`, with `--idle-queues N`: RSS and thread count per node after N extra
  queues are open, and CPU and wakeups per second per node over a 10 second
  window with no traffic, before the workload starts. `--idle-queues 0` gives
  the baseline
- `sleeps_per_message_by_thread`, voluntary switches per thread name: Tokio
  workers and each log's `keratin-writer`, `keratin-fsync` and `keratin-notify`
- `task_polls_per_message`, Tokio task polls by spawn site, from a diagnostic
  build only (see [Profiling](#profiling)). A task polled again was woken again,
  so this maps the handoffs a message takes
- `write_syscalls_per_message`, `staged_batches_per_message`, `fsync_jobs_per_message`

Low-rate latency moves more between runs than throughput, so compare it over
several ABBA runs. Long sessions on one consumer drive drift: durable
saturation measured 126-130k msg/s on a rested drive and about 98k after many
hours of runs. Interleave the arms you compare, and rest or trim the drive
before reference numbers. Brokers and client share one machine and localhost, so
there are no network round trips, and nodes sharing one disk compete for
fsync. Quote numbers with their label.

## Profiling

`bench.py --perf` records one broker while the workload runs: `flat`
samples, `fp` call stacks, or `switches` for context switches with stacks.
`--perf-role follower` picks a follower instead of the owner. Only one broker
per run, because perf's per-user locked memory rarely covers more.

Allow unprivileged profiling and kernel symbols until the next reboot:

```sh
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0
```

Call stacks need frame pointers and line tables. Build a separate binary so
the timed one stays untouched:

```sh
CARGO_TARGET_DIR=target-profile CARGO_PROFILE_RELEASE_DEBUG=line-tables-only \
  RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release --bin fibril-server
```

Task polls by spawn site need no root at all. Build the diagnostic binary
and point `--bin-dir` at it:

```sh
CARGO_TARGET_DIR=target-polls RUSTFLAGS="--cfg tokio_unstable" \
  cargo build --release --features task-polls --bin fibril-server
```

That build also times every poll. `long_polls_by_node` in the summary lists
the spawn sites whose polls held a worker for 1 ms or more, as polls over
1 ms, polls over 4 ms, total ms and the longest in ms. Tasks queued behind a
long poll on the same worker wait for it.

Heap by allocation site comes from a build with the dhat allocator in place
of mimalloc. Each broker writes `dhat-heap.json` in its node directory when
the bench stops it, so pass `--keep-data`. The profiler slows the broker
several times over, so use rates well below what a normal build sustains:

```sh
CARGO_TARGET_DIR=target-heap cargo build --release --features heap-profile --bin fibril-server
```

The profile opens in the dhat viewer. Bytes live at the global peak, by
allocation site, are the figures to compare.

Every run also records replication pipeline counters as `pipeline` in the
summary, one entry per node, over the measured window:

- Owner: reads capped by credit rather than the read limit, records and KiB
  per pushed frame, read and send times, and time parked on zero credit.
- Follower: frames, records and KiB per merged apply, how often another
  frame was already waiting when an apply finished, and time spent idle,
  collecting, applying and returning progress.
- Owner publish sink: messages per batch, idle wait for a batch's first
  request, gathering, the append call and handing confirms over. Per
  message, time from append to the owner's own sync and to the replicated
  confirm.
- Runtime probe: how late a 1 ms sleep woke and how long a yield took to be
  polled again. Tokio rounds the sleep to its 1 ms tick, so lateness up to
  about 1 ms is normal.

Times are power of two histograms, reported as a mean, bucket upper bounds
for p50 and p99, and counts over 1 ms and 4 ms.

For perf recordings, point `--bin-dir` at `target-profile/release` and read the recording
with the scripts in `profile/`:

| Script | Shows |
| --- | --- |
| `subsystems.py` | Inclusive CPU share per broker subsystem |
| `syscalls.py` | CPU share per system call |
| `switches.py` | Where threads went to sleep, for `--perf switches` |
| `frames.py` | The innermost Fibril, Stroma, Keratin or Ganglion frame, optionally only under one hot symbol |

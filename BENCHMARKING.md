# Benchmarking Cookbook

How to measure Fibril throughput and latency without re-deriving the setup each
time. Covers the harness scripts under `scripts/`, copy-paste recipes, what each
metric means, and the ceilings you will hit (and why).

All harnesses take environment overrides and print a parsed summary; the raw
results/log paths are echoed at the end of every run. For a one-line index of
every script in the repo (not just benchmarking), see
[`scripts/README.md`](scripts/README.md).

## Test environment (the box behind the numbers)

Every concrete number in this doc came from one machine. Throughput and latency
are hardware-bound, so treat these as shape/relative results, not absolutes, and
re-measure on your target hardware. The reference box:

- **CPU:** AMD Ryzen 9 5950X, 16 cores / 32 threads
- **RAM:** 60 GiB
- **OS:** Ubuntu 26.04 LTS, kernel 7.0.0-27-generic
- **CPU governor:** `performance`
- **Durable storage under test:** Samsung SSD 970 EVO Plus 2TB (consumer NVMe,
  **no power-loss protection** — so fsync must reach NAND, which is why an
  extending-segment `fdatasync` costs ~2.4ms here; a PLP enterprise drive that
  acks from DRAM would be far lower), ext4 partition on a dedicated scratch mount
- **Zero-fsync isolation:** tmpfs (`/dev/shm`)

Payloads are 128 B unless noted; runs are single-box (client and broker share the
32 threads), which caps absolute throughput versus a dedicated client host.

## Prerequisites

- **Release build.** Always benchmark `--release`. Every harness builds the
  binaries it needs unless you set `BUILD=0` (do that to skip a rebuild when the
  binaries are already current).
- **Storage matters more than anything else** for durable workloads. A harness
  that writes to a `mktemp` dir usually lands on tmpfs or the OS disk. To measure
  a specific device (an nvme scratch mount, the repo SSD, `/dev/shm` tmpfs), point
  the server's data dir at it (see each script's data-dir env below). tmpfs makes
  fsync ~free, so it isolates code cost from storage cost but is not a real
  durability number.
- **CPU governor.** On a laptop the default can be `powersave`. Set
  `performance` for stable numbers (`sudo cpupower frequency-set -g performance`).
  It moved the low-load floor by ~2ms in testing but is not the dominant factor
  (storage is).
- **Background jobs / sandbox.** When running a bench from an automated/background
  context, the sandbox can block the binaries; pass `dangerouslyDisableSandbox`
  on the Bash call. Kill leftover servers with `pkill -x fibril-server` (use
  `-x` for exact match, not `-f`).
- **Watch disk fill.** Long high-rate durable runs grow segments fast; check the
  target filesystem (`df -h <mount>`) between runs, especially on a small nvme
  scratch partition.

## The harnesses

| Script | Measures | Key env |
|---|---|---|
| `scripts/bench-steady-c.sh` | Queue steady-state: publish/confirm/deliver latency + rates, at a fixed target rate, over TCP | `RATE_PER_SEC`, `SIZE`, `WRITERS`, `READERS`, `CONFIRMED`, `SERVER_DATA_DIR`, `SERVER_CONFIG` |
| `scripts/bench-matrix.sh` | Runs named scenarios (rate/payload sweeps) and writes one result/log per case + a summary table | scenario args (`smoke`, `baseline`, `confirmed`, `throughput-1k`, `payload`, `large-backlog`, `all`), `OUT_DIR` |
| `scripts/bench-results-table.sh` | Renders a Markdown table from one or more `*.results.txt` files | result files as args |
| `scripts/bench-e2e-c.sh` | End-to-end: publish N messages, drain, report throughput; `FIREHOSE=1` drops reader latency tracking to isolate broker delivery | `MESSAGES`, `CLIENTS`, `SIZE`, `FIREHOSE`, `CONFIRMED` |
| `scripts/bench-stream.sh` | Plexus stream steady-state (the stream counterpart of steady-c); pick tier with `DURABILITY`, device with `DATA_DIR` | `DURABILITY`, `RATE_PER_SEC`, `PARTITIONS`, `DATA_DIR` |
| `clients/bench-plexus-fanout.sh` | Plexus fan-out with one OS process per reader/writer (honest multi-core fan-out; single-process understates it) | `CLIENT` (rust/go/python/ts/csharp), `READERS`, `WRITERS`, `DURATION` |
| `scripts/fsync-probe.py` | Raw `fdatasync`/`fsync` cost on a device, in-place vs extending vs preallocated | `--extend`, `--fallocate`, `--gap-ms` |

### `bench-steady-c.sh` environment

Defaults in parentheses. `BENCH_MODE` is `mixed` (publish + consume), `publish-only`
(isolate publish/confirm), or `consume-drain` (preload then measure drain).

```
WRITERS(10) READERS(10) BENCH_MODE(mixed) RATE_PER_SEC(100000)
WARMUP_SECS(5) DURATION_SECS(30) DRAIN_TIMEOUT_SECS(10)
SIZE(1024) PREFETCH(16384) CONFIRMED(0) CONFIRM_WINDOW(1024)
BUILD(1) START_SERVER(1) BROKER_ADDR(127.0.0.1:9876) ADMIN_ADDR(127.0.0.1:8081)
DURABILITY_LABEL(local) TOPIC(topic1) PRELOAD_MESSAGES(100000) PRELOAD_CONFIRMED(1)
SERVER_DATA_DIR(<workdir>)  # where the started server keeps segments
SERVER_CONFIG()             # optional server --config (e.g. to set preallocation)
LOG_FILE RESULTS_FILE       # override output locations
```

Note: `bench-stream.sh` calls the equivalent data-dir knob `DATA_DIR`, not
`SERVER_DATA_DIR`.

## Recipes

### Single steady run (latency at a fixed rate)

```bash
RATE_PER_SEC=5000 SIZE=128 CONFIRMED=1 WRITERS=8 READERS=8 \
  DURATION_SECS=10 scripts/bench-steady-c.sh
```

### Rate/payload sweep with a summary table

```bash
scripts/bench-matrix.sh baseline confirmed          # 1KB 50k/150k, unconf + conf
scripts/bench-matrix.sh throughput-1k               # 1KB 250k..500k
scripts/bench-matrix.sh payload                     # 8KB/64KB/512KB/1MB
# -> writes bench-results/steady-<ts>/ with per-case files + summary.md
```

Render a table from arbitrary result files:

```bash
scripts/bench-results-table.sh bench-results/steady-*/**.results.txt
```

### Durable run on a specific device (nvme scratch)

```bash
RATE_PER_SEC=500 SIZE=128 CONFIRMED=1 DURATION_SECS=10 \
  SERVER_DATA_DIR=/mnt/nvme-scratch/fibril-bench \
  scripts/bench-steady-c.sh
```

### Segment preallocation A/B (durable low-latency lever)

Preallocation makes each fsync hit an already-allocated segment (in-place,
~0.67ms) instead of an extending one (~2.4ms), which cuts the durable-publish
floor at low load. Toggle it with a server config:

```bash
printf '[storage.keratin]\nsegment_preallocate_bytes = 67108864\n' > /tmp/prealloc.toml

# OFF
RATE_PER_SEC=500 SIZE=128 CONFIRMED=1 DURATION_SECS=10 \
  SERVER_DATA_DIR=/mnt/nvme-scratch/ab scripts/bench-steady-c.sh
# ON (64 MiB preallocation chunk)
RATE_PER_SEC=500 SIZE=128 CONFIRMED=1 DURATION_SECS=10 \
  SERVER_DATA_DIR=/mnt/nvme-scratch/ab SERVER_CONFIG=/tmp/prealloc.toml \
  scripts/bench-steady-c.sh
```

`segment_preallocate_bytes` is the chunk size preallocated ahead of the write
cursor (0 = off; may equal the segment max). See the interpretation section for
where the win shows up (spoiler: low load only).

### Saturation (uncapped throughput)

Set the target rate far above what the box can do so the client never throttles;
the measured publish/receive rate is the actual ceiling.

```bash
RATE_PER_SEC=5000000 SIZE=128 CONFIRMED=1 WRITERS=8 READERS=8 \
  DURATION_SECS=10 scripts/bench-steady-c.sh
```

### Isolate broker delivery throughput (firehose)

```bash
MESSAGES=1000000 CLIENTS=10 SIZE=1024 FIREHOSE=1 scripts/bench-e2e-c.sh
```

### Plexus streams

```bash
DURABILITY=durable RATE_PER_SEC=100000 PARTITIONS=1 scripts/bench-stream.sh
CLIENT=go READERS=3 WRITERS=1 DURATION=10 clients/bench-plexus-fanout.sh
```

### Raw fsync cost of a device

```bash
python3 scripts/fsync-probe.py --path /mnt/nvme-scratch/probe            # in-place
python3 scripts/fsync-probe.py --path /mnt/nvme-scratch/probe --extend   # extending (grows file)
python3 scripts/fsync-probe.py --path /mnt/nvme-scratch/probe --fallocate # preallocated then append
```

## Reading the metrics

`bench-steady-c.sh` prints (per the `steady_c` client):

- **Actual measured publish rate** / **Measured receive rate** — sustained
  msgs/s over the measurement window (warmup excluded). Equal when the reader
  keeps up; a receive rate below publish means the reader (or delivery path) is
  the bottleneck.
- **Latency publish->server-receive** — client send to broker ingest. Small
  (~1ms); if it grows you are network/ingest bound, not storage bound.
- **Latency publish confirmation** — send to durable-confirm ack. This is the
  **durability latency**; it tracks fsync cost on the target device.
- **Latency publish->deliver** — send to consumer delivery (end to end).
- **Latency server-receive->deliver** — broker-internal ingest to delivery.
- **Sent/Confirmed/Received total**, **Publish/Confirm errors**, **Measured
  missing** — correctness counters. Missing/errors must be 0 for a clean run.
- **Server RSS avg/peak MiB** — sampled once/sec while the server runs.

The harness also greps the server log for `WARN|ERROR|panic|failed` and prints
matches. A durable read bug (e.g. reading into preallocated padding) shows up
here as repeated `BadMagic` errors — a clean run has none.

## Known ceilings and how to interpret them

All numbers below are from the reference box in
[Test environment](#test-environment-the-box-behind-the-numbers) (Ryzen 9 5950X,
970 EVO Plus NVMe, single-box). They are shapes to expect, not absolutes.

- **The architecture floor is sub-millisecond; storage is the wall.** Run the same
  sweep on tmpfs (`/dev/shm`, fsync ~free) to see the code cost with storage removed:
  at 500/s confirm and deliver p50 are both ~0ms (vs nvme ~1-3ms), so the entire
  hop chain (writer -> fsync worker -> notifier -> oneshot -> broker task -> client)
  is <1ms. On real disk, storage dominates the latency at *every* rate tested — at
  50k/s the tmpfs floor is ~4ms vs nvme ~17ms.
- **Low-load durable floor is the extending fsync.** Isolated `fdatasync` is ~0.67ms
  in-place but ~2.4ms on an extending segment (block allocation + extent metadata to
  NAND on a consumer drive with no power-loss protection). That gap is the low-load
  floor and what preallocation removes.
- **Preallocation is a low-latency lever, not a throughput lever.** Measured on
  nvme, 128B, confirmed: at 500/s it cuts confirm p50 ~5ms -> ~1ms; by ~5k/s the
  win is ~20%; by 50k/s it is gone; at saturation it does not move the ceiling.
  The reason: as rate rises the cost shifts from per-fsync *extending metadata*
  (what prealloc removes) to fsync *throughput/count* (what it does not). Use it for
  latency-sensitive, lightly-loaded durable topics.
- **On nvme, saturation is storage-bound (fsync throughput), not delivery-bound.**
  Measured 128B/8w/8r: nvme tops out ~420k msgs/s with delivery keeping up fine
  (deliver p50 ~21ms). Remove storage (same run on tmpfs) and publish jumps to
  ~763k, and *there* the delivery path finally backlogs (deliver p50 ~900ms while
  confirm stays ~8ms). So the delivery-path ceiling (~763k) is nearly 2x the nvme
  fsync-throughput cap — meaning faster storage (a PLP enterprise nvme that acks
  fsync from DRAM) is the highest-leverage lever: it raises the nvme cap toward the
  delivery ceiling with zero semantics change. (Payload-dependent; larger payloads
  hit bandwidth sooner.)
- **The batcher self-adapts.** At low load it commits ~1 record per fsync (no
  added latency); under load it fuses thousands per fsync (throughput). So the
  same knob set gives low latency when idle and high throughput when busy.
- **Watch instrumentation on hot paths.** A `#[tracing::instrument]` at INFO on
  the per-publish path was once the entire single-client ceiling; match span level
  to call frequency. If a number looks mysteriously low, check for hot-path spans.

## Gotchas checklist

- Benchmark `--release`; set `BUILD=0` only when binaries are current.
- Point the server data dir at the device you mean to measure (`SERVER_DATA_DIR`
  here, `DATA_DIR` in `bench-stream.sh`); a bare `mktemp` is probably tmpfs.
- tmpfs = free fsync (good for isolating code cost, not a durability number).
- `pkill -x fibril-server` (exact match) to clean up stragglers.
- From automation, pass `dangerouslyDisableSandbox` so the binaries can run.
- `df -h <mount>` between long durable runs; segments grow fast.
- Set the CPU governor to `performance` for stable low-load latency.
- Correctness counters (missing/errors) and the log `ERROR` grep must be clean, or
  the throughput number is meaningless.

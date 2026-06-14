---
title: Benchmarks
description: Early performance observations for Fibril.
---

Current benchmark numbers are informal architecture checks, not claims of production capacity.

## Early observations

Internal measurements with the current TCP transport and durable path on a single Ubuntu node have observed:

| Workload | Observation |
| --- | --- |
| Ingress | roughly `250k+` messages/sec |
| Egress | roughly `250k+` messages/sec |
| Payload | 1KB messages |
| Machine | Ryzen 5950X |

Memory usage during these runs ranged from a few hundred MB at lower load to roughly 1-2GB near peak throughput, depending on queue depth, batching, and inflight state.

## Interpretation

These numbers are useful mostly as a sanity check:

- the durable path is not obviously too slow
- batching and the queue execution model are promising
- memory behavior still needs tuning
- larger payloads will shift bottlenecks toward memory, copying, storage, and network I/O

## Missing benchmark work

The project still needs:

- broader payload-size sweeps across more hardware
- durability-setting comparisons
- richer latency histograms and structured output
- restart/replay timing
- multi-consumer fairness and backpressure scenarios

## Current TCP benchmark

The current TCP-layer benchmark helper is `e2e_c`. It is still an early
benchmark, but it now reports wall throughput, active receive throughput, sent
and received counts, missing receive count, retry metadata observed on
delivered messages, and latency percentiles from publish time to reader
delivery.

For a quick local run, use the benchmark script:

```sh
MESSAGES=500000 CLIENTS=10 SIZE=1024 PREFETCH=16384 scripts/bench-e2e-c.sh
```

The script builds the release server and benchmark binary, waits for
`/healthz`, runs a small warmup so lazy queue setup is out of the main
measurement path, then starts a reader and writer for the measured run.

Useful knobs:

| Variable | Default | Meaning |
| --- | --- | --- |
| `MESSAGES` | `500000` | Messages per client in the measured run |
| `CLIENTS` | `10` | Parallel reader and writer client count |
| `SIZE` | `1024` | Raw payload size in bytes |
| `PREFETCH` | `16384` | Reader subscription prefetch |
| `WARMUP_MESSAGES` | `1000` | Warmup messages before the measured run |
| `READY_SETTLE_SECONDS` | `0.5` | Pause after reader readiness before starting the writer |
| `IDLE_TIMEOUT_MS` | `10000` | Reader idle timeout before reporting partial receive counts |
| `CONFIRMED` | `0` | Set `1` to wait for publish confirmations for correctness/debug checks |
| `LOG_FILE` | temporary file | Build, server, and noisy runtime logs |
| `RESULTS_FILE` | temporary file | Deterministic benchmark summary and queue snapshots |

The helper can also be run manually. Start the server in one terminal:

```sh
cargo run --release --bin fibril-server
```

Start the writer in another terminal:

```sh
cargo run --release --bin e2e_c -- -m 500000 -c 10 --writer --size 1024
```

Start the reader in a third terminal, as close to the writer start time as
practical:

```sh
cargo run --release --bin e2e_c -- -m 500000 -c 10 --reader --prefetch 16384
```

The reader side prints latency percentiles when it receives messages. If the
reader goes idle before receiving the target count, it reports the partial
count and missing count instead of waiting indefinitely. The wall throughput
includes any idle timeout tail, while active receive throughput uses the span
between the first and last received message. Retry counts are read from
Fibril's reserved delivery metadata headers when present. Structured benchmark
output and scenario tables are still future work.

## Steady-state TCP benchmark

The burst benchmark intentionally lets writers run as fast as possible. That is
useful for saturation checks, but it can build backlog and make latency look
larger as the message count grows.

For latency at a controlled offered load, use the steady-state helper:

```sh
WRITERS=10 READERS=10 RATE_PER_SEC=100000 WARMUP_SECS=3 DURATION_SECS=10 \
  SIZE=1024 PREFETCH=16384 scripts/bench-steady-c.sh
```

The steady helper runs readers and writers in one coordinated process. It marks
warmup messages separately, measures only the configured steady window, and
prints both publish-to-delivery and server-receive-to-delivery latency. The
wrapper also writes full server logs and full benchmark results to files, then
prints a compact summary including publish and confirmation error counts plus
server RSS average and peak sampled during the benchmark run.

The wrapper starts a local `fibril-server` on the default broker and admin
ports. Run one wrapper benchmark at a time. A second run will fail if those
ports are already occupied.

When `CONFIRMED=1`, writers still run with pipelined publish confirmations by
default. Set `CONFIRM_WINDOW=1` if you specifically want the older serial
"publish, wait, publish" shape.

Useful knobs:

| Variable | Default | Meaning |
| --- | --- | --- |
| `WRITERS` | `10` | Parallel writer clients |
| `READERS` | `10` | Parallel reader clients |
| `RATE_PER_SEC` | `100000` | Target aggregate publish rate |
| `WARMUP_SECS` | `5` | Warmup duration excluded from measured results |
| `DURATION_SECS` | `30` | Steady measurement duration |
| `DRAIN_TIMEOUT_SECS` | `10` | Time allowed for measured messages to drain |
| `SIZE` | `1024` | Raw payload size in bytes |
| `PREFETCH` | `16384` | Reader subscription prefetch |
| `CONFIRMED` | `0` | Set `1` to require broker publish confirmations |
| `CONFIRM_WINDOW` | `1024` | In-flight confirmations per writer when `CONFIRMED=1` |
| `BUILD` | `1` | Set `0` to skip rebuilding release binaries |
| `LOG_FILE` | temporary file | Build, server, and noisy runtime logs |
| `RESULTS_FILE` | temporary file | Deterministic benchmark summary and queue snapshots |

Memory numbers are sampled from the local `fibril-server` process RSS once per
second during the wrapper benchmark. The average is over the sampled run period,
not precisely only the warmup-excluded steady window.

For repeatable local sweeps, use the matrix helper:

```sh
scripts/bench-matrix.sh smoke
scripts/bench-matrix.sh baseline confirmed
scripts/bench-matrix.sh throughput-1k payload
```

The matrix helper builds the release server and benchmark binary once, then
runs named steady-state cases with one results file and one log file per case.
It also writes `summary.md`, a Markdown table generated from those result
files. Set `OUT_DIR=...` to choose where files go. Without arguments, it runs
only the quick `smoke` scenario.

Available scenarios:

| Scenario | Purpose |
| --- | --- |
| `smoke` | Short low-rate sanity check |
| `baseline` | 1KB 50k/s and 150k/s unconfirmed |
| `confirmed` | 1KB 50k/s and 150k/s with pipelined confirmations |
| `throughput-1k` | Higher-rate 1KB exploratory sweep |
| `payload` | 8KB, 64KB, 512KB, and 1MB spot checks |
| `large-backlog` | Large-payload cases expected to build backlog |
| `all` | `baseline`, `confirmed`, `throughput-1k`, and `payload` |

To regenerate a table from existing result files:

```sh
scripts/bench-results-table.sh bench-results/steady-*/baseline-*.results.txt
```

### How to read the numbers

The offered rate is the requested publish rate, not a guarantee that the broker
or machine can keep up without queueing. When actual measured rate reaches the
target but latency climbs, the run is usually showing backlog, not low-latency
capacity. When actual measured rate falls below target, writers or the machine
could not sustain the requested input rate.

`Measured missing` should usually be zero. Non-zero values mean the benchmark
stopped before every measured message was delivered, commonly because the drain
timeout expired or the run failed.

RSS is sampled from the server process only. It excludes benchmark client
memory and the operating system page cache, so it is useful for comparing local
runs but not a full machine-memory budget.

Quick validation run from June 7, 2026, using `WRITERS=10`, `READERS=10`,
`SIZE=1024`, `PREFETCH=16384`, `WARMUP_SECS=2`, and `DURATION_SECS=5`:

| Mode | Target rate | Actual measured rate | Missing | publish→deliver p50/p95/p99/max | server-receive→deliver p50/p95/p99/max | Errors | End queue |
| --- | ---: | ---: | ---: | --- | --- | ---: | --- |
| unconfirmed | 50k/s | 50,000/s | 0 | 15 / 22 / 25 / 58 ms | 10 / 16 / 18 / 20 ms | 0 | ready=0, inflight=0 |
| unconfirmed | 150k/s | 149,999/s | 0 | 11 / 15 / 17 / 56 ms | 9 / 13 / 14 / 17 ms | 0 | ready=0, inflight=0 |
| confirmed, window=1024 | 50k/s | 50,010/s | 0 | 14 / 19 / 21 / 28 ms | 11 / 15 / 16 / 21 ms | 0 | ready=0, inflight=0 |
| confirmed, window=1024 | 150k/s | 149,999/s | 0 | 12 / 16 / 17 / 21 ms | 10 / 13 / 15 / 18 ms | 0 | ready=0, inflight=0 |

Higher-rate exploratory sweep from the same run shape:

| Mode | Target rate | Actual measured rate | Missing | publish→deliver p50/p95/p99/max | Notes |
| --- | ---: | ---: | ---: | --- | --- |
| unconfirmed | 250k/s | 250,017/s | 0 | 13 / 16 / 17 / 55 ms | Clean short run |
| unconfirmed | 350k/s | 350,002/s | 0 | 79 / 114 / 122 / 130 ms | Latency knee starts showing |
| unconfirmed | 400k/s | 400,000/s | 0 | 792 / 1060 / 1096 / 1107 ms | Drains, but backlog-driven |
| unconfirmed | 450k/s | 449,953/s | 0 | 1807 / 2038 / 2055 / 2066 ms | Drains, high latency |
| unconfirmed | 500k/s | 499,955/s | 0 | 2520 / 2783 / 2815 / 2833 ms | Drains, high latency |
| unconfirmed | 600k/s | 599,916/s | 0 | 3806 / 4546 / 4579 / 4600 ms | Drains, very high latency |

For this short local run, the practical low-latency region is below roughly
350-400k/s for 1KB messages. Above that, the broker can still drain the run,
but latency reflects backlog building during the measurement window.

Pipelined confirmed publishes follow the same pattern. With
`CONFIRM_WINDOW=1024`, a 400k/s target reached about 385k/s. Raising the window
to `4096` reached the 400k/s target, and 450k/s also reached target, but latency
rose into the 1-2 second range. Larger windows are useful for saturating the
path while preserving publish confirmation correctness. They are not a latency
optimization.

Payload-size spot checks on the same SATA SSD development machine:

| Payload | Target rate | Actual measured rate | Missing | publish→deliver p50/p95/p99/max | Server RSS avg/peak | Notes |
| ---: | ---: | ---: | ---: | --- | --- | --- |
| 8KB | 50k/s | 50,010/s | 0 | 14 / 17 / 19 / 61 ms | not sampled | Clean short run |
| 8KB | 150k/s | 139,987/s | 0 | 2608 / 3117 / 3168 / 3245 ms | not sampled | Could not reach target, backlog-driven |
| 64KB | 10k/s | 10,000/s | 0 | 18 / 22 / 23 / 32 ms | not sampled | Clean short run |
| 64KB | 20k/s | 18,285/s | 0 | 1605 / 1891 / 2144 / 2277 ms | not sampled | Could not reach target, likely storage-bandwidth bound |
| 512KB | 1k/s | 999/s | 0 | 27 / 34 / 39 / 47 ms | 262.9 / 310.2 MiB | Clean short run |
| 512KB | 2k/s | 2,000/s | 0 | 1165 / 1669 / 1756 / 1841 ms | 951.4 / 1538.0 MiB | Drains, but backlog-driven |
| 1MB | 500/s | 498/s | 0 | 33 / 45 / 51 / 63 ms | ~290 / ~334 MiB | Clean short run. Reruns varied slightly |
| 1MB | 1k/s | 1,000/s | 0 | 1812 / 2539 / 2693 / 2801 ms | 847.0 / 1187.5 MiB | Drains, but backlog-driven |

For larger payloads, the bottleneck shifts away from message scheduling and
toward memory copying, TCP throughput, and especially durable storage
bandwidth. On this SATA SSD machine, 64KB at 10k/s is already roughly 625 MiB/s
of application payload before protocol, replication within the durable path,
and filesystem overhead. Treat payload-size numbers as hardware-specific.
The large-payload memory samples also show the expected split: clean runs can
stay in the low hundreds of MiB, while backlog-driven runs retain much more
payload data in-process and can exceed 1 GiB RSS.

Recent local exploratory run, using `WRITERS=10`, `READERS=10`, `SIZE=1024`,
`PREFETCH=16384`, `WARMUP_SECS=3`, and `DURATION_SECS=10`:

| Target rate | Actual measured rate | Missing | publish→deliver p50/p95/p99/max | server-receive→deliver p50/p95/p99/max | End queue |
| ---: | ---: | ---: | --- | --- | --- |
| 50k/s | 49,962/s | 0 | 17 / 25 / 29 / 63 ms | 11 / 17 / 19 / 52 ms | ready=0, inflight=0 |
| 100k/s | 99,785/s | 0 | 13 / 18 / 62 / 136 ms | 10 / 14 / 57 / 130 ms | ready=0, inflight=0 |
| 150k/s | 149,831/s | 0 | 12 / 17 / 173 / 225 ms | 10 / 14 / 171 / 222 ms | ready=0, inflight=0 |
| 200k/s | 199,733/s | 0 | 13 / 78 / 259 / 294 ms | 11 / 76 / 258 / 292 ms | ready=0, inflight=0 |
| 250k/s | 249,591/s | 0 | 14 / 260 / 367 / 397 ms | 12 / 258 / 365 / 394 ms | ready=0, inflight=0 |
| 300k/s | 298,528/s | 0 | 18 / 578 / 623 / 659 ms | 16 / 577 / 613 / 655 ms | ready=0, inflight=0 |

These older results remain useful as a development checkpoint, but the newer
short sweeps above are a better current summary: the broker can drain
substantially higher short-run rates, while the practical low-latency region
depends heavily on payload size, durable storage bandwidth, and whether backlog
is allowed to build. Treat all tables here as reproducible local checkpoints,
not capacity promises.

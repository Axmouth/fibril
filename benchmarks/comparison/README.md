# Repeatable broker comparison

A shared asynchronous Rust queue workload for Fibril, NATS JetStream and RabbitMQ. The Rust executable owns scheduling, confirmation credit, payload generation, counting, validation and latency measurement. Small protocol adapters use each project's client library. Python and Docker Compose provision isolated brokers, collect resource samples, verify settlement and generate a Markdown table.

The automated Compose runner supports one broker, one queue, one stored copy, one publisher connection and one consumer connection. Run the systems sequentially on the same host and filesystem. These measurements describe the complete client/broker setup. A shared runtime and algorithm reduce harness differences; client implementations can still limit throughput.

## Run

Prerequisites: Linux, Rust 1.96+, Python 3, Docker with Compose, `findmnt`, `lscpu`, and access to Docker without an interactive password prompt. Cgroup v2 enables container memory/CPU accounting; unavailable metrics are reported as unavailable. The harness itself is ordinary async Rust. The local orchestration and resource collection currently target Linux.

From the repository root:

```sh
# One command: compile the locked harness, pull pinned images if needed,
# run all three brokers sequentially, validate, clean up, write results/TABLE.md.
benchmarks/comparison/run.sh --rate 1000 --warmup-secs 2 --duration-secs 10

# A saturation run: issue as quickly as the common confirmation window allows.
benchmarks/comparison/run.sh --saturation --repeats 3

# Put all brokers' fresh bind-mounted data on the selected filesystem.
benchmarks/comparison/run.sh --rate 20000 --payload-bytes 16384 \
  --data-root /path/on/ssd --repeats 3

# Explicitly include the weaker JetStream timed-sync profile.
benchmarks/comparison/run.sh --broker nats --nats-sync 2m --saturation
```

The default Fibril image is pinned to revision `12db18b16f29fe6459447c26578c3c927343eb40`; it is a reproducible image baseline, **not a build of the current checkout**. Default competitor images are NATS 2.15.0 and RabbitMQ 4.3.6. All image digests are in `run.py`; actual image IDs, version responses, image labels and container configuration are saved for every invocation. Override `--fibril-image`, `--nats-image` or `--rabbitmq-image` to test another build. Mutable tags are resolved to an immutable local image ID before starting runs.

To test the current inline Fibril backend, build without the experimental `async-io` feature, then use that executable:

```sh
cargo build --release -p fibril --bin fibril-server
benchmarks/comparison/run.sh --rate 1000 \
  --fibril-bin "$PWD/target/release/fibril-server"
```

The broker's normal source build uses the repository's sibling Keratin/Ganglion patches. `--fibril-bin` packages the exact Linux binary and its host runtime libraries into a local scratch image, recording their hashes. This avoids recompiling a different broker or mixing incompatible libc versions. This image is a local measurement artifact. Use the repository Dockerfile for a portable application image. The harness cannot infer which features an arbitrary user-supplied executable was built with; record its build command with the results.

The Rust harness has its own workspace and committed `Cargo.lock`, so adding competitor SDKs does not change the broker's dependency graph. Its Fibril client comes from this checkout; the lockfile pins the git-sourced shared wire types. `--binary /path/to/broker-compare` skips compilation. The standalone executable also supports targeting existing servers; use a **fresh, unused queue name** and matching benchmark credentials (`bench/bench`). Standalone output is marked `client_validated`. Only the runner's additional server checks produce `validated` results.

## Contracts

The common queue profile uses durable storage, publisher confirmations, individual manual consumer acknowledgements, no TTL, no deliberate retries, no compression, no TLS, and no application payload batching. Default confirmation credit is 4096 outstanding messages; consumer prefetch/MaxAckPending is 1024. The common profile permits 1–2000, staying within RabbitMQ’s documented quorum prefetch limit. One queue and one consumer preserve a comparable work-distribution shape. All three systems use Docker bridge networking with loopback-only published ports and data bind-mounted onto the same chosen filesystem.

| System / profile | Queue implementation | Publish confirmation | Consumer acknowledgement |
|---|---|---|---|
| Fibril | One queue partition, `local_durable` | Local durable commit | Individual completion |
| RabbitMQ | Persistent messages, durable one-member quorum queue | Quorum publisher confirm; with one member, local disk sync | Individual AMQP ACK |
| JetStream / always | File-backed, one-replica workqueue stream | Publish ACK with `sync_interval: always` | Individual explicit ACK |
| JetStream / 2m | Same workqueue configuration | Publish ACK with two-minute timed sync | Individual explicit ACK |

The first three belong to the **sync-before-publish-confirm** comparison group. This describes the intended publish acknowledgement boundary. It does not establish identical recovery, metadata persistence, delivery visibility or consumer-ACK durability. The benchmark checks fault-free completion and settlement; it does not run a power-loss durability test. In particular, receiving a delivery can occur before its publish confirmation, and consumer ACK submission is not a separately confirmed durable transaction in this workload.

After the measured workload, the Fibril adapter performs a topology request/response on its single consumer connection before shutdown. This drains queued ACKs through the connection engine and socket. The runner then verifies the broker's settled frontier and empty ready/inflight state. This transport barrier relies on the current single-node, single-connection profile; a future clustered adapter must drain every connection that carried acknowledgements.

The timed-sync JetStream profile belongs to a separate group. It is useful to show this alongside the stronger profiles, with its guarantee visible. A performance lead against this profile is meaningful for the measured workload; it should retain the exact storage, versions, payload, offered load and latency context. The runner verifies JetStream's effective sync configuration. Rabbit queue type, durability and member count are checked through management; Fibril uses explicit local-durable configuration in a fresh single-node deployment.

NATS documents [JetStream persistence and sync behaviour](https://docs.nats.io/nats-concepts/jetstream). RabbitMQ describes [quorum queue disk-sync confirmations](https://www.rabbitmq.com/blog/2025/01/17/how-are-the-messages-stored) and [publisher/consumer acknowledgements](https://www.rabbitmq.com/docs/confirms).

Protocol transport details remain visible rather than being forced into identical internals: JetStream uses a rolling pull iterator, default request batch 1024 (`--pull-batch`, bounded by prefetch). RabbitMQ uses mandatory publishing and rejects returns/nacks. Fibril and JetStream publication offsets/sequences must match the embedded message ID. Fibril's Rust SDK exposes a settlement tag rather than the stored delivery offset, so delivery validation uses payload identity; JetStream also validates delivery sequence and delivery attempt count. Rabbit delivery tags are channel-local and are not treated as message IDs.

## Workload and measurements

- **Fixed offered rate:** message `i` has deadline `start + i/rate`. The original schedule survives late wakeups and exhausted credit. The workload issues exactly `rate × (warmup + duration)` messages, including during drain if overloaded. A bounded drain deadline fails runs that cannot finish; no late messages are silently dropped. Tokio timers can cause short bursts, which appear in admission latency.
- **Saturation:** issuance lasts for warmup plus measurement duration, constrained by the same confirmation window. A message's intended time is when its next publish iteration becomes ready. It includes subsequent credit waiting and makes no claim about an external offered rate. Hitting `--max-messages` fails instead of truncating a run.
- **Warmup:** exclude messages whose intended send time precedes the measurement interval. Keep confirmations and deliveries for the measured cohort even if they arrive during drain. Warmup backlog remains part of the running system.
- **Admission latency:** intended time to acquiring confirmation credit, before constructing the payload and calling the SDK. **Confirmation/delivery latency:** that admission time to observing completion. **Intended-to-completion latency:** includes both. All use one process's monotonic clock. These are application-observed timestamps, not packet-arrival timestamps. Confirm futures are polled concurrently, avoiding an artificial FIFO confirmation collector.
- **Throughput:** `cohort_completed_per_sec` divides measured messages by measurement duration extended through the last confirmation or delivery. `observed_delivery_per_sec` and `observed_confirm_per_sec` count actual completions during the nominal measurement interval; they can include warmup-cohort completions. The JSON records both plus issue/confirm/delivery counts and a 100ms timeline, allowing backlog and drain to be examined.
- **Payload:** `--payload-bytes` is the complete application payload, including a 32-byte header containing magic, ID and two timestamps. Minimum 32 bytes, maximum 1 MiB. The remaining bytes are checked for corruption. IDs must be unique, contiguous and in range. All issued messages must confirm, deliver and have an ACK submitted. The identity bitmap grows by about one bit per message; histograms have bounded memory.
- **Histograms:** HDR, microsecond units, three significant digits, p50/p95/p99/p99.9/max. Nanoseconds round up to microseconds, with zero represented as 1µs. Values above the one-hour histogram bound fail rather than silently clamp. Each run retains its own summaries; the table does not average or pool percentiles.
- **Final settlement:** Fibril requires ready=inflight=0 and settled frontier equal to count. JetStream requires no pending/unacked deliveries, an empty workqueue stream and the final stream sequence. Rabbit requires ready=unacked=0, a one-member quorum queue and management ACK/delivery counters covering the full run, so an old zero snapshot cannot pass.
- **Resources:** every 100ms, record broker container cgroup CPU, current total memory, anonymous memory, charged file cache and task count. Resource scope starts after broker readiness and includes client setup, warmup, measurement, drain and final verification. Client `/proc` CPU and RSS are separate. Container total memory includes charged page cache/kernel memory; it is not process RSS. Memory peaks are sampled. Storage bytes, host-wide memory and swap are not yet measured.

Each repeat rotates broker order. No CPU affinity, cgroup resource quota or cold page-cache reset is applied. Clients and broker share the host. The default data root is the system temporary directory, which may be tmpfs; `placement.json` records the actual filesystem. tmpfs exercises the sync API without testing stable-media durability. Choose a physical mount explicitly for storage comparisons. Replication factor one exercises the broker's queue implementation but provides no node-failure tolerance.

Use several repeats and longer runs before interpreting small differences. Start with 64 B, 1 KiB, 16 KiB and 64 KiB; select common fixed rates below the slowest system's knee as well as saturation. A higher offered rate can expose growing backlog rather than sustainable capacity. The current single producer is deliberate for the first adapter comparison; add concurrency/window sweeps and client CPU headroom checks before describing saturation as the broker ceiling.

## Evidence and failure handling

Each invocation produces provenance, a copy of the harness lockfile, exact commands and settings, broker/client logs, resource samples, per-run JSON and `TABLE.md`. Failed cases keep a `failure.json` and never become a successful table row. The runner stops at the first failure. Fresh data and the invocation's Compose project are removed in `finally`; unrelated containers are untouched. If the runner is forcibly killed, use the recorded project name and Compose file to remove its containers and temporary data.

```sh
cargo test --locked --manifest-path benchmarks/comparison/Cargo.toml
python3 -m unittest discover -s benchmarks/comparison -p 'test_*.py'
python3 benchmarks/comparison/table.py /path/to/result-directory
```

## Existing-server RPC and clustered workloads

The Rust executable also supports three stored copies (`--copies 3`), multiple connection pairs (`--connections N`), and a two-queue request/reply workload (`--rpc`). These modes target deployments provisioned separately. The automated Python runner and its table remain single-node queue tools. `--copies` configures NATS/Rabbit declarations; Fibril placement and confirmation policy must be configured and checked separately. A successful client run does not verify replica placement, disk-sync settings, server settlement or cluster recovery.

For an existing broker with benchmark credentials and fresh queue names:

```sh
broker-compare --broker fibril --endpoint 127.0.0.1:9876 \
  --queue rpc_bench --rpc --pipeline-replies --request-window 512 \
  --service-workers 32 --confirm-window 512 --prefetch 1024 \
  --payload-bytes 1024 --reply-bytes 1024 --rate 1000 \
  --warmup-secs 5 --duration-secs 30 --output rpc.json
```

RPC uses a request queue and a separate reply queue, preserving payload correlation IDs. Each worker sends a reply and waits for its confirmation before acknowledging the corresponding request. With `--pipeline-replies`, workers can send more replies while earlier confirmations are pending; total outstanding work remains bounded. Without it, the worker limit includes the confirmation wait. The request window separately limits unfinished round trips. Replies may complete out of request order, so their stored offsets are not compared with request IDs.

Multiple connections divide total consumer credit and share publisher credit. They do not multiply the offered rate. Merged deliveries are checked for identity and completeness, without a global-order assertion. Fibril's settlement transport barrier covers each connection in the pool.

The source also preserves low-level process-failure probe helpers used during development. They require external orchestration and phase-file handling; they are not part of the automated comparison or a power-loss safety certification. Cluster provisioning and fault runners still need a portable, documented entry point before joining the one-command suite.

## Next supported shapes

1. Stream adapters: Fibril Plexus, JetStream limits-retention streams, RabbitMQ streams through the native stream protocol. Preserve replay/retention behaviour and use independent consumer cursors. Report consumer count, per-consumer latency/lag and aggregate delivered bytes separately from publication throughput.
2. Multiple queue partitions and publisher/consumer concurrency. Keep aggregate versus per-partition credit/rate explicit and show client CPU headroom.
3. Three-node Compose profiles, verified replica placement and durable quorum confirmation settings. Measure confirmation and delivery visibility separately, retain memory/CPU per node and aggregate totals, and label timed-sync profiles independently. Separate same-host clusters from multi-host/networked runs and distinguish shared versus independent storage devices.
4. A matrix runner for payload, offered rate, saturation, repetitions and storage, with retained raw runs and variability summaries. Longer sampled timelines should establish whether each fixed-rate run reached steady state.

The automated runner accepts queue workloads and single-node provisioning. The existing-server modes above provide the workload foundation for the planned extensions. Kafka/Redpanda may be useful for the retained-stream comparison once the stream contract is established.

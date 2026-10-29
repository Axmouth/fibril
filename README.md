<div align="center">

# Fibril
**The connective tissue of distributed systems.**

[![Server CI](https://github.com/Axmouth/fibril/actions/workflows/rust-ci.yaml/badge.svg)](https://github.com/Axmouth/fibril/actions/workflows/rust-ci.yaml)
[![TypeScript client CI](https://github.com/Axmouth/fibril/actions/workflows/typescript-client-ci.yaml/badge.svg)](https://github.com/Axmouth/fibril/actions/workflows/typescript-client-ci.yaml)
[![Deploy website](https://github.com/Axmouth/fibril/actions/workflows/deploy-website.yaml/badge.svg)](https://github.com/Axmouth/fibril/actions/workflows/deploy-website.yaml)

[Docs](https://fibril.sh/) · [Run Fibril](https://fibril.sh/deployment/source/)

<img src="./bannerlogo.png" width="600" alt="Fibril Logo">

*Simple. Durable. Built from first principles.*

---

</div>

### Intent

Fibril is a message broker focused on durable delivery, retries, leasing and
asynchronous workflow coordination, built from first principles in Rust.

Most applications need durable queues, predictable delivery semantics, reconnects
that don't surprise them, and operational controls that are easy to understand
during an incident. Fibril optimizes for that path rather than replicating every
feature of existing brokers.

Fibril is pre-alpha. APIs, persistence formats and operational behavior can still
change between releases.

### The shape of the API

The message carries its own lifecycle:

```rust
while let SubEvent::Delivery(msg) = sub.recv().await {
    process(&msg.payload).await?;

    msg.complete().await?;    // acknowledge
    // or msg.retry().await?  // requeue
    // or msg.fail().await?   // settle as failed
}
```

In Rust, messages are `#[must_use]`, so a forgotten settlement is a compiler
warning.

## What's here

* **Durable work queues** over a custom TCP protocol: explicit ack and nack,
  leasing with redelivery, delayed publish and delayed retry, message TTL, and
  dead-lettering with configurable policies. Queues are durable by default, and
  an experimental **buffered** mode confirms before fsync for lower latency.

* **Plexus streams**, a fan-out channel type where every subscriber receives
  every record, with durable named cursors, per-stream durability tiers and
  header filters.

* **Partitioned queues and streams** with client-side key routing and fan-in,
  plus opt-in wildcard subscribe.

* **Exclusive consumer groups**: ordered, balanced, sticky consumption of a
  partitioned queue.

* **Experimental clustering** through Ganglion coordination: partition
  ownership, follower replication, epoch-fenced failover, majority-durable
  confirms and live repartitioning.

* **First-party clients** in Rust, TypeScript, Python, Go and C#, kept at parity
  by shared wire vectors and a [feature matrix](clients/FEATURE_MATRIX.md).

* **Admin dashboard** for queues, streams, topology, message inspection,
  dead-letter replay and runtime settings, plus `fibrilctl` and Prometheus
  metrics.

* **TLS and authentication**: TLS for clients and between brokers, a replicated
  user store and optional client certificates.

The [project status](https://fibril.sh/status/) and
[implemented surface](https://fibril.sh/implemented-surface/) say exactly what is
wired and under what conditions. The [roadmap](https://fibril.sh/roadmap/) holds
what comes next.

## Performance

The [benchmarks](https://fibril.sh/benchmarks/) page has the measurements, each
labelled with its hardware, durability and workload. Two reference points, both
1 KiB durable messages on one Ryzen 5950X workstation with a consumer NVMe drive:

* A standalone broker sustained roughly 250k messages/s in and out (May 2026).
* Three brokers sharing that drive, with the shipped `majority_durable` default
  and early replication on, reached roughly 125k confirmed messages/s at
  saturation over one client connection (October 2026).

The [repeatable comparison](benchmarks/comparison/README.md) runs the same
workload against Fibril, NATS JetStream and RabbitMQ, and
[BENCHMARKING.md](BENCHMARKING.md) explains how to run each bench.

## Related components

* **Keratin** is the storage layer: a small, fast append-only segmented log,
  kept independent of broker logic. It also holds **Stroma**, the queue and
  stream state machine on top of Keratin, which handles snapshots, checkpoints,
  recovery and sealing. https://github.com/Axmouth/keratin

* **Ganglion** is the raft coordination library behind clustering, built on
  openraft. https://github.com/Axmouth/ganglion

### Local Keratin development

CI and release builds use `stroma-core` from the Keratin git repository:

```txt
https://github.com/Axmouth/keratin.git
```

For local sibling-checkout development, keep the crate dependency lines
git-sourced and enable the workspace patch override:

```sh
scripts/stroma-source.sh local
```

Switch to git mode before CI-style checks or Docker builds. This removes the
local patch override while leaving the committed dependency lines unchanged:

```sh
scripts/stroma-source.sh git
```

The local patch currently expects the Keratin checkout at `../keratin`.

## Naming

The naming follows a loose biological analogy:

* **Fibril** - the structural layer (the broker itself), inspired by the fibrous structures in biology that provide support and connectivity.
* **Stroma** - the internal state and organization, inspired by the supportive tissue in plants and animals that provides structure and coordination.
* **Keratin** - the durable, protective substrate, inspired by the append-only nature of keratin in hair, nails, horns, etc, plus the common shedding of old keratin layers, which is analogous to log compaction and cleanup in a message broker or WAL.

The mapping is not strict, but serves as a guiding theme.

In short:
- Keratin handles durability
- Stroma handles queue state and coordination
- Fibril handles broker and transport behavior

## Further reading

* [Docs](https://fibril.sh/) and [quickstart](https://fibril.sh/quickstart/)
* [Reliability semantics](https://fibril.sh/reliability/semantics/)
* [Follow-ups](FOLLOWUPS.md) and [audits](AUDITS.md) for engineering work in
  progress
* Historical design records are indexed in the [archive](archive/README.md).

---
title: Fibril documentation
description: Start here for the current Fibril documentation.
slug: "0.4"
---

Fibril is a lightweight message broker focused on durable delivery, explicit acknowledgements, leasing, retries, and asynchronous workflow coordination. It also has partitioned queues for scale, exclusive consumer groups for ordered parallel consumption, Plexus streams for fan-out where every subscriber sees every record, and an experimental clustered mode with partition ownership, replication, and failover for both queues and streams. The broker is implemented in Rust, but the user-facing model is about durable messaging rather than a Rust-only ecosystem.

It is early-stage (0.x). The useful baseline works and is tested hard, but APIs, persistence formats, protocol details, and operational behavior can still change between minor versions. The clustering and replication paths are experimental and not yet production-ready high availability.

## Where to start

* Follow the [quickstart](/0.4/quickstart/) to run the broker from source.
* Use the [client guide](/0.4/clients/) for Rust, TypeScript, and Python publishing and subscription examples.
* Use the [admin dashboard guide](/0.4/admin-dashboard/) for queues, streams, users, settings, message inspection, and DLQ replay.
* Read the [core model](/0.4/concepts/core-model/) for the queue lifecycle.
* Read [retries and delays](/0.4/reliability/retries-delays/) and [dead lettering](/0.4/reliability/dead-lettering/) for reliability features and their current limits.
* Read [consumer groups](/0.4/concepts/consumer-groups/) for ordered, scalable consumption across many consumer instances.
* Read [Plexus streams](/0.4/concepts/plexus-streams/) for fan-out delivery where every subscriber sees every record, with per-stream durability tiers.
* Secure a deployment with [TLS and users](/0.4/configuration/) - including [certificates as workload credentials](/0.4/deployment/cluster/#tls-across-nodes) - or bring up a [secured cluster](/0.4/deployment/cluster/).
* Point Prometheus at the broker with the [monitoring guide](/0.4/deployment/monitoring/).
* Read [clustering](/0.4/concepts/clustering/) and [replication](/0.4/reliability/replication/) for the experimental multi-broker ownership, replication, and failover path, or [try a cluster with Docker in under a minute](/0.4/concepts/clustering/#try-a-cluster-with-docker).
* Read [many idle queues](/0.4/concepts/many-idle-queues/) if your workload defines many queues but only uses a few at once.
* Check [project status](/0.4/status/) before depending on a feature.
* Check [implemented surface](/0.4/implemented-surface/) when you need the detailed answer for whether a path is wired and under what conditions.
* Check the [roadmap](/0.4/roadmap/) for recently landed work and near-term pending items.
* Use the [optimization log](/0.4/development/optimization-log/) for benchmark-first performance investigations.

## Design intent

Message handling should read like the intent of the system:

```rust
while let Some(msg) = sub.recv().await {
    process(msg.content()?).await?;
    msg.complete().await?;
}
```

Messages can also be retried or failed explicitly:

```rust
msg.retry().await?;
msg.retry_after(30).await?;
msg.fail().await?;
```

Delayed retry is wired through the broker, protocol, and the Rust, TypeScript, and Python clients. See [project status](/0.4/status/).

The protocol and storage model intentionally lean toward small common-case records. Always-present message metadata is stored as positional metadata, not repeated header-map keys, and uncommon operations use distinct frames or events instead of adding optional fields to hot paths. For example, delayed publish uses a delayed-publish frame rather than adding a delay field to every publish frame.

This is not a rule against richer commands. Infrequent configuration commands can carry complete settings when that makes atomic updates clearer, such as declaring a queue with all of its settings in one command.

## Current documentation

These docs are the frozen snapshot of the 0.4 release line. The documentation for the active pre-1.0 codebase lives at the site root and moves ahead of this snapshot.

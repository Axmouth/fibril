---
title: Fibril documentation
description: Start here for the current Fibril documentation.
slug: overview
---

Fibril is a lightweight message broker focused on durable delivery, explicit acknowledgements, leasing, retries, and asynchronous workflow coordination. It also has partitioned queues for scale, exclusive consumer groups for ordered parallel consumption, Plexus streams for fan-out where every subscriber sees every record, and an experimental clustered mode with partition ownership, replication, and failover for both queues and streams. The broker is implemented in Rust, but the user-facing model is about durable messaging rather than a Rust-only ecosystem.

It is early-stage (0.x). The useful baseline works and is tested hard, but APIs, persistence formats, protocol details, and operational behavior can still change between minor versions. The clustering and replication paths are experimental and not yet production-ready high availability.

## Where to start

* Follow the [quickstart](/quickstart/) to run the broker from source.
* Use the [client guide](/clients/) for Rust, TypeScript, and Python publishing and subscription examples.
* Use the [admin dashboard guide](/admin-dashboard/) for queues, streams, settings, message inspection, and DLQ replay.
* Read the [core model](/concepts/core-model/) for the queue lifecycle.
* Read [retries and delays](/reliability/retries-delays/) and [dead lettering](/reliability/dead-lettering/) for reliability features and their current limits.
* Read [consumer groups](/concepts/consumer-groups/) for ordered, scalable consumption across many consumer instances.
* Read [Plexus streams](/concepts/plexus-streams/) for fan-out delivery where every subscriber sees every record, with per-stream durability tiers.
* Read [clustering](/concepts/clustering/) and [replication](/reliability/replication/) for the experimental multi-broker ownership, replication, and failover path, or [try a cluster with Docker in under a minute](/concepts/clustering/#try-a-cluster-with-docker).
* Read [many idle queues](/concepts/many-idle-queues/) if your workload defines many queues but only uses a few at once.
* Check [project status](/status/) before depending on a feature.
* Check [implemented surface](/implemented-surface/) when you need the detailed answer for whether a path is wired and under what conditions.
* Check the [roadmap](/roadmap/) for recently landed work and near-term pending items.
* Use the [optimization log](/development/optimization-log/) for benchmark-first performance investigations.

## Versions

These pages track the active pre-1.0 codebase. Frozen per-release snapshots live under their version slug (the picker in the sidebar lists them), starting with [/0.2/](/0.2/).

---
title: Fibril documentation
description: Start here for the current Fibril documentation.
slug: 0.4/overview
---

Fibril is a lightweight message broker focused on durable delivery, explicit acknowledgements, leasing, retries, and asynchronous workflow coordination. It also has partitioned queues for scale, exclusive consumer groups for ordered parallel consumption, Plexus streams for fan-out where every subscriber sees every record, and an experimental clustered mode with partition ownership, replication, and failover for both queues and streams. The broker is implemented in Rust, but the user-facing model is about durable messaging rather than a Rust-only ecosystem.

It is early-stage (0.x). The useful baseline works and is tested hard, but APIs, persistence formats, protocol details, and operational behavior can still change between minor versions. The clustering and replication paths are experimental and not yet production-ready high availability.

## Where to start

* Follow the [quickstart](/0.4/quickstart/) to run the broker from source.
* Use the [client guide](/0.4/clients/) for Rust, TypeScript, and Python publishing and subscription examples.
* Use the [admin dashboard guide](/0.4/admin-dashboard/) for queues, streams, settings, message inspection, and DLQ replay.
* Read the [core model](/0.4/concepts/core-model/) for the queue lifecycle.
* Read [retries and delays](/0.4/reliability/retries-delays/) and [dead lettering](/0.4/reliability/dead-lettering/) for reliability features and their current limits.
* Read [consumer groups](/0.4/concepts/consumer-groups/) for ordered, scalable consumption across many consumer instances.
* Read [Plexus streams](/0.4/concepts/plexus-streams/) for fan-out delivery where every subscriber sees every record, with per-stream durability tiers.
* Read [clustering](/0.4/concepts/clustering/) and [replication](/0.4/reliability/replication/) for the experimental multi-broker ownership, replication, and failover path, or [try a cluster with Docker in under a minute](/0.4/concepts/clustering/#try-a-cluster-with-docker).
* Read [many idle queues](/0.4/concepts/many-idle-queues/) if your workload defines many queues but only uses a few at once.
* Check [project status](/0.4/status/) before depending on a feature.
* Check [implemented surface](/0.4/implemented-surface/) when you need the detailed answer for whether a path is wired and under what conditions.
* Secure a deployment with [TLS and users](/0.4/configuration/), or bring up a [secured cluster](/0.4/deployment/cluster/).
* Check the [changelog](https://github.com/Axmouth/fibril/blob/main/CHANGELOG.md) for what each release contains and the [roadmap](/0.4/roadmap/) for near-term direction.
* Use the [optimization log](/0.4/development/optimization-log/) for benchmark-first performance investigations.

## Versions

These pages track the active pre-1.0 codebase. Frozen per-release snapshots live under their version slug (the picker in the sidebar lists them), starting with [/0.2/](/0.4/0.2/).

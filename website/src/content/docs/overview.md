---
title: Fibril documentation
description: Start here for the current Fibril documentation.
slug: overview
---

Fibril is a lightweight message broker focused on durable delivery, explicit acknowledgements, leasing, retries, and asynchronous workflow coordination. It also has partitioned queues for scale, exclusive consumer groups for ordered parallel consumption, Plexus streams for fan-out where every subscriber sees every record, and an experimental clustered mode with partition ownership, replication, and failover for both queues and streams. The broker is implemented in Rust, but the user-facing model is about durable messaging rather than a Rust-only ecosystem.

It is early-stage (0.x). The useful baseline works and is tested hard, but APIs, persistence formats, protocol details, and operational behavior can still change between minor versions. The clustering and replication paths are experimental and not yet production-ready high availability.

## Start using Fibril

- Follow the [quickstart](/quickstart/) to run a broker, then use the
  [client guide](/clients/) for Rust, TypeScript, Python, Go and C# examples.
- Read the [core model](/concepts/core-model/) for queues,
  [consumer groups](/concepts/consumer-groups/) for ordered parallel consumption
  and [Plexus streams](/concepts/plexus-streams/) for fan-out.
- Explore the [admin dashboard](/admin-dashboard/) and its embedded demos, or
  [try a cluster with Docker](/concepts/clustering/#try-a-cluster-with-docker).

## Watch the architecture in motion

The animated stories explain decisions and ordering one step at a time. Each has
playback controls, a transcript and **Save frame** for presentations. Their timing
is illustrative.

- [Explore partition placement](/concepts/clustering/#watch-partition-placement):
  follow queue owners and replicas across brokers.
- [Follow the message path](/reliability/replication/#watch-the-message-path):
  compare delivery modes and the work that must finish before confirmation.
- [Follow a failover](/reliability/recovery-sealing/#follow-a-failover):
  watch owner loss, recovery, activation and rejoin, including a blocked attempt.
- [Follow an agreed checkpoint](/reliability/replication/#agreed-recovery-checkpoints):
  see how replicas establish a shared recovery starting point.

## Operate and evaluate

- Configure [TLS and users](/configuration/) or deploy a
  [secured cluster](/deployment/cluster/), then set up [monitoring](/deployment/monitoring/).
- Review [delivery guarantees](/reliability/semantics/),
  [retries and delays](/reliability/retries-delays/) and
  [dead lettering](/reliability/dead-lettering/) for behavior and limits.
- Use [failure modes](/reliability/failure-modes/) and
  [recovery quarantine](/reliability/recovery-quarantine/) when troubleshooting.
- Consult [benchmarks](/benchmarks/), [backpressure](/concepts/backpressure/) and
  [many idle queues](/concepts/many-idle-queues/) for workload planning.

## Follow development

[Project status](/status/) summarizes maturity. [Implemented surface](/implemented-surface/)
details supported paths and conditions. The [roadmap](/roadmap/) and
[failover plan](/development/failover-plan/) describe remaining work.
[Engineering notes](/development/engineering-notes/) and the
[optimization log](/development/optimization-log/) record fixes and investigations.
The [changelog](https://github.com/Axmouth/fibril/blob/main/CHANGELOG.md) records changes.

## Versions

These pages track the active pre-1.0 codebase. The version picker links to
historical documentation snapshots, starting with [/0.2/](/0.2/).

---
title: Deterministic simulation testing
description: Evaluation of turmoil and madsim for testing Fibril's cluster failure paths, and the staged plan for adopting one.
---

This is a development note: the evaluation behind task #97 and the plan for
getting deterministic simulation into the cluster test suite. It is the
credibility gate for the 1.0 cluster-confidence milestone - the difference
between "the cluster path passes my tests" and "I would run it across nodes."

## What we want to catch

The single-node path is well covered by ordinary tests. The value of simulation
is the **cluster failure paths**, where bugs hide in rare interleavings:

- replication catch-up and checkpoint install under a slow or flapping follower
- epoch-fenced failover with no split-brain (a stale former owner must not serve)
- replica-durable confirm timing and the in-sync floor under partitions
- repartition cutover fencing under reordered or delayed client acks
- coordination (raft) under partitions, message loss, and reordering

These need controlled time, controlled message scheduling, and injectable network
faults - which is what a deterministic simulator provides.

## The seam question

The deciding constraint is how the code reaches the network. Today Fibril calls
`tokio::net::{TcpStream, TcpListener}` directly in roughly seven places (the
broker connection handler, the client, follower replication, the admin server,
the server bootstrap) plus the ganglion raft TCP transport. There is no network
abstraction a simulator can substitute behind. That shapes the tool choice.

## turmoil vs madsim

**turmoil** (the tokio-rs network simulator) simulates the network between
in-process simulated hosts: latency, partitions, message loss, and reordering,
with deterministic time. Code under test uses `turmoil::net` instead of
`tokio::net`. For Fibril this means introducing a small `net` seam (a cfg-swap or
a thin type alias module) at those ~7 call sites plus the ganglion transport.
It keeps the real tokio task scheduler, so it is not fully deterministic at the
task-scheduling level, but it is deterministic for time and network - which is
where the cluster bugs live. Moderate, mostly mechanical integration cost.

**madsim** replaces the async runtime wholesale to get *full* determinism
(scheduling, time, RNG, network), compiled under `--cfg madsim` with
madsim-provided shims for tokio and friends. It is far more thorough, but every
async dependency in the graph has to be madsim-aware or shimmed. Fibril's
coordination is built on **openraft** plus a broad dependency graph, so a
whole-runtime swap is a large, high-friction lift with real risk that a dep does
not cooperate.

## Recommendation

Adopt **turmoil first**, as the task name implies. It targets exactly the
network-fault cluster paths that need proving, at a moderate and mechanical
integration cost, without betting the whole dependency graph on a runtime swap.
Treat **madsim as a later, optional escalation** only if scheduling-order
determinism turns out to be needed beyond what turmoil's network+time
determinism catches - and only after weighing it against the openraft dep graph.

## Prerequisites

1. **A `net` seam.** Introduce a thin module (or cfg-gated type alias) over
   `TcpStream`/`TcpListener` so simulation builds substitute `turmoil::net`. This
   touches the ~7 tokio::net sites and the ganglion raft transport, but the
   change is mechanical.
2. **Multi-broker in-process bootstrap.** The simulator must stand up N brokers
   in one process without going through `main`. This is the already-noted
   bootstrap-wiring refactor (see the near-term roadmap) and is a hard
   prerequisite for any multi-node simulation.

## Staged plan

1. **De-risk the tool.** DONE - confirmed turmoil 0.7 builds and runs in our
   toolchain.
2. **The net seam.** DONE for the data path. `fibril_util::net` re-exports
   tokio's TCP types normally and turmoil's under the `simulation` feature,
   validated by a test pair that runs the same code over a real loopback and
   inside a turmoil Sim. Because the swap is one re-export, call sites just import
   from `fibril_util::net` (no per-site cfg). Converted and verified in both build
   modes: the protocol crate (broker connection handler, follower replication,
   the `Conn` alias) and the client crate, each with a `simulation` feature
   forwarding to `fibril-util/simulation`. The broker crate has no direct
   `tokio::net` (its net lives in the protocol crate). The fibril bootstrap has no
   production `tokio::net` of its own (it uses the converted `run_server`). The
   ganglion raft transport is handled differently (see stage 4a below): instead of
   a cfg seam, its network factory and peer connection were made generic over a
   `RaftDialer`, so a simulated transport is injected the way production injects
   the tokio one - ganglion takes no turmoil dependency. Left on tokio
   deliberately: only the admin server (axum's `serve` needs a real tokio listener
   and admin is off the replication/coordination path). Known gap for sim use: the
   high-level client `connect()` resolves addresses via std DNS, which a sim's
   logical hostnames do not support - in-sim producers either use a
   hostname-direct connect path or the protocol layer directly.
3. **Stand up a multi-broker harness** in-process. DONE. The harness lives in
   `crates/protocol/tests/simulation_tests.rs` (compiled only under
   `--features simulation`). turmoil 0.7 gives each simulated host its own
   current-thread tokio runtime plus a LocalSet, so a `Broker` + `StromaEngine`
   built INSIDE a host closure spawns its background tasks onto that host's
   runtime and its timers run on the simulated clock. The corollary is that a
   broker can only be driven from within its own host - there is no shared
   runtime across hosts - so cross-host orchestration goes through the simulated
   network or through plain shared memory (atomics), never by calling another
   host's broker. A no-network smoke test (build, publish, confirm, checkpoint)
   proves the broker cooperates with turmoil's runtime and clock before any
   cluster scenario builds on it.
4. **First real scenario.** DONE. Two scenarios run on the simulated network
   with static/scripted coordination (no ganglion transport on the seam yet):
   (a) a follower, driven only by its supervised assignment watcher, catches up
   to the owner over the simulated network on the simulated clock. (b) Once
   caught up, the orchestrator partitions the owner away and the follower
   promotes itself under a fenced epoch bump and serves a fresh publish - the
   promoted log continues from exactly the replicated tails (no data loss) and
   promotion happens only under the higher epoch (the fencing mechanism). Both
   are deterministic (identical wall-clock across repeated runs).
4a. **Ganglion raft over the simulator.** DONE. ganglion's raft network factory
   and peer connection are now generic over a `RaftDialer`, and `serve_connection`
   plus the frame codec are generic over the stream, so a turmoil transport is
   injected from fibril test code (a `TurmoilDialer`) with no ganglion dependency
   on the simulator. A test stands up a 3-node ganglion raft cluster inside a
   turmoil Sim, elects a leader, and replicates a committed write entirely over
   the simulated network and clock - every vote, append, and commit RPC crosses
   the injected transport. This is what shared coordination under simulation
   needs, and it is deterministic across runs.
5. **Grow the scenario set.** The returning-old-owner split-brain refusal is
   DONE: three ganglion raft nodes run inside turmoil (two carry brokers, one is
   raft-only for majority), the follower replicates, the owner is partitioned from
   the majority, the majority's leader-only controller reassigns the queue under a
   bumped epoch, the follower promotes, and on heal the old owner's node catches
   up the raft log, observes the fenced reassignment, and refuses writes on its
   existing publisher. One integration note worth carrying: each turmoil host
   shares a single current-thread runtime across its broker and raft node, so a
   busy broker starves raft heartbeats and replication serving - the scenario
   keeps the old owner idle through catch-up and the partition for that reason,
   and raft uses widened election timeouts. Still to add: follower catch-up +
   checkpoint install, ISR-floor refusal under partition, repartition cutover
   under delayed acks, coordination
   under message loss.

## Relationship to other testing

This complements rather than replaces the existing coverage. The
[chaos and soak suite](/latest/) (task #115) exercises real wall-clock runs;
deterministic simulation finds the rare interleavings a soak might hit only once
in a thousand runs, and reproduces them exactly. Loom (task #96, assessed as low
fit) targets fine-grained atomics, a different layer again. Together they form
the cluster-confidence gate for 1.0.

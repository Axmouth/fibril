<div align="center">

# Fibril
**The connective tissue of distributed systems.**

[![Server CI](https://github.com/Axmouth/fibril/actions/workflows/rust-ci.yaml/badge.svg)](https://github.com/Axmouth/fibril/actions/workflows/rust-ci.yaml)
[![TypeScript client CI](https://github.com/Axmouth/fibril/actions/workflows/typescript-client-ci.yaml/badge.svg)](https://github.com/Axmouth/fibril/actions/workflows/typescript-client-ci.yaml)
[![Deploy website](https://github.com/Axmouth/fibril/actions/workflows/deploy-website.yaml/badge.svg)](https://github.com/Axmouth/fibril/actions/workflows/deploy-website.yaml)

[Docs](https://fibril.sh/latest/) · [Run Fibril](https://fibril.sh/latest/deployment/source/)

<img src="./bannerlogo.png" width="600" alt="Fibril Logo">

*Simple. Durable. Built from first principles.*

---

</div>

### Intent

Fibril is a message broker focused on durable delivery, retries, leasing, and asynchronous workflow coordination, built from first principles in Rust.

The project is currently in a pre-alpha stage and under active development. APIs, persistence formats, and operational behavior may still change significantly as the system evolves.

Fibril aims to provide a simpler and more ergonomic messaging model, where the message itself carries the lifecycle of processing:

```rust
msg.complete().await?;
msg.retry().await?;
msg.fail().await?;
```

The architecture is intentionally modular. The broker, durability layer, and state-management components are designed to remain reusable independently, and the broker core may also be embedded directly into applications or custom systems where lightweight durable messaging semantics are useful.

The long-term direction is not to replicate every feature or abstraction of existing brokers, but to provide a practical, observable, and operationally simple system that covers the majority of real-world messaging workflows.

### The shape of the API

Messaging should not feel like bureaucracy. In Fibril, the message itself can carry the intent of the system, allowing for a natural, asynchronous flow:

```rust
while let Some(msg) = sub.recv().await? {
    process(msg.content()?).await?;

    msg.complete().await?;    // Acknowledge successful processing
}
```

Messages can also be retried or failed explicitly:

```rust
    msg.retry().await?;  // Requeue immediately
    msg.fail().await?;   // Mark failed without requeueing
```

Delayed retry is still being wired through the public path. Delayed publish is available through `publisher.publish_delayed(..)`.

Common access patterns:
```rust
    let value: MyType = msg.deserialize()?;
```

> Messages are `#[must_use]` (in Rust). So forgotten lifecycle handling is warned about.

> Fibril is still evolving, and some APIs shown here may change or may not yet
> be fully implemented end to end.

## What's here

* **Basic TCP protocol implementation**
  A simple custom protocol for communication between clients and the broker.

* **Core broker semantics**
  Minimal support for message enqueuing, delivery, and acknowledgement. The focus is on clarity rather than completeness.

* **TUI demo**
  A terminal UI that visualizes messages moving through the system. Useful for observing behavior and debugging interactions.

## Related components

* **Stroma**
  The state machine driving broker behavior. It uses Keratin for durability.
  Currently lives alongside Keratin, but may move into this repository to better align with the broker implementation, as it is already a fairly broker-specific state machine.

* **Keratin**
  A small durability layer focused on implementing a simple, (blazingly) fast append-only segmented log.
  The intention is to keep it simple and reusable, independent of broker-specific logic.

  https://github.com/Axmouth/keratin

### Local Keratin development

CI and release builds use `stroma-core` from the Keratin git repository:

```txt
https://github.com/Axmouth/keratin.git
```

For local sibling-checkout development, switch the Fibril manifests to the
local `stroma-core` path:

```sh
scripts/stroma-source.sh local
```

Switch back before CI-style checks, Docker builds, or commits intended for
main:

```sh
scripts/stroma-source.sh git
```

The local path currently expects the Keratin checkout at `../keratin`.

## Direction

Fibril is being developed with dogfooding in mind, aiming to build small, real use cases on top of it to better understand:

* delivery guarantees
* retries and failure handling
* scheduling and delayed execution
* observability of message flow

The system may change significantly as these areas are explored.

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

## Status

Early and incomplete. Expect rough edges, missing features, and frequent changes.
The early iteration used RocksDB as the persistence layer, but the current version is now fully migrated to Keratin.
All layers from the broker and below have a decent implementation of graceful shutdown.
Glad to have reached a point where the current features should reliably work.
The TCP layer can use meaningful improvements, and the admin interface and internal layers are still very basic.
Moving from shared locked data structures to a more actor like approach for queues and other components has improved reliability and simplicity, and will likely be the default going forward.

## Implemented functionality

The current implementation provides a minimal but working set of broker semantics:

### Messaging

* **Publish (pub)** over a custom TCP protocol
* **Delayed publish** using a distinct delayed-publish protocol frame
* **Subscribe (sub)** to topics and receive messages
* **Topic plus optional group addressing** (partition selection is internal)
* **Basic Rust and TypeScript clients** for publishing and consuming

### Delivery model

* **Pull-based delivery with leasing**

  * Consumers request messages via `PollReadyAndMark`
  * Messages are moved to **inflight** with a lease deadline

* **Explicit acknowledgements**

  * `Ack` / `AckBatch` mark messages as processed
  * ACKs are idempotent and final

* **Negative acknowledgements**

  * `Nack { requeue=true }` → message returns to **ready** with retry increment
  * `Nack { requeue=false }` → terminal (treated as acked)
  * Retry tracking exists but is still evolving

* **At-least-once delivery (best effort)**

  * Messages may be redelivered if not acked before lease expiry

### Queue state model

Each message offset moves through a strict state machine:

* **Ready** → eligible for delivery
* **Inflight** → leased to a consumer (with deadline)
* **Acked** → terminal

Key transitions:

* `Enqueue` → Ready
* `EnqueueDelayed` → Ready after its `not_before` deadline
* `PollReadyAndMark` / `MarkInflight` → Inflight
* `Ack` → Acked (terminal)
* `Nack(requeue=true)` → Ready
* `CollectExpired` → Inflight → Ready

Key invariants:

* An offset exists in at most one of `{ready, inflight, acked}`
* ACK is final (no re-entry into delivery)
* Delivery eligibility is determined solely by **ready**
* All operations are idempotent and replay-safe

### Ordering and acknowledgement tracking

* **Monotonic ACK frontier (`settled_until`)**

  * Tracks the lowest non-acked offset
  * Advances only when contiguous ACKs are present

* **Bounded ACK window**

  * Fixed-size bitset (`ACK_WINDOW`) for out-of-order ACKs

  *Note:*
  ACKs may arrive out of order. Instead of tracking all pending ACKs, a fixed-size window is maintained starting at `ack_window_base` (typically aligned with `settled_until`).

  * Offsets **below `settled_until`** are implicitly considered ACKed
  * Offsets within `[ack_window_base, ack_window_base + ACK_WINDOW)` are tracked in the bitset
  * ACKs beyond the window are accepted but not fully materialized in memory

  The ACK frontier (`settled_until`) advances only when contiguous ACKs are observed. As it advances, the window slides forward and old bits are cleared.

  This keeps memory bounded while still allowing efficient out-of-order ACK handling near the frontier.

### Leasing and retries

* **Lease deadlines**

  * Inflight messages have a deadline
  * Expired leases are re-queued via

* **Retry tracking**

  * Per-offset retry count
  * Max-retry dead-letter routing is wired; replay and inspection tooling is still early

### Persistence

* **Append-only logs (via Keratin)**

  * Message log
  * Event/state log

* **Snapshot + replay**

  * Queue state can be encoded and restored
  * All commands are designed to be replay-safe

### Runtime model

* **Single-threaded command processing per queue**

  * Commands are processed sequentially (actor-like model)
  * Avoids shared mutable state and locking

* **Graceful shutdown**

  * Supported across broker layers

---

This represents the current working baseline. Some areas (DLQ, retries, protocol details) are still incomplete or evolving.

## Performance (early observations)

Informal internal benchmarks using the current TCP transport and durable path on a single Ubuntu 26.04 node have observed throughput in the range of:

* ~250k+ messages/sec ingress
* ~250k+ messages/sec egress
* 1KB payloads
* durable delivery path enabled

Measured on a Ryzen 5950X system.

Observed memory usage during these runs ranged from a few hundred MB at lower load up to roughly ~1–2GB at peak throughput, depending on queue depth, batching behavior, and inflight state. Memory behavior is still being optimized, and allocator retention may cause RSS usage to remain elevated after load subsides.

Throughput decreases as payload sizes increase, as expected. Larger payloads eventually become increasingly constrained by memory pressure, copying overhead, storage throughput, and network I/O characteristics.

These numbers are intended primarily as a sanity check of the current architecture rather than a rigorous benchmark suite.

Current optimization work is focused mostly on architectural efficiency and correctness rather than aggressive micro-optimizations. Areas such as zero-copy delivery paths, allocation reduction, string interning, and further batching improvements are still open for exploration.

Real-world performance will vary significantly depending on:

* storage hardware and durability settings
* batching and client behavior
* workload characteristics
* queue depth and inflight pressure
* network conditions and protocol overhead

For a more typical workload scenario, a sustained run of approximately ~400 messages/sec with ~48KB payloads over roughly 5 hours showed a peak working set around ~150MB, with memory usage later falling back toward the ~40–70MB range during lower activity periods.

Performance characteristics are expected to evolve significantly as the system matures.

## Core areas to expand on:

* **Removal of inefficiencies in command dispatch**

  - ~~Potential microbatching incoming publish events, settle events and append together, clogging the command queue and other inputs much less on high load~~
  - Create Queue handles at Query Engine and Stroma level that do not need to seek the relevant Queue handle on every command, plus related checks
  
  Update: Microbatching implemented, optimizations in calling queue handles less often are ongoing.

* **Faster state loading utilizing snapshots**

  ~~Will likely pair with compation improvements, to make better use of snapshots feature, so a broker with longer history does not rerun many events to initiate its state~~

  Compaction implemented and loading uses snapshots, vastly improving startup after long usage.

* **Client libraries**

  Building client libraries in various languages to make it easier to interact with the broker and test its
  features in real applications.

  Rust client is currently in decent shape, and the TypeScript client covers the same core publish/subscribe surface including delayed publish.
  Considered languages: Typescript, Python, C#

* **Admin interface**

  A more robust and user-friendly interface for managing the broker, monitoring its state, and performing administrative tasks.

* **Event log debug interface**

  Perhaps tucked in admin page somewhere, able to read paginated events to help debug things.

* **Heartbeats and connection management**

  ~~-Implementing a heartbeat mechanism to detect and handle client disconnections more gracefully.~~

  Update: Heartbeat plus timeout implemented on server side plus Rust and TypeScript clients.

* **Higher level test cases**

  Building more complex test cases that simulate real-world usage patterns, including failure scenarios, to validate the broker's behavior and robustness. Especially around the TCP layer.

* **Dead letter queue and retry mechanisms**

  ~~Implementing a dead letter queue for messages that cannot be delivered after a certain number of attempts, and exploring different retry strategies.~~

  Update: Retry is implemented, deadletter logic is implemented with different policies(global dlq, custom set dlq, just discard). Works when maxxing out retries as well.

* **Compaction and cleanup**

  ~~Implementing mechanisms for compacting the message log and cleaning up old messages to manage storage usage effectively.~~ As well as shrinking in memory data structures for tracking state after prolonged inactivity. Plus potentially full teardown of relevant in memory structures after a longer period.

  Update: Periodic compaction of event and message log implemented

* **Consistent backpressure and flow control**

  Exploring strategies for applying backpressure to producers and consumers based on the broker's current load and capacity. (Note: Backpressure exists to some extent thanks to bounded channels in key parts of the broker)

* **Memory/storage limits and message capacity limits**

  Throttle(asymptotically?) publishers based on getting near a memory or undelivered message limit (finding an efficient way to keep track of memory, or the right parts of memory, pending)

* **Transition from primitive types to newtype wrappers**

  To improve type safety and code clarity, consider introducing newtype wrappers around primitive types like `u64` for offsets, `String` for topic names, etc. This can help prevent mix-ups and make the code more self-documenting.

## Potential areas to explore:

* **Protocol improvements**

  Adding features like encryption, and more robust framing. Ability to set settings on the connection/queue/message level, etc.

* **Message Expiration and TTL**

  Support for expiring messages after a certain time, and handling of expired messages.

* **Expiration and cleanup of queues and other resources**

  Implementing mechanisms for expiring and cleaning up unused queues, connections, and other resources to prevent resource leaks.

* **Exactly-once delivery semantics**

  Exploring how to implement exactly-once delivery semantics, and the trade-offs involved in achieving this level of guarantee. The goal is to improve delivery guarantees, not necessarily achieve true exactly once semantics in all cases, as that may not be possible or desirable. This would likely involve optional delivery confirmation acknowledgements and publish confirm acks as incremental improvements.

* **Clustering and distributed coordination**

  Exploring how multiple broker instances can work together, share state, and provide high availability.

* **Direct delivery/Express lane/Speculative Egress**

  A mode to use when we know there are subscribers to a "channel" we publish to. Likely would need to use a shortcut to delivery loop, and confirm on disk write even if the consumer has already act, for zero durability compromise. Might end up too niche however, as stored messages, which came earlier, have higher priority anyway. Also could end up not worth the hassle for what it brings.
  Potential implementation details: Semaphore to open lane slots, when we know ready state empty and no pending publishes. confirm on disk fsync as usual. Explore if better to directly set to inflight together with enqueue.

* **Message lease extension while it is being processed**

  Especially some manner for the client side to keep the lease alive as long as connection is intact

* **Delayed messages and scheduling**

  Delayed publish is wired through the broker protocol plus Rust and TypeScript clients. Delayed retry is still an active area.

### The following features may be explored under a separate advanced routing layer on top of the core broker, to keep the core focused on durability and delivery semantics:
(Note: the above is not the guaranteed direction, but a possibility)

* **Message prioritization**

  Allowing messages to be prioritized in the queue, and exploring different strategies for handling priorities.

* **Routing and topic-based messaging**

  Implementing more complex routing logic, such as topic-based messaging or content-based routing.

* **Exchanges and bindings**

  Implementing concepts like exchanges and bindings to provide more flexible routing options.

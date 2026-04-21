<p align="center">
  <img src="./bannerlogo.png" width="640em">
</p>

# Fibril

Fibril is an experimental message broker built to explore queue semantics, durability, and distributed coordination from first principles.

This project is intentionally small and evolving. It is not production-ready, and it does not (yet) aim to compete with existing systems. The goal is to understand the problem space by building and using the pieces directly.

Every layer and component is meant to be reusable, if desired, and the broker layer embeddable, effectively a channel factory with durabiltity and delivery semantics. While I do not expect this to be a common use case, it is an interesting possibility that the core broker logic could be used as a library for building custom brokers or even embedded directly in service instances.

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
* **Subscribe (sub)** to topics and receive messages
* **Topic + partition addressing** (partition currently mostly structural)
* **Basic Rust client** for publishing and consuming

### Delivery model

* **Pull-based delivery with leasing**

  * Consumers request messages via `PollReadyAndMark`
  * Messages are moved to **inflight** with a lease deadline

* **Explicit acknowledgements**

  * `Ack` / `AckBatch` mark messages as processed
  * ACKs are idempotent and final

* **Negative acknowledgements**

  * `Nack { requeue=true }` → message returns to **ready** with retry increment
  * `Nack { requeue=false }` / `Reject` → terminal (treated as acked)
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
  * Basic max-retry → dead-letter behavior exists but is not fully implemented

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

Informal internal benchmarks under preferential settings (broker level, skipping the TCP layer) show that the current implementation has been observed to sustain:

* ~100k messages/sec (publish + consume)
* 1KB payloads
* durable path enabled, with delivery verified under these conditions

Tested on a single node (Ryzen 5950X).

Observed memory usage during these runs ranged from a few hundred MB at low load up to ~1–2GB at peak throughput, depending on queue depth and in-flight state. Memory is not yet optimized and may retain capacity after load.

Memory usage increases as expected with larger payloads, and throughout decreases, at first less than linearly to the payload size increase, but after a point in the curve (around 32kb) the decrease becomes more pronounced, likely due to hitting memory limits.

These numbers are intended as a rough sanity check of the current design, not as a representative benchmark.

Real-world performance will vary significantly depending on:

* network overhead (TCP layer is still basic)
* batching and client behavior
* workload characteristics

## Core areas to expand on:

* **Faster state loading utilizing snapshots**

  ~~Will likely pair with compation improvements, to make better use of snapshots feature, so a broker with longer history does not rerun many event sto initiate its state~~

  Update: Largely implemented

* **Client libraries**

  Building client libraries in various languages to make it easier to interact with the broker and test its
  features in real applications.

* **Admin interface**

  A more robust and user-friendly interface for managing the broker, monitoring its state, and performing administrative tasks.

* **Event log debug interface**

  Perhaps tucked in admin page somewhere, able to read paginated events to help debug things.

* **Heartbeats and connection management**

  ~~-Implementing a heartbeat mechanism to detect and handle client disconnections more gracefully.~~

  Update: Largely implemented

* **Higher level test cases**

  Building more complex test cases that simulate real-world usage patterns, including failure scenarios, to validate the broker's behavior and robustness. Especially around the TCP layer.

* **Dead letter queue and retry mechanisms**

  Implementing a dead letter queue for messages that cannot be delivered after a certain number of attempts, and exploring different retry strategies.

* **Compaction and cleanup**

  ~~Implementing mechanisms for compacting the message log and cleaning up old messages to manage storage usage effectively. As well as shrinking in memory data structures for tracking state after prolonged inactivity.~~

  Update: Largely implemented

* **Consistent backpressure and flow control**

  Exploring strategies for applying backpressure to producers and consumers based on the broker's current load and capacity. (Note: Backpressure exists to some extent thanks to bounded channels in key parts of the broker)

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

  Exploring how to implement exactly-once delivery semantics, and the trade-offs involved in achieving this level of guarantee. The goal is to improve delivery guarantees, not necessarily achieve true exactl once semantics in all cases, as that may not be possible or desirable. This would likely involve optional delivery confirmation acknowledgements and publish confirm acks as incremental improvements.

* **Clustering and distributed coordination**
  Exploring how multiple broker instances can work together, share state, and provide high availability.

### The following features may be explored under a separate advanced routing layer on top of the core broker, to keep the core focused on durability and delivery semantics:

* **Message prioritization**

  Allowing messages to be prioritized in the queue, and exploring different strategies for handling priorities.

* **Delayed messages and scheduling**

  Support for scheduling messages to be delivered at a later time, and handling of delayed messages.

* **Routing and topic-based messaging**

  Implementing more complex routing logic, such as topic-based messaging or content-based routing.

* **Exchanges and bindings**

  Implementing concepts like exchanges and bindings to provide more flexible routing options.

fibril "jetstreams" (bit with bio theme still) with fanout etc built on top of fibril

1. Split-Brain Mitigation via Epoch Comparison

In DeterministicPartitionPlacement::plan, you determine whether an assignment's epoch changes:
Rust

let epoch = existing
    .filter(|assignment| assignment.owner == owner && assignment.followers == followers)
    .map(|assignment| assignment.epoch)
    .unwrap_or(input.generation);

    The Risk: If an owner falls out of the node map (e.g., a 10-second network hiccup causing its etcd lease to drop) and then suddenly reappears, it might still think it owns that partition.

    The Fix: When a node processes a LocalAssignmentTransition, it must ensure that any incoming write or replicate request includes the current epoch. If an incoming command's epoch is lower than the node's local partition state machine epoch, it must reject the command immediately to prevent stale data overwrites.

2. String Conversions in Inner Loops

Inside plan_local_assignment_transitions, you sort keys using:
Rust

a.topic.to_string()

Because this occurs inside your state-transition engine, doing .to_string() on a Topic performs a heap allocation. If you have tens of thousands of partitions, this will cause memory churn.

    The Fix: If your Topic type implements AsRef<str> or allows direct borrowing, sort using references (&a.topic) to keep your execution completely zero-allocation.

3. String Interning or Node Typing

You are passing node identifiers as String everywhere. For maximum memory efficiency and to keep memory safely flat below your 1GB target, consider introducing an explicit type alias or light wrapper for NodeId (e.g., using Arc<str> or an internal CompactString crate) if you expect your cluster sizes to scale into dozens or hundreds of nodes.


* **Broker Restart Reconciliation**

  * Sessions can be recovered even after broker restart or failover.
  * Extends reconnect semantics beyond "broker stayed alive" into persistent session continuity.

* **Message Expiration (TTL)**

  * Messages can expire automatically after a configured period.
  * Prevents stale work from being processed long after it remains useful.
  expiration map/registry in state, wire to expiry worker, do not expire while inflight, can do so while collecting expired too
  consider if we need new event/frames or just add optional expiration to existing

* **Time-based Retention**

  * Queues can automatically discard old messages according to retention policies.
  * Keeps storage growth bounded without manual intervention.
  sparse worker, sparse offset to time mapping(though we have published.. so we can do a crude binary search instead too, and truncate before, also clear relevant state entries)

* **Queue Purge**

  * Fast removal of all queued messages while preserving the queue itself.
  * Useful during incidents, testing, and recovery scenarios.
  effectively done by reset

* **Queue Deletion**

  * Complete lifecycle management of queues from creation to removal.
  * Eliminates operational cleanup gaps and orphaned resources.
  freeze then filesystem cleanup?

May as well add queue expiration as an option?

Hide inactive queues option in dashboard(useful for when someone really has tons of sparse queues. plus basic search)

* **Documented Failure Semantics**

  * Explicit documentation describing what happens during disconnects, crashes, failovers, reconnects, expirations, and ownership changes.
  * Often more valuable than features themselves because operators know exactly what to expect.

And if I had to pick the three features that would most change how technically-minded people perceive Fibril:

1. **Replication**
2. **Sharding with cluster ownership**
3. **Restart reconciliation built on top of the existing reconnect model**

make client errors have an "embedded retry" (perhaps like retry method, to redo the ack or whatever it was? ideally in a way based on the type of error too. perhaps a retryable method returning an enum with basically yes and no? where yes gives you a retry handle. though ideally with fewer steps overall. maybe like .is_retryable() and some retry op kinda method on the error directly)

How to Mitigate It Without Losing the Latency GainIf you want to keep the "express lane" speed but protect your system's transactional integrity, you can apply a few distributed systems patterns:

1. The "Ghost Flag" (Speculative Delivery)When you push a message down the express lane before it is safely on disk, inject a metadata flag into the frame header sent to the consumer (e.g., X-Speculative: true).This tells the consumer: "I am giving you this message incredibly fast, but it is not durable yet. If I crash right now, you might see this again. Do not mark this as fully committed in your database until you are ready to handle a duplicate."

2. Delayed Publisher Confirmation (The RabbitMQ Approach)RabbitMQ allows the fast-path delivery to the consumer to happen instantly, but it completely holds back the Publisher Confirmation frame. The broker will not tell the producer "OK" until that background 5ms fsync loop actually finishes writing the message to disk.This ensures that even if a crash causes a duplicate delivery later, the publisher and broker stay cryptographically aligned on what data was actually made durable.

If you can successfully implement restart reconciliation (where a broker boots back up, scans its un-acked WAL logs, and immediately maps un-acked states back to reconnecting clients over a graceful grace period), you will have solved one of the most frustrating aspects of distributed systems. This completely saves developers from writing idempotent processing code for standard broker maintenance windows.

building on top of restart reconciliation, update reconciliation. notify clients, drain pending, restart and continue like nothing happened.

add client setting to opt out convenient features like reconnect grace etc

opt in client enforced rate limints, one each way, separate total and per node

Those are the features that make people stop seeing "single-node broker with nice ergonomics" and start seeing "distributed messaging system with an opinionated operational model."

The interesting thing is that **restart reconciliation** is probably the most unique of the bunch. Replication and sharding are expected eventually. A well-defined, persistent session continuity model is much less common and aligns directly with the philosophy you've described: behavior that remains understandable during incidents.

Copy Keratin file lock on Stroma too

keep track off consumer id per message and extend lease for alive consumers instead of expiring. Or simply otherwise no expiry(configurable, depending on TTL there or not?) while client is still connected, bit instant expiry once client deemed lost

Investigate circular Arc<..> dependencies to ensure no leaks happen, use Weak<..> where apt

add express version of shutdown that preempts everything immediately, for emergency use

deny topic etc names beyond simple fs compatible setups

Add display names to topics/groups for logging/ui

unwrap/expect cleanup

test required changes for pre allocating segments so we skip metadata edits etc on every write.

Wire in more debug stats

Figure keratin head offset discrepancy.

Maybe find way to better linearly read from Keratin, faster

explore a cache trying to keep in memory x mb of next deliverable messages(we always know which messages are next with ready set)

opportunistic batching (do not wait if the socket is writable now, but if you would block, accumulate)

direct delivery/express lane (enqueue and inflight immediately whole also sending to delivery loop shortcut)
"Fibril utilizes speculative delivery: messages are streamed to available consumers immediately upon receipt, while persistence to Keratin happens in parallel. Producer acknowledgments are strictly deferred until disk synchronization is confirmed, ensuring zero-compromise durability with sub-millisecond egress."

Investigate single log(storage message log only used for messages beyond a certain size?) for stroma topic state? Message Offsets are now enqueue offsets, requeue event to avoid payload duplication

TODO:
Broker should no longer:
loop compute_start_offset() by calling is_inflight_or_acked() repeatedly.
Instead:
start = stroma.next_deliverable(tp,g, current_cursor, upper)
Also: redelivery queue should remain bounded by inflight cap (it mostly is already), but don’t let it accumulate unbounded offsets from repeated failures—Stroma can own "expired offsets" listing.

consider interning topics etc to save some memory. or somehow using box/arc str

reorganize for structs/models in a common crate, to avoid circles between metrics and storage too

cleanup leftover inflight without message (or better figure why it happens)

establish config framework/files/priorities. some stuff should be on the fly, but then how does it interact with file having the same? are some ui only? does it edit file too?

Handshake two way

ack correct matching(sub id?), in tcp layer

partitions internal generation and assignment

delivery not just with roundrobin bout account for prefetch capacity etc

max unconfirmed per publisher

slow ingress on memory/storage pressure

experiment with spreading delivered messages by making some consumers slower and see what happens

better handling of batching slowdown when confirms not drained

better error handling(return louder errors when failing to publish)

diagnostics queue/channel/mmesage type to allow sending up non fatal issues and tallying/observing them better?

multiple brokers on same storage tests (must fail)

shutdown stops publishing immediately, tries to drain inflight, ensure no late ack flushes race, ensure batchers drain once

Eval persisting inflight map, or delivery tags for inflight, so inflight state can be recovered on startup after crash

clusters (leader through shared networked storage initially, raft replication later?)

RabidMQ easter egg(--version during April 1st? rabbitmq compatibility layer?)

routing layer? (in order to keep invariants stable, maybe we'd need a separate layer, broker delivers to itself, runs script, acks onces derived message completes, effectively)

queues with ttl to discard(not just resend) (or ttl just per message?)

define ops/infra story for easy convenient deployments and handling(not error prone by default, don't assume user does things right)

write an operator runbook

rabbitmq compatible endpoint?

replace epoch in delivery tag with gen and seq. Seq is simple monotonic counter, gen is increased per instance(process? task?) created

dls or embedded language to script transformations, routing, etc?

in memory queues(no durability)? would need to make keratin pluggable or its write target able to take file or memory buffer(like a trait where flush etc becomes noop). Might be more worth doing it on Kerating level, though keeping the same semantics and only changing the storage is more reliably same behavior

phrasing:
Most applications need reliable queues, clear delivery semantics, reconnect behavior that does not surprise them, and admin operations that are obvious. Fibril optimizes for that path.
What happens if the broker restarts?
What happens if a consumer disconnects?
What happens if a queue is idle?
What happens if a message expires?
What happens if I purge while consumers are attached?
What happens during leader failover?
What should I alert on?


The relevant setting will show a random proverb each time. roadmap,locked in:
Stability requires patience. But for those who may not wait:
The surest path is seldom the swiftest. But for those in haste:
What is built to endure is rarely built to hurry. But for those who must:
Speed is easy to borrow from certainty. But for those willing to pay the price:
The fastest message is often the least certain. But for those who accept the risk:
Durability is purchased with time. But for those spending a different currency:
A promise kept takes longer than a promise made. But for those in a hurry:
Patience is the companion of certainty. But not all travelers keep the same company:
The river reaches the sea by refusing shortcuts. But for those seeking them:
What survives the storm is not always first to leave the harbor. But for those who must sail now:
The surest path is seldom the swiftest.
A bridge tested twice carries the heavier load.
The lock that yields quickly opens for all.
The swiftest promise is often the first forgotten
What survives the storm seldom outruns it.
The shortest road is not always the one that arrives.
Certainty walks; haste runs.
A message hurried is a message trusted less.
The patient builder sleeps through the wind
Time spent securing a door is rarely noticed after the theft.
The oak and the reed both reach the sky, but only one expects the storm.
Every shortcut borrows from tomorrow.

The second mouse gets the cheese.
A falling anvil reaches the ground first.
Most parachutes work best when opened before landing.
The tortoise won. The hare became a benchmark.
"You are overriding the default safety assumptions. We trust you know why."

---
title: Consumer groups
description: Ordered, scalable consumption of a partitioned queue with exclusive
  consumer groups.
slug: 0.4/concepts/consumer-groups
---

By default, several consumers on the same queue **compete**: each message is
delivered to exactly one of them (fair dispatch). That maximizes throughput, but
it makes no ordering promise — two messages can be processed at the same time by
different consumers.

When you need **per-key ordering while still spreading work across many consumer
instances**, opt into an *exclusive consumer group* (a cohort).

## What Fibril does

Mark a subscription exclusive and Fibril treats your consumer instances as one
group that jointly consumes the queue:

* **One consumer per partition.** Each partition is delivered to exactly one
  member at a time, so messages within a partition stay ordered.
* **Automatic, balanced, sticky assignment.** Partitions are spread evenly across
  the live members; a rebalance moves the minimum necessary (no needless churn).
* **Automatic failover.** When a member disconnects, its partitions move to the
  rest of the group.

You don't name or coordinate the group — every instance that calls `.exclusive()`
on the same queue *is* the group.

```rust
// Run several instances of this; they self-organize into the group.
let mut sub = client
    .subscribe("orders")
    .exclusive()
    .sub_auto_ack()
    .await?;

while let Some(msg) = sub.recv().await? {
    process(msg.content()?).await?; // per-key order preserved
}
```

Ordering is *per key*, so the producer chooses the key that groups related
messages onto the same partition:

```rust
publisher
    .publish(NewMessage::msg_pack(&order)?.partition_key(order.id))
    .await?;
```

See [partition routing](/0.4/development/partition-routing/) for how keys map
to partitions.

## When it applies

* **Parallelism needs more than one partition.** A single-partition queue has
  only one ordered stream, so one member consumes it and the others stand by
  (hot standbys for failover) — this is the classic "single active consumer"
  pattern. Declare more partitions to consume in parallel; see
  [configuration](/0.4/configuration/).
* **One cohort per queue.** A queue `(topic, group)` has a single exclusive
  group. Subscribing with a conflicting second group id on the same queue is
  rejected. Use a separate `group` namespace for an unrelated workload.
* **Opt-in.** Plain `.sub_auto_ack()` / manual subscriptions stay competing —
  still the right default when you don't need ordering.

## Tradeoffs and limits

* **It is still a work queue, not a log.** A message is consumed once; exclusive
  groups divide *who* processes each partition, they do not fan a copy out to
  multiple independent groups.
* **Capacity hint.** `.consumer_target(n)` advises how many partitions a member
  prefers to own. It is a soft signal for balancing and autoscaling — coverage
  always wins, so a member may still own more when the group is under-provisioned.
* **Reconnects keep membership.** A consumer that reconnects rejoins its group
  rather than dropping back to competing.
* **Across a cluster**, where a queue's partitions are owned by different brokers,
  per-partition ordering holds on each owner; cluster-wide balancing converges
  shortly after membership changes (it is advisory — the per-partition delivery
  rule is always enforced).

Exclusive consumer groups are a recent addition; see the
[roadmap](/0.4/roadmap/) for current status.

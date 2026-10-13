---
title: Buffered queues
description: Queues that confirm and deliver before fsync, what they can lose, and how consumers should handle it.
---

:::caution[Experimental]
Buffered queues are an experimental opt-in. Durable stays the default, and
the mode of an existing queue cannot be changed yet.
:::

A durable queue confirms a publish and hands it to consumers only after every
required copy has synced it to disk. A buffered queue does both once every
required copy has written the message to the operating system, before the
sync. That takes the fsync off the publish path, which lowers latency,
especially on disks with slow syncs.

The trade: a written message sits in the operating system's page cache until
a background sync makes it durable, usually within 100 milliseconds. Power
loss or a storage failure on every required node at once can lose confirmed
messages from that window.

## Turning it on

The mode belongs to the queue and is set by its first declare. A later
declare that names the other mode fails with `409`, and a declare that names
no mode keeps the queue's mode.

```rust
client
    .declare_queue(QueueConfig::new("clicks")?.buffered())
    .await?;
```

Every client has the same option, see [clients](/clients/#queue-durability).
The admin API's create queue request takes `"durability": "buffered"`.

A cluster can make buffered the default for new queues with
`replica_buffered` assignment durability:

```toml
[coordination.ganglion.assignment_durability]
mode = "replica_buffered"
nodes = 2
```

`nodes` counts copies, owner included, the same as `replica_durable`. A
declare that names `durable` still makes a durable queue.

## What survives

"Required copies" are the `nodes` copies, owner included, that a confirm waits
for.

| What happens | Durable queue | Buffered queue |
| --- | --- | --- |
| The owner process crashes, the machine stays up | Nothing lost | Nothing lost. The page cache survives a process crash. |
| The owner machine loses power, followers stay up | Nothing lost | Nothing lost, followers hold written copies. The owner rejoins through recovery. |
| One required machine loses power, with `nodes` of 2 or more | Nothing lost | Nothing lost, the other required copies hold it |
| Every required machine loses power at once | Nothing lost | Messages confirmed since the last completed sync can be lost, and so can recent acks |
| A disk fails a sync on the owner | Unconfirmed messages at risk | The owner drops what it never synced when it reopens. Followers keep the confirmed copies. |
| A single node queue (`nodes` of 1) loses power | Nothing lost | Messages confirmed since the last sync can be lost |

Machines that share a power feed, a hypervisor host or a disk count as losing
power at once.

After a power loss of every required copy, the queue recovers from the
longest history that survived and keeps serving.

## Consumers

Delivery stays at least once. A buffered queue adds two things a consumer can
see after every required copy loses power:

- An ack written since the last sync can be lost, so its message is delivered
  again. This is the usual at-least-once duplicate.
- A message can be lost after a consumer already processed it. The broker no
  longer has it, and a new message can later take its position in the queue.

So deduplicate on your own message identity, such as a key or id in the
payload or headers, never on the delivery tag or the position in the queue.

## Background sync

Each queue log syncs what it has written but not synced on a timer, set by
the `storage.background_sync_interval_ms` runtime setting (default `100`).
The interval is a target, not a limit: a slow or busy disk stretches it. `0`
turns the timer off and leaves syncing to the operating system, which puts no
limit on how long a confirmed message stays unsynced.

Checkpoints, recovery seals and shutdowns also sync first, so none of them
ever relies on records that could still be lost.

## Watching it

The queues page of the admin dashboard marks buffered queues and shows, per
partition, how many records are written but not synced yet. The cluster page
shows a buffered queue's policy as `replica_buffered`. A declare that names a
mode gets the mode in effect back, with a warning about the loss window when
it is buffered.

## Limits

- The mode of an existing queue cannot be changed. Declare a new queue to
  switch.
- A follower that falls back to the pull replication path, which it uses to
  catch up, syncs each batch, so it is slower than the push path.
- A broker whose disk fails a sync stops serving the queue until it restarts.
  The restart cuts its log back to its last sync and recovers the rest from
  the followers. This holds for durable queues too.

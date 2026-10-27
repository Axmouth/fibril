---
title: Resource limits and throttling
description: Design for slowing and pausing publishers before disk or memory run out, per-queue size limits, and a node-wide pool of reusable segment files.
---

This is a proposed design. Nothing here is implemented yet. It complements
the [backpressure](/concepts/backpressure/) model for consumers, and its
settings follow [configuration sources](/development/config-sources/).

## Problem

Nothing slows publishers when a broker runs short of disk or memory.

- **Disk.** A queue keeps every message until it is acknowledged, so a queue
  without consumers grows until the filesystem is full. The next write or
  fsync then fails, and Keratin stops that log by design: Linux reports a
  writeback error only once, so retrying in place is unsafe. The log needs a
  heal and a reopen. Replicas hold the same data and usually fill at the same
  time, so every copy of the queue stops together.
- **Memory.** Most per-log memory is bounded, but the total grows with queue
  count, connections and unconfirmed publishes. Past the machine or container
  limit the kernel kills the process.

Both end in an outage that a publisher could have avoided by waiting. The
goal is for publishers to slow down, then pause, with a clear reason, before
any hard failure, and for everything else to keep working so the backlog can
drain.

## Precedents

| System | Mechanism |
| --- | --- |
| RabbitMQ | Memory and disk alarms block publishing connections cluster-wide while consumers continue. Queue `max-length` and `max-length-bytes` with overflow `drop-head`, `reject-publish` or `reject-publish-dlx`. |
| NATS JetStream | Server `max_file_store` and `max_memory_store`. Stream `max_bytes` and `max_msgs` with discard `old` or `new`. |
| Kafka | Produce and fetch byte-rate quotas. Responses carry `throttle_time_ms` and the client waits. Retention by size or time. |
| Pulsar | Backlog quotas with `producer_request_hold`, `producer_exception` or `consumer_backlog_eviction`. |

Two lessons carry over. Blocking must target publishing only, because
consumers and acknowledgements are what free space. And a known rejection
with a retry hint is easier for clients than an unexplained stall.

## Goals

- Publishers slow down, then pause, before disk or memory run out.
- Consuming, acknowledging, deleting and purging keep working at every level,
  since they free space.
- Coordination, replication catch-up and recovery keep a reserved margin.
- Clients get a specific reason and a retry hint, and operators see the cause.
- Pauses clear on their own when resources free up, without flapping.

## Signals

### Disk

Each node samples free space on every filesystem that holds storage, once per
second and after each segment roll. Free space counts spare segment files as
reclaimable, since the node can delete them at once.

Three levels apply per filesystem. The proposed defaults scale with the
filesystem and have absolute floors, so one cluster-wide setting fits nodes
with different drives.

| Level | Proposed default | Effect |
| --- | --- | --- |
| Soft | 10% free or 4 GiB, whichever is larger | Release spare segments. Slow publishes. Attention item |
| Hard | 5% free or 2 GiB | Reject publishes with a retry hint |
| Reserve | 1% free or 512 MiB | Only coordination, metadata, acknowledgements and recovery may write |

A level clears once free space is back above it by a margin of 1% of the
filesystem, so a level does not flap around its threshold.

Acknowledgements write to the queue's log, so blocking every write would stop
the operations that free space. Below hard, publishes stop and
acknowledgements continue. Below reserve, only writes that free space or keep
the node safe continue.

### Memory

The node compares its resident memory with a limit: the cgroup limit when the
process runs in a container, otherwise a configured value, otherwise 60% of
physical memory. Soft is 80% of the limit and hard is 90%, with the same
effects as disk except spare release. Resident memory can stay high for a
while after memory is freed, because allocators hand pages back lazily, so
the memory signal clears with a longer margin.

## Behavior

### Slowing

Between soft and hard, the owner delays admitting each publish batch by a time
that grows linearly from 0 ms at soft to 50 ms at hard (proposed). Confirmed
publishers slow down in proportion, and nothing is lost. The delay applies per
partition, so other work on the same connection is unaffected.

### Rejecting

At or past hard, publishes fail with a new error code, `ERR_LIMITED` (429),
carrying a reason (`disk`, `memory` or `queue_full`) and `retry_after_ms`.
The publish was not applied, so retrying it is always safe. That is unlike
`ERR_GATEWAY_TIMEOUT`, whose outcome is unknown.

- Confirmed publishes retry automatically with the hint as backoff, within the
  existing publish timeout. A caller waiting on a confirm sees a slower
  publish, and a timeout error naming the limit if it never clears.
- Buffered publishes are counted as rejected and reported through the
  existing dropped-publish path, with the reason.
- Clients that predate the code see an error with the message text. The
  clients in this repo get the code in the same release.

### Which partitions

A partition is limited when any of its in-sync replicas is limited, because a
follower that cannot write would leave the in-sync set and stall confirms
anyway. Nodes report their levels in a heartbeat label, and each owner checks
the levels of its replicas. A disk filling on one node limits only the
partitions with a copy there, not the whole cluster.

### Never blocked

Consuming, acknowledging, rejecting, dead-lettering existing messages, queue
deletion, purge, admin reads and writes, coordination and replication
catch-up are never limited. Replication catch-up stops only at the reserve
level, where the follower leaves the in-sync set as it would after a fault.

## Per-queue limits

Queues can declare limits, as queue settings set through declaration:

- `max_messages` and `max_bytes`, unset by default.
- `overflow`: `reject` (the default when a limit is set) answers new publishes
  with `ERR_LIMITED` and reason `queue_full`. `drop_oldest` removes the oldest
  ready messages to make room, sending them to the dead-letter queue when one
  is configured.

Inflight messages count toward the limit but are never dropped. A queue limit
is enforced by the owner and replicates like any other queue setting.

## Settings

Node-level thresholds are a new `limits` section of the runtime settings, so
they are cluster-wide, live-editable and follow the configuration sources
design. Each threshold has a fraction and an absolute floor. The memory limit
override is a startup field, since it describes the node's machine.

## Segment pool

Spare segment files are the first thing released under disk pressure, and
today each log keeps up to 4 spares in its own directory. A node-wide pool
replaces that.

### Why

A reused segment file is faster than a fresh one: its blocks already exist, so
appends do not wait for the filesystem journal to record new blocks. A node
pool gives new and slow queues reused files from the start, lets idle queues
hand theirs back, and gives one place to enforce a spare byte budget and to
release spares under pressure.

### Ownership

The pool is created by the owner of the storage, one per node, and passed to
every log the node opens, the same way log runtime settings are passed today.
It is not a process-wide static, because tests run several brokers in one
process and their files must stay apart. A log opened without a pool gets a
private one with today's behavior.

Inside the pool, spares are grouped by filesystem, since a file can only be
renamed within one filesystem, and by segment size class. Queue logs and
stream payload logs use the configured segment size, and stream cursor and
metadata logs use one eighth of it, so there are two classes. Each filesystem
group keeps its spares in a `spares` directory on that filesystem.

### Making reuse safe across logs

Recovery currently ends a reused segment at the first record whose offset is
below the segment's base, since older records left in the file come from
earlier segments of the same log. That does not hold across logs: a file from
another queue can hold old records whose offsets continue the new segment
exactly, with valid checksums.

Each segment file carries a generation number in its header, counting how many
times the file has been used. Every record written into the file carries the
file's generation in the record header's reserved field, which today is always
zero. A record is valid only if its generation equals the header's. Every
leftover in a file, from any earlier use by any log, has a lower generation,
so the check is an exact comparison with no reliance on offsets or chance.

- The field is 16 bits. The pool retires a file before its generation would
  wrap, at 65,535 uses, and creates a fresh one instead.
- Records grow by no bytes. Readers and recovery add one comparison per record.
- The record format version and the log layout number change, as part of the
  0.5 storage layout break.
- The same check replaces the offset rule within a single log too.

### Ordering

1. When a file enters the pool, the pool's background worker rewrites its
   header as a spare header with the next generation and no base, then syncs
   it. This happens off the write path, long before the file is needed.
2. When a log takes a spare, it renames the file into its directory and
   writes the real header with the same generation and its base. That header
   syncs with the log's next fsync batch, as today.

A crash between the two leaves a file whose durable header is the spare
header. Recovery removes a trailing segment whose header does not name the
segment's own base, or returns it to the pool, and every record in the file
is older than its generation in any case. The recovery rule changes from
"names an older segment" to "names a different base", since a header from
another log can name a higher base.

### Budget

The pool holds at most a spare byte budget per filesystem, proposed as 2% of
the filesystem or 1 GiB, whichever is smaller. Releasing a spare deletes it.
Below the soft disk level the pool releases spares down to zero before
publishes slow.

## Visibility

- Clients get `ERR_LIMITED` with the reason and retry hint.
- The dashboard shows a banner per node and filesystem with the level, free
  space and the queues that use the most space.
- Metrics per node: level, free bytes, spare bytes, delayed and rejected
  publishes by reason.
- Attention items for every level change, and for any queue that is full.

## Implementation order

1. Generation stamps in records and segment headers, with the recovery rule
   change, inside the 0.5 layout break.
2. The node segment pool with size classes, budget and release.
3. Disk sampling, levels and the publish rejection with `ERR_LIMITED` in the
   broker and the clients.
4. Partition limits from replica levels through heartbeat labels.
5. Slowing between soft and hard.
6. Memory sampling with cgroup detection.
7. Per-queue limits with `reject` and `drop_oldest`.

## Test plan

- Records from a reused file of another log are never accepted, including a
  constructed case whose leftover offsets continue the new segment exactly.
- A crash at every step of taking a spare leaves either a valid segment or a
  removed or returned spare, with io-fault-injection.
- A small filesystem (a loop device or a size-limited tmpfs) filled by a
  queue without consumers: publishes slow, then fail with `ERR_LIMITED`. No
  log stops. Acknowledging frees space and publishes resume.
- The same in a three-node cluster with one small node: only partitions with
  a copy on that node are limited.
- A container memory limit: publishes are rejected before the kernel kills
  the process.
- Per-queue limits for both overflow modes, including dead-lettering on
  `drop_oldest`.
- Clients retry `ERR_LIMITED` with the hint and surface the reason on timeout.

## Open questions

- Whether slowing should also apply on the follower side, or only through the
  owner as described.
- Whether memory limits need finer accounting than resident memory, for
  example allocator statistics, to avoid late clearing.
- Whether per-user or per-connection publish rate quotas belong here or with
  the access policy.

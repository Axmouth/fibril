---
title: Implemented surface
description: Detailed checklist of what is wired in the current Fibril codebase.
---

This page inventories the behavior available in the current codebase, its
client and operator interfaces, and its limits. [Project status](/status/)
summarizes maturity; the [roadmap](/roadmap/) tracks remaining work.

## Reading This Page

Status meanings:

| Status | Meaning |
| --- | --- |
| Implemented | The main path is wired and has tests or direct operational surface. |
| Partial | A useful path exists, but important surfaces, limits, or polish remain. |
| Planned | The docs or design mention it, but users should not depend on it yet. |
| Out of scope | The behavior is intentionally not planned. |

## Queue Identity

See also: [core model](/concepts/core-model/) and
[partition routing](/development/partition-routing/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Topic plus optional group | Implemented | Broker, protocol, Rust client, TypeScript client, Python client, Go client, C# client, admin API, CLI |
| Default group normalization | Implemented | Empty group and `default` normalize to ungrouped on admin and CLI paths, and in Rust, TypeScript, Python, Go, and C# clients |
| Partitioned queue declaration | Implemented | `DeclareQueue.partition_count`, Rust client declare builder, config default, coordination catalogue |
| Producer partition routing | Implemented | Rust client topology cache, round-robin keyless routing, `partition_key` stable routing, version-fenced publish frames |
| Subscription fan-in | Implemented | Rust client opens per-partition subscriptions from topology and merges deliveries into one logical stream |
| Cluster catalogue (client) | Implemented | Rust, TypeScript, Python, Go, and C# clients expose the live set of declared queues and streams (with partition counts) via a snapshot accessor and a change-subscription, derived from topology and kept live by topology pushes (no extra round-trips) |
| Pattern subscribe / discovery routing (client) | Implemented | Opt-in `client.routing()` returns a routing view. `subscribe_pattern` (queues) and `subscribe_stream_pattern` (streams) fan in across every channel whose topic matches a `*`-glob and auto-attach channels that start matching later (driven by the catalogue feed). Manual and auto-ack variants. Rust, TypeScript, Python, Go, and C#. Client-side only, no broker changes |
| Operator-chosen partition id | Out of scope | Normal user-facing paths should choose queue and optional key, not a partition number |

Conditions and limits:

- A queue is addressed by topic plus optional group.
- Groups are namespaces, not consumer groups. They do not coordinate competing consumer membership by themselves.
- Declared queues can have more than one partition.
- Partition selection stays inside Fibril. Producers may provide a partition key
  for stable routing, or omit it for round-robin spread.
- If topology is unknown or standalone, clients conservatively use partition `0`.
- Partition count is fixed at creation in standalone mode. In Ganglion mode an
  experimental live-repartition path can grow or shrink a queue's partition
  count (see the experimental cluster surface below).

## Durable Queue State

See also: [reliability semantics](/reliability/semantics/) and
[many idle queues](/concepts/many-idle-queues/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Append-only message log | Implemented | Storage layer and broker publish path |
| Append-only event log | Implemented | Storage state changes and recovery |
| Snapshots | Implemented | Storage recovery and periodic snapshot path |
| Lazy startup indexing | Implemented | Existing queues can be indexed without materializing all queue state at startup |
| Queue recovery on first use | Implemented | First active operation can materialize and recover an indexed queue |

Conditions and limits:

- Durable messages and queue state live on disk.
- A queue can exist on disk without being loaded into memory.
- Loading a cold queue has a first-use cost.
- Single-node queue deletion is exposed through the admin API/dashboard. A
  coordinated multi-node delete is still pending.
- Message TTL (dropping individual messages by age) is implemented. Log
  retention by age (truncating old durable messages on a schedule) is not yet a
  user-facing feature.

## Recovery Quarantine

See also: [recovery quarantine](/reliability/recovery-quarantine/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Recovery reference verification | Implemented | Recovery checks each replayed event's referenced message offset against the message log's durable tail, and decodes every event record |
| `recovery.on_mismatch` policy | Implemented | Startup config: `quarantine` (default), `refuse`, or `ignore` |
| Per-partition quarantine | Implemented | A bad partition is parked (its ops error) while the rest of the broker stays up |
| Operator repair | Implemented | Admin quarantine banner + `/admin/api/quarantine/repair`; journaled suffix truncation retains earlier events and resumes before log opening |
| Readiness health | Implemented | `/readyz` reflects quarantine state and the configured policy |
| Quarantine metric | Implemented | `recovery.quarantined` gauge and `quarantines_total` counter in the recovery snapshot, exported as `fibril_recovery_quarantined` and `fibril_recovery_quarantines_total` on `/metrics` |

Conditions and limits:

- Recovery folds replayed enqueue/cancel events against the durable message tail.
  It can discard an unconfirmed suffix left by interrupted parallel publication.
  Interrupted checkpoint installation is resolved from its durable installation
  record before this ordinary replay path. See checkpoint recovery below.
- Corrupt events and unexplained non-enqueue references to missing payloads use
  the configured corruption policy. A preceding dangling enqueue does not hide
  these failures or authorize automatic deletion.
- `refuse` is lazy today (a mismatch is caught when the partition is first used);
  an eager whole-disk variant at boot is a tracked follow-up.

- Queue checkpoint decoding rejects malformed fields before replacing actor state.
  Cancelling a checkpoint capture releases its owner pause, and capture serializes
  with role changes and recovery seals.
- Explicit sealed inspection can reconstruct fully retained queue state at a
  common exclusive event boundary, with canonical state and separate input-history
  digests. Compacted checkpoint authority, resource lineage and automatic source
  selection remain pending; see [recovery sealing](/reliability/recovery-sealing/).

## Publish

See also: [client usage](/clients/) and
[retries and delays](/reliability/retries-delays/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Unconfirmed publish | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Confirmed publish | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Pipelined confirmation handles | Implemented | Rust, TypeScript, Python, Go and C# clients; each publish retains an individual confirmation. Rust handles implement `Future` and retain `.confirmed()` |
| Delayed publish | Implemented | TCP protocol, broker, Rust client, TypeScript client, Python client, Go client, C# client |
| Content type metadata | Implemented | Protocol metadata, Rust client, TypeScript client, Python client, Go client, C# client, delivery path |
| Reserved metadata headers | Implemented | Broker protocol handler rejects `fibril.*` and `stroma.*` user headers |
| Partition key routing | Implemented | Rust client `NewMessage::partition_key`, protocol publish metadata, server per-partition publish routing |
| Partitioning-version fence | Implemented | Client stamps routed version, and server redirects stale publishes before appending |
| Message TTL (drop by age) | Implemented | `Publish.ttl_ms` + per-queue `default_message_ttl_ms` on declare, owner resolves an absolute deadline, expiry worker drops via the DLQ/discard pipeline. Rust `Publisher::expiring` + `QueueConfig::default_message_ttl`, TypeScript `Publisher.expiring` + `QueueConfig.defaultMessageTtl`, Python `Publisher.expiring` + `QueueConfig.default_message_ttl` (seconds-native) |

Conditions and limits:

- Confirmed publish returns the broker-assigned offset.
- Unconfirmed client calls only wait for the local client engine or command path, not for a broker-assigned offset.
- Delayed publish uses a distinct delayed-publish frame and a `not_before` deadline.
- Content type is stored outside the user header map for common cases.
- Manual `content-type` headers are interpreted as content type metadata by clients.
- Broker-side validation rejects reserved system header prefixes on normal and delayed publish.
- A partition key affects only partition selection. It is not a RabbitMQ-style
  routing key and is not part of the durable payload.
- Stale partitioning topology is handled by redirecting the client to refresh
  and retry.
- Message TTL drops a message that is not consumed before its deadline. A
  per-message `ttl_ms` wins over the queue's `default_message_ttl_ms`; with
  neither set a message never expires. The owner resolves the deadline against
  its own clock at publish, so it survives recovery and replication. An expired
  message is never dropped while it is in flight, and the drop honors the queue's
  dead-letter policy (discard when no DLQ is configured, otherwise dead-lettered
  with reason `expired`). This is per-message age-drop, not queue expiration
  (auto-deleting an idle queue), which is a separate, not-yet-implemented idea.

## Subscribe and Delivery

See also: [backpressure](/concepts/backpressure/) and
[client usage](/clients/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Manual ack subscriptions | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Auto ack subscriptions | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Bounded prefetch | Implemented | Broker delivery path and clients |
| Backpressure | Implemented | Pull-based delivery bounded by prefetch |
| Unsubscribe redistribution | Implemented | Broker tests cover prefetched unacked messages returning to active subscribers |
| Competing consumers (default) | Implemented | Many consumers per queue, fair dispatch, unordered |
| Exclusive consumer groups | Partial | `.exclusive()`/`consumer_target` (Rust), `consumerGroup()`/`consumerTarget()` (TypeScript), and `consumer_group()`/`consumer_target()` (Python) + TCP protocol, per-partition gate, balanced+sticky assignment, soft `consumer_target`, assignment push, reconnect restore, one-cohort-per-queue guard |
| Cross-broker cohort coordination | Partial | Member identity + controller (aggregate→plan→publish) + owner apply all wired in cluster bootstrap, coordination-level multi-node rebalance test exists, fuller broker/client scenarios are still growing |
| Partition fan-in | Implemented | All five clients subscribe to all known partitions, merge deliveries while keeping per-partition settlement routing, and pick up partitions added by a live grow |

See [consumer groups](/concepts/consumer-groups/) for the user-facing model.

Conditions and limits:

- Manual ack messages must be completed, failed, retried, or retried after a delay.
- Auto ack happens only after the delivery frame is successfully sent.
- Prefetch limits how many messages a subscription can hold at once.
- If a subscription ends with prefetched but unsettled messages, those messages are returned for redelivery.
- A message can be delivered more than once under failure, retry, or lease expiry conditions.
- Exclusive consumer groups are opt-in (Rust `.exclusive()`, TypeScript `.consumerGroup()`, Python `.consumer_group()`). Without them, consumers compete (no ordering). A queue has a single exclusive cohort. The clients expose the assignment-events stream (Rust `assignment_events()`, TypeScript `onAssignmentChange`, Python `on_assignment_change`).
- Cross-broker cohort balance is advisory/eventually-consistent. The per-partition delivery gate is always the correctness backstop. Single-node is fully covered by tests.
- Plain subscriptions fan in over known partitions and pick up partitions added by a live grow. A topology warm step at
  connect prevents pure consumers from staying on partition `0` when topology is
  available.

## Plexus Streams

See also: [Plexus streams](/concepts/plexus-streams/) and
[client usage](/clients/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Stream channel type (fan-out) | Implemented | Stroma StreamEngine (cursors/retention), broker fan-out actor, TCP protocol (`DeclarePlexus`/`SubscribeStream`, reuses Publish/Deliver/Ack), Rust, TypeScript, Python, Go and C# clients |
| Declare plexus (partitions, durability, retention, replication factor) | Implemented | `declare_plexus`/`declarePlexus` + `StreamConfig` in the clients, including a per-stream replication-factor override that beats the cluster default |
| Durable named cursor | Implemented | Broker-side cursor per (channel, partition, name), resuming on restart and advancing on ack |
| Cursor commit microbatching | Implemented | Broker batches durable cursor commits and flushes pending commits during shutdown |
| Ephemeral start position | Implemented | latest / earliest / offset / n-back / by-time |
| Header filter | Implemented | AND of `header == pattern` with `*` glob, stream-only |
| Client-side fan-in across partitions | Implemented | Reuses the queue fan-in supervisor (failover resubscribe + live-grow pickup). Streams stay out of the reconnect-reconcile registry and resume via the cursor |
| Durability tiers (ephemeral/speculative/durable) | Implemented | Express lane wired: durable fsyncs before deliver/confirm, speculative delivers off the staged offset and defers the confirm until durable (with a `fibril.speculative` header), ephemeral delivers and confirms at staging with no fsync (AfterWrite). All log-backed |
| Cross-client wire vectors | Implemented | `DeclarePlexus`/`DeclarePlexusOk`/`SubscribeStream` pinned in `clients/wire_vectors.json`, asserted by all five client test suites |

Conditions and limits:

- Every consumer of a stream sees every record. Partitioning a stream is for write
  throughput and per-key ordering, not consumer work-sharing.
- A durable name is single-active (last commit wins). Distinct names are
  independent fan-out consumers, each with its own per-partition cursor.
- A fresh durable name starts at the earliest retained record so it cannot
  silently miss data.
- Retention drops whole sealed segments (by age/bytes/records) and clamps a cursor
  that lags past the retained window.
- The durability tier changes delivery and confirm timing end to end: speculative
  and ephemeral deliver off the staged offset (no fsync wait), with speculative
  deferring the confirm until durable and ephemeral confirming immediately.

## Reconnects

See also: [reconnects](/reliability/reconnects/) and
[reconnection grace internals](/development/reconnection-grace/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Resume identity handshake | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Reconnect grace window | Implemented | Runtime settings and TCP handler. On by default in the server seed (`connection.reconnect_grace_ms`, 5s); opt out with 0. Both manual and auto-ack subscriptions participate |
| Conservative subscription reconciliation | Implemented | Broker, Rust client, TypeScript client, Python client, Go client, C# client |
| Restore-client-subscriptions policy | Implemented | Broker, Rust client, TypeScript client, Python client, Go client, C# client |
| Reconnect observability | Implemented | Admin overview, TCP metrics log, structured reconciliation logs |
| Planned restart drain | Implemented | `POST /admin/api/drain` broadcasts a `GoingAway` push (grace deadline + message) to connected clients, surfaced by the clients as an app-observable event. In coordinated mode the node also marks itself draining: the controller holds replicated handoffs as pending recovery requests until confirmed-history proof is available, the draining node receives no new placements, and the call returns with handoff progress once ownership has moved or `connection.drain_handoff_timeout_ms` (default 30s) elapses. Follower-less partitions stay put and fail over reactively as before |
| Typed subscription closure | Implemented | Rust `SubEvent::Closed` and `close_reason()`, TypeScript/Python `SubscriptionClosedError`, Go `CloseReason()` after channel closure, C# `SubscriptionClosedException` |
| Safe automatic resubscription | Implemented | Supervised subscriptions recreate on supported owner-move or broker-advised recreate outcomes; opt-out exposes the typed close instead |
| Durable broker restart reconciliation | Implemented | Broker-local persisted session identity and subscription metadata; `resumed_after_restart` within `connection.resume_session_restart_ttl_ms` (default 60s, 0 disables) |
| Stale-delivery settlement | Implemented | All five clients stamp manual deliveries with their connection incarnation, reject stale settles without sending a frame, and route valid settles through the current connection |
| In-flight publish replay | Out of scope | Clients do not replay old in-flight protocol requests |

Conditions and limits:

- Live-process reconnect grace requires the same resume identity before the grace
  window expires. Restart resume uses its separate persisted-session TTL.
- Restart resume is local to the owning broker. It does not transfer a session to
  another node or restore authentication from persisted session metadata.
- Delivery tags do not survive a broker restart. Unacknowledged work redelivers
  according to the queue's at-least-once semantics; settling a held stale delivery
  returns a typed, non-retryable error.
- The conservative policy keeps matching subscriptions, drops server-only
  subscriptions, and closes client-side streams that the broker cannot prove are
  still valid.
- Restore mode can recreate missing server-side subscriptions reported by the
  client after a successful resume.
- Terminal failures surface through the typed receive API. Automatic recreation
  is limited to supported safe outcomes; it does not replay inflight publishes.
- Stream subscriptions resume through their durable cursors. A cursor ACK sent
  immediately after a resumed reconnect can race re-subscription and be dropped;
  the uncommitted record can replay.

## Settlement, Retry, and Leasing

See also: [core model](/concepts/core-model/),
[reliability semantics](/reliability/semantics/), and
[retries and delays](/reliability/retries-delays/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Ack | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Nack without requeue | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Immediate retry | Implemented | TCP protocol, Rust client, TypeScript client, Python client, Go client, C# client |
| Delayed retry | Implemented | TCP protocol, broker, Rust client, TypeScript client, Python client, Go client, C# client |
| Lease expiry | Implemented | Runtime delivery settings and broker/storage path |
| Settlement history | Implemented | Range-compressed settled offsets with a derived contiguous frontier, preserved through snapshots and replication |

Conditions and limits:

- Queue ACK settles a delivered message; stream ACK advances its cursor.
- Fail means nack without requeue. Depending on queue policy, the message may be discarded or dead-lettered.
- Retry means nack with requeue.
- Delayed retry requires `requeue=true` and a `not_before` deadline.
- Expired leases can return inflight messages to ready.
- Lease timing is controlled by runtime settings.
- Queue NACK semantics do not apply to Plexus streams. A stream NACK currently
  has no broker effect even if the client call succeeds; stream retry/fail
  semantics remain undefined.

## Dead Lettering

See also: [dead lettering](/reliability/dead-lettering/) and
[metadata policy](/development/metadata-policy/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Per-queue DLQ policy | Implemented | Rust client, TypeScript client, Python client, Go client, C# client, `fibrilctl`, admin API |
| Global DLQ target | Implemented | Stroma-owned runtime state, admin UI/API, `fibrilctl` |
| Max retry routing | Implemented | Broker/storage path and tests |
| Dead-letter reasons | Implemented | `retries_exhausted`, `terminal_nack`, `pending_recovery`, `expired` (message TTL) |
| DLQ metadata | Implemented | Reserved `stroma.dlq.*` headers on dead-lettered messages |
| DLQ replay by selected offsets | Implemented | Broker/storage path, admin API, admin dashboard, `fibrilctl` |
| Bulk replay filters | Planned | Not yet implemented |
| Delete or ack DLQ items from replay | Planned | Replay copies back to source and leaves the DLQ message in place |

Conditions and limits:

- Queue policy can discard, use the global DLQ target, or use a custom queue-specific target.
- The global target is live persisted runtime state, not startup config.
- Replay requires active DLQ messages with source metadata.
- Replay strips system metadata from the replayed copy.
- Replay skips offsets that are missing, inactive, or missing required DLQ source metadata.
- Replay currently accepts at most `100` offsets per request through the admin API.

## Message Inspection

See also: [admin dashboard](/admin-dashboard/) and
[dead lettering](/reliability/dead-lettering/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Inspect active queue messages | Implemented | Admin API, admin dashboard, `fibrilctl` |
| Include settled offsets | Implemented | Admin API, admin dashboard, `fibrilctl` |
| Payload previews | Implemented | Admin API and dashboard, base64 encoded |
| Status filtering | Implemented | Admin API and dashboard |
| Paginated offset navigation | Implemented | Admin dashboard and API offset parameters |
| High-volume live polling view | Out of scope | Use metrics and logs instead |

Conditions and limits:

- By default, inspection returns messages still active in queue state.
- Active states include ready, inflight, delayed, and pending DLQ.
- Settled records can be included explicitly.
- Offsets without matching inspectable state or log records are skipped rather than shown as misleading partial rows.
- Inspecting a queue can load that queue into memory. If idle cleanup is enabled
  and no active publisher or subscriber exists, cleanup can unload it again.
- Default page size is `50`.
- Current API hard cap is `5000` messages per request.
- Payload previews default to `4096` bytes and cap at `1048576` bytes.
- Inspection reads persisted data and can affect realtime performance on large requests.

## Sparse Queues and Idle Cleanup

See also: [many idle queues](/concepts/many-idle-queues/) and
[idle queue internals](/development/idle-queue-internals/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Lazy loading | Implemented | Storage and broker startup behavior |
| Idle queue cleanup | Implemented | Runtime settings, broker worker, Stroma unmaterialization |
| Publisher idle expiry | Implemented | Runtime settings and broker publisher cache cleanup on incoming connection frames |
| Cleanup race guard | Implemented | Broker prevents cleanup from racing with newly created publisher/subscriber leases |
| Active lease materialization | Implemented | Publisher and subscriber creation materialize storage before returning a usable handle |
| Inspection reload cleanup | Implemented | Queues loaded by admin inspection can be unloaded again by idle cleanup |
| Cleanup observability | Partial | Admin queues page, `/admin/api/queues_debug`, cumulative metrics counters |
| Exact cleanup timing | Out of scope | Cleanup is approximate and sweep based |

Conditions for a queue to be unloaded from memory:

- idle queue cleanup is enabled
- the broker knows the queue as a cleanup candidate
- no active subscribers remain
- no active publishers remain
- no messages are currently leased to consumers
- no pending settlement work is still draining
- no broker delivery tags remain active
- storage reports no inflight messages
- the configured idle window has elapsed
- the cleanup sweep reaches that queue
- storage accepts the unmaterialization attempt
- no publisher or subscriber lease is being created at the same time as cleanup

Conditions that keep a queue in memory or skip cleanup:

- active publisher or subscriber
- a new publisher or subscriber lease arriving while cleanup is trying to unload the queue
- not idle long enough
- pending settlements
- broker-tracked deliveries still active
- storage-tracked inflight messages
- storage race while another operation is materializing or changing the queue
- queue not tracked by the broker for cleanup

Operator-facing behavior:

- Unloading is not deletion.
- Durable messages, events, snapshots, and queue identity stay on disk.
- A later publish, subscribe, or admin operation can reload the queue.
- Creating a publisher or subscriber loads the queue before the handle is returned.
- Admin message inspection can load a queue without creating a publisher or subscriber lease.
- The first operation on a cold queue can pay reload cost.
- Long-lived producer connections can keep queues active unless publisher idle expiry is enabled.
- Automated cleanup does not keep rechecking storage after a queue is already unloaded unless new queue activity happens.

Observability currently includes:

- loaded versus indexed-only queue state on the admin queues page
- active publisher and subscriber counts
- idle time and last-used time when known by the current process
- last cleanup result or skip reason per tracked queue
- cumulative cleanup attempts and selected outcomes in broker metrics snapshots

Developer-facing note:

- Broker `PublisherHandle` is intentionally not cloneable. A broker publisher handle owns one sink task and one active-publisher lease. Creating another independently tracked publisher must go through `get_publisher`, which creates another sink and another lease.

## Runtime Settings and Startup Config

See also: [configuration](/configuration/),
[configuration policy](/development/config-policy/), and
[configuration design](/development/config-design/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| TOML startup config | Implemented | Config crate and server binary |
| Storage writer buffer factor | Implemented | `storage.keratin.writer_buffer_factor` or `FIBRIL_KERATIN_WRITER_BUFFER_FACTOR`; restart-time capacities for message/event log writer and notification channels, default preserved at 8,192 slots each |
| Env and CLI overrides | Implemented | Config crate and server binary |
| Admin auth startup config | Implemented | TOML, env, CLI, server wiring |
| Metrics exposition startup config | Implemented | `admin.metrics_per_channel` via TOML and env |
| Keratin fsync and segment startup config | Implemented | Config crate and server wiring |
| Runtime delivery settings | Implemented | Runtime settings manager and admin UI/API |
| Runtime idle cleanup settings | Implemented | Runtime settings manager, admin UI/API, broker worker |
| Runtime locks | Implemented | Locked groups reject admin edits |
| Global DLQ runtime state | Implemented | Stroma-owned versioned setting |
| Corrupt runtime settings recovery UI | Partial | Load issue reporting exists, richer operator reset flow is pending |

Conditions and limits:

- Startup config is loaded before the process starts serving.
- Startup precedence is defaults, TOML, environment, then CLI.
- Runtime seeds initialize persisted runtime settings only when no runtime settings exist.
- After runtime settings exist, persisted state owns those values unless a group is locked by startup config.
- Runtime updates use expected versions to detect concurrent edits.
- Locked runtime groups reject admin edits instead of silently applying changes.

## Admin Surface

See also: [admin dashboard](/admin-dashboard/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Overview metrics | Implemented | Dashboard and API |
| Control-plane activity feed | Implemented | `GET /admin/api/audit` returns a bounded in-memory ring (newest 512 entries, reset on restart) of operator actions, attention transitions, membership changes, and stream lag-recovery events, rendered live on the dashboard's Activity page |
| Attention feed | Implemented | `GET /admin/api/attention` names conditions needing an operator, most severe first: quarantined partition, expired or expiring certificate, failed settings load, backlog with no consumer, backlog growing despite consumers, low disk on the data directory, stalled replication follower, queue state error, broker left draining. Drives the Overview panel, the sidebar badge, and opt-in desktop notifications |
| Dashboard history series | Implemented | `GET /admin/api/history`: per-broker in-memory time series (5s samples, last 30 minutes, reset on restart) of throughput, backlog, connections, and process memory/CPU/disk, plus per-queue depth series. Feeds the Overview charts, queue sparklines, and the resources panel |
| Served-certificate metadata | Implemented | `GET /admin/api/tls`: fingerprint, validity window, and subject of the leaf the broker currently serves. Feeds the Security page and the certificate-expiry attention rule |
| Live dashboard updates | Implemented | `GET /admin/api/events?families=...` streams named data-family snapshots over server-sent events on a ~2s tick, one multiplexed stream per open dashboard page, serialized once per tick regardless of subscriber count and idle when nobody watches. Dashboard pages fall back to polling when the stream is unavailable |
| Operator test publish | Implemented | `POST /admin/api/publish` sends one text message through the broker's real publish path (partition pick, durable confirm, delivery) to a queue or Plexus stream, stamped with a reserved `fibril.test: admin` header so consumers can recognize operator traffic. Surfaced as a button on the dashboard's queue detail page and stream cards |
| Connections and subscriptions | Implemented | Dashboard and API; per-connection publish counters, and a diagram view of the broker as the fibril ring with publisher connections plugged into one side and subscribers the other, pulses following each connection's live rate |
| Queues page | Implemented | Dashboard and API, per-partition expand, follower-replication view, DLQ-policy column, hide-inactive toggle + search filter, live in/s and out/s columns, and a depth-trend drilldown (chart, rates, oldest ready message's age) |
| Create queue | Implemented | `POST /admin/api/queues` + dashboard form (partition count, optional DLQ policy, optional default message TTL); coordinated in cluster mode - the partitioning registers with coordination first, its count is authoritative, and the controller places every partition |
| Delete queue (single-node) | Implemented | `POST /admin/api/queues/delete` + per-row dashboard button; refuses while messages are inflight (409) and in cluster mode (501) pending coordinated teardown |
| Streams page | Implemented | Dashboard and API (`GET /admin/api/streams`), per-topic partition rows with head/tail/retained, declared durability and retention, per-partition role and applied offset, a follower-replication view (`GET /admin/api/streams_debug`), an append-rate chip, and a durable-cursor table per stream (cursor name, tail/catching-up, behind, read rate, last advance) |
| Create stream | Implemented | `POST /admin/api/streams` + dashboard form (partition count, durability tier, optional retention by records/bytes/age); coordinated in cluster mode like queue declares, so multi-partition streams place fully |
| Settings page | Implemented | Dashboard and API, incl. replication and streaming-replication settings |
| Message inspection page | Implemented | Dashboard and API; targets a specific partition (`partition` query param - offsets are per partition) |
| DLQ replay controls | Implemented | Dashboard and API |
| Topology page | Implemented | Coordination nodes, per-partition ownership/epochs (queues and streams), consensus block, per-node liveness/raft-id/publish+delivery rates/runtime facts (version, uptime, TLS state, cert expiry) from heartbeat labels; rendered as the living cluster diagram (ring sprites, fiber tendrils, load signals) or a card list with a placement matrix (`?view=list`) |
| Repartition control | Partial | `/admin/api/repartition` and a topology-page form (Ganglion mode) |
| Coordination membership control | Partial | Add/remove a consensus voting member from the topology page and API |
| Cohort visibility | Partial | Per-broker exclusive-cohort membership with live per-partition coverage on the subscriptions page, queue-detail partition cards, and `/admin/api/cohorts` |
| Quarantine banner and repair | Implemented | Global banner, `/admin/api/quarantine`, repair endpoint, `/readyz` |
| Prometheus metrics endpoint | Implemented | `GET /metrics` on the same listener and auth, see [Metrics Export](#metrics-export) |
| Basic admin auth | Implemented | Login, logout, session cookie, auth-disabled mode |
| Fine-grained admin roles | Planned | Current model is admin access or auth disabled |

Conditions and limits:

- When admin auth is enabled, dashboard pages require login.
- When admin auth is disabled, dashboard pages are accessible directly and should be protected by network boundaries.
- Settings updates use version checks.
- The dashboard is for operational inspection, not continuous high-frequency monitoring.

## Metrics Export

See also: [monitoring](/deployment/monitoring/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Prometheus `/metrics` endpoint | Implemented | Admin listener, text exposition format, behind admin basic auth and admin HTTPS when enabled |
| Node-level families | Implemented | Broker message totals, storage operation totals, transport and session-resume counters, open connection/subscription gauges, recovery quarantine gauge and counter, replication worker summary |
| Per-channel series | Implemented | Queue ready/inflight, stream subscriptions and lag evictions, follower applied offsets - materialized channels only, gated by `admin.metrics_per_channel` (default on) |
| OpenTelemetry export | Planned | Separate later item, not part of the Prometheus endpoint |

Conditions and limits:

- A scrape reads atomic counters and dashboard snapshot views, never a delivery hot path.
- Counters are process-lifetime monotonic and reset on restart.
- Labels carry channel identity and outcome tags only, never message data.
- Sparse (declared but idle) queues contribute no per-channel series until they materialize.

## CLI Surface

See also: [source deployment](/deployment/source/) and
[dead lettering](/reliability/dead-lettering/).

| Item | Status | Implemented command |
| --- | --- | --- |
| Queue declaration | Implemented | `fibrilctl queue declare` |
| Global DLQ get, set, clear | Implemented | `fibrilctl admin global-dlq` |
| Message inspection | Implemented | `fibrilctl admin messages` |
| DLQ replay | Implemented | `fibrilctl admin dlq replay` |
| Queue observability | Implemented | `fibrilctl admin queues` |
| Pub or sub from CLI | Planned | Useful for manual testing, not currently implemented |

Conditions and limits:

- CLI uses broker TCP for queue declaration.
- CLI uses admin HTTP for admin commands.
- By default it reads the same startup config path handling as the server config crate.
- Container usage assumes the CLI runs where it can reach the broker or admin surface.

## Client Surface

See also: [client usage](/clients/),
[quickstart](/quickstart/), and
[reconnection grace](/development/reconnection-grace/).

Rust, TypeScript, Python, Go and C# expose the shared messaging and reconnect
surface. Python offers asynchronous APIs and a blocking facade. The
[client feature matrix](https://github.com/Axmouth/fibril/blob/main/clients/FEATURE_MATRIX.md)
records language-specific options and exceptions.

| Item | Status | Client surface |
| --- | --- | --- |
| Connect, auth and TLS | Implemented | All five clients, including mTLS and typed TLS errors |
| Confirmed, unconfirmed and delayed publish | Implemented | All five clients, including pipelined confirmation handles and message TTL |
| Queue consume and settlement | Implemented | All five clients: manual/auto ACK, immediate/delayed retry, prefetch and queue declaration |
| Stream consume | Implemented | All five clients: durable cursors, start positions, filters and partition fan-in |
| Routing and discovery | Implemented | All five clients: topology cache, owner redirects, partition-key routing, catalogue and pattern subscriptions |
| Exclusive consumer groups | Implemented | All five clients: membership and assignment-change events; broader cluster scenario coverage remains partial |
| Explicit reconnect and automatic reconnect opt-out | Implemented | All five clients return the handshake outcome and expose a policy switch |
| Reconciliation and typed subscription closure | Implemented | All five clients; see [Reconnects](#reconnects) for resume, recreation and staleness conditions |
| Guided errors and retry advice | Implemented | Broker error frames and client-local errors carry context and retry classification; shared error-guide fixtures cover wording |
| Payload codecs and headers | Implemented | Raw/text/JSON/custom content types and user headers; msgpack support follows each language's codec/dependency options |
| Bounded write coalescing | Implemented | Rust batches ACK writes with queued commands; Python/TypeScript buffer pipelined confirmed publishes; Go/C# drain queued writes into batches |

Conditions and limits:

- Publisher handles and newly created subscriptions use the current connection
  after reconnect. Valid held manual deliveries settle through that connection;
  stale deliveries return a typed error.
- Automatic reconnect defaults to a bounded attempt before a new operation.
  Subscription supervisors handle supported recreation and ownership changes;
  terminal reasons remain visible to the application.
- TypeScript represents protocol `u64` values, including offsets, as `bigint`.
- Buffered writes preserve individual frames and confirmations. A buffered send
  is not evidence of durability; failed connections fail outstanding requests.
- Python's ordinary `publish(confirm=True)` sends immediately; its pipelined
  confirmation API uses the buffer. TypeScript's confirmed APIs share the
  buffered engine path. Python/TypeScript expose byte/count/time bounds, with
  `max_frames=1` / `maxFrames: 1` available for immediate writes.
- Transport failure does not trigger replay of already-inflight publish requests.

## Server Output

| Item | Status | Implemented surface |
| --- | --- | --- |
| Output priority queues | Implemented | Control and replication frames share the high-priority writer path; delivery and publish responses also use the connection writer |
| Prompt tail flushing | Implemented | Buffered output flushes when both outgoing queues drain; count, byte and elapsed-time limits bound batches under sustained traffic |

Output flushing changes when eligible frames reach the socket. Publish
confirmation still waits for the configured local and replica durability gates.
ACK and confirmation semantics remain unchanged.

## Benchmarks and Operational Scripts

See also: [benchmarks](/benchmarks/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Throughput benchmark | Implemented | Rust bench helper and shell scripts |
| Steady-state rate benchmark | Implemented | Bench binary and scripts |
| Memory sampling during bench | Implemented | Bench scripts |
| Load/eviction memory probe | Implemented | `scripts/memory-audit.sh` checks eviction and settled-RSS growth across repeated cycles |
| Formal CI benchmark reporting | Planned | Local and informal docs exist, reproducible CI reporting is pending |
| Repository verification helper | Implemented | `scripts/check.sh` runs shell/template validation, Rust formatting and workspace tests, TypeScript tests, and website verification |

Conditions and limits:

- Current benchmark numbers are local architecture checks.
- They are not a stable published performance contract.
- Hardware, storage, durability settings, payload size, queue depth, and batching strongly affect results.

## Validation and Compatibility Tooling

| Item | Status | Implemented surface |
| --- | --- | --- |
| Deterministic cluster simulation | Implemented | Turmoil multi-broker harness exercises election, replication, failover, partitions, fencing and repartition scenarios |
| Chaos and soak tools | Implemented | Broker recovery/soak tests and `scripts/cluster-tryout.sh --chaos` for process faults under load and reconvergence checks |
| Cross-client wire vectors | Implemented | Shared `clients/wire_vectors.json` fixtures exercised by Rust, TypeScript, Python, Go and C# tests |
| Storage compatibility fixture | Partial | `datadir-v0.4.0` fixture, generator script and recovery test cover standalone queues, snapshots, stream data/cursors and DLQ state |

Conditions and limits:

- Existing scenarios cover specific failures; they do not certify production HA.
- The storage fixture does not cover coordination WAL/snapshots or persisted
  auth-user/runtime-settings documents.
- Supported compatibility baselines, vector regeneration enforcement,
  previous-client checks and mixed-version rolling-upgrade coverage remain open.

## Deployment Surface

See also: [source deployment](/deployment/source/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Website Docker deployment | Implemented | Compose file and Traefik labels |
| Broker server image | Implemented | Dockerfile and publish workflow |
| `fibrilctl` in image | Implemented | Server image includes CLI |
| Cluster tryout and demo workload | Implemented | Cluster Compose and tryout script, with `fibril-demo` running simulated business workloads through the client |
| Source deployment docs | Implemented | Docs site |
| Linux allocator THP policy | Implemented | `MIMALLOC_ALLOW_THP=0` disables transparent huge pages for the broker process at startup; restart required, workload-dependent memory/performance tradeoff documented under [Linux memory policy](/configuration/#linux-memory-policy) |
| Full production hardening guide | Partial | Basic guidance exists, deeper ops runbook is pending |

Conditions and limits:

- Server image exposes broker TCP and admin HTTP ports.
- Persistent broker data should be mounted under the configured data directory.
- Admin auth should be enabled outside local protected environments.

## Transport Security

See also: [configuration](/configuration/) for the `tls` section fields.

| Item | Status | Implemented surface |
| --- | --- | --- |
| TLS startup config (`tls` section) | Implemented | Config crate with env overrides and guided validation errors |
| Broker listener TLS | Implemented | rustls acceptor on the protocol listener |
| Operator-supplied PEM material | Implemented | `tls.cert_path` + `tls.key_path` |
| Per-deployment generated material | Implemented | `tls.auto_self_signed`: CA + server cert under `<data_dir>/tls`, CA SHA-256 fingerprint printed at startup |
| TLS/plaintext mismatch detection | Implemented | Both directions named at accept. A plaintext client on a TLS listener receives an error frame (code 426) instead of a bare close |
| `fibrilctl cert` | Implemented | `cert generate` creates the per-deployment material ahead of first boot (extra SANs via `--san`), `cert fingerprint` prints a PEM certificate's SHA-256 |
| First-boot setup mode | Implemented | `setup.mode = true` + no marker serves only a localhost setup page: TLS choice (generate, supply PEMs, or skip), an optional admin user, and an optional cluster secret. The choices persist as a config overlay layered below explicit config, plus the secret file |
| Dashboard TLS status | Implemented | The settings startup summary reports the TLS state, material source, CA fingerprint, and admin coverage, with the enable guide when disabled |
| Client TLS options | Implemented | Rust, TypeScript, Python, Go, and C#: OS-roots default, CA file, SHA-256 fingerprint pin (leaf-or-CA, path-validated), server-name override, and the shared typed error taxonomy (426 mismatch vs certificate trust vs config). Python CA-fingerprint pinning needs 3.13+ and the optional `cryptography` extra; leaf pinning is standard-library only |
| Admin dashboard HTTPS | Implemented | Served from the same `tls` material when enabled, `tls.admin_enabled = false` keeps it on plain HTTP for reverse-proxy setups |
| Inter-broker TLS | Implemented | `tls.inter_broker` (follows `enabled`) wraps follower-to-owner replication dials and the coordination raft channel; peer trust via `tls.peer_ca_path`, the generated CA, or OS roots; mismatches named in both directions |
| Shared-CA material lane | Implemented | A generated-material dir holding only `ca.pem` + `ca.key` mints that node's server certificate from the shared CA on boot (or via `fibrilctl cert generate`) |
| Live certificate reload | Implemented | `POST /admin/api/tls/reload` + `fibrilctl admin reload-tls`: validated swap of the serving pair, old material keeps serving on rejection, established connections unaffected |
| mTLS client auth | Implemented | `tls.client_auth = off/request/require` + `tls.client_ca_path` (falls back to the generated CA). A verified certificate whose identity (first DNS SAN, else CN) names an existing user authenticates with no AUTH frame; unknown identities are a transport pass only; `@` names never map. `require` rejects certless clients in the handshake. Client cert options + a typed required-cert error in Rust, TypeScript, Python, Go, and C#; `fibrilctl cert issue` mints workload certificates from the deployment CA; brokers present their own leaf on peer dials |

Conditions and limits:

- Live reload rotates the leaf certificate under the same CA. Rotating the CA itself requires a rolling restart and re-pinning fingerprint-pinned clients.
- Inter-broker TLS assumes every node's certificate chains to one CA. Independently generated per-node CAs do not interoperate; use the shared-CA lane or `tls.inter_broker = false`.
- The cluster secret keeps authenticating nodes inside the TLS session; transport identity and membership are separate layers.
- Certificate identities replace the password proof only: the user store stays the single authority on who exists, so listings and future authorization stay uniform.
- Rotating the client CA is restart-required, same rule as the server CA.
- Generated self-signed material that a client does not verify defeats passive snooping only. Clients should trust the generated `ca.pem` or pin the printed fingerprint.
- Fibril never ships certificates.

## Authentication

See also: [configuration](/configuration/) for the `auth` section.

| Item | Status | Implemented surface |
| --- | --- | --- |
| User store | Implemented | Durable document of argon2 hashes, seeded on first boot from `auth.seed_users` or the env pair, after which the store owns the users |
| Loopback-only default credentials | Implemented | `fibril`/`fibril` works from loopback only, remote rejections carry the create-a-user guide. A real `fibril` user replaces the pair |
| Cluster secret (node principal) | Implemented | Replication authenticates as `@node` with the shared secret (`fibrilctl secret generate`), required in ganglion mode. Usernames starting with `@` are reserved |
| User management (fibrilctl + dashboard) | Implemented | `fibrilctl user add/passwd/remove/list` and the dashboard settings Users section, over `/admin/api/users` |
| Cluster replication of user changes | Implemented | Live edits CAS into the `fibril/auth_users` cluster document and other nodes adopt it, parallel to the runtime-settings document |
| Authorization (per-topic permissions) | Planned | Separate later arc |

Conditions and limits:

- Password verification assumes TLS on non-loopback connections; the server
  warns loudly when users exist and TLS is off.
- The auth failure reply names the fix (loopback rule, missing cluster
  secret) instead of failing opaquely.

## Experimental Cluster and Replication Surface

See also: [clustering](/concepts/clustering/) and
[replication](/reliability/replication/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Ganglion coordination mode | Partial | Startup config, embedded coordinator, TCP transport, broker self-registration, topology endpoint |
| Broker advertise address | Partial | `broker.listener.advertise` / `FIBRIL_BROKER_ADVERTISE` (priority-ordered list, peer-derived default), carried to clients as `owner_endpoints`; clients dial the first entry (Rust high-level client connects to socket-address entries only) |
| Queue catalogue and placement controller | Partial | Declared queues register partitions, controller assigns owners and followers, placement is stable and anti-churn |
| Partition ownership gate | Partial | Broker serves only assigned owners in Ganglion mode. Standalone mode owns all queues |
| Follower pull replication | Partial | Node-authenticated follower workers pull owner records, apply durably and install checkpoints when needed. Internal read/apply/checkpoint/streaming controls require node credentials on each physical connection |
| Automatic failover | Partial | Changes involving replicated confirmation persist a pending recovery request and retain the previous assignment/source; automatic recovery and activation await the confirmed-history protocol. Owner-only policy retains its existing failover behavior |
| Cold-restart orphan reconciliation | Partial | A partition reassigned away while a node was down is retained as inert on-disk cold storage after restart (ownership-gated serving means it is never served or materialized) and surfaced at startup. Reclaim of that disk is still manual |
| Epoch fencing | Implemented | Role transitions advance log epochs before serving or applying replicated batches |
| Follower source refresh | Implemented | An owner or epoch change drains and retargets a remaining follower's worker while retaining its replication cursors |
| Checkpoint epoch checks | Implemented | Both source epochs are validated before reset and checked again by each storage writer in command order |
| Recovery seal receiver and retained identity | Partial | Node-authenticated requests are checked against the exact committed transition; local seals and checksummed content fingerprints survive restart and support identical-request retry. Linux validated; non-Unix sealing is unsupported. Automatic dispatch, compatible-history proof and activation remain pending; see [recovery sealing](/reliability/recovery-sealing/) |
| Recovery witness admission | Partial | Explicit calls use fresh authenticated connections and bounded deadlines; collection binds replies to distinct old replicas and the exact transition, preserves the old threshold and rejects contradictions. Count completion awaits history validation. Automatic collection, transfer and activation remain pending |
| Sealed-source reads | Partial | Explicit node-authenticated pages recheck the committed transition and exact durable receipt, verify retained contents from disk, and preserve seals across live/cold reads and cancellation. Page/record limits and single-read admission bound storage work in flight; every page currently rescans retained data. Compatible-history proof, selected transfer and activation remain pending |
| Retained-history inspection | Partial | Explicit two-witness comparison verifies transferred log digests, compares shared offsets and reports whole-event payload references with page/record/byte/deadline limits. Empty overlaps, compaction and state/checkpoint gaps remain explicit; common-origin proof, source selection and activation are pending |
| Checkpoint recovery | Partial | Unix installation journals resume interrupted replacement of both logs and queue state before ordinary replay; completion receipts preserve later backfill on retry. Payload dependencies gate promotion across restart. Linux fault/SIGKILL tests pass; non-Unix installation is unsupported pending durable metadata support |
| Conflict diagnostics | Implemented | Bounded, payload-free control history, offsets and effective record identities accompany overlap reports; checkpoint logs show source epochs and continuation offsets |
| Replica-durable confirms | Partial | Queues require the same follower to cover the payload batch and exact enqueue frontier; epoch/session-fenced progress feeds confirmation and delivery visibility, with timeout and ISR floor |
| Durable stream replication (Plexus) | Partial | Tier-gated: the durable tier replicates record + cursor logs to `stream_replication_factor` followers (express tiers stay owner-only), durable publishes confirm on replica durability, and owner loss triggers follower selection and local promotion checks. Reuses the queue follower-worker, confirm gate, and failover-candidate selection |
| `min_in_sync_replicas` | Implemented | Runtime setting, fail-fast publish refusal when healthy ISR is below floor |
| Live repartitioning | Partial | Grow or shrink a queue's partition count in Ganglion mode (versioned routing, in-flight transition serialization, drain-and-retire on shrink); admin control + API |
| Topology visibility | Partial | Admin API/page (with repartition + coordination-membership controls) and `fibrilctl topology`. Cross-broker lag aggregation is pending |
| Live topology push | Partial | Broker pushes a `TopologyUpdate` to each connection when that connection's routing content changes (not on every coordination metadata bump); the Rust, TypeScript, Python, Go, and C# clients apply it to their routing cache and ack the generation |
| Repartition cutover fencing | Partial | The controller fences a repartition's finalize (retiring shrunk-away partitions and clearing the marker) on cluster-wide client adoption of the new routing, derived from topology acks, bounded by `repartition_adoption_timeout_ms`. Publish version-fencing remains the correctness backstop. See [live routing and cutover](/development/live-routing-and-cutover/) |
| Cohort visibility (admin) | Partial | Per-broker exclusive-cohort membership with live per-partition coverage on the subscriptions page, queue-detail partition cards, and `/admin/api/cohorts`; cluster-wide cohort assignment is broker-local, not centrally committed |
| Multi-node cohort coordinator test | Partial | Coordination-level e2e covers cross-broker membership aggregation and rebalance. Full broker/client scenario coverage is still growing |

Conditions and limits:

- Cluster operation is experimental. Ganglion provides the embedded
  coordination and assignment state.
- Replication uses follower pull or credit-based streaming. Followers overlap
  message/event persistence and report progress after durable completion and state
  application. Reports carry the worker’s assignment epoch and belong to one
  ordered transport session.
- Failover checks assignment epochs, completed application and payload dependencies
  in queue state. Checkpoints awaiting payload backfill remain followers after
  restart, and client admission waits for explicit promotion. A locally complete
  candidate can still lack a batch confirmed on another replica: heartbeat tails
  are advisory and promotion does not yet prove the cluster-wide confirmed
  prefix. That proof and broader broker-level interruption coverage remain pending.
- The placement controller holds ownership, follower-set and durability-policy
  changes involving replicated confirmation, including replicated durable
  streams. Pending recovery records survive metadata restart and are exposed in
  controller status. This prevents automatic promotion of an unproven candidate;
  it currently leaves replicated failover unavailable pending recovery support.
- Conflict diagnostics do not enable automatic repair of divergent histories.
  Checkpoint installation checks both epochs and uses a recoverable journal;
  authoritative source selection and new-quorum activation remain separate gates.
- Replica-durable confirms are meaningful only when the assignment durability
  policy requires more than the owner.
- Cross-broker topology lag and ISR aggregation into the topology page is still
  pending.
- More failure testing is needed before treating this as production-ready HA.

## Not Implemented or Not Planned

| Item | Status | Notes |
| --- | --- | --- |
| Transactions | Out of scope | Not planned |
| Production-ready clustered HA | Planned | Experimental coordination, replication, and failover are wired, but hardening and runbooks remain |
| Multi-node queue deletion | Planned | Coordinated teardown across replicas is pending |
| Queue expiration (auto-delete idle queue) | Planned | Distinct from message TTL; needs global/coordinated idle tracking |
| Log retention by age (truncate old messages) | Planned or undecided | Not currently exposed as a user feature |
| Message purge (empty a queue) | Planned | Re-scoped: needs a replicated reset, not in-memory only |
| Python client | Implemented | Full parity, async plus a blocking facade; see Client Surface |
| Go client | Planned | Next client priority |
| C# client | Planned | Future client priority |
| Java client | Planned | Future client priority |

## Benchmark tooling

The repository includes a [shared Rust queue workload](https://github.com/Axmouth/fibril/tree/main/benchmarks/comparison) with fixed offered rates, saturation, payload identity checks and completion histograms. A Python/Docker runner provisions fresh single-node Fibril, JetStream and RabbitMQ instances, verifies final settlement and records CPU/memory samples. Existing-server modes support RPC reply pipelining, multiple connections and declarations requesting three copies; cluster placement and durability must be verified separately.

Stream adapters, portable cluster/fault provisioning and a repeated workload matrix remain planned. These tools provide reproducible measurements and do not establish power-loss safety or production capacity.

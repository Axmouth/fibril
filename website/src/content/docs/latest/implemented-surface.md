---
title: Implemented surface
description: Detailed checklist of what is wired in the current Fibril codebase.
---

This page is the reverse roadmap: a detailed inventory of behavior that is
currently wired, where it is exposed, and the conditions that decide whether an
idea is actually done.

Use it when a feature sounds implemented but you need to answer a sharper
question: which path is wired, which clients expose it, what blocks it, and
what is still missing.

## Reading This Page

Status meanings:

| Status | Meaning |
| --- | --- |
| Implemented | The main path is wired and has tests or direct operational surface. |
| Partial | A useful path exists, but important surfaces, limits, or polish remain. |
| Planned | The docs or design mention it, but users should not depend on it yet. |
| Out of scope | The behavior is intentionally not planned. |

## Queue Identity

See also: [core model](/latest/concepts/core-model/) and
[partition routing](/latest/development/partition-routing/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Topic plus optional group | Implemented | Broker, protocol, Rust client, TypeScript client, admin API, CLI |
| Default group normalization | Implemented | Empty group and `default` normalize to ungrouped on admin and CLI paths, and in Rust and TypeScript clients |
| Partitioned queue declaration | Implemented | `DeclareQueue.partition_count`, Rust client declare builder, config default, coordination catalogue |
| Producer partition routing | Implemented | Rust client topology cache, round-robin keyless routing, `partition_key` stable routing, version-fenced publish frames |
| Subscription fan-in | Implemented | Rust client opens per-partition subscriptions from topology and merges deliveries into one logical stream |
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

See also: [reliability semantics](/latest/reliability/semantics/) and
[many idle queues](/latest/concepts/many-idle-queues/).

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

See also: [recovery quarantine](/latest/reliability/recovery-quarantine/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Recovery reference verification | Implemented | Recovery checks each replayed event's referenced message offset against the message log's durable tail, and decodes every event record |
| `recovery.on_mismatch` policy | Implemented | Startup config: `quarantine` (default), `refuse`, or `ignore` |
| Per-partition quarantine | Implemented | A bad partition is parked (its ops error) while the rest of the broker stays up |
| Operator repair | Implemented | Admin quarantine banner + `/admin/api/quarantine/repair` truncate-to-valid, follower re-fetches the dropped suffix on next catch-up |
| Readiness health | Implemented | `/readyz` reflects quarantine state and the configured policy |
| Quarantine metric | Implemented | `recovery.quarantined` gauge and `quarantines_total` counter in the recovery snapshot |

Conditions and limits:

- A dangling event reference is only possible as a lost-tail suffix (events are
  written after their messages), so truncate-to-valid is a complete repair.
- A corrupt event record is the genuine mid-log failure and uses the same
  quarantine and truncate machinery.
- `refuse` is lazy today (a mismatch is caught when the partition is first used);
  an eager whole-disk variant at boot is a tracked follow-up.

## Publish

See also: [client usage](/latest/clients/) and
[retries and delays](/latest/reliability/retries-delays/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Unconfirmed publish | Implemented | TCP protocol, Rust client, TypeScript client |
| Confirmed publish | Implemented | TCP protocol, Rust client, TypeScript client |
| Pipelined confirmation handles | Implemented | Rust `publish_with_confirmation`, TypeScript `publishWithConfirmation` |
| Delayed publish | Implemented | TCP protocol, broker, Rust client, TypeScript client |
| Content type metadata | Implemented | Protocol metadata, Rust client, TypeScript client, delivery path |
| Reserved metadata headers | Implemented | Broker protocol handler rejects `fibril.*` and `stroma.*` user headers |
| Partition key routing | Implemented | Rust client `NewMessage::partition_key`, protocol publish metadata, server per-partition publish routing |
| Partitioning-version fence | Implemented | Client stamps routed version, and server redirects stale publishes before appending |
| Message TTL (drop by age) | Implemented | `Publish.ttl_ms` + per-queue `default_message_ttl_ms` on declare, owner resolves an absolute deadline, expiry worker drops via the DLQ/discard pipeline. Rust `Publisher::expiring` + `QueueConfig::default_message_ttl`, TypeScript `Publisher.expiring` + `QueueConfig.defaultMessageTtl` |

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

See also: [backpressure](/latest/concepts/backpressure/) and
[client usage](/latest/clients/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Manual ack subscriptions | Implemented | TCP protocol, Rust client, TypeScript client |
| Auto ack subscriptions | Implemented | TCP protocol, Rust client, TypeScript client |
| Bounded prefetch | Implemented | Broker delivery path and clients |
| Backpressure | Implemented | Pull-based delivery bounded by prefetch |
| Unsubscribe redistribution | Implemented | Broker tests cover prefetched unacked messages returning to active subscribers |
| Competing consumers (default) | Implemented | Many consumers per queue, fair dispatch, unordered |
| Exclusive consumer groups | Partial | `.exclusive()`/`consumer_target` (Rust) and `consumerGroup()`/`consumerTarget()` (TypeScript) + TCP protocol, per-partition gate, balanced+sticky assignment, soft `consumer_target`, assignment push, reconnect restore, one-cohort-per-queue guard |
| Cross-broker cohort coordination | Partial | Member identity + controller (aggregate→plan→publish) + owner apply all wired in cluster bootstrap, coordination-level multi-node rebalance test exists, fuller broker/client scenarios are still growing |
| Partition fan-in | Implemented | Both clients subscribe to all known partitions, merge deliveries while keeping per-partition settlement routing, and pick up partitions added by a live grow |

See [consumer groups](/latest/concepts/consumer-groups/) for the user-facing model.

Conditions and limits:

- Manual ack messages must be completed, failed, retried, or retried after a delay.
- Auto ack happens only after the delivery frame is successfully sent.
- Prefetch limits how many messages a subscription can hold at once.
- If a subscription ends with prefetched but unsettled messages, those messages are returned for redelivery.
- A message can be delivered more than once under failure, retry, or lease expiry conditions.
- Exclusive consumer groups are opt-in (Rust `.exclusive()`, TypeScript `.consumerGroup()`). Without them, consumers compete (no ordering). A queue has a single exclusive cohort. Both clients expose the assignment-events stream (Rust `assignment_events()`, TypeScript `onAssignmentChange`).
- Cross-broker cohort balance is advisory/eventually-consistent. The per-partition delivery gate is always the correctness backstop. Single-node is fully covered by tests.
- Plain subscriptions fan in over known partitions and pick up partitions added by a live grow. A topology warm step at
  connect prevents pure consumers from staying on partition `0` when topology is
  available.

## Reconnects

See also: [reconnects](/latest/reliability/reconnects/) and
[reconnection grace internals](/latest/development/reconnection-grace/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Resume identity handshake | Implemented | TCP protocol, Rust client, TypeScript client |
| Reconnect grace window | Implemented | Runtime settings and TCP handler |
| Conservative subscription reconciliation | Implemented | Broker, Rust client, TypeScript client |
| Restore-client-subscriptions policy | Implemented | Broker, Rust client, TypeScript client |
| Reconnect observability | Implemented | Admin overview, TCP metrics log, structured reconciliation logs |
| Durable broker restart reconciliation | Planned | Design notes only |
| In-flight publish replay | Out of scope | Clients do not replay old in-flight protocol requests |

Conditions and limits:

- Reconnect grace only applies while the broker process stays alive.
- Resume requires the same client resume identity before grace expires.
- The conservative policy keeps matching subscriptions, drops server-only
  subscriptions, and closes client-side streams that the broker cannot prove are
  still valid.
- Restore mode can recreate missing server-side subscriptions reported by the
  client after a successful resume.
- Current Rust and TypeScript subscription receive APIs surface
  reconciliation-closed streams as end-of-stream rather than a typed close
  reason.

## Settlement, Retry, and Leasing

See also: [core model](/latest/concepts/core-model/),
[reliability semantics](/latest/reliability/semantics/), and
[retries and delays](/latest/reliability/retries-delays/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Ack | Implemented | TCP protocol, Rust client, TypeScript client |
| Nack without requeue | Implemented | TCP protocol, Rust client, TypeScript client |
| Immediate retry | Implemented | TCP protocol, Rust client, TypeScript client |
| Delayed retry | Implemented | TCP protocol, broker, Rust client, TypeScript client |
| Lease expiry | Implemented | Runtime delivery settings and broker/storage path |

Conditions and limits:

- Ack settles a delivered message.
- Fail means nack without requeue. Depending on queue policy, the message may be discarded or dead-lettered.
- Retry means nack with requeue.
- Delayed retry requires `requeue=true` and a `not_before` deadline.
- Expired leases can return inflight messages to ready.
- Lease timing is controlled by runtime settings.

## Dead Lettering

See also: [dead lettering](/latest/reliability/dead-lettering/) and
[metadata policy](/latest/development/metadata-policy/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Per-queue DLQ policy | Implemented | Rust client, TypeScript client, `fibrilctl`, admin API |
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

See also: [admin dashboard](/latest/admin-dashboard/) and
[dead lettering](/latest/reliability/dead-lettering/).

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

See also: [many idle queues](/latest/concepts/many-idle-queues/) and
[idle queue internals](/latest/development/idle-queue-internals/).

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

See also: [configuration](/latest/configuration/),
[configuration policy](/latest/development/config-policy/), and
[configuration design](/latest/development/config-design/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| TOML startup config | Implemented | Config crate and server binary |
| Env and CLI overrides | Implemented | Config crate and server binary |
| Admin auth startup config | Implemented | TOML, env, CLI, server wiring |
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

See also: [admin dashboard](/latest/admin-dashboard/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Overview metrics | Implemented | Dashboard and API |
| Connections and subscriptions | Implemented | Dashboard and API |
| Queues page | Implemented | Dashboard and API, per-partition expand, follower-replication view, DLQ-policy column, hide-inactive toggle + search filter |
| Create queue | Implemented | `POST /admin/api/queues` + dashboard form (partition count, optional DLQ policy, optional default message TTL) |
| Delete queue (single-node) | Implemented | `POST /admin/api/queues/delete` + per-row dashboard button; refuses while messages are inflight (409) and in cluster mode (501) pending coordinated teardown |
| Settings page | Implemented | Dashboard and API, incl. replication and streaming-replication settings |
| Message inspection page | Implemented | Dashboard and API |
| DLQ replay controls | Implemented | Dashboard and API |
| Topology page | Implemented | Coordination nodes, per-partition ownership/epochs, consensus block |
| Repartition control | Partial | `/admin/api/repartition` and a topology-page form (Ganglion mode) |
| Coordination membership control | Partial | Add/remove a consensus voting member from the topology page and API |
| Cohort visibility | Partial | Per-broker exclusive-cohort membership on the subscriptions page and `/admin/api/cohorts` |
| Quarantine banner and repair | Implemented | Global banner, `/admin/api/quarantine`, repair endpoint, `/readyz` |
| Basic admin auth | Implemented | Login, logout, session cookie, auth-disabled mode |
| Fine-grained admin roles | Planned | Current model is admin access or auth disabled |

Conditions and limits:

- When admin auth is enabled, dashboard pages require login.
- When admin auth is disabled, dashboard pages are accessible directly and should be protected by network boundaries.
- Settings updates use version checks.
- The dashboard is for operational inspection, not continuous high-frequency monitoring.

## CLI Surface

See also: [source deployment](/latest/deployment/source/) and
[dead lettering](/latest/reliability/dead-lettering/).

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

See also: [client usage](/latest/clients/),
[quickstart](/latest/quickstart/), and
[reconnection grace](/latest/development/reconnection-grace/).

Server-side reconnect grace for inflight settles is implemented in the TCP
handler when `connection.reconnect_grace_ms` is configured. Clients now attempt
one automatic reconnect before a new operation when the old engine is already
closed. Clients and broker also exchange subscription metadata after a
successful resume. Active subscription streams continue when reconciliation
confirms that the subscription should be kept. An opt-in restore policy can ask
the broker to recreate subscriptions that the client still owns but the server
does not currently have.

| Item | Rust | TypeScript |
| --- | --- | --- |
| Connect and auth | Implemented | Implemented |
| Publish unconfirmed | Implemented | Implemented |
| Publish confirmed | Implemented | Implemented |
| Pipelined confirmation handle | Implemented | Implemented |
| Delayed publish | Implemented | Implemented |
| Message TTL (`expiring` publisher + queue `default_message_ttl`) | Implemented | Implemented |
| Manual ack subscription | Implemented | Implemented |
| Auto ack subscription | Implemented | Implemented |
| Exclusive consumer group | Implemented | Implemented |
| Assignment-change events | Implemented | Implemented (`onAssignmentChange`) |
| Resume identity handshake | Implemented | Implemented |
| Explicit reconnect outcome | Implemented | Implemented |
| Existing publishers after explicit reconnect | Implemented | Implemented |
| New subscriptions after explicit reconnect | Implemented | Implemented |
| Conservative automatic reconnect before new operation | Implemented | Implemented |
| Subscription reconciliation metadata exchange | Implemented | Implemented |
| Active subscription recovery after accepted resume | Implemented | Implemented |
| Opt-in subscription restore after accepted resume | Implemented | Implemented |
| Delayed retry | Implemented | Implemented |
| Queue declaration | Implemented | Implemented |
| Content type helpers | Implemented | Implemented |
| Default group normalization | Implemented | Implemented |
| Automatic reconnect and resubscribe | Planned | Planned |

Conditions and limits:

- Both clients expose msgpack, JSON, text, raw payloads, content type metadata, and custom user headers.
- Both clients treat content type separately from the header map.
- Both clients expose delayed publish and delayed retry.
- TypeScript uses `bigint` for protocol `u64` values such as offsets.
- Explicit reconnect returns whether the broker accepted the resume identity.
- Publisher handles use the latest client engine after explicit or automatic reconnect.
- New subscriptions created after reconnect use the latest client engine.
- Automatic reconnect is bounded by client policy and defaults to one attempt before a new operation.
- After a successful resume, both clients send known subscription metadata and read the broker reconciliation result.
- When the broker returns `keep`, both clients route later deliveries for that subscription into the existing stream.
- If the broker keeps a subscription under a different server `sub_id`, both clients remap the existing stream to that server id.
- The default reconciliation policy is conservative. Client-only or mismatched subscriptions are closed client-side, while server-only subscriptions are dropped by the broker.
- The opt-in restore-client-subscriptions policy recreates client-owned subscriptions that are missing server-side, then keeps the existing client stream using the broker's new subscription id.
- Operations already in flight when the socket fails are not replayed.
- Active subscriptions still need application-level handling when resume is rejected or reconciliation reports a mismatch.
- Late settlements after a short disconnect are accepted only when the client explicitly resumes before grace expires.
- Broker process restart reconciliation is not implemented. Current reconnect grace depends on the broker process keeping dormant connection state in memory.

## Benchmarks and Operational Scripts

See also: [benchmarks](/latest/benchmarks/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Throughput benchmark | Implemented | Rust bench helper and shell scripts |
| Steady-state rate benchmark | Implemented | Bench binary and scripts |
| Memory sampling during bench | Implemented | Bench scripts |
| Formal CI benchmark reporting | Planned | Local and informal docs exist, reproducible CI reporting is pending |
| One command pre-commit verification helper | Planned | Individual checks exist, unified helper is still pending |

Conditions and limits:

- Current benchmark numbers are local architecture checks.
- They are not a stable published performance contract.
- Hardware, storage, durability settings, payload size, queue depth, and batching strongly affect results.

## Deployment Surface

See also: [source deployment](/latest/deployment/source/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Website Docker deployment | Implemented | Compose file and Traefik labels |
| Broker server image | Implemented | Dockerfile and publish workflow |
| `fibrilctl` in image | Implemented | Server image includes CLI |
| Source deployment docs | Implemented | Docs site |
| Full production hardening guide | Partial | Basic guidance exists, deeper ops runbook is pending |

Conditions and limits:

- Server image exposes broker TCP and admin HTTP ports.
- Persistent broker data should be mounted under the configured data directory.
- Admin auth should be enabled outside local protected environments.

## Experimental Cluster and Replication Surface

See also: [clustering](/latest/concepts/clustering/) and
[replication](/latest/reliability/replication/).

| Item | Status | Implemented surface |
| --- | --- | --- |
| Ganglion coordination mode | Partial | Startup config, embedded coordinator, TCP transport, broker self-registration, topology endpoint |
| Queue catalogue and placement controller | Partial | Declared queues register partitions, controller assigns owners and followers, placement is stable and anti-churn |
| Partition ownership gate | Partial | Broker serves only assigned owners in Ganglion mode. Standalone mode owns all queues |
| Follower pull replication | Partial | Follower workers pull owner records over protocol, apply durably, install checkpoints when needed |
| Automatic failover | Partial | Dead owner can trigger epoch-bumped reassignment, follower promotion at local tail, stale owner demotion |
| Epoch fencing | Implemented | Role transitions advance log epochs before serving or applying replicated batches |
| Replica-durable confirms | Partial | Owner waits for durable follower progress according to assignment policy, with timeout and ISR floor |
| `min_in_sync_replicas` | Implemented | Runtime setting, fail-fast publish refusal when healthy ISR is below floor |
| Live repartitioning | Partial | Grow or shrink a queue's partition count in Ganglion mode (versioned routing, in-flight transition serialization, drain-and-retire on shrink); admin control + API |
| Topology visibility | Partial | Admin API/page (with repartition + coordination-membership controls) and `fibrilctl topology`. Cross-broker lag aggregation is pending |
| Cohort visibility (admin) | Partial | Per-broker exclusive-cohort membership on the subscriptions page and `/admin/api/cohorts`; cluster-wide cohort assignment is broker-local, not centrally committed |
| Multi-node cohort coordinator test | Partial | Coordination-level e2e covers cross-broker membership aggregation and rebalance. Full broker/client scenario coverage is still growing |

Conditions and limits:

- This surface is experimental on the replication/sharding branch.
- Ganglion mode is the active embedded-coordination path. The older etcd-shaped
  plan remains useful as a design reference, but the current implementation
  uses the same coordination trait with Ganglion underneath.
- Replication is follower-pull. Followers apply durably, then report progress
  through stamped replication reads.
- Failover safety relies on assignment epochs plus local Stroma promotion gates.
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
| Single-node queue deletion | Implemented | Admin API/dashboard; see Admin Surface |
| Multi-node queue deletion | Planned | Coordinated teardown across replicas is pending |
| Message TTL (drop by age) | Implemented | Per-message + per-queue default; see Publish |
| Queue expiration (auto-delete idle queue) | Planned | Distinct from message TTL; needs global/coordinated idle tracking |
| Log retention by age (truncate old messages) | Planned or undecided | Not currently exposed as a user feature |
| Message purge (empty a queue) | Planned | Re-scoped: needs a replicated reset, not in-memory only |
| Python client | Planned | Future client priority, including a blocking client |
| C# client | Planned | Future client priority after Python |
| Go client | Planned | Future client priority after C# |
| Java client | Planned | Future client priority after Go |

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
| Partition selection | Planned | Protocol/storage still carry partition fields internally, but user-facing APIs should not ask operators to pick partitions |

Conditions and limits:

- A queue is addressed by topic plus optional group.
- Groups are namespaces, not consumer groups. They do not coordinate competing consumer membership by themselves.
- Current user-facing paths assume partition `0`.
- Future sharding should choose partitions inside Fibril.

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
- Queue deletion and message retention by age are not implemented as user-facing features.

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

Conditions and limits:

- Confirmed publish returns the broker-assigned offset.
- Unconfirmed client calls only wait for the local client engine or command path, not for a broker-assigned offset.
- Delayed publish uses a distinct delayed-publish frame and a `not_before` deadline.
- Content type is stored outside the user header map for common cases.
- Manual `content-type` headers are interpreted as content type metadata by clients.
- Broker-side validation rejects reserved system header prefixes on normal and delayed publish.

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

Conditions and limits:

- Manual ack messages must be completed, failed, retried, or retried after a delay.
- Auto ack happens only after the delivery frame is successfully sent.
- Prefetch limits how many messages a subscription can hold at once.
- If a subscription ends with prefetched but unsettled messages, those messages are returned for redelivery.
- A message can be delivered more than once under failure, retry, or lease expiry conditions.

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
| Queues page | Implemented | Dashboard and API |
| Settings page | Implemented | Dashboard and API |
| Message inspection page | Implemented | Dashboard and API |
| DLQ replay controls | Implemented | Dashboard and API |
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
handler when `connection.reconnect_grace_ms` is configured. Client automation
around that behavior is still planned.

| Item | Rust | TypeScript |
| --- | --- | --- |
| Connect and auth | Implemented | Implemented |
| Publish unconfirmed | Implemented | Implemented |
| Publish confirmed | Implemented | Implemented |
| Pipelined confirmation handle | Implemented | Implemented |
| Delayed publish | Implemented | Implemented |
| Manual ack subscription | Implemented | Implemented |
| Auto ack subscription | Implemented | Implemented |
| Resume identity handshake | Implemented | Implemented |
| Explicit reconnect outcome | Implemented | Implemented |
| Existing publishers after explicit reconnect | Implemented | Implemented |
| New subscriptions after explicit reconnect | Implemented | Implemented |
| Automatic client recovery after reconnect | Planned | Planned |
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
- Publisher handles created before explicit reconnect use the latest client engine afterward.
- New subscriptions created after explicit reconnect use the latest client engine.
- Active subscription streams created before reconnect are not transparently restored yet.
- Late settlements after a short disconnect are accepted only when the client explicitly resumes before grace expires.

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

## Not Implemented or Not Planned

| Item | Status | Notes |
| --- | --- | --- |
| Transactions | Out of scope | Not planned |
| Replication | Planned | Design and implementation pending |
| Cluster partition ownership | Planned | Design and implementation pending |
| Queue deletion | Planned or undecided | Not currently exposed as a user feature |
| Message retention by age | Planned or undecided | Not currently exposed as a user feature |
| Python client | Planned | Future client priority, including a blocking client |
| C# client | Planned | Future client priority after Python |
| Go client | Planned | Future client priority after C# |
| Java client | Planned | Future client priority after Go |

---
title: Private nodes and internal routing
description: Design for brokers that are never advertised to clients, with public brokers forwarding to them over internal connections.
---

This design note records the design of private brokers. Phase 1 (section 9) is
implemented, section 11 lists where the build differs from the text, and phases
2 and 3 are proposed work. User-facing behavior is in
[private brokers](/configuration/#private-brokers). It expands the roadmap
entry
[public/private endpoints and routing fallback](/roadmap/#publicprivate-endpoints-and-routing-fallback).
Current routing is described in [partition routing](/development/partition-routing/)
and [live routing and cutover](/development/live-routing-and-cutover/), sessions
in [reconnection grace](/development/reconnection-grace/), and node trust in
[TLS and authentication internals](/development/security-internals/).

## Problem

Fibril clients route each partition to its owner. Every broker advertises the
addresses clients should dial, and topology answers and redirects carry the
owner's addresses. That is the fastest topology and stays the default.

Some deployments cannot expose every broker. A load balancer or a Docker Swarm
service publishes one address in front of one or a few brokers, and the rest of
the cluster sits on an internal network. Today a client in that setup is
redirected to an owner it cannot reach, and its operations fail or retry until
their deadline.

The goal is a predictable mode for that layout:

- An operator marks some brokers private in their config.
- Private brokers never appear in topology answers or redirects.
- Clients talk only to public brokers.
- When a partition is owned by a private broker, a public broker serves it by
  forwarding to the owner over an internal, node-authenticated connection.
- A forwarded operation means exactly what the same operation means when sent
  to the owner directly. Forwarding adds latency, never weaker guarantees.

Out of scope: overlapping topology groups, multi-hop routing, and per-topic
authorization (planned separately, see
[implemented surface](/implemented-surface/)).

## Current foundation

| Piece | Where | What it gives this design |
| --- | --- | --- |
| Client advertise list | `ListenerSection::advertise`, `FIBRIL_BROKER_ADVERTISE`, `ServerConfig::broker_advertise_addresses` in `crates/config/src/lib.rs` | Explicit entries first, then the address derived from the node's coordination peer host by `derive_advertise_from_peers` |
| Node record | `spawn_ganglion_broker_tasks` in `crates/fibril/src/lib.rs` registers `NodeInfo { broker_addr, admin_addr }` (`crates/broker/src/coordination.rs`) | `broker_addr` is the first advertise entry. Replication peers dial it (`CoordinationProtocolOwnerPeerResolver` in `crates/protocol/src/v1/replication.rs`) |
| Heartbeat labels | `broker_heartbeat_labels` in `crates/fibril/src/lib.rs`, constants in `crates/coordination-ganglion/src/lib.rs` | `fibril/advertise` carries the full client list. `fibril/draining` shows a per-node flag carried by every heartbeat |
| Process publish | `register_self_with_labels` | One attribute write per process forces a published snapshot, so labels from the first registration become visible promptly |
| Client topology | `GanglionCoordination::client_topology`, `client_queue_owner_endpoint` in `crates/coordination-ganglion/src/targeted_route.rs` | Topology lists every owner endpoint, redirects carry the first one |
| Handler routing | `ClientTopologySource`, `queue_owner_route`, `stream_owner_route`, `fence_stale_partitioning`, `send_owner_redirect_or_error` in `crates/protocol/src/v1/handler.rs` | One place decides redirect, retryable `503`, or plain error |
| Topology push | `handle_connection` polls each second and sends `TopologyUpdate` when content changes. `TopologyAdoptionTracker` counts only connections that acked | Clients follow ownership changes without polling |
| Address tags | `AdvertisedAddress::tags` in `crates/wire/src/lib.rs` | Reserved for selection conventions such as `internal` or `public`. Clients use the first entry only (`first_owner_target` in `crates/client/src/lib.rs`) |
| Node principal | `NODE_PRINCIPAL = "@node"` and `StoreAuthHandler` in `crates/broker/src/auth_store.rs`, `Op::node_control` in `crates/wire/src/lib.rs` | Brokers already authenticate to each other with the cluster secret, and node-only ops are refused to anyone else |
| Peer TLS | `tls.inter_broker`, `tls.peer_ca_path`, `peer_connector_from_config` | Inter-broker dials already encrypt and verify |

Every SDK routes the same way (`clients/ARCHITECTURE.md`): one engine per
`host:port`, a topology cache filled by fetches, pushes and redirects, and a
bootstrap fallback on a cache miss. The Rust client keys its pool by endpoint
string (`ClientShared::engine_slot`) and follows redirects in
`follow_redirect_or_wait`. A design that only changes what endpoints the broker
hands out works with every SDK unchanged.

Two existing facts shape the forwarding model:

- Settles are scoped to the session that holds the subscription. The `Ack` and
  `Nack` handlers look the subscription up by `(topic, partition, group)` in the
  logical session and ignore unknown ones. `Ack` carries no subscription id.
- Exclusive cohort membership is per connection. `install_subscription` resolves
  one member id per connection and mints one when the client sends none.

## 1. Configuration

**Recommendation.** Add `broker.visibility` with values `public` (default) and
`private`, overridden by `FIBRIL_BROKER_VISIBILITY`. It sits in
`BrokerSection` next to `broker.listener`, because it describes the client
broker listener and has no meaning for the admin listener that shares
`ListenerSection`. It is a bootstrap setting under the
[configuration policy](/development/config-policy/): read at startup, changed
by restart.

Separate the peer address from the client list at the same time. Today the node
table's `broker_addr` is the first client advertise entry, and replication
dials it. Behind a shared load balancer address that breaks: a peer would dial
the shared address and land on any broker. Add `broker.peer_advertise` with
`FIBRIL_BROKER_PEER_ADVERTISE`, a single `host:port` that defaults to the
first client advertise entry (explicit entries first, then the address
`derive_advertise_from_peers` computes), then to the bind address. That is the
address peers dialed before, so existing clusters keep their peer traffic. The node table carries the peer
address. The `fibril/advertise` label carries the client list.

| Node | `broker_addr` (peers) | `fibril/advertise` (clients) |
| --- | --- | --- |
| Public, own address | peer address | its advertise list, as today |
| Public, behind a shared address | peer address | the shared address |
| Private | peer address | empty |

**Reaching the cluster.** A private node adds a `fibril/visibility = private`
label in `broker_heartbeat_labels`, beside `fibril/draining`, and omits
`fibril/advertise`. The label rides the first `RegisterNode`, and the
once-per-process attribute write in `register_self_with_labels` publishes it,
so a private node is never visible without it. Absence means public, which keeps
existing clusters unchanged.

Alternatives considered:

- A boolean `broker.private` / `FIBRIL_BROKER_PRIVATE`. Simpler, but an enum
  leaves room for a later scope such as a named topology group without a second
  flag.
- A field on the Ganglion `NodeInfo`. Stronger typing, but it is a change in the
  separate Ganglion repository and a coordination format change for one flag.
  Labels already carry comparable per-node facts.
- A raft attribute per node. It survives the node, which is useful, but it also
  outlives a reconfigured node and needs cleanup. The label expires with the
  node record like `fibril/draining`.

**Validation.**

- A private node in standalone mode fails startup. Nothing can forward to it.
- A private node with explicit `broker.listener.advertise` entries logs a
  warning that they are not given to clients.
- A cluster with no live public node cannot be rejected at boot, since nodes
  join one at a time. Every node checks its committed view, logs a rate-limited
  warning while it sees no live public node, and the admin Cluster page shows
  it. Topology answers then carry empty owner endpoints for private-owned
  partitions, which clients already treat as an unknown owner.
- Rolling upgrade: an older broker ignores the label and would advertise a
  private node. The upgrade guide says to mark nodes private only after every
  node runs a version that understands the label.

## 2. Advertisement

**Gateway set.** For every partition, the topology source computes client
endpoints from the owner record:

- Owner advertises a client list: use it, as today.
- Owner advertises none (private): use the client list of one live public
  node, chosen by rendezvous hashing of `(topic, group, partition)` over live,
  non-draining public nodes.

The same rule feeds `client_topology`, `client_queue_owner_endpoint` (and its
stream counterpart), and therefore `Op::Topology`, `TopologyUpdate`, redirects
and the stale-version fence. No code path builds a client-facing owner address
any other way. Liveness uses the controller's existing
`GanglionCoordination::live_nodes` check.

**Serve where you land.** The gateway choice is advisory. Any public broker
that receives an operation for a partition it cannot redirect to a reachable
owner forwards it. The decision rule in the handler is:

1. This broker owns the partition: serve it, or `503` while recovering, as today.
2. The owner's client list is empty, or every entry is also in this broker's
   own client list: forward.
3. Otherwise: redirect to the owner's client list, as today.

Rule 2 covers private owners and also public brokers that share one load
balancer address. A broker never redirects a client to an address it
advertises itself, which removes the redirect ping-pong a shared address would
otherwise cause (the client would follow the redirect back to the same pool
slot until `max_redirects`). Private nodes never forward. A client that reaches
a private node directly gets served for partitions it owns and redirected for
the rest.

Alternatives considered for which public broker to name:

| Choice | For | Against |
| --- | --- | --- |
| The answering broker | No extra client connection | Topology differs per broker. A client connected to two public brokers sees its cache flip between them on each push, and the pool churns |
| All public brokers | Client can pick | Clients use only the first entry. Multiple entries would be read as fallbacks for one owner |
| Rendezvous hash over live public nodes (recommended) | Identical topology on every broker. Spreads forwarding load. Minimal movement when a public node joins or leaves | A client may open one connection per public broker it is sent to |
| A configured gateway address | Explicit | Another setting that must match the load balancer, and it hides which broker carries the load |

When all public brokers share one address the hash picks among identical lists,
so the result is that address.

**Partitioning version fence.** The public broker runs
`fence_stale_partitioning` against its own view before forwarding. The
forwarded `Publish` keeps the client's `partitioning_version`, so the owner
fences again with its view. A redirect produced by either fence is rebuilt by
the public broker from the gateway set and carries the higher of the two
versions. It never relays an upstream `Redirect` frame unchanged.

**Topology push and repartition.** Public brokers push gateway-rewritten
topology to their clients exactly as today, and their `TopologyAdoptionTracker`
reports their clients' acks in `fibril/topology_adoption`. Private nodes have no
ordinary clients and report no adoption signal, which the tracker already
treats as neutral. The owner does not push topology on forwarded sessions and
those sessions never ack, so they cannot hold back a cutover. A gateway set
change (a public node joins or dies) changes topology content and is pushed
like an owner change.

## 3. Forwarding model

Three shapes were considered.

**(a) Per-operation forwarding over a shared node link.** The public broker
keeps one connection per owner and proxies each publish, subscribe and settle
for all clients over it. The owner sees one session, so delivery tags, cohort
membership and prefetch all belong to that shared session. The public broker
would have to rewrite delivery tags, keep its own lease and subscription table,
split prefetch across clients, and route settles itself. That is a second
implementation of queue session semantics, and one slow client blocks every
other client's deliveries on the shared TCP stream.

**(b) A per-client upstream session (recommended).** For each client session
and each owner it needs, the public broker opens one client-protocol session to
that owner, authenticated as `@node`, and relays the partition's frames on it.
The owner runs its normal session code. Leases, delivery tags, prefetch, settle
validation and cohort membership stay owner-side and unchanged.

**(c) Client-negotiated tunnel.** The topology entry names a gateway plus a
target node, the client opens a separate pooled connection with a tunnel
request in HELLO, and the public broker splices bytes to the owner after that.
The owner sees an ordinary client session authenticated with the client's own
credentials, and a session could even survive a gateway failure by resuming
through another gateway. It needs a protocol change in all five SDKs, a new
pool key, a rule for TLS termination at the gateway, and care that the owner
never treats a tunneled connection as loopback (the built-in `fibril`/`fibril`
pair is loopback-only). It is the strongest option for session continuity and a
reasonable later client opt-in. It is not needed for the first deployments, and
(b) works with existing clients.

A fourth shape, a multiplexed link carrying many virtual sessions per
`(public, owner)` pair, is (b) with fewer sockets. `Op::HistoryReplication`
already wraps an inner frame and is a precedent for that envelope. It needs
per-session flow control to avoid the blocking problem in (a). Keep it as a
later optimization if connection counts become a limit.

### How (b) handles each concern

The public broker's logical session gains an upstream table: one entry per
owner node with the upstream connection, its resume identity, a request id map
and a subscription id map. Upstream sessions are opened lazily on the first
operation for that owner and closed when they hold no subscription and no
pending request for an idle interval.

- **Publish, delayed publish, confirms.** The public broker sends the same op
  with the same body on the upstream session and maps the request id. It returns
  `PublishOk` only when the owner's `PublishOk` arrives, with the owner's offset.
  The owner applied its durability policy before answering, so a forwarded
  confirm means exactly what a direct confirm means. The public broker never
  synthesizes a confirm and never counts the publish in its own
  `fibril_broker_published_total` or `fibril/rate-pub`.
- **Fire-and-forget publishes** are relayed with no reply, as direct ones are
  sent with none. If no upstream is available they are dropped and counted.
  Clients already do not auto-follow redirects for them.
- **Batches.** Pipelined publishes and coalesced writes are individual frames on
  the wire, each relayed in order with its own reply. No batch op is needed.
- **Queue subscribe.** Relayed with the client's prefetch unchanged. The owner
  returns its own `sub_id`. The public broker allocates the client-facing
  `sub_id` and maps `Deliver`, `SubscriptionClosed` and `AssignmentChanged`
  frames back to it.
- **Delivery tags** pass through unchanged. No translation is needed, because a
  settle carries `(topic, group, partition)` and the public broker sends it to
  the upstream session that holds that subscription. The owner validates the tag
  in that session as it does for a direct client.
- **Settle (ack, fail, retry, delay).** `Ack` and `Nack` (with `requeue` and
  `not_before`) are fire-and-forget and relayed as is. A settle for a partition
  with no forwarded subscription is ignored, matching the owner's handling of an
  unknown subscription.
- **Prefetch and backpressure.** The owner bounds unsettled deliveries per
  subscription with prefetch, so the public broker holds at most prefetch
  deliveries per forwarded subscription. The upstream reader waits on the
  client's frame sink, so a slow client stops reading its own upstream socket
  and the owner's normal send path backs off. Other clients are unaffected
  because their upstream sessions are separate.
- **Streams.** `SubscribeStream` and stream `Publish` relay the same way. A
  stream `Ack` is the cursor commit (the tag is the record offset), and is
  relayed to the session holding the stream subscription. Existing stream limits
  in [reconnection grace](/development/reconnection-grace/#stream-limits) apply
  unchanged.
- **Consumer groups.** The public broker resolves one member id per client
  session with the same rules as `install_subscription` (client carried, else
  established, else minted at the public broker) and always sends it explicitly
  upstream. Owners never mint for forwarded sessions, so every owner and any
  local subscription see one member. `AssignmentChanged` is relayed with the
  client-facing fields.
- **Declare and topology** are answered locally. `DeclareQueue` and
  `DeclarePlexus` already go through the replicated `DeclareCoordinator` on any
  broker.
- **Ordering.** All frames for one owner travel on one upstream session in
  arrival order, so per-partition order is the same as on a direct connection.
- **Ownership moves.** If the owner answers a request with a redirect or
  `NotOwner`, it did not apply it. The public broker re-resolves with its own
  view: a new private owner gets one more attempt on that owner's upstream
  session, a new public owner with its own address gets a client redirect, and
  an unchanged view gets `503`. A subscription the owner closes with
  `OwnerMoved` is relayed as `SubscriptionClosed`, and the client supervisor
  resubscribes through the public broker. Held deliveries from the old owner
  settle into a session that no longer has the subscription and are ignored,
  as they are for a direct client.
- **Upstream link drop.** The public broker reconnects with the upstream resume
  identity. Within the owner's `reconnect_grace_ms` the owner resumes the
  session, keeps its subscriptions and inflight tags, and held deliveries stay
  settleable. Requests that were in flight get the unknown-outcome error from
  section 6.

## 4. Identity and authorization

**Recommendation.** The client authenticates to the public broker as it does
today, by password or by a certificate identity mapped through
`decide_certificate`. The public broker opens upstream sessions as
`NODE_PRINCIPAL` with the cluster secret and peer TLS, reusing
`open_protocol_owner_conn`. Right after AUTH it sends a new node-control op,
`ForwardedSession`, carrying:

- the origin node id and the hop count (always 1),
- the acting username,
- the client's id, connection id and remote address, for logs and the
  connections view.

`ForwardedSession` is added to `Op::node_control`, so the existing check in
`handle_connection` refuses it with `403` unless the connection authenticated as
`@node`. The owner answers with an ok frame naming its forwarding protocol
version. An owner that does not know the op answers with an error and the public
broker treats it as not forwarding-capable (section 6). This fails closed.
Adding the marker as an optional HELLO field was rejected, because an older
owner would ignore it silently.

**What the owner trusts.** The node principal is already trusted to replicate,
seal and replace storage, which is strictly more power than acting for a user.
The owner records the acting user on the session and uses it wherever it would
use the authenticated principal: logs, the connections view, and per-topic
authorization once that lands. The client's password never travels upstream, and
the owner never runs a user credential check for a forwarded session.

**ACLs.** There are no per-topic permissions today. When they land, both ends
check: the public broker before forwarding (fast rejection, no upstream work)
and the owner on the asserted identity (the authoritative check). A `403` from
the owner is relayed unchanged.

**Alternative rejected.** Re-authenticating upstream with the client's password
would keep plaintext passwords in gateway memory, would not work for
certificate identities, and adds an argon2 verification per upstream session.

**TLS.** Upstream sessions use the same connector as replication:
`tls.inter_broker` (defaulting to `tls.enabled`) and `tls.peer_ca_path`. A
deployment with a mesh or tunnel can disable inter-broker TLS as it can today.

## 5. Loop prevention and bounds

Forwarding is one hop by construction:

- Only public nodes forward. Private nodes never do.
- A session opened with `ForwardedSession` never forwards. The owner either
  serves the partition or answers with its normal redirect or error, which goes
  back to the public broker.
- The public broker forwards only to the owner named in its committed view, and
  makes at most two upstream attempts per request (the second only after the
  owner reported a move and the view changed).
- An upstream `Redirect` is never relayed raw. The public broker rebuilds any
  client redirect from the gateway set.
- A broker never redirects a client to an address in its own client list.

Bounds on the public broker, as runtime settings in a new `routing` section so
operators can tune them without restart:

| Setting | Default | On limit |
| --- | --- | --- |
| `routing.max_upstream_sessions` | 4096 per broker | New forwarded operations get `503` |
| `routing.max_upstream_sessions_per_owner` | 1024 | `503` for that owner's partitions |
| `routing.max_inflight_forward_bytes` | 256 MiB | Publishes wait up to the forward timeout, then `503` |
| `routing.forward_timeout_ms` | 30000 | Unknown-outcome error for that request |
| `routing.upstream_connect_timeout_ms` | the replication `owner_connect_timeout_ms` | `503`, nothing was sent |
| `routing.upstream_idle_close_ms` | 30000 | Idle upstream sessions close |

Inflight forward bytes count publish payloads sent upstream and not yet
answered. Delivery memory is bounded by prefetch times `max_message_bytes` per
forwarded subscription, already enforced by the owner. Per-upstream write
queues are bounded channels like the connection writer's. Nothing buffers
without a limit. The defaults are placeholders to be set from a load test.

## 6. Failure and retry outcomes

Error codes keep their HTTP meanings (`crates/wire/src/lib.rs`): `409` is
control flow toward the right broker, `503` means retry the same broker shortly,
and the `4xx` codes are terminal. One new code is proposed: `504`, "the owner
did not answer, outcome unknown". Like a lost connection it is retryable advice
for the caller, and like a lost connection the client libraries must not replay
it automatically, because a publish may have been applied. The Rust client
already auto-retries only `ERR_UNAVAILABLE`.

| Situation | Client sees | Why |
| --- | --- | --- |
| Owner unreachable before anything is sent | `503` | Safe to retry, the client waits within its deadline |
| Owner not forwarding-capable (older version) | `503` and a warning log | Temporary during a rolling upgrade |
| Forwarding limit reached | `503` naming the limit | Backpressure |
| Owner recovering | `503` relayed | Same as a direct client |
| Owner moved, new owner private | Retried once upstream, then the owner's answer | The refusal proved nothing was applied |
| Owner moved, new owner public with its own address | `409` redirect to it | Direct routing is faster |
| View not caught up after a move | `503` | The next attempt sees the new owner |
| Upstream lost or timed out after a request was sent | `504` | Outcome unknown, no replay |
| `400`, `404`, `413` from the owner | Relayed unchanged | Terminal for direct clients too |
| Subscription owner moves or dies | `SubscriptionClosed` with `OwnerMoved` | Supervisors resubscribe |
| No live public node | Topology with empty endpoints | Clients treat the owner as unknown and retry |

**Public broker dies.** The client's connection drops. In-flight requests fail
as transport errors with no replay, exactly as on a direct broker failure. The
client reconnects through the load balancer to another public broker. Its
resume identity was issued by the dead broker's owner id, so the outcome is
`resume_not_found`, reconciliation closes subscriptions client-side, and
supervisors resubscribe. The old upstream sessions on owners go dormant for the
owner's `reconnect_grace_ms` and then expire, and their unsettled deliveries
redeliver. This matches the documented limit that session transfer between
brokers needs further design. Option (c) is the path to closing it.

**Public broker drains.** It sends `GoingAway` to its clients as today. A
`GoingAway` received on an upstream session is not relayed. The public broker
lets the owner's drain move partitions and relays the resulting
`SubscriptionClosed` frames.

**Clean close.** When a client session ends for good (grace expired or explicit
close), the public broker should end its upstream sessions without leaving them
dormant on the owner, otherwise redelivery waits for two grace windows. Phase 1
sends an empty `ReconcileClient` on each upstream session before closing it,
so the owner closes every forwarded subscription and redelivers its unsettled
messages at once. The owner's session object itself still waits out its grace
holding nothing. A real session-close signal stays an open question.

## 7. Mixed setups

**Internal clients.** Some clients run inside the network and can reach private
nodes. By default they get the same gateway routes as everyone else, which is
correct but slower. A later option appends the private owner's addresses to the
topology entry, after the gateway entries, tagged `private` through
`AdvertisedAddress::tags`. Clients that only read the first entry keep using the
gateway. A client option such as `prefer_private_endpoints` selects tagged
entries when present. Whether the cluster exposes private hostnames at all is an
operator choice, off by default, since private means never advertised.

**Direct bootstrap to a private node.** Allowed. The private node serves what it
owns and redirects the rest to gateway addresses, so a client using it as a
bootstrap still converges on public routes.

**Admin and dashboard.** Visibility applies to the broker listener only. Private
nodes keep registering `admin_addr` so the dashboard lists them. The Cluster
page marks private nodes and their admin links as internal addresses, since an
operator's browser may not reach them. Cluster-wide views already come from the
committed snapshot and work from any node's admin API.

**Metrics.** Every node keeps its own `/metrics` on the admin listener. Scrapers
usually run inside the network, so private nodes are scraped as today.

## 8. Observability

Logs (with client id, connection id, owner node and acting user):

- upstream session opened, resumed and closed, with the reason,
- forwarded request failures with the code returned to the client,
- rate-limited warnings for limit rejections, non-capable owners and the no
  public node condition,
- on the owner, forwarded sessions opened and closed, with the origin node.

Metrics on the public broker, in `crates/admin/src/metrics_route.rs` beside the
`fibril_tcp_*` family:

- `fibril_forward_upstream_sessions` gauge, and per owner when
  `admin.metrics_per_channel` is on,
- `fibril_forward_requests_total` by op and outcome (ok, redirected, `503`,
  `504`, relayed error),
- `fibril_forward_bytes_in_total` and `fibril_forward_bytes_out_total`,
- `fibril_forward_inflight_bytes` gauge,
- `fibril_forward_rejected_total` by limit,
- forwarded confirm latency through the existing `OpStats` buckets.

On the owner, `fibril_forwarded_sessions` counts sessions served for gateways.
Heartbeat labels add visibility and the upstream session count so the dashboard
can show which public broker carries forwarded load. The connections view on an
owner shows forwarded sessions as "via node P for user U".

## 9. Phased plan

**Phase 1: the Swarm slice (implemented, see section 11).** Enough for a load
balancer in front of one or a few public brokers, with every SDK unchanged.

- `broker.visibility`, `broker.peer_advertise`, their env vars, validation and
  the `fibril/visibility` label.
- Gateway rewriting in `client_topology`, `client_queue_owner_endpoint` and the
  stream route, plus the handler decision rule including shared addresses.
- `ForwardedSession` with node principal, hop marking and owner-side refusal to
  forward.
- Upstream sessions for confirmed and fire-and-forget `Publish`,
  `PublishDelayed`, non-exclusive queue `Subscribe`, `Ack` and `Nack`.
- The `routing` limits, the `504` code and the failure table above.
- Metrics and logs from section 8.

**Phase 2: full client surface.**

- Streams: `SubscribeStream`, stream publish and cursor commits.
- Exclusive consumer groups with member id resolution at the gateway.
- Upstream resume across link drops and the clean session-close signal.
- Reconcile with the restore policy for forwarded subscriptions.
- Dashboard marking of private nodes and forwarded load.

**Phase 3: later options.**

- Tagged private endpoints for internal clients.
- The multiplexed upstream link if socket counts become a limit.
- The client-negotiated tunnel, if gateway failure continuity matters.
- Per-topic authorization at both ends once authorization lands.

### Test plan

- **Unit.** Config and env parsing, standalone rejection, peer address
  defaults. Gateway selection: rendezvous stability, draining and dead public
  nodes excluded, shared addresses. The rule that a broker never redirects to
  its own address. Request and subscription id mapping. Limit accounting
  releasing on success, error and redirect. Error mapping.
- **Handler tests** in `crates/protocol/tests/handler_tests.rs` with a fake
  owner: confirm only after the upstream confirm, settle routed by partition,
  upstream redirect rebuilt, `504` on a dropped upstream with a request in
  flight, `403` for `ForwardedSession` without `@node`.
- **Client tests** in the style of `crates/client/tests/redirect.rs`: a client
  behind a public mock never dials a private address, and `504` is not
  auto-retried. Equivalent checks in each SDK's conformance tests, and new
  `clients/wire_vectors.json` entries for the op and code.
- **Cluster integration** in `crates/fibril/tests`: three nodes, one public, two
  private. Private nodes advertise only unroutable client addresses (TEST-NET)
  while their peer addresses work, so any client dial to them fails the test.
  Cover confirmed publish and consume through a private owner, owner failover to
  the other private node and to the public node mid-stream, public broker restart
  with client reconnect and redelivery, limits, and a repartition while
  forwarding.
- **Recorded scenario.** A `private` scenario for
  `website/scripts/record-crash-demo.mjs`, with a compose example publishing
  only the public broker's port: kill the private owner, then kill the public
  broker, and show the client output and timings.
- **Simulation** in `crates/protocol/tests/simulation_tests.rs` following
  [deterministic simulation](/development/deterministic-simulation/): partitions
  between public and private nodes, link drops with confirmed publishes in
  flight (never a success without the owner's confirm), and settles after an
  upstream resume. Long runs go in the weekly cluster workflow
  (`.github/workflows/rust-scheduled.yaml`).

## 10. Decisions and open questions

Decided for phase 1:

1. Gateway choice: rendezvous hashing over live, non-draining public nodes,
   so every broker gives the same answer.
2. Unknown outcome: a new `504` code. The upstream was lost after the request
   was sent, the outcome is unknown, and clients must not replay blindly.
3. Config shape: the `broker.visibility` enum, with `broker.peer_advertise`
   shipping in phase 1, since a load balancer advertise address breaks
   broker to broker replication without it.
4. Private nodes accept ordinary client AUTH. Private means not advertised,
   so internal clients that can reach a private node directly keep working.

Still open:

1. Should the cluster ever expose private addresses, tagged, to internal
   clients, or must they always go through a gateway?
2. Clean session close: a new node-control op, or a `ForwardedSession` option
   that makes the owner skip grace on a clean socket close from the gateway.
   Phase 1 already releases subscriptions with an empty reconcile.
3. Is per-client upstream connection count acceptable for the expected client
   counts, or should the multiplexed link come before 1.0?
4. Should placement prefer public nodes as owners for hot partitions, so less
   traffic is forwarded, or stay visibility-blind?
5. Is losing session continuity on a public broker failure acceptable long
   term, or is the client tunnel worth its cost in all five SDKs?
6. Should a deployment be able to force gateway mode for every partition, even
   for public owners with their own addresses, for operators who want all
   client traffic to enter through one place?

## 11. Phase 1 as built

Phase 1 follows sections 1 to 8 with these differences, found while building
it. Code: `crates/protocol/src/v1/forwarding.rs` (routing rule, forwarder,
upstream sessions), `crates/coordination-ganglion/src/gateway.rs` (gateway
choice), the handler in `crates/protocol/src/v1/handler.rs`.

**Configuration.**

- The peer address defaults to the first client advertise entry, then the
  bind address, as described in section 1, rather than to the derived address
  first. Peers keep dialing what they dialed before the setting existed.
- The `routing.*` limits are runtime settings with built-in defaults and no
  `runtime_seed.routing` boot seed, the same as the `stream` settings.
- The no-live-public-node condition is a rate-limited log on every broker. The
  admin Cluster page does not show it yet.

**Advertisement.**

- The rendezvous score is a fixed FNV-1a hash with a splitmix64 finish over
  `(topic, group, partition, node id)`, so brokers on different Rust versions
  agree. Liveness compares heartbeat labels with each broker's clock, so near
  the TTL two brokers can briefly name different gateways. Serve where you land
  makes that harmless.
- The stale partitioning fence still redirects to the client-facing endpoints
  even when they are this broker's own: the redirect carries the newer version,
  which is the point of the fence, and owners already did this for themselves.
  Every other redirect follows the never-self rule.
- A private broker, or any broker on a forwarded session, answers an operation
  for a partition it does not own with a redirect to the gateway endpoints, or
  the usual not-owner error when those are its own.

**Forwarding.**

- Client-facing subscription ids of forwarded subscriptions come from a range
  starting at 2^48, so they never collide with the broker's own ids on one
  connection and stay exact in every SDK.
- `Deliver` frames are relayed by rewriting the subscription id in place. The
  rest of the body, delivery tag included, is byte for byte the owner's.
- Deliveries are relayed with no buffer: the upstream reader waits on the
  client's connection, which is the backpressure section 3 describes.
- Streams and exclusive consumer groups through a gateway answer `400` with an
  explanation. A retry cannot help until phase 2, so `503` would only loop.
- A publish waits for in-flight byte room for the forward timeout capped at the
  client heartbeat interval (5 s), not the full forward timeout. The wait holds
  the client's connection loop, and a longer wait would trip the heartbeat
  timeout on both sides.
- A request past the forward timeout closes its whole upstream session. Every
  pending publish gets `504`, pending subscribes get `503` and the session's
  subscriptions close. An owner that sits on one request that long is not
  serving the session, and a timed-out subscribe could otherwise leave a
  subscription on the owner that a retried subscribe would collide with.
- When an upstream session is lost, pending subscribes answer `503`, not
  `504`: the owner drops the subscription with the session, so a retry is safe.
  The session's subscriptions close with `OwnerMoved`, which every SDK's
  supervisor answers by resubscribing through the public broker.
- Upstream resume is phase 2, so a lost upstream session is not resumed. The
  owner keeps the dormant session for its reconnect grace and then redelivers.
- A public broker that held a local publisher for a partition that moved to a
  private owner drops cached publishers it no longer owns on each topology
  tick, and answers a publish that hit the old publisher with `503` instead of
  `409`, so the client retries and the retry is forwarded.
- A reconcile counts forwarded subscriptions as server-side subscriptions. A
  missing manual-ack subscription on a forwarded partition gets
  `RecreateClientSide` under either policy, so the client resubscribes through
  the normal forwarded path. Server-only forwarded subscriptions are closed on
  the owner with a reconcile that lists the ones to keep.
- After an owner refused to connect, operations for it fail fast with `503` for
  one second instead of each waiting out a connect timeout.

**Observability.**

- Forwarded confirm latency is exported as `fibril_forward_confirms_total` and
  `fibril_forward_confirm_latency_microseconds_total`, a count and sum pair,
  rather than buckets.
- Heartbeat labels carry visibility but not the upstream session count yet.
- The connections API marks a forwarded session with its origin node and
  acting user. The dashboard does not render it yet.

**Tests.**

- `crates/protocol/tests/forwarding_tests.rs` drives a gateway handler against
  real and scripted owners: confirm only with the owner's confirm and offset,
  `504` on a dropped upstream, `503` when nothing was sent, rebuilt redirects,
  the one retry on a move to another private owner, the per-owner cap, cohort
  refusal, and the `ForwardedSession` checks (node principal only, one hop,
  once per session, never forwarding again).
- `crates/fibril/tests/private_nodes.rs` runs three broker processes, two of
  them private with only an unroutable client address, and kills the private
  owner mid-test. The partition moves to the other private broker and the
  client keeps publishing with confirms and consuming through the public one.
- Client classification tests for `504` exist in all five SDKs, and the Rust
  client has a mock broker test that a `504` publish is never replayed.
- No `clients/wire_vectors.json` entry for `ForwardedSession`: SDK conformance
  tests require every vector to be encodable by the SDK, and the op is node
  only. The Rust wire tests cover its codec.
- No recorded `private` crash demo scenario and no simulation tests yet.

# Follow-ups and pending work

## Recommended release roadmap (as of the 0.4 cut, 2026-07-04)

The sequence for the pending arcs. Two hard ordering constraints drive
it: gate-3's wire additions must land BEFORE #110 freezes the wire, and
anything touching protocol or client API (#82 per-stream RF, #62
TopologyUpdate) must beat the #111 freeze or wait for 2.0.

Immediately (design-free, decays with every commit on main):
- Generate the v0.4.0 golden fixture from the tag (back-compat brief
  below has the mechanics). Writing gen-compat-fixture.sh here is the
  reusable part - every future cut runs it.

0.5 - the clarity release: guided errors + gate-3 reconciliation
(#102-#105). One release because they are one product story (the broker
says what happened and what to do) and they co-design at the seam:
reconnect-closure reasons belong to #102, not the errors pass. Inside
the arc: broker-side guides first (the broker_error_response funnel is
brick 1, includes the InvalidArgument 500 -> 400 code fix), client-local
guides second, then the gate-3 family per its brief. The 0.5 cut is the
first to prove the back-compat guarantee by opening the v0.4.0 fixture
in CI, and generates its own fixture on the way out.

0.6 - the freeze release: gate-2 family (#109-#112), strict internal
order #109 -> #110 -> #111 -> #112. BEFORE #109 starts, sweep in the
wire-touching stragglers so they do not miss the freeze: #82 per-stream
RF override at declare, and a decision on #62 TopologyUpdate push+ack
(do it or explicitly punt to 2.0). The durable-format policy from the
back-compat brief folds into #110's normative doc.

0.7 - the operator-onboarding release: node enrollment (brief below,
fully unblocked since #153, admin-surface only so zero interaction with
the frozen wire). Bundle the small operator polish: #81 stream debug UI
view, #83 microbatch stream cursor commits.

Parallel track, any time after #111 lands: Go client #119 - built
against the FROZEN API it conforms rather than expands the freeze
scope, and joins the #112 matrix as a fourth column. C# (#120) behind
it.

Then 1.0: gate-4 done, gate-3 completes with 0.5, gate-2 with 0.6,
cluster confidence met since 0.2. After 0.7 the remaining 1.0 work is
soak time and doc polish, not features.

Judgment call recorded: enrollment could swap with the freeze family
(0.6 <-> 0.7) since they do not interact. Freeze stays earlier - every
release shipped pre-freeze adds compat surface owed forever, while
enrollment loses nothing by waiting.

Parked with recorded triggers: per-topic authz (tenancy-dependent,
revisit against the tenancy criteria when a user asks), benchmarks.md
overhaul (blocked on the NVMe slice), the TUI demo cursor
(demo/website work, orthogonal - pick up opportunistically).

## 0.4 arc plans (2026-07-05) - implementation briefs

Written to be executable by a fresh implementer: goal, decisions,
invariants, brick order, test cases, and a map of the existing code each
arc builds on. Recommended execution order: exporter, drain handoff, TLS
tail - the exporter is independent and makes the other two observable,
drain handoff's simulation tests then double as regression cover for the
TLS tail, and the TLS tail is the deepest change.

### Arc: Prometheus exporter (#118) - SHIPPED

Shipped on main (post-0.3, lands in 0.4). As built, matching the brief
below with these concretions: the exposition writer is
crates/admin/src/prometheus.rs and the endpoint is
crates/admin/src/metrics_route.rs; queue ready/inflight come from the
per-partition debug snapshot (materialized, kind == Queue) rather than
get_queues_stats, which collapses partitions; replication adds a
node-level worker summary (always) plus per-follower applied
message/event offsets (flag-gated); recovery exports the quarantine
gauge and counter from the stroma recovery metrics. Docs: an Operations
sidebar section now exists and deployment/monitoring.md documents the
endpoint with scrape configs. OpenTelemetry remains a separate item.

Goal: GET /metrics on the admin server in Prometheus text exposition
format, translating counters that already exist. No new hot-path
instrumentation in this arc - export what is already counted.

Decisions:
- Hand-rolled exposition writer, no prometheus crate. Everything exported
  is a counter or gauge already held in atomics; the format is # HELP /
  # TYPE / name{labels} value lines. A registry dependency buys nothing.
- The endpoint lives on the ADMIN server (auth, HTTPS, and bind already
  exist there), path /metrics, behind the same admin basic auth (every
  scraper speaks basic auth + TLS).
- Naming: fibril_<subsystem>_<name> with _total on counters. Examples:
  fibril_broker_published_total, fibril_broker_delivered_total,
  fibril_tcp_connections_open, fibril_tcp_resume_accepted_total,
  fibril_stream_delivered_total, fibril_stream_lag_evictions_total,
  fibril_recovery_quarantined (gauge), fibril_queue_ready{topic,group,
  partition} (gauge).
- Cardinality policy: node-level aggregates always. Per-channel series
  (queue depth/inflight, stream subscriptions/lag evictions) come from
  the MATERIALIZED channels only, so sparse/idle queues stay out and the
  series count is bounded by active channels. A config flag
  admin.metrics_per_channel (default true) turns the per-channel block
  off for many-active-queue deployments.
- OpenTelemetry is a separate later item, not this arc.

Invariants:
1. A scrape never touches a delivery hot path or takes a per-message
   lock: it reads atomic counters and the same snapshot views the
   dashboard already uses (queues_debug / streams_debug data sources),
   nothing else.
2. Counters are process-lifetime monotonic; restart resets them
   (standard Prometheus counter semantics - document, do not fight).
3. Labels contain only channel identity (topic, group, partition, kind),
   never message data.
4. /metrics honors admin auth and serves over admin HTTPS when enabled.

Code map:
- crates/metrics/src: Metrics owns the registries; accessors
  Metrics::broker() -> Arc<BrokerStats>, ::tcp() -> Arc<TcpStats>,
  ::connections() -> Arc<ConnectionStats>. BrokerStats has the
  publish/deliver counters including the stream counts added with the
  parity work; BrokerStatsSnapshot shows the readable field set.
- crates/broker/src/broker.rs: StreamStatsEntry (live_subscriptions,
  lag_evictions), sparse_queue_observability_report(), the recovery
  snapshot (quarantined gauge + quarantines_total).
- crates/admin/src/server.rs router() for route registration,
  routes.rs check_auth + the queues_debug/streams_debug handlers for the
  safe data sources.

Bricks:
1. Exposition writer module in the admin crate: counter()/gauge()
   helpers building the text body; unit tests for formatting and label
   escaping.
2. Node-level metrics: tcp, connections, broker, resume outcomes.
3. Per-channel metrics behind admin.metrics_per_channel: queue
   ready/inflight from the observability report, stream subscriptions
   and lag evictions from the streams debug source.
4. Replication and recovery: follower applied state, quarantine gauge.
5. Docs: config row, implemented-surface row, admin-dashboard note, and
   a short monitoring page with an example scrape config.

Tests:
- Unit: escaping of label values (quote, backslash, newline), exactly
  one # TYPE per metric name, no trailing whitespace.
- Admin-crate integration (existing harness): 200 + body parses with a
  small line-checker (every non-comment line is name{...} float), 401
  without credentials when auth is enabled.
- fibril integration: boot a broker, publish and consume N, scrape,
  assert published_total >= N and delivered_total advanced; scrape over
  HTTPS reusing the tls_listener harness.
- Cardinality: three declared queues, flag off = no per-channel series,
  flag on = exactly the materialized channels appear.

### Arc: Drain ownership handoff (#132) - SHIPPED

Shipped on main (post-0.3, lands in 0.4), built to the decisions below
with one refinement the brief left implicit: a draining owner whose
followers report NO applied tails keeps its partition (reverting the
planner's move), because unlike a dead owner it is still serving - so
evacuation requires caught-up evidence and never hands ownership away
from the data. As-built pointers: DRAINING_LABEL + evacuation +
placement exclusion in control_iteration; ConnectionSettingsDrainController
.with_handoff (flag + bounded wait over the committed snapshot);
DrainOutcome from /admin/api/drain; drain_handoff_complete pure gate;
sim scenario ganglion_draining_owner_hands_off_before_stopping (label
on a LIVE owner moves ownership under the fence, old owner demotes and
refuses, no loss, owned count reaches zero). The drain-during-repartition
non-deadlock is held by construction (draining nodes leave placement
inputs) plus the no-new-placements controller test rather than a
dedicated sim scenario.

Goal: a draining broker proactively hands its partition ownership to
caught-up followers before stopping, so a planned restart produces a
near-zero delivery gap instead of waiting out reactive failover
(liveness TTL, default 9s).

Today: POST /admin/api/drain -> ConnectionSettingsDrainController
(crates/fibril/src/lib.rs, attached via with_drain) announces GoingAway
(Op 103) to connections; clients settle and reconnect; ownership moves
only after the node's heartbeat goes stale and the controller runs
reactive failover.

Decisions:
- Draining is a coordination-visible state carried as a HEARTBEAT LABEL
  (broker_heartbeat_labels in crates/fibril/src/lib.rs already carries
  advertise + per-queue applied tails). A label beats a raft attribute:
  it expires with the node naturally and needs no new command.
- The controller treats a draining node as an evacuation source: for
  each partition it owns with a caught-up follower (applied-tail labels
  are already how the failover candidate is chosen), plan a promotion
  through the EXISTING failover path (epoch bump, promote arms from the
  73x work). Drain handoff adds a trigger, never a second promotion
  mechanism - the loss-freedom argument is inherited, not re-proven.
- Draining nodes are excluded as targets for any new assignment,
  including repartition placements, which is also what prevents a
  drain/repartition deadlock.
- The broker finishes draining when its assignment view shows zero owned
  partitions, or after drain_handoff_timeout_ms (default 30_000). The
  timeout fallback IS today's behavior: reactive failover remains the
  backstop, so a drain can never wedge.
- No client changes: not-owner redirects, TopologyUpdate pushes, and the
  subscription supervisor already move traffic to the new owner.

Invariants:
1. Loss-freedom is inherited: only the existing promotion path moves
   ownership, with its caught-up and fencing rules untouched.
2. Bounded: a drain completes within the timeout even with zero
   followers (standalone partitions simply stay put and fail over
   reactively as today).
3. Single ownership: at most one owner per partition at every instant
   (epoch fencing; assert with the existing simulation invariants).
4. A draining node accepts and serves normally until each partition
   actually moves - the fence is the promotion, not the drain flag.

Code map:
- crates/fibril/src/lib.rs: broker_heartbeat_labels (add the draining
  label), ConnectionSettingsDrainController, and
  repartition_adoption_satisfied as the model for a pure, unit-testable
  bounded-wait gate.
- crates/coordination-ganglion/src/lib.rs: control_iteration and the
  failover candidate selection (most-caught-up follower via the
  applied-tail labels), the PromoteFollowerToOwner handling,
  ControllerStatus for observability.
- crates/broker: spawn_assignment_watcher_with_follower_replication -
  the broker's live view of what it owns; the drain wait polls this.
- crates/admin/src/routes.rs drain handler: extend the response with
  handoff progress (remaining owned-partition count).
- crates/protocol/tests/simulation_tests.rs: the turmoil multi-broker
  harness where the deterministic drain scenarios belong.

Bricks:
1. Draining label + controller evacuation planning + target exclusion.
2. Broker drain flow: wait for zero owned partitions with the bounded
   timeout, progress logging, admin response progress.
3. Config drain_handoff_timeout_ms (runtime settings, next to the other
   drain/connection knobs) + docs.
4. Simulation scenarios + status/implemented-surface rows (gate 3).

Tests:
- Simulation: three brokers, replicated queue, drain the owner ->
  ownership moves BEFORE drain completes; publish continuously through
  the drain and consume everything (no loss, no duplicate-confirm
  regression); assert no dual-owner instant.
- Simulation: drain with no caught-up follower -> completes at the
  timeout, reactive failover later, replica-durable confirms still hold.
- Simulation: drain during an active repartition transition -> both
  complete, bounded, no deadlock; draining node receives no new
  placements.
- Unit: the bounded-wait gate function (mirroring the
  repartition_adoption_satisfied test style).
- Integration (single real broker): draining label visible in topology
  labels, admin drain response reports progress and reaches zero.

### Arc: TLS tail (#153) - inter-broker TLS, coordination TLS, rotation - SHIPPED

Shipped on main (post-0.3, lands in 0.4). As built, matching the brief
with these concretions: the generation matrix and reloadable resolver
live in fibril-tls (mint-from-shared-CA is the (ca.pem, ca.key, -, -)
lane; the resolver is an ArcSwap<CertifiedKey> behind
rustls ResolvesServerCert, so the admin config shares rotation for
free); replication dials wrap in a connector carried on
ProtocolOwnerPeerResolverConfig (protocol Conn became
Framed<PeerStream> where PeerStream is plain-or-TLS over the net seam);
the ganglion sibling gained RaftAcceptor mirroring RaftDialer plus
start_durable_tcp_with_transport, and fibril injects
TlsRaftDialer/TlsRaftAcceptor (crates/fibril/src/raft_tls.rs) built
from the same material, with the node's coordination peer host added
to generated SANs. Rotation is POST /admin/api/tls/reload +
fibrilctl admin reload-tls, validated before swap, tested under
publish load. Cluster-secret auth on the RAW raft channel was assessed
in-brick and split out: it needs a raft-wire handshake change, and the
TLS peer-CA check now bounds who can reach the channel at all - the
remaining depth (proof of membership on the raft wire itself) folds
into the mTLS arc, where client-certificate identity covers it without
a second mechanism.

Goal: encrypt follower-to-owner replication and ganglion raft traffic,
and allow certificate replacement without a restart. Completes the
transport half of gate 4 (mTLS client auth stays separate).

Trust model (the crux): every node's server certificate must chain to a
CA that all peers trust. Two lanes, mirroring the client lanes:
- BYO: all node certificates issued from the operator's CA; peers trust
  it via tls.peer_ca_path (falling back to <data_dir>/tls/ca.pem when
  present, then OS roots).
- Generated: the deployment shares ONE generated CA. fibrilctl cert
  generate on the first node, copy ca.pem + ca.key into each node's tls
  dir, and each node mints only its own server certificate from that CA.
  This requires a load-bearing change in fibril-tls: TODAY
  ensure_generated_material refuses any partial presence; it must learn
  the "ca.pem + ca.key present, no server material" combination and mint
  the server cert from the existing CA. Enumerate the full presence
  matrix in a table test.
- Peer verification is standard rustls server-name verification against
  the peer's advertise host (generated SANs already include advertise
  hosts). The cluster secret keeps authenticating the @node principal
  INSIDE the session: TLS provides confidentiality and server identity,
  the secret provides membership - defense in depth, not redundancy.
- Config: tls.inter_broker (default follows tls.enabled; explicit false
  for mesh/mTLS-terminating environments) + tls.peer_ca_path.
- Rotation: server-side hot reload through a rustls ResolvesServerCert
  implementation reading an ArcSwap<CertifiedKey> held in ServerTls.
  POST /admin/api/tls/reload (+ fibrilctl admin reload-tls) re-reads
  material from the configured paths, VALIDATES it fully (the
  store_provided_material philosophy: prove it loads before anything
  changes), then swaps. In-flight connections keep the old certificate;
  new handshakes get the new one. CA rotation stays restart-required and
  documented (cross-CA bundles are a later refinement).

Invariants:
1. Mixed TLS/plaintext peers fail loudly: the protocol port already has
   the sniff + 426 machinery - verify it fires for peer connections and
   that the follower logs the guide; the raft transport needs an
   equivalent named error (a raft TLS listener receiving plaintext, and
   the reverse, must not hang).
2. A reload never degrades service: invalid new material is rejected
   with the old material still serving; the swap is atomic.
3. The generation matrix is total: all 16 presence combinations of
   {ca.pem, ca.key, server.pem, server.key} have a defined outcome
   (reuse, mint-from-CA, refuse) and a test each.
4. The simulation feature keeps compiling (the seam discipline from the
   TLS arc; tokio-rustls is IO-generic so TLS composes over turmoil
   streams if a simulated TLS scenario is wanted).

Code map:
- crates/tls (fibril-tls): ensure_generated_material (the matrix
  change), server_config_from, ServerTls (grows the reloadable
  resolver), store_provided_material (validate-before-write model).
- crates/protocol/src/v1/replication.rs: connect_protocol_owner_peer and
  the HELLO handshake helper near it - the follower-to-owner connect to
  wrap in a TLS connector; ProtocolOwnerPeerResolverConfig (with_auth,
  with_timeouts) is where the connector rides.
- crates/client/src/tls.rs build_connector: the connector shape to
  mirror for peers (CA file / OS roots; no fingerprint pin for peers).
- crates/fibril/src/lib.rs: spawn_ganglion_broker_tasks (peer resolver
  construction - inject the connector here), open_tcp_ganglion_parts
  (raft server + dialer injection).
- ../ganglion crates/ganglion-openraft/src/openraft_runtime/tcp.rs:
  RaftDialer (line ~314) is the injectable CLIENT side; TcpRaftServer
  binds tokio TcpListener directly (~line 226) with NO accept seam -
  the sibling change is an injectable acceptor (mirror the fibril
  handle_connection-generic approach), with its own ganglion changelog
  entry.
- Cluster-secret auth on the raft channel was deferred out of the auth
  arc; assess in-brick - if it needs a raft handshake protocol change,
  split it into its own follow-up rather than growing this arc.

Bricks:
1. fibril-tls: generation matrix + reloadable resolver + validated
   reload fn. Table tests first, they define the contract.
2. Replication over TLS: peer connector honoring tls.inter_broker and
   the peer-CA resolution; mismatch tests both directions.
3. Raft over TLS: TLS RaftDialer in fibril + the ganglion accept seam
   (sibling patch); cluster forms over TLS in a test.
4. Rotation: admin endpoint + fibrilctl command + swap wiring + a
   rotation runbook section in the cluster guide (same-CA leaf rotation
   live, CA rotation restart-required).
5. Docs: cluster guide TLS section, implemented-surface rows (flip the
   inter-broker and rotation Planned rows), changelog.

Tests:
- Unit: the 16-combination generation matrix; reload keeps old material
  on invalid input; two handshakes around a reload observe different
  leaf serials.
- Integration: two brokers replicate a queue over TLS end to end
  (follower catch-up + failover promotion, mirroring the existing
  replication integration path but with tls.inter_broker on).
- Mismatch: TLS follower to plaintext owner and reverse - named errors,
  no hangs; raft equivalent.
- Rotation under load: continuous publishes while reloading the owner's
  certificate - zero client-visible errors, new connections present the
  new serial.
- Ganglion: cluster formation over the TLS dialer + acceptor.

### After 0.4 - sequencing notes

- Gate 3 remainder (#102-#105, reconnect reconciliation family): typed
  subscription-close reasons on receive APIs, auto-resubscribe for safe
  recreate_client_side, inflight reconciliation across reconnect,
  durable restart reconciliation. Wants its own precedent-and-brief
  pass when picked up: the close-reason surface interacts with the API
  freeze, so design it immediately before or together with #111.
- Storage and snapshot back-compat (raised at the 0.4 cut, co-design
  with #110/#112): full brief below ("Storage and snapshot
  back-compat"). The quick win - generating the v0.4.0 golden fixture -
  needs no design and should happen BEFORE main drifts.

- Gate 2 freeze family (#109-#112): OPENS with the guided-errors pass
  (below), then newtype/Arc<str> (pure churn, last-moment before
  freezing), then the wire versioning + back-compat policy, client API
  freeze, and the compat matrix. Auth settled the handshake, so nothing
  structural blocks this after the TLS tail lands.
- Security depth, later minors: per-topic authorization (needs its own
  precedent pass: Kafka ACLs vs RabbitMQ per-vhost patterns, and it is
  tenancy-dependent - see the tenancy criteria section) and the
  node-enrollment arc recorded below. mTLS shipped in 0.4.
- Go client (#119): parallel-friendly at any point. The Python port
  playbook plus clients/FEATURE_MATRIX.md is the checklist.
- benchmarks.md overhaul stays blocked on the NVMe slice, spec recorded
  earlier in this file.

### Guided client errors pass (opens the gate-2 family)

Goal: extend the TLS-error philosophy - every error names the likely
fix - across the client-facing error surface, BEFORE the API freeze
locks the variants and wording in.

STATUS (2026-07-04): broker lane brick 1 + most of brick 2 DONE. The
broker_error_response funnel is now exhaustive per BrokerError variant
(InvalidArgument 500->400 code fix so retry_advice reads DoNotRetry,
NotEnoughInSyncReplicas guide names the levers, internal faults say they
are broker-side + point at the logs), with unit tests in handler.rs
error_guide_tests. Per-site guides added: expected-HELLO (port hint),
unknown-opcode (version skew), stream subscribe not-found (declare
first), stream not-owner fallback (owner unresolved = converging or no
coordination). Verified the queue path has NO missing-queue 404 - an
undeclared queue auto-materializes or returns NotOwner, so the brief's
Storage/Engine->404 mapping correctly does not apply.

Brick 3 (declare conflicts) DONE, mostly by verification: the "say WHICH
setting differs" premise largely does not apply. Queue re-declares UPDATE
settings (idempotent, redeclaring_same_kind_is_idempotent), so there is
no settings-mismatch conflict. Partition grow/shrink already guides well
(RepartitionQueueError names current + requested + the integer
multiple/factor constraint). The one real gap was the queue-vs-stream
kind-mismatch (keratin stroma) stating the problem but not the fix -
added the fix hint (keratin commit ba76fea). Repartition-in-progress
message left as is (clear enough). Cohort conflicts already guide.

Client-local lane DONE (2026-07-04). connection-refused now names the
broker-up + broker-vs-admin-port checks in all three clients (Rust
tls.rs connect_error, TS openSocket ECONNREFUSED branch, Python
_open_connection ConnectionRefusedError branch). heartbeat-timeout now
says network/broker stall not a client bug + mentions reconnect, in all
three (Rust engine loop, TS engine.ts heartbeatTick, Python engine.py
_heartbeat_loop). InvalidName left as is - already precise (each message
states the exact rule violated: 1-128 bytes, allowed chars, no leading/
trailing/consecutive dots), better than dumping all rules. Built
clients/error_guides.json as the {case, must_contain[]} parity list, and
a connection_refused parity test in each client reads it (Rust
tests/error_guides.rs, TS tests/error-guides.test.ts, Python
tests/test_error_guides.py) - all green, all connect to a bound-then-
freed ephemeral port. DEFERRED: a live heartbeat_timeout test needs a
stalled-broker harness (accept then go silent) in each client - the
wording is ported and spec'd in error_guides.json, only the live
assertion is pending. Reconnect-closure reasons stay with #102.

The guided-errors pass is now COMPLETE (broker lane + client-local
lane). Opens the gate-2 freeze family per the roadmap.

Two lanes, in order:
1. Broker-side messages first: guides that ride error frames improve
   all three clients at once with no client changes (precedent: the
   auth denials and the 426 guide). Candidates by user impact:
   ERR_NOT_FOUND on publish/subscribe (does the queue exist, is the
   group part of the identity), declare conflicts (say WHICH setting
   differs from the existing queue), ERR_NOT_OWNER surfacing to a user
   (why a redirect loop might be failing), coordination/repartition ops
   in static mode (this needs ganglion mode), operations on a
   quarantined partition (point at the repair endpoint and docs page).
2. Client-local errors second, per client with shared wording:
   connection refused (is the broker up, broker port vs admin port),
   heartbeat timeout vs clean close, InvalidName stating the actual
   naming rules, resume rejection reasons. The reconnect-closure
   reasons belong to #102 (typed close reasons) - co-design, do not
   duplicate.

Invariants:
1. A guide states a fix or a check, never just restates the failure.
2. No error becomes vaguer or loses machine-readable structure (codes
   and types stay; the guide is the message text).
3. Wording parity across Rust/TS/Python via a shared expectations list
   (the wire_vectors pattern applied to error text where the message
   originates client-side).
4. retry_advice stays consistent with the guide (an error telling the
   user to fix config must not advise Retry).

Inventory (done 2026-07-04 - execution starts from this list, the
grep work is already done):

Broker lane, in leverage order:
1. The central funnel FIRST: broker_error_response
   (crates/protocol/src/v1/handler.rs:360) maps only
   BrokerError::NotOwner to a specific code. Storage, Engine,
   InvalidArgument, NotEnoughInSyncReplicas and Unknown all flatten to
   (500, err.to_string()), so most queue-path failures reach users as
   "500: storage error: ...". Give every BrokerError variant an
   explicit (code, guide) arm:
   - InvalidArgument is a 400, not a 500. A code FIX, allowed and
     wanted before #110 pins codes.
   - NotEnoughInSyncReplicas keeps 500 + Retry advice but the guide
     names the knobs (the queue replication factor vs currently live
     replicas - read the exact setting names from the variant's call
     sites when there).
   - Storage/Engine not-found shapes (missing queue, missing
     partition) map to ERR_NOT_FOUND with "queue `X` has not been
     declared (group `Y` is part of the queue identity) - declare it
     or check the name". Verify which ops can actually hit this given
     auto-create gating, and guide the ones that can.
2. Per-site messages, all in crates/protocol/src/v1/handler.rs:
   - :1349 stream subscribe not-found (404): add "declare the stream
     first or check the name".
   - :3238 queue-subscribe-on-a-stream (400): already guides ("use a
     stream subscribe"), wording pass only.
   - :3581 and :3774 stream not-owner fallback ("not the owner of
     this stream partition"): the redirect failed, so say why the
     client sees a terminal error - owner unknown means the cluster is
     still converging (retry shortly) or the broker runs static mode.
   - :2314 "expected HELLO" (400): usually a non-Fibril client or the
     wrong port - "the first frame must be HELLO. Is this a Fibril
     client pointed at the broker port (9876 by default)? The admin
     API and dashboard listen on their own port (8081 by default)".
   - :4036 "unknown opcode" (400): version-skew guide - the client
     speaks a newer protocol than this broker, upgrade the broker or
     pin an older client.
   - :784-787 InstallSubscriptionError arms: CohortConflict and
     MemberIdConflict guides - "another consumer holds this exclusive
     group (one cohort per topic+group): use a different group, or
     join with a distinct member_id".
3. Declare conflicts (the say-WHICH-setting-differs requirement): the
   messages originate as Err(String) around declare_partitioning in
   crates/fibril/src/lib.rs (repartition-in-progress at :352, the
   shrink/grow arms below it) and reach the client via ERR_CONFLICT at
   handler.rs:3403/:3486. Audit each origin so the message always
   names the differing setting with its existing and requested values.
4. Coordination/repartition ops in static mode: already guided
   ("requires ganglion coordination", crates/admin/src/routes.rs:790).
   Wording-parity check only.
5. Quarantined-partition operations: guide points at the repair
   endpoint and the recovery-quarantine docs page (the quarantine
   surface lives in crates/broker/src/queue_engine.rs, QuarantineInfo
   and repair around :307-:314 - find where a client op meets a
   quarantined partition and what it returns today).

Client-local lane (taxonomies are already 1:1 - Rust FibrilError at
crates/client/src/lib.rs:199, TS clients/typescript/src/errors.ts,
Python clients/python/src/fibril/errors.py):
- Connection refused at connect time: "is the broker running at
  {addr}? Clients dial the broker port (9876 by default), not the
  admin port (8081)". Rust Disconnection, TS/Py DisconnectionError.
- InvalidName: state the actual rules from the validator (allowed
  characters, length, the @ prefix reservation), not just "invalid".
- Heartbeat timeout vs clean close: where the engine can tell them
  apart, say which happened - heartbeat loss usually means network or
  broker stall, not a client bug.
- Resume rejection reasons: NOT here. #102 owns the typed close and
  resume surface, co-design there.

Wording parity mechanism: add clients/error_guides.json next to
wire_vectors.json - a list of {case, must_contain: [keywords]}
consumed by all three client test suites for client-local errors.
Broker-side guides ride error frames, so one Rust integration test per
guide is enough (parity across clients is free).

Non-goals: no new codes for existing failures unless the current code
is a bug (the InvalidArgument 500), no guides that guess (a wrong
guide is worse than none), no #102 surface.

Tests: key paths assert the guide is present (contains the fix
keyword), mirrored in the three clients for client-local errors via
error_guides.json.

### Storage and snapshot back-compat (co-design with #110/#112)

Goal: the guarantee, from 0.5 on: a data dir upgrades in place from
the previous minor, and a mixed-version cluster of adjacent minors
works for the duration of a rolling upgrade. #110 covers the wire
protocol - this covers the durable formats. 0.4 made it urgent by
making rolling upgrades a first-class flow (drain-restart node by
node = a mixed-version cluster for the duration).

Audited facts (2026-07-04) per surface:
  Keratin persists queue state on TWO surfaces with DIFFERENT compat
  properties. Both matter for any field change.

  Surface 1 - event-log records: STROMA_VER = 3 (u16, per-record) at
  keratin stroma/core/src/event.rs. Records are length-framed by the log
  (keratin-log record.rs carries payload_len + a per-record RECORD_VERSION
  + CRC), and the event decoder reads exactly each type's known fields
  without asserting it consumed the whole slice, so trailing bytes are
  ignored. That makes ADDITIVE fields both forward and backward compatible
  with NO version bump - append the new field at end-of-record and read it
  behind an `i < bytes.len()` guard (absent on old records = default,
  ignored by old readers), repeatably. Locked by a
  decode_tolerates_trailing_bytes test plus a decode()-site comment in
  keratin so no later hardening adds a full-consumption check. The
  STROMA_VER equality test (`ver != STROMA_VER` -> InvalidData) only bites
  reordering/removing/resizing an EXISTING field: that needs a bump AND a
  kept decode arm for the old value. Sharp edge: MarkInflightMany is a
  count-prefixed fixed-stride array, so a per-entry trailing field needs a
  parallel count-matched trailing array, not a splice into the entries.

  Surface 2 - the snapshot blob: SEPARATE FORMAT_VERSION = 4 (state.rs)
  with an EQUALITY check on decode, and it is NOT trailing-tolerant. Its
  inflight map is a count-prefixed fixed-stride array of (offset, deadline)
  pairs. Adding a field to snapshotted state is therefore a RESHAPE: bump
  FORMAT_VERSION and keep a decode branch for the old version, else old
  snapshots turn unreadable. Snapshots compact away the events before them,
  so state that predates the last snapshot lives ONLY here - a field cannot
  be kept on the event alone.

  Consequence for #105's likely inflight-owner: it touches BOTH surfaces
  (recent deliveries as MarkInflight events, older ones only in the
  snapshot inflight map), so it is a free append on the event but a
  FORMAT_VERSION bump + kept decode branch on the snapshot. Not free
  overall.

  Decision - do NOT pre-add the inflight-owner field behind a placeholder
  now. The snapshot-branch cost is identical whether paid now or later, so
  pre-adding buys nothing and forces the FORMAT_VERSION bump now against a
  guessed shape (wrong type, per-message vs per-lease). A wrong guess
  becomes a second reshape, and it carries dead bytes on the hot delivery
  path meanwhile. Reserve a field only when its shape is confident; #105's
  persisted shape is still open. Still open to audit: whether keratin log
  segment headers carry their own version marker beyond STROMA_VER.

  Approach when the first structural change lands (both surfaces): convert
  the equality gate into version dispatch and migrate forward. Freeze each
  version's byte-decoder, a copy of its struct, and one Vn->Vn+1
  migration, and never edit old links, so each release adds exactly one
  migration. Two keratin simplifiers: the writer stays single-version
  (snapshots rewrite state current, so keep old DECODERS not encoders, and
  a format ages out of a live data dir after one snapshot cycle), and
  links prune with the support window (drop a version's decoder + link +
  golden fixture together once in-place upgrade from it is no longer
  promised). Additive trailing fields still need no link - the chain grows
  only on structural breaks. Migrations must be total and tested end to
  end (old bytes -> current); the golden fixtures are that test.
- Ganglion raft-wal.jsonl + snapshot.json: serde_json of
  PersistedMetadataLogEntry (ganglion crates/ganglion-storage/src/
  lib.rs), NO format marker anywhere. Additive serde-default fields
  are compatible today by luck of the encoding, renames/removals are
  silent corruption. Add a version field (serde(default) so v0 =
  absent) in the next ganglion storage change.
- Stroma global-store documents (runtime settings, auth users): CAS
  versions exist, schema versions do not. serde-default discipline is
  the guard - write it down as normative in the #110 policy doc:
  fields are add-only with defaults, never renamed or repurposed.
- Replication checkpoint blob: ReplicationStateCheckpoint inside
  ReplicationCheckpointExportOk/Install (crates/protocol/src/v1/
  mod.rs:723-733). Crosses BROKER VERSIONS during a rolling upgrade -
  the highest-risk surface. Same add-only serde discipline, and add
  the checkpoint frames to clients/wire_vectors.json so the #110
  vectors-diff CI gate pins their encoding.

Golden fixture mechanics (the enforcement) - BUILT for v0.4.0:
- Generator: crates/compat-fixture-gen (bin compat-fixture-gen) drives a
  running broker through a fixed workload - a partitioned queue (orders,
  4 partitions, group workers, custom DLQ) with messages left ready,
  acked, dead-lettered, delayed, and inflight, plus a durable stream
  (events, committed cursor). It prints a manifest (topic/partition/group,
  state counts, a recoverable_floor, payload prefix). Built from the
  CURRENT tree and pointed at the tag's server, so it only needs wire ops
  present since the tag (additive within a protocol version - add a guard
  when the version bumps).
- Script: scripts/gen-compat-fixture.sh <tag> builds the tag's broker in
  a SIBLING git worktree (../fibril-compat-<tag>, so the ../keratin and
  ../ganglion path patches still resolve), boots it, runs the generator,
  stops it cleanly, and tars the data dir to
  crates/broker/tests/fixtures/datadir-<tag>.tar.gz + .manifest.json.
- CI test: crates/broker/tests/compat_fixture.rs opens every committed
  fixture with the CURRENT engine and asserts zero quarantined partitions
  (a quarantine is how a decode failure surfaces), the durable stream
  recovered, and the queue drains at least recoverable_floor intact
  payloads. Plain #[test], no new CI job.

  Deviations from the original sketch, with reasons:
  - Artifact is .tar.gz via system tar, not .tar.zst - avoids assuming a
    zstd-enabled tar on every runner. 484K data dir (preallocated
    segments) compresses to ~4.7K, so gzip is plenty.
  - Scope is STANDALONE single-node: covers keratin message logs, event
    logs, snapshot/kind markers, the durable stream log + cursor, DLQ,
    and the stroma global store. Does NOT cover the ganglion raft
    wal/snapshot (needs ganglion mode) - a ganglion fixture variant is
    future work. The auth-user and runtime-settings global-store docs are
    reachable in standalone (both persist there) but the current
    generator does not write them yet - add if that surface needs
    pinning.
  - Inflight IS included (MarkInflight events) and is durable on two
    surfaces per the surface-1/surface-2 note above.

Mixed-version lane (LATER, with #112): cluster-tryout.sh grows
--mixed-version <prev-image> - node A on the previous release image, B/C
on the current build, run the --failover-verify flow. Needs published
images, so it lands with #112, not before.

Sequencing: the fixture is DONE for v0.4.0. The version markers and
decode-branch work land with the first format-touching change. The
policy prose lands inside #110's normative doc.

## Node enrollment arc plan (post-0.4, after #153)

Goal: one command joins a freshly booted machine to an existing cluster.
An operator runs an invite on any member, pastes the resulting token into
fibrilctl (or the setup page) on the new node, and the node ends up with
the cluster secret, TLS material chaining to the cluster CA, and
coordination config - then joins, catches up, and receives users and
settings through normal replication. The Elasticsearch enrollment-token
and kubeadm-join pattern.

Precedent anatomy (ES, kubeadm, docker swarm): a token carries reachable
endpoints, a way to VERIFY the cluster before trusting it (a CA
fingerprint pin), and a short-lived credential authorizing the join. The
real secrets travel only inside the verified channel, never in the token.

Token design:
- Opaque base64 of a versioned JSON: { v, endpoints, ca_fingerprint,
  expires_ms, nonce, hmac } where hmac = HMAC-SHA256(cluster_secret,
  canonical(v|endpoints|ca_fingerprint|expires_ms|nonce)).
- The token NEVER contains the cluster secret. Leaking a token leaks a
  bounded-lifetime join authorization, nothing durable. Default TTL 30
  minutes, configurable at mint.
- Single-use: redemption records the nonce in a coordination attribute
  (CAS, with expiry-based GC of old nonces). If the CAS bookkeeping
  proves heavier than expected in-brick, TTL-only is the documented
  fallback, but single-use is the default posture for a secret-yielding
  exchange.

Flow:
1. Any member: fibrilctl cluster invite [--ttl ...] -> POST
   /admin/api/enrollment-token (admin-auth'd). The node must hold the
   cluster secret and the cluster CA to mint.
2. New node, CLI lane: fibrilctl node enroll --token X [--data-dir D],
   with the server NOT yet running. The CLI dials a token endpoint over
   HTTPS pinned to ca_fingerprint (reusing the client fingerprint-pin
   verifier from crates/client/src/tls.rs - the exact machinery the TLS
   arc built), and POSTs /admin/api/enroll with the token plus the new
   node's advertise hosts and proposed listen address.
3. The issuer verifies the HMAC + expiry + unused nonce, then:
   - mints a SERVER certificate for the joiner's SANs from the cluster
     CA (mint-on-issue: the CA key never leaves the nodes that already
     hold it - least privilege, the ES model - so the response carries
     ca.pem + leaf + key, not ca.key),
   - reserves a raft id from committed membership via a CAS attribute
     (never guessed locally - collision safety is cluster-coordinated),
   - records the consumed nonce,
   - returns { cluster_secret, ca_pem, server_pem, server_key,
     coordination: { raft_node_id, peers including self, mode } }.
4. The joiner writes everything into the data dir: cluster.secret (0600),
   tls-provided material, and a coordination section in
   config-overlay.toml (ConfigOverlay grows coordination, same
   layered-below-explicit-config precedence as tls and auth). The
   overlay+marker write is LAST and is the commit point: a failure at
   any earlier step leaves nothing that changes the next boot.
5. Boot: the node starts in ganglion mode from the overlay, joins as a
   LEARNER first and is promoted once caught up - ganglion already has
   this path (the learner_joins_catches_up_and_gets_promoted test) -
   then users, settings, and assignments arrive via existing
   replication. Enrollment installs TRUST and ADDRESSING only; this is
   the payoff of keeping node trust separate from user data in the auth
   arc.
6. Setup-page lane: a "Join a cluster" card (paste token) performs the
   same redemption server-side during first-boot setup, then falls
   through into the normal boot as setup mode already does.

Invariants:
1. The cluster secret and private keys travel only inside TLS pinned to
   the cluster CA fingerprint carried by the token.
2. Tokens are bounded-lifetime and single-use; a tampered, expired, or
   replayed token yields a guided error naming the fix (mint a new
   invite), never a partial join.
3. Joiner-side atomicity: overlay + marker last; any earlier failure
   leaves the data dir boot-inert, and re-running enroll succeeds.
4. Idempotent: enrolling a node that already holds the marker is a
   no-op with a clear message.
5. Raft ids are reserved through coordination CAS, never assigned by
   the joiner.
6. Enrollment grants membership only - no user data rides the
   enrollment response; it replicates after join like everything else.

Prerequisites: #153 lands first - the CA-reuse/mint-from-CA machinery in
fibril-tls (its brick 1) is exactly the mint-on-issue primitive, and
inter-broker TLS is what makes the post-join cluster traffic protected.
Admin HTTPS (shipped in 0.3) already carries the redemption itself.

Code map:
- crates/tls: the #153 mint-from-existing-CA path, plus a
  mint_server_cert_for_sans(ca_pair, sans) helper for the issuer.
- crates/client/src/tls.rs: FingerprintVerifier - reuse for the
  joiner's pinned HTTPS (the CLI can use reqwest with a custom rustls
  config, or a thin hyper call over tokio-rustls; the verifier logic is
  the part to share).
- crates/config: ConfigOverlay + coordination section, CLUSTER_SECRET_FILE,
  the setup marker constants; resolve_cluster_secret shows the read side.
- crates/admin: routes.rs users/enroll route patterns, check_auth;
  setup.rs for the join card (mirror the existing card + apply-callback
  shape).
- crates/coordination-ganglion: cluster attribute CAS (update_users is
  the template for nonce records and raft-id reservation);
  add-voting-member plumbing (CoordinationMembershipManager) and the
  learner-promotion path via ganglion.
- crates/cli: AdminClient + the cert/secret command patterns.

Bricks:
1. Token module: mint/verify (HMAC, expiry, canonical encoding), pure
   and vector-tested (tamper each field, expiry edge, wrong secret).
2. Issuer: enrollment-token + enroll endpoints, mint-on-issue, raft-id
   reservation, nonce single-use; fibrilctl cluster invite.
3. Joiner CLI: fibrilctl node enroll with pinned redemption and the
   atomic write order; guided errors for every refusal.
4. Learner-first join wiring + promote-on-catch-up exposure if not
   already surfaced through coordination-ganglion.
5. Setup-page join card.
6. Docs: the cluster guide's entry-level path becomes invite + enroll;
   manual secret/CA distribution remains documented as the unattended
   and air-gapped lane.

Tests:
- Unit: token vectors (each field tampered, expired, wrong secret,
  version bump), nonce CAS single-use.
- Integration: one-node cluster, invite, enroll a second data dir ->
  files and overlay exactly as specified; boot node 2 -> learner, then
  promoted; a user created on node 1 BEFORE the join appears on node 2
  AFTER it (the payoff assertion for the whole trust design).
- Failure: expired token (nothing written), wrong-CA endpoint (pin
  refuses - the MITM case), replayed token (single-use refuses),
  re-enroll with marker present (no-op).
- Atomicity: inject a write failure between material and overlay ->
  next boot unaffected, re-run enrolls cleanly.

## Gate 3 arc plan: reconnect reconciliation family (#102-#105)

Goal: a client always KNOWS what happened to its subscriptions and
inflight work across a reconnect or broker restart, as typed surface
rather than silence or a generic disconnect. Finishes the operational
lifecycle gate together with drain handoff.

Standing decision (recorded earlier): the typed close-reason surface
(#102) and auto-resubscribe (#103) are API-shape decisions and are
designed TOGETHER with the client API freeze (#111). Do not invent the
enum here and freeze a different one there.

What exists (code map):
- crates/protocol/src/v1/mod.rs: ReconcileClient/Server/Result,
  ReconcileAction { Keep, CloseClientSide, CloseServerSide,
  RecreateClientSide }, ReconcilePolicy { Conservative,
  Restore }, ResumeIdentity/ResumeOutcome.
- crates/protocol/src/v1/handler.rs: reconcile_subscriptions, the resume
  session store (resume_sessions, forget_if_dormant/forget_if_generation),
  reconnect grace (connection.reconnect_grace_ms).
- Clients: reconnect_reconcile_policy across Rust/TS/Python; the
  subscription supervisor (failover.rs / routing) that already
  re-subscribes on owner moves - auto-resubscribe must go through it,
  not around it.
- Done groundwork: #101 cold-start orphaned-partition reconciliation,
  #108 owner-scoped resume identity.

Design outline:
- #102 typed close reason: subscription receive APIs end with a typed
  terminal event, not just stream end: enum SubscriptionClosed { reason:
  Reconciled(ReconcileAction + human reason), OwnerMoved, Drained,
  BrokerRestarted, Shutdown, Lagged(stream policy - the deferred P3
  events fold in here) }. One enum across Rust/TS/Python, wire-carried
  where the server knows the reason (extend ReconcileSubscriptionResult
  reason from free string to a tagged code + message, keeping the string
  for humans).
- #103 auto-resubscribe: when reconcile returns RecreateClientSide and
  the subscription is safely recreatable (manual-ack, no exclusive
  cohort surprise - exclusive rejoin already carries member_id), the
  client re-subscribes through the existing supervisor instead of
  surfacing a close. Opt-out flag; default ON for supervised
  subscriptions to match product philosophy.
- #104 inflight reconciliation: define and enforce tag validity across
  resume. Semantics to implement: within reconnect grace + successful
  resume, delivery tags remain settleable (the session held them); after
  grace or a failed resume, tags are invalid and settles get a typed
  stale-delivery error while the messages redeliver. The client marks
  its in-hand deliveries stale on ResumeOutcome != Resumed so user code
  learns immediately rather than on the failed ack.
- #105 durable restart reconciliation: a broker RESTART currently voids
  sessions (memory of sessions is in-process). Decided scope (ratify
  when picked up): persist resume session skeletons (client_id, owner_id, token,
  subscription set) in the global store with a bounded TTL, so a fast
  restart lets clients resume with Reconciled outcomes instead of
  ResumeNotFound; messages redeliver per at-least-once as today. NOT a
  delivery-tag preservation across restarts - tags die with the
  process, and #104's stale-tag surface covers that honestly.
  Storage-format note: if #105 is ever taken FURTHER than redelivery -
  i.e. a reconnecting client reclaims its still-held inflight leases
  across the restart instead of redelivering - the broker has to know
  who each inflight message belonged to, which means persisting a
  consumer/session identity wherever inflight state lives. Per the
  back-compat brief that is BOTH surfaces: a free additive trailing field
  on the MarkInflight/MarkInflightMany event, but a FORMAT_VERSION bump +
  kept decode branch on the snapshot inflight map (fixed-stride, equality
  checked), because a message inflight before the last snapshot exists
  only in the snapshot. Not pre-added now (decision recorded in that
  brief). The v0.4.0 golden fixture pins today's owner-less encoding on
  both surfaces, so whenever this lands CI proves old inflight state still
  decodes.

Invariants:
1. At-least-once is never weakened: any ambiguity resolves to
   redelivery plus a typed signal, never silent drop.
2. Every subscription termination has exactly one typed terminal event;
   no path may end a stream silently (test: grep-level exhaustiveness -
   every `break`/close in the delivery pump maps to a reason).
3. Auto-resubscribe never changes settlement semantics: unsettled
   deliveries from the old incarnation stay governed by #104 rules.
4. Wire changes are additive (serde-default fields, tagged code beside
   the existing reason string) - this family lands BEFORE the wire
   freeze and must not force a version bump.

Bricks: (1) wire: tagged reason codes on reconcile results + close
frames where missing; (2) Rust client terminal-event surface + stale-tag
marking; (3) auto-resubscribe via the supervisor; (4) TS/Python parity +
FEATURE_MATRIX rows; (5) broker durable session skeletons + restart
reconcile path; (6) docs (reconnects page rewrite).

Tests: kill-connection matrix per policy x outcome asserting the exact
terminal event; settle-after-grace gets the typed stale error and the
message redelivers exactly once more; auto-resubscribe continuity (no
gap, no dup-settle) under owner restart; broker restart with skeletons:
resume succeeds, subscriptions reconcile, redeliveries flagged
redelivered; soak-suite extension asserting no silent stream ends.

## Gate 2 arc plan: freeze family (#109-#112)

Goal: 1.0's compatibility promise. Sequenced after the reconciliation
family (its wire additions land first) and after security settled the
handshake.

- #109 newtype + Arc<str> pass: Offset/Epoch-style domain integers get
  distinct serde(transparent) newtypes (Partition already is one,
  DeliveryTag exists as a struct in crates/common/src/lib.rs:12).
  Known starting anchors: `pub type Offset = u64` is a bare alias at
  crates/storage/src/lib.rs:16, and epoch rides as bare u64 inside
  DeliveryTag (common/src/lib.rs:13) and throughout
  crates/broker/src/coordination.rs. Topic/Group become validated
  Arc<str> newtypes end to end (interning groundwork exists from #65).
  Pure mechanical churn - do it IMMEDIATELY before freezing so nothing
  re-churns after. Compiler does the work; tests are the existing
  suites passing. Newtyping wire-visible fields must stay
  serde(transparent) so no wire bytes change (the vectors gate below
  proves it).
- #110 wire versioning + back-compat policy: PROTOCOL_V1 and the HELLO
  negotiation already exist (crates/protocol/src/v1/mod.rs:31). Write
  the policy down as normative doc: additive-only within a protocol
  version (serde-default fields, new opcodes), version bump criteria,
  support window (broker supports N and N-1; clients declare, broker
  answers with negotiated), clients/wire_vectors.json is the
  cross-client byte pin and CI gate. Add a vectors-diff check to CI
  (rust-ci.yaml + typescript-client-ci.yaml already run the vector
  suites - the missing piece is a regeneration step that fails when
  committed vectors and regenerated vectors differ). The durable
  formats get their policy from the back-compat brief above, written
  into the same doc.
- #111 client API freeze: the public-API review across Rust/TS/Python
  against FEATURE_MATRIX; the typed subscription-lifecycle enum from the
  reconciliation family is ratified here; deprecations resolved; then
  semver discipline begins (breaking = major).
- #112 compat matrix + enforcement: a CI job running the PREVIOUS
  released clients (crates.io/npm/pypi or the previous tag) against the
  new broker image for a smoke (connect, publish, consume, stream,
  TLS) - the matrix in docs is generated from what CI actually proves,
  never hand-claimed. The mixed-version BROKER lane from the
  back-compat brief (cluster-tryout.sh --mixed-version <prev-image>)
  lands here too - same prerequisite (published images), same matrix.

Invariant: nothing merges to main after the freeze commit that changes
wire bytes for existing frames (the vectors gate) or breaks the
previous-client smoke (the matrix gate).

## Security depth plans (post-0.4 minors)

### Arc: mTLS client auth (#114 tail, gate-4 depth) - SHIPPED

Shipped on main (post-0.3, lands in 0.4), built exactly to the decisions
below. As-built pointers: verifier + identity + issue_client_certificate in
fibril-tls (identity extraction itself lives in fibril-util so the protocol
crate can use it); verified_identity flows serve_connection ->
handle_connection; StoreAuthHandler::decide_certificate is the exists-check;
peer_connector_from_config presents the node leaf on ALL broker dials
unconditionally; fibrilctl cert issue writes <identity>.pem/.key. One
Python-specific hedge, documented in the FEATURE_MATRIX footnote: asyncio
flattens the certificate-required alert into clean EOF, so a certless TLS
connect ending with no HELLO reply maps to the typed error.

Goal: a verified client certificate is an identity. Workloads connect
with a certificate instead of a password, an unidentified peer cannot
finish the handshake when certs are required, and the cluster's raft
and replication listeners become reachable only by deployment-CA
certificate holders (closing the raw-raft-channel gap split out of the
TLS tail).

Decisions:
- Config: tls.client_auth = "off" | "request" | "require" (default
  off) + tls.client_ca_path. The client CA falls back to the generated
  <data_dir>/tls/ca.pem when present (the self-contained deployment
  lane, which also admits peer brokers' minted leaves); with neither,
  enabling client_auth is a guided config error - OS roots make no
  sense for client certificates.
- Server: WebPkiClientVerifier on the ServerTls build; request mode
  uses allow_unauthenticated so certless clients still connect and
  password-auth (the migration lane), require rejects them in the
  handshake. The verifier is built once at boot; rotating the CLIENT
  CA is restart-required, same rule as the server CA.
- Identity: first DNS SAN, else CN. A verified identity X
  authenticates the connection as user X with no AUTH frame when X
  exists in the user store - the store stays the single authority on
  who exists, certs only replace the proof. A valid cert with an
  unknown identity is a transport pass only: the connection proceeds
  unauthenticated and may still password-auth. Identities starting
  with @ never map (the node namespace stays unclaimable), and an AUTH
  frame on a cert-authenticated connection gets the existing
  already-authenticated error.
- Threading: serve_connection captures peer_certificates() after the
  TLS accept, fibril-tls extracts the identity (x509-parser, already
  in-tree via rcgen), and handle_connection gains
  verified_identity: Option<String> alongside peer_addr. The
  AuthHandler trait gains decide_certificate(identity) so the store
  handler owns the exists-check and the static test handler stays
  trivial.
- Outbound: when client_auth is on, broker peer dials (replication
  connector and raft dialer) present the node's own server leaf as
  their client certificate (with_client_auth_cert) - standard node
  practice, no second key pair. Client options gain
  cert_path/key_path in the existing TLS options (Rust, then TS and
  Python in the parity brick); a required-cert rejection joins the
  typed error taxonomy as its own reason.

Bricks:
1. fibril-tls: client-verifier modes on the server build, identity
   extraction (SAN else CN, @ never maps), client-cert-bearing
   connector variant. Unit tests define the identity contract.
2. Protocol + broker: verified_identity through serve_connection into
   handle_connection, decide_certificate on the AuthHandler trait and
   StoreAuthHandler, pre-authenticated connection state. Integration:
   cert connect with no AUTH frame against a booted broker.
3. Inter-broker under require: dials present the node leaf; a cluster
   replicates and elects with client_auth = require end to end.
4. Rust client cert options + typed required-cert error; TS + Python
   parity with shared expectations.
5. Docs: configuration rows, cluster guide (workload identity lane),
   implemented-surface flip, status, changelog. No setup-mode card:
   client certs are PKI-issued, not first-boot material.

Tests: the mode x credential matrix (off/request/require x
cert/certless/wrong-CA cert), identity-mapped connect performs real
ops without AUTH, unknown-identity cert still password-auths in
request mode, @-identity cert never maps, require mode rejects
certless brokers' dials until they present the leaf (then the cluster
converges), reload keeps serving after leaf rotation with client_auth
on.

### Per-topic authorization (compact brief - precedent first)

Standing note (2026-07-04): this item is NOT committed -
it reads as tenancy-adjacent and only
happens if a real need appears. Treat the brief below as a shelf
design, not queued work; revisit alongside the tenancy criteria.

- Precedent to weigh in-brick: Kafka ACLs (principal x operation x
  resource pattern, LITERAL/PREFIXED) vs RabbitMQ per-vhost regex
  triples (configure/write/read). Lean: per-user rule list
  { topic_glob, ops subset of publish/consume/declare/admin }, stored ON
  the user record in the existing replicated user document (no new
  store), enforced at handler op dispatch where the op already knows
  topic + authenticated principal.
- Migration invariant: users without rules keep full access (rules are
  opt-in narrowing), so shipping authz breaks nobody; a default-deny
  mode can be a config flag later.
- Denials are guided (which rule would be needed), 403-coded, never
  retried by clients (extend retry_advice).
- Tests: glob matrix, deny messages, rules replicate with the user
  document, dashboard/fibrilctl editing, @node exempt (node principal
  is transport, not user ops).

### Go client (#119) playbook (compact)
- The Python port process is the template: (1) wire codec pinned
  byte-for-byte against clients/wire_vectors.json first; (2) engine
  (HELLO/AUTH/heartbeat/dispatch) against the fake-broker harness
  pattern (see clients/python/tests/fake_broker.py); (3) client layer
  (topology cache, pool, redirects, supervisor) mirroring
  clients/ARCHITECTURE.md; (4) TLS + the five-error taxonomy + 426
  mapping; (5) FEATURE_MATRIX column driven to parity, notes for gaps.
- Go-specifics decided up front: context.Context on every blocking call,
  channels for subscriptions, no reflection-based codec (hand-rolled
  like the others), module path + CI job mirroring the TS workflow.

### C# client (#120) playbook (investigated brief)

How .NET messaging clients are actually consumed, from the ecosystem
reference points: Confluent.Kafka (poll loop, callback logging),
RabbitMQ.Client v7 (async-first rewrite, IChannel), NATS.Net (the most
modern shape: IAsyncEnumerable subscriptions, ValueTask, Channels), and
Azure Service Bus (Sender/Receiver plus a callback Processor). The
dominant consumption pattern is an ASP.NET Core app registering the
client as a DI singleton and consuming inside an IHostedService
background loop with a CancellationToken. Decisions below follow the
NATS.Net-style modern shape, which matches Fibril's pull model best.

Stack decisions:
- Target net8.0 (LTS floor; multi-target net9.0 if free). NuGet package
  Fibril.Client, SourceLink, XML docs, nullable enabled.
- Transport: System.IO.Pipelines over NetworkStream/SslStream - the
  canonical high-performance framing stack; the 20-byte frame header
  parses with BinaryPrimitives (big-endian) over spans. Codec is
  hand-rolled like every other client, zero codec dependencies.
- TLS: SslStream with SslClientAuthenticationOptions. ca_path lane uses
  X509Certificate2.CreateFromPemFile + X509ChainPolicy.CustomTrustStore
  with TrustMode.CustomRootTrust (the proper custom-CA mechanism since
  net5, not a hand-rolled callback). Fingerprint-pin lane uses a
  RemoteCertificateValidationCallback hashing cert.RawData across the
  presented chain (SHA-256, colons optional - same normalization rules
  as the other clients). Server-name override maps to TargetHost.
- Errors are EXCEPTIONS, per platform law, mirroring the taxonomy 1:1:
  FibrilException base; TlsRequiredByBrokerException (the 426 mapping in
  the HELLO wait), TlsNotSupportedByBrokerException,
  TlsCertificateUntrustedException, TlsConfigException,
  ServerException(Code), DisconnectionException, plus RetryAdvice
  exposed as a property/helper so retry classification survives the
  enum-to-exception translation.
- API shape: immutable options builder in PascalCase
  (new ClientOptions().WithAuth(...).WithTlsCaPath(...)), then
  ConnectAsync(address, CancellationToken). Subscriptions are
  IAsyncEnumerable<FibrilMessage> (await foreach + msg.CompleteAsync() /
  RetryAsync / FailAsync), backed internally by a bounded
  System.Threading.Channels channel so prefetch backpressure is
  structural. CancellationToken on every awaitable; IAsyncDisposable on
  client and subscription (await using). PeriodicTimer for heartbeats.
  No callback Processor and no sync facade in v1 (TS precedent: n/a).
- MsgPack content type: optional, matching the #95 policy - core stays
  dependency-free (raw/json/text built in via System.Text.Json);
  MessagePack support ships as a small companion package
  (Fibril.Client.MessagePack) since NuGet has no feature flags.
- Core stays free of Microsoft.Extensions.*: no ILogger dependency in
  the client itself (diagnostic hooks as plain events). A DI companion
  (AddFibrilClient(IServiceCollection) + options binding) is a
  follow-up nicety once the core is stable, not part of v1 parity.

Port order (the Python playbook, restated for this stack):
1. Wire codec pinned byte-for-byte against clients/wire_vectors.json
   (xUnit theory over the vector file) before any networking.
2. Engine: HELLO/AUTH/heartbeat/dispatch against a fake broker
   (TcpListener speaking the real codec - port the shape of
   clients/python/tests/fake_broker.py).
3. Client layer: topology cache, per-endpoint pool, redirect follow,
   publish failover retry, subscription supervisor - mirror
   clients/ARCHITECTURE.md, using Channels and tasks where Python used
   asyncio primitives.
4. TLS + the full error taxonomy including the 426 path, tested with
   run-time-minted openssl certs like the TS/Python suites (no
   committed material).
5. FEATURE_MATRIX column driven to parity with notes; CI job mirroring
   the TS workflow (dotnet test on ubuntu, plus the examples-as-tests
   lane against the published broker image).

Invariants: wire bytes proven by the shared vectors before anything
else builds on them; every blocking operation accepts a
CancellationToken; no exception path loses the taxonomy (a transport
mismatch must never surface as a bare IOException); package versioning
tracks the workspace release line like the TS and Python packages.


## Tenancy examination (deferred question, criteria recorded)
Decide AFTER authz ships, by answering: do groups-as-namespace-prefixes
plus per-user authz rules already give tenant isolation for the real
asks (credential isolation: yes after authz; quota isolation: no -
would need per-principal rate/storage limits, a separate arc; admin
isolation: no - dashboard is cluster-wide)? If the missing pieces have
no concrete demand, tenancy stays a documentation pattern (prefix +
authz rules), not a feature.


## Auth beyond the static handler (gate 4, #114) - design brief (2026-07-05)

The next arc. Decisions below are settled from precedent (Kafka, RabbitMQ,
Elasticsearch, etcd, MongoDB), with the TLS arc as the prerequisite that
makes password auth meaningful on the wire.

Decided model:
- Users are cluster-shared DATA, replicated through the same machinery as
  the runtime-settings document (raft-backed in ganglion mode, node-local
  persisted standalone). Create once anywhere, works everywhere, survives
  failover. Redis-style per-node ACL files are the known anti-pattern.
  Stored as salted argon2 hashes (SCRAM verifiers are a possible later
  upgrade for challenge-response on the wire).
- Wire auth stays username/password over the existing AUTH frame, verified
  server-side against the store. This assumes TLS, like SASL/PLAIN
  everywhere else. Loud guide when real auth is enabled on a plaintext
  non-loopback listener.
- Node-to-node trust is OPERATOR-PROVISIONED and separate from the user
  database - that is how every system breaks the circularity of "user data
  syncs over replication but replication needs auth". A cluster shared
  secret (Erlang cookie / MongoDB keyFile precedent, fibrilctl secret
  generate) authenticates follower-to-owner replication and ganglion peer
  connections as a NODE principal, never as a user account (the Kafka
  inter-broker-user wart entangles node trust with user lifecycle).
  Cert-based node identity is the later evolution and converges with #153,
  but auto mode mints a CA per node, so cluster CA sharing/enrollment
  machinery belongs there, not here.
- Default credentials (fibril/fibril) become LOOPBACK-ONLY, the RabbitMQ
  guest model: localhost dev stays zero-friction, a remote connection with
  default credentials is rejected with a mini-guide to create a real user.
- First-boot setup mode gains a set-admin-credentials step alongside the
  TLS choice.
- Authz (per-topic permissions) is a separate later arc. This one is
  authentication only.

Bricks, in order:
1. Credential store: replicated user document (name + argon2 hash), seeded
   from config on first boot, standalone persistence via the existing
   runtime-settings path.
2. DONE (2026-07-05). AuthDecision on the AuthHandler trait (denials carry
   the guide the client sees in AuthErr), StoreAuthHandler decision order:
   node principal, stored users from anywhere, default pair from loopback
   only with the create-a-user guide, timing-flattened unknown-user
   denials. Boot warns loudly on users + no TLS. Ride-along fix: the
   connection writer now drains and flushes queued frames on shutdown
   (bounded, abort backstop) - final error replies used to lose a race
   with teardown and surface as bare EOF.
3. DONE (2026-07-05). Cluster secret: FIBRIL_CLUSTER_SECRET env, then
   coordination.secret_path, then the <data_dir>/cluster.secret convention
   file (what fibrilctl secret generate writes, 0600, value behind
   --show). Ganglion mode refuses to start without one (guided error).
   Replication authenticates as the @node principal with the secret,
   usernames starting with @ are reserved for node principals. The demo
   compose carries a labelled demo-only secret. NOTE: the user document
   is per-node local until the coordination bridge lands with brick 4, so
   live user edits do not replicate yet - config-seeded users stay
   consistent because every node seeds the same config. Raft-transport
   secret auth rides with #153 (it needs the transport work there).
4. DONE (2026-07-05). UserAdmin trait on the admin server serving
   /admin/api/users (LocalUserAdmin standalone, GanglionUserAdmin cluster),
   fibrilctl user add/passwd/remove/list, dashboard Users section, and the
   cluster bridge: a fibril/auth_users cluster document (versioned, CAS)
   with a watch-sync task that adopts newer versions locally, parallel to
   the runtime-settings document. NOTE: admin.auth (dashboard basic auth)
   still stays a separate config today, not folded into the store - a role
   flag on the user store is a later refinement, not blocking 0.3.
5. DONE (2026-07-05). Setup page gained an optional admin-user card and an
   optional cluster-secret card (none / generate / paste). Credentials
   persist as auth.seed_users in the config overlay (ConfigOverlay grew an
   auth section, layered below explicit config), the secret writes to the
   data-dir convention file. Explicitly configured tls AND auth
   auto-completes setup. Integration tested end to end (setup-created
   remote user authenticates over the setup-enabled TLS, secret written).
6. DONE (2026-07-05). Cluster setup guide at deployment/cluster: secret,
   TLS across nodes, entry-level setup-mode bring-up, the unattended
   config/env lane, verification via fibrilctl admin topology, user
   management. Linked in the sidebar.
7. Client polish: typed auth errors where they are still generic.

Future arc: NODE ENROLLMENT - full implementation brief in the
"Node enrollment arc plan" section above (token design, mint-on-issue,
learner-first join, invariants, bricks, tests).

Post-arc examination item: tenancy. Fibril groups are already namespace
prefixes, which is a natural hook if multi-tenant isolation ever becomes
meaningful. Assess after the arc, not designed now.

MILESTONE: cut v0.3.0 when bricks 1-5 land. Theme: security (TLS in
transit + real authentication). Do NOT hold the cut for #153, mTLS,
authz, or the Prometheus exporter - those are 0.4 material. The deployed
docs only refresh at a release, so the TLS work stays invisible on the
site until this cut.


## TLS in transit (gate 4, #113) - design brief (2026-07-04)

The next arc. Decisions below are settled; implementation has not started.

Decided cert model:
- Never ship certs. Shipped default certs are the default-password
  vulnerability class.
- Primary mode: operator-supplied PEMs (tls.cert_path, tls.key_path,
  optional client CA path reserved for mTLS under #114 later).
- Just-works mode: opt-in per-deployment generation via rcgen, the
  Elasticsearch 8 pattern - generate a CA + server cert into the data dir on
  first boot when enabled, print the CA fingerprint for clients to pin.
  Clients gain ca_path and fingerprint-pin options. Docs honesty: a
  self-signed cert the client does not verify defeats passive snooping only.
- Setup UX (SHIPPED 2026-07-04): fibrilctl cert generate/fingerprint, and
  first-boot setup mode as designed. setup.mode = true (or
  FIBRIL_SETUP_MODE) with no setup_complete marker in the data dir serves
  ONLY a loopback setup page (generate material, paste a PEM pair which is
  validated before writing, or an explicit continue-without-TLS) while the
  broker listener stays down, so no plaintext traffic can precede the
  choice. The choice persists as <data_dir>/config-overlay.toml, layered
  below explicit file/env config (explicit tls settings always win), plus
  the marker. The broker then starts in the same process (nothing else had
  started, so re-arm is falling through). Deleting the marker re-arms
  setup. With tls already configured explicitly, setup marks itself
  complete and boots through. The normal-mode dashboard shows the TLS
  state (source, CA fingerprint, admin coverage, enable guide) in the
  startup summary. Post-setup changes go through config or fibrilctl, not
  the dashboard - re-running setup means deleting the marker.
- Misconfiguration is loud and guided on BOTH sides: server logs a mini
  guide (admin board link, fibrilctl command, the concrete steps), and the
  client surfaces a SOLID TYPED ERROR from connect (not a log line)
  carrying the same guidance. Mechanism (as built in brick 2): the frame
  header has NO magic (the per-message FHL1-style magics sit inside the
  payload), so the accept path sniffs the first bytes of every connection.
  A TLS ClientHello starts 0x16 0x03 and a plaintext first frame starts
  with two zero length bytes. Without the sniff a ClientHello read by the
  plaintext codec parses as a ~369MB frame length and hangs silently. A
  TLS listener that sniffs a plaintext frame replies with a plaintext
  error frame, code 426 ERR_TLS_REQUIRED, echoing the sniffed HELLO's
  request id so even pre-TLS clients surface it through their pending
  request instead of a bare disconnect. The reverse direction (TLS client
  on a plaintext listener) closes fast and has no in-band channel, so the
  client types it from the handshake-EOF heuristic as a probable cause.
- Client error taxonomy (brick 3, user-directed): keep "TLS is not set up
  or mismatched" SEPARATE from other TLS errors. Three classes with
  different fixes: transport mismatch (426 definitive, or handshake-EOF
  probable - fix is config on one side), certificate verification
  (untrusted CA, name mismatch, expired - fix is ca_path or fingerprint
  pin), and other handshake/IO errors. Never collapse them into one
  variant.

Implementation bricks, in order (commit per brick, docs-currency in the
same change, settings discipline for every knob):
1. DONE (2026-07-04). Config surface: tls section (enabled, cert/key
   paths, auto_self_signed, admin_enabled override), guided validation,
   env overrides, example TOML + configuration.md + TlsMode accessor.
   Cert reload/rotation stays a follow-up, not this arc.
2. DONE (2026-07-04). Broker listener over rustls: handle_connection is
   generic over the stream so TLS composes above the fibril_util::net
   seam with no per-site cfg (simulation still compiles and passes).
   fibril_util::sniff holds the first-bytes classifier + PrefixedStream
   replay. fibril::tls builds the acceptor (BYO PEMs or rcgen generation
   under <data_dir>/tls with 0600 keys, SANs = localhost set + advertise
   hosts, CA fingerprint logged each boot, partial material refused).
   Integration tests cover the TLS HELLO round trip, the 426 reply, and
   fast-fail on the reverse mismatch.
3. DONE (2026-07-04). Rust client: ClientOptions tls() / tls_ca_path /
   tls_ca_fingerprint / tls_server_name over a MaybeTlsStream at the two
   stream-construction sites (the engine was already generic). Typed
   errors per the taxonomy: TlsRequiredByBroker (426, definitive),
   TlsNotSupportedByBroker (handshake EOF, probable),
   TlsCertificateUntrusted, TlsConfig, TlsHandshake - all DoNotRetry.
   Fingerprint pin verifies any cert in the presented chain (the broker's
   generated mode serves leaf + CA for this) while still verifying
   handshake signatures. Integration tests run the whole taxonomy against
   a real TLS broker. Gotcha recorded: run_server_from_config always
   requires broker auth (fibril/fibril static handler), unauthenticated
   test clients get dropped after HELLO.
4. DONE (2026-07-04). TS client (node:tls at the one openSocket factory,
   TLSSocket extends Socket so the engine is untouched) and Python client
   (ssl context in _open_connection), same option names and the same
   five-error taxonomy, all mapped from each platform's native errors. Pin
   verification walks the presented chain (TS via issuerCertificate links,
   Python via get_unverified_chain on 3.13+ with a documented leaf-only
   fallback below that). Tests mint throwaway certs with the openssl
   binary at run time, so no certificate material is committed. Wire
   vectors unaffected (TLS sits below framing).
5. DONE (2026-07-04): fibrilctl cert generate/fingerprint (material
   module lives in crates/tls as fibril-tls, re-exported as fibril::tls),
   first-boot setup mode with the loopback setup page, the config
   overlay, and the dashboard TLS status line. As-built details in the
   setup UX bullet above.
6. DONE (2026-07-04). Admin server HTTPS: the dashboard serves HTTPS via
   axum-server (tls-rustls-no-provider) from the shared ServerTls
   rustls config, tls.admin_enabled = false keeps it plain HTTP for
   reverse-proxy setups. The dashboard carries basic-auth credentials, so
   it is covered by default rather than left plaintext next to an
   encrypted data plane. Integration tested in both modes.
7. Later bricks, not this arc: inter-broker replication and ganglion raft
   TLS, mTLS client auth (#114), cert rotation.


## Stream audits (2026-07-03, both done)

Findings live in the S section of PERF_AUDIT_HOT_PATHS.md (perf, SF1-SF6)
and STREAM_PARITY_AUDIT.md (parity, P1-P5). Perf outcome: no server-side
knee (3.2M records/s fan-out at p50 1ms once the bench client was split
across processes), SF1 chain batching measured neutral and parked on branch
perf-stream-batching. P3 lag gap-skip is FIXED (168b44d, contiguous
delivery with watermark re-attach). The concurrent-declare 500 race is FIXED
(keratin 32f5962, shared temp file in the kind marker write). A parametrized
stream fan-out stress suite now mirrors the queue stress conventions (10k
always, 100k/1M release-only, slow consumers and filters mixed in). P1, P2,
P4, and P5 are all closed: idle eviction via the stream runtime settings plus
an ephemeral flush dirty-gate, stream traffic in BrokerStats, shutdown cursor
flush, and lag observability in stream stats and the admin page. Remaining
stream deferrals live with the typed subscription-lifecycle surface (lag
events and policies).

## Release state (2026-07-03)

- v0.2.0 was re-cut on branch `recut-0.2.0` to include the whole perf
  campaign and the stream correctness fixes (the lag gap-skip was silent
  data loss, not worth shipping around). The /0.2 docs snapshot was
  regenerated from the current docs (including a restored /0.2 overview
  page, which the dist smoke checks) and the 0.2.0 changelog section was
  extended and re-dated. Publishing is: merge the branch to main
  (fast-forward), push all three repos plus `git push origin v0.2.0`.
  Reverting instead is: delete the branch, retag 0315a3a.
- Cross-repo: ganglion and keratin have changelogs now (Unreleased-only, no
  tags yet) but still need their own release.sh and version model so the
  overlord drives them. When each cuts its first tag, write a first-release
  baseline paragraph like fibril's 0.2.0 entry, mining
  archive/replication-sharding-plan/ (the worklog there tracks far more than
  replication) for what accumulated in each repo.
- Changelog policy: CHANGELOG.md is the single history record (also each
  GitHub release body). The roadmap stays forward-looking and the
  implemented-surface page stays the what-exists inventory. No parallel
  recently-landed lists.

## Site and docs polish (2026-07-03, all landed)

- Custom 404: a styled docs 404 page plus an nginx error_page directive in
  the site container (the default nginx page came from the missing
  directive, not a missing file).
- Landing version badge and footer read the workspace manifest version at
  build time (the Dockerfile stages Cargo.toml into the site build).
- Root docs overview lives at /overview with a Start Here sidebar entry, so
  latest has a docs landing next to the marketing page. The dist smoke test
  covers all three.

## Test and expiry notes (2026-07-03)

- delayed_publish_over_tcp_waits_until_not_before flaked once under a parallel
  full-suite run (passes solo and in 3 repeat full runs). Timing-sensitive
  deadline assertion, same class as the fixed follower-loop cancel race. Worth
  a determinism pass if it recurs in CI. Two relatives were fixed after the
  0.2.0 push surfaced them on the slow CI runners: the stream catalogue
  placement test read the watch view before the async apply landed (now
  asserts on the returned snapshot and waits on the watch), and the crash
  recovery soak asserted ack durability after a plain shutdown that never
  drains settles (now uses the graceful shutdown). Reproduce this class
  locally with taskset 2-cpu pinning before patching.
- Mass-expiry cost: the expiry worker now resolves expired offsets to delivery
  tags with one scan of the inflight records per pass. If a mass consumer
  die-off with a very large inflight set ever shows up as a pause, the lever
  is engine-side batching of the requeue appends, not parallelizing the scan.

- Dead-consumer delivery recovery: a failed delivery send removes the consumer
  and leaves the messages inflight until TTL expiry redelivers them. With
  per-consumer batch dispatch the blast radius is a whole poll batch, not one
  message. Immediate release-and-requeue on send failure would tighten
  recovery (the release_inflight_batch engine path already exists).

- Adaptive tunables direction (2026-07-03): prefer deriving operating points
  from measured behavior within configured bounds. Candidates in value order:
  keratin adaptive fsync floor (with the dev-only latency injection flag),
  coalesce windows keyed to measured round-trip plus a startup timer-floor
  probe, and an adaptive spawn_blocking offload threshold for replication
  encode/decode. Guardrails: hysteresis, bounded ranges with a pin override,
  derived values visible in the debug surface, bench/DST validation.
  Window budget rule: a coalesce window should stay at or under about 10
  percent of expected overall latency (20 percent as the stretch bound).
  First review candidate: the 500us settle coalesce window is now 12-25
  percent of the improved 2-4ms delivery p50.

- Slow-storage levers (2026-07-03, from the SATA bracket data): (1) assess
  merging the msg-log and event-log durability legs into one commit, two
  barriers per confirm become one with no semantic change, the strongest
  slow-drive win on merit; (2) speculative queue delivery as a per-queue
  opt-in tier (deliver from staged state before the enqueue fsync, ghosts
  possible on crash, producer confirms unaffected), design it together with
  the API freeze (#111) and mirror the existing stream speculative tier.
  The roadmap's async replication fsync idea belongs to the same family and
  pairs with the replica-durable re-bench. Re-bench done 2026-07-03: 100k/s
  held at RF 2 with deliver p50 8ms, the June 45k/s-at-204ms verdict is
  obsolete. Remaining: a confirmed-publish cluster run and the rare ~175ms
  follower-apply stalls behind the p99 tail.

- benchmarks.md overhaul once the perf arc settles: replace the informal
  250k+ figures with the measured post-audit numbers (paced 500k/s at 1KB
  with p50 under 10ms on fast storage, knee 500-600k), a storage-class table
  (tmpfs/NVMe-class vs SATA vs slow-VPS with the fsync floors), the zero-loss
  saturation results, and the honest cluster replica-durable state. Method
  notes: bench-matrix scenarios, run counts, drive-variance caveat. Include
  the RSS story prominently: 40-50MiB serving 50k/s and 60-90MiB at
  150-250k/s is an understated strength against JVM-class brokers at
  moderate loads, which are the common case.
  User-centric scenarios measured 2026-07-03 (tmpfs, 32-core box), publish
  alongside the saturation numbers since most deployments live here: idle
  broker 0.0% CPU / 20MiB RSS, 4 durable stream readers at 100/s confirmed
  = sub-ms p50 delivery and confirm at ~3% of one core / 29MiB, 100 fan-out
  readers at 1k/s publish (100k del/s) = p50 4ms at ~0.6 core / 57MiB,
  confirmed work queue at 1k-10k/s = p50 3-4ms confirm / ~28MiB. One wart:
  an idle EPHEMERAL stream costs ~1.1% of a core (the 5ms flush ticker runs
  with nothing dirty), evidence attached to the stream idle-eviction task.
  Stream fan-out headline: 3.2M records/s (1KB, 32 readers) at p50 1ms.
  Comparables policy: loopback plus tmpfs numbers are not comparable to
  vendor benchmarks on real networks and disks (3.2M x 1KB is ~26Gbit/s,
  past a 10GbE NIC), so publish reproducible method + machine + caveats and
  avoid competitive claims until a same-box head-to-head harness exists
  (nats bench, kafka-producer-perf-test, RabbitMQ PerfTest as candidates).

Consolidated open items, extracted before the replication-effort working docs
were archived so nothing is lost. Full detail and rationale live in
`archive/replication-sharding-plan/` (the worklog, replication planning, and
design notes). Audit follow-ups live in [AUDITS.md](AUDITS.md), the audit status
board, and are not duplicated here.

Source tags: `[WL]` worklog, `[PLAN]` replication planning, `[DN]` design notes,
`[MEM]` memory, `[RACE]` race-windows, `[AUDIT]` audit board, `[AUTHOR]` author note.
Tiers are grouped by concern, not strictly ordered.

## #97 deterministic simulation - status

DONE: net seam (`fibril_util::net`, tokio normally / turmoil under `simulation`),
protocol + client crates converted and building both modes. Multi-broker turmoil
harness in `crates/protocol/tests/simulation_tests.rs` (compiled only under
`--features simulation`), with three deterministic tests: (1) broker runs inside a
turmoil host (no-net smoke - construction, keratin disk I/O, publish-confirm on the
simulated clock); (2) a follower catches up to the owner over the simulated
network via its supervised watcher; (3) owner partitioned away -> follower promotes
under a fenced epoch bump and serves a fresh publish, with the promoted log = the
replicated tails + 1 (no loss). Key constraint learned + documented: turmoil gives
each host its OWN current-thread runtime + LocalSet, so a broker must be built
INSIDE its host closure and can only be driven from there - cross-host
orchestration goes via the simulated network or shared atomics (the failover test
steps the sim and injects the partition from the main thread once a shared
caught-up flag is set). A fourth test stands up a 3-node ganglion raft cluster
inside turmoil (leader election + replicated committed write over the simulated
network). Run: `cargo test -p fibril-protocol --features simulation --test
simulation_tests`. Dev note "Deterministic simulation testing" stages 3, 4, 4a
marked done.

CROSS-REPO DONE (ganglion, committed, UNPUSHED): the raft TCP transport is now
injectable over a `RaftDialer` (TokioDialer = production, TcpNetworkFactory /
TcpRaftConnection kept as aliases so callers are unchanged); `serve_connection` +
frame codec are generic over the stream and `serve_connection` is pub - so a
caller runs its own accept loop over any transport. ganglion takes NO turmoil dep:
the TurmoilDialer lives in fibril test code. Also fixed a pre-existing timing-race
flake in ganglion's `learner_joins_catches_up_and_gets_promoted` (read-after-write
+ stale-leader-id), and re-exported `CoordinationSnapshot` from the ganglion
umbrella. fibril sim test binds through the `ganglion` umbrella crate, not
ganglion-openraft directly.

SPLIT-BRAIN DONE: `ganglion_returning_old_owner_is_demoted_under_simulated_partition`
runs 3 ganglion raft nodes inside turmoil (a-owner + b-follower carry brokers,
coordinator is raft-only for majority), partitions the old owner, the majority's
leader-only controller reassigns under a bumped epoch, the follower promotes, and
on heal the old owner observes its demotion and refuses publishes. Two integration
lessons baked into the test: (1) each turmoil host shares ONE current-thread
runtime across its broker + raft node, so a busy broker starves raft heartbeats and
replication serving - the old owner is kept idle through catch-up + partition, and
raft uses widened election timeouts (heartbeat 200ms, election 1000-2000ms).
(2) The controller keeps all coordination WRITES on the leader (register +
control_iteration only when is_leader, and only when state must change) so it never
hits the follower-forward path - `client_write_remote` still dials via TokioDialer,
so cross-host forwarding from a non-leader/non-member is NOT sim-compatible yet
(members forward internally over the RaftNetwork, which is on the dialer). Making
client_write_remote dialer-generic is the remaining gap if a future scenario needs
follower-forwarded writes under sim.

SCENARIO SET (8 green, deterministic): smoke, static-coordination catch-up,
static-coordination failover, raft-cluster-over-turmoil, split-brain,
lossy-link catch-up (flapping follower), raft-cluster-converges-under-message-loss,
durable-publish-unconfirmed-while-replica-partitioned (ReplicaDurable 2-node: no
false durability ack while the replica is partitioned). The lossy ones use turmoil
fail_rate/repair_rate + latency bounds + a fixed rng_seed; loss/latency are kept
under the raft timers (heartbeat 200ms, election 1000-2000ms) so a majority stays
connected. Gotcha baked in: start_split_brain_node HARDCODES membership addrs
a-owner/b-follower/coordinator, so any cluster test reusing it must name its turmoil
hosts those exact names.

FINDING (surfaced by the durable scenario) - FIXED: the follower replication client
had NO client-side timeout, so a partition that DROPS an in-flight read response or
a connect SYN (turmoil partition, not an RST) left the worker on a dead connection
until the transport itself broke. Fixed in protocol replication.rs: a read waits at
most its long-poll window + DEFAULT_READ_TIMEOUT_SLACK_MS (10s), and connection
setup (TCP connect + HELLO/AUTH) at most DEFAULT_OWNER_CONNECT_TIMEOUT_MS (5s); on
either the worker errors, drops the conn, and redials. The durable scenario now
asserts recovery-after-heal too. (Both are named const defaults with builder
overrides; full config-crate/runtime-settings wiring is a minor follow-up.)

SCENARIO SET COMPLETE (11 green, deterministic): the 8 above plus
follower_installs_checkpoint_after_owner_truncates (owner truncates past the
follower start via the new StromaEngine::truncate_messages_before, follower
installs the owner checkpoint to reach the tail) and
repartition_cutover_waits_for_delayed_topology_ack (real client acks over the sim
net; turmoil hold/release delays the topology exchange so the adoption fence holds
below the new generation, then finalizes on release - note: use hold/release, NOT
partition_oneway, for delaying a live TCP stream, since one-way drop desyncs the
byte stream with no retransmit). #97 can be considered functionally done as an
evaluation+harness; madsim remains the documented later escalation only if
scheduling-order determinism is ever needed.

NEXT (post-#97): the read/connect timeout slack is now config-driven - DONE
(ReplicationSettings seed + ReplicationRuntimeSettings -> BrokerConfig ->
ProtocolOwnerPeerResolverConfig::with_timeouts -> peer; documented in
configuration.md; protocol consts remain the defaults). Captured at peer
construction (consistent with auth/client peer config), not per-read-live, so a
runtime change applies when the peer is next rebuilt - acceptable for connection
timeouts.

CHAOS/SOAK (#115) DONE (first suite): crates/broker/tests/soak.rs - real broker,
real wall-clock, real fsync. (1) durable_crash_recovery_soak restarts the engine
from disk across cycles, asserts every confirmed message survives + delivered
exactly once + strictly increasing offsets + settled (acked) messages never
redelivered after restart. (2) concurrent_load_no_loss_soak runs N producers +
a consumer for a wall-clock window, asserts every confirmed publish consumed
exactly once (no dup offsets). CI-small defaults; FIBRIL_SOAK_CYCLES/BATCH/SECS/
PRODUCERS scale to a real soak.

REAL MULTI-NODE RUN (#116) DONE - and it already existed: scripts/cluster-tryout.sh
stands up N real fibril-server PROCESSES forming one Ganglion raft cluster over
real TCP. `--failover-verify` runs an identity-tagged producer/consumer through
public client routing, kills the partition owner mid-run, and asserts every
confirmed id survives failover (zero loss, no phantoms); `--chaos` repeats mixed
faults (pause/resume, kill/rejoin) under sustained load and asserts zero loss +
reconvergence. Both executed green (3-node, failover-verify 3000 msgs @3000/s
zero-loss; chaos 2 rounds zero-loss + reconverged). Lesson: this harness predated
my work - reuse it, do NOT add a new cluster script (a redundant cluster_validation.sh
was written then deleted). The dedicated verifier bin is fibril-benches::failover_verify.

CLUSTER-CONFIDENCE GATE (#124) - all three legs now hold: deterministic simulation
(11 turmoil scenarios), chaos/soak suite (#115), and a real multi-node run (#116
via cluster-tryout). The multi-broker chaos-under-load variant the soak-growth note
wanted is exactly cluster-tryout --chaos, so that follow-up is satisfied too.

With the DST harness proven across election, replication, failover,
split-brain, and lossy networks, the cluster-confidence gate (#124) has a real
deterministic base to build on.

- Stream/staging perf levers from the staging-efficiency audit. DONE: removed the
  per-publish replication-cache clone (cache removed entirely, keratin 27940f8) and
  the per-record fan-out round-trip in the stream drain (fibril 115b370, +~23%
  ephemeral single-partition). OPEN: (a) keratin encode_record builds the fixed
  32-byte record header via 8 extend_from_slice calls - pack into a stack [u8;32] +
  one extend (micro-opt). (b) BIG: the payload is memcpied into write_buf before the
  single write(); a vectored write (writev of [header, headers, payload, crc] with
  CRC fed incrementally) would avoid the copy - real lever for large payloads, but a
  substantial write-path rewrite (partial writes, segment rolls). (c) durable
  single-partition throughput is noisy run-to-run (~196k-245k at 300k offered, 1KB);
  worth tracing what causes the variance (fsync batching cadence? scheduling?).
  Client-side: a single reader's multi-partition fan-in tops out ~260k records/s -
  its own bottleneck, separate from the broker. See tasks #61/#62/#65 for the topic
  routing + Arc<str> interning follow-ups.

- Stream (fan-out) filter performance + filtering expansion (task #129).
  PHASE 1 ASSESSED 2026-06-29 (microbench `crates/broker/benches/stream_filter.rs`,
  Ryzen 5950X): the common cases are already optimal - an empty/no filter
  short-circuits at ~3ns and patterns are precompiled at subscribe time
  (`WildcardPattern`), with an alloc-free `matches`. A filtered match costs ~22ns
  for 1 clause and ~57ns for 3, dominated by the `HashMap<String,String>` header
  KEY lookup (~18ns), not the wildcard match (exact 4ns / glob 7ns / multi-seg
  21ns). The remaining lever is the redundant per-subscriber eval when many
  subscribers SHARE one filter: it is linear, ~39ns/sub, so a single 2-clause
  filter across 4096 subscribers is ~160us/record of filter eval. Two phase-2
  levers, both justified by these baselines: (a) evaluate once per distinct
  filter per record - cleanest via interning filters as `Arc` and grouping
  subscribers by Arc identity (note: `try_send` per matched subscriber likely
  dominates the loop, so dedup saves filter-eval, not the whole per-sub cost); (b)
  a faster header-key lookup (the SipHash key hashing is the per-clause cost).
  PHASE 2: expand the filtering vocabulary (OR groups, negation, value ranges,
  header-set membership) while staying declarative and bounded - NOT
  content-routing scripting, which is out of scope (see roadmap). The bench is the
  regression baseline for both.

This file tracks the replication and clustering roadmap leftovers. Non-replication
feature ideas live in their own track, summarized at the end.

Idea backlog (pick from these): express lane / speculative delivery + deferred
publisher confirm (the ghost-flag pattern); in-memory non-durable queues
(pluggable keratin write target); producer dedup / max-unconfirmed-per-publisher;
client embedded-retry (is_retryable + retry handle); documented failure semantics
/ operator runbook; crash-recover "leftover inflight without message" smell;
split-brain epoch-fence test; multi-broker-same-storage must-fail test; #97 DST
simulation. Inflight persistence is confirmed (encode_snapshot writes inflight
(offset, deadline) pairs, load_snapshot restores them, MarkInflight events
replay) so leased-unacked survives crash/restart.

## Done since the inventory was last curated (2026-06-22)

The `implemented-surface.md` inventory was recurated on 2026-06-22 and now
reflects everything below. Keep this list empty-ish: fold new surface into the
inventory as it lands (see the docs-currency directive in the Docs section).

- (curated into the inventory 2026-06-22) Admin create-queue, delete-queue
  (single-node), hide-inactive toggle + search on the queues page.
- (curated 2026-06-22) Message TTL: per-message `ttl_ms` + per-queue
  `default_message_ttl_ms`, DLQ-routed expired drop (`DeadLetterReason::Expired`),
  client `expiring` publisher + `default_message_ttl`. Rust + TypeScript.
- Topology page glowup: adaptive ellipse/staggered-grid layout, click-to-inspect
  broker panel, and a Diagram/List view toggle.
- Admin SPA-feel: boosted navigation, vanilla, no framework.
- Admin node management from the topology page: repartition plus add/remove
  coordination voting member controls.
- CI guard: inline template-JS syntax check (`scripts/check-template-js.sh`,
  wired into `scripts/check.sh`).

## Correctness and durability

- FIXED 2026-06-28 (found via the cluster tryout after the admin error-swallow fix):
  the admin /admin/api/queues endpoint 500d on a node hosting a Plexus stream, and
  the expiry worker logged "queue actor is gone" (broker.rs:3842) every poll. The
  earlier owner->follower-demotion hypothesis was WRONG. Real cause: the work-queue
  sweeps (get_queues_stats, collect_expired, next_expiry_hint, the TTL-drop sweep)
  iterated ALL materialized handles, including stream partitions. A stream runs a
  StreamEngine with no work-queue command actor, so a status report or lease-expiry
  collection routed to one failed with "Status report failed: channel closed", and
  one such failure aborted the whole sweep. Leader-specific only because that node
  happened to host the stream. Fixes shipped: (a) per-queue tolerance - get_queues_stats
  reports a tagged Ok/Error enum per queue (keratin 9ad589c, fibril a331b35) and the
  admin route returns the error instead of swallowing it to {} (fibril 86b82dc);
  (b) ROOT CAUSE - exclude stream-kind partitions from the work-queue sweeps
  (keratin 4994353); (c) STRUCTURAL FIX (keratin 7404042, task #99): engine-specific
  ops now live on typed WorkQueueHandle/StreamHandle projections reachable only via
  as_work_queue()/as_stream(), so a work-queue command on a stream partition is a
  compile error, not a runtime channel failure. The runtime kind guards in the
  sweeps were replaced by the projection, and the two remaining latent siblings
  (the TTL-drop sweep and validate, which iterated all partitions and only guarded
  on role) are fixed by the same change.

- Plexus stream routing after failover relies on the per-partition `.kind` marker
  being present on the new owner. The marker is a LOCAL file written at
  `create_stream` / queue `declare` time on whichever broker first declared the
  channel, and it is not replicated with the log. Two consequences on a failover to
  a node that never ran the declare: (1) a stream publish can fall down the queue
  path until the stream channel is opened there, and (2) the same-topic kind guard
  and the materialize-by-marker path default to Queue when the marker is absent, so
  a node could in principle materialize a stream partition as a queue (a MIXED-kind
  topic across nodes). Same-node mixing is already prevented (the declare guards key
  off the marker and both declares iterate from partition 0, so a conflict aborts
  before any partition of the other kind is created). The cluster fix is to make
  kind durable/replicated: replicate the marker, or persist kind in the replicated
  partition state, or have owner-activation/catalogue-sync open the correct engine
  for owned partitions. Until then, declaring the channel against the new owner
  re-materializes it. Low risk in single-node / declare-before-publish flows.

- DECIDED 2026-06-29 (task #101): cold-restart orphaned on-disk partitions. On a
  cold restart a partition the node was disowned of while down is indexed as an
  unmaterialized slot but the empty assignment cache makes the planner compute
  Noop, so the on-disk data is never materialized and never cleaned. Decision:
  leave it as inert cold storage, do NOT auto-destroy on restart. It is already
  safe - serving is ownership-gated (ensure_queue_owner runs before
  materialize_owned_queue), so an orphaned partition is never served and never
  mis-materialized (regression tests cold_restart_disowned_partition_is_inert_
  cold_storage + orphan_reconciliation_ignores_partitions_unknown_to_coordination).
  Retained rather than reclaimed because a later re-acquisition reuses the on-disk
  log (fast failover-back without re-replication) and the startup snapshot can lag
  a reassignment about to hand the partition back. `Broker::orphaned_on_disk_
  partitions` surfaces the set (logged once at startup) for operator visibility.
  Two follow-ups left open: (a) opt-in reclaim - an admin action or a setting-gated
  startup purge of partitions coordination has provably reassigned elsewhere, so
  disk does not leak forever (Kafka-style stray-log move-aside is the safer shape
  than immediate delete); (b) a deeper hazard, separate from this item: if a node
  RE-ACQUIRES ownership directly from cold (BecomeOwner, not via a caught-up
  follower) it would materialize its STALE local log and could serve messages the
  current owner accepted while it was down. Re-acquisition should catch up from the
  authoritative owner (or epoch-fence the stale tail) before serving. Needs a test
  and likely a become-follower-first-then-promote path on cold re-ownership.
  This cold-storage decision is the partition-side counterpart of the broader
  vision (roadmap, longer term): durable broker restart reconciliation that
  extends the live-process graceful reconnect across a process restart, so a
  client within its grace window continues transparently and rolling upgrades
  become trivial.

- Durable stream throughput RESOLVED (was ~847/s). The earlier pipelining was
  correct but the per-channel ingest awaited the staged offset between appends
  (append_stream_records_batch), so keratin's writer never held more than one append
  at a time and its fsync coalescing could not engage: one fsync per record. The fix
  (keratin afb507f, fibril 588e51c) is append_stream_records_enqueue, which returns
  before stage (housekeeping moves to a background task), so the ingest enqueues the
  next batch without waiting for keratin to report back and keratin coalesces fsyncs
  across the queued appends. Durable now reaches line rate (~197k records/s at 1KB,
  ~14ms confirm) on SSD; SSD ~= tmpfs because the fsync is amortized. A follow-up
  (keratin a430df3, fibril 4058b3f) routes ALL tiers through the same batched ingest
  (drain sets fan-out/confirm timing by tier), so speculative now matches durable
  (~190k/s) and ephemeral keeps the lowest latency (~1ms deliver). Bench via
  scripts/bench-stream.sh (DURABILITY + DATA_DIR knobs). Open: ephemeral throughput
  in confirmed mode is lower than speculative despite no fsync - a client-side churn
  artifact of confirming at stage (very fast confirms -> high re-publish churn), not
  a server limit (1ms delivery). Also: light-load confirm/deliver latency is the
  keratin fsync interval, config-tunable, separate from this.

- Ephemeral writeback tail RESOLVED (keratin db92777 + fibril da35400). It had a
  fat delivery tail on real disk despite the best p50 (SSD, 1KB, 150k/s: p50=1ms
  but p95~530ms) because AfterWrite never fsyncs, so dirty pages piled up until the
  kernel throttled the writer mid-write. Confirmed by tmpfs (no writeback) having no
  tail. Fix: a per-ephemeral-channel periodic flush task (5ms) calls Stroma::
  sync_stream, and WriterCmd::Sync now hands the fsync to keratin's fsync WORKER
  stage (carries the responder through FsyncReq/FsyncDone) instead of running it on
  the writer thread, so it drains dirty pages without stalling staging. Ephemeral is
  now p50=1ms / p95=2ms / p99=7ms at full line rate, beating tmpfs, lowest RSS, and
  stays the lighter tier (still AfterWrite, no per-record fsync). The 5ms interval is
  a broker-local const (EPHEMERAL_FLUSH_INTERVAL) like the ring capacities; promote
  to a runtime setting if it needs tuning in the field.

- Possible future channel mode: a true memory-only stream (no log at all, lost on
  restart, no durable cursors/replay/retention, lowest possible latency). Distinct
  from the `ephemeral` durability tier, which is defined as log-backed (persist
  async, no fsync, do not gate). Would be a separate channel flag or a 4th mode,
  not a redefinition of `ephemeral`. The writeback finding above strengthens the
  case: a memory-only mode sidesteps writeback entirely. Examine when there is a
  real need.

- Offsets are an unstable internal storage detail, not a stable consumer-facing
  identity (Fibril follows work-queue semantics, not a replayable-log model). The
  clients deliberately do NOT expose starting a stream subscription at a raw offset
  (resume is via a durable cursor name). The wire `StreamStart::Offset` variant is
  kept for internal/ops use only. Open question the user raised: also make the
  delivered-message `.offset` accessor less prominent or remove it across both
  channel types (it is currently exposed on Message in all three clients). Confirm
  before removing, since queue consumers may read it for logging.

- FIXED (found by the chaos soak): a consumer stopped receiving and never caught up
  after the OWNER broker was killed and rejoined WITHOUT a failover (a bounce faster
  than failure detection, so committed ownership stayed on the same broker). It was
  never data loss (confirmed stayed durable, zero phantoms/dups) - the consumer's
  subscription to that owner just did not re-establish after the owner's restart.
  Two-part client fix: (1) `start_engine` now reconciles active subscriptions on
  ANY reconnect, not only a `Resumed` session, so a fresh broker session restores
  or closes them; (2) the subscription supervisor proactively re-dials the bound
  owner's connection on its owner-check even when the owner endpoint is unchanged,
  because nothing else reconnects a passive subscription's connection. Confirmed:
  the 20-round chaos that lost ~70% now passes 4/4 with zero loss, plus a
  deterministic unit regression (`reconnect_reconciles_subscriptions_when_resume_rejected`).
- Idempotent producer dedup: broker reads `fibril.client.producer_id`/`seq` for
  effectively-once delivery (the headers are already on the wire). The one
  success criterion left genuinely not done. [WL/DN/PLAN phase 8]
- Split-brain: believed addressed by epoch fencing in Keratin plus the Stroma
  freeze. Verify, and add adversarial tests for the reappearing-stale-owner case
  (reject any write or replicate whose epoch is below the local partition epoch). [PLAN]
- Durable queue role (Stroma hardening, defense-in-depth for the ownership gate):
  a Stroma queue defaults to `QueueRole::Owner` on create/recovery and the role
  is in-memory only (`stroma/core/src/state.rs`). So a Frozen/Follower queue
  loses that role on eviction or restart and re-materializes as Owner, which can
  resurface a stale owner accepting writes if the broker gate is bypassed or
  diverges. Two layers found while fixing
  `ganglion_returning_old_owner_is_demoted_and_refuses_publishes`:
  (A) the broker gate/watcher divergence - FIXED (broker `locally_owned` reconcile,
  commit aea4d50, demotes a de-facto owner even with no observed BecomeOwner);
  (B) this engine-role durability gap - OPEN. Lighter increment: pin a
  frozen-for-transition queue against eviction (survives eviction, not restart).
  Robust fix: persist the role (or "not owner") so recovery restores a non-owner
  state and ownership is always coordination's decision, never a default (covers
  eviction AND restart). The gate masks this in normal operation, so it is
  defense-in-depth. [AUTHOR]
- Ex-owner rejoins the cluster after losing privileges while its replicas were
  not fully caught up and its data was not shared: define and handle the
  mechanics (ties to epoch fencing plus recovery verification). [WL]
- Ensure follower queues are materialized in memory on demand. [WL]
- Low priority: verify snapshot replay strictly begins at the offset after the
  snapshot (believed done via `recovery_replays_only_events_after_snapshot_offset`). [WL]
- `[RACE]` STALE, needs re-verification against current code: ack versus
  redelivery-worker idempotency (a snapshot `list_expired` can race an ack). The
  delivery-tag epoch work may already cover this. Confirm or add a generation or
  is-acked guard before requeue. [RACE Race 2]

## Performance and scale

- Arc<str> + soft interning for topic/group (#65): ASSESSED 2026-06-28, NOT WORTH
  IT - do not pursue the broad refactor. Findings: the per-message hot path
  already allocates the topic exactly once (publish decodes `reader.str()?
  .to_owned()`) and routing is alloc-free (`slot_lookup_no_alloc` takes `&str`),
  so there is no repeated-clone pattern for Arc<str> to optimize - the many
  topic `.to_string()` / `.clone()` sites in broker.rs are cold (admin, topology,
  list, assignment events). A microbench (50 topics, 5M ops) showed interning is
  ~3x SLOWER than the current short-string alloc even with no lock contention
  (String::to_owned 7.8 ns/op; Mutex<HashMap> intern 24.5; RwLock read+clone
  24.4) because you still materialize the str to hash it, then pay a map lookup +
  atomic refcount. Memory dedup is also small (topics <=128 B; duplicated topic
  strings across partitions are KB-scale unless partition counts are enormous).
  Cost would be large (60+ String->Arc<str> fields across keratin/fibril/protocol
  + wire). If extreme partition-count memory ever shows up in profiling, intern
  ONLY the cold registry/state storage, never the message path. Verdict: skip.

- Owner-side read and encode fan-out (shared tail, private catch-up) for RF >= 3. [WL]
- Parallel-fsync / async-fsync for replicated append (the top durable-replication
  perf lever; user-reconfirmed 2026-06-29). Concrete: keratin's replicated-append
  path (writer.rs `stage_replicated_req`) does a SYNCHRONOUS inline `log.fsync()`
  per batch, so on disk the writer blocks on every replicated fsync. Route it
  through the batcher + async fsync pipeline (`fsync_tx` / `drain_fsync_done`),
  mirroring the local ephemeral sync_stream rework, so replicated commit/event
  fsyncs coalesce off the hot path. Full analysis + bench numbers in
  REPLICATION_WORKLOG.md (search "stage_replicated_req" / "async-fsync"). [WL]
- Replication-lag backpressure hook in the append path (slow accept when
  followers lag). [PLAN]
- Replication streamed decode (decode while fetching and applying), separate
  payload and event replication streams, and a more push-focused replication
  architecture. [WL]
- Low priority: make `BLOCKING_DECODE_BYTES` adaptive instead of a CPU-tuned
  const (piecewise decode-time model, startup calibration first). Details in the
  code comment. [WL]

## Operability and quality of life

- Broker advertise address, separate from the bind address - MOSTLY DONE (#94).
  Was: the broker registered `config.broker.listener.bind` as its endpoint, so in
  a container (`0.0.0.0:9876`) peers/clients could not dial it back. Built:
  `broker.listener.advertise: Vec<String>` + `FIBRIL_BROKER_ADVERTISE` (config
  crate), with a zero-config default derived from the coordination peer host (our
  own `FIBRIL_COORDINATION_PEERS` entry) + the broker port, falling back to the
  bind. The node table holds the primary (first) endpoint as a connectable String
  (service names survive - the SocketAddr parse that dropped them is gone), and the
  full priority list rides a per-node heartbeat label (`fibril/advertise`), so
  `owner_endpoints` is an ORDERED LIST of `AdvertisedAddress{host,port,tags}` on the
  wire (Queue/Stream topology + Redirect). All three clients decode the list; the
  TUI, TS, and Python clients connect by the first endpoint (by name, so service
  names work). Containerized routing + replication now work.
  - REMAINING (b3c): the high-level Rust client's connection pool is keyed by
    `SocketAddr`, so it takes the first owner address that PARSES as one and
    logs+skips a service-name address (`first_socket_endpoint`). Making it connect
    by string and try the list in order (the real probe) is a focused
    connection-manager refactor (pool key + `engine_slot` + bootstrap + prune +
    `EngineSlot` connect/reconnect, ~25 `SocketAddr` sites). Until then a Rust app
    using the high-level client against a service-name-advertising cluster routes
    via bootstrap, not owners. The TUI/TS/Python paths already connect by name.
  - try-in-order client behavior (all clients currently use the FIRST endpoint, not
    a true connect-probe): give the per-attempt connect a short timeout so a
    black-hole first entry does not stall the fall-through. Tags ride the wire
    unused, for future selection conventions.
  - Heavier evolution if try-in-order's connect-failure latency ever bites:
    Kafka-style named bind listeners with selection by the listener the client
    bootstrapped through. More moving parts; the ordered-list probe is the lighter
    cut and the `AdvertisedAddress{...tags}` shape is a clean stepping stone. [AUTHOR]
- Programmatic scale up and down: join (learner to voter to rebalance) and
  drain-and-leave via fibrilctl plus the admin API, autoscaler-drivable. [PLAN]
- Consumer assignment push and client fan-in narrowing: today a cohort client
  fans in to all partitions of a queue and the per-partition delivery gate
  enforces exclusivity. The deferred optimization is an assignment-change push
  (`Op::AssignmentChanged`) so a client only pulls from partitions it is
  assigned, with the gate staying as the correctness backstop. This also enables
  per-partition leave (today `leave` drops the whole connection subscription).
  [WL phase-2a limitation (c)] [MEM]
- Unclean-leader-election toggle, off by default. Minor. [PLAN]
- Settings tiering: basic, advanced, expert, with collapsible sections. [DN/WL]
- Settings presets, orthogonal to tiers: opinionated bundles such as low-latency,
  hands-off, and power-user. Tiers are how much you see, presets are what the
  defaults do. [AUTHOR]
- Relational settings nudges (soft warn, not reject): for example warn when a
  failover-sensitive timeout is set below the failure-detection cadence. Needs
  cross-setting advisories at config load and runtime PUT, plus an inline admin
  hint. Pairs with settings tiering. [AUTHOR]
- Eager opt-in startup recovery: `recover_all` exists but is unused (recovery is
  lazy via `queue_handle`). A config to eagerly recover all on-disk partitions at
  boot makes `recovery.on_mismatch = refuse` a literal refuse-to-start. Lazy
  stays the default. [AUTHOR]
- Snapshot cadence: wire `snap_cfg.every_events` as an additional knob alongside
  the time and dirty triggers. The gate is commented out in
  `periodic_snapshot_step`, `last_snapshot_event_offset` is already tracked, so
  wiring is low-risk. Currently `#[allow(dead_code)]` with a FIXME. [AUTHOR]
- Onboarding and easy trial - LARGELY DONE, verified end to end 2026-06-28. The
  one-command path exists and works: `ghcr.io/axmouth/fibril-server` is public
  (anonymous `docker pull` confirmed), `compose.cluster.example.yaml` brings up a
  three-broker ganglion cluster with service-DNS peers, a healthcheck-gated
  one-shot seeder (demo queues + a stream), and each admin dashboard exposed, and
  `website/public/tryout.sh` (served at `fibril.sh/tryout.sh`, documented in
  `concepts/clustering.md`) is the curl-to-shell bootstrap. The quickstart page
  covers single-broker `docker run` and links the under-a-minute cluster tryout.
  Verified: cluster forms (healthy consensus, leader elected), seed declares
  orders/payments/emails + the events stream, and `/admin/api/topology` shows
  partition ownership spread across the three brokers.
  - REMAINING (genuine): in-memory (non-durable) mode for an even lighter trial
    (no data volume, lost on restart). This is really a storage feature (a
    pluggable Keratin write target / memory-only tier), larger than onboarding -
    see the "In-memory (non-durable) queues" item in the non-replication track.
  - The leader `/admin/api/queues` returning `{}` after seeding, noticed during the
    smoke test, was NOT lazy materialization - it was the stream-in-work-queue-sweep
    bug, now FIXED (see the FIXED entry under "Correctness and durability"). Not a
    trial-path issue.
  [WL/AUTHOR]
- Admin dashboard: a lost-connection banner. When the admin page can no longer
  reach its broker (broker down, failover, network blip), show a clear banner
  instead of silently stale data. [AUTHOR]
### Queue lifecycle, retention, expiry (ordered plan)

Small user-facing features, ordered cheapest-and-highest-visibility first. The
first three are admin-thin (the primitive exists, just expose it).

1. **Admin create-queue** - DONE first cut (commit f2d91b7). POST
   /admin/api/queues declares partitions 0..count locally with an optional DLQ
   policy + max retries, plus a create form on the queues page. PARITY TODO: go
   through `declare_partitioning` for an authoritative count + conflict detection
   (multi-partition cluster currently relies on the catalogue-sync loop to
   register the declared partitions).
2. **Queue purge.** RE-SCOPED to M after digging in: "purge is reset" was
   optimistic. `QueueCommand::Reset` / `state.reset()` only re-inits the in-memory
   consumption state (offsets/acks/inflight) - it does NOT drop log messages and
   does NOT replicate (messages re-present after it). `StromaEvent::ResetQueue`
   exists and is applied/decodable but is only EMITTED in tests (unwired
   scaffolding). A real purge = truncate the message log to head (the
   `message truncate ... before` primitive in `stroma.rs` exists) + reset state +
   emit a replicated `ResetQueue` event, plumbed Stroma -> engine -> admin, with
   replication correctness. Cross-crate, so do after the genuinely-thin items.
3. **Hide-inactive-queues toggle + basic search** on the queues page - DONE.
   Frontend-only filter bar (search by topic/group, hide queues with no active
   publishers/subscribers); summary cards stay full counts.
4. **Admin delete-queue** - WE WANT IT (the earlier "no delete yet" meant "we do
   not have it", not "skip it"). Split into two:
   - SINGLE-NODE - DONE. `POST /admin/api/queues/delete` (`delete_queue` handler +
     `DeleteQueueRequest{ tp, group, partition_count }` in `routes.rs`) loops
     `destroy_partition` over 0..count, mapping HasInflight -> 409 and other
     errors -> 500, gated off in cluster (`server.coordination.is_some()` ->
     501 "cluster_delete_unsupported"). UI: per-row Delete button on the queue
     agg row in `queues.html` (carries data-group/topic/partitions, confirm
     dialog, `apiPost` then refresh). Tests cover empty-topic -> 400 and a
     destroy-declared-partitions happy path.
   - MULTI-NODE (planned, M): a coordinated teardown - deregister from the
     coordination catalogue (so the controller stops placing it and the
     catalogue-sync loop stops re-registering it), destroy on ALL replicas (not
     just the clicked node), ordered so sync cannot resurrect it between steps.
     destroy_partition is only the local primitive - this is the real work.
5. **Time-based retention.** Primitive `safe_message_truncate_before` exists.
   Add a sparse worker: map time -> offset (crude binary search on the stored
   `published` is fine, no new index needed), truncate before the cutoff, and
   clear the relevant state entries. Per-queue retention config.
6. **Message TTL (drop by age).** GENUINELY NEW - do not conflate with the
   existing `expiry_heap` / `collect_expired`, which is the lease-timeout
   REDELIVERY path (inflight -> requeue, never ack), not age-drop. DESIGN (locked,
   traced against current code):
   - Fork resolved - do BOTH per-message and per-queue-default, collapsed to one
     absolute deadline at publish on the owner (broker = clock authority):
     `expire_at = per_message_ttl ?? queue_default_ttl ?? none`. Per-message =
     optional `ttl_ms` on the existing `Publish` op (no new op/frame). Per-queue
     default = `default_ttl_ms: Option<u64>` on `DeclareMeta` (declare-time).
   - State (stroma `state.rs`): a single `ttl_deadlines: RangeMap<Offset,
     Deadline>` keyed by OFFSET (quantized deadline). NOT a deadline-ordered heap.
     Keying by offset is the win: the mapping persists across
     ready->inflight->ready and is removed only on terminal settle, so there is no
     "bucket fired while inflight then requeued and never re-checked" bug, and no
     re-insert logic. RangeMap (not heap) because a TTL structure is bounded by
     the READY BACKLOG (every msg can carry a deadline), largest exactly when TTL
     matters (lagging consumers); RangeMap collapses contiguous same-deadline
     spans -> near-O(1) per burst for uniform/queue-default TTL. `ready` is already
     a RangeSet, so this is idiomatic. (Plain offset-keyed structure also answers
     reactive lookups; a deadline-keyed map/heap would need a reverse index.)
   - Two-tier check: (a) REACTIVE / correctness - `next_deliverable` + mark-inflight
     drop any offset with deadline <= now, so stale work is never delivered
     regardless of worker timing; (b) PROACTIVE / cleanup - the EXISTING
     expiry_worker (broker.rs `spawn_expiry_worker`) scans the front of
     ttl_deadlines each tick, dropping expired READY offsets bounded by
     `expiry_batch_max`. Hint piggybacks the front entry (exact for uniform TTL,
     else falls back to poll cadence). NEVER drop inflight.
   - Drop = a DURABLE settle (Ack-path event) - unlike lease-requeue and
     delayed-enqueue, which are derived/non-durable. v1 = discard; DLQ-on-expiry
     is a cheap fast-follow (`DeadLetterReason::Expired` + existing DLQ path).
   - Durability/replication/recovery: `expire_at` rides the `Enqueue` event
     (bump `STROMA_VER` 2->3) so followers + recovery rebuild ttl_deadlines.
     Snapshots (`encode_snapshot`/`load_snapshot`, bump `FORMAT_VERSION` 2->3)
     append the ttl_deadlines ranges, since snapshots compact away the Enqueue
     events. Quantization granularity = a delivery runtime setting (ceil to bucket
     so we never drop early); guarantee mirrors delayed-publish ("dropped within
     granularity + worker period after expiry").
   - Client: a `publisher.expiring(ttl)` builder (a SETTING, not a new type) that
     stamps a default per-publish ttl; ttl via the Delayable-style trait (bare
     number = seconds, or a Duration). Composable with delayed/reliable modes.
   - Brick order (each its own green commit): (1) `DeclareMeta.default_ttl_ms` +
     `Enqueue` `expire_at` + STROMA_VER bump (stroma codec/event); (2)
     ttl_deadlines + reactive drop in next_deliverable/mark_inflight + proactive
     drop in collect_expired + snapshot FORMAT_VERSION bump (stroma state); (3)
     broker publish-path wiring (resolve effective ttl) + worker durable-drop
     emit; (4) wire `ttl_ms` on `Publish` (protocol); (5) client `expiring()`.
7. **Queue expiration (auto-delete idle queues).** A DECLARE-TIME queue setting
   (the queue carries its own idle-TTL, e.g. expire-after-idle), not an external
   config. A sparse worker destroys queues idle beyond their declared TTL. Builds
   on delete (#4) + idle tracking.
   - DISTINCT from message-TTL default (#6): #6 drops individual MESSAGES by age
     (per-partition, no coordination); #7 deletes the whole QUEUE after idle. Use
     a clearly separate DeclareMeta field (e.g. `expire_after_idle_ms`), never
     reuse the message-TTL default.
   - DESIGN NOTE: queue expiration is ideally GLOBAL (per queue), not
     per-partition. "Is the queue idle?" must aggregate activity across ALL the
     queue's partitions and replica nodes, so a per-partition timer is wrong - the
     decision + the destroy need to be coordinated (controller-side aggregation of
     last-activity, then a coordinated teardown like multi-node delete in #4).
     Needs brainstorming; deferred. Per-partition idle is only a building block.

Shared time-based internals (map for #5/#6/#7 - they are "close" but share CONFIG
more than MECHANISM, so do them one-by-one, not as one unified subsystem):
- Granularity differs: TTL (#6) is per-OFFSET (RangeMap + collect_expired +
  expiry_worker + durable settle); retention (#5) is per-SEGMENT by age
  (`safe_message_truncate_before` + message `published` ts + a coarse truncate
  worker); queue-expiration (#7) is per-QUEUE idle (`destroy_partition` (done) +
  idle tracking, which broker observability already exposes as `idle_for_ms`).
  Different core data structures - do NOT try to unify the mechanisms.
- GENUINELY shared surface = `DeclareMeta` (all three are declare-time per-queue
  settings: `default_ttl_ms`, retention window, idle-TTL). Worth shaping ONCE: a
  consistent `Option<u64>`-ms field convention + declare->replicate->recover
  plumbing (the `Declare` event already carries DeclareMeta). TTL's
  `default_ttl_ms` lands in the shape #5/#7 reuse.
- Also shared: the worker + runtime-settings cadence pattern
  (`expiry_poll_min_ms`/`expiry_batch_max` is the template; #5/#7 each want their
  own cadence knob), and SNAPSHOTS - any per-offset/per-queue durable state added
  for these must round-trip `encode_snapshot`/`load_snapshot` (FORMAT_VERSION bump)
  since snapshots compact away the events that would otherwise rebuild it.

Nack semantics enrichment - plumb the richer nack vocabulary
through. The `NackType` enum already exists in stroma `event.rs` (Discard,
RetryNow, RetryLater, RequeueNow, RequeueLater) but is reserved/dead-code: the
live wire + state path only carries the simpler `(requeue, not_before)` pair.
Goal: let a CONSUMER choose the disposition explicitly - e.g. discard/trash
immediately (straight to DLQ-or-drop, skip retries), requeue WITHOUT bumping the
retry counter, retry-now vs retry-later. This likely changes the meaning of the
current nack "verbs" on the wire and in the handler, so it needs a deliberate
pass (wire op fields + handler mapping + state transitions + client API), not a
bolt-on. Related: the TTL drop (#6) already reuses the terminal-nack ->
DLQ/discard pipeline with `DeadLetterReason::Expired`, which is the first
non-TerminalNack reason flowing through that path - a useful precedent for
threading explicit dispositions.

Usefulness read (value, anchored to Fibril being a work queue - consumed=gone):
- Create + purge: HIGH value, low effort. Round out the admin board (we can
  repartition / set DLQ today but not create or empty a queue) and cover common
  ops needs. Do first.
- Message TTL (+ the expiring publisher): HIGH product value - "do not process
  stale work" is core work-queue semantics (a RabbitMQ flagship). Higher value
  than its cheapest-first slot implies, so consider pulling it forward right after
  the admin-thin wins.
- Time retention: MEDIUM. A backlog safety valve for slow/dead consumers, DLQs,
  and delayed messages. Less central than in a log system since acked messages
  are already gone.
- Hide-inactive + search: MEDIUM, scales with queue count. Low-effort QoL win.
- Delete: MEDIUM-HIGH. Closes the lifecycle / orphan-cleanup gap, but
  safety-sensitive, so deferred for now.
- Queue expiration (declare-time idle-TTL): MEDIUM / niche. Great for ephemeral
  per-session queues (RPC reply, per-client), and needs delete first.

Also queued (correctness): the split-brain adversarial test (see "Split-brain"
under Correctness and durability) - assert a returning stale owner's write AND
replicate at an epoch below the local partition epoch are both rejected. The
mechanism is in place (epoch bump in placement + persisted per-log epoch fence +
the demotion fix aea4d50). This is the missing proof. Non-trivial (needs a
replication + stale-epoch-apply harness), so it lands after the admin-thin items.

## Features (replication-related)

- Plexus: fan-out / stream channel type. See the dedicated "Plexus streams"
  design section below. [DN]

## Plexus streams (fan-out channels) - BUILT (kept as design reference)

Plexus = a fan-out / stream channel type beside the work queue. Every consumer
sees every message, vs the queue's consumed=gone. Selected per channel via
`declare(type: queue | plexus)`. NOW BUILT end to end (the BUILD ORDER steps 1-4
below are all shipped, including durable replication; step 5 topology-as-a-stream
stays deferred as #52). Kept here as the design reference. Supersedes the older
sketch in archive DESIGN_NOTES.md 584-661.

LAYER BOUNDARY (firm):
- stroma stores the retained record log (reuse) plus a retention policy plus a
  small named-cursor map (name -> committed offset). It has NO concept of
  consumers, subscriptions, or fan-out. A cursor is an opaque named bookmark, not
  a consumer.
- fibril owns consumer matching, fan-out delivery, the newest-X in-memory ring
  (live tail), the express lane plus confirm timing, fan-in, per-sub filtering,
  and the consumer -> cursor-name mapping.

STROMA StreamEngine (new, minimal, much smaller than the work-queue engine):
- Plugs into the SAME substrate as the queue engine (per-partition keratin
  message log + event log + snapshot + replication). No lease/ack/inflight/ready/
  DLQ/TTL-redelivery.
- State: cursors {name -> offset}, retention config, head/tail watermarks.
- Events: CursorCommit{name, offset} and retention truncation (reuse
  safe_message_truncate_before). They ride the existing event-log + replication
  path, so failover restores cursors.
- API to fibril: append (reuse), read(from_offset, max) (reuse the replication
  read path), commit_cursor(name, offset), get_cursor(name), plus a retention
  worker. Snapshot encodes the cursor map.
- Build via a surgical substrate/engine seam: formalize the trait the work queue
  already satisfies, keep WorkQueueEngine as-is, add StreamEngine.

FIBRIL stream actor (per channel + partition, owner-side):
- Holds the newest-X ring, the live-subscriber registry, each sub's position.
- Subscribe resolves a start position (latest / earliest / offset / N-back /
  by-time / durable-resume), backfills from the stroma log read until caught up to
  the ring window, then attaches to the live tail. Same catch-up-then-tail pattern
  the replication followers already use.
- Publish (express lane): append to the ring and multicast to live subs
  immediately, hand to stroma to persist in parallel. Confirm timing and ghost
  flag follow the durability tier. Express lane is far simpler here than for the
  queue (no lease, ack, or single-consumer selection).
- Rebuilds the ring from the stroma log tail on start or failover.

CURSOR TRACKING (decided): support BOTH. Broker-side is the RECOMMENDED default
for peace of mind.
- Broker-side durable cursor (DEFAULT, blessed): the client supplies a durable
  NAME (trusted, gated by auth, the Kafka group.id / JetStream durable norm, no
  broker cookie needed). The broker persists and replicates the cursor, advances
  it on ack (at-least-once), and lets a fresh process resume anywhere after a
  crash or redeploy. This is the "name it, it remembers, no offset bookkeeping"
  reassurance most users want. An optional single-active-consumer lease on the
  name stops two clients clobbering one cursor (ownership, separate from identity).
- Client-driven start (OPT-IN, ephemeral or advanced): the client states a start
  position, the broker stays stateless about it, retention is the only bound
  ("beyond grace you are at the mercy of retention"). For live tails, replays, and
  fire-hose fan-out where per-message ack is unwanted.
- Two existing identity layers stay distinct: resume_token (broker-minted, for
  in-grace SESSION resume) vs durable name (client-supplied, for cross-process
  consumer resume).

DURABILITY TIERS (per-channel knob, not a third channel type):
- ephemeral: persist to the log async, do not gate delivery or confirm (lowest
  latency).
- speculative: deliver now plus an X-Speculative header, with the producer confirm
  DEFERRED until durable (fast and honest, the ghost-flag pattern from
  TODOTHOUGHTS).
- durable: persist (and replicate to min-ISR if configured) then confirm.

RETENTION wins over slow cursors. A stream drops by policy. A durable cursor that
falls behind the new head is clamped to head and flagged "lagged" to the consumer.
Confirmed by the "mercy of the retention policy" call.

SELECTION ("subscribe to one X or all") - NO broker routing (NATS subjects
rejected). Three composable dumb-broker pieces:
- channel granularity = coarse separation (the user's scaling lever).
- per-sub header FILTER = fine in-channel selection. STREAM-ONLY. Minimal fixed
  grammar: a set of header == value matches, AND-ed, with an optional `*` glob on
  the value (e.g. region == eu-*). EXPLICITLY OUT, with rationale: regex, OR/NOT,
  nesting, numeric ranges, SQL-style selectors. It is a consumer skip-predicate,
  not routing, so the broker keeps no routing state. Saves egress, not the scan.
- topology-as-a-stream = the broker streams its own facts (catalogue, topology,
  ownership, assignment changes) on a system Plexus channel. Clients subscribe and
  do their OWN routing (pattern fan-in, auto-pickup of new matching channels which
  replaces the grow-pickup and catalogue polls, load-routing later). Dumb broker,
  smart client.

  REFRAMED (2026-06-25): for LIVE ROUTING UPDATES + repartition cutover fencing,
  the mechanism is a TopologyUpdate PUSH FRAME + client ack over the existing
  connection (see #62), not a stream. A stream is the wrong tool there: subscribing
  to a topology stream needs topology to find its owner (circular bootstrap), a
  fan-out stream gives no per-client apply ack (which fencing requires), and a push
  can be targeted per connection. Clients already get the initial snapshot via
  TopologyRequest, so push deltas + ack covers live routing end to end.

  STATUS: DONE end to end (#62, #88, #89, #90). The broker pushes a TopologyUpdate
  on generation change; the Rust, TS, and Python clients apply it (replace + pool
  prune, generation-guarded) and ack the generation. The routing cache + pool are
  created before the bootstrap connection so a push sent right after HELLO lands
  with no wiring race. #90: the broker records per-connection acked generations
  (TopologyAdoptionTracker), reports the cluster minimum as a heartbeat label, and
  the repartition controller fences a cutover's finalize on cluster-wide adoption
  (drained AND adopted-or-timed-out), bounded by repartition_adoption_timeout_ms.
  Full mechanism + assumptions: website dev note "Live routing and cutover".

  #93 DONE (push efficiency): the broker now triggers the push on the routing
  CONTENT (queues + streams), not the raw coordination generation. CORRECTION to
  the original premise: heartbeats do NOT bump the generation - the openraft state
  machine absorbs label-only RegisterNode updates silently (storage.rs special
  case). The generation DOES bump on any committed metadata change cluster-wide
  (other topics' declares, transition markers, runtime settings, unrelated
  failovers), so generation-triggering pushed identical topology to every client on
  any such change; content-triggering narrows to this client's routing. The pushed
  frame still carries the live generation to ack. Because content-gated pushes stop
  firing once a cutover settles (while unrelated activity keeps bumping generation),
  adoption_generation is stamped EAGERLY at the cutover (not lazily), so a
  connection's acked generation stays at a value the gate can clear. Tests:
  broker_pushes_topology_update_on_generation_change (content change pushes),
  broker_does_not_push_topology_when_content_unchanged (churn does not).

  HARDENING (from a pathological-case review): global_topology_adoption now counts
  only LIVE nodes (heartbeat within liveness TTL). A dead node's frozen adoption
  label represented departed clients and pinned the cluster minimum down, stalling
  every cutover on the timeout. Added pure-fn tests (live_topology_adoption excludes
  dead nodes; repartition_adoption_satisfied gate decision) and a client test that
  a stale/out-of-order push is ignored but still acked with the current generation.

  topology-as-a-stream is therefore NOT the routing path. It survives only as an
  OPTIONAL higher-level DISCOVERY layer: subscribe-to-a-pattern / auto-pickup of
  matching channels (the "replace NATS subjects" vision) and a consumable catalogue
  feed for tools. Build it on the same facts ONLY if/when that discovery feature is
  wanted; it is not a prerequisite for routing or cutover. (#52)

  CLOSED 2026-06-28: #52 is done/dropped. The discovery layer shipped over the main
  protocol, not as an actual stream - catalogue snapshot + change subscription (#91)
  and pattern subscribe + auto-pickup (#92) across the Rust/TS/Python clients. A
  literal topology Plexus stream was rejected (circular bootstrap, no per-client
  ack, no per-connection targeting), so nothing of value remains. The only
  untouched sliver is a catalogue feed for EXTERNAL (non-fibril-client) tools,
  which the admin HTTP API already covers by polling.

  DISCOVERY LAYER 1 DONE (#91): all three clients expose a live cluster catalogue
  (declared queues + streams with partition counts) via a snapshot accessor
  (catalogue()) AND a change subscription (Rust catalogue_events() broadcast;
  TS/Python on_catalogue_change handler). Derived from the topology already held,
  refreshed on every full replace (push or fetch), generation-guarded, emits only
  on real add/remove/partition-count change (not owner-only churn). No wire change.

  DISCOVERY LAYER 2 (#92, NOT building yet, user-deferred): client-side rough
  routing - subscribe_pattern(glob) fans in across all matching queues/streams and
  auto-attaches newly matching channels on each catalogue change. Reuses the
  existing fan-in supervisor + grow-pickup + the Layer 1 change feed. Small glob
  grammar only (prefix/suffix *), matching the per-sub filter grammar. No broker
  change. Scope line: catalogue is declared channels + owners, NOT node membership
  or connection presence (those are a separate observability concern).

QUEUE-SIDE FILTERING: UNCERTAIN, reevaluate later, maybe. Per-sub filters break
the work queue (orphans that match no consumer plus head-of-line, starvation
across differing filters, an unbounded smart scan, inflight discipline) - the
classic JMS/ActiveMQ message-selector trap. Kept STREAM-ONLY for now, queue engine
left untouched. Guidance for selective consumption on a queue: use separate queues
or use a stream. If ever demanded, the clean version is a deliberate design
(bounded scan depth plus an orphan no-match TTL to DLQ plus fairness round-robin),
not a bolt-on.

CLIENT IMPACT: modest and additive. Reuse the Deliver frame, fan-in, and failover.
New: declare(type=plexus, durability, retention), subscribe with a start position
plus optional durable-name plus optional header filter, the speculative header,
and settle = cursor commit (durable) or no-op (ephemeral). Recommend
advance-on-ack (at-least-once) for the durable default rather than
advance-on-delivery.

FUTURE ESCAPE HATCHES (named so we do not paint ourselves in, not building now):
- a keyed-index read in stroma ("records for key=K from offset X") for efficient
  one-of-millions selective consumption. A storage index, opt-in, NOT routing.
- queue-side filtering (above), if ever justified.
- subjects/wildcards stay rejected as a broker feature.

BUILD ORDER (prerequisite chain, each step is final-form, not an MVP gate):
1. Carve the stroma substrate/engine seam (formalize the trait the work queue
   satisfies). Low-risk enabler.
2. StreamEngine in stroma (cursors, retention, apply, snapshot, replication).
3. Fibril stream actor (ring, fan-out, backfill, express lane, durability tiers,
   per-sub filter) plus broker routing by channel type.
4. Protocol and client (declare plexus, subscribe start + durable-name + filter,
   speculative header, settle = commit, fan-in reuse).
5. Topology-as-a-stream system channel plus client-side pattern fan-in and
   auto-pickup.
[DN/AUTHOR/MEM]

## Clients

- Per-client feature matrix lives at `clients/FEATURE_MATRIX.md` (Rust reference
  vs each client). Keep it updated as bricks land - it is the at-a-glance parity
  view and the checklist for any new client.
- `clients/ARCHITECTURE.md` is the language-agnostic design reference (layering,
  invariants, continuity/routing/reliability models, porting lessons). Read it
  before starting a new client (the Python client is next).
- Python client: spec LOCKED in `clients/PYTHON_CLIENT_PLAN.md` (async core +
  blocking facade, Python 3.11, build straight to full parity). Implementation is
  a deliberate fresh-context big push - not started yet.
- Future: Python 3.10 backport. 3.11 is the floor; keep the few 3.11-only bits
  (mainly `asyncio.TaskGroup`) isolated behind helpers so dropping to 3.10 is a
  localized change. 3.9 is not a target (forces future-annotations everywhere +
  older-asyncio quirks). Driven by real-world clinging to old runtimes.
- TS<->Rust feature parity: CLOSED (2026-06-23). All FEATURE_MATRIX rows are
  done for both: assignment-events stream (`onAssignmentChange`), live-grow
  partition pickup, typed `WireError` taxonomy, message TTL, producer-id dedup.
- Per-client throughput expectation: define a target each client should sustain
  (msgs/s ingress+egress at a stated payload size, confirmed vs unconfirmed),
  measure it with a small client-side bench, and record it next to the matrix so
  a new client has a perf bar to hit, not just a feature checklist. The server
  bench (`benches/`) measures the broker; this is the client-overhead view.

  ## Client performance audit (IN PROGRESS, 2026-07-04)

  RESULTS (Rust, steady_c, 1KB, localhost, single-node):
  - 1 producer: ~96k/s confirmed (window 1024), ~98k confirmed (window
    8192), ~104k UNCONFIRMED. 10 producers: ~708k/s (7.35x).
  - 1 consumer drain: ~112k/s. 10 consumers: ~510k/s (needs a bigger
    preload than 2M to trust the 10-reader number - it drains in ~4s).
  DIAGNOSIS: the single-client ceiling (~100k both sides) is CLIENT-side
  per-message overhead, NOT the broker (708k with 10) and NOT the confirm
  path (unconfirmed is no faster, and an 8192 window does not beat 1024,
  so it is not confirm-RTT bound). Root cause: the client engine select!
  loop in crates/client/src/lib.rs processes ONE command per iteration
  (`Some(cmd) = cmd_rx.recv()`), paying a channel recv + task wakeup +
  select repoll per publish (~10us/msg). Socket writes are ALREADY
  coalesced (feed + flush_ticker) and confirms ALREADY pipelined, so
  those are not the limiter.
  HYPOTHESIS 1 (batch-drain the command channel) TESTED AND DISPROVEN on
  branch client-dispatch-batching, reverted. Bounded-drain the select!
  cmd_rx arm: confirmed 96k->100k (noise), unconfirmed 104k->98k (a
  regression). A single client does NOT back up the command channel - the
  writer never gets far enough ahead - so the drain almost always finds
  one command and the extra try_recv is pure overhead. The engine loop is
  NOT the limiter.

  HYPOTHESIS 2 (engine_for re-resolution / per-message String allocs is
  the cost) ALSO DISPROVEN by profiling. perf is blocked (paranoid=4), so
  used in-code per-segment timers on publish() (temp, reverted). Result,
  ns/msg averaged over 3M publishes: into_message=22, route=83,
  engine_for=156, send=156 unsaturated. engine_for is only 156ns - not a
  meaningful cost. publish() total is ~554ns, i.e. a single publish could
  sustain ~1.8M/s on its own.

  THE ACTUAL RANKING (profiled):
  - The original "100k" numbers were a BENCH ARTIFACT. steady_c's
    run_rate_limited_writers paces every publish through a
    tokio::time::interval(period).tick().await; a timer-wheel tick per
    message is ~microseconds and caps a ticked writer near 100k
    regardless of client speed. The tight-loop preload path (no tick)
    hits ~148k unconfirmed and ~150k confirmed - the REAL single-client
    ceiling.
  - Under saturation (tight loop) the send segment jumps 156ns -> 1561ns:
    that is publish_unconfirmed().await blocking on the bounded command
    channel. The real limiter is the writer->engine TASK HANDOFF (channel
    round-trip + scheduling), not publish() logic and not the broker
    (708k with 10 clients). So a single client at ~148-150k already has
    the whole broker to itself and is bounded by its own client-side
    pipeline handoff.

  NEXT (informed by the profile, not guessed):
  - Re-test the batch-drain HERE, on the tight-loop scenario where the
    channel actually backs up (send=1561ns) - the earlier test used the
    ticked path that never backed up, so it was measured in the wrong
    place. This is the one scenario where amortizing the engine drain can
    help.
  - Consider the cmd channel capacity and reducing writer->engine hops
    (the handoff, not publish() internals, is the lever).
  - Restate the goal honestly: single-client real ceiling is ~150k, not
    100k. Decide whether ~150k is "enough" or worth pushing via the
    handoff, and fix the BENCH so its rate limiter does not cap the
    unthrottled measurement (add a no-tick saturate mode).

  Requirements for whatever lands (standing): edge-case tests (fatal
  mid-burst, redirect/topology-change invalidating a cached engine,
  ordering preserved, shutdown), and it must not make MORE ops fail than
  the unoptimized path would - a cached-engine staleness must resolve to a
  redirect/retry, never a wrong-owner silent failure. Comment discipline.
  (Bench runs need dangerouslyDisableSandbox.)

  ## Client performance audit - original spec

  Goal: a SINGLE client should not be crippled by per-client overhead.
  Compare 1 producer + 1 consumer against 10 + 10, holding everything else
  equal, and confirm parallelism scales aggregate WITHOUT the single-client
  number collapsing. Target: a single Rust client sustains >=100k msg/s
  (1KB) on its own; multi-client scales toward the broker's multi-core
  aggregate. Stretch: similar single-client numbers in Python and TS.

  The deciding variable is PIPELINING, not the broker. A synchronous
  client (publish -> await confirm -> repeat) is RTT-bound (~20-50k/s on
  localhost) no matter how fast the broker is; 100k+ needs confirms in
  flight (publish_with_confirmation, not publish_confirmed), fire-and-
  forget, or batching. The broker already saturates single-node at
  ~500-600k/s (1KB), so 100k single-client is ~1/5 of ceiling - a modest
  ask if the client pipelines. Precedent: NATS millions/s fire-and-forget,
  Kafka hundreds-of-thousands-to-millions with batching (few thousand
  synchronous acks=all), RabbitMQ tens-of-thousands to ~100k, all
  per-connection.

  Method (apples to apples is the whole point):
  - Every number states confirm mode + in-flight depth + payload size. Do
    NOT compare a synchronous single client against a pipelined 10-client
    aggregate - that "learns" the wrong lesson.
  - Reuse benches/steady_c + scripts/bench-e2e-c.sh / bench-matrix.sh (they
    already take an in-flight-confirm depth for --confirmed). Add a 1-vs-N
    producers/consumers sweep. (Bench runs need dangerouslyDisableSandbox.)
  - Diagnostic: if a single client sits at ~30k while 10 aggregate to
    ~500k, parallelism is hiding synchronous per-client blocking - the fix
    is client-side pipelining, not more clients. Suspects to inspect:
    confirmed-publish in-flight cap, consumer prefetch/credit release
    timing, per-message syscalls vs batched writes, ack round-trips.
  - Python: GIL + per-message overhead means ~10-30k synchronous but 100k+
    is reachable batched/pipelined. TS (Node): event-loop pipelined can
    clear 100k for small payloads, msgpack encode is the CPU ceiling.
  - benchmarks.md overhaul (blocked on the NVMe slice) is where the final
    numbers land; this audit can run on the current hardware first.
- TypeScript client parity pass (BIG, multi-brick): `clients/typescript` is
  basically pre-replication-branch (~3100 lines). It has the single-broker basics
  (publish/confirm/delayed, manual+auto ack, reconnect) but is behind on two big
  fronts. Brick-by-brick, foundation first:
  - BRICK 1 (prerequisite) WIRE FORMAT: DONE. The broker moved frame bodies from
    msgpack to a custom simple binary format (`crates/protocol/src/v1/wire.rs`:
    magic + field-by-field put_u16/put_u32/put_u64/put_uuid/put_str/put_bytes/
    put_headers, ~20 client-facing ops) - much faster, fixed scheduler starvation.
    Ported to TS as `clients/typescript/src/wire.ts` (byte-exact codec with
    round-trip + byte-layout tests) and a symmetric `frames.ts` adapter that maps
    protocol.ts structs to the wire bodies. codec.ts now delegates body encode/
    decode to the adapter while user payloads keep msgpack. The 20-byte frame
    header is unchanged - only bodies moved. Remaining wire fields the protocol
    structs do not carry yet (partition_key, consumer routing) default null/0 and
    get populated by bricks 2-6.
  - BRICK 2 topology + owner routing: DONE (publish path). FetchTopology op +
    TopologyCache + owner resolution per (topic, partition) + a connection pool
    keyed by endpoint + follow-redirect retry on confirmed publishes (bounded by
    maxRedirects). Mirrors the Rust pool/cache shape in idiomatic single-threaded
    TS (plain Map/object/counter, no locks/atomics). NOTE: subscribe still uses
    the bootstrap connection - routing subscribes to partition owners is folded
    into bricks 4-6.
  - BRICK 3 partitioning: DONE (publish path). routePartition (FNV-1a key routing
    + keyless round-robin) stamps partition/partition_key/partitioning_version on
    the wire, byte-exact hashing vs the broker. Still TODO: transparent
    multi-partition fan-in on subscribe (pairs with the subscribe-routing work in
    bricks 4-6).
  - FOLLOWUP (bricks 2-3): a multi-node real-broker smoke for routing/redirects.
    The unit + integration tests cover routing against the real wire codec, and
    brick 1 validated the publish path against a live standalone broker, but a
    ganglion cluster smoke that exercises a real cross-owner redirect is still
    worth adding (fits the brick 8 examples-as-light-tests runner).
  - BRICK 4 reconnect + reconcile + resume: DONE. ResumeIdentity/Outcome on Hello
    and the Reconcile* ops were already wired. The gap was the owner-restart case.
    Reconcile now fires on ANY reconnect that has active subscriptions (not only a
    resumed session) so a bounced owner's fresh session restores or closes the
    streams instead of leaving them open-but-unfed. Regression-tested. Done in
    brick 5: the supervisor now triggers a reconnect for a passive consumer whose
    connection drops.
  - BRICK 5 failover ride-through: DONE (single-partition). Publish side: transient
    owner-failover retry with throttled topology refresh, jittered backoff,
    deadline, not-found fast-fail (publishTimeoutMs), same loop as redirect-follow.
    Consume side: a subscription supervisor reads from a merged queue and
    re-subscribes to the current owner when the per-connection stream closes
    (owner death/restart), tagging each delivery with its engine so manual ack
    settles correctly. Continuity model: supervised subs own continuity via fresh
    re-subscribe and stay out of the reconcile registry (mutually exclusive with
    the brick-4 reconcile path, which is the supervision-off behavior).
    Multi-partition fan-in and graceful-owner-move detection (periodic topology
    owner-check) are now DONE too. STILL TODO (actionable, see below): picking up
    partitions added by a live grow, and lease preservation across re-subscribe
    (today an unsettled InflightMessage from a dead owner fails its ack and is
    redelivered, at-least-once safe).
  - BRICK 6 exclusive consumer groups: DONE. SubscriptionBuilder.consumerGroup/
    consumerTarget, plus cluster-scoped member-id mint-and-carry (server mints on
    the first exclusive subscribe, client latches and stamps every later one).
    Exclusivity is enforced by the broker per-partition gate. The AssignmentChanged
    push is consumed as an assignment-events stream (DONE, parity closed 2026-06-23).
  - BRICK 7 reliability: DONE. retryAdvice/isRetryable classification, the
    reserved-namespace header guard, and a ReliablePublisher that stamps producer
    id + monotonic seq (fibril.client.*) and retries until confirmed. At-least-once
    today. Effectively-once once the broker dedups on those keys (both sides TODO).
  - BRICK 8 examples-as-light-tests: DONE. Self-validating examples/*.example.ts
    (roundtrip, confirmed-delayed, manual-ack-retry, stream) + run-all.sh that
    starts a broker and runs them all. Continuous examples take --check for a
    bounded validated burst. Wired into CI (typescript-client-ci "examples" job)
    by reusing the published fibril-server image rather than building from source.
    STILL TODO: a multi-node cluster smoke for a real cross-owner redirect (needs
    a ganglion cluster, not just one container).
  - ACTIONABLE client gaps - now DONE (TS/Rust parity closed 2026-06-23, Python
    followed). Both items below shipped across all clients; kept as a record:
    - Live-grow partition pickup: the fan-in growth poll subscribes to partitions
      added by a live grow (FEATURE_MATRIX "Failover resubscribe + live-grow pickup").
    - Assignment-events stream: AssignmentChanged is decoded and exposed
      (onAssignmentChange / assignment_events; FEATURE_MATRIX "Assignment events
      stream"). Observability, not correctness (the per-partition gate enforces
      exclusivity).
  - Effectively-once: TWO paths. (a) CLIENT-ONLY and actionable - a consumer-side
    dedup helper that skips already-seen (producer_id, seq); the headers already
    reach the consumer, so no server change is needed. (b) SERVER-GATED - broker
    dedup that drops dups at publish (only a "read by broker producer-dedup later"
    comment exists). At-least-once today until one of these ships.
  - SERVER-GATED client items (build the client half when the server side lands):
    - Lease preservation across re-subscribe (a shared at-least-once limitation).
  Pairs with AUDITS.md "Client API parity" and the client reliability docs item.
  [AUTHOR/AUDIT]
  See clients/ARCHITECTURE.md for the design reference and clients/FEATURE_MATRIX.md
  for status. Next client: Python.

## Code health and structure

- DONE 2026-06-29: `plan_local_assignment_transitions` (coordination.rs) now
  sorts keys by borrowed `&str` (topic/group `as_str()`/`as_deref()`) instead of
  allocating a `(String, _, String)` tuple per comparison, so the transition
  planner sort is zero-alloc.

- Routing/pattern-subscribe parity: DONE on Rust, TS, async + blocking Python.
  Integration coverage: Rust (static queue fan-in), TS (queue fan-in AND
  auto-attach via a pushed topology update), Python (queue fan-in). Remaining
  smaller gaps: (a) no auto-attach integration test for Rust or Python (TS proves
  the shared design end to end. The watcher rides the already-tested catalogue
  feed); (b) no STREAM-pattern integration test in any client (the wire mocks do
  not exercise the stream-subscribe + deliver path for a pattern); (c) assess
  whether pattern subscribe deserves its own docs page rather than the clients.mdx
  entry.

- ACK WINDOW: BUILT 2026-06-28 (keratin e952542 settled RangeSet + 78de9ef
  is_settled rename). The settled set replaced the bitset window, the frontier is
  derived, and the dead ack-window command/handle surface was deleted.
  FORMAT_VERSION bumped to 4. Residual follow-ups: (a) DONE 2026-06-28 (keratin
  cd738d3 + fibril af21b63) - renamed `lowest_unacked_offset` ->
  `lowest_unsettled_offset` and the sibling `lowest_not_acked_offset` ->
  `lowest_not_settled_offset` (plus the GetLowestUnacked / GetLowestNotAcked
  commands and the broker `partition_lowest_unsettled_offset`) across both repos.
  (b) Broader dead-command audit: several
  `QueueCommand` query variants (e.g. GetLowestUnacked, GetLowestNotAcked,
  GetNextDeliverable, GetRetries, FilterNotEnqueued) may have no external handle
  caller now that the broker surface has narrowed - sweep for handle methods with
  zero callers and trim. The original assessment is kept below for the record.

- Dead-command audit (2026-06-28, keratin 7c51a96): swept every command-wrapping
  WorkQueueHandle method for callers across keratin + fibril. Removed the six
  batch wrappers with ZERO callers anywhere (enqueue_many, enqueue_delayed,
  enqueue_delayed_many, ack_many, nack_many, mark_pending_dlq_many) - the broker
  builds those commands directly on the apply path, so the wrappers were
  redundant. Their command variants + state methods stay (the apply path uses
  them). The named suspects (GetLowestUnacked / GetLowestNotAcked /
  GetNextDeliverable / GetRetries / FilterNotEnqueued) turned out LIVE - their
  handle methods have callers. LOWER-CONFIDENCE candidates left for review (each
  has a single keratin caller that is a test or an admin-only Stroma API the
  broker does not exercise, so they are test-covered surface rather than clearly
  dead): retries, filter_not_enqueued, is_inflight, dump_inflight,
  collect_ttl_expired, mark_inflight_batch, canonical / debug_dump_queue,
  inspect_offsets. Decide per item whether the Stroma API is intended product
  surface or can go with its test.
  - REVIEWED 2026-06-28 (keratin bb322d6). Removed the two genuinely dead chains:
    `filter_not_enqueued` (Stroma method + handle + QueueCommand::FilterNotEnqueued
    + state method, zero callers anywhere) and `count_inflight` (Stroma wrapper,
    zero callers; the underlying `inflight_len` handle stays, used by has_inflight
    and evict). Kept the rest as live or test-covered: `inspect_offsets` /
    `inspect_messages` (admin, used by fibril), `collect_ttl_expired` (the TTL
    worker), `dump_inflight` (via `validate`, a crash-recovery invariant test),
    `retries` / `is_inflight` (queue-state assertions in dlq tests),
    `canonical` / `debug_dump_queue` (snapshot round-trip assertions in replay
    tests), and the `mark_inflight_one` / `mark_inflight_batch` test-helper block.
  - QUEUE-ENGINE pass 2026-06-28 (fibril 05ce6a6): swept the broker `QueueEngine`
    + `StreamStore` traits and `StromaEngine` inherent methods. Every QueueEngine
    trait method has a live broker caller. Removed three dead ones:
    `StromaEngine::become_queue_owner` (the epoch-fenced
    `become_queue_owner_with_epoch` is the live owner-activation path), the
    singular `StreamStore::commit_stream_cursor` (superseded by the batched
    `commit_stream_cursors` from the cursor-commit microbatcher, #83), and
    `StromaEngine::stream_replication_next_offsets` (never invoked). Note: the
    keratin `Stroma::commit_stream_cursor` it forwarded to stays - it is exercised
    by keratin's own streams tests.

- ACK WINDOW assessment (2026-06-28): KEEP it, it is load-bearing. In
  `QueueInternalState` (keratin stroma/core/src/state.rs) the ack state is
  `settled_until` (the contiguous acked-prefix frontier) plus an out-of-order
  record `ack_window_base` + `ack_bits` (a fixed `ACK_WINDOW`-wide bitset). On
  `ack(offset)`: an ack at the frontier advances `settled_until` then
  `advance_frontier` slides the bitset absorbing the now-contiguous run; an ack
  ABOVE the frontier sets a bit so the frontier can advance through it later. So
  the window is exactly the out-of-order ack tracker that makes `settled_until`
  contiguous and monotonic.
  - NOT derivable from the inflight set: an out-of-order ack removes the offset
    from `inflight`, so without the bitset there is no record that it was acked
    (vs never-existed) and the frontier could not advance through it.
  - NOT redundant with the frontier alone: the frontier is only the contiguous
    prefix, out-of-order acks live strictly above it.
  - What it OFFERS: a bounded, snapshot-able, replicable record of out-of-order
    settlements driving a contiguous monotonic `settled_until`, which gates
    message-log retention/truncation, delivery start, and the replication
    confirm. The byte-blob accessors (`ack_bits_bytes` /
    `SetAckWindowFromBytes`) + `ack_window_base` exist to ship that state in
    snapshots / state-checkpoints and to followers.
  - Refined direction (2026-06-28): replace the fixed-size bitset window with an
    unbounded `settled: RangeSet<Offset>` of terminal settlements ABOVE the
    frontier (mirroring `ready: RangeSet`). Key points:
    - Rename ack -> settle. `ack()` is the shared frontier-advance for ack,
      terminal nack, and DLQ commit (it is called from nack_at and
      dead_letter_commit, "reuse existing frontier-advance logic"), so the
      structure already tracks all terminal settlements, not just acks. The
      honest name is `settled` + `settled_until`.
    - Store the WHOLE settled set from 0, not just the part above the frontier.
      With a RangeSet `[0, frontier)` is a single `(0, N)` entry regardless of N,
      so including the prefix costs one range and makes the set the single source
      of truth. (With the old bitset window the frontier HAD to be separate
      because the window only covered a bounded span above it. The RangeSet
      removes that constraint, so this is now a semantics call, not a cost one.)
    - No window. A RangeSet is unbounded, so the fixed `ACK_WINDOW` cap and its
      far-ack in-memory-drop divergence both go away (this subsumes the old
      follow-up about documenting the cap).
    - Shape: `is_settled(o) = settled.contains(o)`; `settled_until()` is DERIVED
      as the end of the range covering 0 (else 0). The frontier advance is free:
      inserting a settled offset adjacent to the `[0, frontier)` range coalesces
      it, so the explicit `advance_frontier` slide logic disappears. No separate
      `settled_until` field to keep in sync.
    - Hot-path note: deriving the frontier is a first-range lookup rather than a
      bare field read, but the range count is tiny (about out-of-order span + 1),
      so it is cheap. If a hot path ever needs a bare read, cache the frontier as
      a pure memo of the set (recomputed on insert), not as a second independent
      field.
    - Cost / deciding factor: this is a snapshot + replication FORMAT change. The
      state-checkpoint and follower sync ship `ack_window_base` + `ack_bits_bytes`
      today; they would ship the ranges instead, needing a `STROMA_VER` bump and
      replacing the `SetAckWindow` / `SetAckWindowFromBytes` / `ack_bits_bytes` /
      `ack_window_base` command + client surface with a `settled`-range
      equivalent. The in-memory swap to RangeSet is the small part.

- Code-TODO triage (from a quick scan, ~58 in fibril crates + ~37 in keratin/stroma. Most
  are minor inline idea-markers, the full sweep is the #63 hygiene pass). The
  few worth elevating rather than leaving buried in comments:
  - `protocol/.../handler.rs` publish drowning out delivery - LARGELY SOLVED by
    cursor-commit microbatching (#83), so the inline TODO is now a NOTE. Re-examine
    only if delivery fairness regresses under heavy publish (no action otherwise).
  - `broker/src/broker.rs` x3 "do not keep handle (memory leak) ..." - INVESTIGATED,
    NOT a genuine leak (TODOs were stale, now reworded). They are per-QUEUE tasks
    (publisher_sink, confirm_sink, delivery_loop), not per-connection. TaskGroup
    wraps tokio_util TaskTracker, which tracks only an in-flight count and does not
    retain JoinHandles, so finished tasks are reaped. All three break on
    owner_runtime_shutdown (fired by cancel_owner_runtime on demotion/eviction) and
    the TaskGroup cancel token (broker shutdown). Residual is only an invariant:
    every owner-runtime teardown must call cancel_owner_runtime (it does today).
  - `keratin-log/src/writer.rs` "tests showing guaranteed order" + "more tests for
    failures and edge cases (batch flush on shutdown, etc.)" - test-coverage gaps in
    the durability-critical writer, plus the noted "more pipelining" lever.
  - Already tracked elsewhere (no action): the snapshot-cadence FIXME (stroma.rs ->
    "Snapshot cadence" item), the BrokerConfig builder TODO (the item below), and the
    failover-retry-constant knobs (settings tiering). Stale comment trimmed: the
    client reconnection-path "possibly resubscribe/redeliver" TODOs were superseded
    by the failover supervisor + reconcile and reworded in place. [AUTHOR]
- Convert wide config structs (starting with `BrokerConfig`, and `StromaOptions`)
  to a builder pattern. They are currently constructed with exhaustive struct
  literals across many call sites (tests, replication, main), so adding a field
  churns all of them and discourages putting new tunables in config (e.g. the
  stream ring/live-channel sizes are module consts in broker.rs as a result). A
  builder with defaults lets new fields land without touching existing call sites,
  and is the clean home for those stream tunables. [AUTHOR]

- Rework the `tui-example` (`crates/tui-example`): a small TUI app that connects
  to a broker and visualizes messages (packs) flowing in and out. It has disabled
  instrumentation (latency tracking + compute_stats were dead, removed in the
  dedup sweep). Bring it back to a clean, illustrative live-client demo. Also
  `benches/bin/bench_e2e.rs` is half-disabled (dead channels, hardcoded
  reporter/broker params) and wants the same treatment. [AUTHOR]
- `stroma.rs` by-concern file split: a readability refactor independent of
  clustering. A full module sketch is preserved in the archived worklog. Do the
  low-risk type modules first, then the engine impl split incrementally. [WL]
- substrate-versus-engine split (WorkQueueEngine plus StreamEngine over a shared
  substrate), which enables Plexus. Now spec'd as build step 1 in the "Plexus
  streams" design section above. [DN]
- Optional de-raft finish: fibril-side de-raft is complete. Remaining is optional,
  routing the protocol dev-dep and coordination-ganglion through the `ganglion`
  umbrella crate, or the bigger approach-B of moving raft-node construction up out
  of fibril entirely. [WL]
- Ganglion domain hygiene: keep all coordination-domain code in ganglion so fibril
  depends on a stable surface, not internals. Ongoing. [DN/MEM]
- Clustering-module separation: done for broker, stroma-core, and client (protocol
  was already done). Kept here only as a reference point. [MEM/DN]
- Replace `::MAX` config branches with `Option`s for clearer semantics, and a
  mutex-refactor pass (concurrency-primitive discipline). [WL]
- Assess persisting the queue catalogue (which queues plus path) in the stroma
  store instead of filesystem discovery. Filesystem discovery is likely good
  enough. [WL]
- Low priority: `lifecycle_locks` map pruning plus a bench (rare optimization, no
  latency or throughput impact). [WL]

## Docs

- Client reliability example or tutorial on the docs site (`clients.mdx`):
  confirmed publish with the `is_retryable`/`retry_advice` match pattern, the
  ReliablePublisher opt-in, producer ids and the dedup path, and a short
  failover-behavior note. Keep it copy-paste-able. [WL]
- Manual failover runbook (partially covered by `FAILURE_MODES.md`). [PLAN]
- Keep the docs current in the SAME change as any user-visible surface change
  (protocol/client API, admin endpoint/page, CLI, config/runtime setting,
  behavior/limit): update `website/.../implemented-surface.md`,
  `clients/FEATURE_MATRIX.md`, the relevant `website/.../` guide, `README.md` if
  it lists the feature, and the "Done since the inventory was last curated"
  section. Last full recuration: 2026-06-22.

## Testing and hardening

- Chaos soak harness: `scripts/cluster-tryout.sh --chaos` runs repeated mixed
  faults (kill+rejoin and SIGSTOP/SIGCONT pause) under confirmed load and asserts
  zero loss plus reconvergence. It found (and now passes after the fix for) the
  owner-bounce consumer-resume bug. Each round deterministically faults the
  topic's current owner with the consumer connected outside the replica set, so a
  run reliably exercises the recovery path. Manual diagnostic, not in CI. A future
  step would be wiring a trimmed version into CI as a nightly soak. [chaos]
- Adversarial tests through all layers, plus a realistic chaotic benchmark
  (bursty, non-steady supply, consume, and bandwidth, not steady saturation). [WL]
- Cluster benchmark profiles: replica-durable confirms, follower catch-up,
  partitioned fan-in, and redirects. [AUDIT]
- A test pass to ensure tests pin correct behavior, not current bugs. [WL]
- Revisit the audits in [AUDITS.md](AUDITS.md) and harvest anything still
  actionable (several are Audited with open Next items). [AUTHOR]

## Far horizon (v2+)

- Multi-region and geo placement: region and zone labels feeding the planner,
  per-queue placement hints, region-aware placement strategy. [PLAN]
- Load-aware placement and routing: node and partition load scores (advisory,
  off the coordination log, hysteresis, power-of-two-choices), plus a consumer
  scheduling policy and a per-consumer override of the global partition target. [DN/MEM]
- Leadership health transfer (working name "abdication"): let the coordination
  (raft) leader hand off leadership when it is the bottleneck, so the control
  plane does not run on a degraded node. NEEDS EXAMINATION, not committed. Notes:
  - Scope: this is the CONTROLLER leader (commits placements, declares, failover
    decisions, settings, membership), NOT the message hot path (queue/stream data
    replication has its own caught-up-follower failover). So "cannot keep up" is
    almost never throughput saturation, it is a degraded leader node (slow log
    fsync, pegged CPU, noisy neighbor). The real harm is delayed failover planning
    and repartition cutovers, the brain reacting slowly to OTHER failures. That is
    the resilience win: do not run the brain on the sickest node.
  - Core discriminator (makes or breaks it): leader-LOCAL slowness vs CLUSTER-WIDE
    slowness. Transfer only helps the former. The trigger must be "I am slow AND a
    specific follower is demonstrably healthier and caught up", not absolute commit
    latency, or it ping-pongs between equally slow nodes.
  - Signals to combine: openraft 0.9 self-metrics (quorum-ack / time-to-commit
    latency, per-follower replication lag, millis_since_quorum_ack), local log
    fsync latency, and controller-loop schedule drift (actual vs expected cadence,
    the user's "starting updates on schedule" intuition: sustained drift, not a
    blip).
  - Guards (non-negotiable): use graceful leadership TRANSFER to a chosen caught-up
    voter (openraft transfer), never a bare step-down (which risks re-electing the
    same slow node or a lagging one, with an election gap). Never transfer to a
    non-caught-up follower. Minimum leadership tenure, cooldown, hysteresis, a
    global transfer rate limit, and anti-ping-pong memory. All thresholds as config
    settings. Start ADVISORY (log "would transfer to N because X", surface health
    scores in the admin topology panel), measure, then enable.
  - Fit: phase-2 of the load-aware direction above (reuse the node load/health
    scoring substrate rather than a bespoke mechanism). Gate on a feasibility check
    of openraft 0.9's leadership-transfer API and metric access, and instrument
    what actually causes leader slowness in practice BEFORE building the policy. [AUTHOR]
- Live repartitioning beyond fixed-at-create: partition_count is already
  versioned and routing is version-parameterized. The hard deferred semantic is
  per-key ordering across a resize. [PLAN/MEM]
- Transactional or cross-partition writes. [PLAN]
- Self-hosted metadata to replace external coordination. Explicitly deferred. [PLAN]
- Follower reads: rejected for the work-queue model (no analog). [PLAN]

## Non-replication track (own roadmap)

These elevate Fibril feature-wise but are tracked separately, not on the
replication roadmap. Raw notes are preserved in
`archive/replication-sharding-plan/TODOTHOUGHTS.md`.

- TTL and message expiration, time-based retention, queue purge, and queue
  deletion lifecycle (the author's first post-wrap nice-to-haves).
- Broker restart reconciliation and update reconciliation: persistent session
  continuity built on the existing reconnect model. Noted as the most distinctive
  of the bunch.
- In-memory (non-durable) queues via a pluggable Keratin write target.
- Express-lane and speculative delivery with deferred publisher confirmation.
- Client opt-out of convenience features and client-enforced rate limits.
- Settings proverbs (a delight touch on the durability override), more Keratin
  writer pipelining, topic and node id interning, dashboard QoL such as hiding
  inactive queues plus search, and a RabbitMQ-compatibility easter egg.

## Low-latency durable publish (active - see LOW_LATENCY_DURABILITY_PLAN.md)

Root-caused (2026-07-07) the ~15-20ms nvme low-load durable-publish latency floor:
it is ~4 serialized fdatasyncs per publish (keratin sync() does .log + .idx, x two
serial durability round-trips - msg-log durable THEN event-log durable). Measured,
not theorized (tmpfs=2ms proves it is fsync cost, not scheduling; flat across linger
and rate sweeps). Three-phase plan in `LOW_LATENCY_DURABILITY_PLAN.md`, executing
Phase 1:
1. Drop the per-commit `.idx` fdatasync; rebuild the `.idx` tail on dirty open.
2. Specialized `CancelEnqueue(offset)` event + runtime compensation on msg-fsync failure.
3. Parallel fsync (event off the msg APPEND offset, both concurrent, ack on both-durable)
   + recovery fold-then-validate (Enqueue+Cancel annihilate; clean-tail-suffix dangling
   reclassified as expected truncation, not quarantine). Safety anchor: confirm only on
   both-durable, so any dangling enqueue is unconfirmed and safe to drop.

## Client API freeze bundle (#111) - Tier 5 Group A of the client-API audit

From clients/API_CONSISTENCY_AUDIT.md (archived). These reshape the client
receive/error surface, so they are designed ONCE at the API freeze, not piecemeal.
Recommended actions (assessed on merit 2026-07-09):

1. Typed stream-close reason (ANCHOR). Today a closed subscription just ends
   (None/null/channel-close/iteration-end) with no reason. Make the receive surface
   yield a typed terminal reason (End / Unsubscribed / ReconciledClosed / Recreated /
   Disconnected / BrokerError / PermissionRevoked). Design decision already made
   (reconnection-grace note 2026-06-29): the reason travels WITH the stream, not a
   side channel. This changes every recv()/iteration signature, so design it first;
   everything else slots in.
2. Reconnect outcome - TRIM, do not build the full state machine. All 5 clients
   already return a terminal ReconnectOutcome (Resumed/FreshConnection/...). Just add
   a ReconciliationFailed terminal variant. Skip the observable transient
   Reconnecting/Disconnected states - that fights "just works" and users rarely need
   to watch mid-reconnect.
3. Whole-surface error taxonomy - SPLIT, mostly do-now. Grow error_guides.json cases
   + align messages incrementally (the Group B way, no API change). Only the typed
   retryability accessor on every error type needs the freeze.
4. Typed unsafe-resume value (pairs with 1). On a non-resume, old subscription
   handles are unsafe; make them a distinctly-typed value you cannot accidentally
   treat as live. Reshapes handle types -> design with item 1.

Sequencing: item 3 progresses now (fixtures); items 1 + 4 + the outcome-enum half of
2 are the one typed-lifecycle freeze pass. Folds in the shelved #102/#103.

## SECURITY (HIGH): CA-fingerprint TLS pinning is MITM-bypassable - RESOLVED 2026-07-09

FIXED across all five clients. The bug: fingerprint pinning accepted the chain if
ANY presented cert matched the pinned SHA-256 while only the handshake SIGNATURE
(leaf key) was verified, so a network MITM could present [rogue_leaf (attacker key),
real_CA_cert] and be trusted (the real CA cert is public). CA-fingerprint pinning was
bypassable; leaf-fingerprint pinning was already sound.

The fix is uniform across stacks: leaf pin (pin == presented leaf) is accepted
directly (the handshake proves the leaf key); a CA pin (pin == an issuer) now
path-validates the presented leaf against the pinned certificate as the sole trust
anchor, accepting only when the pinned CA genuinely signed the leaf. Hostname is not
verified (the pin, not a name, is the trust basis). EKU is lenient (absent EKU is
fine) in every stack, so the broker's no-EKU server cert still validates.

Per-client resolution (each with AC1 adversarial + AC2/AC3/AC4 unit tests):
- Rust: crates/client/src/tls.rs FingerprintVerifier uses rustls's own
  `verify_server_cert_signed_by_trust_anchor` (no new dependency, no hostname check).
  Tests in the same file's `mod tests` (rcgen dev-dep).
- Go: clients/go/tlsconn.go `verifyPinnedChain` uses x509 CertPool(C) + leaf.Verify.
  Tests in tlsconn_test.go.
- C#: clients/csharp/Fibril/Tls.cs `ValidatePinnedChain` uses X509Chain CustomRootTrust
  with C as the sole custom root. Tests in Fibril.Tests/TlsTest.cs.
- TS: clients/typescript/src/client.ts `verifyPinnedChain` walks the chain verifying
  each signed link (X509Certificate checkIssued + verify + validity). Tests in
  tests/tls.test.ts.
- Python: clients/python/src/fibril/client.py `_verify_pinned_chain`. Leaf pin is
  stdlib-only; CA pin path-validates via the OPTIONAL `cryptography` extra
  (`pip install fibril[tls-pin]`, needs Python 3.13+ for the chain), else a loud
  actionable error. Tests in tests/test_tls.py (incl. a monkeypatched no-cryptography
  case). CI installs `--extra tls-pin`.

Broker unchanged (crates/tls still prints the CA fp; its no-EKU cert validates under
the lenient path check). CHANGELOG has the Security Fixed entry. Full design record
was archive/SECURITY_CA_PIN_FIX.md.

## BUG REPORT (ROOT CAUSE CONFIRMED 2026-07-11): publish wedged until restart under e2e_c load

RESOLUTION OF THE HUNT (Phase 1, 2026-07-11): the wedge is a deadlock between
the keratin writer thread and its fsync worker, confirmed by captured thread
stacks in a controlled repro. Mechanism, in keratin-log/src/writer.rs:

- The writer sends commits to the fsync worker over a bounded channel
  (fsync_tx, capacity = max_inflight_fsyncs) with a BLOCKING send in commit().
- maybe_commit_due's second disjunct (elapsed >= fsync_interval) issues
  commits without honoring the inflight cap, so under saturated-drive fsyncs
  (0.9-1.2s each vs a 5ms interval) the writer routinely fills the channel and
  parks in fsync_tx.send.
- The fsync-fusion drain (recv + try_recv loop) can collect one MORE request
  than the channel capacity when the parked writer wakes and enqueues while
  the worker is preempted mid-drain. The worker then owes cap+1 FsyncDones,
  delivered by BLOCKING sends into done_tx, which is also sized to cap.
- If the writer has meanwhile refilled fsync_tx and parked again, the worker
  parks mid-fan-out: writer waits for the worker to recv, worker waits for
  the writer to drain dones. Both are plain OS threads with no timeout -
  permanent mutual park. Every later append piles into the writer command
  channel (cap 8192) and then blocks its senders, wedging the stroma queue
  actor: queue flow dead, control plane alive, exactly the field shape. The
  preemption-race window is microseconds, which is why the field hit it only
  three times across days of saturated benching.

Evidence: keratin-log/examples/wedge_stress.rs (real writer, AfterFsync
appends) run single-core-pinned on the nvme scratch drive with a parallel
dd+fsync hog stretching ext4 journal commits to field latencies. Wedged in
~90s. gdb stacks show the writer parked in crossbeam send<FsyncReq> at
writer.rs:1547 (via maybe_commit_due writer.rs:1212) and the fsync worker
parked in crossbeam send<FsyncDone> at writer.rs:394, completions counter
frozen. fd count at wedge: 18, so no fd exhaustion. The third occurrence's
connect-time ECONNRESETs were a downstream symptom of the wedged broker, not
a cause.

FIX SHIPPED (keratin, same day): scheduled commits now honor the pipeline
capacity as a hard cap (the interval-due branch no longer overruns it), and
every fsync handoff runs behind an ensure_fsync_slot gate that drains
completions until a slot is free, so no fsync_tx send can ever park and the
worker's fused group always fits the done channel - the cycle is structurally
impossible. Verified: full keratin workspace suite green, and the wedge recipe
(cap=1, single core, dd-fsync drive hog) that killed the unfixed writer in
~90s at 514 completions ran 22 minutes clean to 3.2M completions (cap=8
variant likewise, 2.95M), with a side bonus of ~30x saturated-throughput at
cap=1 because the writer keeps staging instead of parking. Repro tool kept as
keratin-log/examples/wedge_stress.rs (no unsafe).

Original case file kept below for the record.

## BUG REPORT original notes (2026-07-10): publish wedged until restart under e2e_c load

Reported while running `benches` e2e_c (writer -m 50000 -c 10 --size 1024, then
reader): publishing stalled permanently mid-run; only a broker restart cleared
it. Logs around the time show only routine reconnect-grace entries/expiries
(all 20 conns dropping at once when the e2e processes exit is normal). Not
reproduced on demand yet.

Facts from the captured excerpt (note: excerpt is from a FRESH restart, uptime
20s, over an inherited data dir - the wedge event itself is not in it):
- ready=5,078,479 on topic1/0 with settled offset ~70.7M (data dir carries
  many prior runs).
- total_expired > 0 and climbing (a message TTL is in play); the expiry worker
  drains expired messages in 8192-message iterations.
- KERATIN IO fsync times ~0.9-1.2s per batch: the drive is saturated
  (consistent with the consumer-NVMe fsync floor) under writer traffic +
  truncate storms + snapshot writes.
- Stroma command lane depths all 0 at snapshot time (no actor backlog then).

Wedge candidates (unconfirmed - trace before patching):
1. Publish confirm path stalled behind pathological storage IO (expiry storm
   over a huge aged backlog + truncations), looking like a hang from the
   client.
2. State wedge in the connection/session layer (grace cleanup interacting
   with resume sessions) blocking new publishes or new HELLOs.
3. A leaked permit/backpressure cap on the publish path that a dropped
   connection never returns.

SECOND OCCURRENCE (same day, better evidence): with the broker wedged,
- fresh writer runs each push ~20k msgs/connection then stall or die with
  BrokenPipe - the TCP-backpressure shape of the broker no longer draining
  its read sockets;
- fresh READER runs receive 0 (subscribe succeeds, nothing delivered);
- accept/HELLO/subscribe stay healthy throughout.
Everything through the topic1 queue actor is stuck while the control plane
lives => REFINED PRIME SUSPECT: a lost append/durability completion wedging
the stroma queue actor, most plausibly in the new keratin parallel-fsync /
fsync-fusion path under drive saturation (0.9-1.2s fsyncs, 300k-record fused
batches) interleaved with truncation storms and the TTL-expiry worker
(8192/iteration over an aged 5M backlog).
Smoking-gun check at wedge time: the periodic Stroma debug report in the
broker log (or the admin Diagnostics page) - a command lane pinned high with
the queue frozen dirty confirms the actor wedge.
THIRD occurrence: with the broker wedged, a fresh reader's connections are
RESET (os error 104) at connect time - not accepted-then-silent like before.
New candidate that fits active resets: RESOURCE EXHAUSTION, most plausibly
file descriptors (leaked sockets from dormant/grace sessions + segment file
churn under truncate storms). Capture next time: `ls /proc/$(pidof
fibril*)/fd | wc -l` and `ulimit -n` while wedged, plus whether the admin
port also resets.
Hypothesis space stays OPEN: the wedge could equally live in the protocol
handler / session layer (read loop stalled on something other than the actor
lane, writer-task wedge, permit accounting) - the actor/fsync-fusion lead is
the strongest single candidate, not a conclusion. The wedged-era data dir
still exists (server restarted and published over it since, so it is
polluted but may retain structure worth a look).
Hunt plan: dedicated instrumented session - reproduce with an aged data dir +
TTL + repeated 5M x 1KB writer runs under drive saturation; instrument
keratin fsync-fusion completion-set accounting AND the handler read/publish
path, and watch stroma lane depths. Trace before patching.

## Future: enable TLS from the dashboard (no config edit)

User idea (2026-07-10): first-run TLS setup from the Security page - e.g. a
"Enable TLS" flow that triggers the auto-self-signed generation (the material
machinery already exists) and switches the listeners over, without touching
the config file. Precedent exists: setup.mode already does exactly this flow
at FIRST boot (crates/admin/src/setup.rs); the idea extends it to a running
broker. Design questions to settle: listeners currently build TLS at
startup, so this needs live listener rebinding or a dual-listen window; how
the choice persists across restarts without a config write (persist a
data-dir marker? write back to config with consent?); and the plaintext-426
guidance story during the switchover. Pairs well with the existing live
rotation and the cert card already on the Security page.

## Dashboard + broker roadmap (AGREED 2026-07-11, work in this order)

Phase 0 - Land the redesign. Merge administrative-ascension -> main: user
click-through sign-off, full workspace test suite + vocab-lint at the branch
tip, keep the branch history (coherent steps, not noise).

Phase 1 - The publish-wedge hunt (dedicated instrumented session). Correctness
outranks features; case file in the BUG REPORT section above (three
occurrences, discriminating observations, fd-exhaustion check ready).

Phase 2 - Quick feature wins, one sitting each, in order:
  2.1 URL filter state (?q=...) on queues/subscriptions/connections.
  2.2 Publish a test message (admin publish endpoint + queue-detail button;
      deliberately early - it is also a debugging tool for later phases).
  2.3 Per-partition consumer coverage on queue detail via the cohorts data.
  2.4 Storage breakdown segmented bar behind the Overview disk stat.

Phase 3 - SSE for live dashboard data, replacing the 2s polling (autoRefresh
centralizes the plumbing). Must land before Phase 4. Full plan below in
"Phase 3 SSE plan (SCOPED 2026-07-11)".

Phase 4 - The feed layer:
  4.1 Control-plane audit feed page (bounded in-memory event ring: declares,
      deletes, drains, settings changes, quarantines, membership changes).
  4.2 Desktop notifications + optional user threshold rules on attention.

Phase 5 - The showpiece: Cluster diagram restyle TOGETHER with mascot stages
S0-S2 (sprite rings per broker, procedural tendrils - see the mascot plan).
Wakes when the cleaned ring art lands; one effort, not two.

Phase 6 - Docs/changelog synchronization sweep (END of roadmap, before the
next release cut). Scope: website admin-dashboard.md re-walk against the
final surface (publish-test-message, SSE, audit feed, notifications,
coverage, storage breakdown), deployment/monitoring.md cross-refs,
implemented-surface.md admin rows (new endpoints: history, attention, tls,
audit, publish), a screenshots pass if the site gains any, and a CHANGELOG
read-through for coherence. NOTE: this sweep does not suspend the standing
rule - every phase still adds its own CHANGELOG bullet and doc touch in the
same change; Phase 6 is the final consistency pass over the accumulated
surface, not a deferral of docs work.

Floating (slot on mood): enable-TLS-from-UI (own design pass), flavor tuning,
deny_unknown_fields hardening on admin request DTOs (a typoed field name in a
declare request is silently ignored today - guided-errors philosophy says name
it).

## Known intermittent: one fibril-admin test flakes under full-workspace runs

Seen 2026-07-11, roughly 1 in 4 `cargo test --workspace` runs: the admin test
binary reports 71 passed / 1 failed, but solo `-p fibril-admin` runs and rerun
attempts stay green and the name was not captured. Next time it fires, grab
the name (`cargo test --workspace --no-fail-fast 2>&1 | tee /tmp/suite.log`)
and deflake properly - likely a timing-sensitive TCP-bound admin test under
parallel load.

## Future: broker memory audit (filed 2026-07-11)

User observation: RSS does not drop back to earlier levels after all active
queues evict. Suspects to examine in a dedicated session: mimalloc arena
retention (freed memory kept in arenas, not returned to the OS - check its
purge/decommit options), keratin tail caches (64 MB budget per log - confirm
they release on eviction), admin in-memory state (history rings, audit ring,
metrics - all bounded, should be small), and plain heap fragmentation. Method:
baseline RSS -> load N queues -> evict all -> compare against mimalloc stats
(MIMALLOC_SHOW_STATS=1) and a heaptrack/dhat profile, so "held by allocator"
and "genuinely leaked" separate cleanly. Trace before patching.

## Phase 3 SSE plan (SCOPED 2026-07-11)

Goal: replace the dashboard's 2s polling with one server-sent-events stream
per open page, so data pushes on the broker's clock and an idle dashboard
costs the broker nothing.

Design decisions (settled):
- ONE multiplexed SSE endpoint, GET /admin/api/events?families=a,b,c - named
  events per data family (overview, queues_debug, history, attention,
  subscriptions, cohorts, connections). One EventSource per page, never more
  (browsers cap ~6 connections per origin on HTTP/1, and connection hygiene
  under boosted navigation must be deterministic).
- Full-snapshot pushes on a server tick (~2s), reusing the existing JSON
  producers verbatim. Deltas/true events are Phase 4 (audit feed), not here.
- Broadcaster task in the admin crate (history sampler is the structural
  precedent): each tick serializes every family WITH at least one subscriber
  exactly once and fans the shared string out over a tokio broadcast channel.
  Zero subscribers = no tick work at all. Lagging receivers drop and the
  client reconnects (EventSource auto-retry).
- Client: a shared liveData(families, onData) helper in admin.js owning the
  page's single EventSource; page scripts pass the same refresh callbacks
  they use today. Teardown rides the existing boosted-nav timer hygiene
  (extend the __spaIntervals-style registry to close EventSources on swap -
  leaked connections across swaps are the failure mode to test for).
- Live pill driven by the SSE connection state (open / reconnecting / dead)
  instead of last-fetch age stamping.
- Fallback: if EventSource is unavailable or stays failed, pages fall back to
  the current autoRefresh polling (which therefore stays in admin.js as the
  fallback engine, not deleted).
- Mutations (declare/delete/publish/drain) stay plain POSTs; pages may keep
  their immediate one-shot refresh after actions, the next tick converges
  everyone else.
- Auth: the SSE route goes through check_auth like every admin route
  (same-origin EventSource carries the session cookie).

Acceptance criteria:
1. All dashboard pages receive their data over one SSE connection at <= 2s
   freshness with the polling loops gone in the SSE path.
2. Server serializes each family once per tick regardless of subscriber
   count, and does no tick work with zero subscribers.
3. After N boosted navigations the broker holds exactly the connections of
   the currently open pages (leak check via server-side connection count or
   fd inspection).
4. Live pill states track the stream (live / reconnecting-stale / dead).
5. With EventSource broken (simulate), every page still works via the
   polling fallback.
6. Workspace suite green, new tests for the events route (auth, family
   filter, event shape) and the broadcaster (tick sharing, zero-subscriber
   pause); changelog + admin-dashboard.md + implemented-surface row in the
   same change.

Execution order: recon (axum SSE idiom for the pinned version, autoRefresh +
timer-hygiene internals) -> broadcaster + route with the four core families
(overview, queues_debug, history, attention) -> liveData helper + migrate
Overview as proof -> migrate the remaining pages, extending families
(subscriptions, cohorts, connections, streams debug) -> live pill + fallback
-> leak/regression verification incl. a dashboard-open-during-bench sanity ->
docs/changelog sweep for the phase.

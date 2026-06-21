# Follow-ups and pending work

Consolidated open items, extracted before the replication-effort working docs
were archived so nothing is lost. Full detail and rationale live in
`archive/replication-sharding-plan/` (the worklog, replication planning, and
design notes). Audit follow-ups live in [AUDITS.md](AUDITS.md), the audit status
board, and are not duplicated here.

Source tags: `[WL]` worklog, `[PLAN]` replication planning, `[DN]` design notes,
`[MEM]` memory, `[RACE]` race-windows, `[AUDIT]` audit board, `[USER]` author note.
Tiers are grouped by concern, not strictly ordered.

This file tracks the replication and clustering roadmap leftovers. Non-replication
feature ideas live in their own track, summarized at the end.

## Done since the inventory was last curated (2026-06-21)

- Topology page glowup: adaptive ellipse/staggered-grid layout, click-to-inspect
  broker panel, and a Diagram/List view toggle.
- Admin SPA-feel: boosted navigation, vanilla, no framework.
- Admin node management from the topology page: repartition plus add/remove
  coordination voting member controls.
- CI guard: inline template-JS syntax check (`scripts/check-template-js.sh`,
  wired into `scripts/check.sh`).

## Correctness and durability

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
  defense-in-depth. [USER]
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

- Owner-side read and encode fan-out (shared tail, private catch-up) for RF >= 3. [WL]
- Parallel-fsync, now unblocked since the recovery event-to-message verification
  landed. [WL]
- Replication-lag backpressure hook in the append path (slow accept when
  followers lag). [PLAN]
- Replication streamed decode (decode while fetching and applying), separate
  payload and event replication streams, and a more push-focused replication
  architecture. [WL]
- Low priority: make `BLOCKING_DECODE_BYTES` adaptive instead of a CPU-tuned
  const (piecewise decode-time model, startup calibration first). Details in the
  code comment. [WL]

## Operability and quality of life

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
  defaults do. [USER]
- Relational settings nudges (soft warn, not reject): for example warn when a
  failover-sensitive timeout is set below the failure-detection cadence. Needs
  cross-setting advisories at config load and runtime PUT, plus an inline admin
  hint. Pairs with settings tiering. [USER]
- Eager opt-in startup recovery: `recover_all` exists but is unused (recovery is
  lazy via `queue_handle`). A config to eagerly recover all on-disk partitions at
  boot makes `recovery.on_mismatch = refuse` a literal refuse-to-start. Lazy
  stays the default. [USER]
- Snapshot cadence: wire `snap_cfg.every_events` as an additional knob alongside
  the time and dirty triggers. The gate is commented out in
  `periodic_snapshot_step`, `last_snapshot_event_offset` is already tracked, so
  wiring is low-risk. Currently `#[allow(dead_code)]` with a FIXME. [USER]
- Onboarding and easy trial (move on this soon): make "cluster from nothing"
  genuinely fast and low-ceremony. The local tryout still needs a clone and a
  build, so it is not really a 60-second path. Most of the pipeline is already
  done: a `Dockerfile`, a CI job (`server-docker-image`) that publishes
  `ghcr.io/<owner>/fibril-server` (`:main` and `:sha-...`) on every push to main,
  and a single-server `compose.server.example.yaml`. Remaining to reach a
  one-command trial:
  - Make the GHCR package public so `docker run` and compose work anonymously
    (a GitHub package-visibility toggle, not a repo change).
  - Add a multi-node cluster compose (a few `fibril-server` services in ganglion
    mode, shared network, service-DNS peers, bootstrap on one) plus the admin
    dashboard, so a coordinated cluster comes up in one `docker compose up`.
  - A curl-to-shell bootstrap that fetches and runs that compose for a single
    pasted command. Offer the compose itself as the safe default. This is what
    would earn back a real "60 seconds from nothing" claim.
  - In-memory (non-durable) mode for an even lighter trial. [WL/USER]
- Admin dashboard: a lost-connection banner. When the admin page can no longer
  reach its broker (broker down, failover, network blip), show a clear banner
  instead of silently stale data. [USER]
### Queue lifecycle, retention, expiry (ordered plan) [USER]

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
   REDELIVERY path (inflight -> requeue, never ack), not age-drop. Approach
   (user): a map in queue state like the lease heap - a RangeMap if expiry ranges
   are commonly contiguous (a span of offsets sharing a deadline) - wired to the
   expiry worker. Never expire an inflight message, and dropping can happen
   during the same `collect_expired` tick. Decide: optional per-message
   expires-at on the wire vs a per-queue default TTL (likely both - prefer adding
   an optional field to the existing publish over new ops/frames). Client
   convenience: a publisher SETTING, e.g. a `publisher.expiring(ttl)` builder
   (not necessarily a separate type), that stamps a default expiry on every
   publish. Composable with the other publish modes. The ttl arg follows the
   client's existing Delayable-style trait - accept either a bare number (seconds)
   or a Duration, same as the delayed-publish API.
7. **Queue expiration (auto-delete idle queues).** A DECLARE-TIME queue setting
   (the queue carries its own idle-TTL, e.g. expire-after-idle), not an external
   config. A sparse worker destroys queues idle beyond their declared TTL. Builds
   on delete (#4) + idle tracking.

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

- Plexus: a fan-out or stream channel type built on its own arc, enabled by the
  substrate-versus-engine split below. [DN]

## Clients

- Per-client feature matrix lives at `clients/FEATURE_MATRIX.md` (Rust reference
  vs each client). Keep it updated as bricks land - it is the at-a-glance parity
  view and the checklist for any new client.
- `clients/ARCHITECTURE.md` is the language-agnostic design reference (layering,
  invariants, continuity/routing/reliability models, porting lessons). Read it
  before starting a new client (the Python client is next).
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
    Exclusivity is enforced by the broker per-partition gate. STILL TODO
    (actionable, see below): consume the AssignmentChanged push as an
    assignment-events stream.
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
  - ACTIONABLE client gaps (server support EXISTS - verified in code 2026-06-21,
    correcting an earlier wrong "server-gated" call):
    - Live-grow partition pickup. The broker ships live repartition (grow by a
      multiple, shrink by a factor) in cluster mode: `fibrilctl repartition`,
      `/admin/api/repartition` (gated on ganglion), and broker transition
      machinery (`repartition_transitions`, hold/release delivery during the
      drain) in `crates/broker/src/broker.rs`. After a grow the TS consumer does
      NOT pick up the new partitions (its fan-in set is fixed at subscribe). The
      Rust client does, via `partition_resubscribe_loop_*`. Port that loop.
    - Assignment-events stream. The broker DOES push `AssignmentChanged` to
      exclusive-cohort members (`crates/protocol/src/v1/handler.rs`
      spawn_assignment_forwarder). The TS client ignores the op. Port the Op
      decode + an `assignmentEvents()` stream (Rust has it). Observability and
      future client-side partition narrowing, not correctness (the per-partition
      gate already enforces exclusivity).
  - Effectively-once: TWO paths. (a) CLIENT-ONLY and actionable - a consumer-side
    dedup helper that skips already-seen (producer_id, seq); the headers already
    reach the consumer, so no server change is needed. (b) SERVER-GATED - broker
    dedup that drops dups at publish (only a "read by broker producer-dedup later"
    comment exists). At-least-once today until one of these ships.
  - SERVER-GATED client items (build the client half when the server side lands):
    - Lease preservation across re-subscribe (a shared at-least-once limitation).
  Pairs with AUDITS.md "Client API parity" and the client reliability docs item.
  [USER/AUDIT]
  See clients/ARCHITECTURE.md for the design reference and clients/FEATURE_MATRIX.md
  for status. Next client: Python.

## Code health and structure

- Rework the `tui-example` (`crates/tui-example`): a small TUI app that connects
  to a broker and visualizes messages (packs) flowing in and out. It has disabled
  instrumentation (latency tracking + compute_stats were dead, removed in the
  dedup sweep). Bring it back to a clean, illustrative live-client demo. Also
  `benches/bin/bench_e2e.rs` is half-disabled (dead channels, hardcoded
  reporter/broker params) and wants the same treatment. [USER]
- `stroma.rs` by-concern file split: a readability refactor independent of
  clustering. A full module sketch is preserved in the archived worklog. Do the
  low-risk type modules first, then the engine impl split incrementally. [WL]
- substrate-versus-engine split (WorkQueueEngine plus StreamEngine over a shared
  substrate), which enables Plexus. [DN]
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
- Keep the implemented-surface inventory current. It is critical and easily falls
  out of date. [MEM]

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
  actionable (several are Audited with open Next items). [USER]

## Far horizon (v2+)

- Multi-region and geo placement: region and zone labels feeding the planner,
  per-queue placement hints, region-aware placement strategy. [PLAN]
- Load-aware placement and routing: node and partition load scores (advisory,
  off the coordination log, hysteresis, power-of-two-choices), plus a consumer
  scheduling policy and a per-consumer override of the global partition target. [DN/MEM]
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

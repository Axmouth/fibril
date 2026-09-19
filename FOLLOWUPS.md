# Follow-ups and pending work

## Priorities

1. Make checkpoint installation and backfill safe under interruption and
   promotion. [CHECKPOINT_RECOVERY_PLAN.md](CHECKPOINT_RECOVERY_PLAN.md) defines
   the reproduced failures and acceptance checks.
2. Complete stream settlement semantics and reconnect continuity coverage.
   [GATE3_RECONNECT_PLAN.md](GATE3_RECONNECT_PLAN.md) defines this work.
3. Complete domain typing, wire/storage compatibility policy, the five-client
   API review, and automated compatibility enforcement (#109-#112).
4. Add node enrollment and operational workflows for cluster management.

Current capabilities, interfaces and limits belong in
[implemented surface](website/src/content/docs/implemented-surface.md).
Detailed decisions for completed work are retained in
[implementation notes](archive/implementation-notes/FOLLOWUPS_COMPLETED.md).

## Partitioned queue ordering exploration

Priority: later research. Assess useful ordering guarantees across a partitioned
queue before selecting an implementation or adding a public promise.

Questions:

- Which ordering scope is required: one producer, one key, one partition, or the
  entire logical queue across producers and partitions?
- Does the guarantee concern publish acceptance, delivery, processing start, or
  processing completion? Concurrent consumers can preserve one and violate another.
- How should retries, delayed messages, lease expiry and redelivery affect order?
- What happens when an owner fails, a consumer reconnects, or repartitioning moves
  a key between partitions?

Compare key-affinity/serial-per-key delivery, sequence-aware consumer merging,
watermarks and explicit sequencing. Measure head-of-line blocking, slow or missing
partition behavior, memory bounds, latency and sustainable throughput. State which
approaches require an opt-in mode or application participation.

Deliverable: documented semantics and tradeoffs, a small prototype only where
needed to test a candidate, and fault/repartition scenarios that verify the
proposed guarantee. Preserve durability and at-least-once settlement semantics.

## Storage and snapshot compatibility

Define a support window and explicit migration rules for queue event records,
queue snapshots, log manifests/headers, coordination WAL/snapshots, global-store
documents and replication checkpoints. Each format needs version dispatch and a
retained decoder for supported structural versions. Additive fields require an
explicit reader/writer compatibility rule for that encoding.

Extend the existing standalone storage fixture coverage to coordination data,
persisted users and runtime settings. Use recovery tests to verify payloads,
settlement state and cursor positions; quarantine or silent state loss must fail
the compatibility check. Generate new fixtures from each selected baseline.

Add a mixed-broker rolling-upgrade lane alongside previous-client compatibility:
select baseline and candidate binaries, run drain/restart/failover under load,
and verify confirmed IDs and convergence. The policy must state supported
baseline versions rather than infer support from a fixture filename or tag.

## Node enrollment

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

Use the existing shared-CA certificate minting in `fibril-tls`, inter-broker
TLS for post-join traffic, and admin HTTPS for invitation redemption.

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

## Client lifecycle completion

See [GATE3_RECONNECT_PLAN.md](GATE3_RECONNECT_PLAN.md) for stream settlement,
resubscription continuity and acceptance criteria. Public-contract decisions must
precede the client API freeze.

## Compatibility freeze (#109-#112)

Goal: 1.0's compatibility promise. Sequenced after the reconciliation
family (its wire additions land first) and after security settled the
handshake.

- #109 newtype + Arc<str> pass: Offset/Epoch-style domain integers get
  distinct serde(transparent) newtypes (Partition already is one,
  DeliveryTag exists as a struct in crates/common/src/lib.rs:12).
  Current starting anchors: `pub type Offset = u64` remains at
  crates/broker/src/storage.rs. Review epoch integers throughout the
  coordination and storage boundary. The Rust client already has validated
  TopicName/GroupName wrappers backed by Box<str>; the proposed Arc<str>
  direction and end-to-end domain typing are not complete.
  Pure mechanical churn - do it IMMEDIATELY before freezing so nothing
  re-churns after. Compiler does the work; tests are the existing
  suites passing. Newtyping wire-visible fields must stay
  serde(transparent) so no wire bytes change (the vectors gate below
  proves it).
- #110 wire versioning + back-compat policy: PROTOCOL_V1 and the HELLO
  negotiation already exist in the shared crates/wire vocabulary. Write
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
- #111 client API freeze: the public-API review across Rust/TS/Python/Go/C#
  against FEATURE_MATRIX; the typed subscription-lifecycle enum from the
  reconciliation family is ratified here; deprecations resolved; then
  semver discipline begins (breaking = major).
- #112 compat matrix + enforcement: a CI job running explicitly selected
  baseline clients (published packages or pinned source revisions) against the
  candidate broker image for a smoke (connect, publish, consume, stream,
  TLS) - the matrix in docs is generated from what CI actually proves,
  never hand-claimed. The mixed-version BROKER lane from the
  storage compatibility plan (cluster-tryout.sh --mixed-version <baseline-image>)
  lands here too, using reproducible baseline and candidate images.

Invariant: nothing merges to main after the freeze commit that changes
wire bytes for existing frames (the vectors gate) or breaks the
previous-client smoke (the matrix gate).

## Authorization exploration

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

## Tenancy examination (deferred question, criteria recorded)
Decide AFTER authz ships, by answering: do groups-as-namespace-prefixes
plus per-user authz rules already give tenant isolation for the real
asks (credential isolation: yes after authz; quota isolation: no -
would need per-principal rate/storage limits, a separate arc; admin
isolation: no - dashboard is cluster-wide)? If the missing pieces have
no concrete demand, tenancy stays a documentation pattern (prefix +
authz rules), not a feature.


## Deferred client and simulation coverage

- Add stalled-broker heartbeat-timeout wording tests across all five clients,
  using the shared error-guide fixtures.
- Consider a C# dependency-injection companion with options binding once the core
  API is stable; keep framework dependencies in that companion.
- Make Ganglion's `client_write_remote` transport injectable when simulation
  scenarios need writes forwarded from non-leader or non-member callers.

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

## Performance research

- Assess storage encoding changes against the staging-efficiency baseline:
  (a) keratin encode_record builds the fixed
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

## Correctness and durability

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

- Design opt-in reclamation of cold orphaned partitions after authoritative
  coordination checks, with a recoverable move-aside option. Test cold ownership
  re-acquisition: a stale local log must catch up or be fenced before serving.
- Investigate confirmed ephemeral-stream throughput and light-load latency under
  controlled client pacing. Assess exposing the periodic ephemeral flush cadence
  as a runtime setting if measurements justify it.

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

- Complete ordered advertised-endpoint probing across clients, with a bounded
  connect timeout per candidate and service-name support in the Rust connection
  pool. Assess listener-selection conventions if failover latency needs them.
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
- Admin dashboard: a lost-connection banner. When the admin page can no longer
  reach its broker (broker down, failover, network blip), show a clear banner
  instead of silently stale data. [AUTHOR]
### Queue lifecycle, retention and expiry

- Complete coordinated multi-node deletion: remove the authoritative catalogue
  entry, stop placement and catalogue re-registration, and destroy every replica
  in an order that prevents resurrection.
- Implement replicated purge across message-log truncation, state reset and the
  reset event. Verify recovery, follower catch-up and concurrent operations at
  each boundary.
- Add queue retention by age using a time-to-offset lookup and safe truncation,
  with per-queue policy and bounded background work.
- Design queue expiration with coordinated activity tracking across all
  partitions and replicas. Reuse the coordinated deletion workflow.
- Review the reserved richer NACK vocabulary and decide which operations warrant
  a public contract before wiring them through storage, protocol and clients.

Retention and idle-expiration settings should use consistent declare-time
millisecond fields and survive replication and snapshot recovery. Each worker
needs a bounded batch size and a configurable cadence.

## Stream settlement

[GATE3_RECONNECT_PLAN.md](GATE3_RECONNECT_PLAN.md) specifies the remaining stream
NACK contract and resumed cursor-settlement work, including cross-client checks.

## Stream and filtering exploration

Assess keyed-index reads for efficient selective stream consumption when a
concrete workload needs them. Queue-side filtering requires bounded scanning,
no-match handling and fairness guarantees before it can be considered.

## Clients

Use [FEATURE_MATRIX.md](clients/FEATURE_MATRIX.md) for current parity and
[ARCHITECTURE.md](clients/ARCHITECTURE.md) for shared invariants.

- Measure single-client and multi-client publish/delivery capacity across all
  five clients at stated payload sizes, confirmation depths and storage settings.
  Use unpaced saturation runs alongside controlled-rate latency runs.
- Profile scheduling handoffs, encoding/decoding, write batching and backpressure.
  Validate fatal mid-burst failure, ordering, shutdown and topology changes for
  each optimization. Apply shared improvements across clients where relevant.
- Add multi-node client smokes that force a real cross-owner redirect and verify
  subscription recovery, including a broker bounce before failure detection.
- Assess consumer-side producer-id/sequence deduplication as a separate opt-in
  helper. Broker-side producer deduplication remains a distinct correctness task.
- Define lease preservation across re-subscription before exposing that guarantee.
- Consider a Python 3.10 backport if users need it; retain the asynchronous core
  and blocking facade, and isolate runtime-version dependencies.

## Code health and structure

- Extend pattern-subscription integration coverage: automatic attachment in Rust
  and Python, and stream-pattern delivery across all five clients. Assess whether
  pattern subscriptions need a dedicated user guide.

- Extend Keratin writer tests for append ordering, shutdown flushes and failure
  boundaries. Reassess pipelining only against measured workloads.
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
- Optional de-raft finish: fibril-side de-raft is complete. Remaining is optional,
  routing the protocol dev-dep and coordination-ganglion through the `ganglion`
  umbrella crate, or the bigger approach-B of moving raft-node construction up out
  of fibril entirely. [WL]
- Ganglion domain hygiene: keep all coordination-domain code in ganglion so fibril
  depends on a stable surface, not internals. Ongoing. [DN/MEM]
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
- Update implemented-surface, the client matrix and relevant user guides in the
  same change as a public behavior or interface. Remove completed tasks from
  active plans and retain design rationale in implementation notes. Use
  [Documentation style](website/src/content/docs/development/docs-writing.md)
  for the maintenance workflow.

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

Historical design input: typed close reasons and stale-delivery settlement
have since landed across all five clients. Do not reimplement the lifecycle
from this older "Today" description. Review and ratify the actual surface at
#111; remaining proposals below are review questions, not accepted new scope.

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

PLAN SKELETON (2026-07-12 - settle the two design forks WITH THE USER before
coding):
1. Inventory pass: where the broker and admin listeners construct their TLS
   acceptors at startup (crates/fibril/src/lib.rs listener setup + the tls
   crate), and what the live-rotation path already swaps (cert material
   swaps inside an existing acceptor - this feature must swap
   plaintext -> acceptor-present, which is one level up).
2. Fork A (switchover): hard switch on an ArcSwap<Option<Acceptor>> checked
   per-accept vs a dual-listen grace window. Recommendation to bring to the
   user: per-accept swap with a drain-style notice to connected plaintext
   clients, no second port.
3. Fork B (persistence): reuse the first-boot setup overlay
   (apply_setup_overlay in config already loads a data-dir overlay at boot)
   so enabling TLS writes the same overlay with explicit UI consent, config
   file untouched.
4. Flow: Security page "Enable TLS" button -> modal (what happens, client
   migration note, consent checkbox for persistence) ->
   POST /admin/api/tls/enable -> generate self-signed via the existing
   setup machinery -> swap acceptors -> respond with fingerprint ->
   Security card flips to the served-cert view with a copy-paste client
   snippet.
5. Tests: route test (enable on a plaintext test server, assert acceptor
   present + fingerprint returned + overlay written), an e2e that connects
   a TLS client after enabling, and a plaintext-client-during-switch test
   pinning whatever guidance behavior fork A settles on.
6. Docs: security.md + configuration.md + admin-dashboard.md + CHANGELOG in
   the same change. FEATURE_MATRIX row only if the client surface changes
   (it should not).

## Dashboard follow-ups

- Add user-defined attention thresholds and external notification delivery; see
  the threshold-rules brief below.
- Review URL filter persistence, per-partition consumer coverage and storage
  breakdown against the current dashboard before selecting further UI work.
- Keep dashboard guides, endpoint inventory and screenshots aligned with changes.

## Known intermittent: one fibril-admin test flakes under full-workspace runs

Seen 2026-07-11, roughly 1 in 4 `cargo test --workspace` runs: the admin test
binary reports 71 passed / 1 failed, but solo `-p fibril-admin` runs and rerun
attempts stay green and the name was not captured. Likely a timing-sensitive
TCP-bound admin test under parallel load.

DEFLAKE PROTOCOL (execution-ready):
1. Capture the name. Loop until it fires:
   `for i in $(seq 1 20); do cargo test --workspace --no-fail-fast 2>&1 \
      | tee /tmp/suite-$i.log | grep -E "FAILED|test result: FAILED. " ; done`
   then `grep -B2 "FAILED" /tmp/suite-N.log` for the test name.
2. Reproduce it solo under contention. Run the single test in a loop while a
   parallel `cargo build --workspace` churns CPU and file handles:
   `while cargo test -p fibril-admin <name>; do :; done` in one shell,
   `cargo clean -p fibril && cargo build --workspace` in another.
3. Classify before patching (trace-first rule):
   - wall-clock assertion (sleeps, elapsed comparisons) -> convert the test
     to tokio paused time (`#[tokio::test(start_paused = true)]`) or await
     the state change explicitly instead of sleeping.
   - TCP bind/accept race (tests bind 127.0.0.1:0) -> check for hardcoded
     ports or reuse of a just-closed listener, await readiness not sleep.
   - SSE/tick timing (events.rs families) -> subscribe first, then trigger,
     then read with a generous timeout, never assert tick counts.
4. Acceptance: 30 consecutive `cargo test --workspace` runs green, and the
   fixed test documents WHY it was flaky in a comment.


## Memory follow-up

Identify the residual per-materialized-partition memory cost recorded in
[MEMORY_AUDIT.md](MEMORY_AUDIT.md), using the existing regression probe.

## Future: mirrored ring orientation flag (2026-07-12)

The cluster diagram's tendril renderer is fully parametrized around one ring
orientation (3/4 view, right side turned away: strands under the band left,
over the band right, cut at 0.38r). Add a per-ring flag that renders the
opposite orientation seamlessly: flip the sprite horizontally (SVG image
transform), swap the under/over side logic in the batch pass and joinAt's
`over` field, mirror the cut to the left band, and flip pulseHidden's
which-side test. Would let facing direction vary per ring (or alternate by
grid column) so big grids read less uniform.


## Future: notification threshold rules (brief 2026-07-12)

User-configurable rules layered on the attention system, e.g. "warn when
backlog on topic X exceeds N" or "warn when disk free drops under Y%".

DESIGN DECISIONS (made): rules evaluate SERVER-side in
crates/admin/src/attention.rs so the panel, the Activity feed, the SSE
stream, and desktop notifications all inherit them for free. Rules are
broker state, not per-operator preference, so they live in the
runtime-settings document (cluster-replicated, versioned, lock-aware) as a
new OPTIONAL section - old settings documents must keep loading, so the
field is Option<Vec<Rule>> with a permissive default, and the settings body
deliberately does NOT take deny_unknown_fields (see the admin DTO
hardening note).

Rule schema v1 (consult the user on the kind set before building):
  { id: string (user-chosen, unique), kind: backlog | disk_free_pct |
    delivery_stall, scope: { topic, group } with glob support for backlog
    kinds, threshold: number, severity: warning | critical, enabled: bool }

PLAN:
1. Settings: add the section to RuntimeSettings (broker crate) + locks
   entry + validation (unique ids, sane thresholds, known kinds).
2. Attention: each tick, evaluate enabled rules against the same snapshots
   the built-in rules already read (queue depths, disk stats, follower
   progress). Emit conditions keyed `rule:<id>:<scope-instance>` so
   raise/resolve dedup and notification keying work unchanged.
3. UI: a Settings-page card listing rules as editable rows (add/remove,
   kind dropdown, scope fields, threshold, severity, enable toggle),
   saving through the normal runtime-settings PUT with expected_version.
4. Tests: rule triggers and resolves through the attention payload, a
   settings roundtrip with rules present, an OLD document without the
   section still loads, invalid rule rejected with a guided error.
5. Docs: admin-dashboard.md attention section + CHANGELOG same change.

## Future: dashboard live metrics drilldown (brief 2026-07-12)

Semi-live per-queue and per-stream throughput on the list views, plus a
click-through detail popup. Prior decision (memory): build on existing
debug endpoints, no new broker-side per-queue rate state.

PLAN:
1. Verify what queues_debug and streams_debug already carry per queue
   (message counts / positions). The dashboard receives them on every SSE
   tick, so successive ticks give rates by client-side delta - same
   pattern as the S4 connections view.
2. List views: a rate column (published/s, delivered/s for queues,
   append/s for streams) computed in JS from the last two ticks, with the
   existing trend-tint grammar (growing backlog warns).
3. Popup: clicking a row name opens a modal with mini 5-minute charts
   built from a client-side ring of recent ticks (per open page, resets on
   reload - acceptable and stated in the UI as "since this page opened").
   Reuse the Overview chart drawing helpers.
4. Tests: none server-side (no server change expected). Template JS check
   + screenshots. CHANGELOG + admin-dashboard.md.

## Future: Python msgpack fallback to JSON (brief 2026-07-12)

Today a dict/list payload without the optional msgpack extra hard-errors
(why the Python CI needs --extra msgpack). Wanted: default structured
payloads to JSON when msgpack is absent, mirroring what the payload was
going to be anyway for JSON-first users. Explicitly requested msgpack
(content_type msgpack or an explicit encode call) still errors, with a
guided message naming the missing extra.

PLAN:
1. Locate the encode fork in clients/python (the payload-encoding helper
   that raises on missing msgpack import).
2. Absent extra + structured payload -> JSON encode + the JSON content
   type. Absent extra + explicit msgpack request -> the current guided
   error. Present extra -> unchanged.
3. Check the Rust reference client's default first and mirror it (the
   matrix rule: verify the support set before widening). If Rust defaults
   structured payloads to msgpack, note the cross-client divergence in
   FEATURE_MATRIX explicitly.
4. Tests: a no-extra job leg (uv run without --extra msgpack) covering
   dict publish -> JSON on the wire, plus the guided error for explicit
   msgpack. Keep the with-extra leg green.
5. FEATURE_MATRIX + client docs pages + CHANGELOG (clients intro list
   stays current).

## Future: client perf round 2 (brief 2026-07-12)

Three sittings, each bench-driven on the reference box (Ryzen 9 5950X,
970 EVO Plus, single box - cite hardware per the benchmarking rule), with
an A/B microbench before any optimization lands (validate-hotspots rule).

1. Coalesce params public + self-adapting. The client write-coalescing
   windows are internal constants today. Make them public client options,
   then derive defaults from measured behavior (adaptive-tunables
   direction: bounds as config, hysteresis, derived values observable).
2. Ack/deliver coalescing. The server->client delivery path and the
   client->server ack path both send small frames; batch them under load
   the way publish coalescing batches writes. Wire-visible only as fewer
   syscalls - byte format unchanged.
3. The encoding wall. Past coalescing, encode cost dominates the client
   CPU profile. Investigate buffer reuse / zero-copy paths in the Rust
   client first, then port wins to TS/Python if they replicate.
Each sitting: strace/flamegraph proof, FEATURE_MATRIX row when a public
option lands, BENCHMARKING.md numbers refresh, CHANGELOG.

## Future: live fake admin board in the docs (brief 2026-07-21)

A demo dashboard embedded in the website: the REAL admin assets (captured
page shell + the version's CSS/JS) rendered against canned data, so
visitors see the board alive without running anything. Replay, not a
hosted broker (no ops burden, no admin abuse surface).

Shape:
- Data source: run fibril-demo against a real broker and RECORD a window
  of every /admin/api/* response plus the SSE event stream (a small
  recording proxy or a capture flag). Pick a window containing one
  sluggish-napper arc so the attention feed shows backlog rising and
  resolving on loop.
- Transport seam: data enters admin.js through two doors, fetch and
  EventSource. A demo-mode flag swaps both: fetches resolve from the
  fixture map, a fake EventSource replays the recorded event log on
  timers, looping, with timestamps rebased client-side so charts and
  relative times read live.
- Read-only: mutation endpoints (declare, drain, test publish) stub with
  a "demo board" toast. Auth bypassed. The live-pill machine and the
  passive /healthz probe get stubbed healthy.
- Drift: fold "record demo fixtures" into the per-release docs snapshot
  step, so fixtures always come from the same version as the assets.

## Future: fibrilctl + admin API cookbooks (brief 2026-07-12)

Two website pages in the BENCHMARKING.md copy-paste-recipe style: common
operator tasks, each as one runnable block with expected output. Candidate
recipes: declare/inspect/delete a queue, publish a test message, inspect
messages with filters, replay dead letters, drain a broker, repartition,
manage admin users, issue certs, check cluster membership and topology.
METHOD: launch a scenario.sh sandbox broker, run every recipe against it
verbatim, paste real output. A recipe that was not executed does not ship.
Home: website/src/content/docs/operations/ (new pages), linked from
admin-dashboard.md and the CLI section. CHANGELOG.

## Future: TUI demo rework - remaining milestones (brief 2026-07-12)

The tui-example rework was mid-flight when parked. Remaining milestones:
1. Cohort partition ownership display: show which consumer in the cohort
   owns which partitions, live, as the churn scenario reassigns them.
2. Website single-command tryout: one copy-paste command that launches
   the TUI demo against a fresh broker (decide cargo-run vs container).
3. Live repartition keys: a keybinding that triggers a repartition so the
   ownership display visibly rebalances during a demo.
Session cursor with the exact mid-edit state lives in assistant memory
(tui-demo-cursor) - read it before resuming.

## Future: per-partition edge rates for the diagram (brief 2026-07-12)

S3 links pulse by the OWNER's node rate (proxy). Honest per-edge rates need
per-partition throughput on the heartbeat. Before building, assess cost:
one label per owned partition doubles label churn on wide brokers - prefer
a single JSON label mapping partition -> bucketed rate (buckets, not raw,
to keep the committed snapshot quiet). Then drawLinkJob takes the summed
buckets of the partitions riding each edge instead of the owner bucket.
Gate on actually noticing the proxy being wrong in real use.

## Dashboard list and settings refinements

Use [the archived mockup](archive/admin-mockup-reference.html) as the visual
reference for remaining refinements:

- Review queue and stream rate columns, group spacing and sparkline drilldowns
  against the current implementation. Preserve left-aligned group labels.
- Assess a cheap oldest-ready-message timestamp without adding storage scans.
- Expose defaults alongside current runtime settings so changed values can show
  their default, and add a recent-settings-changes strip from the audit ring.

## Demo lifecycle scenarios

Add optional repartitioning and node-churn scenarios around the existing
`fibril-demo` workload. Coordinate node lifecycle through the scenario runner.

## Durable declared-queues catalogue in stroma (2026-07-17)

Incident: a stray dir named `deleted` inside a topic's on-disk tree refused
the whole engine open ("bad partition dir"). Fixed in keratin be2b943 - the
scan now finishes interrupted destroys (.trash- leftovers) and skips unknown
dirs with a warning instead of failing the boot, regression-tested.

The deeper direction the user wants: stop deriving the queue set from
directory scans and persist the DECLARED catalogue in stroma storage itself
(topics, groups, partition counts, policies). Boot then reconciles disk
against the catalogue instead of trusting dir names, which also gives honest
detection of missing/extra partitions and folds into the queue-lifecycle
plan above (declare_partitioning as the authority) and the golden-fixture /
back-compat story. Design it with the storage back-compat brief - a new
persisted structure needs versioning from day one.

## Transient 404 on queue publish during standalone boot (filed 2026-07-17)

Seen once per restart-under-load: a queue publish arriving in the boot
window gets 404 "topic/0 is not declared in the cluster" on a STANDALONE
broker, then heals on the client's next retry. Two things to fix when
touched: the boot window itself (queue index warm racing the listener,
same family as the stream warm-up fixed in the same change), and the
message (it names "the cluster" on a broker that has none). Repro: run
fibril-demo, restart the broker mid-day.

## Admin catalogue of recent internal errors (2026-07-17)

Internal 500s now carry a trace id stamped on both the client message and
the broker log line, so operators have something to grep. The bigger idea:
a bounded ring of recent internal errors (trace id, time, error, op)
surfaced on the admin board - diagnostics page or the activity feed family -
so a client-reported trace id can be looked up without shell access to the
broker's logs. Rides the same in-memory ring pattern as the audit feed.

## Cluster stream/queue kind races - PARTIALLY FIXED, remainder filed (2026-07-17)

Found via fibril-demo against a 3-node scenario cluster. Fixed in the same
sitting: (1) redeclare-through-non-owner role-mismatch 500 that wedged
idempotent declares, (2) catalogue sync registering streams as queues
(spurious queue assignments + queue follower workers on stream partitions).
Both verified: full demo world declares through a non-bootstrap node,
assignment overlap between queue and stream catalogues is empty, ownership
spread healthy (6/5/5 queues, 2/2/1 streams).

STILL OPEN, with evidence captured:
1. KIND RACE at stream birth: during the convergence window a stream
   publish reached a broker before it knew the topic was a stream, went
   down the queue path, and AUTO-MATERIALIZED a queue-kind partition -
   observed end state: node-1 held kitchen.telemetry/0 as kind=queue,
   role=owner while coordination assigned it as a stream partition to the
   same broker. Also observed transiently as "expected Owner, current role
   is Frozen" (trace 578dee12 in that run). First-writer-decides-kind makes
   the damage durable. Direction: publishes must consult the authoritative
   kind (coordination stream config or a durable catalogue) before
   auto-materializing anything in cluster mode - ties into the durable
   declared-queues catalogue direction above.
2. Owners with assignments but EMPTY local queues_debug: brokers 2/3 owned
   5 partitions each yet listed nothing after 60s of demo traffic. Either
   materialization-on-owner is lazier than expected or routing kept all
   traffic on node-1. Trace before patching.
3. UI: the Queues page renders NOTHING below the filter bar when
   queues_debug is empty (the user's "blank page" in the tryout). It needs
   an honest empty state: the metrics strip at zero plus "this broker hosts
   no materialized queues - see the Cluster page for placement".

## Field-note triage (2026-07-17 scribbles + agent summary)

Actioned already: queues-page investigation (see the kind-races section
above - two fixes landed, empty-state UI + kind race remain), tryout demo
wiring (shipped), unreachable pill (shipped), trace ids on internal errors
(shipped; the error-catalogue idea is its own section above).

Filed for the attention arc (extends the threshold-rules brief above):
- Debounce/coalesce same-origin attention events in the EVALUATION layer
  (hold-before-raise or hysteresis-on-resolve) so built-in and future
  user rules both inherit it and cannot flap.
- The rules UI as a structured condition builder: metric dropdown +
  comparison + threshold + optional trend, independent rows, small fixed
  comparison set. No row composition, no syntax - demand pulls a real
  engine later or never.
- Scope decision BEFORE storage: queue/stream rules are cluster state
  (settings-replicated, owner-evaluated), resource rules per-node.
- Configurable grace/threshold on existing built-in conditions as the
  smallest first slice.
- External delivery as channels consuming the same condition-event stream
  the panel and desktop notifications already consume: a webhook POST on
  raise/resolve first, attention-as-Prometheus-gauges second. No native
  integrations.

Copy/vocabulary sweep (one sitting):
- "Auth disabled" top-bar pill vs connections "authenticated" chips read
  as a contradiction - scope the pill (admin auth) and the chips (client
  auth) with distinct wording.
- Action-verb audit across tables (Detail / Inspect / Open / Delete).
- "version: 0" on runtime settings reads like a build version - reword as
  revision.
- "Frozen" partition role could hint its cause (mid-transition).

Also noted: distinguish planned drain vs crash in the at-a-glance
indicators; connections diagram degradation at 10x scale; landing-page
hero shot of the demo under load; benchmark tiering (SATA/NVMe/tmpfs) with
conditions glued to numbers; latency-floor honesty in docs; Arc<str> for
Topic/Group hot paths; keratin clean-shutdown marker to skip integrity
checks on clean restart.

## Dashboard liveness and render failures

Scope the live indicator to the current page's data/render pipeline, or label
its broker-connectivity meaning explicitly. Unrelated successful API calls must
not mask a failed page render. Log swallowed render errors with throttling,
including failures after initial paint.

## Test-support unification (small cleanup)

crates/fibril/tests/common/mod.rs now carries the hardened broker boot
(port-collision retry + error surfacing + boot cap) and metrics_endpoint +
user_admin use it. tls_listener still carries its own equivalent copy
(independently hardened and 30x-verified) - fold it onto common in a quiet
moment. raft_tls binds its raft listeners immediately after freeing them
(tiny window) and was left alone.

## Thread-per-core assessment (2026-07-18, latency-first)

Verdict from the discussion: full sync rip-out is executor-rewrite class
for ~zero perf gain, but thread-per-core (runtime-per-thread, pinned
current_thread shards owning partition actors, SPSC between shards) is an
EVOLUTION of the existing sharded-actor design, not a rewrite - async
stays, openraft/axum untouched. The durable-publish p99 is floored by
coalescing windows + fsync (100us-ms), so TPC's us-class wins only matter
for (a) the speculative stream tier and (b) the deliver-path contention,
which may be cross-core cache bouncing - the disease TPC actually cures.
ORDER: 1) latency decomposition bench via e2e_c percentiles (client
coalesce / wire / handler / actor hop / append+fsync / confirm gate),
2) the deliver-path contention probe (already filed - its result IS the
TPC verdict), 3) a pinned-shard experiment: one pinned current_thread
runtime hosting hot partition actors behind the unchanged channel API,
bench the confirm-path and delivery deltas. Adaptive coalescing windows
remain the biggest durable-latency lever regardless.

THE MILLION TARGET (user 2026-07-18: "if we could do a casual million,
worth the grind"): 411k/s saturation at ~5.7/32 busy cores = ~72k/s per
effective core - clean sharding across ~14 cores hits 1M, so the ceiling
is contention, not cycles. Two checks gate the grind: (1) the 411k may be
LOAD-GEN limited (single box, e2e_c competes with the broker for cores) -
re-measure with multi-process or second-box load gen first; (2) TPC only
pays if the contention is shardable (cache bouncing, contended locks) -
a structural single-writer choke would survive sharding. Rule out the
cheap classic first: shared metrics counters false-sharing across cores
(fix = padding / per-core counters, no architecture needed).
PIPELINING NOTE: per-partition ordering serializes the ORDER, not the
work - stages of one partition's flow (parse/validate/append/fsync/apply/
confirm) can overlap across successive messages, Disruptor-style, so the
per-message serial cost is the longest stage, not the sum. Group commit
and the parallel msg/event fsyncs already do this for the worst stage;
the probe should also ask which stages still run INSIDE the serialized
actor context without needing to (payload encode, delivery selection vs
socket write, one-by-one confirm-gate wakeups).

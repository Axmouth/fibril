# Changelog

All notable changes to the Fibril repo (the broker, the Rust, TypeScript,
Python, Go, and C# clients, the admin dashboard, and the CLI) are recorded here.
Ganglion and Keratin track their own changelogs in their own repos.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). Pre-1.0, minor
versions may still change the API and wire protocol. 1.0 commits to stability.

## [Unreleased]

### Added

- The admin dashboard rebuilt as an operations surface. A sidebar shell with a
  command palette, live 30-minute throughput and backlog charts, a
  needs-attention panel computed by the broker (backlog with no consumer,
  certificate near expiry, failed settings load, quarantined partition - each
  with a link to its fix), per-queue depth sparklines and a queue detail page,
  a dead-letters page, connections and subscriptions views with identity-first
  tables, a security page (certificate status with live reload, admin users),
  the first drain button, and - in cluster mode - a top-bar switcher to any
  other broker's admin. State is color: leased work is blue, completions
  green, failures red, attention amber, everywhere. Dark and light modes plus
  named accent flavors (neuronic, chlorophyll, crimson, eosin, azure, iris).
- Dashboard filters persist in the URL: the search boxes on the queues,
  streams, connections, and subscriptions pages (and the hide-inactive toggle
  on queues) mirror into query parameters, so a filtered view survives reload
  and can be shared by copying the address.
- A scenario builder for demos and dashboard verification:
  `scripts/scenario.sh` runs a scripted sequence of operator actions (declare,
  publish bursts, consumers, test messages, deletes, drains, node kill and
  restart in cluster mode) against script-managed brokers on non-default
  ports, with example scenarios under `scripts/scenarios/`. The `e2e_c` bench
  client gained an `--addr` flag to point at any broker.
- Existing names suggest themselves: the message-inspection topic and group
  fields and the declare panel's group and dead-letter fields are native
  comboboxes now, lazily filled from the queue catalogue - one keystroke to an
  existing name, free typing for new ones.
- Six more attention rules, so the needs-attention panel (and the desktop
  notifications and Activity feed built on it) covers the conditions that
  hurt most when unnoticed: the data directory's filesystem running low
  (warning under 10% free, critical under 3%), a replication follower that
  is behind and has stopped advancing, a backlog growing despite live
  consumers, a queue whose state reports an error short of quarantine, a
  broker left draining, and stream subscribers repeatedly lag-recovering
  (as Activity entries). An expired certificate now escalates from warning
  to critical.
- Desktop notifications for new attention conditions, opt-in from the
  settings page: critical only, or critical and warning. The browser is asked
  for permission on enable, the choice lives in that browser alone, and a
  page load never replays standing conditions as fresh alerts.
- Optional resource graphs on Overview: a collapsed panel with memory, CPU,
  and disk over the last 30 minutes (the history sampler now records them).
  The open state sticks per browser.
- An Activity page: a live control-plane feed of what happened on this broker
  - queue and stream declares, deletes, drains, operator test publishes,
  attention conditions raised and resolved, and cluster membership changes -
  with severity-colored entries and a filter. Served at `/admin/activity`,
  backed by `GET /admin/api/audit` (a bounded in-memory ring of the newest 512
  entries, reset on restart) and pushed live over the events stream.
- More at-a-glance color on the queues and streams views. Queue depth numbers
  take a tone from their 30-minute trend (a growing backlog warns, a draining
  one reassures), publisher counts light up when live, stream partition roles
  become chips, live stream subscriptions get a per-topic chip, and lag
  recoveries warn when nonzero.
- The dashboard's data now arrives over one server-sent-events stream per open
  page (`GET /admin/api/events`) instead of polling every two seconds. The
  broker pushes each page's data families on its own tick, serializing each
  family once no matter how many pages watch, and does no work at all while no
  dashboard is open. Pages fall back to the previous polling automatically
  when the stream is unavailable, and the live pill tracks the stream's
  health.
- Storage breakdown on the Overview disk stat: a segmented bar under the disk
  number shows which queues the bytes belong to (largest first, split into
  message-log and event-log bytes in the admin payload), so "disk is growing"
  comes with "and this queue is why".
- Per-partition consumer coverage on the queue detail page. When a queue is
  consumed by an exclusive cohort, each partition card names the member whose
  live subscription covers it and flags uncovered partitions, and the
  subscriptions page's cohort table lists each member's partitions. The
  cohorts admin payload now carries live per-partition coverage alongside
  membership.
- Publish a test message from the dashboard. The queue detail page and each
  stream card gain a button that sends one text message through the broker's
  real publish path (`POST /admin/api/publish`) - partition pick, durable
  confirm, and delivery all engage, so an operator can verify a queue or
  stream end to end without a client. Test messages carry a reserved
  `fibril.test: admin` header, which clients cannot forge, so consumers can
  recognize operator traffic.
- Admin dashboard mechanisms behind the new operations views: a per-broker
  in-memory time series (`GET /admin/api/history`, sampled every 5s over the last
  30 minutes) for throughput and backlog-over-time; a server-computed attention
  feed (`GET /admin/api/attention`) that names conditions needing an operator -
  a backlog with no consumer, a certificate near expiry, a settings document
  that failed to load, a quarantined partition - each with a link to its fix;
  and served-certificate metadata (`GET /admin/api/tls`: fingerprint, validity,
  subject). All in-memory and reset on restart; Prometheus (`/metrics`) remains
  the durable history.
- Go client (`clients/go`), the fourth first-party client, at full parity with
  the others. Native `context.Context` on every network method, a byte-exact wire
  codec pinned to the shared conformance vectors, TLS, cluster routing with
  reconnect and reconcile, supervised, multi-partition fan-in, exclusive-cohort,
  Plexus stream, and pattern subscribe, ergonomic message and publisher handles,
  examples that double as integration tests, benchmarks, and CI.
- C# client (`clients/csharp`), the fifth first-party client, at full parity with
  the others. A byte-exact wire codec pinned to the shared conformance vectors,
  TLS and mTLS, cluster routing with reconnect and reconcile, supervised, fan-in,
  cohort, Plexus stream, and pattern subscribe, and a reliable publisher. Ships
  unit tests, real-broker examples, and CI.
- Cross-client parity fills. The TypeScript and Python clients gained the
  exclusive-cohort shorthand, and the Python client gained delayed publish with a
  confirmation handle, so all clients cover the same surface. A shared feature
  matrix and shared conformance fixtures (wire vectors and error-message guides)
  keep the clients byte- and word-identical.
- Explicit-unit durations in the TypeScript client. `publishDelayed`, `retryAfter`,
  and `expiring` now accept `{ seconds }`, `{ ms }`, or `{ minutes }` in addition to
  the millisecond number and a `Date` deadline, so the unit can be explicit at the
  call site. Non-breaking - a bare number is still milliseconds.
- Storage config passthrough for Keratin segment preallocation
  (`storage.keratin.segment_preallocate_bytes`, off by default, also settable via
  the `FIBRIL_KERATIN_SEGMENT_PREALLOCATE_BYTES` env override), so operators can
  preallocate segment space ahead of the write cursor and cut durable-publish
  latency on consumer NVMe at low load. The fsync-fusion tunables are exposed too
  (`storage.keratin.max_inflight_fsyncs`, `storage.keratin.pipeline_commit_records`,
  and the matching `FIBRIL_KERATIN_*` env overrides).

### Changed

- Tendrils grew into the brand look (second live review): every live ring
  carries a packed batch of organic stems threading through it and forking
  into branches on both sides, links between brokers are one branching stem
  per pair (thicker with more shared partitions) plus one brighter organic
  consensus strand, and all movement is slower and more contained. The
  broker switcher paints its label instantly from a per-broker cache and the
  live pill shows a neutral "connecting" state before the first fetch, so
  switching brokers no longer jolts the top bar. The queues page's follower
  table moved below the queues under a clearer title ("Replication status -
  copies this broker follows").
- The consensus layer joined the diagram: the heartbeat now carries each
  node's raft id, so the leader marking finally correlates in real clusters
  (raft ids never matched broker node ids before) and faint gold strands
  connect the leader to every voter - the consensus mesh visible alongside
  the data-replication bundles. Tendril bundles also gained body: strands
  spread across the ring opening in lanes instead of converging on its
  center, and a dead broker's ring no longer sneaks a blink. Links now fuse
  onto the fiber tip whose direction best matches the peer and melt into
  that fiber's color across the junction, and the lanes through the opening
  undulate gently instead of reading as ruler-straight lines. Bigger
  clusters extend instead of shrinking: rings keep their full size up to 24
  brokers with the grid growing downward, and a Fibers toggle turns the
  animated layer off (it defaults off past 12 rings, and either choice
  sticks). Fuse points are welded - every strand strokes a dark outline
  under its color, so whichever path drew second used to bite a dark seam
  across the other. And the gold consensus mesh moved behind a Consensus
  toggle, off by default: membership already implies a connection to the
  leader, and hiding it lets the data links breathe. Grid rows gained air
  (cells wide enough that neighbouring fans clear each other), strands
  emerge from behind the ring band with no hidden stretch (the over-layer
  cut now sits on the band's measured inner edge), and links between close
  neighbours draw one gentle curve instead of folding into hairpins.
- Checkboxes and radios follow the flavor accent instead of the OS default
  color, and the danger red (delete and drain buttons, failure badges)
  sharpened from a salmon that read as the default flavor's accent to an
  unmistakable red in every flavor.
- Cluster diagram, continued: grid columns follow the actual container
  width (a wide screen fits five full-size rings per row), the over-band
  strand redraw clips the identical path instead of stroking an
  approximation (the last visible seam inside the ring opening is gone),
  and the leader's glow paints last so strands on its turned-away side
  carry the gold cast too.
- The Cluster diagram came alive, then got its first live-review polish:
  replication links draw as proper fiber bundles (three to five snaking,
  outlined strands with layered waves, tapered into their rings) instead of
  thin curves, the consensus leader breathes under a soft gold glow, brokers
  whose coordination heartbeat stops now show as X-eyed ghosts immediately
  (the topology payload carries a per-node `live` flag, and the Activity
  feed's joined/left entries follow heartbeats rather than registration),
  and blink cadence is floored so even busy rings stay calm.
- The Cluster diagram came alive. Each broker renders as the fibril ring
  itself: active nodes blink more often than idle ones, the band lights
  breathe, and a broker that vanishes from the cluster lingers for ten
  minutes as a dimmed X-eyed ghost so the hole has a face. Replication
  links draw as procedural pixel tendrils (strand count follows how many
  partitions ride the link, gently swaying), a lone broker sprouts a stub
  strand per declared queue, and hover still spotlights a broker's links.
  The diagram finally follows the theme tokens, so it renders correctly in
  light mode and every flavor. Reduced-motion preferences get still frames
  and still strands.
- The fibril mascot arrives (stage one): the pixel-art ring with the teal
  face appears on the login page and the 404 page, and its face - the little
  screen from the ring's band - is the dashboard favicon. Placements stay
  out of data-dense views by design.
- Flavor and light-mode tuning from real-use screens: the non-crimson flavor
  accents gained saturation and brightness (chlorophyll and azure had drifted
  gray-adjacent), light mode sits on a toned paper ground a step less light
  with firmer panel edges, and the identical ground declarations duplicated
  across every light flavor collapsed into the base light theme.
- Clearer sidebar icons: Messages is an envelope, Dead letters an
  undeliverable (struck-through) envelope, and Activity takes the alert badge,
  leaving the pulse line to Diagnostics alone.
- The settings page says what saves what: the save button is named "Save
  runtime settings" with a note that it applies the runtime section live,
  while Startup Config below is read-only. The startup summary gains the
  newer storage knobs: min fsync interval, segment preallocation, max
  in-flight fsyncs, and the fsync pipelining threshold.
- Durable publish latency. Publishing overlaps the message-log and event-log
  fsyncs instead of serializing them, roughly halving single-node durable
  publish-to-confirm latency, and the storage layer coalesces small commits so
  low-latency workloads no longer trade away throughput. Delivery waits for the
  payload to be durable, so nothing is delivered or confirmed before it is safe.
- Client publish throughput. The Rust, TypeScript, and Python clients now coalesce
  fire-and-forget writes into batched socket writes (exposed as a public client
  option), coalesce consumer acks, and flush buffered frames on graceful shutdown,
  and the per-publish tracing span moved off the hot path. Single-client publish
  roughly doubles.
- Guided error surfaces. Broker-side and client-local errors now point at the
  likely fix, so a rejected declare, an authentication denial, or a content-kind
  mismatch names what to do rather than surfacing as an opaque failure.
- Leaner Rust client dependencies. The wire codec moved into a standalone
  `fibril-wire` crate that `fibril-client` depends on directly, so depending on the
  client no longer pulls in the broker, storage, and coordination crates.
- Optional msgpack in the Rust client. msgpack encode and decode now sit behind a
  default-on `msgpack` Cargo feature, so `default-features = false` drops the
  rmp-serde dependency for users who publish JSON, text, or raw bytes. This matches
  the optional-msgpack the TypeScript and Python clients already offer.
- Client text-body accessor renamed to `text`. The Rust and TypeScript clients'
  read accessor and write constructor move from `content()` to `text()` (matching
  the `raw`/`json`/`msgpack` set), and the Python write constructor moves from
  `content()` to `text()` to match its `text()` reader. Content-type accessors are
  unchanged.
- Reconnect reconcile policy value renamed to `restore`. The Rust
  (`ReconcilePolicy::Restore`), TypeScript, and Python (`"restore"`) clients replace
  `RestoreClientSubscriptions` / `"restore_client_subscriptions"`. The wire encoding
  is an unchanged numeric byte.
- Admin HTTP API topic field. The admin API and `fibrilctl` now spell the topic
  `topic` (matching every client and the rest of the API) instead of the
  abbreviated `tp` in the create-queue, delete-queue, create-stream, per-queue
  dead-letter, and global dead-letter request and response bodies. The persisted
  storage field and the broker wire keep their internal `tp` name, so on-disk state
  and the protocol are unchanged; only the HTTP surface is renamed. The vocab-lint
  guards it.
- Rust client delays require an explicit `std::time::Duration`. The delayed-publish,
  message-TTL, retry-after, and retention-age APIs no longer accept a bare integer,
  which silently meant seconds and disagreed with the TypeScript client's
  milliseconds; pass `Duration::from_secs(30)` or `Duration::from_millis(250)`. The
  Python (seconds) and TypeScript (milliseconds) bare-number conventions are
  unchanged, matching their own language norms.

### Fixed

- Light mode works with every flavor, not just plain. The dark flavor blocks
  set their tinted grounds without a theme qualifier, so a flavored light
  mode kept dark backgrounds under light text; the grounds are now scoped to
  dark. Primary buttons also swap to white-on-accent in light mode - their
  near-black text assumed the bright dark-theme accents and read muddy on the
  darkened light-mode ones.
- Zeros stopped shouting: stat values render dim at zero and take their state
  color only when the number does, so an idle broker's Overview no longer
  pulls the eye to five colored nothings.
- Buttons no longer render bold on Windows. The stylesheet used fractional
  font weights (550, 650), which interpolate on variable fonts but round UP
  on static families like Segoe UI - a primary button asked for 650 and got
  full Bold. All weights now sit on standard stops (500, 600) that map to
  real cuts everywhere.
- The memory stat reported kilobytes as "MB": the process-memory sampler
  divided sysinfo's byte count by 1024 once instead of twice. The Overview
  card silently compensated, but the new memory graph (and the history
  samples feeding it) showed thousands-of-"MB" values. The sampler now
  publishes true megabytes and the card reads it directly.
- Saving runtime settings reports its outcome as a toast. The result message
  lands in a div at the top of a long page while the save button sits at the
  bottom, so a saved (or failed, or version-conflicted) outcome was invisible
  without scrolling up.
- Deleting a queue from the dashboard works again on standalone brokers. The
  cluster-only guard keyed off having a coordination handle, which every
  broker gained when the topology page's single-node view was wired, so a
  plain broker's deletes were refused as "cluster mode". The guard now keys
  off an explicit cluster flag set only under real coordination.
- Dashboard pages that read their query parameters (the queue detail topic,
  message inspection, the persisted filters) got the PREVIOUS page's address
  when reached through an in-dashboard link: the boosted navigation pushed the
  new URL only after running the new page's scripts. A queue's Detail link
  rendered the "no queue with this topic" panel until a manual refresh. The
  URL now updates before the scripts run.
- The dashboard's publisher and consumer activity counts always read zero. The
  queues page looked activity up under a per-partition key while the broker
  reports it per topic and group, so the lookup never matched. Activity is now
  keyed by topic and group, and queue rows show the aggregate.
- A non-retryable connection close no longer triggers a reconnect storm in the
  clients.
- A plaintext broker is named as such when a TLS client connects to it, rather
  than surfacing as a generic handshake failure.
- Security: CA-fingerprint TLS pinning is no longer bypassable by a
  man-in-the-middle. A CA-fingerprint pin previously accepted any presented chain
  that merely contained the pinned certificate, so an attacker could staple the
  broker's public CA certificate beside a leaf it controlled and be trusted. The
  clients now path-validate the presented leaf against the pinned certificate as
  the sole trust root, accepting it only when the pinned CA genuinely signed the
  leaf (so a CA pin still survives leaf rotation). Leaf-fingerprint pins were
  always sound and are unaffected. Pinning a CA fingerprint in the Python client
  now needs Python 3.13+ and the optional `cryptography` package
  (`pip install fibril[tls-pin]`); leaf pinning stays dependency-free and
  standard-library only.

## [0.4.0] - 2026-07-04

The operations release. Running Fibril in production got materially easier on
three fronts: Prometheus scrapes the broker directly, a planned restart hands
partition ownership off before the process stops, and the TLS story is now
complete end to end - inter-broker encryption, live certificate rotation, and
mutual TLS that turns client certificates into credentials. Nothing changes
for existing deployments until the new switches are flipped, with one
default worth knowing: with TLS enabled, inter-broker traffic now encrypts
too, so multi-node generated-material deployments need the shared-CA lane
(or an explicit `tls.inter_broker = false`).

### Added

- Graceful ownership handoff on drain. `POST /admin/api/drain` now also
  marks a coordinated-mode broker as draining: the controller hands each
  partition with a caught-up follower to it through the same fenced
  promotion as failover, the draining node receives no new placements, and
  the call returns with handoff progress (`owned_partitions_remaining`,
  `handoff_complete`) once ownership has moved or
  `connection.drain_handoff_timeout_ms` (default 30s) elapses. A planned
  restart moves from a liveness-TTL delivery gap to near zero; follower-less
  partitions stay put and fail over reactively as before.
- Inter-broker TLS. With TLS enabled, follower-to-owner replication and the
  coordination raft channel are encrypted too (`tls.inter_broker`, following
  `tls.enabled`; explicit `false` opts out for mesh deployments). Peers verify
  each other against `tls.peer_ca_path`, the generated CA, or OS roots, and
  transport mismatches are named in both directions. A generated-material dir
  holding only the copied `ca.pem` + `ca.key` mints that node's server
  certificate from the shared deployment CA. Nodes keep authenticating with
  the cluster secret inside the session.
- Mutual TLS. `tls.client_auth = request | require` verifies client
  certificates against `tls.client_ca_path` (or the generated CA), and a
  verified identity (first DNS SAN, else CN) that names an existing user
  authenticates the connection with no password - certificates become
  workload credentials while the user store stays the single authority.
  `require` rejects certless clients in the handshake, closing every
  listener to unidentified peers; brokers present their own certificate on
  inter-broker dials so clusters converge at any setting. `fibrilctl cert
  issue` mints workload certificates from the deployment CA, and the Rust,
  TypeScript, and Python clients gain client-certificate options plus a
  typed required-certificate error.
- Live certificate rotation. `POST /admin/api/tls/reload` and
  `fibrilctl admin reload-tls` re-read and validate the serving pair, then
  swap it for new handshakes without a restart; invalid material is rejected
  with the old certificate still serving. CA rotation remains
  restart-required.
- Prometheus metrics. `GET /metrics` on the admin listener serves the text
  exposition format behind the same admin auth and HTTPS as the dashboard:
  node-level broker/storage/transport/session-resume counters, open
  connection gauges, recovery quarantine state, and replication worker
  summaries, plus per-channel series (queue ready/inflight, stream
  subscriptions and lag evictions, follower applied offsets) from
  materialized channels, gated by `admin.metrics_per_channel` (default on).

## [0.3.0] - 2026-07-05

The security release. Connections can now be encrypted end to end, and the
broker authenticates who is connecting instead of accepting a built-in
credential from anywhere. Both were designed to be easy for an operator: TLS
material is either supplied or generated per deployment, users are managed from
the dashboard or `fibrilctl`, and a first-boot setup flow can drive the whole
thing from a browser. Nothing is encrypted or authenticated by default that was
not before, so upgrading an existing single-node deployment changes nothing
until TLS or a user is configured.

Deliberately out of scope for this release, and planned for later: TLS on
inter-broker replication and coordination connections, mutual-TLS client
authentication, certificate rotation, and per-topic authorization.

### Added

- TLS in transit. The broker listener serves TLS from operator-supplied PEM
  files (`tls.cert_path`, `tls.key_path`) or from per-deployment material
  generated under `<data_dir>/tls` (`tls.auto_self_signed`), with the CA
  fingerprint printed at startup for clients to pin. The Rust, TypeScript, and
  Python clients connect over TLS with OS-root trust, a CA file, or a
  fingerprint pin, and surface a typed error taxonomy that keeps a
  transport mismatch apart from a certificate-trust failure. A plaintext
  connection to a TLS listener and a TLS connection to a plaintext listener are
  each named in both directions rather than hanging or failing opaquely.
- Broker authentication against a user store. Users are argon2-hashed, seeded
  from config on first boot, and managed live through `/admin/api/users`,
  `fibrilctl user add/passwd/remove/list`, and a dashboard Users section. In
  cluster mode user changes replicate to every node. The built-in
  `fibril`/`fibril` credentials are accepted from loopback only, so a remote
  connection requires a real user.
- Cluster shared secret for node-to-node authentication. Replication and
  coordination connections authenticate as a node principal with a shared
  secret (`FIBRIL_CLUSTER_SECRET`, `coordination.secret_path`, or the
  `<data_dir>/cluster.secret` file written by `fibrilctl secret generate`),
  never with a user account. Ganglion mode requires one.
- Admin dashboard HTTPS from the same TLS material, with `tls.admin_enabled`
  to opt out for reverse-proxy deployments.
- First-boot setup mode. `setup.mode` serves a localhost setup page before the
  broker starts: choose or generate TLS material, optionally create an admin
  user, and optionally set a cluster secret. The choices persist as a config
  overlay layered below explicit config, so a fully configured deployment boots
  straight through unattended.
- `fibrilctl cert generate`/`cert fingerprint` and `fibrilctl secret generate`
  for provisioning TLS material and the cluster secret ahead of first boot.
- A cluster setup guide in the docs covering the secret, TLS across nodes, the
  entry-level setup-mode bring-up, and the unattended config/env lane.

### Changed

- The TLS material and rustls setup live in a dedicated `fibril-tls` crate so
  the CLI can provision certificates without depending on the server.

### Fixed

- The connection writer now drains and flushes queued frames on shutdown. A
  final error reply (an authentication denial or a rejected handshake) could
  previously lose a race with connection teardown and reach the client as a
  bare end-of-stream instead of the guided error.

## [0.2.0] - 2026-07-03

The first tagged release. The baseline: durable single-node work queues and
Plexus fan-out streams, partitioned queues with client-side fan-in, exclusive
consumer groups, message TTL and dead lettering, Rust, TypeScript, and Python
clients at parity, an admin dashboard and `fibrilctl`, and an experimental
cluster path (placement, replication, failover, live repartitioning). The
entries below cover what 0.2.0 hardened and added on top of that baseline.
The full wired surface and its conditions are documented in the
implemented-surface page of the docs.

This release focuses on cluster confidence and operational hardening. The
cluster path remains experimental, now backed by a deterministic simulation
suite, a chaos/soak suite, and a real multi-node failover run.

### Added

- Deterministic simulation testing (turmoil): an in-process multi-broker harness
  with eleven scenarios - broker-in-sim smoke, follower catch-up, owner-partition
  failover, a real ganglion raft cluster over the simulated network,
  returning-old-owner split-brain, lossy-link catch-up, raft under message loss,
  replica-durable no-false-ack under partition, checkpoint install, and
  repartition-cutover under delayed acks. Built behind a `simulation` feature.
- Chaos and soak suite (`crates/broker/tests/soak.rs`): durable crash-recovery
  across restarts and sustained concurrent-load no-loss, scalable via
  `FIBRIL_SOAK_*` environment variables.
- Real multi-node validation: `scripts/cluster-tryout.sh --failover-verify`
  (identity-tagged zero-loss check under a live owner kill) and `--chaos`
  (repeated mixed faults under load, zero loss plus reconvergence).
- Planned-restart drain: a `GoingAway` notice the broker broadcasts and clients
  surface as an observable event, plus an admin drain endpoint.
- Configurable replication read-timeout slack and owner connection-setup timeout
  so a dropped response or SYN can no longer hang a follower on a dead
  connection.
- Idle stream channel eviction through the new `stream` runtime settings
  (disabled by default), with the ephemeral flush ticker gated to only sync
  after writes so idle ephemeral streams cost nothing.
- Stream observability: per-partition live subscription counts and lag
  recovery totals in stream stats and the admin streams page, and stream
  publish and delivery counted in the broker overview rates.
- A `min_fsync_interval_ms` storage setting, a group-commit cadence floor for
  storage where per-fsync cost dominates (consumer SATA class).

### Changed

- Reconnect grace is on by default (5s), with the policy questions resolved.
- The ack-tracking window is a settled `RangeSet` rather than a bounded bitset.
- The injectable raft transport (a `RaftDialer` in Ganglion) lets the coordination
  layer run over real or simulated TCP without a Ganglion test dependency.
- Delivery-path throughput: consumer flow-control credit is released when an
  ack is accepted rather than after its fsync, and broker-to-connection
  delivery moves per-consumer batches. Single-node 1KB saturation onset moved
  from roughly 350-400k/s to 500-600k/s with lower high-rate memory use.
- Publish confirm p50 dropped to a few milliseconds on fast storage with
  Keratin's self-clocking group commit (the storage-side detail lives in
  Keratin's own changelog).

### Fixed

- Restart cold-start reconciliation treats orphaned on-disk partitions as inert
  cold storage rather than mis-materializing them.
- A lagging connected stream subscriber could silently lose records: a full
  live buffer dropped individual records and auto-ack advanced the durable
  cursor past the gap. Stream delivery is now contiguous per subscriber on
  every tier, with watermark-based lag recovery from the ring or the log.
- Concurrent identical stream declares could fail with a spurious 500 (a
  shared temp file race in the storage kind marker, fixed in Keratin).
- Graceful shutdown flushes pending stream cursor commits, so a planned
  restart loses no committed consumer progress.

[Unreleased]: https://github.com/axmouth/fibril/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/axmouth/fibril/releases/tag/v0.4.0
[0.3.0]: https://github.com/axmouth/fibril/releases/tag/v0.3.0
[0.2.0]: https://github.com/axmouth/fibril/releases/tag/v0.2.0

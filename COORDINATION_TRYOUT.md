# Trying out coordination, topology, and the playgrounds

Hands-on guide for the coordination/metadata plane: real multi-process clusters, the topology
API, `fibrilctl topology`, and the consensus playgrounds. Everything runs locally with no
external services.

## 0. Real multi-process cluster + CLI topology check

Starts N actual `fibril-server` processes (separate ports and data dirs) and checks each one's
topology through the real `fibrilctl` CLI over HTTP:

```bash
scripts/cluster-tryout.sh              # 3 standalone servers, topology-checked, shut down
scripts/cluster-tryout.sh --ganglion   # 3 servers forming ONE raft coordination cluster
scripts/cluster-tryout.sh --staggered  # watch the cluster FORM: nodes join one by one
scripts/cluster-tryout.sh --nodes 5 --ganglion
scripts/cluster-tryout.sh --ganglion --keep   # leave it running and explore with fibrilctl
```

`--staggered` narrates cluster formation in real time — node 1 alone has no quorum
(`leader:null, healthy:false`), node 2's arrival triggers the election
(`leader:1, healthy:true`), node 3 joins the running cluster. The `healthy` and
`listener_serving` flags in the `raft` JSON block are the coordination health surfaces
(see ganglion FAILURE_MODES §4b).

`--ganglion` starts each server with an embedded raft coordinator (config:
`[coordination] mode = "ganglion"`, here wired via `FIBRIL_COORDINATION_*` env vars). The
servers talk raft over real TCP — msgpack frames by default; set
`[coordination.ganglion] wire_format = "json"` (or `FIBRIL_COORDINATION_WIRE_FORMAT=json`) to
watch the wire in plaintext. Frames are individually tagged, so mixed-format clusters
interoperate.

Each broker also **registers itself** into the shared node table and keeps a heartbeat fresh
(`heartbeat_interval_ms`, default 3000): followers forward their registrations through the
leader automatically. The script asserts every node reports the same leader/voter set AND sees
all brokers registered:

```
nodes:
  NODE             BROKER                 ADMIN
  broker-1         127.0.0.1:7181         127.0.0.1:7191
  broker-2         127.0.0.1:7182         127.0.0.1:7192
  broker-3         127.0.0.1:7183         127.0.0.1:7193

raft (embedded coordinator):
  local=3 leader=3 voters=[1, 2, 3] learners=[] applied=9 committed_generation=3
...
shared cluster confirmed: leader=3 voters=[1,2,3] on all 3 nodes
```

While it runs (`--keep`), try killing one server process: its heartbeat stops refreshing, the
survivors keep one leader, and `live_nodes(ttl)` on the controller side stops counting it.
Failure behaviors are catalogued in detail in `ganglion/FAILURE_MODES.md` (see §4b for
startup/connectivity modes: unreachable peers, missing bootstrap node, leaderless heartbeats).

With `--keep` it prints ready-to-paste `fibrilctl --admin ... admin topology` commands per
node, plus where logs and data live.

The script also exercises the current server-side integration:

- declares an `orders` queue through node 1, even when another node is the raft leader
- waits for the embedded controller to assign an owner, follower set, and epoch
- checks every node sees the same assignment through topology
- updates runtime settings on node 1
- waits for the replicated settings document to apply on the other nodes

That makes this a useful smoke test for the coordination path, not only a
topology display demo.

## 1. Raft cluster playground (ganglion)

A real openraft cluster — durable WALs, persisted snapshots, election, membership — in one
process. Lives in the ganglion repo:

```bash
cd ../ganglion
scripts/cluster-playground.sh                  # 3 durable nodes, interactive
scripts/cluster-playground.sh --nodes 5
scripts/cluster-playground.sh --script "status; write 1; kill 2; status; quit"
```

Commands: `status` (topology + durability telemetry per node), `write <gen>`, `kill <id>`,
`restart <id>` (recovers from its data dir), `add <id>` (learner then voter), `remove <id>`,
`quit`. Things worth trying:

- `kill` the leader and watch the survivors re-elect (`status`).
- `kill` + `restart` a node and check it catches up (`applied` index converges).
- `write` a few hundred times, then `restart` — recovery loads the persisted snapshot and
  replays only the short WAL tail (see `replayed` in telemetry).

## 2. Coordination playground (fibril + embedded controller)

Three fibril `Coordination` providers sharing a raft group; the raft leader acts as the
controller, assigning queues with fencing epochs:

```bash
scripts/coordination-playground.sh
scripts/coordination-playground.sh --script \
  "status; assign orders 2; kill broker-1; assign orders 2; status; quit"
```

Commands: `status`, `assign <topic> <partitions>`, `kill <broker-id>` (drops it from the live
set), `revive <broker-id>`, `quit`. The key thing to observe in the scripted run above:

- `orders/0` was owned by `broker-1`; after the kill + reassign, ownership moves and the
  **epoch bumps 1 → 2** (split-brain fencing).
- `orders/1` keeps its owner, so its epoch stays 1 — follower churn never fences.
- All three brokers report identical committed state (they consume the same watch).

## 3. Topology API and `fibrilctl topology`

Every fibril server exposes `GET /admin/api/topology`. Start a server and ask:

```bash
cargo run -p fibril --bin fibril-server &        # or however you usually start it
cargo run -p fibril-cli --bin fibrilctl -- admin topology
cargo run -p fibril-cli --bin fibrilctl -- admin topology --json   # raw JSON for scripts
```

The text view renders three blocks:

- `cluster:` reporting node + snapshot generation,
- `nodes:` / `assignments:` tables (owner, followers, **epoch** per queue partition),
- `raft:` consensus internals (leader, voters, learners, applied index) — only present when an
  embedded ganglion coordinator is attached.

A standalone server shows itself as the single `local` node; with
`[coordination] mode = "ganglion"` (see section 0) the raft block appears and all cluster
members serve the same view.

The same JSON also drives the **admin Topology page**: open
`http://127.0.0.1:<admin_port>/admin/topology` in a browser for the live diagram — brokers on a
ring, owner→follower edges per assignment, consensus leader/voters in the middle, plus the
assignments table. It refreshes every 3 seconds.

## What is intentionally not here yet

- **Cross-broker exclusive cohort e2e**: the cohort coordinator wiring exists,
  but the scripted tryout does not yet prove one logical exclusive cohort
  balancing across partition owners.
- **Adversarial failover runbook**: try killing a process manually with
  `--keep`, but the script does not yet automate stale-owner return,
  promotion refusal, or partition scenarios.
- **Client topology assertions**: the script validates topology through
  `fibrilctl`; it does not yet run a topology-aware Rust client across multiple
  owners.

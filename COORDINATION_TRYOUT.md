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
scripts/cluster-tryout.sh --nodes 5 --ganglion
scripts/cluster-tryout.sh --ganglion --keep   # leave it running and explore with fibrilctl
```

`--ganglion` starts each server with an embedded raft coordinator (config:
`[coordination] mode = "ganglion"`, here wired via `FIBRIL_COORDINATION_*` env vars). The
servers talk raft over real TCP (msgpack frames; set `GANGLION_WIRE_FORMAT=json` to watch the
wire in plaintext) and the script asserts every node reports the same leader and voter set:

```
raft (embedded coordinator):
  local=2 leader=1 voters=[1, 2, 3] learners=[] applied=1 committed_generation=0
...
shared cluster confirmed: leader=1 voters=[1,2,3] on all 3 nodes
```

With `--keep` it prints ready-to-paste `fibrilctl --admin ... admin topology` commands per
node, plus where logs and data live.

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

A standalone server shows itself as the single `local` node. The full clustered view appears
once a clustered provider is wired (`with_coordination` / `with_raft_topology` on
`AdminServer`; config-driven selection is planned as phase F3 in
`REPLICATION_PLANNING.md` § "Ganglion coordination provider").

The same JSON is the contract for the future admin-page topology diagram.

## What is intentionally not here yet

- **Multi-process raft clusters**: the ganglion transport is in-process today; a wire transport
  (gRPC) is a planned ganglion item. Playgrounds therefore run all nodes in one process.
- **Config-driven coordination selection** (`coordination = "static" | "ganglion"`): phase F3.
- **Admin page diagram**: planned; it consumes the `GET /admin/api/topology` JSON unchanged.

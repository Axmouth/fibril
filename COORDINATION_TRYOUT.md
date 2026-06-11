# Trying out coordination, topology, and the playgrounds

Hands-on guide for the coordination/metadata plane: real multi-process clusters, the topology
API, `fibrilctl topology`, and the consensus playgrounds. Everything runs locally with no
external services.

## 0. Real multi-process cluster + CLI topology check

Starts N actual `fibril-server` processes (separate ports and data dirs) and checks each one's
topology through the real `fibrilctl` CLI over HTTP:

```bash
scripts/cluster-tryout.sh              # 3 servers, topology-checked, then shut down
scripts/cluster-tryout.sh --nodes 5
scripts/cluster-tryout.sh --keep       # leave them running and play with fibrilctl yourself
```

With `--keep` it prints ready-to-paste `fibrilctl --admin ... admin topology` commands per
node, plus where logs and data live. Today each server reports its own single-node topology;
once cluster coordination is config-wired (phase F3 + a raft wire transport), this same script
becomes the shared-cluster assertion — the harness is already real-process.

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

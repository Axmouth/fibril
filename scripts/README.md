# scripts/

Reference for the repo's helper scripts. Each entry lists what the script does
and its most useful knobs; run any with `-h`/`--help` where noted, or read the
header comment for the full env list. Benchmarking scripts are summarized here and
documented in depth (recipes, metric glossary, ceilings) in
[`../BENCHMARKING.md`](../BENCHMARKING.md).

## Benchmarking

| Script | Does | Key env |
|---|---|---|
| `bench-steady-c.sh` | Queue steady-state over TCP at a fixed target rate: publish/confirm/deliver latency + rates, RSS, queue snapshot. | `RATE_PER_SEC`, `SIZE`, `WRITERS`, `READERS`, `CONFIRMED`, `BENCH_MODE`, `SERVER_DATA_DIR` (device to measure), `SERVER_CONFIG` (e.g. enable preallocation) |
| `bench-matrix.sh` | Runs named scenarios and writes one result/log per case plus a `summary.md`. | scenarios: `smoke` `baseline` `confirmed` `throughput-1k` `payload` `large-backlog` `all`; `OUT_DIR` |
| `bench-results-table.sh` | Renders a Markdown table from one or more `*.results.txt` files. | result files as args |
| `bench-e2e-c.sh` | End-to-end: publish N, drain, report throughput. `FIREHOSE=1` drops reader latency tracking to isolate broker delivery. | `MESSAGES`, `CLIENTS`, `SIZE`, `FIREHOSE`, `CONFIRMED` |
| `bench-stream.sh` | Plexus stream steady-state (stream counterpart of `bench-steady-c.sh`). | `DURABILITY` (tier), `RATE_PER_SEC`, `PARTITIONS`, `DATA_DIR` (device to measure) |
| `fsync-probe.py` | Raw `fdatasync`/`fsync` cost on a device: in-place vs extending vs preallocated. | `--extend`, `--fallocate`, `--gap-ms`, `--path` |

Also see `../clients/bench-plexus-fanout.sh` (Plexus fan-out with one OS process
per reader/writer, for honest multi-core fan-out numbers).

Note: `bench-steady-c.sh` names the server data-dir knob `SERVER_DATA_DIR`;
`bench-stream.sh` names the same idea `DATA_DIR`.

## Dev / CI gates

| Script | Does |
|---|---|
| `check.sh` | Local CI gate: syntax-checks every script, runs `check-template-js.sh`, `cargo fmt --check`, and the clippy/test suite. Run before committing. |
| `check-template-js.sh` | Extracts the inline `<script>` blocks from the admin HTML templates and compiles each with node, so a broken inline script fails at check time instead of only in a browser. |

## Cluster demos

| Script | Does |
|---|---|
| `cluster-tryout.sh` | Starts N real `fibril-server` processes (own ports/data dirs) and checks each one's topology via `fibrilctl`. `--ganglion` forms one raft coordination cluster; `--staggered` joins nodes one by one. |
| `coordination-playground.sh` | 3 brokers + an embedded raft controller in one process. Interactive, or `--script "status; assign orders 2; kill broker-1; ..."` for a scripted demo. |

## Release

| Script | Does |
|---|---|
| `release.sh` | Per-repo release: bumps the repo version everywhere, gates on a changelog entry, snapshots docs, checks the build, commits, and tags `v<version>`. Never pushes (you push; the tag triggers the release workflow). |
| `release-all.sh` | Overlord release: drives every repo's own `release.sh` in dependency order at one version, for the synced cuts done while the repo group evolves together. |

## Dependency sourcing / fixtures

| Script | Does |
|---|---|
| `stroma-source.sh` | Strips the local `[patch."…/Axmouth/*.git"]` blocks (Keratin/Ganglion) so dependency lines stay git-sourced for CI, Docker, and release builds. |
| `ci-stroma-source.sh` | The CI/Docker variant: removes the local development patches and re-resolves the git deps (no `--locked`, since the committed lockfile is in local-patch form). |
| `gen-compat-fixture.sh <tag>` | Generates a storage back-compat golden fixture: a data dir written by a released broker version (built from a worktree at `<tag>`), captured so a later broker proves in CI it still opens it. |

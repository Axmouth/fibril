#!/usr/bin/env bash
# Three data replicas leave one follower after another follower is promoted.
# Post-exit publications prevent pre-crash deliveries alone from passing the test.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
run_root="${CLUSTER_TRYOUT_RUN_ROOT:-/tmp}"
mkdir -p "$run_root"
config=$(mktemp "$run_root/fibril-two-followers.XXXXXX.toml")
trap 'rm -f "$config"' EXIT
printf '[coordination.ganglion]\ntarget_followers = 2\n' > "$config"
export FIBRIL_CONFIG="$config"
export FAILOVER_VERIFY_COUNT=20000 FAILOVER_VERIFY_RATE=1000 FAILOVER_VERIFY_RECOVERY_MIN=1000 FAILOVER_VERIFY_EXPECT_FOLLOWERS=2
cargo build --release -p fibril -p fibril-cli -p fibril-benches \
  --bin fibril-server --bin fibrilctl --bin failover_verify
timeout --kill-after=10s 180s bash scripts/cluster-tryout.sh \
  --nodes 3 --ganglion --port-offset -10000 --assignment-durability replica_durable:2 \
  --failover-verify --hard-kill --prealloc --admin-wait-secs 20 "$@"

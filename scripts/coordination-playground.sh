#!/usr/bin/env bash
# Coordination playground: 3 brokers + embedded raft controller in one process.
#
# Examples:
#   scripts/coordination-playground.sh                                      # interactive
#   scripts/coordination-playground.sh --script "status; assign orders 2; kill broker-1; assign orders 2; status; quit"
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
exec cargo run -p fibril-coordination-ganglion --example coordination_demo --quiet -- "$@"

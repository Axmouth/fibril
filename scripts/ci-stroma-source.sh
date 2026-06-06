#!/usr/bin/env bash
set -euo pipefail

scripts/stroma-source.sh git

# CI commits the local-path development lockfile. After switching Stroma to a
# git source, refresh only the dependency entries affected by that source swap
# before running locked cargo commands.
cargo update stroma-core keratin-log

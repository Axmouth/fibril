#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  echo
  echo "==> $*"
  "$@"
}

run bash -n "$repo_root"/scripts/*.sh
run cargo fmt --all -- --check
run cargo test --workspace --locked

if [ -d "$repo_root/clients/typescript" ]; then
  (
    cd "$repo_root/clients/typescript"
    run npm test
  )
fi

if [ -d "$repo_root/website" ]; then
  (
    cd "$repo_root/website"
    run npm run check
  )
fi

echo
echo "All checks passed."

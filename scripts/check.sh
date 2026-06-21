#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  echo
  echo "==> $*"
  "$@"
}

run bash -n "$repo_root"/scripts/*.sh
run bash "$repo_root"/scripts/check-template-js.sh
run cargo fmt --all -- --check
rust_test_flags="${RUSTFLAGS:-}"
if [[ " $rust_test_flags " != *" -Awarnings "* ]]; then
  rust_test_flags="${rust_test_flags:+$rust_test_flags }-Awarnings"
fi
run env RUSTFLAGS="$rust_test_flags" cargo test --quiet --workspace

if [ -d "$repo_root/clients/typescript" ]; then
  (
    cd "$repo_root/clients/typescript"
    run npm --silent run test:quiet
  )
fi

if [ -d "$repo_root/website" ]; then
  (
    cd "$repo_root/website"
    run npm --silent run verify
  )
fi

echo
echo "All checks passed."

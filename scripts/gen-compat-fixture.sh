#!/usr/bin/env bash
# Generate a storage back-compat golden fixture: a data dir written by a released
# broker version, captured so a later broker can prove in CI that it still opens
# it (crates/broker/tests/compat_fixture.rs).
#
# Usage:
#   scripts/gen-compat-fixture.sh <tag>       # e.g. v0.4.0
#
# The broker is built from a worktree at <tag> so the on-disk bytes belong to
# that release. The generator (compat-fixture-gen) is built from the CURRENT
# tree and drives the broker over the network - the broker, not the generator,
# writes the data dir, so the generator only needs to speak wire operations that
# existed at <tag>. Within a single protocol version those are additive, so a
# current-tree generator drives any same-version release server. When the
# protocol version bumps this needs a matching guard.
#
# Output (committed):
#   crates/broker/tests/fixtures/datadir-<tag>.tar.gz         the data dir
#   crates/broker/tests/fixtures/datadir-<tag>.manifest.json  what it contains
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO_ROOT="$(pwd)"

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  echo "usage: scripts/gen-compat-fixture.sh <tag>" >&2
  exit 2
fi
if ! git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "FAIL: tag '$TAG' does not exist" >&2
  exit 1
fi

FIXTURE_DIR="$REPO_ROOT/crates/broker/tests/fixtures"
mkdir -p "$FIXTURE_DIR"
ARCHIVE="$FIXTURE_DIR/datadir-$TAG.tar.gz"
MANIFEST="$FIXTURE_DIR/datadir-$TAG.manifest.json"

# The path patches in Cargo.toml resolve ../keratin and ../ganglion relative to
# the repo, so the worktree must be a sibling of this repo for a build to link.
WORKTREE="$(dirname "$REPO_ROOT")/fibril-compat-$TAG"
RUN_DIR="$(mktemp -d)"
BROKER_PORT=17999
ADMIN_PORT=18999
SRV_PID=""

cleanup() {
  [[ -n "$SRV_PID" ]] && kill -TERM "$SRV_PID" 2>/dev/null || true
  [[ -n "$SRV_PID" ]] && wait "$SRV_PID" 2>/dev/null || true
  rm -rf "$RUN_DIR"
  git -C "$REPO_ROOT" worktree remove --force "$WORKTREE" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> building generator from the current tree"
cargo build --quiet --release -p fibril-compat-fixture-gen
GEN="$REPO_ROOT/target/release/compat-fixture-gen"

echo "==> checking out $TAG in a sibling worktree and building its broker"
git -C "$REPO_ROOT" worktree remove --force "$WORKTREE" 2>/dev/null || true
git -C "$REPO_ROOT" worktree add --detach "$WORKTREE" "$TAG" >/dev/null
( cd "$WORKTREE" && cargo build --quiet --release -p fibril )
SERVER="$WORKTREE/target/release/fibril-server"

echo "==> booting the $TAG broker on a fresh data dir"
DATA_DIR="$RUN_DIR/data"
env FIBRIL_DATA_DIR="$DATA_DIR" \
    FIBRIL_BROKER_BIND="127.0.0.1:$BROKER_PORT" \
    FIBRIL_ADMIN_BIND="127.0.0.1:$ADMIN_PORT" \
    "$SERVER" >"$RUN_DIR/server.log" 2>&1 &
SRV_PID="$!"
for _ in $(seq 1 100); do
  curl -sf "http://127.0.0.1:$ADMIN_PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done
if ! curl -sf "http://127.0.0.1:$ADMIN_PORT/healthz" >/dev/null 2>&1; then
  echo "FAIL: broker never came up; log:" >&2
  tail -20 "$RUN_DIR/server.log" >&2
  exit 1
fi

echo "==> driving the fixture workload"
"$GEN" --broker "127.0.0.1:$BROKER_PORT" --tag "$TAG" --out "$MANIFEST"

echo "==> stopping the broker cleanly (flushes pending writes)"
kill -TERM "$SRV_PID"; wait "$SRV_PID" 2>/dev/null || true
SRV_PID=""

echo "==> archiving the data dir"
# Exclude the keratin lock files: they are process-held runtime state, not part
# of the durable format, and a stale one only adds noise.
tar -czf "$ARCHIVE" --exclude='.keratin.lock' -C "$RUN_DIR" data

echo "wrote $ARCHIVE"
echo "wrote $MANIFEST"

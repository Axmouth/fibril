#!/usr/bin/env bash
# Run every terminating example against a real single-node broker as a light
# end-to-end smoke test. Starts a fresh broker, runs each example (each connects
# with the built-in fibril/fibril loopback credentials, does a small exchange, and
# exits non-zero on failure), tears the broker down, and exits non-zero if any
# example failed.
#
# Usage: clients/python/examples/run-all.sh
#   Reuse an already-running broker instead of starting one:
#     FIBRIL_ADDR=127.0.0.1:9876 FIBRIL_EXTERNAL_BROKER=1 examples/run-all.sh
#
# The continuous/discovery demos are intentionally excluded: subscribe (runs
# forever) and pattern_subscribe (needs a multi-node broker that advertises
# topology; validated instead by the fake-broker tests).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CLIENT_DIR="$(cd "$HERE/.." && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
PORT="${FIBRIL_PORT:-9876}"
ADDR="${FIBRIL_ADDR:-127.0.0.1:$PORT}"

# The terminating examples (each doubles as a light integration test).
EXAMPLES=(publish confirmed_delayed manual_ack_retry stream)

if [[ "${FIBRIL_EXTERNAL_BROKER:-0}" != "1" ]]; then
  SERVER="$REPO_ROOT/target/release/fibril-server"
  if [[ ! -x "$SERVER" ]]; then
    echo "building fibril-server (release)..."
    (cd "$REPO_ROOT" && cargo build -q --release -p fibril-server)
  fi
  DATA_DIR="$(mktemp -d)"
  FIBRIL_DATA_DIR="$DATA_DIR" \
    FIBRIL_BROKER_BIND="127.0.0.1:$PORT" \
    FIBRIL_ADMIN_BIND="127.0.0.1:$((PORT + 1))" \
    "$SERVER" >"$DATA_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  trap 'kill "$SERVER_PID" 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT
  # Wait for the broker's listener to come up.
  for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
      exec 3>&- 3<&-
      break
    fi
    sleep 0.3
  done
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "broker did not start (is port $PORT already in use?); server log:" >&2
    cat "$DATA_DIR/server.log" >&2
    exit 1
  fi
fi

failures=0
for name in "${EXAMPLES[@]}"; do
  echo "=== $name ==="
  if ! (cd "$CLIENT_DIR" && FIBRIL_ADDR="$ADDR" uv run --quiet python "examples/$name.py"); then
    failures=$((failures + 1))
  fi
done

echo "-------------------------------------"
if [[ "$failures" -eq 0 ]]; then
  echo "all examples passed"
else
  echo "$failures example(s) failed"
fi
exit "$failures"

#!/usr/bin/env bash
# Client vocabulary lint: fail if a concept that the clients agreed to spell one
# way drifts back to a banned spelling. This is the guard behind the glossary
# unification (see clients/API_CONSISTENCY_AUDIT.md) - it is why the surface can
# stay uniform without a reviewer catching every rename by eye.
#
# It only bans spellings that have NO legitimate use left anywhere (so it never
# fires on internal wire DTOs). Concepts whose "old" word is still legitimate on
# the wire (retention max_records, the reconcile policy byte) are intentionally
# NOT checked here - extend this only with tokens that are unambiguous.
#
# Usage: clients/vocab-lint.sh   (run from the repo root or anywhere)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
cd "$REPO_ROOT"

# Source trees to scan (client public surfaces + the Rust reference client).
SCAN=(clients/go clients/csharp/Fibril clients/python/src clients/typescript/src crates/client/src crates/cli/src)

fail=0

# report NAME PATTERN CANONICAL [extra-grep-excludes...]
report() {
  local name="$1" pattern="$2" canonical="$3"; shift 3
  local hits
  hits="$(grep -rnE "$pattern" \
    --exclude-dir=bin --exclude-dir=obj --exclude-dir=node_modules \
    --exclude-dir=target --exclude-dir=dist --exclude-dir=.git \
    "${SCAN[@]}" 2>/dev/null "$@")"
  if [[ -n "$hits" ]]; then
    echo "VOCAB DRIFT [$name]: use '$canonical' instead."
    echo "$hits"
    echo
    fail=1
  fi
}

# Text body accessor: the write constructor and the read accessor are `text`
# everywhere now. `content_type` is a different concept and is excluded.
report "text-body write ctor" 'NewMessage[.:]{1,2}content\(' 'NewMessage.text(...)'
report "text-body read accessor" '\.content\(\)' '.text()' --include='*.go' --include='*.cs' --include='*.py' --include='*.ts' --include='*.rs'

# Positive settle verb is `complete` / `Complete` (Go and C# no longer expose Ack
# or a raw public Nack; Fail/Retry/RetryAfter cover the negative cases).
report "public settle verb (Go)" 'func \(d Delivery\) (Ack|Nack)\(' 'Complete() (or Fail/Retry/RetryAfter)'
report "public settle verb (C#)" 'public void (Ack|Nack)\(' 'Complete() (or Fail/Retry/RetryAfter)'

# Declare config type is `QueueConfig` / `StreamConfig`.
report "declare config type (C#)" 'QueueDeclareOptions|PlexusDeclareOptions' 'QueueConfig / StreamConfig'
report "declare config type (Go)" 'type Declare(Queue|Plexus) struct' 'QueueConfig / StreamConfig'

if [[ "$fail" -ne 0 ]]; then
  echo "Client vocabulary lint FAILED. See clients/API_CONSISTENCY_AUDIT.md for the canonical table."
  exit 1
fi
echo "Client vocabulary lint passed."

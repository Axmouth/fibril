#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/stroma-source.sh <git|local>

Switch the stroma-core dependency source used by crates/broker,
crates/storage, and protocol tests.

  git    Use https://github.com/Axmouth/keratin.git branch main.
         This is the CI/release/Docker mode.

  local  Use ../../../keratin/stroma/core for sibling-checkout development.
         This expects a local Keratin checkout at ../keratin.
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

case "$1" in
  git)
    replacement='stroma-core = { git = "https://github.com/Axmouth/keratin.git", branch = "main" }'
    ;;
  local)
    replacement='stroma-core = { path = "../../../keratin/stroma/core" }'
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

python3 - "$replacement" crates/broker/Cargo.toml crates/storage/Cargo.toml crates/protocol/Cargo.toml <<'PY'
from pathlib import Path
import sys

replacement = sys.argv[1]
files = [Path(path) for path in sys.argv[2:]]

for path in files:
    text = path.read_text()
    lines = text.splitlines()
    changed = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("stroma-core = {"):
            lines[i] = replacement
            changed = True
            break
    if not changed:
        raise SystemExit(f"no stroma-core dependency found in {path}")
    path.write_text("\n".join(lines) + "\n")
    print(f"{path}: {replacement}")
PY

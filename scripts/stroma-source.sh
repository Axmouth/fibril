#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/stroma-source.sh <git|local>

Switch the stroma-core dependency source used by crates/broker,
crates/storage, and protocol tests.

  git    Use https://github.com/Axmouth/keratin.git.
         This is the CI/release/Docker mode. Defaults to branch main.
         Set STROMA_SOURCE_REF_KIND to branch, tag, or rev and
         STROMA_SOURCE_REF to pin a specific source ref.

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
    ref_kind="${STROMA_SOURCE_REF_KIND:-branch}"
    ref="${STROMA_SOURCE_REF:-main}"
    case "$ref_kind" in
      branch | tag | rev) ;;
      *)
        echo "STROMA_SOURCE_REF_KIND must be branch, tag, or rev" >&2
        exit 2
        ;;
    esac
    replacement="stroma-core = { git = \"https://github.com/Axmouth/keratin.git\", ${ref_kind} = \"${ref}\" }"
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

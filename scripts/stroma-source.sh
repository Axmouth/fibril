#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/stroma-source.sh <git|local>

Switch the workspace Stroma source override.

  git    Use the git dependency declared in the crate manifests.
         This removes the local [patch] block and is used by CI/release builds.

  local  Use ../keratin as a local patch for sibling-checkout development.
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

patch_header='[patch."https://github.com/Axmouth/keratin.git"]'
patch_block='# Local development uses the sibling Keratin checkout while committed
# dependency lines stay git-sourced for CI, Docker, and release builds.
[patch."https://github.com/Axmouth/keratin.git"]
stroma-core = { path = "../keratin/stroma/core" }
keratin-log = { path = "../keratin/keratin-log" }'

remove_patch_block() {
  python3 - <<'PY'
from pathlib import Path

path = Path("Cargo.toml")
lines = path.read_text().splitlines()
out = []
i = 0

while i < len(lines):
    if (
        lines[i] == "# Local development uses the sibling Keratin checkout while committed"
        and i + 2 < len(lines)
        and lines[i + 1] == "# dependency lines stay git-sourced for CI, Docker, and release builds."
        and lines[i + 2] == '[patch."https://github.com/Axmouth/keratin.git"]'
    ):
        i += 3
        while i < len(lines) and not lines[i].startswith("["):
            i += 1
        continue

    if lines[i] == '[patch."https://github.com/Axmouth/keratin.git"]':
        i += 1
        while i < len(lines) and not lines[i].startswith("["):
            i += 1
        continue

    out.append(lines[i])
    i += 1

path.write_text("\n".join(out).rstrip() + "\n")
PY
}

case "$1" in
  git)
    remove_patch_block
    echo "Cargo.toml: removed local Keratin patch override"
    ;;
  local)
    if grep -Fq "$patch_header" Cargo.toml; then
      echo "Cargo.toml: local Keratin patch override already present"
    else
      printf '\n%s\n' "$patch_block" >> Cargo.toml
      echo "Cargo.toml: added local Keratin patch override"
    fi
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/stroma-source.sh <git|local>

Switch the workspace vendored-source overrides for Keratin (Stroma) and Ganglion.

  git    Use the git dependencies declared in the crate manifests.
         This removes the local [patch] blocks and is used by CI/Docker/release.

  local  Use the sibling ../keratin and ../ganglion checkouts as local patches
         for sibling-checkout development.
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

patch_block='# Local development uses the sibling Keratin checkout while committed
# dependency lines stay git-sourced for CI, Docker, and release builds.
[patch."https://github.com/Axmouth/keratin.git"]
stroma-core = { path = "../keratin/stroma/core" }
stroma-common = { path = "../keratin/stroma/common" }
keratin-log = { path = "../keratin/keratin-log" }

[patch."https://github.com/Axmouth/ganglion.git"]
ganglion-core = { path = "../ganglion/crates/ganglion-core" }
ganglion-openraft = { path = "../ganglion/crates/ganglion-openraft" }
ganglion = { path = "../ganglion/crates/ganglion" }'

# Remove every local [patch."https://github.com/Axmouth/*.git"] block (Keratin and
# Ganglion) plus the shared development comment that introduces them.
remove_patch_blocks() {
  python3 - <<'PY'
import re
from pathlib import Path

path = Path("Cargo.toml")
lines = path.read_text().splitlines()
out = []
i = 0
n = len(lines)
header = re.compile(r'^\[patch\."https://github\.com/Axmouth/[^"]+\.git"\]$')

def dev_comment_before_patch(idx):
    if not (
        idx + 1 < n
        and lines[idx] == "# Local development uses the sibling Keratin checkout while committed"
        and lines[idx + 1] == "# dependency lines stay git-sourced for CI, Docker, and release builds."
    ):
        return False
    j = idx + 2
    while j < n and lines[j].strip() == "":
        j += 1
    return j < n and bool(header.match(lines[j]))

while i < n:
    if dev_comment_before_patch(i):
        i += 2
        continue
    if header.match(lines[i]):
        i += 1
        while i < n and not lines[i].startswith("["):
            i += 1
        continue
    out.append(lines[i])
    i += 1

path.write_text("\n".join(out).rstrip() + "\n")
PY
}

case "$1" in
  git)
    remove_patch_blocks
    echo "Cargo.toml: removed local Keratin and Ganglion patch overrides"
    ;;
  local)
    if grep -Fq '[patch."https://github.com/Axmouth/keratin.git"]' Cargo.toml \
      || grep -Fq '[patch."https://github.com/Axmouth/ganglion.git"]' Cargo.toml; then
      echo "Cargo.toml: local patch overrides already present"
    else
      printf '\n%s\n' "$patch_block" >> Cargo.toml
      echo "Cargo.toml: added local Keratin and Ganglion patch overrides"
    fi
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

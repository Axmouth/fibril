#!/usr/bin/env bash
# Overlord release: cut a coordinated release across the whole repo group at one
# version. The group is developed together today, so this drives every repo's own
# release script in dependency order (lowest layer first) with the same version.
#
# Long term each repo releases on its own cadence (Ganglion and Keratin already
# carry independent version lines; Fibril and Stroma tend to move together). This
# overlord is only for the synced cuts that are convenient while they evolve in
# lockstep - it simply runs each repo's per-repo scripts/release.sh. A repo
# without one yet is reported and skipped.
#
# Usage: scripts/release-all.sh <version>     e.g. scripts/release-all.sh 0.2.0
set -euo pipefail

cd "$(dirname "$0")/.."

version="${1:-}"
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  || { echo "overlord: usage: release-all.sh <MAJOR.MINOR.PATCH>" >&2; exit 1; }

# Sibling repos in dependency order. Keratin currently also holds Stroma; when
# Stroma splits out it joins this list. Paths are relative to the Fibril repo.
repos=(../keratin ../ganglion .)

echo "overlord: coordinated release v$version across the repo group"
for repo in "${repos[@]}"; do
  name="$(basename "$(cd "$repo" && pwd)")"
  if [[ -x "$repo/scripts/release.sh" ]]; then
    echo
    echo "### releasing $name ###"
    (cd "$repo" && scripts/release.sh "$version")
  else
    echo "overlord: SKIP $name (no scripts/release.sh yet - add one to include it)"
  fi
done

echo
echo "overlord: done. Review the per-repo commits and tags, then push each repo."

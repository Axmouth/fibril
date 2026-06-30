#!/usr/bin/env bash
# Release the Fibril repo (broker, clients, CLI, admin) at one version.
#
# This is the per-repo release script. It bumps the single repo version
# everywhere, gates on a documented changelog entry, snapshots the docs for the
# release, checks the build, commits, and creates the v<version> tag. Pushing the
# tag triggers .github/workflows/release.yaml (version-tagged images + a GitHub
# release). It never pushes - the user pushes.
#
# Sibling repos (Ganglion, Keratin, and eventually Stroma) carry their own
# version lines and get their own copy of this script; scripts/release-all.sh is
# the overlord that drives a synced release across all of them for now.
#
# Usage:
#   scripts/release.sh <version>   bump to <version>, snapshot docs, tag v<version>
#   scripts/release.sh --check     verify the repo is release-ready at its current
#                                  version (no bump, no commit, no tag)
set -euo pipefail

cd "$(dirname "$0")/.."

fail() { echo "release: $*" >&2; exit 1; }

current_version() {
  # The single source of truth: [workspace.package] version in Cargo.toml.
  awk '/^\[workspace\.package\]/{f=1} f&&/^version *=/{gsub(/[" ]/,"",$3); print $3; exit}' \
    Cargo.toml | sed 's/version=//'
}

mode="release"
case "${1:-}" in
  "") fail "usage: release.sh <version> | --check" ;;
  --check) mode="check"; version="$(current_version)" ;;
  *) version="$1" ;;
esac

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "version must be semver MAJOR.MINOR.PATCH, got '$version'"
minor="${version%.*}"   # e.g. 0.2.0 -> 0.2

# Gate: the changelog must document this exact version with a date (not just
# Unreleased). This is the "before you version" gate - the release is described.
grep -qE "^## \[$version\] - [0-9]{4}-[0-9]{2}-[0-9]{2}" CHANGELOG.md \
  || fail "CHANGELOG.md has no dated '## [$version]' section - document the release first"

if [[ "$mode" == "release" ]]; then
  [[ -z "$(git status --porcelain)" ]] || fail "working tree not clean - commit or stash first"
fi

echo "release: target version $version (docs version $minor), mode=$mode"

# Bump every version location to <version> (idempotent).
set_versions() {
  sed -i -E "s/^(version = \")[0-9]+\.[0-9]+\.[0-9]+(\")/\1$version\2/" Cargo.toml
  sed -i -E "s/(\"version\": \")[0-9]+\.[0-9]+\.[0-9]+(\")/\1$version\2/" clients/typescript/package.json
  sed -i -E "s/(DEFAULT_CLIENT_VERSION = \")[0-9]+\.[0-9]+\.[0-9]+(\")/\1$version\2/" clients/typescript/src/client.ts
  sed -i -E "s/^(version = \")[0-9]+\.[0-9]+\.[0-9]+(\")/\1$version\2/" clients/python/pyproject.toml
  sed -i -E "s/(DEFAULT_CLIENT_VERSION = \")[0-9]+\.[0-9]+\.[0-9]+(\")/\1$version\2/" clients/python/src/fibril/client.py
  sed -i -E "s/(client_version: str = \")[0-9]+\.[0-9]+\.[0-9]+(\")/\1$version\2/" clients/python/src/fibril/engine.py
  sed -i -E "s/^(__version__ = \")[0-9]+\.[0-9]+\.[0-9]+(\")/\1$version\2/" clients/python/src/fibril/__init__.py
}

if [[ "$mode" == "release" ]]; then
  set_versions
fi

# Snapshot the docs: freeze /latest under /<minor> so an adopter on this release
# sees the surface as it shipped while /latest moves ahead.
docs_latest="website/src/content/docs/latest"
docs_snapshot="website/src/content/docs/$minor"
if [[ -d "$docs_latest" ]]; then
  rm -rf "$docs_snapshot"
  cp -r "$docs_latest" "$docs_snapshot"
  echo "release: snapshotted docs $docs_latest -> $docs_snapshot"
fi

echo "release: checking the workspace builds at $version"
cargo check --workspace --quiet

if [[ "$mode" == "check" ]]; then
  echo "release: OK - $version is release-ready (changelog present, builds)"
  # Leave the docs snapshot in place; a check run is also how the snapshot is
  # refreshed without tagging.
  exit 0
fi

git add -A
if ! git diff --cached --quiet; then
  git commit -q -m "Release v$version"
fi

# Tag with the changelog section as the annotation body.
notes="$(awk -v v="$version" '
  $0 ~ "^## \\["v"\\]" {grab=1; next}
  grab && /^## \[/ {exit}
  grab {print}
' CHANGELOG.md)"
git tag -a "v$version" -m "fibril v$version" -m "$notes"

echo
echo "release: tagged v$version. To publish:"
echo "    git push origin HEAD && git push origin v$version"
echo "Pushing the tag triggers the release workflow (version-tagged images + GitHub release)."

#!/usr/bin/env bash
set -euo pipefail

scripts/stroma-source.sh git

# CI and Docker build against the git dependencies, not the sibling checkouts, so
# remove the local development patches. The committed lockfile is in local-patch
# form (matching everyday local builds), so cargo re-resolves the git deps here
# and these commands do not pass --locked.

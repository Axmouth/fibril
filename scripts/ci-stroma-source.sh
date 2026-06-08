#!/usr/bin/env bash
set -euo pipefail

scripts/stroma-source.sh git

# The committed dependency lines and lockfile are git-sourced. CI only removes
# the local development patch before running locked cargo commands.

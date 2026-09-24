#!/usr/bin/env bash
# verify-generated.sh — ensure generated files are up-to-date.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Running make generate manifests ..."
make generate manifests

if ! git diff --quiet --exit-code; then
  echo ""
  echo "ERROR: Generated files are out of date. Please run:"
  echo "  make generate manifests"
  echo "and commit the result."
  echo ""
  git diff --stat
  exit 1
fi

echo "==> Generated files are up-to-date."

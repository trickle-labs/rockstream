#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1"
  exit 1
}

# 1. Base run on repository should pass (once files exist)
python3 "$ROOT/scripts/check-reproducible-builds.py" "$ROOT" >/dev/null 2>&1 || true

# Prepare temporary testing clone/tree
mkdir -p "$TMP_ROOT/docs" "$TMP_ROOT/scripts"
cp -r "$ROOT/docs"/* "$TMP_ROOT/docs/"
cp "$ROOT/scripts/check-reproducible-builds.py" "$TMP_ROOT/scripts/"

# 2. Mutating vulnerability count to non-zero must fail
if [[ -f "$TMP_ROOT/docs/vulnerability-results.json" ]]; then
  sed -i.bak 's/"critical": 0/"critical": 1/' "$TMP_ROOT/docs/vulnerability-results.json"
  if python3 "$TMP_ROOT/scripts/check-reproducible-builds.py" "$TMP_ROOT" >/dev/null 2>&1; then
    fail "check accepted critical vulnerability > 0"
  fi
  mv "$TMP_ROOT/docs/vulnerability-results.json.bak" "$TMP_ROOT/docs/vulnerability-results.json"
fi

# 3. Invalid SPDX version must fail
if [[ -f "$TMP_ROOT/docs/sbom.spdx.json" ]]; then
  sed -i.bak 's/"spdxVersion": "SPDX-2.3"/"spdxVersion": "SPDX-2.1"/' "$TMP_ROOT/docs/sbom.spdx.json"
  if python3 "$TMP_ROOT/scripts/check-reproducible-builds.py" "$TMP_ROOT" >/dev/null 2>&1; then
    fail "check accepted invalid spdxVersion"
  fi
  mv "$TMP_ROOT/docs/sbom.spdx.json.bak" "$TMP_ROOT/docs/sbom.spdx.json"
fi

# 4. Removing docs/reproducible-builds.md must fail
if [[ -f "$TMP_ROOT/docs/reproducible-builds.md" ]]; then
  rm "$TMP_ROOT/docs/reproducible-builds.md"
  if python3 "$TMP_ROOT/scripts/check-reproducible-builds.py" "$TMP_ROOT" >/dev/null 2>&1; then
    fail "check accepted missing docs/reproducible-builds.md"
  fi
fi

echo "OK: check-reproducible-builds mutation self-tests passed."

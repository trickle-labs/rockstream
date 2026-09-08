#!/usr/bin/env bash
# Fail if production CLI dispatch names a test fixture or mock client.
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
SOURCE_ROOT="${ROCKSTREAM_CLI_SOURCE_ROOT:-$ROOT/crates/rockstream-cli/src}"
DISPATCH_FILES=("$SOURCE_ROOT/main.rs" "$SOURCE_ROOT/lib.rs")

for dispatch in "${DISPATCH_FILES[@]}"; do
  if [ ! -f "$dispatch" ]; then
    echo "FAIL: production CLI dispatch source not found: $dispatch" >&2
    exit 1
  fi
done

if matches=$(rg -n -e 'with_defaults' -e 'with_mock_data' \
  -e 'MockCatalogClient' -e 'MockTopologyClient' -e 'MockOperationClient' \
  "$SOURCE_ROOT" -g '*.rs'); then
  echo "$matches" >&2
  echo "FAIL: fixture or mock client is reachable from production CLI dispatch." >&2
  exit 1
fi

echo "OK: production CLI dispatch uses no fixture constructors or mock clients."

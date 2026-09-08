#!/usr/bin/env bash
# Self-test for check-no-fixture-dispatch.sh.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-no-fixture-dispatch.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

mkdir -p "$TMP_ROOT/src"
printf '%s\n' 'fn main() {}' > "$TMP_ROOT/src/main.rs"
printf '%s\n' 'pub fn run() {}' > "$TMP_ROOT/src/lib.rs"
ROCKSTREAM_CLI_SOURCE_ROOT="$TMP_ROOT/src" bash "$CHECKER" "$TMP_ROOT" >/dev/null

printf '%s\n' 'fn main() { CatalogClient::with_defaults(); }' > "$TMP_ROOT/src/main.rs"
if ROCKSTREAM_CLI_SOURCE_ROOT="$TMP_ROOT/src" bash "$CHECKER" "$TMP_ROOT" >/dev/null 2>&1; then
  echo "FAIL: fixture dispatch was not rejected." >&2
  exit 1
fi

echo "OK: check-no-fixture-dispatch.sh self-test passed."

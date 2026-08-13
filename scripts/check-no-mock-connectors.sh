#!/usr/bin/env bash
# Reject connector source that describes itself as a mock rather than a real client.
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

violations=0
CONNECTOR_FILES=(
  crates/rockstream-connectors/src/kafka_source.rs
  crates/rockstream-connectors/src/kafka_sink.rs
  crates/rockstream-connectors/src/postgres_cdc.rs
)

while IFS= read -r file; do
  while IFS= read -r match; do
    [ -z "$match" ] && continue
    echo "VIOLATION: $file:$match: forbidden mock-connector prose"
    violations=$((violations + 1))
  done < <(grep -niE '^[[:space:]]*//.*(mock|simulated|in-production-this-would)' "$file" || true)
done < <(printf '%s\n' "${CONNECTOR_FILES[@]}")

if [ "$violations" -gt 0 ]; then
  echo "FAIL: $violations forbidden mock-connector prose violation(s) found." >&2
  exit 1
fi

echo "OK: no mock-connector prose found."

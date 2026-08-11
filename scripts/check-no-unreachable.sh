#!/usr/bin/env bash
# Reject unreachable!() calls on reachable input-dependent branches in production crates.
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

violations=0

while IFS= read -r match; do
  [ -z "$match" ] && continue
  echo "$match: unreachable! found in production source"
  violations=$((violations + 1))
done < <(grep -rn --include="*.rs" "unreachable!" crates/*/src/ || true)

if [ "$violations" -gt 0 ]; then
  echo "FAIL: $violations unreachable!() calls found in production source code."
  exit 1
fi

echo "OK: zero unreachable!() calls found in production source code."

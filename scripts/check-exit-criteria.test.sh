#!/usr/bin/env bash
# Self-test the exit-criteria checker, including the §8 admission gate.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-exit-criteria.sh"
TMP_ROOT="$(mktemp -d)"
OUT="$(mktemp)"
trap 'rm -rf "$TMP_ROOT"; rm -f "$OUT"' EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

cp "$ROOT/NEW_ROADMAP.md" "$TMP_ROOT/"
cp -R "$ROOT/sign-offs" "$TMP_ROOT/"
printf '%s\n' \
  '| v9.9 | Synthetic new connector family | Candidate surface | Unit |' \
  >> "$TMP_ROOT/NEW_ROADMAP.md"

if bash "$CHECKER" "$TMP_ROOT" >"$OUT" 2>&1; then
  cat "$OUT"
  fail "missing §8 admission checklist was accepted"
fi
grep -q "ADMISSION: v9.9" "$OUT" || {
  cat "$OUT"
  fail "missing admission diagnostic"
}

printf '%s\n' \
  '' \
  '### Admission: v9.9' \
  '## Product fit' \
  '- [x] Product fit is documented.' \
  '## Semantic fit' \
  '- [x] Semantics are documented.' \
  '## Operational fit' \
  '- [x] Operational behavior is bounded.' \
  '## Scope cost' \
  '- [x] Scope cost is accepted.' \
  '## Proof' \
  '- [x] Proof plan is named.' \
  >> "$TMP_ROOT/NEW_ROADMAP.md"

if ! bash "$CHECKER" "$TMP_ROOT" >"$OUT" 2>&1; then
  cat "$OUT"
  fail "completed §8 admission checklist was rejected"
fi

if ! bash "$CHECKER" >"$OUT" 2>&1; then
  cat "$OUT"
  fail "unmodified tree failed the exit-criteria check"
fi

echo "OK: check-exit-criteria.sh self-test passed."

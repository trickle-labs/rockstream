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

# Compare complete output and status for active roadmap and plan enforcement.
expect_check() {
  local expected_status="$1" expected_output="$2" actual_status=0
  bash "$CHECKER" "$TMP_ROOT" >"$OUT" 2>&1 || actual_status=$?
  if [ "$actual_status" -ne "$expected_status" ] || [ "$(cat "$OUT")" != "$expected_output" ]; then
    cat "$OUT"
    fail "exit-criteria status or complete output differed from expectation"
  fi
}

printf '%s\n' '| v0.60 | Product truth ✅ Done |' > "$TMP_ROOT/NEW_ROADMAP.md"
printf '%s\n' \
  '| v0.60.0 | Product truth ✅ Done |' \
  '| **v0.61.0** | Golden path ✅ Done |' \
  '| v0.61.1 | Baseline | Planned |' > "$TMP_ROOT/ROADMAP.md"
mkdir -p "$TMP_ROOT/docs/implementation-plans"
printf '%s\n' '| V061-01 | Exact project output | Transcript |' \
  '| V061-02 | Restart | Process proof |' > "$TMP_ROOT/docs/implementation-plans/v0.61.md"
printf '%s\n' '- [x] V061-01: code, docs, test, and result links' > "$TMP_ROOT/sign-offs/v0.61.md"
error_footer=$'\n\n1 sign-off problem(s) found.\nA version cannot be marked Done without a complete sign-offs/vX.Y.md file.\nUse \'make approve VERSION=X.Y\' to create the template.'
expect_check 1 $'OK: v0.60\nINCOMPLETE: sign-offs/v0.61.md lacks checked evidence for V061-02.'"$error_footer"

printf '%s\n' '- [x] V061-02:' >> "$TMP_ROOT/sign-offs/v0.61.md"
expect_check 1 $'OK: v0.60\nINCOMPLETE: sign-offs/v0.61.md lacks checked evidence for V061-02.'"$error_footer"
printf '%s\n' '- [x] V061-02: process restart evidence' >> "$TMP_ROOT/sign-offs/v0.61.md"
expect_check 0 $'OK: v0.60\nOK: v0.61\nAll Done versions have complete sign-offs.'

printf '%s\n' '| v0.61.1 | Baseline ✅ Done |' >> "$TMP_ROOT/ROADMAP.md"
printf '%s\n' '- [x] V0611-01: measured baseline evidence' > "$TMP_ROOT/sign-offs/v0.61.1.md"
expect_check 1 $'OK: v0.60\nOK: v0.61\nMISSING: v0.61.1 has no committed implementation plan.'"$error_footer"
printf '%s\n' 'No exit criteria yet.' > "$TMP_ROOT/docs/implementation-plans/v0.61.1.md"
expect_check 1 $'OK: v0.60\nOK: v0.61\nINCOMPLETE: v0.61.1 implementation plan has no exit criterion IDs.'"$error_footer"
printf '%s\n' '| V0611-01 | Baseline | Measurements |' > "$TMP_ROOT/docs/implementation-plans/v0.61.1.md"
expect_check 0 $'OK: v0.60\nOK: v0.61\nOK: v0.61.1\nAll Done versions have complete sign-offs.'

printf '%s\n' '- [ ] V0611-02: unavailable measurement' >> "$TMP_ROOT/sign-offs/v0.61.1.md"
expect_check 1 $'OK: v0.60\nOK: v0.61\nINCOMPLETE: sign-offs/v0.61.1.md has unchecked items — all must be checked off before marking Done.'"$error_footer"

printf '%s\n' '| v9.9 | Synthetic new connector family | Candidate |' > "$TMP_ROOT/NEW_ROADMAP.md"
printf '%s\n' '| v0.61 | Golden path | Planned |' > "$TMP_ROOT/ROADMAP.md"
expect_check 1 $'ADMISSION: v9.9 is a new product-surface row without a completed §8 checklist.\n\n1 exit-criteria problem(s) found.\nA version cannot be marked Done without a complete sign-offs/vX.Y.md file.\nUse \'make approve VERSION=X.Y\' to create the template.'

echo "OK: check-exit-criteria.sh self-test passed."

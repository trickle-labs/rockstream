#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
DENY="$ROOT/deny.toml"
POLICY="$ROOT/DEPENDENCY_POLICY.md"
FIXTURE="$ROOT/scripts/fixtures/known-vulnerable"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1"
  exit 1
}

check_exception_records() {
  local deny_file="$1"
  local policy_file="$2"
  local today
  today="$(date -u +%F)"

  while IFS= read -r advisory; do
    [[ -n "$advisory" ]] || continue
    local row
    row="$(grep -F "| $advisory |" "$policy_file" || true)"
    [[ -n "$row" ]] || { printf 'missing exception record: %s\n' "$advisory"; return 1; }
    local owner rationale review removal
    owner="$(awk -F'|' '{gsub(/^ +| +$/, "", $3); print $3}' <<<"$row")"
    rationale="$(awk -F'|' '{gsub(/^ +| +$/, "", $4); print $4}' <<<"$row")"
    review="$(awk -F'|' '{gsub(/^ +| +$/, "", $5); print $5}' <<<"$row")"
    removal="$(awk -F'|' '{gsub(/^ +| +$/, "", $6); print $6}' <<<"$row")"
    [[ -n "$owner" && -n "$rationale" && -n "$removal" ]] || {
      printf 'incomplete exception record: %s\n' "$advisory"
      return 1
    }
    [[ "$review" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ && "$review" > "$today" ]] || {
      printf 'expired or invalid review date: %s\n' "$advisory"
      return 1
    }
  done < <(grep -oE 'RUSTSEC-[0-9]{4}-[0-9]{4}' "$deny_file" | sort -u)
}

check_exception_records "$DENY" "$POLICY" || fail "real advisory exceptions are not governed"

audit_output="$TMP_ROOT/audit.out"
if cargo audit --no-fetch --file "$FIXTURE/Cargo.lock" >"$audit_output" 2>&1; then
  fail "cargo audit accepted the known-vulnerable fixture"
fi
grep -Eq 'RUSTSEC-|vulnerabilit' "$audit_output" || fail "cargo audit did not report an advisory"

deny_output="$TMP_ROOT/deny.out"
if cargo deny --manifest-path "$FIXTURE/Cargo.toml" --config "$DENY" check advisories >"$deny_output" 2>&1; then
  fail "cargo deny accepted the known-vulnerable fixture"
fi
grep -Eq 'RUSTSEC-|advis|vulnerab' "$deny_output" || fail "cargo deny did not report an advisory"

grep -v 'RUSTSEC-2025-0141' "$POLICY" >"$TMP_ROOT/policy-missing.md"
if check_exception_records "$DENY" "$TMP_ROOT/policy-missing.md" >/dev/null 2>&1; then
  fail "missing exception record was accepted"
fi

sed 's/2027-01-31/2020-01-31/' "$POLICY" >"$TMP_ROOT/policy-expired.md"
if check_exception_records "$DENY" "$TMP_ROOT/policy-expired.md" >/dev/null 2>&1; then
  fail "expired exception record was accepted"
fi

echo "OK: dependency audit gates and exception-record self-test passed."

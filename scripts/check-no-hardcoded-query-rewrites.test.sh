#!/usr/bin/env bash
# check-no-hardcoded-query-rewrites.test.sh — self-test harness for
# check-no-hardcoded-query-rewrites.sh.
#
# Verifies the checker actually *detects* a reintroduced hardcoded
# query-rewrite (not just vacuously passing) by mutating a temp copy of the
# tree to reinsert an exact-string SQL-literal comparison (the same shape
# as the deleted `rewrite_session_sql`), then confirms:
#   1. The checker exits 1 against the mutated copy and names the deleted
#      file/line in its output.
#   2. The checker exits 0 against the real, unmodified tree.
#
# Invoked as its own CI step, alongside (not instead of) the
# check-no-hardcoded-query-rewrites.sh step itself, so a regression in the
# checker script is caught even if crates/ never regains a hardcoded
# rewrite.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHECKER="$ROOT/scripts/check-no-hardcoded-query-rewrites.sh"

TMP="$(mktemp -d)"
OUT_MUTATED="$(mktemp)"
OUT_REAL="$(mktemp)"
cleanup() {
  rm -rf "$TMP"
  rm -f "$OUT_MUTATED" "$OUT_REAL"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

# ── Case 1: a tree with a deliberately reintroduced hardcoded rewrite must
#    fail (exit 1) ───────────────────────────────────────────────────────────
mkdir -p "$TMP/crates/rockstream-gateway/src"
cp -R "$ROOT/crates/." "$TMP/crates/"
MUTATED_FILE="$TMP/crates/rockstream-gateway/src/probe_hardcoded_rewrite.rs"
cat >"$MUTATED_FILE" <<'RUST'
fn rewrite_probe_sql(sql: &str) -> String {
    let normalized = sql.trim().to_ascii_lowercase();
    if normalized
        == "select bidder, count(*) as bid_count from bid group by bidder, session(date_time, interval '10 seconds')"
    {
        return "SELECT 1".to_string();
    }
    sql.to_string()
}
RUST

if bash "$CHECKER" "$TMP" >"$OUT_MUTATED" 2>&1; then
  cat "$OUT_MUTATED"
  fail "checker passed on a tree with a reintroduced hardcoded rewrite (expected exit 1)"
fi
grep -q "probe_hardcoded_rewrite.rs" "$OUT_MUTATED" ||
  fail "checker's failure output did not name the mutated file"

# ── Case 2: the real, unmodified tree must pass (exit 0) ────────────────────
if ! bash "$CHECKER" >"$OUT_REAL" 2>&1; then
  cat "$OUT_REAL"
  fail "checker failed against the real, unmodified tree"
fi

echo "OK: check-no-hardcoded-query-rewrites.sh self-test passed."

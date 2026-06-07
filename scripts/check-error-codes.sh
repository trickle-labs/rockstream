#!/usr/bin/env bash
# check-error-codes.sh — enforce that every operator-visible logged error
# carries an `RS-XXXX` error code.
#
# Called by CI on every push and PR. Also callable locally:
#   ./scripts/check-error-codes.sh
#
# Rule: every `error!(...)` / `tracing::error!(...)` invocation must reference an
# error code — either an `RS-XXXX` literal, an `error_code` path, an `ErrorCode`
# value, or a `code = ...` structured field. A logged error without a code is a
# build-breaking violation: it gives operators no actionable handle.
#
# User-facing *returned* errors carry codes through the `rockstream-types`
# error-code registry (see `crates/rockstream-types/src/error_code.rs`), whose
# own test asserts every registered code has a description and actionable
# next-steps text.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

violations=0

# Scan every tracked Rust source file under crates/.
while IFS= read -r file; do
  # awk accumulates each `error!( ... );` statement (which may span multiple
  # lines) and checks the accumulated text for a code reference.
  out=$(awk '
    BEGIN { inmac = 0; buf = ""; startln = 0 }
    {
      if (inmac == 0) {
        if ($0 ~ /(^|[^a-zA-Z0-9_])error!\(/) {
          inmac = 1; buf = $0; startln = NR
        }
      } else {
        buf = buf "\n" $0
      }
      if (inmac == 1 && index($0, ";") > 0) {
        if (buf ~ /RS-[0-9]{4}/ || buf ~ /error_code/ || buf ~ /ErrorCode/ || buf ~ /code[ \t]*=/) {
          # has a code reference — ok
        } else {
          printf("%s:%d: error! logged without an RS-XXXX code\n", FILENAME, startln)
        }
        inmac = 0; buf = ""
      }
    }
  ' "$file")
  if [ -n "$out" ]; then
    echo "$out"
    violations=$((violations + 1))
  fi
done < <(find crates -name '*.rs' -not -path '*/target/*' | sort)

if [ "$violations" -gt 0 ]; then
  echo ""
  echo "Found logged error(s) without an RS-XXXX code."
  echo "Every error!/tracing::error! must reference a registered code from"
  echo "crates/rockstream-types/src/error_code.rs (e.g. code = %RS_0001)."
  exit 1
fi

echo "OK: all logged errors carry an RS-XXXX code."

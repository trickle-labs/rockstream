#!/usr/bin/env bash
# check-error-codes.sh — enforce that every operator-visible logged or
# returned error carries an `RS-XXXX` error code.
#
# Called by CI on every push and PR. Also callable locally:
#   ./scripts/check-error-codes.sh
#
# Usage: ./scripts/check-error-codes.sh [ROOT_DIR]
#   ROOT_DIR defaults to the git repository root. An explicit ROOT_DIR is
#   used by check-error-codes.test.sh to run the check against a mutated
#   copy of the tree (self-test).
#
# Rule 1 (logged errors): every `error!(...)` / `tracing::error!(...)`
# invocation must reference an error code — either an `RS-XXXX` literal, an
# `error_code` path, an `ErrorCode` value, or a `code = ...` structured
# field. A logged error without a code is a build-breaking violation: it
# gives operators no actionable handle.
#
# Rule 2 (bare returned errors, v0.45.7): every ad hoc, terminal error
# construction site — `Err(format!(...))`, `Err(String::from(...))`,
# `Err("...".to_string()/.into())`, `.ok_or_else(|| format!(...))`, or
# `.ok_or_else(|| "...")` — must reference an `RS-XXXX` literal, an
# `error_code` path, or an `ErrorCode` value. A site is exempt if the
# constructed value is immediately re-raised via the `?`-operator (i.e. the
# last non-whitespace token before the terminating `;` is `?`) — this is a
# context-wrapping propagation idiom (e.g. `.map_err(|e| format!(...))?`),
# not a fresh ad hoc error, and is intentionally out of scope. Sites inside
# `#[cfg(test)]` modules are excluded (test-only assertions, not
# operator-facing). CLI-argument-parsing binaries under `crates/*/src/bin/`
# are excluded — they are standalone CI tooling, not the `rockstream`
# product binary, and their errors are developer/CI console output, never
# an operator- or user-visible pipeline failure.
#
# User-facing *returned* errors carry codes through the `rockstream-types`
# error-code registry (see `crates/rockstream-types/src/error_code.rs`), whose
# own test asserts every registered code has a description and actionable
# next-steps text.
set -euo pipefail

if [ "$#" -ge 1 ]; then
  ROOT="$1"
else
  ROOT="$(git rev-parse --show-toplevel)"
fi
cd "$ROOT"

violations=0

# ── Pass 1: `error!(...)` / `tracing::error!(...)` must carry a code ────────

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
done < <(find crates -name '*.rs' -not -path '*/target/*' -not -path '*/src/bin/*' | sort)

# ── Pass 2: bare ad hoc `Err(...)` / `.ok_or_else(...)` construction sites ──
# must carry a code, unless immediately propagated via `?`. Sites inside
# `#[cfg(test)]` modules are skipped via a crude brace-depth tracker (same
# heuristic class as pass 1's accumulate-until-`;` technique). Termination of
# a construct is driven by parenthesis-depth returning to zero (not a bare
# semicolon scan), since `next_steps` prose routinely embeds literal `;`
# characters (e.g. "check X; also check Y") that would otherwise truncate
# the accumulated text before it reaches the trailing code reference.

while IFS= read -r file; do
  out=$(awk '
    BEGIN {
      instate = 0; buf = ""; startln = 0; paren_depth = 0
      intest = 0; testdepth = -1; depth = 0; pending_test_attr = 0
    }
    {
      line = $0
      open_count = gsub(/\{/, "{", line)
      close_count = gsub(/\}/, "}", line)

      if ($0 ~ /^[ \t]*#\[cfg\(test\)\]/) { pending_test_attr = 1 }
      if (pending_test_attr && $0 ~ /mod[ \t]+[A-Za-z0-9_]+[ \t]*\{/) {
        intest = 1
        testdepth = depth
        pending_test_attr = 0
      }

      depth += open_count
      depth -= close_count

      if (intest && depth <= testdepth) { intest = 0 }

      if (intest) {
        instate = 0; buf = ""; paren_depth = 0
        next
      }

      if (instate == 0) {
        if ($0 ~ /(^|[^a-zA-Z0-9_])Err\(format!\(/ ||
            $0 ~ /(^|[^a-zA-Z0-9_])Err\(String::from\(/ ||
            $0 ~ /(^|[^a-zA-Z0-9_])Err\([ \t]*"[^"]*"[ \t]*\.[ \t]*(to_string\(\)|into\(\))/ ||
            $0 ~ /\.ok_or_else\([ \t]*\|\|[ \t]*format!\(/ ||
            $0 ~ /\.ok_or_else\([ \t]*\|\|[ \t]*"[^"]*"[ \t]*\.[ \t]*(to_string\(\)|into\(\))/) {
          instate = 1; startln = NR; buf = ""; paren_depth = 0
        }
      }

      if (instate == 1) {
        pcopy = $0
        popens = gsub(/\(/, "(", pcopy)
        pcopy2 = $0
        pcloses = gsub(/\)/, ")", pcopy2)
        paren_depth += popens - pcloses

        buf = (buf == "" ? $0 : buf "\n" $0)

        # The construct ends once the opening call parens balance
        # back to zero — not at the next bare ";", since next_steps prose
        # routinely embeds literal ";" characters that would otherwise
        # truncate the accumulated text before it reaches a trailing code
        # reference or ?-operator that appears later on the same line (or a
        # semicolon that belongs to an unrelated enclosing statement).
        if (paren_depth <= 0) {
          final_text = buf

          if (final_text ~ /RS-[0-9]{4}/ || final_text ~ /RS_[0-9]{4}/ ||
              final_text ~ /error_code/ || final_text ~ /ErrorCode/) {
            # has a code reference — ok
          } else {
            tmp = final_text
            gsub(/[ \t\n]+$/, "", tmp)
            if (substr(tmp, length(tmp), 1) == ";") {
              tmp = substr(tmp, 1, length(tmp) - 1)
              gsub(/[ \t\n]+$/, "", tmp)
            }
            last_char = substr(tmp, length(tmp), 1)
            if (last_char == "?") {
              # immediately propagated via ?-operator — exempt
            } else {
              printf("%s:%d: bare error construction without an RS-XXXX code\n", FILENAME, startln)
            }
          }
          instate = 0; buf = ""; paren_depth = 0
        }
      }
    }
  ' "$file")
  if [ -n "$out" ]; then
    echo "$out"
    violations=$((violations + 1))
  fi
done < <(find crates -name '*.rs' -not -path '*/target/*' -not -path '*/src/bin/*' | sort)

if [ "$violations" -gt 0 ]; then
  echo ""
  echo "Found error(s) without an RS-XXXX code."
  echo "Every error!/tracing::error! and every bare Err(format!(...))/"
  echo "Err(String::from(...))/Err(\"...\")/.ok_or_else(|| format!(...))/"
  echo ".ok_or_else(|| \"...\") construction must reference a registered code"
  echo "from crates/rockstream-types/src/error_code.rs, unless immediately"
  echo "propagated via the ?-operator."
  exit 1
fi

echo "OK: all logged/returned errors carry an RS-XXXX code."

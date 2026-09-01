#!/usr/bin/env bash
# Enforce zero-hidden-skip policy across test suites and codebase.
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

violations=0

# 1. Check for unrecorded / undocumented #[ignore] without an explanation
while IFS= read -r match; do
  [ -z "$match" ] && continue
  file="$(printf '%s\n' "$match" | cut -d: -f1)"
  lineno="$(printf '%s\n' "$match" | cut -d: -f2)"
  # Check if previous 2 lines or same line contain reason comment or tracking
  context="$(sed -n "$((lineno > 2 ? lineno - 2 : 1)),${lineno}p" "$file" 2>/dev/null || true)"
  if ! printf '%s\n' "$context" | grep -qiE '(reason|track|issue|waiver|feature|fuzz|flaky)'; then
    echo "VIOLATION: $match: undocumented #[ignore] attribute without explanatory comment"
    violations=$((violations + 1))
  fi
done < <(grep -rn --include="*.rs" -E '^[[:space:]]*#\[ignore\]' crates/ 2>/dev/null || true)

# 2. Check for silent skips without structured logging (e.g. return on !docker_available without eprintln/log)
while IFS= read -r match; do
  [ -z "$match" ] && continue
  file="$(printf '%s\n' "$match" | cut -d: -f1)"
  lineno="$(printf '%s\n' "$match" | cut -d: -f2)"
  block="$(sed -n "${lineno},$((lineno + 6))p" "$file" 2>/dev/null || true)"
  if printf '%s\n' "$block" | grep -qE 'return(;|\s+None|\s+Ok)' && ! printf '%s\n' "$block" | grep -qE '(eprintln|println|warn|info|SKIP|skip)'; then
    echo "VIOLATION: $match: silent early return on docker availability without logging skip reason"
    violations=$((violations + 1))
  fi
done < <(grep -rn --include="*.rs" "if !.*docker_available" crates/ 2>/dev/null || true)

if [ "$violations" -gt 0 ]; then
  echo "FAIL: $violations hidden test skip violation(s) found." >&2
  exit 1
fi

echo "OK: no hidden test skips found."

#!/usr/bin/env bash
# check-no-hardcoded-query-rewrites.sh — CI gate: no exact-string SQL-literal
# comparison anywhere in crates/.
#
# v0.51.4 Slice 6 deleted `rewrite_session_sql` (an exact-string,
# whitespace-normalized, lowercased match against one specific Nexmark q11
# SQL text, rewritten to a DataFusion-executable equivalent) once
# `PlanNode::SessionWindow` compiled through `compile_plan`/`StatefulPipeline`
# for real. This script guards against that pattern being reintroduced
# anywhere — a hardcoded per-query string match is a "works for this one
# query" trap, not a real fix.
#
# Called by CI's `check` job. Also callable locally:
#   ./scripts/check-no-hardcoded-query-rewrites.sh
#
# Usage: ./scripts/check-no-hardcoded-query-rewrites.sh [ROOT_DIR]
#   ROOT_DIR defaults to the git repository root. An explicit ROOT_DIR is
#   used by check-no-hardcoded-query-rewrites.test.sh to run the check
#   against a mutated copy of the tree (self-test).
#
# What counts as a violation: a normalized (whitespace-collapsed and/or
# lowercased) SQL string being compared with `==` for *exact equality*
# against a full `select ... from ...`-shaped string literal — the same
# shape as the deleted `rewrite_session_sql` (which lowercased/whitespace-
# normalized a whole query and compared it against one specific literal
# statement text). Deliberately narrow to full-statement literals (must
# contain both "select" and "from") so keyword-shape sniffing like
# `sql.contains("select *")` (a legitimate "is this a star-select" check,
# not a per-query rewrite) is not flagged — only `.contains(` is exempt from
# this rule; `==` full-statement equality is the actual per-query-rewrite
# smell.
set -euo pipefail

if [ "$#" -ge 1 ]; then
  ROOT="$1"
else
  ROOT="$(git rev-parse --show-toplevel)"
fi
cd "$ROOT"

violations=0

while IFS= read -r file; do
  # Match (case-insensitively) a `==` or `.contains(` immediately followed
  # by a double-quoted string literal whose content starts (after any
  # leading whitespace inside the quotes) with the word "select" *and*
  # also contains "from" later in the same literal — i.e. a full
  # `select ... from ...`-shaped statement literal, not a bare keyword
  # fragment like `"select *"`.
  out=$(grep -nEi '(==[[:space:]]*"[[:space:]]*select[^"]*from[^"]*"|\.contains\([[:space:]]*"[[:space:]]*select[^"]*from[^"]*")' "$file" || true)
  if [ -n "$out" ]; then
    while IFS= read -r line; do
      lineno="${line%%:*}"
      echo "VIOLATION: $file:$lineno: hardcoded SQL-literal exact-match/contains (see script header)"
      violations=$((violations + 1))
    done <<<"$out"
  fi
done < <(find crates -name '*.rs' | sort)

if [ "$violations" -gt 0 ]; then
  echo ""
  echo "FAIL: $violations hardcoded SQL-query-rewrite violation(s) found." >&2
  echo "A per-query exact-string match/rewrite is not a real fix — compile the" >&2
  echo "query shape through compile_plan/StatefulPipeline (or rockstream-sql's" >&2
  echo "frontend lowering) instead. See v0.51.4's rewrite_session_sql removal." >&2
  exit 1
fi

echo "OK: no hardcoded SQL-query-rewrite patterns found."

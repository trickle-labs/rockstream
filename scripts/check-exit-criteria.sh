#!/usr/bin/env bash
# check-exit-criteria.sh — enforce that every version marked ✅ Done in
# NEW_ROADMAP.md has a complete sign-off file in sign-offs/.
#
# Called by CI on every push and PR. Also callable locally: ./scripts/check-exit-criteria.sh
#
# A sign-off file is considered complete when it has no unchecked checklist
# items (i.e. no '- [ ]' lines remain).
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
ROADMAP="$ROOT/NEW_ROADMAP.md"
SIGNOFFS_DIR="$ROOT/sign-offs"
ERRORS=0

# Extract versions marked Done from roadmap version table rows.
# A Done row carries the explicit marker "✅ Done", e.g.:
#   | v0.1 | Workspace and CI ✅ Done | ... |
# We only extract the version from the first column to avoid false matches
# on version numbers mentioned inside the row description text.
#
# The version number may have two OR three dot-separated components
# (e.g. `v0.42` or `v0.42.1` for a remediation sub-version) — a two-part-only
# pattern silently skips every three-part row and its sign-off requirement,
# which is exactly the bug found by the <=v0.42.3 implementation review
# (2026-07-10): v0.42.1/v0.42.2/v0.42.3 were marked Done with no sign-off
# file and this check never noticed.
done_versions=$(grep -E '^\| v[0-9]+\.[0-9]+(\.[0-9]+)? \|.*✅ Done' "$ROADMAP" \
  | sed 's/^| \(v[0-9]*\.[0-9]*\(\.[0-9]*\)*\) |.*/\1/' || true)

if [ -z "$done_versions" ]; then
  echo "No versions marked Done in NEW_ROADMAP.md."
  exit 0
fi

for version in $done_versions; do
  signoff="$SIGNOFFS_DIR/${version}.md"

  if [ ! -f "$signoff" ]; then
    echo "MISSING: $version is marked Done in NEW_ROADMAP.md but sign-offs/${version}.md does not exist."
    echo "  Run: make approve VERSION=${version#v}"
    ERRORS=$((ERRORS + 1))
    continue
  fi

  # Fail if any checklist item is still unchecked.
  # Search for '[ ]' (unchecked box) using -e to avoid BSD grep treating
  # patterns starting with '-' as option flags.
  if grep -q -e '\[ \]' "$signoff"; then
    echo "INCOMPLETE: sign-offs/${version}.md has unchecked items — all must be checked off before marking Done."
    ERRORS=$((ERRORS + 1))
    continue
  fi

  echo "OK: $version"
done

if [ "$ERRORS" -gt 0 ]; then
  echo ""
  echo "$ERRORS sign-off problem(s) found."
  echo "A version cannot be marked Done without a complete sign-offs/vX.Y.md file."
  echo "Use 'make approve VERSION=X.Y' to create the template."
  exit 1
fi

echo "All Done versions have complete sign-offs."

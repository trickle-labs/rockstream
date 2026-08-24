#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
required=(
  docs/README.md docs/getting-started.md docs/operator.md docs/contributors.md
  docs/history.md docs/adr/README.md docs/adr/0001-documentation-navigation.md
  docs/adr/0002-reference-compatibility.md
  docs/reference/cli.md docs/reference/configuration.md
  docs/reference/functions.md docs/reference/sql-support.md
  docs/reference/catalog.md docs/reference/metrics.md docs/reference/errors.md
)

for path in "${required[@]}"; do
  test -f "$ROOT/$path" || { echo "missing documentation target: $path" >&2; exit 1; }
done

rg -q 'reference/cli\.md' "$ROOT/docs/cli.md"
rg -q 'reference/configuration\.md' "$ROOT/docs/configuration.md"
rg -q 'reference/errors\.md' "$ROOT/docs/error-codes.md"
rg -q 'reference/functions\.md' "$ROOT/docs/language-features.md"
rg -q 'reference/sql-support\.md' "$ROOT/docs/language-features.md"
echo "OK: documentation navigation targets are present."

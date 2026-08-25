#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=$("$root/scripts/check-diagnostic-coverage.sh" "$root")
test "$output" = "diagnostic coverage: pass"

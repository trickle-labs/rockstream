#!/usr/bin/env bash
# check-invariant-pairs.sh — CI cross-check that every FizzBee invariant
# modeled in formal/*.fizz has a paired Rust runtime assertion (or a
# justified INVARIANT-BY-CONSTRUCTION comment) somewhere in crates/.
#
# Called by CI's `check` job, immediately after check-error-codes.sh. Also
# callable locally:
#   ./scripts/check-invariant-pairs.sh
#
# Usage: ./scripts/check-invariant-pairs.sh [ROOT_DIR]
#   ROOT_DIR defaults to the git repository root. An explicit ROOT_DIR is
#   used by check-invariant-pairs.test.sh to run the check against a
#   mutated copy of the tree (self-test).
#
# See scripts/check_invariant_pairs.py for the parsing/matching logic.
set -euo pipefail

if [ "$#" -ge 1 ]; then
  ROOT="$1"
else
  ROOT="$(git rev-parse --show-toplevel)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec python3 "$SCRIPT_DIR/check_invariant_pairs.py" "$ROOT"

#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
DOC="$ROOT/docs/threat-model.md"
violations=0

fail() {
  printf 'VIOLATION: %s\n' "$1"
  violations=$((violations + 1))
}

if [[ ! -f "$DOC" ]]; then
  fail "missing docs/threat-model.md"
else
  boundaries=(
    "Client to pgwire gateway"
    "Pgwire DDL/DML authorization"
    "Operator CLI to control plane"
    "Worker to control plane"
    "Worker to worker shuffle"
    "TLS rollover"
    "Secret storage and connector credential resolution"
    "SQL injection and malformed ingress"
    "Dependency supply chain"
  )

  for boundary in "${boundaries[@]}"; do
    line=$(grep -F "| $boundary |" "$DOC" || true)
    if [[ -z "$line" ]]; then
      fail "missing boundary: $boundary"
      continue
    fi

    cells=$(awk -F'|' '{ for (i = 2; i <= 7; i++) { gsub(/^ +| +$/, "", $i); print $i } }' <<<"$line")
    mapfile -t fields <<<"$cells"
    for index in 0 1 2 3 4 5; do
      if [[ -z "${fields[$index]:-}" || "${fields[$index]}" == "---" ]]; then
        fail "$boundary has an empty required field"
      fi
    done

    proof="${fields[5]//\`/}"
    if [[ "$proof" == *"::"* ]]; then
      proof_file="${proof%%::*}"
      proof_symbol="${proof##*::}"
      if [[ ! -f "$ROOT/$proof_file" ]]; then
        fail "$boundary proof file is missing: $proof_file"
      elif ! grep -Fq "$proof_symbol" "$ROOT/$proof_file"; then
        fail "$boundary proof symbol is missing: $proof"
      fi
    elif [[ ! -f "$ROOT/$proof" ]]; then
      fail "$boundary proof link is missing: $proof"
    fi
  done
fi

if (( violations > 0 )); then
  exit 1
fi

echo "OK: threat-model boundaries and proof links passed."

#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

LIB="crates/rockstream-connectors/src/lib.rs"
DOC="docs/connectors.md"
BASELINE="|kafka_source|kafka_sink|postgres_cdc|"
KEYS=(
  core_ivm_improvement
  no_kafka_or_postgres_boundary
  demonstrated_production_demand
  failure_and_recovery_semantics
  acceptable_maintenance_burden
  permanent_compatibility_value
)

violations=0
while IFS= read -r module; do
  file="crates/rockstream-connectors/src/$module.rs"
  if [[ ! -f "$file" ]] || ! awk '
    /#\[cfg\(test\)\]/ { exit }
    /impl[[:space:]]+(SourceConnector|SinkConnector)(<[^>]+>)?[[:space:]]+for[[:space:]]+/ { found=1 }
    END { exit(found ? 0 : 1) }
  ' "$file"; then
    continue
  fi
  [[ "$BASELINE" == *"|$module|"* ]] && continue

  heading="### Admission: $module"
  record="$(awk -v heading="$heading" '
    $0 == heading { found=1; next }
    found && /^### / { exit }
    found { print }
  ' "$DOC")"
  if [[ -z "${record//[[:space:]]/}" ]]; then
    echo "VIOLATION: $module: missing admission record ($heading)"
    violations=$((violations + 1))
    continue
  fi

  shopt -s nocasematch
  for key in "${KEYS[@]}"; do
    count="$(printf '%s\n' "$record" | grep -cE "^[[:space:]]*$key[[:space:]]*:" || true)"
    if [[ "$count" -eq 0 ]]; then
      echo "VIOLATION: $module: missing admission key $key"
      violations=$((violations + 1))
      continue
    fi
    if [[ "$count" -gt 1 ]]; then
      echo "VIOLATION: $module: duplicate admission key $key"
      violations=$((violations + 1))
      continue
    fi
    value="$(printf '%s\n' "$record" | sed -nE "s/^[[:space:]]*$key[[:space:]]*:[[:space:]]*(.*)$/\1/p")"
    if [[ -z "${value//[[:space:]]/}" ]]; then
      echo "VIOLATION: $module: blank admission key $key"
      violations=$((violations + 1))
    elif [[ "$value" =~ TBD|N/A|TODO ]]; then
      echo "VIOLATION: $module: placeholder admission key $key"
      violations=$((violations + 1))
    fi
  done
  shopt -u nocasematch
done < <(sed -nE 's/^[[:space:]]*pub[[:space:]]+mod[[:space:]]+([a-zA-Z0-9_]+)[[:space:]]*;[[:space:]]*$/\1/p' "$LIB")

if [[ "$violations" -gt 0 ]]; then
  echo "FAIL: connector admission check found $violations violation(s)." >&2
  exit 1
fi

echo "OK: connector admission check passed."

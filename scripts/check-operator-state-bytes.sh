#!/usr/bin/env bash
# Reject un-accounted state arrangement fields in rockstream-ops stateful operators.
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

OPERATOR_FILES=(
  crates/rockstream-ops/src/join.rs
  crates/rockstream-ops/src/outer_join.rs
  crates/rockstream-ops/src/aggregate.rs
  crates/rockstream-ops/src/minmax.rs
  crates/rockstream-ops/src/distinct.rs
  crates/rockstream-ops/src/topk.rs
  crates/rockstream-ops/src/window.rs
  crates/rockstream-ops/src/time_window.rs
  crates/rockstream-ops/src/index_arrange.rs
  crates/rockstream-ops/src/lateral.rs
  crates/rockstream-ops/src/recursion.rs
)

violations=0

for file in "${OPERATOR_FILES[@]}"; do
  if [ ! -f "$file" ]; then
    echo "ERROR: file $file not found"
    violations=$((violations + 1))
    continue
  fi

  # Check 1: Must implement state_bytes
  if ! grep -q "fn state_bytes" "$file"; then
    echo "$file: missing state_bytes() implementation"
    violations=$((violations + 1))
  fi

  # Check 2: Verify collection fields in State structs have state_bytes accounting.
  while read -r field; do
    [ -z "$field" ] && continue
    count=$(grep -cw "$field" "$file" || true)
    if [ "$count" -lt 2 ] || ! grep -q "state_bytes" "$file"; then
      echo "$file: collection field '$field' in state struct is not referenced in accounting logic"
      violations=$((violations + 1))
    fi
  done < <(awk '
    /pub struct [A-Za-z0-9_]*State|struct [A-Za-z0-9_]*State/ { in_struct=1; next }
    in_struct && /}/ { in_struct=0 }
    in_struct {
      if ($0 ~ /^[[:space:]]*\/\// || $0 ~ /^[[:space:]]*\/\*/) next;
      if ($0 ~ /HashMap|BTreeMap|BTreeSet|HashSet|VecDeque|Vec/) {
        split($0, parts, ":")
        gsub(/^[[:space:]]+/, "", parts[1])
        gsub(/pub[[:space:]]+/, "", parts[1])
        field = parts[1]
        if (length(field) > 0 && field !~ /^\//) {
          print field
        }
      }
    }
  ' "$file")
done

if [ "$violations" -gt 0 ]; then
  echo "FAIL: $violations accounting violation(s) found."
  exit 1
fi

echo "OK: all stateful operators implement state_bytes accounting for arrangement fields."

#!/usr/bin/env bash
# Reject poisoning locks in the v0.51.19 audited state owners.
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel)}"
cd "$ROOT"

AUDITED_FILES=(
  crates/rockstream-gateway/src/server.rs
  crates/rockstream-gateway/src/auth.rs
  crates/rockstream-control/src/namespace.rs
  crates/rockstream-control/src/acl.rs
  crates/rockstream-sql/src/frontend.rs
  crates/rockstream-types/src/dlq.rs
)
violations=0

for file in "${AUDITED_FILES[@]}"; do
  while IFS= read -r match; do
    [ -z "$match" ] && continue
    echo "$file:$match: unapproved std::sync poisoning lock"
    violations=$((violations + 1))
  done < <(grep -nE 'std::sync::(Mutex|RwLock)|use std::sync::\{[^}]*\b(Mutex|RwLock)\b' "$file" || true)
done

if [ "$violations" -gt 0 ]; then
  exit 1
fi

echo "OK: no unapproved std::sync poisoning locks in audited sources."

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
EXCEPTION_FILES=(
  crates/rockstream-connectors/src/delta_sink.rs
  crates/rockstream-connectors/src/iceberg_sink.rs
)
violations=0

for file in "${AUDITED_FILES[@]}"; do
  while IFS= read -r match; do
    [ -z "$match" ] && continue
    echo "$file:$match: unapproved std::sync poisoning lock"
    violations=$((violations + 1))
  done < <(grep -nE 'std::sync::(Mutex|RwLock)|use std::sync::\{[^}]*\b(Mutex|RwLock)\b' "$file" || true)
done

for file in "${EXCEPTION_FILES[@]}"; do
  imports=$(grep -Ec '^[[:space:]]*use std::sync::Mutex;$' "$file" || true)
  if [ "$imports" -ne 1 ] \
    || ! grep -Fq 'Drop the mutex lock before the `.await` points below' "$file" \
    || ! grep -Fq 'std::sync::MutexGuard across an await is flagged by clippy' "$file"; then
    echo "$file: expected exactly one documented test-only std::sync::Mutex exception"
    violations=$((violations + 1))
  fi
done

if [ "$violations" -gt 0 ]; then
  exit 1
fi

echo "OK: no unapproved std::sync poisoning locks in audited sources."

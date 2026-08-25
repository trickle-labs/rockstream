#!/usr/bin/env bash
set -euo pipefail

root=${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
required=(
  "crates/rockstream-types/src/diagnostic.rs:pub struct DiagnosticOccurrence"
  "crates/rockstream-gateway/src/error.rs:pub fn diagnostic_occurrence"
  "crates/rockstream-gateway/src/error.rs:record_diagnostic"
  "crates/rockstream-gateway/src/server.rs:diagnostic_query_response"
  "crates/rockstream-gateway/src/session.rs:pub occurrence: DiagnosticOccurrence"
  "crates/rockstream-cli/src/lib.rs:pub occurrence: DiagnosticOccurrence"
  "crates/rockstream-cli/src/output.rs:pub struct CliErrorEnvelope"
)

for entry in "${required[@]}"; do
    file=${entry%%:*}
    pattern=${entry#*:}
    if ! rg -q --fixed-strings "$pattern" "$root/$file"; then
        echo "missing diagnostic coverage: $file: $pattern" >&2
        exit 1
    fi
done

if rg -q --fixed-strings "#[error(" "$root/crates/rockstream-gateway/src/error.rs"; then
    echo "gateway error display bypasses DiagnosticOccurrence" >&2
    exit 1
fi

echo "diagnostic coverage: pass"

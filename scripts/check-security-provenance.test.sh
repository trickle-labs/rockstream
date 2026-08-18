#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

fail() {
  printf 'FAIL: %s\n' "$1"
  exit 1
}

# 1. Base run on repository should pass
python3 "$ROOT/scripts/check-security-provenance.py" "$ROOT" >/dev/null 2>&1 || fail "baseline check failed on valid repo"

# Prepare temporary testing clone/tree
mkdir -p "$TMP_ROOT/docs" "$TMP_ROOT/scripts"
cp -r "$ROOT/docs"/* "$TMP_ROOT/docs/"
cp "$ROOT/scripts/check-security-provenance.py" "$TMP_ROOT/scripts/"
cp "$ROOT/scripts/check-threat-model-links.sh" "$TMP_ROOT/scripts/"

# 2. Mutating open_p0 to 1 must fail
sed -i.bak 's/"open_p0": 0/"open_p0": 1/' "$TMP_ROOT/docs/security-assessment.json"
if python3 "$TMP_ROOT/scripts/check-security-provenance.py" "$TMP_ROOT" >/dev/null 2>&1; then
  fail "check accepted open_p0 = 1"
fi
mv "$TMP_ROOT/docs/security-assessment.json.bak" "$TMP_ROOT/docs/security-assessment.json"

# 3. Altering report content without updating digest must fail
echo "Extra malicious finding" >> "$TMP_ROOT/docs/security-report.md"
if python3 "$TMP_ROOT/scripts/check-security-provenance.py" "$TMP_ROOT" >/dev/null 2>&1; then
  fail "check accepted modified report digest"
fi

# 4. Removing security report must fail
rm "$TMP_ROOT/docs/security-report.md"
if python3 "$TMP_ROOT/scripts/check-security-provenance.py" "$TMP_ROOT" >/dev/null 2>&1; then
  fail "check accepted missing security report"
fi

echo "OK: check-security-provenance mutation self-tests passed."

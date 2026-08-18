#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
CHECK_SCRIPT="$ROOT/scripts/check-release-governance.sh"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "=== Running check-release-governance tests ==="

# Test 1: Real repository check passes
echo "Test 1: test_real_repo_governance_passes"
bash "$CHECK_SCRIPT" "$ROOT" >/dev/null
echo "  -> OK"

# Set up clean fixture tree in TMPDIR
mkdir -p "$TMPDIR/.github" "$TMPDIR/docs"
cp "$ROOT/.github/CODEOWNERS" "$TMPDIR/.github/CODEOWNERS"
cp "$ROOT/docs/release-governance.md" "$TMPDIR/docs/release-governance.md"

# Test 2: Branch protection policy enforced
echo "Test 2: test_branch_protection_policy_enforced"
grep -v -i "branch protection" "$ROOT/docs/release-governance.md" > "$TMPDIR/docs/release-governance.md"
if bash "$CHECK_SCRIPT" "$TMPDIR" >/dev/null 2>&1; then
    echo "FAILED: Expected failure when branch protection is missing" >&2
    exit 1
fi
cp "$ROOT/docs/release-governance.md" "$TMPDIR/docs/release-governance.md"
echo "  -> OK"

# Test 3: Signed release tag policy
echo "Test 3: test_signed_release_tag_policy"
grep -v -i "signed release tag" "$ROOT/docs/release-governance.md" > "$TMPDIR/docs/release-governance.md"
if bash "$CHECK_SCRIPT" "$TMPDIR" >/dev/null 2>&1; then
    echo "FAILED: Expected failure when signed release tag policy is missing" >&2
    exit 1
fi
cp "$ROOT/docs/release-governance.md" "$TMPDIR/docs/release-governance.md"
echo "  -> OK"

# Test 4: CODEOWNERS covers release workflows
echo "Test 4: test_codeowners_covers_release_workflows"
grep -v -i "workflows" "$ROOT/.github/CODEOWNERS" > "$TMPDIR/.github/CODEOWNERS"
if bash "$CHECK_SCRIPT" "$TMPDIR" >/dev/null 2>&1; then
    echo "FAILED: Expected failure when workflows rule missing" >&2
    exit 1
fi
cp "$ROOT/.github/CODEOWNERS" "$TMPDIR/.github/CODEOWNERS"
echo "  -> OK"

# Test 5: CODEOWNERS covers formal specs
echo "Test 5: test_codeowners_covers_formal_specs"
grep -v -i "formal" "$ROOT/.github/CODEOWNERS" > "$TMPDIR/.github/CODEOWNERS"
if bash "$CHECK_SCRIPT" "$TMPDIR" >/dev/null 2>&1; then
    echo "FAILED: Expected failure when formal rule missing" >&2
    exit 1
fi
cp "$ROOT/.github/CODEOWNERS" "$TMPDIR/.github/CODEOWNERS"
echo "  -> OK"

# Test 6: CODEOWNERS covers security policy
echo "Test 6: test_codeowners_covers_security_policy"
grep -v -i "security\.md" "$ROOT/.github/CODEOWNERS" > "$TMPDIR/.github/CODEOWNERS"
if bash "$CHECK_SCRIPT" "$TMPDIR" >/dev/null 2>&1; then
    echo "FAILED: Expected failure when security rule missing" >&2
    exit 1
fi
cp "$ROOT/.github/CODEOWNERS" "$TMPDIR/.github/CODEOWNERS"
echo "  -> OK"

# Test 7: CODEOWNERS covers capability contracts
echo "Test 7: test_codeowners_covers_capability_contracts"
grep -v -i "capabilities\.toml" "$ROOT/.github/CODEOWNERS" > "$TMPDIR/.github/CODEOWNERS"
if bash "$CHECK_SCRIPT" "$TMPDIR" >/dev/null 2>&1; then
    echo "FAILED: Expected failure when capabilities.toml rule missing" >&2
    exit 1
fi
cp "$ROOT/.github/CODEOWNERS" "$TMPDIR/.github/CODEOWNERS"
echo "  -> OK"

echo "All 7 check-release-governance tests passed successfully."

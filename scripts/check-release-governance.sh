#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"

ERRORS=0

function fail() {
    echo "ERROR: $1" >&2
    ERRORS=$((ERRORS + 1))
}

# 1. Check CODEOWNERS file
CODEOWNERS="$ROOT/.github/CODEOWNERS"
if [[ ! -f "$CODEOWNERS" ]]; then
    fail ".github/CODEOWNERS does not exist at $CODEOWNERS"
else
    # Verify required ownership rules
    declare -a REQUIRED_RULES=(
        "/.github/workflows/ @rockstream-maintainers"
        "/.github/prompts/ @rockstream-maintainers"
        "/formal/ @rockstream-formal-methods"
        "/FIZZBEE_TEST_PLAN.md @rockstream-formal-methods"
        "/SECURITY.md @rockstream-security"
        "/SECURITY_REVIEW_COMMISSION.md @rockstream-security"
        "/docs/threat-model.md @rockstream-security"
        "/capabilities.toml @rockstream-maintainers"
        "/docs/language-features.md @rockstream-maintainers"
        "/NEW_ROADMAP.md @rockstream-maintainers"
    )

    for rule in "${REQUIRED_RULES[@]}"; do
        pattern="${rule%% *}"
        owner="${rule##* }"
        if ! grep -E "^[[:space:]]*${pattern//\//\\/}[[:space:]]+.*${owner}" "$CODEOWNERS" >/dev/null 2>&1; then
            fail "CODEOWNERS is missing rule for '$pattern' with owner '$owner'"
        fi
    done
fi

# 2. Check release governance documentation
GOV_DOC="$ROOT/docs/release-governance.md"
if [[ ! -f "$GOV_DOC" ]]; then
    fail "docs/release-governance.md does not exist at $GOV_DOC"
else
    if ! grep -qi "Branch Protection" "$GOV_DOC"; then
        fail "docs/release-governance.md does not specify Branch Protection policy"
    fi
    if ! grep -qi "force push" "$GOV_DOC"; then
        fail "docs/release-governance.md does not prohibit force pushes"
    fi
    if ! grep -qi "Signed Release Tag" "$GOV_DOC"; then
        fail "docs/release-governance.md does not specify Signed Release Tag policy"
    fi
    if ! grep -qi "CODEOWNERS" "$GOV_DOC"; then
        fail "docs/release-governance.md does not specify CODEOWNERS policy"
    fi
fi

if [[ $ERRORS -gt 0 ]]; then
    echo "Release governance check failed with $ERRORS errors." >&2
    exit 1
fi

echo "OK: Release governance policies and CODEOWNERS validated successfully."
exit 0

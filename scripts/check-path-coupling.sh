#!/usr/bin/env bash
# check-path-coupling.sh — DC.2 path-coupling gate (v0.22).
#
# Any change to a coordination crate or DESIGN.md must be accompanied by a
# corresponding touch to formal/*.fizz or FIZZBEE_TEST_PLAN.md in the same
# commit/PR. This prevents coordination-protocol changes from silently
# drifting away from their formal models.
#
# Called by CI on every push and PR. Also callable locally.
#
# Usage:
#   ./scripts/check-path-coupling.sh            # uses HEAD~1..HEAD
#   BASE=main ./scripts/check-path-coupling.sh  # compares feature branch to main
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"

# Determine the diff range.
if [ -n "${GITHUB_BASE_REF:-}" ]; then
    # Running in a GitHub Actions PR — compare PR head against the base branch.
    BASE_SHA=$(git merge-base "origin/${GITHUB_BASE_REF}" HEAD)
    RANGE="${BASE_SHA}..HEAD"
elif [ -n "${BASE:-}" ]; then
    RANGE="${BASE}..HEAD"
else
    RANGE="HEAD~1..HEAD"
fi

changed_files=$(git diff --name-only "$RANGE" 2>/dev/null || true)

if [ -z "$changed_files" ]; then
    echo "check-path-coupling: no changed files found for range '$RANGE' — skipping."
    exit 0
fi

# Benchmark baselines calibrate measurements; they do not change coordination.
changed_files=$(echo "$changed_files" | grep -Ev '^crates/rockstream-runtime/benches/baseline/' || true)

if [ -z "$changed_files" ]; then
    echo "check-path-coupling: only benchmark baselines changed — OK."
    exit 0
fi

# Coordination crates and design doc — changes here require a model touch.
COORDINATION_PATTERNS=(
    "crates/rockstream-runtime/"
    "crates/rockstream-control/"
    "crates/rockstream-connectors/"
    "crates/rockstream-storage/"
    "DESIGN.md"
)

# Model files — at least one must be touched if coordination files changed.
MODEL_PATTERNS=(
    "formal/"
    "FIZZBEE_TEST_PLAN.md"
)

coordination_changed=0
for pattern in "${COORDINATION_PATTERNS[@]}"; do
    if echo "$changed_files" | grep -q "^${pattern}"; then
        coordination_changed=1
        break
    fi
done

if [ "$coordination_changed" -eq 0 ]; then
    echo "check-path-coupling: no coordination-crate or DESIGN.md changes — OK."
    exit 0
fi

# Coordination files changed — verify that a model file was also touched.
model_touched=0
for pattern in "${MODEL_PATTERNS[@]}"; do
    if echo "$changed_files" | grep -q "^${pattern}"; then
        model_touched=1
        break
    fi
done

if [ "$model_touched" -eq 0 ]; then
    echo ""
    echo "PATH-COUPLING VIOLATION: coordination-crate or DESIGN.md changed without a"
    echo "corresponding touch to formal/*.fizz or FIZZBEE_TEST_PLAN.md."
    echo ""
    echo "Changed coordination files:"
    echo "$changed_files" | grep -E "^(crates/rockstream-(runtime|control|connectors|storage)/|DESIGN\.md)" || true
    echo ""
    echo "Fix: update the relevant FizzBee model and/or FIZZBEE_TEST_PLAN.md §3.6"
    echo "to reflect the protocol change before merging."
    echo ""
    echo "See FIZZBEE_TEST_PLAN.md §4.4 DC.2 for the full policy."
    exit 1
fi

echo "check-path-coupling: coordination change accompanied by model touch — OK."

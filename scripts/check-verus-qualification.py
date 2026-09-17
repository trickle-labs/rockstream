#!/usr/bin/env python3
"""Check the reproducible VS6 qualification contract."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path


EVIDENCE_FIELDS = (
    "theorem_evidence",
    "model_evidence",
    "test_evidence",
    "review_evidence",
    "qualification_evidence",
    "verifier_output",
)
CLAIM_ID = re.compile(r"^VS[0-5]-[0-9]{2}$")


def load(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read {path}: {error}") from error


def add(errors: list[str], message: str) -> None:
    errors.append(message)


def check_files(root: Path, data: dict, errors: list[str]) -> None:
    for key in ("plan", "manifest", "sign_off", "maintenance", "runner", "recorder"):
        relative = data.get(key)
        if not isinstance(relative, str) or not relative:
            add(errors, f"qualification.{key} must name a file")
        elif not (root / relative).is_file():
            add(errors, f"qualification.{key} is missing: {relative}")

    for relative in data.get("artifact_digest_paths", []):
        if not isinstance(relative, str) or not (root / relative).is_file():
            add(errors, f"artifact digest path is missing: {relative}")

    budgets = data.get("budgets")
    if not isinstance(budgets, dict):
        return
    for relative in (budgets.get("r1_profile"), budgets.get("r1_thresholds")):
        if not isinstance(relative, str) or not (root / relative).is_file():
            add(errors, f"frozen R1 input is missing: {relative}")


def manifest_claims(root: Path, errors: list[str]) -> dict[str, str]:
    try:
        manifest = load(root / "formal/verus/manifest.toml")
    except ValueError as error:
        add(errors, str(error))
        return {}
    claims = manifest.get("claims")
    if not isinstance(claims, list):
        add(errors, "manifest.claims must be an array")
        return {}
    result: dict[str, str] = {}
    for claim in claims:
        claim_id = claim.get("id") if isinstance(claim, dict) else None
        if not isinstance(claim_id, str) or not CLAIM_ID.fullmatch(claim_id):
            add(errors, f"manifest has invalid claim id: {claim_id}")
        elif claim_id in result:
            add(errors, f"manifest has duplicate claim id: {claim_id}")
        else:
            result[claim_id] = claim.get("status", "")
    return result


def check_claim_evidence(root: Path, data: dict, errors: list[str], claims: dict[str, str]) -> None:
    records = data.get("claim_evidence")
    if not isinstance(records, list):
        add(errors, "claim_evidence must be an array")
        return
    seen: set[str] = set()
    for record in records:
        claim_id = record.get("id") if isinstance(record, dict) else None
        if not isinstance(claim_id, str):
            add(errors, "every claim evidence record needs an id")
            continue
        if claim_id in seen:
            add(errors, f"duplicate claim evidence: {claim_id}")
        seen.add(claim_id)
        if claim_id not in claims:
            add(errors, f"claim evidence references unknown claim: {claim_id}")
        for field in EVIDENCE_FIELDS:
            value = record.get(field) if isinstance(record, dict) else None
            if not isinstance(value, str) or not value.strip():
                add(errors, f"{claim_id} has unclassified {field}")
            elif value.lower().startswith("skipped"):
                add(errors, f"{claim_id} marks {field} as skipped; use blocked")
        qualification = record.get("qualification_evidence", "")
        if claims.get(claim_id) == "release-qualified" and qualification.lower().startswith("blocked"):
            add(errors, f"release-qualified claim {claim_id} has blocked qualification evidence")
    missing = set(claims) - seen
    for claim_id in sorted(missing):
        add(errors, f"claim has no VS6 evidence record: {claim_id}")


def check_builds(data: dict, errors: list[str]) -> None:
    builds = data.get("builds")
    if not isinstance(builds, list):
        add(errors, "builds must be an array")
        return
    seen: set[str] = set()
    for build in builds:
        build_id = build.get("id") if isinstance(build, dict) else None
        if not isinstance(build_id, str) or not build_id:
            add(errors, "every build needs an id")
            continue
        if build_id in seen:
            add(errors, f"duplicate build: {build_id}")
        seen.add(build_id)
        for field in ("kind", "toolchain", "profile", "target", "command", "artifacts"):
            if not build.get(field):
                add(errors, f"{build_id} lacks {field}")
        if build.get("required") is not True:
            add(errors, f"{build_id} is not required")
        if not isinstance(build.get("features"), list):
            add(errors, f"{build_id} lacks a feature matrix entry")
    required_specs = (
        ("production", "debug", "x86_64-unknown-linux-gnu", "default"),
        ("production", "release", "x86_64-unknown-linux-gnu", "default"),
        ("production", "debug", "x86_64-unknown-linux-gnu", "all-features"),
        ("production", "release", "aarch64-apple-darwin", "default"),
        ("verifier", "verification", "x86_64-unknown-linux-gnu", "default"),
        ("verifier", "verification", "aarch64-apple-darwin", "default"),
    )
    for kind, profile, target, feature in required_specs:
        if not any(
            build.get("kind") == kind
            and build.get("profile") == profile
            and build.get("target") == target
            and build.get("features") == [feature]
            for build in builds
        ):
            add(errors, f"required build is missing: {kind}/{profile}/{target}/{feature}")


def check_budgets(root: Path, data: dict, errors: list[str]) -> None:
    budgets = data.get("budgets")
    if not isinstance(budgets, dict):
        add(errors, "budgets must be a table")
        return
    for field in (
        "cold_verification_seconds",
        "incremental_verification_seconds",
        "production_compile_seconds",
        "runtime_regression_percent",
        "rss_regression_percent",
        "proof_maintenance_hours",
    ):
        if not isinstance(budgets.get(field), (int, float)) or budgets[field] <= 0:
            add(errors, f"budget {field} must be positive")
    if not isinstance(budgets.get("solver_resource_settings"), str) or not budgets["solver_resource_settings"].strip():
        add(errors, "solver resource settings must be recorded")
    if budgets.get("tolerances_frozen") is not True:
        add(errors, "VS6 runtime tolerances must be frozen")


def check_mutations(root: Path, data: dict, errors: list[str]) -> None:
    mutations = data.get("mutations")
    if not isinstance(mutations, list):
        add(errors, "mutations must be an array")
        return
    seen: set[str] = set()
    for mutation in mutations:
        mutation_id = mutation.get("id") if isinstance(mutation, dict) else None
        fixture = mutation.get("fixture") if isinstance(mutation, dict) else None
        target = mutation.get("target") if isinstance(mutation, dict) else None
        target_path = target.split("::", 1)[0] if isinstance(target, str) else None
        if not isinstance(mutation_id, str) or not mutation_id:
            add(errors, "every mutation needs an id")
            continue
        if mutation_id in seen:
            add(errors, f"duplicate mutation: {mutation_id}")
        seen.add(mutation_id)
        if not isinstance(fixture, str) or not (root / fixture).is_file():
            add(errors, f"{mutation_id} fixture is missing: {fixture}")
        if not isinstance(target, str) or not target_path or not (root / target_path).is_file():
            add(errors, f"{mutation_id} target is missing: {target}")
        if mutation.get("required") is not True:
            add(errors, f"{mutation_id} is not required")
        if not mutation.get("kind") or not mutation.get("expected"):
            add(errors, f"{mutation_id} lacks kind or expected failure")
    mutation_policy = data.get("mutation_policy")
    required_kinds = set(mutation_policy.get("required_kinds", [])) if isinstance(mutation_policy, dict) else set()
    if not required_kinds:
        add(errors, "mutation_policy.required_kinds must not be empty")
    actual_kinds = {
        mutation.get("kind")
        for mutation in mutations
        if isinstance(mutation, dict) and mutation.get("required") is True
    }
    for kind in sorted(required_kinds - actual_kinds):
        add(errors, f"required mutation kind is missing: {kind}")


def check_maintenance(root: Path, data: dict, errors: list[str]) -> None:
    maintenance = (root / data["maintenance"]).read_text(encoding="utf-8")
    for term in ("verifier", "solver", "support-library", "assumption", "upgrade", "blocked", "R1"):
        if term.lower() not in maintenance.lower():
            add(errors, f"maintenance policy omits {term}")

    sign_off = (root / data["sign_off"]).read_text(encoding="utf-8")
    for task in range(1, 7):
        if f"- [x] VS6-0{task}" not in sign_off:
            add(errors, f"VS6 sign-off does not complete VS6-0{task}")
    ownership = data.get("ownership")
    if not isinstance(ownership, dict) or not ownership.get("release") or not ownership.get("specification"):
        add(errors, "qualification ownership must name release and specification roles")
    supported_backends = data.get("supported_backends")
    backends = supported_backends.get("required", []) if isinstance(supported_backends, dict) else []
    if not isinstance(backends, list) or not {"local", "LFS", "MinIO"}.issubset(backends):
        add(errors, "qualification must list local, LFS, and MinIO backend evidence")

    makefile = (root / "Makefile").read_text(encoding="utf-8")
    workflow = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    if "check-verus-qualification.py" not in makefile:
        add(errors, "Makefile does not run the VS6 qualification checker")
    if "verus-verify:" not in workflow or "verify-proof-contracts" not in workflow:
        add(errors, "CI does not keep the Verus qualification gate wired")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    root = args.root.resolve()
    errors: list[str] = []
    try:
        data = load(root / "formal/verus/qualification.toml")
    except ValueError as error:
        print(f"verus qualification: RS-0906: {error}")
        return 1

    if data.get("schema") != 1:
        add(errors, "qualification schema must be 1")
    check_files(root, data, errors)
    claims = manifest_claims(root, errors)
    check_claim_evidence(root, data, errors, claims)
    check_builds(data, errors)
    check_budgets(root, data, errors)
    check_mutations(root, data, errors)
    if not errors:
        check_maintenance(root, data, errors)
    if errors:
        for error in errors:
            print(f"verus qualification: RS-0906: {error}")
        return 1
    print(
        f"verus qualification: {len(claims)} claims classified; "
        f"{len(data['builds'])} builds and {len(data['mutations'])} mutation controls registered."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

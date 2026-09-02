#!/usr/bin/env python3
"""Validate the v0.57.1 capability source, documentation, and generated matrix."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

TIERS = ("Core", "Maintain", "Experimental")
BEHAVIORS = (
    "incremental",
    "backfill",
    "checkpoint_recovery",
    "state_growth",
    "failure",
)
EXPECTED_EXTERNAL_SURFACE = {
    "connector.kafka-source",
    "connector.postgres-cdc",
    "sink.kafka",
}
PROMISE_FILES = ("README.md", "DESIGN.md", "NEW_IMPLEMENTATION_PLAN.md")
LANGUAGE_TIER_RE = re.compile(
    r"^- \*\*Tier: (?P<tier>Core|Maintain|Experimental)\*\* —", re.MULTILINE
)
CONNECTOR_ROW_RE = re.compile(
    r"^\| (?P<name>[^|]+) \| \*\*(?P<tier>Core|Maintain|Experimental)\*\* \|",
    re.MULTILINE,
)


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def load_source(root: Path, errors: list[str]) -> tuple[dict, list[dict]]:
    source = root / "capabilities.toml"
    try:
        data = tomllib.loads(source.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(errors, f"cannot parse capabilities.toml: {error}")
        return {}, []
    contract = data.get("contract")
    capabilities = data.get("capability")
    if not isinstance(contract, dict) or not isinstance(capabilities, list):
        fail(errors, "capabilities.toml must define [contract] and capability records")
        return {}, []
    return contract, capabilities


def check_documented_tiers(
    root: Path, capabilities: list[dict], errors: list[str]
) -> None:
    language_doc = root / "docs/language-features.md"
    connector_doc = root / "docs/connectors.md"
    try:
        language_text = language_doc.read_text(encoding="utf-8")
        connector_text = connector_doc.read_text(encoding="utf-8")
    except OSError as error:
        fail(errors, f"cannot read contract documentation: {error}")
        return

    language = [item for item in capabilities if item.get("kind") == "language"]
    try:
        language_inventory = language_text.split("## Implemented Today", 1)[1]
    except IndexError:
        fail(errors, "language documentation is missing the Implemented Today inventory")
        language_inventory = ""
    language_inventory = "\n".join(
        line for line in language_inventory.splitlines() if "(historical note)" not in line
    )
    language_tiers = LANGUAGE_TIER_RE.findall(language_inventory)
    if len(language_tiers) != len(language):
        fail(
            errors,
            f"language documentation has {len(language_tiers)} tiered entries; "
            f"capabilities.toml has {len(language)}",
        )
    for tier in TIERS:
        expected = sum(item.get("tier") == tier for item in language)
        actual = language_tiers.count(tier)
        if expected != actual:
            fail(
                errors,
                f"language documentation tier count for {tier} is {actual}; "
                f"capabilities.toml has {expected}",
            )

    core_lines = re.findall(
        r"^- \*\*Tier: Core\*\* —.*$", language_inventory, re.MULTILINE
    )
    if any("Proof:" not in line for line in core_lines):
        fail(errors, "every Core language entry must name its proof test")

    connector_rows = {
        match.group("name"): match.group("tier")
        for match in CONNECTOR_ROW_RE.finditer(connector_text)
    }
    expected_rows = {
        "PostgreSQL CDC source": "Core",
        "Kafka source": "Core",
        "Kafka sink": "Core",
    }
    if connector_rows != expected_rows:
        fail(errors, "connector documentation must list exactly the three Core surfaces")
    if "RS-4017" not in connector_text or "connector-migration.md" not in connector_text:
        fail(errors, "connector documentation must retain the RS-4017 migration reference")
    if not (root / "docs/connector-migration.md").is_file():
        fail(errors, "docs/connector-migration.md is missing")

    maintain_count = sum(item.get("tier") == "Maintain" for item in capabilities)
    if maintain_count and not re.search(
        r"Maintain compatibility and deprecation policy", language_text
    ):
        fail(errors, "Maintain capabilities require a compatibility and deprecation policy")
    if maintain_count and "ROCKSTREAM_PROJECT_FOCUS.md" not in language_text:
        fail(errors, "Maintain policy must reference the roadmap admission rule")


def check_external_surface(capabilities: list[dict], errors: list[str]) -> None:
    actual = {
        item.get("id")
        for item in capabilities
        if item.get("kind") in {"connector", "sink"}
    }
    if actual != EXPECTED_EXTERNAL_SURFACE:
        unknown = sorted(actual - EXPECTED_EXTERNAL_SURFACE)
        missing = sorted(EXPECTED_EXTERNAL_SURFACE - actual)
        details = []
        if unknown:
            details.append(f"unknown {', '.join(unknown)}")
        if missing:
            details.append(f"missing {', '.join(missing)}")
        fail(errors, f"connector/sink inventory mismatch ({'; '.join(details)})")
    for item in capabilities:
        if item.get("kind") in {"connector", "sink"} and item.get("tier") != "Core":
            fail(errors, f"{item.get('id')} must be Core")


def check_proof_levels(capabilities: list[dict], errors: list[str]) -> None:
    valid_levels = {"L0", "L1", "L2", "L3", "L4", "L5"}
    for item in capabilities:
        if item.get("tier") != "Core":
            continue
        cap_id = item.get("id")
        achieved = item.get("proof_levels_achieved")
        minimum = item.get("min_proof_level")
        if not isinstance(achieved, list) or not achieved:
            fail(errors, f"{cap_id} is missing proof_levels_achieved")
            continue
        if not isinstance(minimum, list) or not minimum:
            fail(errors, f"{cap_id} is missing min_proof_level")
            continue
        if not set(achieved) <= valid_levels or not set(minimum) <= valid_levels:
            fail(errors, f"{cap_id} has an invalid proof level (must be one of {sorted(valid_levels)})")
            continue
        missing = set(minimum) - set(achieved)
        if missing:
            fail(errors, f"{cap_id} is missing required proof level(s): {sorted(missing)}")


def check_promises(root: Path, promise: object, errors: list[str]) -> None:
    if not isinstance(promise, str) or not promise:
        fail(errors, "contract.promise must not be empty")
        return
    for relative in PROMISE_FILES:
        try:
            text = (root / relative).read_text(encoding="utf-8")
        except OSError as error:
            fail(errors, f"cannot read {relative}: {error}")
            continue
        if text.count(promise) != 1:
            fail(errors, f"{relative} does not contain the exact product promise once")


def check_roadmap(root: Path, contract: dict, errors: list[str]) -> None:
    version = contract.get("version")
    roadmap = contract.get("roadmap")
    if not isinstance(version, str) or not version:
        return
    if not isinstance(roadmap, str) or not roadmap:
        fail(errors, "contract.roadmap must name the roadmap file")
        return
    try:
        text = (root / roadmap).read_text(encoding="utf-8")
    except OSError as error:
        fail(errors, f"cannot read {roadmap}: {error}")
        return
    if not re.search(rf"^\| {re.escape(version)} \|.*✅ Done", text, re.MULTILINE):
        fail(errors, f"roadmap does not mark contract version {version} as Done")


def check_generated_matrix(root: Path, errors: list[str]) -> None:
    generator = root / "scripts/generate-capability-matrix.py"
    matrix = root / "docs/capability-matrix.md"
    if not generator.is_file() or not matrix.is_file():
        fail(errors, "capability matrix generator or output is missing")
        return
    with tempfile.NamedTemporaryFile(
        dir=root, prefix=".capability-matrix.", suffix=".md", delete=False
    ) as stream:
        generated = Path(stream.name)
    try:
        result = subprocess.run(
            [
                sys.executable,
                str(generator),
                "--root",
                str(root),
                "--output",
                str(generated),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            if result.stderr:
                for line in result.stderr.rstrip().splitlines():
                    fail(errors, line)
            else:
                fail(errors, "capability matrix generation failed")
            return
        if generated.read_bytes() != matrix.read_bytes():
            fail(errors, "docs/capability-matrix.md is not byte-identical to generated output")
    finally:
        generated.unlink(missing_ok=True)


def proof_exists(root: Path, proof: object, label: str, errors: list[str]) -> None:
    if not isinstance(proof, str) or not proof:
        fail(errors, f"{label} has an empty proof")
        return
    match = re.fullmatch(r"(?P<path>[^:]+)::(?P<test>[A-Za-z_][A-Za-z0-9_]*)", proof)
    if not match:
        fail(errors, f"{label} has an invalid proof target: {proof!r}")
        return
    path = root / match["path"]
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        fail(errors, f"{label} proof target does not resolve: {proof}")
        return
    if not re.search(
        rf"\b(?:async\s+)?fn\s+{re.escape(match['test'])}\s*\(", text
    ) and not re.search(rf"\b{re.escape(match['test'])}\b", text):
        fail(errors, f"{label} proof target does not resolve: {proof}")


def generated_block(text: str) -> str | None:
    match = re.search(
        r"<!-- BEGIN GENERATED CORE SEMANTICS -->.*?"
        r"<!-- END GENERATED CORE SEMANTICS -->",
        text,
        re.DOTALL,
    )
    return match.group(0) if match else None


def check_full_semantics(
    root: Path, contract: dict, capabilities: list[dict], errors: list[str]
) -> None:
    if contract.get("version") not in {"v0.57.1", "v0.59.1", "v0.59.3", "v0.59.4", "v0.59.6", "v0.59.7", "v0.59.18", "v0.59.20", "v0.59.22"}:
        fail(errors, "capabilities.toml contract.version must be v0.57.1, v0.59.1, v0.59.3, v0.59.4, v0.59.6, v0.59.7, v0.59.18, v0.59.20, or v0.59.22")


    for capability in capabilities:
        if capability.get("tier") != "Core":
            continue
        rows = capability.get("behavior")
        if not isinstance(rows, list):
            fail(errors, f"{capability.get('id')} is missing Core semantic behavior rows")
            continue
        by_name: dict[object, dict] = {}
        for row in rows:
            if not isinstance(row, dict):
                fail(errors, f"{capability.get('id')} has an invalid semantic behavior row")
                continue
            behavior = row.get("behavior")
            if behavior not in BEHAVIORS:
                fail(errors, f"{capability.get('id')} has unknown behavior {behavior}")
            if behavior in by_name:
                fail(errors, f"{capability.get('id')} has duplicate behavior {behavior}")
            by_name[behavior] = row
        for behavior in BEHAVIORS:
            row = by_name.get(behavior)
            label = f"{capability.get('id')} {behavior}"
            if row is None:
                fail(errors, f"{capability.get('id')} is missing Core semantic behavior {behavior}")
                continue
            statement = row.get("statement")
            if not isinstance(statement, str) or not statement.strip():
                fail(errors, f"{label} has an empty statement")
            proof_exists(root, row.get("proof"), label, errors)
            paired_proof = row.get("paired_proof")
            if paired_proof is not None:
                proof_exists(root, paired_proof, f"{label} paired proof", errors)
            if behavior == "state_growth":
                for field in ("bound", "metric", "on_bound"):
                    if not isinstance(row.get(field), str) or not row[field].strip():
                        fail(errors, f"{label} is missing {field}")

    decisions = contract.get("tier_decision", [])
    if not isinstance(decisions, list) or not decisions:
        fail(errors, "full semantics mode requires a tier decision")
    capability_ids = {item.get("id") for item in capabilities}
    for decision in decisions:
        if not isinstance(decision, dict):
            fail(errors, "tier decision must be a table")
            continue
        for field in ("capability", "old_tier", "new_tier", "reason", "evidence"):
            if not isinstance(decision.get(field), str) or not decision[field].strip():
                fail(errors, f"tier decision is missing {field}")
        if decision.get("old_tier") not in TIERS or decision.get("new_tier") not in TIERS:
            fail(errors, "tier decision has an invalid tier")
        if decision.get("capability") not in capability_ids:
            fail(errors, "tier decision references an unknown capability")
        if decision.get("old_tier") == decision.get("new_tier"):
            fail(errors, "tier decision must record a tier change")
        if isinstance(decision.get("evidence"), str) and decision["evidence"].strip():
            proof_exists(root, decision["evidence"], "tier decision evidence", errors)

    matrix_text = (root / "docs/capability-matrix.md").read_text(encoding="utf-8")
    language_text = (root / "docs/language-features.md").read_text(encoding="utf-8")
    matrix_block = generated_block(matrix_text)
    language_block = generated_block(language_text)
    if matrix_block is None:
        fail(errors, "capability matrix is missing the generated Core semantics block")
    if language_block is None:
        fail(errors, "language documentation is missing the generated Core semantics block")
    if matrix_block is not None and language_block is not None and matrix_block != language_block:
        fail(errors, "generated Core semantics blocks are not byte-identical")


def main() -> int:
    arguments = sys.argv[1:]
    roots = [argument for argument in arguments if argument != "--full-semantics"]
    root = Path(roots[0]).resolve() if roots else Path.cwd().resolve()
    errors: list[str] = []
    contract, capabilities = load_source(root, errors)
    if contract:
        if contract.get("version") not in {"v0.57", "v0.57.1", "v0.59.1", "v0.59.3", "v0.59.4", "v0.59.6", "v0.59.7", "v0.59.18", "v0.59.20", "v0.59.22"}:
            fail(errors, "capabilities.toml contract.version must be v0.57, v0.57.1, v0.59.1, v0.59.3, v0.59.4, v0.59.6, v0.59.7, v0.59.18, v0.59.20, or v0.59.22")

        check_promises(root, contract.get("promise"), errors)
        check_roadmap(root, contract, errors)
    check_external_surface(capabilities, errors)
    check_proof_levels(capabilities, errors)
    check_documented_tiers(root, capabilities, errors)
    check_generated_matrix(root, errors)
    if "--full-semantics" in arguments:
        check_full_semantics(root, contract, capabilities, errors)

    if errors:
        print("FAIL: capability contract check found violations.", file=sys.stderr)
        for error in errors:
            print(f"VIOLATION: {error}", file=sys.stderr)
        return 1
    print("OK: capability contract check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

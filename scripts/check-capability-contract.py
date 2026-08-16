#!/usr/bin/env python3
"""Validate the v0.57 capability source, documentation, and generated matrix."""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

TIERS = ("Core", "Maintain", "Experimental")
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


def main() -> int:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd().resolve()
    errors: list[str] = []
    contract, capabilities = load_source(root, errors)
    if contract:
        if contract.get("version") != "v0.57":
            fail(errors, "capabilities.toml contract.version must be v0.57")
        check_promises(root, contract.get("promise"), errors)
    check_external_surface(capabilities, errors)
    check_documented_tiers(root, capabilities, errors)
    check_generated_matrix(root, errors)

    if errors:
        print("FAIL: capability contract check found violations.", file=sys.stderr)
        for error in errors:
            print(f"VIOLATION: {error}", file=sys.stderr)
        return 1
    print("OK: capability contract check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

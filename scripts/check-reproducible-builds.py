#!/usr/bin/env python3
"""
check-reproducible-builds.py — Validates reproducible build instructions,
SBOM (SPDX 2.3 & CycloneDX 1.5), SLSA v1.0 provenance attestation,
vulnerability scan results, and release runbooks.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

REQUIRED_DOCS = [
    "docs/reproducible-builds.md",
    "docs/sbom.spdx.json",
    "docs/sbom.cyclonedx.json",
    "docs/vulnerability-results.json",
    "docs/provenance.slsa.json",
    "docs/release-notes-v1.0.md",
    "docs/known-limitations.md",
]


def fail(violations: list[str], msg: str) -> None:
    violations.append(f"VIOLATION: {msg}")


def check_reproducible_instructions(root: Path, violations: list[str]) -> None:
    doc = root / "docs" / "reproducible-builds.md"
    if not doc.is_file():
        fail(violations, "docs/reproducible-builds.md is missing")
        return

    content = doc.read_text(encoding="utf-8")
    for pattern in ["SOURCE_DATE_EPOCH", "--remap-path-prefix", "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"]:
        if pattern not in content:
            fail(violations, f"docs/reproducible-builds.md missing required instruction/flag: {pattern}")


def check_sbom_spdx(root: Path, violations: list[str]) -> None:
    spdx_path = root / "docs" / "sbom.spdx.json"
    if not spdx_path.is_file():
        fail(violations, "docs/sbom.spdx.json is missing")
        return

    try:
        data = json.loads(spdx_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        fail(violations, f"docs/sbom.spdx.json invalid JSON: {err}")
        return

    if data.get("spdxVersion") != "SPDX-2.3":
        fail(violations, f"docs/sbom.spdx.json spdxVersion must be 'SPDX-2.3', got {data.get('spdxVersion')}")

    packages = data.get("packages", [])
    if not packages or not isinstance(packages, list):
        fail(violations, "docs/sbom.spdx.json packages list is missing or empty")
    else:
        names = {pkg.get("name") for pkg in packages if isinstance(pkg, dict)}
        if "rockstream" not in names and "rockstream-core" not in names:
            fail(violations, "docs/sbom.spdx.json missing core package definitions")


def check_sbom_cyclonedx(root: Path, violations: list[str]) -> None:
    cdx_path = root / "docs" / "sbom.cyclonedx.json"
    if not cdx_path.is_file():
        fail(violations, "docs/sbom.cyclonedx.json is missing")
        return

    try:
        data = json.loads(cdx_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        fail(violations, f"docs/sbom.cyclonedx.json invalid JSON: {err}")
        return

    if data.get("bomFormat") != "CycloneDX":
        fail(violations, f"docs/sbom.cyclonedx.json bomFormat must be 'CycloneDX', got {data.get('bomFormat')}")

    if str(data.get("specVersion")) != "1.5":
        fail(violations, f"docs/sbom.cyclonedx.json specVersion must be '1.5', got {data.get('specVersion')}")

    components = data.get("components", [])
    if not components or not isinstance(components, list):
        fail(violations, "docs/sbom.cyclonedx.json components list is missing or empty")


def check_vulnerability_results(root: Path, violations: list[str]) -> None:
    vuln_path = root / "docs" / "vulnerability-results.json"
    if not vuln_path.is_file():
        fail(violations, "docs/vulnerability-results.json is missing")
        return

    try:
        data = json.loads(vuln_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        fail(violations, f"docs/vulnerability-results.json invalid JSON: {err}")
        return

    summary = data.get("summary", {})
    critical = summary.get("critical", summary.get("p0", -1))
    high = summary.get("high", summary.get("p1", -1))
    if critical != 0:
        fail(violations, f"docs/vulnerability-results.json critical/P0 vulnerabilities must be 0, found {critical}")
    if high != 0:
        fail(violations, f"docs/vulnerability-results.json high/P1 vulnerabilities must be 0, found {high}")


def check_slsa_provenance(root: Path, violations: list[str]) -> None:
    prov_path = root / "docs" / "provenance.slsa.json"
    if not prov_path.is_file():
        fail(violations, "docs/provenance.slsa.json is missing")
        return

    try:
        data = json.loads(prov_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        fail(violations, f"docs/provenance.slsa.json invalid JSON: {err}")
        return

    if data.get("_type") != "https://in-toto.io/Statement/v1":
        fail(violations, f"docs/provenance.slsa.json _type mismatch: {data.get('_type')}")

    if data.get("predicateType") != "https://slsa.dev/provenance/v1":
        fail(violations, f"docs/provenance.slsa.json predicateType mismatch: {data.get('predicateType')}")

    subject = data.get("subject", [])
    if not subject or not isinstance(subject, list):
        fail(violations, "docs/provenance.slsa.json subject list is empty")


def check_release_notes_and_limitations(root: Path, violations: list[str]) -> None:
    rn_path = root / "docs" / "release-notes-v1.0.md"
    if not rn_path.is_file():
        fail(violations, "docs/release-notes-v1.0.md is missing")

    kl_path = root / "docs" / "known-limitations.md"
    if not kl_path.is_file():
        fail(violations, "docs/known-limitations.md is missing")


def main() -> None:
    root_str = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    root = Path(root_str).resolve()
    violations: list[str] = []

    check_reproducible_instructions(root, violations)
    check_sbom_spdx(root, violations)
    check_sbom_cyclonedx(root, violations)
    check_vulnerability_results(root, violations)
    check_slsa_provenance(root, violations)
    check_release_notes_and_limitations(root, violations)

    if violations:
        print("FAIL: Reproducible build verification failed.", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        sys.exit(1)

    print("OK: Reproducible builds, SBOM, SLSA provenance, and release notes verified.")


if __name__ == "__main__":
    main()

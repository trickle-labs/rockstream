#!/usr/bin/env python3
"""
check-security-provenance.py — Validates security report, assessor identity,
assessment metadata, report digest integrity, and threat model links.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

REQUIRED_CRATES = [
    "rockstream-core",
    "rockstream-storage",
    "rockstream-ops",
    "rockstream-control",
    "rockstream-gateway",
    "rockstream-sim",
    "rockstream-cli",
]

EXPECTED_ASSESSOR = "RockStream Security Architecture & Assessment Group"


def fail(violations: list[str], msg: str) -> None:
    violations.append(f"VIOLATION: {msg}")


def compute_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def check_security_report(root: Path, violations: list[str]) -> str:
    report_path = root / "docs" / "security-report.md"
    if not report_path.is_file():
        fail(violations, "docs/security-report.md is missing")
        return ""

    content = report_path.read_text(encoding="utf-8")
    if EXPECTED_ASSESSOR not in content:
        fail(violations, f"docs/security-report.md must name assessor '{EXPECTED_ASSESSOR}'")

    for crate in REQUIRED_CRATES:
        if crate not in content:
            fail(violations, f"docs/security-report.md missing crate in scope: {crate}")

    if "2026-08-10" not in content or "2026-08-18" not in content:
        fail(violations, "docs/security-report.md must state audit dates (2026-08-10 to 2026-08-18)")

    if not re.search(r"Open P0 Vulnerabilities\*{0,2}:\s*0", content, re.IGNORECASE):
        fail(violations, "docs/security-report.md must state 0 open P0 vulnerabilities")

    if not re.search(r"Open P1 Vulnerabilities\*{0,2}:\s*0", content, re.IGNORECASE):
        fail(violations, "docs/security-report.md must state 0 open P1 vulnerabilities")


    return compute_sha256(report_path)


def check_security_assessment(root: Path, report_digest: str, violations: list[str]) -> None:
    assessment_path = root / "docs" / "security-assessment.json"
    if not assessment_path.is_file():
        fail(violations, "docs/security-assessment.json is missing")
        return

    try:
        data = json.loads(assessment_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as err:
        fail(violations, f"docs/security-assessment.json is invalid JSON: {err}")
        return

    assessor = data.get("assessor", {})
    if assessor.get("name") != EXPECTED_ASSESSOR:
        fail(violations, f"docs/security-assessment.json assessor name mismatch: {assessor.get('name')}")

    scope = data.get("scope", [])
    for crate in REQUIRED_CRATES:
        if crate not in scope:
            fail(violations, f"docs/security-assessment.json scope missing {crate}")

    dates = data.get("audit_dates", {})
    if dates.get("start") != "2026-08-10" or dates.get("end") != "2026-08-18":
        fail(violations, f"docs/security-assessment.json audit dates mismatch: {dates}")

    findings = data.get("findings", {})
    if findings.get("open_p0", -1) != 0:
        fail(violations, f"docs/security-assessment.json open_p0 must be 0, found {findings.get('open_p0')}")
    if findings.get("open_p1", -1) != 0:
        fail(violations, f"docs/security-assessment.json open_p1 must be 0, found {findings.get('open_p1')}")
    if findings.get("status") != "Closed":
        fail(violations, f"docs/security-assessment.json findings status must be 'Closed', found {findings.get('status')}")

    if report_digest:
        declared_digest = data.get("report_sha256", "")
        if declared_digest.lower() != report_digest.lower():
            fail(violations, f"docs/security-assessment.json report_sha256 ({declared_digest}) does not match docs/security-report.md ({report_digest})")


def check_security_doc_references(root: Path, violations: list[str]) -> None:
    sec_doc = root / "docs" / "security.md"
    if not sec_doc.is_file():
        fail(violations, "docs/security.md is missing")
        return

    content = sec_doc.read_text(encoding="utf-8")
    if "security-report.md" not in content:
        fail(violations, "docs/security.md must reference security-report.md")
    if "security-assessment.json" not in content:
        fail(violations, "docs/security.md must reference security-assessment.json")


def check_threat_model(root: Path, violations: list[str]) -> None:
    script = root / "scripts" / "check-threat-model-links.sh"
    if not script.is_file():
        fail(violations, "scripts/check-threat-model-links.sh is missing")
        return

    res = subprocess.run(["bash", str(script), str(root)], capture_output=True, text=True)
    if res.returncode != 0:
        fail(violations, f"scripts/check-threat-model-links.sh failed:\n{res.stderr or res.stdout}")


def main() -> None:
    root_str = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    root = Path(root_str).resolve()
    violations: list[str] = []

    report_digest = check_security_report(root, violations)
    check_security_assessment(root, report_digest, violations)
    check_security_doc_references(root, violations)
    check_threat_model(root, violations)

    if violations:
        print("FAIL: Security provenance check failed.", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        sys.exit(1)

    print("OK: Security provenance and assessment validation passed.")


if __name__ == "__main__":
    main()

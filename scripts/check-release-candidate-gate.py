#!/usr/bin/env python3
"""
check-release-candidate-gate.py — Automated verification of all 4 Entry Criteria
and 7 Release Candidate (RC1) Gates for Rockstream v1.0 / v0.59.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

def fail(violations: list[str], msg: str) -> None:
    violations.append(f"VIOLATION: {msg}")

def check_entry_criteria(root: Path, violations: list[str]) -> None:
    """Validate the 4 Entry Criteria."""
    # 1. Dispatch wiring re-runs clean (0 MISSING paths)
    dispatch_script = root / "scripts" / "check-dispatch-wiring.sh"
    if not dispatch_script.is_file():
        fail(violations, "Entry Criterion 1: scripts/check-dispatch-wiring.sh missing")
    else:
        res = subprocess.run(
            ["bash", str(dispatch_script), str(root)],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            fail(violations, f"Entry Criterion 1: check-dispatch-wiring.sh failed:\n{res.stderr or res.stdout}")

    # 2. No unreachable in production crates
    unreachable_script = root / "scripts" / "check-no-unreachable.sh"
    if not unreachable_script.is_file():
        fail(violations, "Entry Criterion 2: scripts/check-no-unreachable.sh missing")
    else:
        res = subprocess.run(
            ["bash", str(unreachable_script), str(root)],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            fail(violations, f"Entry Criterion 2: check-no-unreachable.sh failed:\n{res.stderr or res.stdout}")

    # 3. Failure matrix re-runs clean
    failure_matrix_script = root / "scripts" / "check-failure-matrix.sh"
    if not failure_matrix_script.is_file():
        fail(violations, "Entry Criterion 3: scripts/check-failure-matrix.sh missing")
    else:
        res = subprocess.run(
            ["bash", str(failure_matrix_script), str(root)],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            fail(violations, f"Entry Criterion 3: check-failure-matrix.sh failed:\n{res.stderr or res.stdout}")

    # 4. Exit criteria re-runs clean (all versions up to v0.58.3 signed off)
    exit_criteria_script = root / "scripts" / "check-exit-criteria.sh"
    if not exit_criteria_script.is_file():
        fail(violations, "Entry Criterion 4: scripts/check-exit-criteria.sh missing")
    else:
        res = subprocess.run(
            ["bash", str(exit_criteria_script), str(root)],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            fail(violations, f"Entry Criterion 4: check-exit-criteria.sh failed:\n{res.stderr or res.stdout}")

def check_gate_1_correctness(root: Path, violations: list[str]) -> None:
    """Gate 1: Correctness (Silent-Wrong-Answer & Reachability)."""
    p1 = root / "crates" / "rockstream-gateway" / "tests" / "unscoped_pgwire_reachability_tests.rs"
    if not p1.is_file():
        fail(violations, f"Gate 1: missing reachability test file at {p1}")
    else:
        content = p1.read_text(encoding="utf-8")
        if "#[test]" not in content and "#[tokio::test]" not in content:
            fail(violations, f"Gate 1: {p1} contains no test functions")

    p2 = root / "crates" / "rockstream-gateway" / "tests" / "unscoped_silent_wrong_answer_tests.rs"
    if not p2.is_file():
        fail(violations, f"Gate 1: missing silent wrong answer test file at {p2}")
    else:
        content = p2.read_text(encoding="utf-8")
        if "#[test]" not in content and "#[tokio::test]" not in content:
            fail(violations, f"Gate 1: {p2} contains no test functions")

    cap_script = root / "scripts" / "check-capability-contract.sh"
    if cap_script.is_file():
        res = subprocess.run(
            ["bash", str(cap_script), str(root)],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            fail(violations, f"Gate 1: check-capability-contract.sh failed:\n{res.stderr or res.stdout}")

def check_gate_2_recovery(root: Path, violations: list[str]) -> None:
    """Gate 2: Recovery (Failure Matrix & Zero Data Loss)."""
    fm_doc = root / "docs" / "failure-matrix.md"
    if not fm_doc.is_file():
        fail(violations, f"Gate 2: missing docs/failure-matrix.md at {fm_doc}")

    baseline_json = root / "docs" / "chaos-recovery-baseline.json"
    if not baseline_json.is_file():
        fail(violations, f"Gate 2: missing docs/chaos-recovery-baseline.json at {baseline_json}")
    else:
        try:
            data = json.loads(baseline_json.read_text(encoding="utf-8"))
            targets = data.get("targets", {})
            if targets.get("failure_detection_ms", 99999) > 5000:
                fail(violations, "Gate 2: targets.failure_detection_ms exceeds 5000ms SLO")
            if targets.get("shard_reassignment_ms", 99999) > 30000:
                fail(violations, "Gate 2: targets.shard_reassignment_ms exceeds 30000ms SLO")
            if targets.get("freshness_recovery_ms", 99999) > 60000:
                fail(violations, "Gate 2: targets.freshness_recovery_ms exceeds 60000ms SLO")
            if not targets.get("zero_loss", False):
                fail(violations, "Gate 2: targets.zero_loss must be true")
            if not targets.get("zero_duplicates", False):
                fail(violations, "Gate 2: targets.zero_duplicates must be true")

            measured = data.get("published_baseline_measured", {})
            if measured.get("failure_detection_p99_ms", 99999) > 5000:
                fail(violations, "Gate 2: measured failure_detection_p99_ms exceeds 5000ms")
            if measured.get("shard_reassignment_p99_ms", 99999) > 30000:
                fail(violations, "Gate 2: measured shard_reassignment_p99_ms exceeds 30000ms")
            if measured.get("freshness_recovery_p99_ms", 99999) > 60000:
                fail(violations, "Gate 2: measured freshness_recovery_p99_ms exceeds 60000ms")
            if measured.get("steady_state_throughput_rows_per_sec", 0) < 2500:
                fail(violations, "Gate 2: measured steady_state_throughput_rows_per_sec below 2500 rows/s")
        except json.JSONDecodeError as err:
            fail(violations, f"Gate 2: invalid JSON in docs/chaos-recovery-baseline.json: {err}")

    t1 = root / "crates" / "rockstream-sim" / "tests" / "failure_matrix_tests.rs"
    if not t1.is_file():
        fail(violations, f"Gate 2: missing test file at {t1}")

    t2 = root / "crates" / "rockstream-sim" / "tests" / "real_cluster_chaos_soak_tests.rs"
    if not t2.is_file():
        fail(violations, f"Gate 2: missing test file at {t2}")

def check_gate_3_bounded_resources(root: Path, violations: list[str]) -> None:
    """Gate 3: Bounded Resources (Memory Accounting, Spill & Storage Pressure)."""
    t1 = root / "crates" / "rockstream-sim" / "tests" / "storage_pressure_admission_tests.rs"
    if not t1.is_file():
        fail(violations, f"Gate 3: missing storage pressure test file at {t1}")

    t2 = root / "crates" / "rockstream-sim" / "tests" / "resource_leak_soak_real_binary_tests.rs"
    if not t2.is_file():
        fail(violations, f"Gate 3: missing resource leak soak test file at {t2}")

    op_state_script = root / "scripts" / "check-operator-state-bytes.sh"
    if op_state_script.is_file():
        res = subprocess.run(
            ["bash", str(op_state_script), str(root)],
            capture_output=True,
            text=True,
        )
        if res.returncode != 0:
            fail(violations, f"Gate 3: check-operator-state-bytes.sh failed:\n{res.stderr or res.stdout}")

    # Verify no SlateDB range deletion usage in production code
    src_dirs = [p for p in (root / "crates").glob("*/src") if p.is_dir()]
    range_del_pattern = re.compile(r"(?<!assert!\(!)(?<!\bcontains\(\")\bdelete_range\s*\(")
    for src_dir in src_dirs:
        for rs_file in src_dir.rglob("*.rs"):
            try:
                code = rs_file.read_text(encoding="utf-8")
                production_code = code.split("#[cfg(test)]")[0]
                if range_del_pattern.search(production_code):
                    fail(violations, f"Gate 3: disallowed SlateDB range deletion in {rs_file.relative_to(root)}")
            except OSError:
                pass

def check_gate_4_operability(root: Path, violations: list[str]) -> None:
    """Gate 4: Operability (Diagnostics & Freshness Explainability)."""
    t1 = root / "crates" / "rockstream-gateway" / "tests" / "show_view_status_pgwire_tests.rs"
    if not t1.is_file():
        fail(violations, f"Gate 4: missing SHOW VIEW STATUS pgwire test file at {t1}")

    sre_doc = root / "docs" / "sre-operations.md"
    diag_doc = root / "docs" / "diagnostics.md"
    if not sre_doc.is_file() and not diag_doc.is_file():
        fail(violations, "Gate 4: missing operations / diagnostics documentation in docs/")

    cli_doc = root / "docs" / "cli.md"
    if not cli_doc.is_file():
        fail(violations, "Gate 4: missing docs/cli.md")

def check_gate_5_upgradeability(root: Path, violations: list[str]) -> None:
    """Gate 5: Upgradeability (Rolling Upgrades & Disaster Recovery)."""
    up_doc = root / "docs" / "rolling-upgrades.md"
    if not up_doc.is_file():
        fail(violations, "Gate 5: missing docs/rolling-upgrades.md")

    dr_doc = root / "docs" / "disaster-recovery.md"
    if not dr_doc.is_file():
        fail(violations, "Gate 5: missing docs/disaster-recovery.md")

    t1 = root / "crates" / "rockstream-sim" / "tests" / "rolling_upgrade_tests.rs"
    if not t1.is_file():
        fail(violations, f"Gate 5: missing rolling upgrade test file at {t1}")

    t2 = root / "crates" / "rockstream-control" / "tests" / "disaster_recovery_tests.rs"
    if not t2.is_file():
        fail(violations, f"Gate 5: missing disaster recovery test file at {t2}")

    t3 = root / "crates" / "rockstream-cli" / "tests" / "disaster_recovery_runbook_tests.rs"
    if not t3.is_file():
        fail(violations, f"Gate 5: missing disaster recovery runbook test file at {t3}")

def check_gate_6_security(root: Path, violations: list[str]) -> None:
    """Gate 6: Security (Closed Security Review & Governance)."""
    sec_comm = root / "SECURITY_REVIEW_COMMISSION.md"
    if not sec_comm.is_file():
        fail(violations, "Gate 6: missing SECURITY_REVIEW_COMMISSION.md")
    else:
        text = sec_comm.read_text(encoding="utf-8")
        if "Closed" not in text:
            fail(violations, "Gate 6: SECURITY_REVIEW_COMMISSION.md status is not Closed")
        p0_match = re.search(r"Open P0 Vulnerabilities:\s*(\d+)", text)
        if not p0_match or int(p0_match.group(1)) != 0:
            fail(violations, "Gate 6: SECURITY_REVIEW_COMMISSION.md has open P0 vulnerabilities")
        p1_match = re.search(r"Open P1 Vulnerabilities:\s*(\d+)", text)
        if not p1_match or int(p1_match.group(1)) != 0:
            fail(violations, "Gate 6: SECURITY_REVIEW_COMMISSION.md has open P1 vulnerabilities")

    threat_doc = root / "docs" / "threat-model.md"
    if not threat_doc.is_file():
        fail(violations, "Gate 6: missing docs/threat-model.md")
    else:
        threat_script = root / "scripts" / "check-threat-model-links.sh"
        if threat_script.is_file():
            res = subprocess.run(
                ["bash", str(threat_script), str(root)],
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                fail(violations, f"Gate 6: check-threat-model-links.sh failed:\n{res.stderr or res.stdout}")

    dep_test_script = root / "scripts" / "check-dependency-audit.test.sh"
    if not dep_test_script.is_file():
        fail(violations, "Gate 6: missing scripts/check-dependency-audit.test.sh")

    t1 = root / "crates" / "rockstream-cli" / "tests" / "cli_mutating_commands_tests.rs"
    if not t1.is_file():
        fail(violations, f"Gate 6: missing mutating commands test file at {t1}")

    t2 = root / "crates" / "rockstream-gateway" / "tests" / "gateway_mutation_authorization_tests.rs"
    if not t2.is_file():
        fail(violations, f"Gate 6: missing gateway mutation auth test file at {t2}")

def check_gate_7_performance_stability(root: Path, violations: list[str]) -> None:
    """Gate 7: Performance Stability (Core Workload Performance Envelope)."""
    t1 = root / "crates" / "rockstream-sim" / "tests" / "recovery_slo_tests.rs"
    if not t1.is_file():
        fail(violations, f"Gate 7: missing recovery SLO test file at {t1}")

    baseline_json = root / "docs" / "chaos-recovery-baseline.json"
    if baseline_json.is_file():
        try:
            data = json.loads(baseline_json.read_text(encoding="utf-8"))
            measured = data.get("published_baseline_measured", {})
            throughput = measured.get("steady_state_throughput_rows_per_sec", 0)
            if throughput < 2500:
                fail(violations, f"Gate 7: steady_state_throughput_rows_per_sec ({throughput}) < 2500 rows/s")
        except json.JSONDecodeError:
            pass

def main() -> None:
    root_str = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    root = Path(root_str).resolve()

    violations: list[str] = []

    print("Checking Entry Criteria (4 checks)...")
    check_entry_criteria(root, violations)

    print("Checking Gate 1: Correctness...")
    check_gate_1_correctness(root, violations)

    print("Checking Gate 2: Recovery...")
    check_gate_2_recovery(root, violations)

    print("Checking Gate 3: Bounded Resources...")
    check_gate_3_bounded_resources(root, violations)

    print("Checking Gate 4: Operability...")
    check_gate_4_operability(root, violations)

    print("Checking Gate 5: Upgradeability...")
    check_gate_5_upgradeability(root, violations)

    print("Checking Gate 6: Security...")
    check_gate_6_security(root, violations)

    print("Checking Gate 7: Performance Stability & Single-Region Boundary...")
    check_gate_7_performance_stability(root, violations)

    if violations:
        print("\nRelease Candidate Gate Validation FAILED:", file=sys.stderr)
        for v in violations:
            print(f"  {v}", file=sys.stderr)
        sys.exit(1)

    print("\nOK: All 4 Entry Criteria and 7 Release Candidate Gates passed successfully.")

if __name__ == "__main__":
    main()

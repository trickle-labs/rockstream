#!/usr/bin/env python3
"""
check-failure-matrix.py — Validate that docs/failure-matrix.md is complete,
syntactically valid, links to existing SimRuntime tests, references permanent seeds,
and asserts non-vacuous recovery properties for all 11 production failure modes.
"""

import os
import re
import sys

REQUIRED_FAILURE_MODES = [
    f"FM-{i:03d}" for i in range(1, 12)
]

VACUOUS_PATTERNS = [
    r"did not crash",
    r"does not crash",
    r"no crash",
    r"passes without panic",
    r"no panic",
    r"does not panic",
    r"without panic",
    r"no error",
    r"runs fine",
    r"works fine",
]

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
    doc_path = os.path.join(root, "docs", "failure-matrix.md")

    if not os.path.isfile(doc_path):
        print(f"VIOLATION: missing docs/failure-matrix.md at {doc_path}", file=sys.stderr)
        sys.exit(1)

    with open(doc_path, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.splitlines()
    table_rows = []
    in_table = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("|") and ("FM-" in stripped or "ID" in stripped):
            in_table = True
            if not stripped.startswith("| ---") and not stripped.startswith("| ID"):
                table_rows.append(stripped)
        elif in_table and not stripped.startswith("|"):
            in_table = False

    if not table_rows:
        print("VIOLATION: No failure mode table rows found in docs/failure-matrix.md", file=sys.stderr)
        sys.exit(1)

    found_modes = {}
    violations = 0

    for row in table_rows:
        # Table columns: | ID | Scenario | Category | Fault Injection | Asserted Recovery Outcome | Owning Version | Deterministic Test | Permanent Seeds | Real-Backend Counterpart Test | Budget | Absolute SLO Target | Status / Reasoned Exemption |
        cols = [c.strip() for c in row.split("|")[1:-1]]
        if len(cols) < 8:
            print(f"VIOLATION: Malformed table row (expected at least 8 columns, found {len(cols)}): {row}", file=sys.stderr)
            violations += 1
            continue

        raw_id = cols[0]
        scenario = cols[1]
        category = cols[2]
        fault_inj = cols[3]
        outcome = cols[4]
        owning_ver = cols[5]
        test_link = cols[6]
        seeds = cols[7]
        real_backend_test = cols[8] if len(cols) > 8 else ""
        budget = cols[9] if len(cols) > 9 else ""
        slo_target = cols[10] if len(cols) > 10 else ""
        exemption = cols[11] if len(cols) > 11 else ""

        fm_id = raw_id.replace("`", "").strip()

        if not fm_id.startswith("FM-"):
            continue

        found_modes[fm_id] = {
            "scenario": scenario,
            "category": category,
            "fault_inj": fault_inj,
            "outcome": outcome,
            "owning_ver": owning_ver,
            "test_link": test_link.replace("`", "").strip(),
            "seeds": seeds.replace("`", "").strip(),
            "real_backend_test": real_backend_test.replace("`", "").strip(),
            "budget": budget.replace("`", "").strip(),
            "slo_target": slo_target.replace("`", "").strip(),
            "exemption": exemption.replace("`", "").strip(),
        }

    # 1. Check all required modes are present
    for req_mode in REQUIRED_FAILURE_MODES:
        if req_mode not in found_modes:
            print(f"VIOLATION: Missing failure mode: {req_mode}", file=sys.stderr)
            violations += 1
            continue

        cell = found_modes[req_mode]

        # 2. Check scenario is non-empty
        if not cell["scenario"] or cell["scenario"] == "---":
            print(f"VIOLATION: {req_mode} has empty scenario", file=sys.stderr)
            violations += 1

        # 3. Check for vacuous recovery outcome
        outcome_text = cell["outcome"].lower()
        if not outcome_text or outcome_text == "---":
            print(f"VIOLATION: {req_mode} has empty asserted recovery outcome", file=sys.stderr)
            violations += 1
        else:
            for pat in VACUOUS_PATTERNS:
                if re.search(pat, outcome_text):
                    print(f"VIOLATION: {req_mode} has vacuous recovery assertion: '{cell['outcome']}' (matches '{pat}')", file=sys.stderr)
                    violations += 1
                    break

        # 4. Check deterministic test resolution
        test_link = cell["test_link"]
        if not test_link or test_link == "---":
            print(f"VIOLATION: {req_mode} has missing deterministic test link", file=sys.stderr)
            violations += 1
        else:
            if "::" in test_link:
                test_file_rel, test_symbol = test_link.split("::", 1)
            else:
                test_file_rel, test_symbol = test_link, ""

            test_file_path = os.path.join(root, test_file_rel)
            if not os.path.isfile(test_file_path):
                print(f"VIOLATION: {req_mode} test file missing: {test_file_rel}", file=sys.stderr)
                violations += 1
            elif test_symbol:
                with open(test_file_path, "r", encoding="utf-8") as tf:
                    file_text = tf.read()
                if test_symbol not in file_text:
                    print(f"VIOLATION: {req_mode} test symbol '{test_symbol}' missing in {test_file_rel}", file=sys.stderr)
                    violations += 1

        # 5. Check permanent seeds reference
        seeds_text = cell["seeds"]
        if not seeds_text or seeds_text == "---" or not re.search(r"0x[0-9a-fA-F_]+", seeds_text):
            print(f"VIOLATION: {req_mode} missing permanent seed corpus reference (found '{seeds_text}')", file=sys.stderr)
            violations += 1

        # 6. Check real-backend counterpart test resolution or reasoned exemption
        real_backend = cell["real_backend_test"]
        exemption = cell["exemption"]
        has_real_test = bool(real_backend and real_backend != "---")
        has_exemption = bool(exemption and ("Reasoned Exemption" in exemption or "exemption" in exemption.lower()))

        if not has_real_test and not has_exemption:
            print(f"VIOLATION: {req_mode} has neither a real backend test counterpart nor a reasoned exemption", file=sys.stderr)
            violations += 1

        if has_real_test:
            if "::" in real_backend:
                rb_file_rel, rb_symbol = real_backend.split("::", 1)
            else:
                rb_file_rel, rb_symbol = real_backend, ""
            rb_file_path = os.path.join(root, rb_file_rel)
            if not os.path.isfile(rb_file_path):
                print(f"VIOLATION: {req_mode} real backend test file missing: {rb_file_rel}", file=sys.stderr)
                violations += 1
            elif rb_symbol:
                with open(rb_file_path, "r", encoding="utf-8") as rbf:
                    rb_file_text = rbf.read()
                if rb_symbol not in rb_file_text:
                    print(f"VIOLATION: {req_mode} real backend test symbol '{rb_symbol}' missing in {rb_file_rel}", file=sys.stderr)
                    violations += 1

        # 7. Check time budget
        budget_str = cell["budget"].rstrip("s").strip()
        if not budget_str or not budget_str.isdigit() or int(budget_str) <= 0:
            print(f"VIOLATION: {req_mode} has invalid or missing time budget: '{cell['budget']}'", file=sys.stderr)
            violations += 1

        # 8. Check absolute SLO target
        slo_text = cell["slo_target"].lower()
        if not slo_text or slo_text == "---":
            print(f"VIOLATION: {req_mode} has empty absolute SLO target", file=sys.stderr)
            violations += 1
        else:
            for pat in VACUOUS_PATTERNS:
                if re.search(pat, slo_text):
                    print(f"VIOLATION: {req_mode} has vacuous absolute SLO target: '{cell['slo_target']}' (matches '{pat}')", file=sys.stderr)
                    violations += 1
                    break

    if violations > 0:
        print(f"\nFailed with {violations} violation(s).", file=sys.stderr)
        sys.exit(1)

    print(f"OK: All {len(REQUIRED_FAILURE_MODES)} production failure modes validated successfully.")
    sys.exit(0)

if __name__ == "__main__":
    main()

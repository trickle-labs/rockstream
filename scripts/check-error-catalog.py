#!/usr/bin/env python3
"""check-error-catalog.py — validate error catalog conformance and drift gates (DOC-01).

Checks:
1. Validates contracts/errors.toml structure, completeness, uniqueness, and fields.
2. Asserts zero drift between contracts/errors.toml and docs/error-codes.md.
3. Asserts zero drift between contracts/errors.toml and Rust constants in
   crates/rockstream-types/src/error_code.rs.

Usage:
    python3 scripts/check-error-catalog.py [ROOT_DIR]
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

VALID_SEVERITIES = {"Info", "Warning", "Error", "Fatal"}
VALID_RETRY_CLASSES = {
    "NonRetryable",
    "Immediate",
    "ExponentialBackoff",
    "AfterLeaderElection",
    "AfterClusterRecovery",
}

SUBSYSTEMS = [
    ("0xxx", "Internal & General System", 0, 999),
    ("1xxx", "Pipeline, Plan & Optimization", 1000, 1699),
    ("17xx", "Lease Management & Raft Leadership", 1700, 1999),
    ("2xxx", "Gateway, Query Execution & Wire Protocol", 2000, 2399),
    ("24xx", "Authentication, mTLS & Secrets", 2400, 2499),
    ("25xx-26xx", "Extended Query, Cursors & Transactions", 2500, 2999),
    ("3xxx", "Storage, Execution, Memory & Shuffle", 3000, 3999),
    ("4xxx", "DDL, Catalog, Ingestion & Removed Connectors", 4000, 4999),
    ("5xxx", "Cluster, Node Lifecycle & Shard Coordination", 5000, 5999),
    ("6xxx", "Connector Schema Evolution", 6000, 6999),
    ("8xxx", "Frontier Aggregation", 8000, 8999),
    ("9xxx", "Admission Control", 9000, 9999),
]


def parse_code_number(code_str: str) -> int:
    m = re.match(r"^RS-(\d{4})$", code_str)
    if not m:
        raise ValueError(f"Invalid error code format: '{code_str}' (expected RS-XXXX)")
    return int(m.group(1))


def render_markdown(contract: dict, errors: list[dict]) -> str:
    sorted_errors = sorted(errors, key=lambda e: parse_code_number(e["code"]))

    lines: list[str] = []
    lines.append("# RockStream Error Code Reference (`RS-XXXX`)")
    lines.append("")
    version = contract.get("version", "v0.59.12")
    lines.append(f"Contract version: `{version}` — Authoritative Static Error Catalog (`DOC-01`)")
    lines.append("")
    lines.append("Every user-visible, client-returned, or operator-logged error in RockStream carries a registered `RS-XXXX` code.")
    lines.append("This document is generated directly from `contracts/errors.toml` with zero manual drift.")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Subsystem Index")
    lines.append("")

    for sub_id, sub_name, start_n, end_n in SUBSYSTEMS:
        sub_errors = [e for e in sorted_errors if start_n <= parse_code_number(e["code"]) <= end_n]
        if sub_errors:
            lines.append(f"- [{sub_id}: {sub_name}](#{sub_id.lower()}-{sub_name.lower().replace(' ', '-').replace('&', '').replace(',', '')}) ({len(sub_errors)} codes)")

    lines.append("")
    lines.append("---")
    lines.append("")

    for sub_id, sub_name, start_n, end_n in SUBSYSTEMS:
        sub_errors = [e for e in sorted_errors if start_n <= parse_code_number(e["code"]) <= end_n]
        if not sub_errors:
            continue

        section_anchor = f"{sub_id.lower()}-{sub_name.lower().replace(' ', '-').replace('&', '').replace(',', '')}"
        lines.append(f"## {sub_id}: {sub_name}")
        lines.append("")
        lines.append("| Code | Key | Title | Severity | SQLSTATE | Retry Class |")
        lines.append("|---|---|---|---|---|---|")
        for e in sub_errors:
            lines.append(f"| [`{e['code']}`](#{e['doc_anchor']}) | `{e['key']}` | {e['title']} | `{e['severity']}` | `{e['sqlstate']}` | `{e['retry_class']}` |")
        lines.append("")

        for e in sub_errors:
            lines.append(f"### <a id=\"{e['doc_anchor']}\"></a> `{e['code']}` — {e['title']}")
            lines.append("")
            lines.append(f"- **Key**: `{e['key']}`")
            lines.append(f"- **Severity**: `{e['severity']}`")
            lines.append(f"- **SQLSTATE**: `{e['sqlstate']}`")
            lines.append(f"- **Retry Class**: `{e['retry_class']}`")
            lines.append(f"- **Default Next Steps**: {entry_steps(e)}")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def entry_steps(e: dict) -> str:
    return str(e.get("default_next_steps", ""))



def main() -> int:
    parser = argparse.ArgumentParser(description="Validate error catalog and drift gates")
    parser.add_argument("root_dir", nargs="?", default=None, help="Root directory of the repository")
    parser.add_argument("--root", dest="opt_root", default=None, help="Root directory of the repository")
    args = parser.parse_args()

    root_path_str = args.opt_root or args.root_dir or "."
    root = Path(root_path_str).resolve()

    toml_path = root / "contracts" / "errors.toml"
    docs_path = root / "docs" / "error-codes.md"
    rust_path = root / "crates" / "rockstream-types" / "src" / "error_code.rs"

    violations: list[str] = []

    if not toml_path.exists():
        print(f"VIOLATION: contracts/errors.toml not found at {toml_path}", file=sys.stderr)
        return 1

    try:
        data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"VIOLATION: Error parsing {toml_path}: {e}", file=sys.stderr)
        return 1

    contract = data.get("contract", {})
    errors = data.get("error", [])

    if not errors:
        violations.append("contracts/errors.toml contains no [[error]] definitions")

    seen_numbers: set[int] = set()
    seen_keys: set[str] = set()
    seen_anchors: set[str] = set()

    for idx, entry in enumerate(errors):
        for req_field in [
            "code",
            "key",
            "title",
            "severity",
            "sqlstate",
            "retry_class",
            "default_next_steps",
            "doc_anchor",
        ]:
            if req_field not in entry or not str(entry[req_field]).strip():
                violations.append(f"Entry #{idx} missing required field '{req_field}'")

        code_str = entry.get("code", "")
        try:
            num = parse_code_number(code_str)
            if num in seen_numbers:
                violations.append(f"Duplicate error code number: {num} ({code_str})")
            seen_numbers.add(num)
        except ValueError as e:
            violations.append(str(e))

        key = entry.get("key", "").strip()
        if key in seen_keys:
            violations.append(f"Duplicate error key: '{key}'")
        seen_keys.add(key)

        anchor = entry.get("doc_anchor", "").strip()
        if anchor in seen_anchors:
            violations.append(f"Duplicate doc_anchor: '{anchor}'")
        seen_anchors.add(anchor)

        sev = entry.get("severity", "")
        if sev not in VALID_SEVERITIES:
            violations.append(f"Invalid severity '{sev}' for {code_str}")

        sqlstate = entry.get("sqlstate", "").strip()
        if len(sqlstate) != 5 or not sqlstate.isalnum():
            violations.append(f"Invalid SQLSTATE '{sqlstate}' for {code_str}")

        retry = entry.get("retry_class", "")
        if retry not in VALID_RETRY_CLASSES:
            violations.append(f"Invalid retry_class '{retry}' for {code_str}")

    # Check documentation drift
    if not docs_path.exists():
        violations.append(f"docs/error-codes.md not found at {docs_path}")
    else:
        actual_docs = docs_path.read_text(encoding="utf-8")
        expected_docs = render_markdown(contract, errors)
        if actual_docs != expected_docs:
            violations.append(
                "docs/error-codes.md has drifted from contracts/errors.toml (run scripts/generate-error-catalog.py to update)"
            )

    # Check Rust constants drift
    if not rust_path.exists():
        violations.append(f"crates/rockstream-types/src/error_code.rs not found at {rust_path}")
    else:
        rust_src = rust_path.read_text(encoding="utf-8")
        # Match pub const RS_XXXX: ErrorCode = ErrorCode::new(XXXX); or ErrorCode(XXXX);
        const_matches = re.findall(r"pub\s+const\s+(RS_\d{4}):\s*ErrorCode\s*=\s*ErrorCode(?:::new)?\((\d+)\);", rust_src)
        rust_constants = {int(num): name for name, num in const_matches}

        # Check all toml errors have constants
        for num in seen_numbers:
            if num not in rust_constants:
                violations.append(f"Missing Rust constant for RS_{num:04} in {rust_path}")
            elif rust_constants[num] != f"RS_{num:04}":
                violations.append(f"Mismatched constant name for RS_{num:04}: {rust_constants[num]}")

        # Check all constants in rust_path exist in toml
        for num, name in rust_constants.items():
            if num not in seen_numbers:
                violations.append(f"Extraneous Rust constant {name} (code {num}) in {rust_path} not found in contracts/errors.toml")

    if violations:
        print(f"VIOLATION: Found {len(violations)} error catalog violation(s):", file=sys.stderr)
        for v in violations:
            print(f"  - {v}", file=sys.stderr)
        return 1

    print(f"OK: Error catalog validated with {len(errors)} descriptors, 0 drift across constants and documentation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

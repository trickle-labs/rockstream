#!/usr/bin/env python3
"""generate-error-catalog.py — generates docs/error-codes.md from contracts/errors.toml (DOC-01).

Usage:
    python3 scripts/generate-error-catalog.py [--check] [--root ROOT_DIR]
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


def load_catalog(toml_path: Path) -> tuple[dict, list[dict]]:
    if not toml_path.exists():
        print(f"Error: catalog file not found at {toml_path}", file=sys.stderr)
        sys.exit(1)

    try:
        data = tomllib.loads(toml_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error parsing {toml_path}: {e}", file=sys.stderr)
        sys.exit(1)

    contract = data.get("contract", {})
    errors = data.get("error", [])
    if not errors:
        print(f"Error: no [[error]] entries found in {toml_path}", file=sys.stderr)
        sys.exit(1)

    return contract, errors


def validate_catalog(errors: list[dict]) -> list[str]:
    issues: list[str] = []
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
                issues.append(f"Entry #{idx} missing required field '{req_field}'")

        code_str = entry.get("code", "")
        try:
            num = parse_code_number(code_str)
            if num in seen_numbers:
                issues.append(f"Duplicate error code number: {num} ({code_str})")
            seen_numbers.add(num)
        except ValueError as e:
            issues.append(str(e))

        key = entry.get("key", "").strip()
        if key in seen_keys:
            issues.append(f"Duplicate error key: '{key}'")
        seen_keys.add(key)

        anchor = entry.get("doc_anchor", "").strip()
        if anchor in seen_anchors:
            issues.append(f"Duplicate doc_anchor: '{anchor}'")
        seen_anchors.add(anchor)

        sev = entry.get("severity", "")
        if sev not in VALID_SEVERITIES:
            issues.append(f"Invalid severity '{sev}' for {code_str} (expected one of {sorted(VALID_SEVERITIES)})")

        sqlstate = entry.get("sqlstate", "").strip()
        if len(sqlstate) != 5 or not sqlstate.isalnum():
            issues.append(f"Invalid SQLSTATE '{sqlstate}' for {code_str} (must be 5 alphanumeric characters)")

        retry = entry.get("retry_class", "")
        if retry not in VALID_RETRY_CLASSES:
            issues.append(f"Invalid retry_class '{retry}' for {code_str} (expected one of {sorted(VALID_RETRY_CLASSES)})")

    return issues


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
            lines.append(f"- **Default Next Steps**: {e['default_next_steps']}")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate docs/error-codes.md from contracts/errors.toml")
    parser.add_argument("--check", action="store_true", help="Check that docs/error-codes.md matches without modifying")
    parser.add_argument("--root", type=Path, default=None, help="Root directory of the repository")
    args = parser.parse_args()

    root = args.root or Path(__file__).resolve().parent.parent
    toml_path = root / "contracts" / "errors.toml"
    doc_path = root / "docs" / "error-codes.md"

    contract, errors = load_catalog(toml_path)
    issues = validate_catalog(errors)
    if issues:
        print(f"Validation failed for {toml_path}:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        sys.exit(1)

    rendered = render_markdown(contract, errors)

    if args.check:
        if not doc_path.exists():
            print(f"Error: {doc_path} does not exist. Run scripts/generate-error-catalog.py to generate it.", file=sys.stderr)
            sys.exit(1)
        actual = doc_path.read_text(encoding="utf-8")
        if actual != rendered:
            print("Error: docs/error-codes.md is out of sync with contracts/errors.toml. Run scripts/generate-error-catalog.py.", file=sys.stderr)
            sys.exit(1)
        print("OK: docs/error-codes.md is in sync with contracts/errors.toml")
    else:
        doc_path.parent.mkdir(parents=True, exist_ok=True)
        doc_path.write_text(rendered, encoding="utf-8")
        print(f"Successfully generated {doc_path} ({len(errors)} error codes cataloged)")


if __name__ == "__main__":
    main()

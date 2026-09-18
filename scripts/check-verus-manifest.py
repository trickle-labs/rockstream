#!/usr/bin/env python3
"""Check the registered Verus claims and their production symbols."""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path


STATUSES = {"planned", "kernel-verified", "adapter-qualified", "release-qualified"}
SYMBOL_FIELDS = ("implementation", "theorems", "production_callers", "regression_tests")
FORBIDDEN = re.compile(
    r"\b(?:assume|admit)\s*\(|external_(?:body|fn_specification)|"
    r"\bcfg\s*\([^)]*\bverus_only\b",
    re.DOTALL,
)
PUBLIC_FUNCTION = re.compile(
    r"^\s*pub\s+(?:(?:open|proof|spec)\s+)*fn\s+([A-Za-z_][A-Za-z0-9_]*)\b"
)
CLAIM_MARKER = re.compile(r"verus-claim:\s*([A-Za-z0-9._-]+)")


def fail(message: str) -> None:
    print(f"verus manifest: ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def check_symbol(root: Path, reference: str, claim_id: str) -> None:
    path_text, separator, symbol = reference.partition("::")
    if not separator or not symbol:
        fail(f"{claim_id} has an invalid symbol reference: {reference}")
    path = root / path_text
    if not path.is_file():
        fail(f"{claim_id} references missing file: {path_text}")
    source = path.read_text()
    pattern = re.compile(
        rf"\b(?:pub\s+)?(?:async\s+)?(?:spec\s+|proof\s+)?fn\s+{re.escape(symbol)}\b"
    )
    if not pattern.search(source):
        fail(f"{claim_id} references missing symbol: {reference}")


def check_trust_policy(root: Path) -> None:
    policy_path = root / "formal/verus/assumptions.toml"
    if not policy_path.is_file():
        fail(f"missing trust policy: {policy_path}")
    application = tomllib.loads(policy_path.read_text()).get("application", {})
    for field in ("unchecked_assumptions", "excluded_items", "verification_only_executable_branches"):
        if application.get(field) != []:
            fail(f"trust policy {field} must be empty")
    if application.get("unsafe_code_added") is not False:
        fail("trust policy unsafe_code_added must be false")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    root = parser.parse_args().root.resolve()
    check_trust_policy(root)
    manifest_path = root / "formal/verus/manifest.toml"
    if not manifest_path.is_file():
        fail(f"missing manifest: {manifest_path}")

    data = tomllib.loads(manifest_path.read_text())
    claims = data.get("claims")
    if not isinstance(claims, list) or not claims:
        fail("manifest must contain at least one claim")

    claim_ids: set[str] = set()
    for claim in claims:
        claim_id = claim.get("id")
        if not isinstance(claim_id, str) or not claim_id:
            fail("every claim needs a non-empty id")
        if claim_id in claim_ids:
            fail(f"duplicate claim id: {claim_id}")
        claim_ids.add(claim_id)

        status = claim.get("status")
        if status not in STATUSES:
            fail(f"{claim_id} has invalid status: {status}")
        if not claim.get("domain") or not claim.get("assumptions"):
            fail(f"{claim_id} must declare its domain and assumptions")
        if status != "planned" and not claim.get("evidence"):
            fail(f"completed claim {claim_id} has no evidence")
        for field in SYMBOL_FIELDS:
            references = claim.get(field, [])
            if not isinstance(references, list) or not references:
                fail(f"{claim_id} must list {field}")
            for reference in references:
                check_symbol(root, reference, claim_id)

    source_root = root / "crates/rockstream-verified/src"
    if not source_root.is_dir():
        fail(f"missing verified source directory: {source_root}")
    registered_markers = set()
    for source_path in source_root.rglob("*.rs"):
        source = source_path.read_text()
        if FORBIDDEN.search(source):
            fail(f"unchecked proof construct found in {source_path.relative_to(root)}")
        lines = source.splitlines()
        for line_number, line in enumerate(lines):
            function = PUBLIC_FUNCTION.match(line)
            if function:
                context = "\n".join(lines[max(0, line_number - 4) : line_number])
                if not CLAIM_MARKER.search(context):
                    fail(
                        f"missing verus claim marker for public function "
                        f"{source_path.relative_to(root)}::{function.group(1)}"
                    )
        registered_markers.update(CLAIM_MARKER.findall(source))

    unregistered = registered_markers - claim_ids
    if unregistered:
        fail(f"unregistered verus claim markers: {', '.join(sorted(unregistered))}")

    print(
        f"verus manifest: {len(claims)} claim(s) checked; "
        f"{len(registered_markers)} source marker(s); zero unchecked application assumptions."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

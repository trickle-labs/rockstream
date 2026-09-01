#!/usr/bin/env python3
"""Generate the v0.57.1 capability matrix from capabilities.toml."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import tomllib
from pathlib import Path

TIERS = {"Core", "Maintain", "Experimental"}
KINDS = {"language", "connector", "sink"}
PROOF_RE = re.compile(r"^(?P<path>[^:]+)::(?P<test>[A-Za-z_][A-Za-z0-9_]*)$")
BEHAVIORS = (
    "incremental",
    "backfill",
    "checkpoint_recovery",
    "state_growth",
    "failure",
)


def fail(message: str) -> None:
    raise ValueError(message)


def check_file(root: Path, relative: str, label: str) -> str:
    path = root / relative
    if not path.is_file():
        fail(f"{label} does not exist: {relative}")
    return path.read_text(encoding="utf-8")


def validate(
    root: Path, data: dict
) -> tuple[dict, list[dict], dict[str, dict], str]:
    contract = data.get("contract")
    if not isinstance(contract, dict):
        fail("capabilities.toml must define [contract]")
    version = contract.get("version")
    if version not in {"v0.57.1", "v0.59.1", "v0.59.3", "v0.59.4", "v0.59.6", "v0.59.7", "v0.59.18", "v0.59.20"}:
        fail("capabilities.toml contract.version must be v0.57.1, v0.59.1, v0.59.3, v0.59.4, v0.59.6, v0.59.7, v0.59.18, or v0.59.20")

    roadmap = contract.get("roadmap")
    if not isinstance(roadmap, str):
        fail("contract.roadmap must be a path")
    roadmap_text = check_file(root, roadmap, "roadmap")
    roadmap_rows = [
        row
        for row in re.findall(
            rf"^\| {re.escape(str(version))} \|.*$", roadmap_text, re.MULTILINE
        )
        if len(row.split("|")) >= 6 and "✅ Done" in row
    ]
    if len(roadmap_rows) != 1:
        fail(f"NEW_ROADMAP.md has no {version} version row")

    roadmap_fingerprint = hashlib.sha256(
        roadmap_rows[0].encode("utf-8")
    ).hexdigest()
    if not contract.get("promise"):
        fail("contract.promise must not be empty")

    dispatches = data.get("dispatch", [])
    if not isinstance(dispatches, list) or not dispatches:
        fail("capabilities.toml must define dispatch evidence")
    dispatch_by_id: dict[str, dict] = {}
    for evidence in dispatches:
        evidence_id = evidence.get("id")
        if not isinstance(evidence_id, str) or evidence_id in dispatch_by_id:
            fail(f"dispatch evidence has duplicate or invalid id: {evidence_id!r}")
        path = evidence.get("path")
        symbol = evidence.get("symbol")
        surface = evidence.get("surface")
        if not all(isinstance(value, str) and value for value in (path, symbol, surface)):
            fail(f"dispatch evidence {evidence_id} is missing path, symbol, or surface")
        text = check_file(root, path, f"dispatch evidence {evidence_id}")
        if symbol not in text:
            fail(f"dispatch evidence {evidence_id} cannot find {symbol} in {path}")
        dispatch_by_id[evidence_id] = evidence

    capabilities = data.get("capability", [])
    if not isinstance(capabilities, list) or not capabilities:
        fail("capabilities.toml must define capability records")
    ids: set[str] = set()
    for capability in capabilities:
        capability_id = capability.get("id")
        if not isinstance(capability_id, str) or capability_id in ids:
            fail(f"capability has duplicate or invalid id: {capability_id!r}")
        ids.add(capability_id)
        kind = capability.get("kind")
        if kind not in KINDS:
            fail(f"{capability_id} has invalid kind: {kind!r}")
        tier = capability.get("tier")
        if tier not in TIERS:
            fail(f"{capability_id} has invalid tier: {tier!r}")
        for field in ("name", "reachability", "documentation"):
            if not isinstance(capability.get(field), str) or not capability[field]:
                fail(f"{capability_id} is missing {field}")
        documentation = capability["documentation"].split("#", 1)[0]
        check_file(root, documentation, f"{capability_id} documentation")
        references = capability.get("dispatch")
        if not isinstance(references, list):
            fail(f"{capability_id} dispatch must be an array")
        for reference in references:
            if reference not in dispatch_by_id:
                fail(f"{capability_id} references unknown dispatch evidence {reference}")
        proof = capability.get("proof", "")
        if not isinstance(proof, str):
            fail(f"{capability_id} proof must be a string")
        if tier == "Core":
            match = PROOF_RE.fullmatch(proof)
            if not match:
                fail(f"{capability_id} is Core but has no named proof test")
            proof_text = check_file(root, match["path"], f"{capability_id} proof")
            if not re.search(
                rf"\b(?:async\s+)?fn\s+{re.escape(match['test'])}\s*\(",
                proof_text,
            ):
                fail(f"{capability_id} proof test is not present: {proof}")
            if not references:
                fail(f"{capability_id} is Core but has no dispatch evidence")
        elif proof:
            match = PROOF_RE.fullmatch(proof)
            if not match:
                fail(f"{capability_id} proof must be empty or a named test")
            proof_text = check_file(root, match["path"], f"{capability_id} proof")
            if not re.search(
                rf"\b(?:async\s+)?fn\s+{re.escape(match['test'])}\s*\(",
                proof_text,
            ):
                fail(f"{capability_id} proof test is not present: {proof}")

    return contract, capabilities, dispatch_by_id, roadmap_fingerprint


def markdown_proof(proof: str) -> str:
    return f"`{proof}`" if proof else "—"


def semantic_rows(capability: dict) -> list[dict]:
    rows = capability.get("behavior", [])
    return rows if isinstance(rows, list) else []


def render_semantics(capabilities: list[dict]) -> list[str]:
    lines = [
        "<!-- BEGIN GENERATED CORE SEMANTICS -->",
        "## Core semantic ledger",
        "",
        "This block is generated from the five-behavior ledger in "
        "`capabilities.toml`.",
        "",
        "| Capability | Behavior | Statement | Proof | Paired proof | Bound | Metric | Bound outcome |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for capability in sorted(
        (
            item
            for item in capabilities
            if item["tier"] == "Core"
        ),
        key=lambda item: item["id"],
    ):
        for row in semantic_rows(capability):
            lines.append(
                f"| `{capability['id']}` | `{row['behavior']}` | "
                f"{row['statement']} | {markdown_proof(row['proof'])} | "
                f"{markdown_proof(row.get('paired_proof', ''))} | "
                f"{row.get('bound', '—')} | {row.get('metric', '—')} | "
                f"{row.get('on_bound', '—')} |"
            )
    lines.extend(["", "<!-- END GENERATED CORE SEMANTICS -->"])
    return lines


def generate(root: Path, source: Path, output: Path) -> None:
    with source.open("rb") as stream:
        data = tomllib.load(stream)
    contract, capabilities, dispatches, roadmap_fingerprint = validate(root, data)

    rows = sorted(capabilities, key=lambda item: (item["kind"], item["id"]))
    core_dispatch_ids = sorted(
        {
            dispatch_id
            for capability in capabilities
            if capability["tier"] == "Core"
            for dispatch_id in capability["dispatch"]
        }
    )

    lines = [
        "# RockStream Capability Matrix",
        "",
        "<!-- Generated by scripts/generate-capability-matrix.py; do not edit. -->",
        "",
        f"Contract version: `{contract['version']}`",
        "",
        f"Roadmap row fingerprint: `sha256:{roadmap_fingerprint}`",
        "",
        f"> {contract['promise']}",
        "",
        "The matrix is generated from `capabilities.toml`. `Core` records are "
        "release-gated; `Maintain` records remain supported without being a "
        "growth area; `Experimental` records have no continuity guarantee.",
        "",
        "## Capabilities",
        "",
        "| ID | Class | Capability | Tier | Reachability | Dispatch evidence | Proof | Documentation |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in rows:
        dispatch = ", ".join(f"`{entry}`" for entry in item["dispatch"]) or "—"
        lines.append(
            f"| `{item['id']}` | {item['kind']} | {item['name']} | {item['tier']} "
            f"| {item['reachability']} | {dispatch} | {markdown_proof(item['proof'])} "
            f"| `{item['documentation']}` |"
        )

    lines.extend(
        [
            "",
            "## Core dispatch inventory",
            "",
            "This inventory is derived from dispatch evidence referenced by "
            "`Core` records; it is not a second hand-written operator list.",
            "",
            "| Identity | Source | Anchor | Surface |",
            "| --- | --- | --- | --- |",
        ]
    )
    for dispatch_id in core_dispatch_ids:
        evidence = dispatches[dispatch_id]
        lines.append(
            f"| `{dispatch_id}` | `{evidence['path']}` | `{evidence['symbol']}` "
            f"| {evidence['surface']} |"
        )

    lines.extend(["", *render_semantics(capabilities)])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    source = (args.source or root / "capabilities.toml").resolve()
    output = (args.output or root / "docs/capability-matrix.md").resolve()
    try:
        generate(root, source, output)
    except (OSError, tomllib.TOMLDecodeError, ValueError) as error:
        print(f"capability matrix generation failed: {error}", file=sys.stderr)
        return 1
    try:
        output_name = output.relative_to(root)
        source_name = source.relative_to(root)
    except ValueError:
        output_name = output
        source_name = source
    print(f"Generated {output_name} from {source_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

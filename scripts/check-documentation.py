#!/usr/bin/env python3
"""Check the current documentation entry points and contributor commands."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote

REQUIRED = (
    "docs/README.md",
    "docs/getting-started.md",
    "docs/operator.md",
    "docs/contributors.md",
    "docs/history.md",
    "docs/test-commands.md",
    "docs/schema-evolution.md",
    "docs/adr/README.md",
    "docs/reference/cli.md",
    "docs/reference/configuration.md",
    "docs/reference/functions.md",
    "docs/reference/sql-support.md",
    "docs/reference/catalog.md",
    "docs/reference/metrics.md",
    "docs/reference/errors.md",
    "docs/cli.md",
    "docs/configuration.md",
    "docs/error-codes.md",
    "docs/language-features.md",
)
NAVIGATION = (
    "README.md",
    "CONTRIBUTING.md",
    "docs/README.md",
    "docs/getting-started.md",
    "docs/operator.md",
    "docs/contributors.md",
    "docs/history.md",
    "docs/adr/README.md",
    "docs/adr/0001-documentation-navigation.md",
    "docs/adr/0002-reference-compatibility.md",
    "docs/cli.md",
    "docs/configuration.md",
    "docs/error-codes.md",
    "docs/language-features.md",
    "examples/reference-app/README.md",
)
GENERATED_HEADINGS = {
    "docs/reference/cli.md": "# CLI reference",
    "docs/reference/configuration.md": "# Configuration reference",
    "docs/reference/functions.md": "# Functions reference",
    "docs/reference/sql-support.md": "# SQL support reference",
    "docs/reference/catalog.md": "# Catalog reference",
    "docs/reference/metrics.md": "# Metrics reference",
    "docs/reference/errors.md": "# Errors reference",
}
OBSOLETE_TERMS = {"pipeline": "view"}


def rel(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def github_anchor(value: str) -> str:
    value = re.sub(r"[`*_~]", "", value).lower()
    value = re.sub(r"[^\w\s&-]", "", value)
    value = re.sub(r"\s+", "-", value.strip())
    return value.replace("&", "")


def anchors(path: Path) -> set[str]:
    result = set(re.findall(r'<a\s+id=["\']([^"\']+)["\']', path.read_text()))
    for heading in re.findall(r"^#{1,6}\s+(.+?)\s*$", path.read_text(), re.MULTILINE):
        result.add(github_anchor(heading))
    return result


def check_links(root: Path, errors: list[str]) -> None:
    link_pattern = re.compile(r"!?\[[^]]*\]\(([^)\s]+)(?:\s+[^)]*)?\)")
    for name in NAVIGATION:
        source = root / name
        if not source.exists():
            errors.append(f"missing documentation target: {name}")
            continue
        for line_number, line in enumerate(source.read_text().splitlines(), 1):
            for raw_target in link_pattern.findall(line):
                target = unquote(raw_target)
                if target.startswith(("http://", "https://", "mailto:")):
                    continue
                target_path, _, fragment = target.partition("#")
                destination = source.parent / target_path if target_path else source
                if not destination.exists():
                    errors.append(
                        f"missing documentation target: {name}:{line_number} -> {target_path}"
                    )
                elif fragment and destination.is_file() and fragment not in anchors(destination):
                    errors.append(
                        f"missing documentation anchor: {name}:{line_number} -> {target}"
                    )


def check_required(root: Path, errors: list[str]) -> None:
    for name in REQUIRED:
        if not (root / name).exists():
            errors.append(f"missing documentation target: {name}")


def check_generated(root: Path, errors: list[str]) -> None:
    for name, heading in GENERATED_HEADINGS.items():
        path = root / name
        if path.exists() and path.read_text().splitlines()[0:1] != [heading]:
            errors.append(f"stale generated documentation: {name}")


def check_claims(root: Path, errors: list[str]) -> None:
    claim_pattern = re.compile(r"<!--\s*claim:\s*(.*?)\s*-->")
    proof_pattern = re.compile(r"Proof:\s*`([^`]+)::([^`]+)`")
    for path in (root / "docs").rglob("*.md"):
        lines = path.read_text().splitlines()
        for index, line in enumerate(lines):
            if not claim_pattern.search(line):
                continue
            proof = next((proof_pattern.search(candidate) for candidate in lines[index + 1 :] if candidate.strip()), None)
            if not proof:
                errors.append(f"unsupported documentation claim: {rel(root, path)}:{index + 1}")
                continue
            proof_path = root / proof.group(1)
            if not proof_path.exists() or proof.group(2) not in proof_path.read_text():
                errors.append(f"unsupported documentation claim: {rel(root, path)}:{index + 1}")


def check_terms(root: Path, errors: list[str]) -> None:
    paths = [
        root / name
        for name in (
            "docs/README.md",
            "docs/getting-started.md",
            "docs/operator.md",
            "docs/contributors.md",
            "docs/history.md",
            "docs/schema-evolution.md",
            "docs/test-commands.md",
        )
    ]
    for path in paths:
        if not path.exists():
            continue
        for line_number, line in enumerate(path.read_text().splitlines(), 1):
            for old, new in OBSOLETE_TERMS.items():
                if re.search(rf"\b{re.escape(old)}\b", line.lower()):
                    errors.append(
                        f"obsolete term '{old}'; use '{new}' at {rel(root, path)}:{line_number}"
                    )


def make_targets(root: Path) -> set[str]:
    return set(re.findall(r"^([A-Za-z0-9_.-]+):", (root / "Makefile").read_text(), re.MULTILINE))


def check_command(root: Path, command: str, errors: list[str]) -> None:
    parts = command.split()
    if not parts:
        return
    if parts[0] == "make":
        if len(parts) < 2 or parts[1] not in make_targets(root):
            errors.append(f"undocumented or nonexistent contributor command: '{command}'")
        return
    if parts[0] in {"bash", "sh", "python3"} and len(parts) > 1:
        if not (root / parts[1]).exists():
            errors.append(f"undocumented or nonexistent contributor command: '{command}'")
        return
    if parts[0] == "cargo":
        if "deny" in parts and not (root / "deny.toml").exists():
            errors.append(f"undocumented or nonexistent contributor command: '{command}'")
        elif "audit" in parts and not (root / "Cargo.lock").exists():
            errors.append(f"undocumented or nonexistent contributor command: '{command}'")
        elif "--test" in parts:
            test_name = parts[parts.index("--test") + 1]
            package = parts[parts.index("-p") + 1] if "-p" in parts else ""
            test_path = root / "crates" / package / "tests" / f"{test_name}.rs"
            if not test_path.exists():
                errors.append(f"undocumented or nonexistent contributor command: '{command}'")
        return
    errors.append(f"undocumented or nonexistent contributor command: '{command}'")


def check_commands(root: Path, errors: list[str]) -> None:
    path = root / "docs/test-commands.md"
    if not path.exists():
        return
    in_block = False
    for line in path.read_text().splitlines():
        if line.strip().startswith("```"):
            in_block = not in_block
        elif in_block and line.startswith("$ "):
            check_command(root, line[2:], errors)


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    errors: list[str] = []
    check_required(root, errors)
    check_links(root, errors)
    check_generated(root, errors)
    check_claims(root, errors)
    check_terms(root, errors)
    check_commands(root, errors)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("OK: documentation checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

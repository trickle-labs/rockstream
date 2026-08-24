#!/usr/bin/env python3
"""generate-product-surface.py — Generate deterministic docs/product-surface.json (DOC-001, DOC-004).

Usage:
    python3 scripts/generate-product-surface.py [--output PATH] [--check]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate deterministic RockStream product surface manifest (DOC-001, DOC-004)"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("docs/product-surface.json"),
        help="Target output path for product-surface.json",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check whether current docs/product-surface.json matches generated surface without writing",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="Repository root path",
    )
    args = parser.parse_args()

    root = args.root or Path(__file__).resolve().parent.parent
    output_path = args.output if args.output.is_absolute() else root / args.output

    if args.check:
        cmd = [
            "cargo",
            "run",
            "-q",
            "-p",
            "rockstream-docgen",
            "--",
            "check",
            "--manifest-path",
            str(output_path),
        ]
        result = subprocess.run(cmd, cwd=root)
        sys.exit(result.returncode)

    cmd = [
        "cargo",
        "run",
        "-q",
        "-p",
        "rockstream-docgen",
        "--",
        "generate",
        "--output",
        str(output_path),
    ]
    result = subprocess.run(cmd, cwd=root)
    if result.returncode != 0:
        print("Failed to generate product surface manifest", file=sys.stderr)
        sys.exit(result.returncode)

    print(f"Product surface manifest written to: {output_path}")


if __name__ == "__main__":
    main()

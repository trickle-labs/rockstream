#!/usr/bin/env python3
"""Guard the immutable v0.72 parent acceptance inputs; not capacity evidence."""

import hashlib
from pathlib import Path
import sys

FROZEN_SHA256 = {
    "docs/acceptance-contracts/132.md": "cc91b392f193e771e38336c7da3c62480ef7337083ce1a5c301f576096335734",
    "benchmarks/r1-local/profile-v072.toml": "d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a",
}


def check_frozen_inputs(root: Path, expected: dict[str, str] = FROZEN_SHA256) -> list[str]:
    failures = []
    for relative_path, digest in expected.items():
        path = root / relative_path
        if path.is_symlink():
            failures.append(f"{relative_path}: symlink is not a frozen file")
            continue
        try:
            observed = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            failures.append(f"{relative_path}: missing or unreadable")
            continue
        if observed != digest:
            failures.append(f"{relative_path}: expected sha256 {digest}, observed {observed}")
    return failures


def main() -> int:
    failures = check_frozen_inputs(Path(__file__).resolve().parent.parent)
    for failure in failures:
        print(f"v0.72 acceptance input drift: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("v0.72 parent contract and profile match their frozen digests (not capacity proof)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

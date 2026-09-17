#!/usr/bin/env python3
"""Record source and artifact identity for a completed VS6 run."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tomllib
from datetime import datetime, timezone
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_revision(root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def tool_version(command: str) -> str:
    try:
        result = subprocess.run([command, "--version"], check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"
    return result.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--status", choices=("passed", "blocked", "failed"), required=True)
    parser.add_argument("--steps", type=Path, required=True)
    parser.add_argument("--runtime-artifact", action="append", type=Path, default=[])
    parser.add_argument("--target", default="host")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    with (root / "formal/verus/qualification.toml").open("rb") as handle:
        qualification = tomllib.load(handle)

    artifacts = {}
    for relative in qualification["artifact_digest_paths"]:
        path = root / relative
        if not path.is_file():
            raise SystemExit(f"RS-0906: missing artifact for digest: {relative}")
        artifacts[relative] = sha256(path)
    runtime_artifacts = {
        path.name: sha256(path) for path in args.runtime_artifact if path.is_file()
    }

    record = {
        "schema": 1,
        "run_id": args.run_id,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "source_revision": git_revision(root),
        "target": args.target or "host",
        "status": args.status,
        "tool_versions": {
            "rustc": tool_version("rustc"),
            "cargo": tool_version("cargo"),
            "verus": tool_version("verus"),
        },
        "artifacts": artifacts,
        "runtime_artifacts": runtime_artifacts,
        "steps": args.steps.read_text(encoding="utf-8").splitlines(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(f"verus qualification: recorded {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

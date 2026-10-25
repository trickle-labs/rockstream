#!/usr/bin/env python3
"""Record source and artifact identity for a completed VS6 run."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
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


def mutation_evidence(directory: Path, root: Path, inventory: list[dict]) -> dict[str, str]:
    results = directory / "results.json"
    data = json.loads(results.read_text())
    if data.get("command") != ["cargo", "verus", "verify", "-p", "rockstream-verified", "--locked"]:
        raise ValueError("production mutation verifier command mismatch")
    expected_baselines = {"baseline-before.log", "baseline-after.log"}
    if set(data.get("baselines", {})) != expected_baselines:
        raise ValueError("production mutation baselines are incomplete")
    files = {"results.json": sha256(results)}
    for name, checksum in data["baselines"].items():
        path = directory / name
        if sha256(path) != checksum or not re.search(r"verification results:: \d+ verified, 0 errors", path.read_text()):
            raise ValueError(f"production mutation baseline is invalid: {name}")
        files[name] = checksum
    records = data.get("mutations", [])
    if [record.get("id") for record in records] != [case["id"] for case in inventory]:
        raise ValueError("production mutation results are incomplete")
    for case, record in zip(inventory, records):
        relative = case["target"].split("::", 1)[0]
        name = f"{case['id']}.log"
        if any(record.get(key) != case[key] for key in ("id", "claim", "target", "kind")) or \
                record.get("original_sha256") != sha256(root / relative) or \
                not isinstance(record.get("mutant_sha256"), str) or \
                not re.fullmatch(r"[0-9a-f]{64}", record["mutant_sha256"]) or \
                record.get("mutant_sha256") == record.get("original_sha256") or \
                record.get("log") != name or type(record.get("verifier_exit")) is not int or \
                record["verifier_exit"] == 0:
            raise ValueError(f"production mutation result is invalid: {case['id']}")
        path = directory / name
        proof = path.read_text()
        if sha256(path) != record.get("log_sha256") or relative not in proof or \
                not re.search(r"verification results:: \d+ verified, [1-9]\d* errors", proof):
            raise ValueError(f"production mutation rejection is invalid: {case['id']}")
        files[name] = record["log_sha256"]
    return {f"mutations/{name}": checksum for name, checksum in files.items()}


def merge_phases(directory: Path, output: Path, revision: str, target: str,
                 artifacts: dict, budgets: dict, root: Path, inventory: list[dict]) -> tuple[list[str], list[Path], dict, dict]:
    """Only complete, matching phase evidence can become full qualification."""
    steps, binaries, versions = [], [], {}
    mutations = mutation_evidence(directory / "debug/mutations", root, inventory)
    for phase, ids in (
        ("debug", ["verifier", "production-mutations", "verifier-incremental", "cargo-debug", "cargo-check", "cargo-all-features"]),
        ("release", ["cargo-release"]),
    ):
        record = json.loads((directory / phase / "phase.json").read_text())
        expected = {"schema": 1, "phase": phase, "source_revision": revision,
                    "target": target, "status": "passed", "artifacts": artifacts}
        if {key: record.get(key) for key in expected} != expected:
            raise ValueError(f"{phase}: qualification identity or status mismatch")
        rows = record["steps"]
        fields = [row.split("\t", 3) for row in rows]
        if [row[0] for row in fields] != ids:
            raise ValueError(f"{phase}: qualification steps differ from required steps")
        for row in fields:
            budget = budgets["cold_verification_seconds" if row[0] == "verifier" else
                             "incremental_verification_seconds" if row[0] == "verifier-incremental" else
                             "production_compile_seconds"]
            if len(row) != 4 or row[1] != "0" or not row[3] or not 0 <= int(row[2]) <= budget:
                raise ValueError(f"{phase}: unsuccessful or over-budget step {row[0]}")
        binary = directory / phase / f"rockstream-{phase}"
        if record["runtime_artifacts"] != {binary.name: sha256(binary)}:
            raise ValueError(f"{phase}: runtime artifact digest mismatch")
        steps.extend(rows)
        binaries.append(binary)
        versions[phase] = record["tool_versions"]
    # Validate both phases before staging either artifact or publishing a full record.
    output.mkdir(parents=True, exist_ok=True)
    staged = []
    for binary in binaries:
        destination = output / binary.name
        temporary = destination.with_suffix(".tmp")
        shutil.copyfile(binary, temporary)
        temporary.chmod(0o755)  # Artifact downloads do not retain executable mode.
        temporary.replace(destination)
        staged.append(destination)
    mutation_destination = output / "mutations"
    if mutation_destination.exists():
        shutil.rmtree(mutation_destination)
    shutil.copytree(directory / "debug/mutations", mutation_destination)
    return steps, staged, versions, mutations


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--status", choices=("passed", "blocked", "failed"), required=True)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--steps", type=Path)
    inputs.add_argument("--merge", type=Path, help="Directory with debug/release phase artifacts")
    parser.add_argument("--phase", choices=("all", "debug", "release"), default="all")
    parser.add_argument("--runtime-artifact", action="append", type=Path, default=[])
    parser.add_argument("--target", default="host")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.merge and (args.phase != "all" or args.status != "passed"):
        parser.error("phase assembly requires --phase all and --status passed")
    if args.merge:
        args.output.unlink(missing_ok=True)
    root = args.root.resolve()
    with (root / "formal/verus/qualification.toml").open("rb") as handle:
        qualification = tomllib.load(handle)

    artifacts = {}
    for relative in qualification["artifact_digest_paths"]:
        path = root / relative
        if not path.is_file():
            raise SystemExit(f"RS-0906: missing artifact for digest: {relative}")
        artifacts[relative] = sha256(path)
    revision = git_revision(root) if args.merge else None
    phase_versions = None
    mutations = None
    if args.merge:
        try:
            steps, args.runtime_artifact, phase_versions, mutations = merge_phases(
                args.merge, args.output.parent, revision, args.target or "host",
                artifacts, qualification["budgets"], root, qualification["production_mutations"])
        except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
            raise SystemExit(f"RS-0906: incomplete qualification: {error}") from error
    else:
        steps = args.steps.read_text(encoding="utf-8").splitlines()
    if args.status == "passed":
        missing_runtime = [path for path in args.runtime_artifact if not path.is_file()]
        if not args.runtime_artifact or missing_runtime:
            missing = missing_runtime[0] if missing_runtime else "<none supplied>"
            raise SystemExit(f"RS-0906: missing runtime artifact: {missing}")
        if args.phase == "all" and not args.merge:
            try:
                mutations = mutation_evidence(args.output.parent / "mutations", root,
                                              qualification["production_mutations"])
            except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
                raise SystemExit(f"RS-0906: incomplete qualification: {error}") from error
    runtime_artifacts = {
        path.name: sha256(path) for path in args.runtime_artifact if path.is_file()
    }

    record = {
        "schema": 1,
        "run_id": args.run_id,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "source_revision": revision or git_revision(root),
        "phase": args.phase,
        "target": args.target or "host",
        "status": args.status,
        "tool_versions": phase_versions["debug"] if phase_versions is not None else {
            "rustc": tool_version("rustc"),
            "cargo": tool_version("cargo"),
            "verus": tool_version("verus"),
        },
        "artifacts": artifacts,
        "runtime_artifacts": runtime_artifacts,
        "steps": steps,
    }
    if phase_versions is not None:
        record["phase_tool_versions"] = phase_versions
    if mutations is not None:
        record["production_mutations"] = mutations
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    print(f"verus qualification: recorded {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

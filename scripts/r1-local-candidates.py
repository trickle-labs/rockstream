#!/usr/bin/env python3
"""Build and verify the reproducible R1 B0/current candidate records."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


B0_ID = "b0-v0.59.4-local-rebuild"
CURRENT_ID = "current"
PROFILE_ID = "MBP-M5Pro-48GB-v1"
PROFILE_REVISION = 1
CONTRACT_CHECKER = "scripts/check-r1-local-evidence.py"


def fail(message: str) -> "NoReturn":
    raise RuntimeError(message)


def run(command: list[str], cwd: Path, *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or error.stdout.strip()
        fail(f"{' '.join(command)} failed: {detail}")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def git(root: Path, *args: str, text: bool = True) -> str | bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=text,
        check=True,
    )
    return result.stdout.strip() if text else result.stdout


def commit_sha(root: Path, revision: str) -> str:
    return str(git(root, "rev-parse", f"{revision}^{{commit}}"))


def source_tree_sha256(root: Path, revision: str) -> str:
    archive = git(root, "archive", "--format=tar", revision, text=False)
    return sha256_bytes(archive)


def commit_file(root: Path, revision: str, name: str) -> bytes:
    try:
        return bytes(git(root, "show", f"{revision}:{name}", text=False))
    except subprocess.CalledProcessError:
        fail(f"{revision} does not contain {name}")


def contract_digests(root: Path) -> dict[str, str]:
    result = run(
        [sys.executable, str(root / CONTRACT_CHECKER), "--contract-digest", str(root)],
        root,
    )
    values: dict[str, str] = {}
    for line in result.stdout.splitlines():
        name, separator, value = line.partition("=")
        if separator:
            values[name] = value
    if set(values) != {
        "profile_sha256",
        "corpus_sha256",
        "sql_sha256",
        "thresholds_sha256",
        "contract_sha256",
    }:
        fail("contract digest command returned an incomplete result")
    return values


def status_is_clean(root: Path) -> bool:
    status = str(git(root, "status", "--porcelain", "--untracked-files=all"))
    allowed = ("AGENTS.md", "evidence/r1-local/")
    return all(
        not line or line[3:] == allowed[0] or line[3:].startswith(allowed[1])
        for line in status.splitlines()
    )


def toolchain_name(toolchain: bytes) -> str:
    for line in toolchain.decode().splitlines():
        if line.strip().startswith("channel"):
            return line.split("=", 1)[1].strip().strip('"')
    fail("rust-toolchain.toml has no channel")


def public_json(binary: Path, cwd: Path, *args: str) -> dict:
    result = run([str(binary), *args], cwd)
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"{binary} returned invalid JSON for {' '.join(args)}: {error}")
    if not isinstance(value, dict):
        fail(f"{binary} returned a JSON value that is not an object")
    return value


def build_candidate(root: Path, candidate_id: str, artifact_dir: Path) -> dict:
    revision = "a4e4ad4" if candidate_id == B0_ID else "HEAD"
    source_commit = commit_sha(root, revision)
    lockfile = commit_file(root, source_commit, "Cargo.lock")
    toolchain = commit_file(root, source_commit, "rust-toolchain.toml")
    source_sha = source_tree_sha256(root, source_commit)
    artifact_path = artifact_dir / candidate_id / "rockstream"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f"r1-{candidate_id}-") as temporary:
        worktree = Path(temporary) / "source"
        run(["git", "worktree", "add", "--detach", str(worktree), source_commit], root)
        try:
            pinned_toolchain = toolchain_name(toolchain)
            rustc_version = run(["rustup", "run", pinned_toolchain, "rustc", "--version"], worktree).stdout.strip()
            build_env = {
                **dict(os.environ),
                "CARGO_TARGET_DIR": str(Path(temporary) / "target"),
                "CARGO_INCREMENTAL": "0",
                "ROCKSTREAM_COMMIT_SHA": source_commit,
                "ROCKSTREAM_BUILD_TIMESTAMP": str(git(root, "show", "-s", "--format=%cI", source_commit)),
                "ROCKSTREAM_RUSTC_VERSION": rustc_version,
            }
            run(
                [
                    "rustup",
                    "run",
                    pinned_toolchain,
                    "cargo",
                    "build",
                    "--locked",
                    "--release",
                    "--bin",
                    "rockstream",
                ],
                worktree,
                env=build_env,
            )
            built_binary = Path(build_env["CARGO_TARGET_DIR"]) / "release" / "rockstream"
            if not built_binary.is_file():
                fail(f"release binary was not produced: {built_binary}")
            shutil.copy2(built_binary, artifact_path)
            version = public_json(artifact_path, worktree, "version", "--json")
            effective_config = public_json(
                artifact_path,
                worktree,
                "--output",
                "json",
                "config",
                "print-effective",
            )
        finally:
            run(["git", "worktree", "remove", "--force", str(worktree)], root)

    expected_version = "0.59.4" if candidate_id == B0_ID else "0.59.7"
    if version.get("semantic_version") != expected_version:
        fail(f"{candidate_id} reports version {version.get('semantic_version')!r}, expected {expected_version}")
    if version.get("commit_sha") != source_commit:
        fail(f"{candidate_id} reports source {version.get('commit_sha')!r}, expected {source_commit}")
    if version.get("lockfile_digest") != sha256_bytes(lockfile):
        fail(f"{candidate_id} reports a lockfile digest different from its source")

    return {
        "id": candidate_id,
        "kind": "baseline_rebuild" if candidate_id == B0_ID else "current_candidate",
        "rebuild": candidate_id == B0_ID,
        "workloads": ["ordinary-aggregate", "ordinary-join"]
        if candidate_id == B0_ID
        else [
            "shared-arrangement",
            "factorized-join",
            "ordinary-aggregate",
            "ordinary-join",
            "uniform-worker-scaling",
        ],
        "source_commit": source_commit,
        "source_tree_sha256": source_sha,
        "lockfile_sha256": sha256_bytes(lockfile),
        "toolchain_sha256": sha256_bytes(toolchain),
        "toolchain": toolchain_name(toolchain),
        "binary_path": artifact_path.relative_to(root).as_posix(),
        "binary_sha256": sha256_file(artifact_path),
        "version": version,
        "effective_config_sha256": sha256_bytes(
            json.dumps(effective_config, sort_keys=True, separators=(",", ":")).encode()
        ),
    }


def record(root: Path, output: Path, artifact_dir: Path) -> None:
    if not status_is_clean(root):
        fail("source worktree is not clean; commit product changes before recording candidates")
    digests = contract_digests(root)
    candidates = [
        build_candidate(root, B0_ID, artifact_dir),
        build_candidate(root, CURRENT_ID, artifact_dir),
    ]
    document = {
        "schema_version": 1,
        "profile_id": PROFILE_ID,
        "profile_revision": PROFILE_REVISION,
        "contract_digests": digests,
        "candidates": candidates,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"recorded {len(candidates)} R1 candidates in {output}")


def verify(root: Path, record_path: Path) -> None:
    try:
        document = json.loads(record_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read candidate record: {error}")
    if document.get("schema_version") != 1:
        fail("candidate record schema_version must be 1")
    if document.get("profile_id") != PROFILE_ID or document.get("profile_revision") != PROFILE_REVISION:
        fail("candidate record is bound to the wrong R1 profile")
    if document.get("contract_digests") != contract_digests(root):
        fail("candidate record contract digests do not match the frozen R1 contract")
    candidates = document.get("candidates")
    if not isinstance(candidates, list) or {item.get("id") for item in candidates} != {B0_ID, CURRENT_ID}:
        fail("candidate record must contain exactly B0 and current")
    for candidate in candidates:
        source_commit = commit_sha(root, candidate["source_commit"])
        if source_commit != candidate["source_commit"]:
            fail(f"{candidate['id']} source commit is not full-length")
        if source_tree_sha256(root, source_commit) != candidate["source_tree_sha256"]:
            fail(f"{candidate['id']} source tree digest mismatch")
        lockfile = commit_file(root, source_commit, "Cargo.lock")
        toolchain = commit_file(root, source_commit, "rust-toolchain.toml")
        if sha256_bytes(lockfile) != candidate["lockfile_sha256"]:
            fail(f"{candidate['id']} lockfile digest mismatch")
        if sha256_bytes(toolchain) != candidate["toolchain_sha256"]:
            fail(f"{candidate['id']} toolchain digest mismatch")
        binary = root / candidate["binary_path"]
        if not binary.is_file():
            fail(f"{candidate['id']} binary is missing: {binary}")
        if sha256_file(binary) != candidate["binary_sha256"]:
            fail(f"{candidate['id']} binary digest mismatch")
        version = public_json(binary, root, "version", "--json")
        if version != candidate["version"]:
            fail(f"{candidate['id']} public version surface changed")
        if version.get("commit_sha") != source_commit:
            fail(f"{candidate['id']} public source SHA does not match its binary record")
        effective_config = public_json(binary, root, "--output", "json", "config", "print-effective")
        effective_digest = sha256_bytes(
            json.dumps(effective_config, sort_keys=True, separators=(",", ":")).encode()
        )
        if effective_digest != candidate["effective_config_sha256"]:
            fail(f"{candidate['id']} effective configuration changed")
    print("R1 local candidate records verified")


def main() -> int:
    root_default = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    record_parser = subparsers.add_parser("record")
    record_parser.add_argument("--root", type=Path, default=root_default)
    record_parser.add_argument("--output", type=Path, default=root_default / "evidence/r1-local/candidates.json")
    record_parser.add_argument("--artifact-dir", type=Path, default=root_default / "evidence/r1-local/artifacts")
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--root", type=Path, default=root_default)
    verify_parser.add_argument("--record", type=Path, default=root_default / "evidence/r1-local/candidates.json")
    args = parser.parse_args()
    root = args.root.resolve()
    try:
        if args.command == "record":
            record(root, args.output.resolve(), args.artifact_dir.resolve())
        else:
            verify(root, args.record.resolve())
    except (OSError, RuntimeError, subprocess.CalledProcessError, KeyError, TypeError) as error:
        print(f"VIOLATION: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Require pinned Verus to reject mutations of isolated production kernels."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib


ROOT = Path(__file__).resolve().parents[1]
VERIFY = ["cargo", "verus", "verify", "-p", "rockstream-verified", "--locked"]
PROOF_FAILURE = re.compile(r"verification results:: \d+ verified, [1-9]\d* errors")
PROOF_SUCCESS = re.compile(r"verification results:: \d+ verified, 0 errors")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify(candidate: Path, log: Path, env: dict[str, str]) -> tuple[int, str]:
    with log.open("wb") as output:
        result = subprocess.run(VERIFY, cwd=candidate, env=env, stdout=output, stderr=subprocess.STDOUT)
    return result.returncode, log.read_text(errors="replace")


def require_baseline(candidate: Path, log: Path, env: dict[str, str]) -> None:
    status, output = verify(candidate, log, env)
    if status != 0 or not PROOF_SUCCESS.search(output):
        raise RuntimeError(f"unchanged baseline did not verify; full log: {log}")


def main() -> int:
    with (ROOT / "formal/verus/qualification.toml").open("rb") as source:
        cases = tomllib.load(source).get("production_mutations", [])
    if len(cases) != 6 or any(case.get("required") is not True for case in cases):
        raise RuntimeError("six required production mutations must be registered")

    env = os.environ.copy()
    if not shutil.which("cargo-verus") or not shutil.which("verus"):
        cached = sorted((ROOT / ".cache/verus").glob("*/cargo-verus"))
        if not cached:
            raise RuntimeError("pinned cargo-verus and verus are required")
        env["PATH"] = str(cached[0].parent) + os.pathsep + env.get("PATH", "")
    env["RUSTUP_TOOLCHAIN"] = os.environ.get("VERUS_RUST_TOOLCHAIN", "1.98.1")

    cache = ROOT / ".cache"
    cache.mkdir(exist_ok=True)
    output = Path(os.environ.get("VERUS_MUTATION_OUTPUT") or tempfile.mkdtemp(prefix="verus-mutations-", dir=cache))
    output.mkdir(parents=True, exist_ok=True)
    records = []
    source_hashes = {
        case["target"].split("::", 1)[0]: digest((ROOT / case["target"].split("::", 1)[0]).read_bytes())
        for case in cases
    }
    with tempfile.TemporaryDirectory(prefix="verus-production-") as temp:
        candidate = Path(temp) / "candidate"
        shutil.copytree(ROOT, candidate, ignore=shutil.ignore_patterns(".git", ".cache", "target", "evidence"))
        env["CARGO_TARGET_DIR"] = str(candidate / "target")
        require_baseline(candidate, output / "baseline-before.log", env)
        for case in cases:
            name = case["id"]
            relative, symbol = case["target"].split("::", 1)
            if Path(relative).is_absolute() or ".." in Path(relative).parts:
                raise RuntimeError(f"{name}: invalid target path")
            source = candidate / relative
            original = source.read_bytes()
            before, after = case["before"], case["after"]
            text = original.decode()
            if text.count(before) != 1 or before == after or f"fn {symbol}(" not in text:
                raise RuntimeError(f"{name}: exact production target was not found")
            changed = text.replace(before, after, 1).encode()
            if digest(changed) == digest(original):
                raise RuntimeError(f"{name}: mutation did not change production source")
            source.write_bytes(changed)
            try:
                log = output / f"{name}.log"
                status, proof = verify(candidate, log, env | {"VERUS_MUTATION_ID": name})
                if status == 0 or not PROOF_FAILURE.search(proof) or relative not in proof:
                    raise RuntimeError(f"{name}: intended verifier rejection was not observed; full log: {log}")
                records.append({
                    "id": name,
                    "claim": case["claim"],
                    "target": case["target"],
                    "kind": case["kind"],
                    "original_sha256": digest(original),
                    "mutant_sha256": digest(changed),
                    "verifier_exit": status,
                    "log": log.name,
                })
                print(f"{name}: verifier rejected {case['target']}")
            finally:
                source.write_bytes(original)
        require_baseline(candidate, output / "baseline-after.log", env)

    if any(digest((ROOT / path).read_bytes()) != checksum for path, checksum in source_hashes.items()):
        raise RuntimeError("production source changed during isolated mutation run")

    (output / "results.json").write_text(json.dumps({"command": VERIFY, "mutations": records}, indent=2) + "\n")
    print(f"production mutations: {len(records)} rejected; full logs: {output}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, KeyError, ValueError, RuntimeError) as error:
        print(f"production mutations: {error}", file=sys.stderr)
        sys.exit(1)

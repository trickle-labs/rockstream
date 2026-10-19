#!/usr/bin/env python3
"""Bounded libFuzzer execution; compilation and corpus replay cannot pass this gate."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shutil
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
MANDATORY = {
    "fuzz_sql_parser", "fuzz_pgwire_decoder", "fuzz_postgres_cdc",
    "fuzz_control_worker_decoder", "fuzz_raft_rpc_decoder",
    "fuzz_kafka_payload_decoder", "fuzz_oidc_jwt_decoder",
}
SCOPE = "byte-decoder panic/sanitizer safety only; no stateful protocol or semantic-equivalence coverage"


def hashes(directory):
    return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(directory.iterdir()) if p.is_file()}


def registered_targets(root):
    manifest = tomllib.loads((root / "fuzz/Cargo.toml").read_text())
    targets = [entry["name"] for entry in manifest["bin"]]
    if not MANDATORY <= set(targets) or len(targets) != len(set(targets)):
        raise ValueError("missing mandatory or duplicate fuzz target")
    for entry in manifest["bin"]:
        if not (root / "fuzz" / entry["path"]).is_file():
            raise ValueError(f"missing target source: {entry['name']}")
        corpus = root / "fuzz/corpus" / entry["name"]
        if not corpus.is_dir() or not any(corpus.glob("valid-*")):
            raise ValueError(f"missing known-valid seed: {entry['name']}")
    return targets


def execute(command, log, deadline):
    with log.open("w") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        timed_out = False
        try:
            process.wait(timeout=deadline)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
    return {"command": command, "exit_code": process.returncode,
            "timed_out": timed_out, "log": log.name}


def execution_error(result, output, corpus_size):
    if result["timed_out"]:
        return "wall-clock timeout"
    if result["exit_code"] != 0:
        return "engine/target failure"
    done = re.search(r"^Done (\d+) runs in \d+ second", output, re.MULTILINE)
    if not done or int(done[1]) <= corpus_size + 1:
        return "no mutation execution beyond corpus replay"
    if not re.search(r"^#\d+\s+DONE\s+cov: [1-9]\d*", output, re.MULTILINE):
        return "missing coverage-guided completion"
    return None


def failure_signature(result, output):
    summary = re.findall(r"^SUMMARY: .+$", output, re.MULTILINE)
    return {"exit_code": result["exit_code"], "timed_out": result["timed_out"],
            "summary": summary}


def run_target(target, binary, corpus, destination, budget):
    destination.mkdir(parents=True, exist_ok=True)
    report = {"target": target, "scope": SCOPE, "budget": budget,
              "corpus_before": hashes(corpus), "failures": [], "errors": []}
    flags = [f"-timeout={budget['timeout_seconds']}", f"-rss_limit_mb={budget['rss_mb']}",
             f"-seed={budget['seed']}"]
    command = [str(binary), str(corpus), f"-max_total_time={budget['seconds']}",
               f"-artifact_prefix={destination}/", "-max_len=65536", *flags]
    report["command"] = command
    try:
        report["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        result = execute(command, destination / "engine.log",
                         budget["seconds"] + budget["timeout_seconds"] + 15)
        report["engine"] = result
        error = execution_error(result, (destination / "engine.log").read_text(errors="replace"),
                                len(report["corpus_before"]))
        if error:
            report["errors"].append(error)
        for original in sorted(destination.iterdir()):
            if not re.fullmatch(r"(?:crash|leak|timeout|oom)-[0-9a-f]+", original.name):
                continue
            minimized = destination / ("minimized-" + original.name)
            shutil.copyfile(original, minimized)
            minimize = execute([str(binary), str(original), "-minimize_crash=1",
                                f"-exact_artifact_path={minimized}",
                                f"-max_total_time={budget['minimize_seconds']}", *flags],
                               destination / (original.name + ".minimize.log"),
                               budget["minimize_seconds"] + budget["timeout_seconds"] + 15)
            minimized_ok = (not minimize["timed_out"] and minimize["exit_code"] == 0
                            and minimized.is_file())
            replay_input = minimized if minimized_ok else original
            replays = []
            for attempt in range(2):
                log = destination / (original.name + f".replay-{attempt}.log")
                replay = execute([str(binary), str(replay_input), "-runs=1", *flags],
                                 log, budget["timeout_seconds"] + 15)
                replay["signature"] = failure_signature(replay, log.read_text(errors="replace"))
                replays.append(replay)
            reproducible = (replays[0]["signature"] == replays[1]["signature"]
                            and replays[0]["exit_code"] != 0
                            and not replays[0]["timed_out"]
                            and bool(replays[0]["signature"]["summary"]))
            report["failures"].append({
                "original": original.name, "input": replay_input.name,
                "sha256": hashlib.sha256(replay_input.read_bytes()).hexdigest(),
                "minimize": minimize, "replays": replays, "reproducible": reproducible,
            })
            if not minimized_ok:
                report["errors"].append(f"minimization incomplete: {original.name}")
            if not reproducible:
                report["errors"].append(f"failure not reproducible: {original.name}")
    except (OSError, ValueError) as error:
        report["errors"].append(str(error))
    report["corpus_after"] = hashes(corpus)
    report["status"] = "failed" if report["errors"] else "passed"
    (destination / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"{target}: {report['status']}" + (": " + "; ".join(report["errors"]) if report["errors"] else ""))
    return report


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("budget must be positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--seconds", type=positive, default=120)
    args = parser.parse_args()
    destination = ROOT / "target/fuzz-results"
    destination.mkdir(parents=True, exist_ok=True)
    report = {"scope": SCOPE, "targets": [], "errors": []}
    report["build_command"] = ["cargo", "fuzz", "build", "--dev", "--target-dir", "target/fuzz"]
    report["build_environment"] = {key: os.environ[key] for key in (
        "RUSTUP_TOOLCHAIN", "RUSTFLAGS", "CARGO_PROFILE_DEV_DEBUG", "CARGO_PROFILE_DEV_INCREMENTAL"
    ) if key in os.environ}
    try:
        for key, command in {
            "candidate_revision": ["git", "rev-parse", "HEAD"],
            "toolchain": ["rustc", "-Vv"],
            "cargo_fuzz": ["cargo", "fuzz", "--version"],
        }.items():
            report[key] = subprocess.check_output(command, cwd=ROOT, text=True).strip()
        targets = registered_targets(ROOT)
        report["registered_targets"] = targets
        budget = {"seconds": args.seconds, "timeout_seconds": 5, "rss_mb": 2048,
                  "minimize_seconds": 30, "seed": 130}
        for target in targets:
            report["targets"].append(run_target(
                target, (args.bin_dir / target).resolve(), ROOT / "fuzz/corpus" / target,
                destination / target, budget))
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        report["errors"].append(str(error))
    report["status"] = "failed" if report["errors"] or any(
        target["status"] != "passed" for target in report["targets"]) else "passed"
    (destination / "run.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"fuzz run: {report['status']}; {SCOPE}")
    return int(report["status"] != "passed")


if __name__ == "__main__":
    sys.exit(main())

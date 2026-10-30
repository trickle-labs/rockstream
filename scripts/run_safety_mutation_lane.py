#!/usr/bin/env python3
"""Run a fixed, reviewed Rust mutation sample in an isolated source copy."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "docs/testing/safety-mutation-lane.json"
TEST_OK = re.compile(r"^test (.+) \.\.\. ok$", re.MULTILINE)
TEST_FAILED = re.compile(r"^test (.+) \.\.\. FAILED$", re.MULTILINE)
OK_SUMMARY = "test result: ok. 1 passed; 0 failed;"
FAILED_SUMMARY = "test result: FAILED. 0 passed; 1 failed;"
MIN_AVAILABLE_BYTES = 10 * 1024**3
STOP_AVAILABLE_BYTES = 2 * 1024**3
RESOURCE_SAMPLE_SECONDS = 5
CAPACITY_FAILURE_MARKERS = ("no space left on device", "disk quota exceeded", "enospc")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def peak_rss_bytes(value: int) -> int:
    return value if sys.platform == "darwin" else value * 1024


def source_fingerprint_matches(path: Path, expected: str) -> bool:
    return digest(path.read_bytes()) == expected


def command(argv: list[str], cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, cwd=cwd, env=env, text=True, capture_output=True, check=False)


def directory_bytes(path: Path) -> int:
    total = 0
    pending = [path]
    while pending:
        current = pending.pop()
        if not current.exists():
            continue
        with os.scandir(current) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
    return total


def filesystem_usage(paths: dict[str, Path]) -> list[dict]:
    filesystems = {}
    for role, path in paths.items():
        path.mkdir(parents=True, exist_ok=True)
        stat = path.stat()
        usage = os.statvfs(path)
        filesystem = filesystems.setdefault(stat.st_dev, {
            "device": str(stat.st_dev),
            "total_bytes": usage.f_blocks * usage.f_frsize,
            "free_bytes": usage.f_bfree * usage.f_frsize,
            "available_bytes": usage.f_bavail * usage.f_frsize,
            "paths": [],
        })
        filesystem["paths"].append(role)
    return sorted(filesystems.values(), key=lambda item: item["device"])


def disk_snapshot(paths: dict[str, Path]) -> dict:
    return {
        "filesystems": filesystem_usage(paths),
        "directory_bytes": {role: directory_bytes(path) for role, path in paths.items()},
    }


def minimum_available_bytes(filesystems: list[dict]) -> int:
    return min((filesystem["available_bytes"] for filesystem in filesystems), default=0)


def preflight_disk_space(target: Path, paths: dict[str, Path], minimum_bytes: int) -> dict:
    before = disk_snapshot(paths)
    pruned_bytes = 0
    if minimum_available_bytes(before["filesystems"]) < minimum_bytes:
        pruned_bytes = directory_bytes(target)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
    after = disk_snapshot(paths)
    return {
        "minimum_available_bytes": minimum_bytes,
        "before": before,
        "target_pruned_bytes": pruned_bytes,
        "after": after,
        "qualified": minimum_available_bytes(after["filesystems"]) >= minimum_bytes,
    }


def sample_disk(paths: dict[str, Path], report: dict) -> None:
    for filesystem in filesystem_usage(paths):
        device = filesystem["device"]
        report["minimum_available_bytes_by_device"][device] = min(
            filesystem["available_bytes"],
            report["minimum_available_bytes_by_device"].get(device, filesystem["available_bytes"]),
        )
        used = filesystem["total_bytes"] - filesystem["free_bytes"]
        report["peak_used_bytes_by_device"][device] = max(
            used, report["peak_used_bytes_by_device"].get(device, used)
        )
        if filesystem["available_bytes"] < STOP_AVAILABLE_BYTES:
            report["low_space_stop"].set()
    report["samples"] += 1


def monitor_disk(paths: dict[str, Path], stop: threading.Event, low_space: threading.Event, report: dict) -> None:
    while not stop.wait(RESOURCE_SAMPLE_SECONDS):
        try:
            sample_disk(paths, report)
        except OSError as error:
            report["telemetry_error"] = str(error)
            low_space.set()
            return


def require_clean_checkout() -> None:
    result = command(["git", "status", "--porcelain", "--untracked-files=all"], ROOT, os.environ.copy())
    if result.returncode != 0 or result.stdout:
        raise RuntimeError("mutation lane requires a clean source checkout")


def validate_inventory(inventory: dict, head: str) -> None:
    cases = inventory.get("cases")
    if not isinstance(cases, list) or len(cases) != inventory.get("max_cases_per_run"):
        raise RuntimeError("candidate count does not match the reviewed per-run bound")
    if not cases or len({case.get("id") for case in cases}) != len(cases):
        raise RuntimeError("candidate inventory is empty or contains duplicate IDs")
    ancestor = command(
        ["git", "merge-base", "--is-ancestor", inventory["reviewed_source_revision"], head],
        ROOT,
        os.environ.copy(),
    )
    if ancestor.returncode != 0:
        raise RuntimeError("current source is not descended from the reviewed mutation source")
    for relative, expected in inventory["source_files"].items():
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            raise RuntimeError(f"invalid source fingerprint path: {relative}")
        if not source_fingerprint_matches(ROOT / path, expected):
            raise RuntimeError(f"stale candidate or detector source: {relative}")
    for case in cases:
        if not re.fullmatch(r"[A-Z0-9-]+", case["id"]):
            raise RuntimeError(f"invalid mutation ID: {case['id']}")
        source = Path(case["source_path"])
        if source.is_absolute() or ".." in source.parts or case["source_path"] not in inventory["source_files"]:
            raise RuntimeError(f"{case['id']}: mutation source is not fingerprinted")
        if not case["before"] or not case["test_command"] or not case["test_id"]:
            raise RuntimeError(f"{case['id']}: missing exact mutation or detector")
        if case["test_command"][0:2] != ["cargo", "test"] or any(
            flag not in case["test_command"] for flag in ("--locked", "--offline")
        ):
            raise RuntimeError(f"{case['id']}: detector must use locked offline Cargo tests")
        if case["test_command"][-2:] != ["--", "--exact"] or case["test_id"] not in case["test_command"][:-2]:
            raise RuntimeError(f"{case['id']}: detector test ID is not exact")


def baseline_passed(status: int | None, output: str, test_id: str) -> bool:
    return status == 0 and TEST_OK.findall(output) == [test_id] and OK_SUMMARY in output


def apply_mutation(text: str, case: dict) -> str:
    before = "\n".join(case["before"])
    after = "\n".join(case["after"])
    if text.count(before) != 1 or before == after:
        raise RuntimeError(f"{case['id']}: exact mutation source is missing or ambiguous")
    return text.replace(before, after, 1)


def classify_mutant(status: int | None, output: str, case: dict) -> str:
    if any(marker in output.lower() for marker in CAPACITY_FAILURE_MARKERS):
        return "disk_full_not_qualified"
    if status is None:
        return "timeout"
    if status == 0:
        if TEST_OK.findall(output) == [case["test_id"]] and OK_SUMMARY in output:
            return "survivor"
        return "test_not_run"
    if TEST_FAILED.findall(output) != [case["test_id"]] or FAILED_SUMMARY not in output:
        return "build_or_harness_failure"
    if case["expected_failure_marker"] not in output:
        return "unintended_failure"
    return "caught_by_intended_assertion"


def terminate_process(process: subprocess.Popen) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            process.kill()
        process.wait()


def run_test(
    case: dict,
    cwd: Path,
    env: dict[str, str],
    log: Path,
    timeout_seconds: int,
    low_space: threading.Event | None = None,
) -> tuple[dict, str]:
    started = time.monotonic()
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    timed_out = False
    resource_stop = False
    status = None
    test_env = env.copy()
    test_env["ROCKSTREAM_CHAOS_ARTIFACT_DIR"] = str(
        log.with_name(f"{log.stem}-artifacts").resolve()
    )
    with log.open("wb") as output:
        process = subprocess.Popen(
            case["test_command"],
            cwd=cwd,
            env=test_env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=(os.name == "posix"),
        )
        deadline = started + timeout_seconds
        while process.poll() is None:
            if low_space is not None and low_space.is_set():
                resource_stop = True
                terminate_process(process)
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                terminate_process(process)
                break
            try:
                status = process.wait(timeout=min(0.5, remaining))
            except subprocess.TimeoutExpired:
                continue
        if status is None and not timed_out and not resource_stop:
            status = process.returncode
    text = log.read_text(errors="replace")
    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    return ({
        "exit_code": status,
        "timed_out": timed_out,
        "resource_stop": resource_stop,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "child_cpu_user_seconds": round(usage_after.ru_utime - usage_before.ru_utime, 3),
        "child_cpu_system_seconds": round(usage_after.ru_stime - usage_before.ru_stime, 3),
        "child_peak_rss_bytes_high_water": peak_rss_bytes(usage_after.ru_maxrss),
        "log": str(Path("logs") / log.name),
        "log_sha256": digest(log.read_bytes()),
    }, text)


def run_case(
    case: dict,
    candidate: Path,
    env: dict[str, str],
    logs: Path,
    timeout_seconds: int,
    low_space: threading.Event | None = None,
) -> dict:
    source = candidate / case["source_path"]
    original = source.read_bytes()
    text = original.decode()

    record = {
        "id": case["id"],
        "severity": case["severity"],
        "invariant": case["invariant"],
        "test_id": case["test_id"],
        "command": case["test_command"],
        "source_path": case["source_path"],
        "source_sha256": digest(original),
    }
    baseline, output = run_test(
        case, candidate, env, logs / f"{case['id']}-baseline-before.log", timeout_seconds, low_space
    )
    record["baseline_before"] = baseline
    if baseline["resource_stop"]:
        record["classification"] = "resource_limit_not_qualified"
        return record
    if any(marker in output.lower() for marker in CAPACITY_FAILURE_MARKERS):
        record["classification"] = "disk_full_not_qualified"
        return record
    if not baseline_passed(baseline["exit_code"], output, case["test_id"]):
        record["classification"] = "baseline_failed_or_test_not_run"
        return record

    mutant_text = apply_mutation(text, case)
    mutant_bytes = mutant_text.encode()
    source.write_bytes(mutant_bytes)
    diff = "".join(difflib.unified_diff(
        text.splitlines(keepends=True),
        mutant_text.splitlines(keepends=True),
        fromfile=case["source_path"],
        tofile=f"{case['source_path']}[{case['id']}]",
    ))
    diff_path = logs / f"{case['id']}.diff"
    diff_path.write_text(diff)
    record["mutation_diff"] = str(Path("logs") / diff_path.name)
    record["mutation_diff_sha256"] = digest(diff.encode())
    record["mutant_source_sha256"] = digest(mutant_bytes)
    try:
        mutant, output = run_test(
            case, candidate, env, logs / f"{case['id']}-mutant.log", timeout_seconds, low_space
        )
    finally:
        source.write_bytes(original)
    if digest(source.read_bytes()) != record["source_sha256"]:
        raise RuntimeError(f"{case['id']}: source was not restored exactly")
    record["mutant"] = mutant
    record["classification"] = classify_mutant(mutant["exit_code"], output, case)
    if mutant["resource_stop"]:
        record["classification"] = "resource_limit_not_qualified"
        return record
    if record["classification"] == "disk_full_not_qualified":
        return record
    baseline, output = run_test(
        case, candidate, env, logs / f"{case['id']}-baseline-after.log", timeout_seconds, low_space
    )
    record["baseline_after"] = baseline
    if baseline["resource_stop"]:
        record["classification"] = "resource_limit_not_qualified"
        return record
    if any(marker in output.lower() for marker in CAPACITY_FAILURE_MARKERS):
        record["classification"] = "disk_full_not_qualified"
        return record
    if not baseline_passed(baseline["exit_code"], output, case["test_id"]):
        record["classification"] = "restored_baseline_failed_or_test_not_run"
    return record


def render_summary(report: dict) -> str:
    lines = [
        "# Bounded safety mutation run",
        "",
        f"Status: {report['status']}",
        f"Source SHA: {report.get('source_sha', 'unavailable')}",
        f"Cases selected: {report.get('selected_count', 0)}",
        f"Wall seconds: {report.get('run_seconds', 0)}",
        f"Targeted test seconds: {report.get('tested_wall_seconds', 0)}",
        f"Setup and verification seconds: {report.get('setup_verification_seconds', 0)}",
        f"Mutant total/median seconds: {report.get('mutant_wall_total_seconds', 0)} / {report.get('mutant_wall_median_seconds', 'n/a')}",
        f"Child CPU seconds: {report.get('child_cpu_seconds', 0)}",
        f"Peak child RSS bytes: {report.get('peak_child_rss_bytes', 0)}",
        f"Minimum available bytes by filesystem: {report.get('disk_samples', {}).get('minimum_available_bytes_by_device', {})}",
        f"Peak sampled filesystem use bytes: {report.get('disk_samples', {}).get('peak_used_bytes_by_device', {})}",
        f"Preflight filesystem bytes: {json.dumps(report.get('disk_preflight', {}).get('before', {}).get('filesystems', []), sort_keys=True)}",
        f"End filesystem bytes: {json.dumps(report.get('resource_end', {}).get('filesystems', []), sort_keys=True)}",
        f"Preflight path bytes: {json.dumps(report.get('disk_preflight', {}).get('before', {}).get('directory_bytes', {}), sort_keys=True)}",
        f"End path bytes: {json.dumps(report.get('resource_end', {}).get('directory_bytes', {}), sort_keys=True)}",
        f"Candidate copy bytes: {report.get('candidate_copy_bytes', 0)}",
        "Candidate inventory: [candidate-inventory.json](candidate-inventory.json)",
        "",
        "| Mutation | Invariant | Severity | Test | Result | Mutant seconds | Diff | Log |",
        "| --- | --- | --- | --- | --- | ---: | --- | --- |",
    ]
    for case in report.get("cases", []):
        mutant = case.get("mutant", {})
        diff = case.get("mutation_diff", "not-run")
        log = mutant.get("log", "not-run")
        lines.append(
            f"| {case['id']} | {case['invariant']} | {case['severity']} | {case['test_id']} | "
            f"{case['classification']} | {mutant.get('elapsed_seconds', 'n/a')} | "
            f"[{diff}]({diff}) | [{log}]({log}) |"
        )
    for item in report.get("uncovered_invariants", []):
        lines.append(f"| {item['id']} | {item['id']} | {item['severity']} | not selected | {item['status']} | n/a | n/a | n/a |")
    lines.extend(["", "This bounded result is not an end-to-end durability qualification.", ""])
    return "\n".join(lines)


def run_lane(output: Path) -> int:
    started = time.monotonic()
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    report = {
        "status": "not_qualified",
        "source_sha": None,
        "selected_count": 0,
        "cases": [],
        "uncovered_invariants": [],
        "errors": [],
        "mutation_method": "Single-occurrence replacements in a temporary repository copy, restored byte-for-byte.",
        "source_clean_before": None,
        "source_clean_after": None,
        "resource_method": "resource.getrusage(RUSAGE_CHILDREN); peak RSS is a high-water mark.",
        "disk_resource_method": "5-second filesystem sampling; free-byte high-water is runner-wide per involved device.",
        "runner_os": os.environ.get("RUNNER_OS", "local"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID"),
        "workflow_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "python": sys.version.split()[0],
        "runner_sha256": digest(Path(__file__).read_bytes()),
    }
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        report["errors"].append("output directory was not empty; stale artifacts rejected")
        report["run_seconds"] = round(time.monotonic() - started, 3)
        usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        report["child_cpu_seconds"] = round(
            usage_after.ru_utime - usage_before.ru_utime + usage_after.ru_stime - usage_before.ru_stime, 3
        )
        report["peak_child_rss_bytes"] = peak_rss_bytes(usage_after.ru_maxrss)
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        (output / "summary.md").write_text(render_summary(report))
        return 1
    logs = output / "logs"
    logs.mkdir()
    target = ROOT / "target/safety-mutation-lane"
    scratch = Path(os.environ.get("ROCKSTREAM_MUTATION_TMP", Path(tempfile.gettempdir()) / "safety-mutation-work"))
    paths = {"cargo_target": target, "candidate_scratch": scratch, "output": output.resolve()}
    disk_samples = {
        "sample_interval_seconds": RESOURCE_SAMPLE_SECONDS,
        "samples": 0,
        "minimum_available_bytes_by_device": {},
        "peak_used_bytes_by_device": {},
        "low_space_stop": threading.Event(),
        "telemetry_error": None,
    }
    low_space = threading.Event()
    monitor_stop = threading.Event()
    monitor_thread = None
    try:
        require_clean_checkout()
        report["source_clean_before"] = True
        inventory = json.loads(INVENTORY.read_text())
        report["inventory_sha256"] = digest(INVENTORY.read_bytes())
        shutil.copyfile(INVENTORY, output / "candidate-inventory.json")
        report["reviewed_source_revision"] = inventory["reviewed_source_revision"]
        report["selected_count"] = len(inventory.get("cases", []))
        report["uncovered_invariants"] = inventory.get("uncovered_invariants", [])
        head = command(["git", "rev-parse", "HEAD"], ROOT, os.environ.copy())
        if head.returncode:
            raise RuntimeError("could not identify source revision")
        report["source_sha"] = head.stdout.strip()
        validate_inventory(inventory, report["source_sha"])
        report["rust_toolchain_expected"] = inventory["rust_toolchain"]
        rustc = command(["rustc", "--version"], ROOT, os.environ.copy())
        cargo = command(["cargo", "--version"], ROOT, os.environ.copy())
        if rustc.returncode or cargo.returncode:
            raise RuntimeError("pinned Rust toolchain is unavailable")
        report["rustc"] = rustc.stdout.strip()
        report["cargo"] = cargo.stdout.strip()
        if not report["rustc"].startswith(f"rustc {inventory['rust_toolchain']}."):
            raise RuntimeError("rustc version does not match the reviewed toolchain")
        if not report["cargo"].startswith(f"cargo {inventory['rust_toolchain']}."):
            raise RuntimeError("cargo version does not match the reviewed toolchain")
        env = os.environ.copy()
        env.update({
            "CARGO_TERM_COLOR": "never",
            "CARGO_PROFILE_DEV_DEBUG": "0",
            "CARGO_PROFILE_TEST_DEBUG": "0",
            "CARGO_BUILD_JOBS": "2",
            "ROCKSTREAM_QUALIFICATION_REVISION": report["source_sha"],
        })
        configured_target = Path(env.get("CARGO_TARGET_DIR", target)).absolute()
        if target.is_symlink() or configured_target != target.absolute():
            raise RuntimeError("CARGO_TARGET_DIR must be the lane-owned target/safety-mutation-lane directory")
        target.mkdir(parents=True, exist_ok=True)
        env["CARGO_TARGET_DIR"] = str(target)
        scratch.mkdir(parents=True, exist_ok=True)
        sample_disk(paths, disk_samples)
        report["disk_preflight"] = preflight_disk_space(target, paths, MIN_AVAILABLE_BYTES)
        if not report["disk_preflight"]["qualified"]:
            available = minimum_available_bytes(report["disk_preflight"]["after"]["filesystems"])
            raise RuntimeError(
                f"disk_full_not_qualified: {available} available bytes after pruning the owned target; "
                f"{MIN_AVAILABLE_BYTES} required"
            )
        with tempfile.TemporaryDirectory(prefix="rockstream-safety-mutation-", dir=scratch) as temp:
            candidate = Path(temp) / "candidate"
            shutil.copytree(
                ROOT,
                candidate,
                ignore=shutil.ignore_patterns(".git", "target", ".cache", "openwiki", ".openwiki"),
            )
            report["candidate_copy_bytes"] = directory_bytes(candidate)
            sample_disk(paths, disk_samples)
            monitor_thread = threading.Thread(
                target=monitor_disk, args=(paths, monitor_stop, low_space, disk_samples), daemon=True
            )
            monitor_thread.start()
            for case in inventory["cases"]:
                record = run_case(
                    case, candidate, env, logs, inventory["command_timeout_seconds"], low_space
                )
                report["cases"].append(record)
                if record["classification"] != "caught_by_intended_assertion":
                    report["errors"].append(f"{case['id']}: {record['classification']}")
                if low_space.is_set():
                    if record["classification"] == "caught_by_intended_assertion":
                        record["classification"] = "resource_limit_not_qualified"
                    reason = disk_samples["telemetry_error"] or "available space reached the 2 GiB reserve"
                    report["errors"].append(f"resource_limit_not_qualified: {reason}")
                    break
        require_clean_checkout()
        report["source_clean_after"] = True
        if (
            len(report["cases"]) == report["selected_count"]
            and all(case["classification"] == "caught_by_intended_assertion" for case in report["cases"])
            and not report["errors"]
        ):
            report["status"] = "partial_evidence_not_qualification"
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        message = str(error)
        if any(marker in message.lower() for marker in CAPACITY_FAILURE_MARKERS):
            message = f"disk_full_not_qualified: {message}"
        report["errors"].append(message)
    finally:
        if monitor_thread is not None:
            monitor_stop.set()
            monitor_thread.join()
            try:
                sample_disk(paths, disk_samples)
            except OSError as error:
                report["errors"].append(f"disk telemetry failed: {error}")
        disk_samples["low_space_stop"] = disk_samples["low_space_stop"].is_set()
        report["disk_samples"] = disk_samples
        if paths["cargo_target"].exists() and paths["candidate_scratch"].exists():
            try:
                report["resource_end"] = disk_snapshot(paths)
            except OSError as error:
                report["errors"].append(f"disk telemetry failed: {error}")
        if disk_samples.get("telemetry_error"):
            report["errors"].append(f"disk telemetry failed: {report['disk_samples']['telemetry_error']}")
        if disk_samples["low_space_stop"] and not any(
            error.startswith("resource_limit_not_qualified:") for error in report["errors"]
        ):
            report["errors"].append("resource_limit_not_qualified: available space reached the 2 GiB reserve")
    if report["errors"]:
        report["status"] = "not_qualified"
    report["run_seconds"] = round(time.monotonic() - started, 3)
    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    report["child_cpu_seconds"] = round(
        usage_after.ru_utime - usage_before.ru_utime + usage_after.ru_stime - usage_before.ru_stime, 3
    )
    report["peak_child_rss_bytes"] = peak_rss_bytes(usage_after.ru_maxrss)
    test_runs = [
        run
        for case in report["cases"]
        for run in (case.get("baseline_before"), case.get("mutant"), case.get("baseline_after"))
        if run is not None
    ]
    report["tested_wall_seconds"] = round(sum(run["elapsed_seconds"] for run in test_runs), 3)
    report["setup_verification_seconds"] = round(
        max(0, report["run_seconds"] - report["tested_wall_seconds"]), 3
    )
    mutant_runs = [case["mutant"]["elapsed_seconds"] for case in report["cases"] if "mutant" in case]
    report["mutant_wall_total_seconds"] = round(sum(mutant_runs), 3)
    report["mutant_wall_median_seconds"] = round(statistics.median(mutant_runs), 3) if mutant_runs else None
    report["mutant_repetitions_per_case"] = 1
    (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    (output / "summary.md").write_text(render_summary(report))
    return 0 if report["status"] == "partial_evidence_not_qualification" else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    return run_lane(parser.parse_args().output_dir)


if __name__ == "__main__":
    sys.exit(main())

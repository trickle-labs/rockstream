"""Execute the fixed qualification checks and report only observed libtest results."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHECKS = {
    "e2e_qualification_tests": ("rockstream-sim", (
        "test_helper_in_memory_topology_lifecycle",
        "test_helper_generated_kafka_oracle_bookkeeping",
        "test_helper_generated_cdc_commit_filtering",
        "test_helper_oracle_sink_history_comparison",
        "test_helper_supplied_relation_comparison",
        "test_helper_aggregate_oracle_comparison",
    )),
    "qualification_recovery_tests": ("rockstream-sim", (
        "test_helper_observer_heartbeat_loss_and_shard_reassignment",
        "test_helper_observer_control_leader_failover",
        "test_helper_observer_fencing_epoch_advancement",
        "test_helper_observer_selected_checkpoint_recovery",
        "test_helper_observer_source_offset_lsn_recovery",
        "test_helper_observer_view_frontier_monotonicity",
        "test_helper_observer_sink_2pc_transaction_atomicity",
        "test_helper_observer_first_post_recovery_query_correctness",
    )),
    "qualification_harness_mutation_tests": ("rockstream-types", (
        "single_process_simulation_rejected",
        "duplicate_worker_id_rejected",
        "idle_worker_rejected",
        "unowned_shard_rejected",
        "generator_saturation_rejected",
        "sink_consumer_lag_rejected",
        "stale_oracle_result_rejected",
        "duplicate_or_lost_sink_output_rejected",
        "constant_timestamp_rejected",
        "skipped_workload_rejected",
        "environment_shift_rejected",
        "all_harness_mutations_fail_closed",
    )),
}
SUITES = {
    "all": tuple(CHECKS),
    "e2e": ("e2e_qualification_tests",),
    "recovery": ("qualification_recovery_tests", "qualification_harness_mutation_tests"),
    "mutations": ("qualification_recovery_tests", "qualification_harness_mutation_tests"),
}
# These mandatory claims remain unsupported by the single worker/Kafka/MinIO scenario.
LIVE_OBSERVATIONS = {
    "e2e_qualification_tests": "live::e2e",
}
APPLICATION_TARGET = "real_cluster_chaos_soak_tests"
APPLICATION_CASE = "worker_failure_recovery_from_view_and_kafka"
CONTROL_FAILURES = {
    "missing_event": "MissingObservation", "constant_timings": "Timings",
    "stale_query": "Progress", "changed_value": "ViewOutput",
    "duplicate_output": "SinkOutput", "helper_only_status": "Ownership",
}
UNSUPPORTED = ["general-kafka-recovery", "postgres-cdc", "distributed-control-ha", "pgwire-transport", "recovery-p99"]
CASE = re.compile(r"test (\S+) \.\.\. (ok|FAILED|ignored)(?:, .*)?")
SUMMARY = re.compile(
    r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; "
    r"(\d+) measured; (\d+) filtered out; finished in .+"
)


def execute(command, **kwargs):
    try:
        return subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False, **kwargs)
    except OSError as error:
        return subprocess.CompletedProcess(command, 127, "", str(error))


def parse_results(target, output, filtered=0):
    """Validate the complete libtest transcript, including its derived summary."""
    scenarios, errors, summaries, starts = [], [], [], []
    for line in output.splitlines():
        match = CASE.fullmatch(line)
        if match:
            name, status = match.groups()
            scenarios.append({"id": f"{target}::{name}", "status": {
                "ok": "passed", "FAILED": "failed", "ignored": "ignored",
            }[status]})
        elif line.startswith("test ") and not line.startswith("test result:"):
            errors.append(f"{target}: malformed result record: {line}")
        if line.startswith("test result:"):
            summaries.append(SUMMARY.fullmatch(line))
        if re.fullmatch(r"running \d+ tests?", line):
            starts.append(int(line.split()[1]))
    ids = [case["id"] for case in scenarios]
    if len(ids) != len(set(ids)):
        errors.append(f"{target}: duplicate result records")
    counts = [sum(case["status"] == status for case in scenarios)
              for status in ("passed", "failed", "ignored")]
    if (len(summaries) != 1 or summaries[0] is None
            or list(map(int, summaries[0].groups()[1:])) != counts + [0, filtered]
            or summaries[0][1] != ("FAILED" if counts[1] else "ok")
            or starts != [len(scenarios)]):
        errors.append(f"{target}: missing or inconsistent test summary")
    return sorted(scenarios, key=lambda case: case["id"]), errors


def make_report(targets, processes, candidate, settings, services=None, application=None):
    expected = sorted(f"{target}::{name}" for target in targets for name in CHECKS[target][1])
    needs_recovery = not settings["fast"] and "qualification_recovery_tests" in targets
    if needs_recovery:
        expected.append(f"{APPLICATION_TARGET}::{APPLICATION_CASE}")
    scenarios, errors = [], []
    for process in processes:
        scenarios.extend(process["scenarios"])
        errors.extend(process["errors"])
        if process["exit_code"]:
            errors.append(f"{process['target']}: test process exited {process['exit_code']}")
    observed = {case["id"] for case in scenarios}
    omissions = sorted(set(expected) - observed)
    executed = {case["id"] for case in scenarios if case["status"] != "ignored"}
    if not expected or not executed.intersection(expected):
        errors.append("zero mandatory execution")
    for case in scenarios:
        if case["status"] != "passed":
            errors.append(f"{case['id']}: {case['status']}")
    if not settings["fast"]:
        omissions.extend(LIVE_OBSERVATIONS[target] for target in targets if target in LIVE_OBSERVATIONS)
        if any(target in LIVE_OBSERVATIONS for target in targets):
            errors.append("live qualification observations are unavailable; helper checks cannot qualify a release")
        if needs_recovery and application is None:
            omissions.append("live::worker-failure-kafka-minio-v1")
        if application is not None:
            errors.extend(validate_application(application, candidate))
        for name, status in (services or {}).items():
            if status != "available":
                errors.append(f"required service unavailable: {name}")
    if omissions:
        errors.append("mandatory checks or observations omitted")
    return {
        "status": "FAILED" if errors else "PASSED",
        "evidence_kind": "helper" if settings["fast"] else "release",
        "release_qualified": not errors and needs_recovery,
        "qualification_scope": ["worker-failure-kafka-minio-v1"] if needs_recovery and not errors else [],
        "unsupported_claims": UNSUPPORTED,
        "application_proof": application,
        "candidate": candidate,
        "settings": settings,
        "services": services or {},
        "processes": [{**process, "evidence_kind": "application" if process["target"] == APPLICATION_TARGET
                       else "helper"} for process in processes],
        "scenarios": sorted(scenarios, key=lambda case: case["id"]),
        "scenarios_passed": sum(case["status"] == "passed" for case in scenarios),
        "scenarios_failed": sum(case["status"] == "failed" for case in scenarios),
        "scenarios_ignored": sum(case["status"] == "ignored" for case in scenarios),
        "mandatory_omissions": sorted(omissions),
        "mandatory_skipped": sum(case["id"] in expected and case["status"] == "ignored"
                                 for case in scenarios) + len(omissions),
        "errors": errors,
    }


def validate_application(records, candidate):
    """Check the complete records emitted and validated by this run's Rust process."""
    try:
        application = records["application"]
        observations = application["observations"]
        timings = {
            "detection": observations["detection"]["timestamp_ms"] - observations["fault_at_ms"],
            "ownership": observations["ownership_observed_at_ms"] - observations["fault_at_ms"],
            "query": observations["first_correct_query_at_ms"] - observations["fault_at_ms"],
        }
        expected = {"candidate": candidate, "observations": observations,
                    "recovery_durations_ms": timings, "status": "PASSED", "error": None}
        controls = {}
        for name, reason in CONTROL_FAILURES.items():
            corrupted = json.loads(json.dumps(observations))
            durations = dict(timings)
            if name == "missing_event":
                del corrupted["detection"]
            elif name == "constant_timings":
                durations = {key: 0 for key in timings}
            elif name == "stale_query":
                corrupted["query_frontier"] = 3
            elif name == "changed_value":
                corrupted["query_rows"] = ["1\twrong"]
            elif name == "duplicate_output":
                corrupted["sink_records"].append(observations["sink_records"][2])
            elif name == "helper_only_status":
                corrupted["after"]["process_id"] = observations["before"]["process_id"]
            controls[name] = dict(expected, observations=corrupted, recovery_durations_ms=durations,
                                  status="FAILED", error=reason)
        manifest = ROOT / "docs/testing/worker-failure-kafka-minio-v1.json"
        if (records != {"application": expected, "negative_controls": controls}
                or observations["revision"] != candidate["commit_sha"]
                or observations["manifest_sha256"] != hashlib.sha256(manifest.read_bytes()).hexdigest()):
            raise ValueError("inconsistent records")
    except (KeyError, TypeError, ValueError, IndexError, AttributeError):
        return ["invalid executed recovery qualification records"]
    return []


def candidate_identity():
    sha = execute(["git", "rev-parse", "HEAD"])
    diff = execute(["git", "diff", "HEAD", "--binary"])
    untracked = execute(["git", "ls-files", "--others", "--exclude-standard", "-z"])
    compiler = execute(["rustc", "--version"])
    if sha.returncode or diff.returncode or untracked.returncode or compiler.returncode:
        raise ValueError("cannot determine candidate identity")
    return {
        "commit_sha": sha.stdout.strip(),
        "compiler_version": compiler.stdout.strip(),
        "tracked_diff_sha256": hashlib.sha256(diff.stdout.encode()).hexdigest(),
        "untracked_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                             for name in untracked.stdout.split("\0") if name},
        "lockfile_sha256": hashlib.sha256((ROOT / "Cargo.lock").read_bytes()).hexdigest(),
    }


def check_services():
    # The scenario provisions Kafka and MinIO on dynamic ports; no ambient services count as proof.
    return {"docker": "available" if execute(["docker", "info"]).returncode == 0 else "unavailable"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="all")
    parser.add_argument("--fast", "--quick", action="store_true", help="helper checks only")
    parser.add_argument("--check-prerequisites", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.suite not in SUITES:
        print(f"RS-0001: Unknown qualification suite: {args.suite!r} (expected: all, e2e, recovery, mutations)", file=sys.stderr)
        return 1
    fast_env = {name: os.environ[name] for name in
                ("ROCKSTREAM_QUALIFY_FAST", "ROCKSTREAM_QUALIFY_MOCK_ENV") if os.environ.get(name)}
    settings = {"suite": args.suite, "fast": args.fast or bool(fast_env),
                "environment": {**fast_env, **{name: os.environ[name] for name in
                    ("RUSTUP_TOOLCHAIN", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET")
                    if name in os.environ}}, "test_threads": 1, "profile": "debug"}
    services = {} if settings["fast"] else check_services()
    if args.check_prerequisites:
        print(json.dumps({"evidence_kind": "prerequisites", "release_qualified": False,
                          "services": services}, sort_keys=True))
        return int(any(status != "available" for status in services.values()))
    try:
        candidate = candidate_identity()
    except (ValueError, OSError) as error:
        print(f"RS-0001: {error}", file=sys.stderr)
        return 1
    processes, application = [], None
    # An unavailable release service prevents compilation/execution, never a fast fallback.
    if all(status == "available" for status in services.values()):
        for target in SUITES[args.suite]:
            command = ["cargo", "test", "--locked", "-p", CHECKS[target][0], "--test", target,
                       "--", "--format=pretty", "--color=never", "--test-threads=1"]
            result = execute(command)
            scenarios, errors = parse_results(target, result.stdout)
            processes.append({"target": target, "command": command, "exit_code": result.returncode,
                              "scenarios": scenarios, "errors": errors})
            if result.returncode or errors:
                print(result.stdout + result.stderr, file=sys.stderr)
        if not settings["fast"] and "qualification_recovery_tests" in SUITES[args.suite]:
            artifact_root = args.output.parent if args.output else ROOT / "target/qualification"
            artifact_root.mkdir(parents=True, exist_ok=True)
            artifact_dir = Path(tempfile.mkdtemp(prefix="recovery-", dir=artifact_root)).resolve()
            command = ["cargo", "test", "--locked", "-p", "rockstream-sim",
                       "--features", "simulation,docker_tests", "--test", APPLICATION_TARGET,
                       APPLICATION_CASE, "--", "--exact", "--format=pretty", "--color=never", "--test-threads=1"]
            result = execute(command, env={**os.environ,
                "ROCKSTREAM_CHAOS_ARTIFACT_DIR": str(artifact_dir),
                "ROCKSTREAM_QUALIFICATION_CANDIDATE": json.dumps(candidate)})
            scenarios, errors = parse_results(APPLICATION_TARGET, result.stdout, filtered=1)
            try:
                application = json.loads((artifact_dir / "qualification-records.json").read_text())
            except (OSError, ValueError):
                errors.append("missing or malformed executed recovery qualification records")
            try:
                if candidate_identity() != candidate:
                    errors.append("candidate changed during qualification")
            except (ValueError, OSError):
                errors.append("cannot determine final candidate identity")
            processes.append({"target": APPLICATION_TARGET, "command": command,
                              "exit_code": result.returncode, "scenarios": scenarios, "errors": errors})
            if result.returncode or errors:
                print(result.stdout + result.stderr, file=sys.stderr)
    report = make_report(SUITES[args.suite], processes, candidate, settings, services, application)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")
    return int(report["status"] != "PASSED")


if __name__ == "__main__":
    sys.exit(main())

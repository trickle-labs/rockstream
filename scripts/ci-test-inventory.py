#!/usr/bin/env python3
"""Fail-closed Cargo target assignment and libtest execution accounting (#110).

Cargo metadata is the target oracle; compiled libtest JSON events are the case oracle.
A build, a --list result, or an ignored test is never credited as execution.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "scripts" / "ci-test-matrix.json"
SKIP_LINE = re.compile(r"(?im)^\s*(?:SKIP\b|SKIPPING\b|.*\bSKIP:\s*)")


def target_id(package: str, kind: str, name: str) -> str:
    return f"{package}/{kind}:{name}"


def case_id(package: str, kind: str, name: str, case: str) -> str:
    return f"{package}::{kind}/{name}::{case}"


def kind_of(target: dict) -> str | None:
    if not target.get("test", False):
        return None
    kinds = set(target.get("kind", []))
    if "test" in kinds:
        return "test"
    if "bin" in kinds:
        return "bin"
    if kinds & {"lib", "rlib", "proc-macro", "dylib", "cdylib"}:
        return "lib"
    return None  # benches, examples, custom-build and fuzzers aren't test harnesses


def cargo_metadata() -> dict:
    proc = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        cwd=ROOT, text=True, capture_output=True, check=True,
    )
    return json.loads(proc.stdout)


def inventory(metadata: dict) -> tuple[dict, dict]:
    members = set(metadata["workspace_members"])
    packages = {p["name"]: p for p in metadata["packages"] if p["id"] in members}
    targets = {}
    for name, package in packages.items():
        for target in package["targets"]:
            kind = kind_of(target)
            if kind is not None:
                key = target_id(name, kind, target["name"])
                if key in targets:
                    raise ValueError(f"duplicate Cargo target {key}")
                targets[key] = {"package": name, "kind": kind, "name": target["name"]}
    return packages, targets


def assignments(targets: dict, packages: dict, policy: dict) -> dict:
    errors = []
    owner = {}
    defined_jobs = set(policy["jobs"])
    declared_sim = {}
    for job, names in policy["sim_groups"].items():
        if job not in defined_jobs:
            errors.append(f"undefined simulation job {job}")
        for name in names:
            if name in declared_sim:
                errors.append(f"simulation target {name} assigned twice: {declared_sim[name]} and {job}")
            declared_sim[name] = job
    if policy["sim_lib_job"] not in defined_jobs:
        errors.append("undefined simulation lib job")
    configured_packages = set(policy["package_jobs"]) | {"rockstream-sim"}
    for p in sorted(set(packages) - configured_packages):
        errors.append(f"unassigned workspace package {p}")
    for p in sorted(configured_packages - set(packages)):
        errors.append(f"stale or unknown workspace package {p}")
    for name, job in policy["package_jobs"].items():
        if job not in defined_jobs:
            errors.append(f"undefined package job {job} for {name}")
    live_sim = set()
    for key, t in sorted(targets.items()):
        if t["package"] == "rockstream-sim":
            if t["kind"] == "test":
                live_sim.add(t["name"])
                job = declared_sim.get(t["name"])
            else:
                job = policy["sim_lib_job"]
        else:
            job = policy["package_jobs"].get(t["package"])
        if not job:
            errors.append(f"unassigned Cargo target {key}")
        else:
            owner[key] = job
            errors.extend(check_requirements(key, job, policy))
    for name in sorted(set(declared_sim) - live_sim):
        errors.append(f"stale simulation assignment for nonexistent Cargo target {name}")
    for job, spec in policy["jobs"].items():
        for name in spec.get("supplemental_targets", []):
            key = target_id("rockstream-sim", "test", name)
            if key not in targets:
                errors.append(f"{job}: supplemental target not discovered by Cargo: {key}")
            if job == owner.get(key):
                errors.append(f"{job}: supplemental target is already primary")
        for name in spec.get("cases", {}):
            if name not in spec.get("supplemental_targets", []):
                errors.append(f"{job}: case filter only allowed for supplemental target {name}")
    if errors:
        raise ValueError("\n".join(errors))
    return owner


def check_requirements(key: str, job: str, policy: dict) -> list[str]:
    pkg, rest = key.split("/", 1)
    spec = policy["jobs"][job]
    features = set(spec.get("package_features", {}).get(pkg, spec.get("features", [])))
    services = set(spec.get("services", []))
    required = {}
    for selector, values in policy.get("target_requirements", {}).items():
        if selector == key or (selector.endswith(":*") and key.startswith(selector[:-1])):
            for label in ("features", "services"):
                required.setdefault(label, set()).update(values.get(label, []))
    errors = []
    for feature in sorted(required.get("features", set()) - features):
        errors.append(f"{key}: {job} missing required feature {feature}")
    for service in sorted(required.get("services", set()) - services):
        errors.append(f"{key}: {job} missing required service {service}")
    return errors


def selected_targets(job: str, targets: dict, owner: dict, policy: dict) -> list[dict]:
    spec = policy["jobs"][job]
    supplement = {target_id("rockstream-sim", "test", n) for n in spec.get("supplemental_targets", [])}
    return [t for key, t in sorted(targets.items()) if owner.get(key) == job or key in supplement]


def observed_results(
    t: dict, listed: list[str], events: list[dict], waivers: dict,
    selected_cases: list[str] | None = None,
) -> tuple[list[dict], list[str], int]:
    """Compare discovered IDs with terminal libtest events, including ignored IDs."""
    pkg, kind, name = t["package"], t["kind"], t["name"]
    errors = []
    if len(listed) != len(set(listed)):
        errors.append("duplicate test IDs in --list")
    selected = set(selected_cases) if selected_cases is not None else set(listed)
    if not selected.issubset(set(listed)):
        errors.append(f"missing selected mandatory IDs: {sorted(selected - set(listed))}")
    if not selected and (kind == "test" or selected_cases is not None):
        errors.append("zero selected mandatory tests")
    terminal = {}
    for e in events:
        if e.get("type") != "test" or e.get("event") not in ("ok", "failed", "ignored"):
            continue
        test_name = e.get("name")
        if test_name in terminal:
            errors.append(f"duplicate terminal event for {test_name}")
        terminal[test_name] = e
        if test_name not in selected:
            errors.append(f"unselected test actually executed: {test_name}")
    tests = []
    mandatory_passed = 0
    for test_name in listed:
        cid = case_id(pkg, kind, name, test_name)
        event = terminal.get(test_name)
        item = {"id": cid, "outcome": "not_run", "reason": None}
        if test_name not in selected:
            item.update(outcome="not_selected", reason="Supplemental invocation: other cases owned by the primary target job")
        elif event is None:
            item.update(reason="libtest emitted no terminal execution event")
            errors.append(f"missing terminal execution event: {cid}")
        elif event["event"] == "ignored":
            if cid in waivers:
                item.update(outcome="limitation", reason=waivers[cid])
            else:
                item.update(outcome="unexpected_skip", reason="mandatory test is #[ignore]")
                errors.append(f"ignored mandatory case: {cid}")
        elif SKIP_LINE.search(event.get("stdout") or ""):
            item.update(outcome="unexpected_skip", reason="test body printed a SKIP marker despite libtest success")
            errors.append(f"unexpected in-test skip: {cid}")
        elif event["event"] == "failed":
            item.update(outcome="failed", reason=(event.get("stdout") or "test failed")[-1200:])
            errors.append(f"failed mandatory case: {cid}")
        else:
            item["outcome"] = "passed"
            mandatory_passed += 1
        tests.append(item)
    for extra in sorted(set(terminal) - set(listed)):
        errors.append(f"execution ID absent from --list: {extra}")
    if selected and not any(case_id(pkg, kind, name, test_name) not in waivers for test_name in selected):
        errors.append(f"zero selected mandatory tests: {pkg}/{kind}:{name}")
    return tests, errors, mandatory_passed


def parse_list(stdout: str) -> list[str]:
    return [line[:-6] for line in stdout.splitlines() if line.endswith(": test")]


def parse_events(stdout: str) -> list[dict]:
    events = []
    for line in stdout.splitlines():
        try:
            value = json.loads(line)
        except (ValueError, TypeError):
            continue
        if isinstance(value, dict) and value.get("type") in ("test", "suite"):
            events.append(value)
    return events


def run_cmd(argv: list[str], cwd: Path, env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(argv, cwd=cwd, env=env, text=True, capture_output=True, errors="replace")


def service_problems(spec: dict, env: dict) -> list[str]:
    problems = []
    for name in spec.get("required_env", []):
        if not env.get(name):
            problems.append(f"required environment variable {name} is not configured")
    if "docker" in spec.get("services", []):
        check = run_cmd(["docker", "info", "--format", "{{.ServerVersion}}"], ROOT, env)
        if check.returncode:
            problems.append("required Docker daemon is unavailable")
    return problems


def build_artifacts(package: dict, targets: list[dict], features: list[str], env: dict) -> dict:
    cmd = ["cargo", "test", "--locked", "-p", package["name"], "--no-run", "--message-format=json"]
    if features:
        cmd += ["--features", ",".join(features)]
    for t in targets:
        if t["kind"] == "test":
            cmd += ["--test", t["name"]]
        elif t["kind"] == "bin":
            cmd += ["--bin", t["name"]]
        else:
            cmd += ["--lib"]
    print("+", " ".join(cmd), flush=True)
    process = run_cmd(cmd, ROOT, env)
    if process.returncode:
        diagnostics = []
        for line in process.stdout.splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if item.get("reason") == "compiler-message":
                message = item.get("message", {})
                if message.get("level") == "error":
                    diagnostics.append(message.get("rendered") or message.get("message", ""))
        raise RuntimeError(
            f"Cargo failed for {package['name']}:\\n"
            + "\\n".join(diagnostics[-30:])
            + f"\\nstderr:\\n{process.stderr[-4000:]}"
        )
    got = {}
    for line in process.stdout.splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if entry.get("reason") != "compiler-artifact" or not entry.get("executable"):
            continue
        if not entry.get("profile", {}).get("test"):
            continue
        target = entry.get("target", {})
        kind = kind_of(target)
        if kind is None or entry.get("package_id") != package["id"]:
            continue
        key = target_id(package["name"], kind, target["name"])
        if key in got and got[key] != entry["executable"]:
            raise RuntimeError(f"more than one harness executable for {key}")
        got[key] = entry["executable"]
    expected = {target_id(t["package"], t["kind"], t["name"]) for t in targets}
    if set(got) != expected:
        raise RuntimeError(f"Cargo harness mismatch for {package['name']}: missing={sorted(expected-set(got))}, unexpected={sorted(set(got)-expected)}")
    return got


def run_job(job: str, packages: dict, targets: dict, owner: dict, policy: dict, out: Path) -> int:
    spec = policy["jobs"][job]
    selected = selected_targets(job, targets, owner, policy)
    result = {
        "schema_version": 1, "job": job, "tier": spec["tier"],
        "revision": os.environ.get("GITHUB_SHA", "local"),
        "targets": [], "errors": [], "status": "failed",
    }
    env = os.environ.copy()
    env["RUSTC_BOOTSTRAP"] = "1"  # matches existing PGWire libtest JSON CI invocation
    if "docker" in spec.get("services", []):
        env["ROCKSTREAM_REQUIRE_DOCKER"] = "1"
    result["features_and_services"] = {
        "services": spec.get("services", []),
        "package_features": spec.get("package_features", {}),
        "default_features": spec.get("features", []),
    }
    errors = result["errors"]
    if not selected:
        errors.append(f"zero Cargo targets selected by {job}")
    try:
        problems = service_problems(spec, env) if selected else []
        if problems:
            errors.extend(problems)
            for t in selected:
                result["targets"].append({
                    **t, "status": "blocked", "reason": "; ".join(problems),
                    "features": spec.get("package_features", {}).get(t["package"], spec.get("features", [])),
                    "services": spec.get("services", []), "tests": [],
                })
        else:
            by_package = defaultdict(list)
            for t in selected:
                by_package[t["package"]].append(t)
            passed = 0
            for package_name, pts in sorted(by_package.items()):
                features = spec.get("package_features", {}).get(package_name, spec.get("features", []))
                try:
                    harnesses = build_artifacts(packages[package_name], pts, features, env)
                except (RuntimeError, OSError) as exc:
                    errors.append(str(exc))
                    for t in pts:
                        result["targets"].append({**t, "status": "blocked", "reason": str(exc),
                                                  "features": features, "services": spec.get("services", []), "tests": []})
                    continue
                cwd = Path(packages[package_name]["manifest_path"]).parent
                for t in pts:
                    key = target_id(t["package"], t["kind"], t["name"])
                    executable = harnesses[key]
                    record = {**t, "features": features, "services": spec.get("services", []),
                              "executable": executable, "tests": [], "status": "failed"}
                    try:
                        listing = run_cmd([executable, "--list"], cwd, env)
                        if listing.returncode:
                            raise RuntimeError(f"--list failed: {listing.stderr[-1000:]}")
                        listed = parse_list(listing.stdout)
                        cases = spec.get("cases", {}).get(t["name"])
                        args = [executable, "-Z", "unstable-options", "--format", "json", "--test-threads=1"]
                        if cases is not None:
                            # Supplemental jobs select explicit expensive cases, one execution per ID.
                            events = []
                            nonzero = []
                            for case in cases:
                                proc = run_cmd(args + ["--exact", case], cwd, env)
                                events.extend(parse_events(proc.stdout))
                                if proc.returncode:
                                    nonzero.append(f"{case}: exit {proc.returncode} {proc.stderr[-500:]}")
                            execution_error = "; ".join(nonzero)
                        else:
                            proc = run_cmd(args, cwd, env)
                            events = parse_events(proc.stdout)
                            execution_error = f"libtest exit {proc.returncode}: {proc.stderr[-800:]}" if proc.returncode else ""
                        tests, violations, n_passed = observed_results(
                            t, listed, events, policy["known_limitations"], cases
                        )
                        record["tests"] = tests
                        if execution_error:
                            violations.append(execution_error)
                        if not listed and t["kind"] != "test":
                            record["reason"] = "Non-integration Cargo target exposes no libtest cases; compiled, not credited as tested"
                        record["status"] = "failed" if violations else ("no_cases" if not listed else "passed")
                        errors.extend(f"{key}: {v}" for v in violations)
                        passed += n_passed
                    except (RuntimeError, OSError) as exc:
                        record["reason"] = str(exc)
                        errors.append(f"{key}: {exc}")
                    result["targets"].append(record)
                    print(f"{key}: {record['status']}", flush=True)
            if not passed:
                errors.append("zero selected mandatory tests executed successfully")
    finally:
        result["status"] = "failed" if errors else "passed_with_limitations" if any(
            t.get("outcome") == "limitation" for r in result["targets"] for t in r.get("tests", [])
        ) else "passed"
        result["counts"] = {
            status: sum(t.get("outcome") == status for r in result["targets"] for t in r.get("tests", []))
            for status in ("passed", "failed", "limitation", "unexpected_skip", "not_run", "not_selected")
        }
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=2) + "\n")
        print(f"CI test evidence: {out} ({result['status']})", flush=True)
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
    return 1 if errors else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "run"))
    parser.add_argument("--job", help="CI job name when running tests")
    parser.add_argument("--out", type=Path, help="Write inventory or execution evidence JSON")
    args = parser.parse_args()
    policy = json.loads(POLICY.read_text())
    if policy["schema_version"] != 1:
        parser.error("unsupported test policy version")
    packages, targets = inventory(cargo_metadata())
    owner = assignments(targets, packages, policy)
    if args.action == "check":
        data = {"schema_version": 1, "status": "complete", "targets": [
            {**t, "owner": owner[key],
             "features": policy["jobs"][owner[key]].get("package_features", {}).get(t["package"], policy["jobs"][owner[key]].get("features", [])),
             "services": policy["jobs"][owner[key]].get("services", [])}
            for key, t in sorted(targets.items())
        ]}
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(data, indent=2) + "\n")
        print(f"PASS: {len(targets)} Cargo testable targets assigned, including {sum(t['package'] == 'rockstream-sim' and t['kind'] == 'test' for t in targets.values())} simulation integrations")
        return 0
    if not args.job or args.job not in policy["jobs"]:
        parser.error("run requires a defined --job")
    return run_job(args.job, packages, targets, owner, policy,
                   args.out or Path("target/ci-test-results") / (args.job + ".json"))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, subprocess.CalledProcessError, ValueError, KeyError) as exc:
        print(f"CI TEST INVENTORY ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

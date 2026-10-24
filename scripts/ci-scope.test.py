#!/usr/bin/env python3
"""Regression checks for CI selection, real Git diffs, and required job results."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("ci-scope.py")
spec = importlib.util.spec_from_file_location("ci_scope", SCRIPT)
scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope)
BASE = "a" * 40
HEAD = "b" * 40
EVENT = {"pull_request": {"base": {"sha": BASE}, "head": {"sha": HEAD}}}


def needs_for(lane):
    return {
        "ci-policy": {"result": "success", "outputs": {
            "scope": lane, "expensive": "false" if lane == "prose" else "true"}},
        "check-quality": {"result": "success"},
        "verus-verify": {"result": "success"},
        "nexmark-regression-gate": {"result": "success"},
        "benchmark": {"result": "success"},
        **{job: {"result": "skipped" if lane == "prose" else "success"}
           for job in scope.HEAVY_JOBS},
    }


class ScopeTests(unittest.TestCase):
    def test_matrices_reject_every_unexpected_result(self):
        for command, job, label in (
            ("check-benchmarks", "benchmark-suites", "Benchmark suites"),
            ("check-verus", "verus-qualify", "Verus phases"),
        ):
            for lane, expected in (("full", "success"), ("prose", "skipped")):
                classification = needs_for(lane)["ci-policy"]
                for result in ("success", "skipped", "failure", "cancelled", "", None):
                    needs = {"ci-policy": classification, job: {"result": result}}
                    with self.subTest(lane=lane, result=result):
                        if result == expected:
                            self.assertEqual(self.run_main(command, {"CI_NEEDS": json.dumps(needs)}),
                                             (0, f"{label} passed ({lane} CI); excluded jobs are not execution evidence.\n", ""))
                        else:
                            self.assertEqual(self.run_main(command, {"CI_NEEDS": json.dumps(needs)}),
                                             (1, "", f"::error::{job}: unexpected result {result!r} for {lane} CI\n"))
                valid = {"ci-policy": classification, job: {"result": expected}}
                for needs in ({"ci-policy": classification}, {**valid, "extra": {"result": "success"}}):
                    self.assertEqual(self.run_main(command, {"CI_NEEDS": json.dumps(needs)}),
                                     (1, "", "::error::CI dependency set differs from the required jobs\n"))
                valid["ci-policy"] = {"result": "cancelled", "outputs": classification["outputs"]}
                self.assertEqual(self.run_main(command, {"CI_NEEDS": json.dumps(valid)}),
                                 (1, "", "::error::CI classification did not succeed with a valid scope\n"))

    def run_main(self, command, env):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, env, clear=True), patch.object(sys, "argv", [str(SCRIPT), command]):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = scope.main()
        return code, stdout.getvalue(), stderr.getvalue()

    def test_allowlist(self):
        paths = sorted(scope.PROSE_FILES) + [
            ".github/prompts/plan.md", ".github/skills/example/SKILL.md",
            "docs/agents/domain.md", "docs/implementation-plans/v1.0.md",
            "sign-offs/v1.0.md",
        ]
        self.assertEqual(scope.scope_for_paths(paths), "prose")
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(scope.scope_for_paths([path]), "prose")

    def test_unknown_mixed_and_executable_documentation_require_full_ci(self):
        paths = [
            "README.md", "DESIGN.md", "docs/getting-started.md",
            "docs/reference/management-api.md", "docs/product-surface.json",
            "docs/testing/worker-failure-kafka-minio-v1.json",
            "crates/rockstream-cli/src/main.rs", "crates/example/README.md",
            "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", ".cargo/config.toml",
            "Dockerfile", "contracts/errors.toml", "formal/smoke.fizz",
            "scripts/ci-scope.py", ".github/workflows/ci.yml",
            ".github/skills/example/run.sh", "sign-offs/evidence.json",
            "docs/agents-other/example.md", "new-directory/file.md",
            "docs/agents/file.md\nCargo.toml",
        ]
        self.assertEqual(scope.scope_for_paths([]), "full")
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(scope.scope_for_paths([path]), "full")
                self.assertEqual(scope.scope_for_paths(["ROADMAP.md", path]), "full")

    def test_non_pr_events_always_run_full_ci(self):
        for event in ("push", "schedule", "workflow_dispatch", "", "merge_group"):
            with self.subTest(event=event):
                self.assertEqual(self.run_main("scope", {"GITHUB_EVENT_NAME": event}),
                                 (0, "scope=full\n", "CI scope: full\n"))

    def test_diff_failure_and_bad_event_fall_back_to_full_ci(self):
        env = {"GITHUB_EVENT_NAME": "pull_request", "GITHUB_EVENT_PATH": "event.json"}
        for event, error in ((EVENT, "git unavailable"),
                             ({"pull_request": {"base": {"sha": "--bad"}, "head": {"sha": HEAD}}},
                              "invalid PR commit identity")):
            with self.subTest(error=error), patch.object(Path, "read_text", return_value=json.dumps(event)):
                with patch.object(scope.subprocess, "run", side_effect=OSError("git unavailable")):
                    self.assertEqual(self.run_main("scope", env), (
                        0, "scope=full\n",
                        f"Cannot classify changes; using full CI: {error}\nCI scope: full\n",
                    ))
        self.assertEqual(self.run_main("scope", {"GITHUB_EVENT_NAME": "pull_request"}), (
            0, "scope=full\n",
            "Cannot classify changes; using full CI: 'GITHUB_EVENT_PATH'\nCI scope: full\n",
        ))

    def test_real_git_diff_covers_renames_deletions_and_all_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.run(
                    ["git", "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false",
                     "-c", "user.name=CI test", "-c", "user.email=ci-test@example.invalid", *args],
                    cwd=root, check=True, capture_output=True, text=True,
                ).stdout.strip()

            def commit(message):
                git("add", ".")
                git("commit", "-qm", message)
                self.assertEqual(git("log", "-1", "--format=%B"), message)
                return git("rev-parse", "HEAD")

            def classify(base, head):
                event = {"pull_request": {"base": {"sha": base}, "head": {"sha": head}}}
                event_path = root / "event.json"
                event_path.write_text(json.dumps(event))
                result = subprocess.run(
                    [sys.executable, "-B", str(SCRIPT), "scope"], cwd=root,
                    env={**os.environ, "GITHUB_EVENT_NAME": "pull_request",
                         "GITHUB_EVENT_PATH": str(event_path)}, capture_output=True, text=True,
                )
                event_path.unlink()
                return result.returncode, result.stdout, result.stderr

            git("init", "-q")
            (root / "README.md").write_text("executable transcript\n")
            (root / "ROADMAP.md").write_text("plan\n")
            base = commit("base")
            (root / "ROADMAP.md").write_text("updated plan\n")
            docs = commit("prose")
            self.assertEqual(classify(base, docs), (0, "scope=prose\n", "CI scope: prose\n"))
            self.assertEqual(classify(docs, docs), (0, "scope=full\n", "CI scope: full\n"))
            (root / "ROADMAP.md").unlink()
            deleted = commit("delete prose")
            self.assertEqual(classify(docs, deleted), (0, "scope=prose\n", "CI scope: prose\n"))
            (root / "README.md").rename(root / "ROADMAP.md")
            renamed = commit("rename executable docs to prose")
            self.assertEqual(classify(deleted, renamed), (0, "scope=full\n", "CI scope: full\n"))
            plans = root / "docs/implementation-plans"
            plans.mkdir(parents=True)
            for index in range(301):
                (plans / f"{index:03}.md").write_text("plan\n")
            (root / "zzz.rs").write_text("fn main() {}\n")
            many = commit("mixed changes beyond API file limits")
            self.assertEqual(classify(renamed, many), (0, "scope=full\n", "CI scope: full\n"))
            (plans / "line\nbreak.md").write_text("plan\n")
            newline = commit("prose filename with newline")
            self.assertEqual(classify(many, newline), (0, "scope=prose\n", "CI scope: prose\n"))

    def test_gate_success_outputs(self):
        for lane in ("full", "prose"):
            with self.subTest(lane=lane):
                message = f"Check passed ({lane} CI); excluded jobs are not execution evidence.\n"
                self.assertEqual(self.run_main("check", {"CI_NEEDS": json.dumps(needs_for(lane))}),
                                 (0, message, ""))

    def test_gate_rejects_failed_cancelled_or_unexpectedly_skipped_jobs(self):
        for lane in ("full", "prose"):
            for job in ("check-quality", "verus-verify", "nexmark-regression-gate", "benchmark", *scope.HEAVY_JOBS):
                for result in ("failure", "cancelled", "skipped", "", "pending"):
                    with self.subTest(lane=lane, job=job, result=result):
                        needs = needs_for(lane)
                        needs[job]["result"] = result
                        allowed_skip = lane == "prose" and job in scope.HEAVY_JOBS and result == "skipped"
                        if allowed_skip:
                            self.assertEqual(scope.check_results(needs),
                                             "Check passed (prose CI); excluded jobs are not execution evidence.")
                        else:
                            self.assertEqual(self.run_main("check", {"CI_NEEDS": json.dumps(needs)}), (
                                1, "", f"::error::{job}: unexpected result {result!r} for {lane} CI\n",
                            ))

    def test_gate_rejects_missing_extra_and_invalid_classification(self):
        for job in needs_for("full"):
            needs = needs_for("full")
            del needs[job]
            message = ("CI classification did not succeed with a valid scope" if job == "ci-policy"
                       else "CI dependency set differs from the required jobs")
            with self.subTest(missing=job):
                self.assertEqual(self.run_main("check", {"CI_NEEDS": json.dumps(needs)}),
                                 (1, "", f"::error::{message}\n"))
        needs = needs_for("prose")
        needs["extra"] = {"result": "success"}
        self.assertEqual(self.run_main("check", {"CI_NEEDS": json.dumps(needs)}),
                         (1, "", "::error::CI dependency set differs from the required jobs\n"))
        for classification in ({}, {"result": "failure", "outputs": {"scope": "prose"}},
                               {"result": "cancelled", "outputs": {"scope": "prose"}},
                               {"result": "success", "outputs": {"scope": "unknown"}}):
            needs = needs_for("prose")
            needs["ci-policy"] = classification
            with self.subTest(classification=classification):
                self.assertEqual(self.run_main("check", {"CI_NEEDS": json.dumps(needs)}),
                                 (1, "", "::error::CI classification did not succeed with a valid scope\n"))


if __name__ == "__main__":
    unittest.main()

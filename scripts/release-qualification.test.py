"""Exact report oracles and real libtest fixtures for qualification."""

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location("qualification", Path(__file__).with_name("release-qualification.py"))
Q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(Q)
REGISTERED_CHECKS = Q.CHECKS
SETTINGS = {"suite": "fixture", "fast": True, "environment": {}, "test_threads": 1, "profile": "debug"}
CANDIDATE = {"commit_sha": "a" * 40, "lockfile_sha256": "b" * 64,
             "tracked_diff_sha256": "c" * 64, "untracked_sha256": {}, "compiler_version": "rustc fixture"}
VALID = ("running 2 tests\ntest negative_control ... ok\ntest positive ... ok\n"
         "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s\n")
PASSED = (("negative_control", "passed"), ("positive", "passed"))


class QualificationTests(unittest.TestCase):
    def setUp(self):
        registry = patch.object(Q, "CHECKS", {"fixture": ("fixture-package", ("positive", "negative_control"))})
        registry.start()
        self.addCleanup(registry.stop)
        observations = patch.object(Q, "LIVE_OBSERVATIONS", {"fixture": "live::fixture"})
        observations.start()
        self.addCleanup(observations.stop)

    def expected(self, cases=(), errors=(), omissions=(), exit_code=0, process_errors=(),
                 processes=True, settings=None, services=None):
        settings = settings or SETTINGS
        scenarios = [{"id": f"fixture::{name}", "status": status} for name, status in sorted(cases)]
        return {
            "status": "FAILED" if errors else "PASSED",
            "evidence_kind": "helper" if settings["fast"] else "release",
            "release_qualified": False,
            "candidate": CANDIDATE,
            "settings": settings,
            "services": services or {},
            "processes": [{"target": "fixture", "command": ["fixture"], "exit_code": exit_code,
                           "scenarios": scenarios, "errors": list(process_errors)}] if processes else [],
            "scenarios": scenarios,
            "scenarios_passed": sum(status == "passed" for _, status in cases),
            "scenarios_failed": sum(status == "failed" for _, status in cases),
            "scenarios_ignored": sum(status == "ignored" for _, status in cases),
            "mandatory_omissions": list(omissions),
            "mandatory_skipped": len(omissions) + sum(status == "ignored" for _, status in cases),
            "errors": list(errors),
        }

    def report(self, output, exit_code=0, settings=None, services=None):
        scenarios, errors = Q.parse_results("fixture", output)
        process = {"target": "fixture", "command": ["fixture"], "exit_code": exit_code,
                   "scenarios": scenarios, "errors": errors}
        return Q.make_report(("fixture",), [process], CANDIDATE, settings or SETTINGS, services)

    def run_fixture(self, source):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "fixture.rs").write_text(source)
            build = subprocess.run(["rustc", "--test", str(path / "fixture.rs"), "-o", str(path / "fixture")],
                                   capture_output=True, text=True, check=False)
            self.assertEqual((build.returncode, build.stdout, build.stderr), (0, "", ""))
            return subprocess.run([str(path / "fixture"), "--format=pretty", "--color=never", "--test-threads=1"],
                                  capture_output=True, text=True, check=False)

    def test_empty_selection(self):
        self.assertEqual(Q.make_report((), [], CANDIDATE, SETTINGS),
                         self.expected(errors=("zero mandatory execution",), processes=False))

    def test_comment_only_input(self):
        result = self.run_fixture("// #[test] fn positive() {}\n// #[test] fn negative_control() {}\n")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.report(result.stdout), self.expected(
            errors=("zero mandatory execution", "mandatory checks or observations omitted"),
            omissions=("fixture::negative_control", "fixture::positive")))

    def test_registered_names_in_comments_cannot_satisfy_proof(self):
        with patch.object(Q, "CHECKS", REGISTERED_CHECKS):
            for target, (_, names) in REGISTERED_CHECKS.items():
                with self.subTest(target=target):
                    result = self.run_fixture("\n".join(f"// #[test] fn {name}() {{}}" for name in names))
                    self.assertEqual((result.returncode, result.stderr), (0, ""))
                    scenarios, errors = Q.parse_results(target, result.stdout)
                    process = {"target": target, "command": ["fixture"], "exit_code": 0,
                               "scenarios": scenarios, "errors": errors}
                    expected = self.expected(
                        errors=("zero mandatory execution", "mandatory checks or observations omitted"),
                        omissions=sorted(f"{target}::{name}" for name in names))
                    expected["processes"][0]["target"] = target
                    self.assertEqual(Q.make_report((target,), [process], CANDIDATE, SETTINGS), expected)

    def test_valid_run_executes_negative_control(self):
        result = self.run_fixture(
            "#[test] fn positive() { assert_eq!(2 + 2, 4); }\n"
            "#[test] fn negative_control() { assert!(\"bad\".parse::<u64>().is_err()); }\n")
        self.assertEqual((result.returncode, result.stderr), (0, ""))
        self.assertEqual(self.report(result.stdout), self.expected(cases=PASSED))

    def test_failed_negative_control(self):
        result = self.run_fixture(
            "#[test] fn positive() { assert_eq!(2 + 2, 4); }\n"
            "#[test] fn negative_control() { assert!(\"bad\".parse::<u64>().is_ok()); }\n")
        self.assertEqual(result.returncode, 101)
        self.assertEqual(self.report(result.stdout, result.returncode), self.expected(
            cases=(("negative_control", "failed"), ("positive", "passed")), exit_code=101,
            errors=("fixture: test process exited 101", "fixture::negative_control: failed")))

    def test_unexpected_skip(self):
        result = self.run_fixture(
            "#[test] fn positive() { assert_eq!(2 + 2, 4); }\n"
            "#[test] #[ignore] fn negative_control() {}\n")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self.report(result.stdout), self.expected(
            cases=(("negative_control", "ignored"), ("positive", "passed")),
            errors=("fixture::negative_control: ignored",)))

    def test_malformed_and_incomplete_records(self):
        for output, errors in (
            ("test positive ... banana\n", ("fixture: malformed result record: test positive ... banana",
                                             "fixture: missing or inconsistent test summary")),
            ("", ("fixture: missing or inconsistent test summary",)),
        ):
            with self.subTest(output=output):
                self.assertEqual(self.report(output), self.expected(
                    process_errors=errors,
                    errors=errors + ("zero mandatory execution", "mandatory checks or observations omitted"),
                    omissions=("fixture::negative_control", "fixture::positive")))

    def test_inconsistent_and_duplicate_records(self):
        prefix = "running 2 tests\ntest negative_control ... ok\ntest positive ... ok\n"
        for output, cases, errors in (
            (prefix + "test result: ok. 99 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s\n",
             PASSED, ("fixture: missing or inconsistent test summary",)),
            (prefix + "test positive ... ok\n", PASSED + (("positive", "passed"),),
             ("fixture: duplicate result records", "fixture: missing or inconsistent test summary")),
        ):
            with self.subTest(output=output):
                self.assertEqual(self.report(output), self.expected(cases=cases, errors=errors, process_errors=errors))

    def test_failed_process_with_passing_cases(self):
        self.assertEqual(self.report(VALID, 9), self.expected(
            cases=PASSED, exit_code=9, errors=("fixture: test process exited 9",)))

    def test_missing_mandatory_check(self):
        output = ("running 1 test\ntest positive ... ok\n"
                  "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0s\n")
        self.assertEqual(self.report(output), self.expected(
            cases=(("positive", "passed"),), omissions=("fixture::negative_control",),
            errors=("mandatory checks or observations omitted",)))

    def test_release_requires_live_observations_and_services(self):
        settings = dict(SETTINGS, fast=False)
        for services in ({"docker": "available", "kafka": "unavailable"}, {"docker": "available", "kafka": "available"}):
            with self.subTest(services=services):
                errors = ["live qualification observations are unavailable; helper checks cannot qualify a release"]
                if services["kafka"] == "unavailable":
                    errors.append("required service unavailable: kafka")
                errors.append("mandatory checks or observations omitted")
                self.assertEqual(self.report(VALID, settings=settings, services=services), self.expected(
                    cases=PASSED, settings=settings, services=services,
                    omissions=("live::fixture",), errors=errors))

    def test_all_ignored_is_zero_mandatory_execution(self):
        output = ("running 2 tests\ntest negative_control ... ignored\ntest positive ... ignored\n"
                  "test result: ok. 0 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0s\n")
        self.assertEqual(self.report(output), self.expected(
            cases=(("negative_control", "ignored"), ("positive", "ignored")),
            errors=("zero mandatory execution", "fixture::negative_control: ignored", "fixture::positive: ignored")))

    def test_unavailable_service_prevents_execution_and_writes_failed_report(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True), \
                patch.object(Q, "SUITES", {"fixture": ("fixture",)}), \
                patch.object(Q, "candidate_identity", return_value=CANDIDATE), \
                patch.object(Q, "check_services", return_value={"docker": "unavailable"}), \
                patch.object(Q, "execute") as execute:
            path = Path(directory) / "report.json"
            with patch.object(sys, "argv", ["qualification", "--suite", "fixture", "--output", str(path)]), \
                    contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(Q.main(), 1)
            expected = self.expected(
                settings=dict(SETTINGS, fast=False), services={"docker": "unavailable"}, processes=False,
                omissions=("fixture::negative_control", "fixture::positive", "live::fixture"),
                errors=("zero mandatory execution",
                        "live qualification observations are unavailable; helper checks cannot qualify a release",
                        "required service unavailable: docker", "mandatory checks or observations omitted"))
            self.assertEqual(path.read_text(), json.dumps(expected, indent=2, sort_keys=True) + "\n")
            self.assertEqual(stdout.getvalue(), path.read_text())
            execute.assert_not_called()

    def test_unknown_suite_fails_before_any_execution(self):
        for suite in ("nonexistent-audit-suite", ""):
            with self.subTest(suite=suite), patch.object(sys, "argv", ["qualification", "--suite", suite]), \
                    patch.object(Q, "execute") as execute, tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "report.json"
                sys.argv += ["--output", str(output)]
                stdout, stderr = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    self.assertEqual(Q.main(), 1)
                self.assertEqual((stdout.getvalue(), stderr.getvalue()),
                                 ("", f"RS-0001: Unknown qualification suite: {suite!r} (expected: all, e2e, recovery, mutations)\n"))
                execute.assert_not_called()
                self.assertFalse(output.exists())

    def test_entrypoint_writes_full_report_and_preserves_settings(self):
        command = ["cargo", "test", "--locked", "-p", "fixture-package", "--test", "fixture",
                   "--", "--format=pretty", "--color=never", "--test-threads=1"]
        for fast_option, environment in ((["--fast"], {}), ([], {"ROCKSTREAM_QUALIFY_MOCK_ENV": "1"}),
                                         ([], {"ROCKSTREAM_QUALIFY_FAST": "1"})):
            with self.subTest(environment=environment), tempfile.TemporaryDirectory() as directory, \
                    patch.object(Q, "SUITES", {"fixture": ("fixture",)}), \
                    patch.object(Q, "candidate_identity", return_value=CANDIDATE), \
                    patch.object(Q, "execute", return_value=subprocess.CompletedProcess(command, 0, VALID, "")) as execute, \
                    patch.dict(os.environ, environment, clear=True):
                path = Path(directory) / "nested" / "report.json"
                with patch.object(sys, "argv", ["qualification", "--suite", "fixture", *fast_option, "--output", str(path)]), \
                        contextlib.redirect_stdout(io.StringIO()) as stdout:
                    self.assertEqual(Q.main(), 0)
                expected = self.expected(cases=PASSED, settings=dict(SETTINGS, environment=environment))
                expected["processes"][0]["command"] = command
                self.assertEqual(path.read_text(), json.dumps(expected, indent=2, sort_keys=True) + "\n")
                self.assertEqual(stdout.getvalue(), path.read_text())
                execute.assert_called_once_with(command)


if __name__ == "__main__":
    unittest.main()

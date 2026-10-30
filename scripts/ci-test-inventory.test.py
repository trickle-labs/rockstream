#!/usr/bin/env python3
"""Negative-control tests for the fail-closed CI accounting gate (#110)."""
from __future__ import annotations
import copy
import contextlib
import importlib.util
import io
import json
import re
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch
from subprocess import CompletedProcess

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ci_test_inventory", ROOT / "scripts/ci-test-inventory.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
POLICY = json.loads((ROOT / "scripts/ci-test-matrix.json").read_text())


class GateSelfTests(unittest.TestCase):
    def test_gateway_conformance_feature_and_target_matrix(self):
        manifest = tomllib.loads((ROOT / "crates/rockstream-gateway/Cargo.toml").read_text())
        self.assertEqual(manifest["features"], {"testcontainers": []})
        self.assertEqual(POLICY["jobs"]["pr-gateway"]["features"], ["testcontainers"])
        self.assertEqual(POLICY["jobs"]["pr-gateway"]["services"], ["docker"])
        self.assertEqual(POLICY["jobs"]["pr-gateway"]["conformance_targets"], [
            "rockstream-gateway/test:" + name for name in (
                "auth_scram_tests", "gateway_dml_tests", "gateway_extended_query_tests",
                "gateway_integration_tests", "gateway_proof_tests", "golden_wire_tests",
                "listen_notify_tests", "transaction_savepoint_tests",
            )
        ])

    def setUp(self):
        self.target = {"package": "rockstream-sim", "kind": "test", "name": "chaos_tests"}
        self.key = gate.target_id(**self.target)

    def test_balanced_ownership_and_nightly_scale_preserve_pr_families(self):
        packages, targets = gate.inventory(gate.cargo_metadata())
        owner = gate.assignments(targets, packages, POLICY)
        self.assertEqual([gate.target_id(**t) for t in gate.selected_targets(
            "pr-gateway", targets, owner, POLICY)], sorted(
                POLICY["jobs"]["pr-gateway"]["conformance_targets"] +
                ["rockstream-gateway/test:conformance_doc_tests"]))
        for package, job in {
            "rockstream-oracle": "pr-oracle", "rockstream-ops": "pr-ops",
            "rockstream-storage": "pr-storage-sql", "rockstream-sql": "pr-storage-sql",
        }.items():
            self.assertEqual({k: v for k, v in owner.items() if k.startswith(package + "/")},
                             {k: job for k in targets if k.startswith(package + "/")})
        nightly = "rockstream-gateway/test:view_refresh_proportionality_tests"
        ordinary = "rockstream-gateway/test:view_refresh_proportionality_pr_tests"
        self.assertEqual({k: owner[k] for k in (nightly, ordinary)}, {
            nightly: "nightly-seeds", ordinary: "pr-gateway-components",
        })
        self.assertEqual(POLICY["jobs"]["nightly-seeds"]["package_features"], {"rockstream-gateway": []})
        self.assertEqual(POLICY["jobs"]["pr-gateway-components"], {
            "tier": "pull_request", "features": ["testcontainers"], "services": ["docker"],
        })
        for name, rows, chunk in ((nightly, "1_000_000", "50_000"), (ordinary, "10_000", "1_000")):
            source = (ROOT / "crates/rockstream-gateway/tests" / (name.split(":")[1] + ".rs")).read_text()
            self.assertIn('#[path = "support/view_refresh_proportionality.rs"]', source)
            self.assertEqual(re.findall(r"proportionality::(\w+)\(([^,]+), ([^)]+)\).await;", source),
                             [(family, rows, chunk) for family in (
                                 "stateless_project", "aggregate", "join", "session_window")])
        workflow = (ROOT / ".github/workflows/test-accounting-expensive.yml").read_text()
        self.assertIn('cron: "23 7 * * *"', workflow)
        self.assertIn("run: python3 scripts/ci-test-inventory.py run --job nightly-seeds", workflow)
        self.assertIn("name: nightly-seeds-test-evidence", workflow)

    def assertFails(self, fn, message):
        with self.assertRaisesRegex(ValueError, message):
            fn()

    def test_new_simulation_target_unassigned(self):
        policy = copy.deepcopy(POLICY)
        targets = {self.key: self.target, "rockstream-sim/test:new_regression": {
            "package": "rockstream-sim", "kind": "test", "name": "new_regression"
        }}
        # Other explicit assignments are removed to create an isolated synthetic metadata fixture.
        policy["sim_groups"] = {"pr-sim": ["chaos_tests"]}
        self.assertFails(lambda: gate.assignments(targets, {"rockstream-sim": {}}, policy), "unassigned Cargo target")

    def test_missing_required_feature(self):
        policy = copy.deepcopy(POLICY)
        policy["jobs"]["pr-sim"]["features"] = []
        self.assertTrue(any("missing required feature simulation" in err for err in
                            gate.check_requirements(self.key, "pr-sim", policy)))

    def test_missing_required_service(self):
        policy = copy.deepcopy(POLICY)
        key = "rockstream-sim/test:real_cluster_chaos_soak_tests"
        self.assertTrue(any("missing required service docker" in err for err in
                            gate.check_requirements(key, "pr-sim", policy)))

    def test_zero_selected_mandatory(self):
        tests, errors, passed = gate.observed_results(self.target, [], [], {})
        self.assertEqual(passed, 0)
        self.assertIn("zero selected mandatory tests", errors)

    def test_gateway_runs_required_postgres_differential_ignored_case(self):
        target = {"package": "rockstream-gateway", "kind": "test",
                  "name": "pg18_differential_conformance_tests"}
        key = gate.target_id(**target)
        self.assertEqual(POLICY["target_jobs"][key], "pr-postgres-differential")
        self.assertEqual(POLICY["jobs"]["pr-postgres-differential"]["features"], [])
        self.assertEqual(gate.libtest_flags(key, POLICY["jobs"]["pr-postgres-differential"]),
                         ["--include-ignored"])
        self.assertEqual(gate.libtest_flags(self.key, POLICY["jobs"]["pr-gateway"]), [])
        case = "pg18_reference::tests::pinned_postgres_numeric_and_array_corpus"
        events = [{"type": "test", "name": case, "event": "ok"}]
        results, errors, passed = gate.observed_results(target, [case], events, {})
        self.assertEqual(errors, [])
        self.assertEqual(passed, 1)
        self.assertEqual(results[0]["outcome"], "passed")

    def test_ignored_mandatory_case(self):
        events = [{"type": "test", "name": "must_run", "event": "ignored"}]
        tests, errors, passed = gate.observed_results(self.target, ["must_run"], events, {})
        self.assertEqual(tests[0]["outcome"], "unexpected_skip")
        self.assertTrue(any("ignored mandatory case" in e for e in errors))
        self.assertEqual(passed, 0)

    def test_unexpected_skip_despite_success_event(self):
        events = [{"type": "test", "name": "must_run", "event": "ok",
                   "stdout": "SKIP must_run: unavailable Docker"}]
        tests, errors, passed = gate.observed_results(self.target, ["must_run"], events, {})
        self.assertEqual(tests[0]["outcome"], "unexpected_skip")
        self.assertTrue(any("unexpected in-test skip" in e for e in errors))
        self.assertEqual(passed, 0)

    def test_ignored_declared_limitation_does_not_count(self):
        cid = gate.case_id("rockstream-sim", "test", "chaos_tests", "old_case")
        events = [{"type": "test", "name": "old_case", "event": "ignored"},
                  {"type": "test", "name": "mandatory", "event": "ok"}]
        tests, errors, passed = gate.observed_results(
            self.target, ["old_case", "mandatory"], events, {cid: "known unsupported transport"})
        self.assertEqual(errors, [])
        self.assertEqual([t["outcome"] for t in tests], ["limitation", "passed"])
        self.assertEqual(passed, 1)

    def test_post_ack_block_crash_case_is_qualified_outside_package_shard(self):
        target = {"package": "rockstream-storage", "kind": "test", "name": "enospc_recovery_tests"}
        case = "dm_log_writes_replays_the_exact_post_ack_epoch"
        ordinary_case = "enospc_withholds_ack_and_recovers_a_complete_epoch"
        cid = gate.case_id(**target, case=case)
        ordinary_cid = gate.case_id(**target, case=ordinary_case)
        reason = (
            "Requires Linux root and dm-log-writes device-mapper support; the dedicated "
            "post-ack-block-crash workflow runs this ignored case and captures source-bound "
            "recovery evidence, so the ordinary package shard does not execute it."
        )
        self.assertEqual(POLICY["known_limitations"][cid], reason)
        tests, errors, passed = gate.observed_results(
            target, [case, ordinary_case], [
                {"type": "test", "name": case, "event": "ignored"},
                {"type": "test", "name": ordinary_case, "event": "ok"},
            ], POLICY["known_limitations"],
        )
        self.assertEqual((tests, errors, passed), (
            [
                {"id": cid, "outcome": "limitation", "reason": reason},
                {"id": ordinary_cid, "outcome": "passed", "reason": None},
            ], [], 1,
        ))

    def test_empty_selection_with_only_waiver(self):
        cid = gate.case_id("rockstream-sim", "test", "chaos_tests", "old_case")
        events = [{"type": "test", "name": "old_case", "event": "ignored"}]
        _, errors, passed = gate.observed_results(
            self.target, ["old_case"], events, {cid: "known unsupported transport"})
        self.assertTrue(any("zero selected mandatory tests" in e for e in errors))
        self.assertEqual(passed, 0)

    def test_mandatory_missing_terminal_event(self):
        _, errors, passed = gate.observed_results(self.target, ["mandatory"], [], {})
        self.assertTrue(any("missing terminal execution event" in e for e in errors))
        self.assertEqual(passed, 0)

    def test_exact_ids_and_failures(self):
        events = [{"type": "test", "name": "case_a", "event": "ok"},
                  {"type": "test", "name": "case_b", "event": "failed", "stdout": "assertion failed"}]
        tests, errors, passed = gate.observed_results(self.target, ["case_a", "case_b"], events, {})
        self.assertEqual(tests[0]["id"], "rockstream-sim::test/chaos_tests::case_a")
        self.assertEqual(tests[1]["outcome"], "failed")
        self.assertTrue(any("failed mandatory case" in e for e in errors))
        self.assertEqual(passed, 1)

    def test_real_policy_covers_simulation_fixture(self):
        for target_list in POLICY["sim_groups"].values():
            self.assertEqual(len(target_list), len(set(target_list)))
        self.assertIn("dst_fuzz_harness", POLICY["sim_groups"]["nightly-seeds"])
        self.assertIn("rolling_upgrade_tests", POLICY["sim_groups"]["release-docker"])

    def test_results_parser(self):
        raw = '{"type":"test","event":"ok","name":"a"}\nnot json\n{"type":"suite","event":"ok"}\n'
        self.assertEqual(len(gate.parse_events(raw)), 2)
        self.assertEqual(gate.parse_list("a: test\nb: benchmark\n"), ["a"])

    def test_single_execution_supplies_both_reports_and_doc_gate(self):
        for event, returncode, expected_status in (
            ("ok", 0, "passed"), ("failed", 1, "failed"),
            (None, 0, "failed"), ("ignored", 0, "failed"), ("ok", 1, "failed"),
        ):
            with self.subTest(event=event, returncode=returncode), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                report = root / "target/pgwire-test-results.json"
                report.parent.mkdir()
                report.write_text("stale passing evidence")
                package = "rockstream-gateway"
                names = ["conformance_doc_tests", "wire_tests"]
                targets = {gate.target_id(package, "test", n): {
                    "package": package, "kind": "test", "name": n,
                } for n in names}
                wire_key = gate.target_id(package, "test", "wire_tests")
                spec = {"tier": "pull_request", "features": ["testcontainers"],
                        "conformance_targets": [wire_key], "services": [],
                        "test_threads": {wire_key: 4}}
                policy = {"jobs": {"fixture": spec}, "known_limitations": {}}
                events = [] if event is None else [{"type": "test", "name": "must_run", "event": event,
                                                   "exec_time": 1.25}]
                calls = []

                def run(argv, cwd, env):
                    calls.append(argv)
                    self.assertEqual(cwd, root)
                    self.assertEqual(env["CARGO_MANIFEST_DIR"], str(root))
                    if argv[1:] == ["--list"]:
                        return CompletedProcess(argv, 0, "must_run: test\n", "")
                    threads = 1 if argv[0] == "conformance_doc_tests" else 4
                    self.assertEqual(argv[1:], ["-Z", "unstable-options", "--format", "json", "--report-time", f"--test-threads={threads}"])
                    if argv[0] == "conformance_doc_tests":
                        self.assertEqual(report.read_text(), "".join(json.dumps(e) + "\n" for e in events))
                        output = [{"type": "test", "name": "must_run", "event": "ok"}]
                        return CompletedProcess(argv, 0, "\n".join(map(json.dumps, output)), "")
                    return CompletedProcess(argv, returncode, "\n".join(map(json.dumps, events)), "")

                out = root / "accounting.json"
                with patch.object(gate, "ROOT", root), patch.object(gate, "run_cmd", side_effect=run), \
                     patch.object(gate, "build_artifacts", return_value={k: t["name"] for k, t in targets.items()}) as build, \
                     patch.object(gate.time, "monotonic", return_value=1), \
                     contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    code = gate.run_job("fixture", {package: {"manifest_path": str(root / "Cargo.toml")}},
                                        targets, {k: "fixture" for k in targets}, policy, out)
                self.assertEqual(build.call_args.args[2], ["testcontainers"])
                self.assertEqual(calls, [
                    ["wire_tests", "--list"],
                    ["wire_tests", "-Z", "unstable-options", "--format", "json", "--report-time", "--test-threads=4"],
                    ["conformance_doc_tests", "--list"],
                    ["conformance_doc_tests", "-Z", "unstable-options", "--format", "json", "--report-time", "--test-threads=1"],
                ])
                result = json.loads(out.read_text())
                self.assertEqual(result["status"], expected_status)
                self.assertEqual(code, int(expected_status == "failed"))
                self.assertEqual(result["targets"][0]["tests"], [{
                    "id": "rockstream-gateway::test/wire_tests::must_run",
                    **({"execution_seconds": 1.25} if event is not None else {}),
                    "outcome": {"ok": "passed", "failed": "failed", None: "not_run", "ignored": "unexpected_skip"}[event],
                    "reason": {"ok": None, "failed": "test failed", None: "libtest emitted no terminal execution event",
                               "ignored": "mandatory test is #[ignore]"}[event],
                }])

    def test_parallel_allowlist_is_explicit_and_owned(self):
        self.assertEqual(POLICY["jobs"]["pr-workspace"]["test_threads"], {
            "rockstream-types/test:canonical_type_tests": 4,
            "rockstream-types/test:arrangement_spec_isolation_tests": 4,
        })
        packages, targets = gate.inventory(gate.cargo_metadata())
        for field, value, message in (
            ("test_threads", {"rockstream-types/test:canonical_type_tests": 0}, "invalid parallel"),
            ("test_threads", {"rockstream-gateway/lib:rockstream_gateway": 4}, "invalid parallel"),
            ("conformance_targets", ["rockstream-gateway/test:nonexistent"], "conformance target"),
        ):
            with self.subTest(field=field, value=value):
                policy = copy.deepcopy(POLICY)
                policy["jobs"]["pr-workspace"][field] = value
                self.assertFails(lambda: gate.assignments(targets, packages, policy), message)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Contract tests, including actual libFuzzer crash/minimization/replay fixtures."""

import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("run_fuzz", Path(__file__).with_name("run-fuzz.py"))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
parser = argparse.ArgumentParser()
parser.add_argument("--bin-dir", type=Path, required=True)
args, remaining = parser.parse_known_args()
BIN_DIR = args.bin_dir.resolve()
BUDGET = {"seconds": 1, "timeout_seconds": 2, "rss_mb": 512,
          "minimize_seconds": 1, "seed": 130}


class FuzzRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.corpus = self.root / "corpus"
        self.corpus.mkdir()
        self.output = self.root / "results"

    def test_failure_is_captured_minimized_and_replayed_twice(self):
        self.addCleanup(shutil.copytree, self.output,
                        runner.ROOT / "target/fuzz-self-test-results", dirs_exist_ok=True)
        (self.corpus / "seed").write_bytes(b"padding!padding")
        report = runner.run_target("fixture", BIN_DIR / "deliberate_failure",
                                   self.corpus, self.output, BUDGET)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["errors"], ["engine/target failure"])
        self.assertEqual(report["budget"], BUDGET)
        self.assertEqual(report["command"], [
            str(BIN_DIR / "deliberate_failure"), str(self.corpus), "-max_total_time=1",
            f"-artifact_prefix={self.output}/", "-max_len=65536", "-timeout=2",
            "-rss_limit_mb=512", "-seed=130",
        ])
        [failure] = report["failures"]
        self.assertEqual((self.output / failure["original"]).read_bytes(), b"padding!padding")
        self.assertEqual((self.output / failure["input"]).read_bytes(), b"!")
        self.assertEqual(failure["sha256"], runner.hashes(self.output)[failure["input"]])
        self.assertEqual(failure["minimize"]["exit_code"], 0)
        self.assertEqual(failure["reproducible"], True)
        self.assertEqual([r["signature"] for r in failure["replays"]], [
            {"exit_code": 77, "timed_out": False, "summary": ["SUMMARY: libFuzzer: deadly signal"]},
            {"exit_code": 77, "timed_out": False, "summary": ["SUMMARY: libFuzzer: deadly signal"]},
        ])
        for replay in failure["replays"]:
            self.assertIn("deliberate fuzz failure #130", (self.output / replay["log"]).read_text())
        self.assertEqual(json.loads((self.output / "report.json").read_text()), report)

    def test_real_mutating_engine_passes_and_retains_corpus(self):
        (self.corpus / "seed").write_bytes(b"safe")
        report = runner.run_target("safe", BIN_DIR / "healthy", self.corpus, self.output, BUDGET)
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["corpus_after"], runner.hashes(self.corpus))
        self.assertEqual(report["scope"], runner.SCOPE)

    def test_already_minimal_failure_still_replays(self):
        (self.corpus / "seed").write_bytes(b"!")
        report = runner.run_target("minimal", BIN_DIR / "deliberate_failure",
                                   self.corpus, self.output, BUDGET)
        self.assertEqual(report["errors"], ["engine/target failure"])
        [failure] = report["failures"]
        self.assertEqual((self.output / failure["input"]).read_bytes(), b"!")
        self.assertEqual(failure["reproducible"], True)

    def test_zero_execution_cannot_pass(self):
        (self.corpus / "seed").write_bytes(b"safe")
        report = runner.run_target("zero", BIN_DIR / "zero_execution",
                                   self.corpus, self.output, BUDGET)
        self.assertEqual(report["errors"], ["no mutation execution beyond corpus replay"])
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["engine"]["exit_code"], 0)
        self.assertEqual(report["failures"], [])

    def test_result_gate(self):
        result = {"exit_code": 0, "timed_out": False}
        output = "#42\tDONE   cov: 3 ft: 7\nDone 42 runs in 1 second(s)\n"
        cases = [
            (result, output, None),
            (result, "compile succeeded", "no mutation execution beyond corpus replay"),
            (result, "Done 2 runs in 0 second(s)\n", "no mutation execution beyond corpus replay"),
            (result, "Done 42 runs in 1 second(s)\n", "missing coverage-guided completion"),
            ({"exit_code": 70, "timed_out": False}, output, "engine/target failure"),
            ({"exit_code": 71, "timed_out": False}, output, "engine/target failure"),
            ({"exit_code": -9, "timed_out": True}, output, "wall-clock timeout"),
        ]
        self.assertEqual([runner.execution_error(r, log, 1) for r, log, _ in cases],
                         [expected for _, _, expected in cases])

    def test_wall_clock_kills_process_group(self):
        result = runner.execute([sys.executable, "-c", "import time; time.sleep(60)"],
                                self.root / "timeout.log", 0.1)
        self.assertEqual(result, {"command": [sys.executable, "-c", "import time; time.sleep(60)"],
                                  "exit_code": -9, "timed_out": True, "log": "timeout.log"})

    def test_missing_binary_is_a_reported_failure(self):
        report = runner.run_target("missing", BIN_DIR / "does-not-exist",
                                   self.corpus, self.output, BUDGET)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["failures"], [])
        self.assertEqual(report["errors"], [f"[Errno 2] No such file or directory: '{BIN_DIR / 'does-not-exist'}'"])

    def test_registry_and_valid_seed_gate(self):
        shutil.copytree(runner.ROOT / "fuzz", self.root / "fuzz")
        expected = ["fuzz_sql_parser", "fuzz_pgwire_decoder", "fuzz_postgres_cdc",
                    "fuzz_control_worker_decoder", "fuzz_raft_rpc_decoder",
                    "fuzz_kafka_payload_decoder", "fuzz_oidc_jwt_decoder"]
        self.assertEqual(runner.registered_targets(self.root), expected)
        self.assertEqual(runner.selected_targets(expected, None), expected)
        self.assertEqual(runner.selected_targets(expected, "fuzz_pgwire_decoder"), ["fuzz_pgwire_decoder"])
        with self.assertRaisesRegex(ValueError, "^unregistered fuzz target: missing$"):
            runner.selected_targets(expected, "missing")
        manifest = self.root / "fuzz/Cargo.toml"
        original = manifest.read_text()
        manifest.write_text(original.replace('name = "fuzz_sql_parser"', 'name = "missing_sql_parser"'))
        with self.assertRaisesRegex(ValueError, "^missing mandatory or duplicate fuzz target$"):
            runner.registered_targets(self.root)
        manifest.write_text(original)
        for seed in (self.root / "fuzz/corpus/fuzz_sql_parser").glob("valid-*"):
            seed.unlink()
        with self.assertRaisesRegex(ValueError, "^missing known-valid seed: fuzz_sql_parser$"):
            runner.registered_targets(self.root)


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0], *remaining], buffer=True)

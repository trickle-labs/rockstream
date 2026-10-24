#!/usr/bin/env python3
"""Exact command and failure checks for independently scheduled benchmark suites."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("run-benchmark-gate.sh")
SUITES = {
    "ops": ("perf_regression", "v0.45.4-ops"),
    "storage": ("storage_bench", "v0.45.4-storage"),
    "runtime": ("exchange_bench", "v0.51-runtime"),
    "control": ("frontier_bench", "v0.45.4-control"),
}


class BenchmarkTests(unittest.TestCase):
    def run_gate(self, suite, failures=0, build_failure=False):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cargo = root / "cargo"
            cargo.write_text(
                "#!/usr/bin/env python3\nimport json, os, sys\nfrom pathlib import Path\n"
                "p = Path('calls.json')\ncalls = json.loads(p.read_text()) if p.exists() else []\n"
                "calls.append(sys.argv[1:])\np.write_text(json.dumps(calls))\n"
                "if sys.argv[1] == 'bench':\n"
                "    if os.environ['BUILD_FAILURE'] == '1':\n"
                "        print('build failed', file=sys.stderr)\n        sys.exit(7)\n"
                "    print('benchmark output')\n"
                "else:\n"
                "    sys.exit(int(sum(c[0] == 'run' for c in calls) <= int(os.environ['GATE_FAILURES'])))\n"
            )
            cargo.chmod(0o755)
            result = subprocess.run(["bash", str(SCRIPT), suite], cwd=root, text=True, capture_output=True,
                                    env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                         "GATE_FAILURES": str(failures), "BUILD_FAILURE": str(int(build_failure))})
            calls = json.loads((root / "calls.json").read_text()) if (root / "calls.json").exists() else []
            outputs = {p.name: p.read_text() for p in root.glob("*-bench-*.out")}
            return result.returncode, result.stdout, result.stderr, calls, outputs

    def test_each_suite_keeps_its_baseline_and_existing_retry_limit(self):
        for suite, (bench, baseline) in SUITES.items():
            for failures in (0, 2, 3):
                with self.subTest(suite=suite, failures=failures):
                    attempts = min(failures + 1, 3)
                    calls = []
                    outputs = {}
                    for attempt in range(attempts):
                        output = f"{suite}-bench-{attempt}.out"
                        calls += [
                            ["bench", "-p", f"rockstream-{suite}", "--bench", bench, "--", "--noplot"],
                            ["run", "-p", "rockstream-ops", "--bin", "bench_regression_gate", "--",
                             "--baseline", f"crates/rockstream-{suite}/benches/baseline/{baseline}.json",
                             "--output", output, "--tag", suite],
                        ]
                        outputs[output] = "benchmark output\n"
                    self.assertEqual(self.run_gate(suite, failures), (
                        int(failures == 3), "benchmark output\n" * attempts, "", calls, outputs))

    def test_build_failure_is_not_hidden_by_tee_or_retried(self):
        self.assertEqual(self.run_gate("ops", build_failure=True), (
            7, "", "build failed\n",
            [["bench", "-p", "rockstream-ops", "--bench", "perf_regression", "--", "--noplot"]],
            {"ops-bench-0.out": ""},
        ))

    def test_unknown_suite_cannot_pass(self):
        self.assertEqual(self.run_gate("other"), (1, "", "unknown benchmark suite: other\n", [], {}))


if __name__ == "__main__":
    unittest.main()

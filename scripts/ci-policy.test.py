#!/usr/bin/env python3
"""Negative controls for qualification selection and candidate image reuse."""
import importlib.util
import contextlib
import io
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("selection", ROOT / "scripts/ci-selection.py")
selection = importlib.util.module_from_spec(spec)
spec.loader.exec_module(selection)


class PolicyTests(unittest.TestCase):
    def test_revision_bound_selection_artifact_and_output(self):
        for event_name in ("pull_request", "workflow_dispatch"):
            with self.subTest(event_name=event_name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                event = root / "event.json"
                event.write_text(json.dumps({"pull_request": {"base": {"sha": "base"}}}))
                with patch.dict(os.environ, {"GITHUB_EVENT_NAME": event_name, "GITHUB_EVENT_PATH": str(event),
                                             "GITHUB_SHA": "candidate", "GITHUB_OUTPUT": str(root / "output")}), \
                     patch.object(selection.subprocess, "run", return_value=subprocess.CompletedProcess(
                         [], 0, "README.md\0docs/guide.md\0", "")) as run, \
                     contextlib.chdir(root), contextlib.redirect_stdout(io.StringIO()):
                    selection.main()
                expensive = event_name != "pull_request"
                self.assertEqual(json.loads((root / "target/ci-selection.json").read_text()), {
                    "revision": "candidate", "expensive": expensive,
                    "reason": "full qualification required" if expensive else "documentation-only PR",
                    "paths": [] if expensive else ["README.md", "docs/guide.md"],
                })
                self.assertEqual((root / "output").read_text(), f"expensive={str(expensive).lower()}\n")
                if event_name == "pull_request":
                    self.assertEqual(run.call_args.args, (["git", "diff", "--name-only", "--no-renames", "-z", "base", "candidate"],))
                else:
                    run.assert_not_called()

    def test_required_checks_fail_closed_and_expensive_steps_are_guarded(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        blocks = dict(re.findall(r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S))
        check = blocks["check"]
        dependencies = re.findall(r"^      - ([\w-]+)$", check, re.M)
        self.assertEqual(dependencies, ["ci-policy", "check-quality", "check-tests", "gateway",
                                       "postgres-differential", "control-plane-ha", "formal-verify",
                                       "verus-verify", "nexmark-regression-gate", "benchmark", "docker-build"])
        condition = re.search(r"if: \$\{\{ (.*?) \}\}", check)[1]
        self.assertEqual(condition, " || ".join(f"needs.{job}.result != 'success'" for job in dependencies))
        # Exercise the actual gate expression for each non-success dependency result.
        for job in dependencies:
            for outcome in ("success", "failure", "cancelled", "skipped", ""):
                expression = condition
                for dep in dependencies:
                    expression = expression.replace(f"needs.{dep}.result", repr(outcome if dep == job else "success"))
                self.assertEqual(eval(expression.replace("||", "or"), {"__builtins__": {}}), outcome != "success")
        for job in ("verus-verify", "nexmark-regression-gate", "benchmark"):
            block = blocks[job]
            self.assertIn("    needs: ci-policy\n", block)
            steps = re.split(r"^      - ", block.split("    steps:\n")[1], flags=re.M)[1:]
            self.assertIn('run: test "$EXPENSIVE" = true || test "$EXPENSIVE" = false', steps[0])
            for step in steps[1:-1]:
                self.assertRegex(step, r"if: (always\(\) && )?needs.ci-policy.outputs.expensive == 'true'")
            self.assertIn("if: needs.ci-policy.outputs.expensive == 'false'", steps[-1])

    def test_lane_policy(self):
        for event, paths, expensive in (
            ("pull_request", ["docs/ci-test-accounting.md", "README.md"], False),
            ("pull_request", ["crates/rockstream-gateway/src/lib.rs"], True),
            ("pull_request", ["crates/rockstream-storage/src/lib.rs"], True),
            ("pull_request", ["formal/verus/routing.rs"], True),
            ("pull_request", ["Cargo.lock"], True),
            ("pull_request", ["crates/rockstream-ops/benches/nexmark.rs"], True),
            ("pull_request", [".github/workflows/ci.yml"], True),
            ("pull_request", ["new/unknown"], True),
            ("pull_request", ["docs/new-script.py"], True),
            ("pull_request", ["README.md", "old-code.rs"], True),
            ("pull_request", [], True),
            ("schedule", ["README.md"], True),
            ("workflow_dispatch", ["README.md"], True),
            ("push", ["README.md"], True),
        ):
            with self.subTest(event=event, paths=paths):
                self.assertEqual(selection.selection(paths, event), {
                    "expensive": expensive, "paths": paths,
                    "reason": "full qualification required" if expensive else "documentation-only PR",
                })

    def test_diff_failure_cannot_become_a_docs_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            event = Path(tmp) / "event.json"
            event.write_text(json.dumps({"pull_request": {"base": {"sha": "base"}}}))
            with patch.dict(os.environ, {"GITHUB_EVENT_NAME": "pull_request", "GITHUB_EVENT_PATH": str(event),
                                         "GITHUB_SHA": "candidate"}), \
                 patch.object(selection.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "git")):
                with self.assertRaises(subprocess.CalledProcessError):
                    selection.main()

    def test_image_identity_and_prerequisite_failures(self):
        for override, passed in (
            ({}, True), ({"MOCK_ID": "other-image"}, False),
            ({"MOCK_REV": "other-revision"}, False), ({"MOCK_FAIL": "info"}, False),
            ({"MOCK_FAIL": "load"}, False), ({"EXPECTED_IMAGE_ID": ""}, False),
        ):
            with self.subTest(override=override), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                docker = root / "docker"
                docker.write_text(
                    "#!/usr/bin/env python3\nimport json, os, sys\na = sys.argv[1:]\n"
                    "with open('calls.jsonl', 'a') as f: f.write(json.dumps(a) + '\\n')\n"
                    "if a[0] == os.environ.get('MOCK_FAIL'): sys.exit(1)\n"
                    "if a[:2] == ['image', 'inspect']:\n"
                    "    print(os.environ.get('MOCK_ID', 'sha256:candidate') if a[3] == '{{.Id}}' "
                    "else os.environ.get('MOCK_REV', 'candidate'))\n"
                )
                docker.chmod(0o755)
                env = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                       "GITHUB_SHA": "candidate", "EXPECTED_IMAGE_ID": "sha256:candidate", **override}
                proc = subprocess.run(["bash", str(ROOT / "scripts/ci-load-image.sh")],
                                      cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(proc.returncode == 0, passed, proc.stderr)
                calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
                if passed:
                    self.assertEqual(calls, [
                        ["info"], ["load", "--input", "target/ci-image/candidate.tar"],
                        ["image", "inspect", "--format", "{{.Id}}", "rockstream:ci-candidate"],
                        ["image", "inspect", "--format", '{{index .Config.Labels "org.opencontainers.image.revision"}}', "rockstream:ci-candidate"],
                        ["tag", "rockstream:ci-candidate", "rockstream-tc-test:latest"],
                    ])
                    self.assertEqual((root / "target/ci-test-results/candidate-image.tsv").read_text(),
                                     "candidate\tsha256:candidate\n")
                else:
                    self.assertEqual([call for call in calls if call[0] == "tag"], [])
                    self.assertFalse((root / "target/ci-test-results/candidate-image.tsv").exists())


if __name__ == "__main__":
    unittest.main()

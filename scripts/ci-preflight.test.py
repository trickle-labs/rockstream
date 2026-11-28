#!/usr/bin/env python3
"""Exercise the actual preflight script with cheap, isolated command stubs."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("ci-preflight.sh")


class PreflightTests(unittest.TestCase):
    def run_preflight(self, scope, failure=""):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scripts = root / "scripts"
            scripts.mkdir()
            shutil.copyfile(SCRIPT, scripts / SCRIPT.name)
            for name, path in (
                ("cargo", root / "cargo"),
                ("docs", scripts / "check-documentation.sh"),
                ("self-test", scripts / "check-documentation.test.sh"),
            ):
                path.write_text(
                    '#!/usr/bin/env bash\n'
                    f'printf "%s %s\\n" "{name}" "$*" >> "$TRACE"\n'
                    f'if [[ "$FAILURE" == "{name}" ]]; then exit 7; fi\n'
                )
                path.chmod(0o755)
            trace = root / "trace"
            result = subprocess.run(
                ["bash", str(scripts / SCRIPT.name), scope],
                cwd=root,
                env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                     "TRACE": str(trace), "FAILURE": failure},
                capture_output=True, text=True,
            )
            return (result.returncode, result.stdout, result.stderr,
                    trace.read_text() if trace.exists() else "")

    def test_full_scope_runs_exact_commands(self):
        self.assertEqual(self.run_preflight("full"),
                         (0, "", "", "cargo fmt --all --check\ndocs \nself-test \n"))

    def test_prose_skips_only_rustfmt(self):
        self.assertEqual(self.run_preflight("prose"),
                         (0, "", "", "docs \nself-test \n"))

    def test_invalid_scope_never_admits(self):
        for scope in ("unknown", ""):
            self.assertEqual(self.run_preflight(scope),
                             (2, "", f"Invalid CI preflight scope: {scope}\n", ""))

    def test_failures_stop_before_further_work(self):
        for failure, trace in (
            ("cargo", "cargo fmt --all --check\n"),
            ("docs", "cargo fmt --all --check\ndocs \n"),
            ("self-test", "cargo fmt --all --check\ndocs \nself-test \n"),
        ):
            with self.subTest(failure=failure):
                self.assertEqual(self.run_preflight("full", failure), (7, "", "", trace))
        self.assertEqual(self.run_preflight("prose", "docs"), (7, "", "", "docs \n"))


if __name__ == "__main__":
    unittest.main()

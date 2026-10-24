#!/usr/bin/env python3
"""Reject incomplete, mixed-revision or corrupted parallel qualification evidence."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("record-verus-qualification.py")
spec = importlib.util.spec_from_file_location("recorder", SCRIPT)
recorder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recorder)
IDS = {"debug": ["verifier", "verifier-incremental", "cargo-debug", "cargo-check", "cargo-all-features"],
       "release": ["cargo-release"]}
BUDGETS = {"cold_verification_seconds": 100, "incremental_verification_seconds": 10,
           "production_compile_seconds": 100}


class MergeTests(unittest.TestCase):
    def fixture(self, root):
        records = {}
        for phase, ids in IDS.items():
            directory = root / phase
            directory.mkdir()
            binary = directory / f"rockstream-{phase}"
            binary.write_bytes(phase.encode())
            binary.chmod(0o644)
            records[phase] = {
                "schema": 1, "phase": phase, "source_revision": "candidate", "target": "target",
                "status": "passed", "artifacts": {"Cargo.lock": "digest"},
                "runtime_artifacts": {binary.name: recorder.sha256(binary)},
                "steps": [f"{name}\t0\t1\tcommand {name}" for name in ids],
                "tool_versions": {"rustc": "1.88", "cargo": "1.88", "verus": "pinned"},
            }
        return records

    def write_records(self, root, records):
        for phase, record in records.items():
            (root / phase / "phase.json").write_text(json.dumps(record))

    def merge(self, root):
        return recorder.merge_phases(root, root / "complete", "candidate", "target",
                                     {"Cargo.lock": "digest"}, BUDGETS)

    def test_exact_complete_steps_artifacts_and_executable_modes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            records = self.fixture(root)
            self.write_records(root, records)
            self.assertEqual(self.merge(root), (
                records["debug"]["steps"] + records["release"]["steps"],
                [root / "complete/rockstream-debug", root / "complete/rockstream-release"],
                {phase: record["tool_versions"] for phase, record in records.items()},
            ))
            self.assertEqual({p.name: (p.read_bytes(), p.stat().st_mode & 0o777)
                              for p in (root / "complete").iterdir()},
                             {"rockstream-debug": (b"debug", 0o755), "rockstream-release": (b"release", 0o755)})

    def test_phase_identity_step_failures_and_budgets_fail_closed(self):
        identity = "release: qualification identity or status mismatch"
        steps = "release: qualification steps differ from required steps"
        unsuccessful = "release: unsuccessful or over-budget step cargo-release"
        changes = [
            ("source_revision", "old", identity), ("target", "other", identity), ("phase", "all", identity),
            ("status", "failed", identity), ("status", "skipped", identity), ("schema", 2, identity),
            ("artifacts", {"Cargo.lock": "stale"}, identity), ("steps", [], steps),
            ("steps", ["cargo-release\t1\t1\tbuild"], unsuccessful),
            ("steps", ["cargo-release\t0\t101\tbuild"], unsuccessful),
            ("steps", ["cargo-release\t0\t-1\tbuild"], unsuccessful),
            ("steps", ["cargo-release\t0\t1\t"], unsuccessful),
            ("steps", ["cargo-release\t0\t1\tbuild"] * 2, steps),
            ("runtime_artifacts", {"rockstream-release": "corrupt"}, "release: runtime artifact digest mismatch"),
        ]
        for key, value, message in changes:
            with self.subTest(key=key, value=value), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                records = self.fixture(root)
                records["release"][key] = value
                self.write_records(root, records)
                with self.assertRaises(ValueError) as raised:
                    self.merge(root)
                self.assertEqual(str(raised.exception), message)
                self.assertFalse((root / "complete").exists())

    def test_missing_or_changed_artifact_and_missing_phase_fail_closed(self):
        for missing in ("release/phase.json", "release/rockstream-release", "debug/rockstream-debug"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self.write_records(root, self.fixture(root))
                (root / missing).unlink()
                with self.assertRaises(OSError) as raised:
                    self.merge(root)
                self.assertEqual((raised.exception.errno, raised.exception.filename), (2, str(root / missing)))
                self.assertFalse((root / "complete").exists())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_records(root, self.fixture(root))
            (root / "release/rockstream-release").write_bytes(b"tampered")
            with self.assertRaises(ValueError) as raised:
                self.merge(root)
            self.assertEqual(str(raised.exception), "release: runtime artifact digest mismatch")


if __name__ == "__main__":
    unittest.main()

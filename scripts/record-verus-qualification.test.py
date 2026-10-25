#!/usr/bin/env python3
"""Reject incomplete, mixed-revision or corrupted parallel qualification evidence."""
import importlib.util
import json
from pathlib import Path
import tempfile
import tomllib
import unittest

SCRIPT = Path(__file__).with_name("record-verus-qualification.py")
SOURCE = SCRIPT.parent.parent
with (SOURCE / "formal/verus/qualification.toml").open("rb") as handle:
    INVENTORY = tomllib.load(handle)["production_mutations"]
spec = importlib.util.spec_from_file_location("recorder", SCRIPT)
recorder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recorder)
IDS = {"debug": ["verifier", "production-mutations", "verifier-incremental", "cargo-debug", "cargo-check", "cargo-all-features"],
       "release": ["cargo-release"]}
BUDGETS = {"cold_verification_seconds": 100, "incremental_verification_seconds": 10,
           "production_compile_seconds": 100}


class MergeTests(unittest.TestCase):
    def fixture(self, root):
        records = {}
        mutations = root / "debug/mutations"
        mutations.mkdir(parents=True)
        baselines = {}
        for name in ("baseline-before.log", "baseline-after.log"):
            log = mutations / name
            log.write_text("verification results:: 68 verified, 0 errors\n")
            baselines[name] = recorder.sha256(log)
        cases = []
        for case in INVENTORY:
            relative = case["target"].split("::", 1)[0]
            name = f"{case['id']}.log"
            log = mutations / name
            log.write_text(f"{relative}: assertion failed\nverification results:: 67 verified, 1 errors\n")
            cases.append({
                "id": case["id"], "claim": case["claim"], "target": case["target"],
                "kind": case["kind"], "original_sha256": recorder.sha256(SOURCE / relative),
                "mutant_sha256": "0" * 64, "verifier_exit": 1, "log": name,
                "log_sha256": recorder.sha256(log),
            })
        (mutations / "results.json").write_text(json.dumps({
            "command": ["cargo", "verus", "verify", "-p", "rockstream-verified", "--locked"],
            "baselines": baselines, "mutations": cases,
        }))
        for phase, ids in IDS.items():
            directory = root / phase
            directory.mkdir(exist_ok=True)
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
                                     {"Cargo.lock": "digest"}, BUDGETS, SOURCE, INVENTORY)

    def test_exact_complete_steps_artifacts_and_executable_modes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            records = self.fixture(root)
            self.write_records(root, records)
            self.assertEqual(self.merge(root), (
                records["debug"]["steps"] + records["release"]["steps"],
                [root / "complete/rockstream-debug", root / "complete/rockstream-release"],
                {phase: record["tool_versions"] for phase, record in records.items()},
                {f"mutations/{path.name}": recorder.sha256(path)
                 for path in (root / "debug/mutations").iterdir()},
            ))
            self.assertEqual({p.name: (p.read_bytes(), p.stat().st_mode & 0o777)
                              for p in (root / "complete").iterdir() if p.is_file()},
                             {"rockstream-debug": (b"debug", 0o755), "rockstream-release": (b"release", 0o755)})
            self.assertEqual({f"mutations/{p.name}": recorder.sha256(p)
                              for p in (root / "complete/mutations").iterdir()},
                             {f"mutations/{p.name}": recorder.sha256(p)
                              for p in (root / "debug/mutations").iterdir()})

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

    def test_missing_or_changed_mutation_evidence_fails_closed(self):
        for name, contents, expected in (
            ("results.json", None, None),
            ("PM01.log", None, None),
            ("PM01.log", "error: compiler failed\n", "production mutation rejection is invalid: PM01"),
            ("baseline-before.log", "verification results:: 1 verified, 1 errors\n",
             "production mutation baseline is invalid: baseline-before.log"),
        ):
            with self.subTest(name=name, contents=contents), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                self.write_records(root, self.fixture(root))
                path = root / "debug/mutations" / name
                if contents is None:
                    path.unlink()
                else:
                    path.write_text(contents)
                with self.assertRaises((OSError, ValueError)) as raised:
                    self.merge(root)
                if contents is None:
                    self.assertEqual((raised.exception.errno, raised.exception.filename), (2, str(path)))
                else:
                    self.assertEqual(str(raised.exception), expected)
                self.assertFalse((root / "complete").exists())

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.write_records(root, self.fixture(root))
            results = root / "debug/mutations/results.json"
            data = json.loads(results.read_text())
            data["mutations"].pop()
            results.write_text(json.dumps(data))
            with self.assertRaises(ValueError) as raised:
                self.merge(root)
            self.assertEqual(str(raised.exception), "production mutation results are incomplete")
            self.assertFalse((root / "complete").exists())


if __name__ == "__main__":
    unittest.main()

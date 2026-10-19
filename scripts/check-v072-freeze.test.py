#!/usr/bin/env python3
"""Regression tests for the immutable v0.72 acceptance-input guard."""

import hashlib
from pathlib import Path
import runpy
import tempfile
import unittest

CHECK = runpy.run_path(str(Path(__file__).with_name("check-v072-freeze.py")), run_name="v072_guard")[
    "check_frozen_inputs"
]
PARENT = "docs/acceptance-contracts/132.md"
PROFILE = "benchmarks/r1-local/profile-v072.toml"


class V072FreezeTests(unittest.TestCase):
    def test_exact_frozen_bytes_are_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = {}
            for relative, data in ((PARENT, b"contract-v2\n"), (PROFILE, b"profile-v1\n")):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                expected[relative] = hashlib.sha256(data).hexdigest()
            self.assertEqual(CHECK(root, expected), [])

    def test_missing_parent_and_profile_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(
                CHECK(root, {PARENT: "a" * 64, PROFILE: "b" * 64}),
                [f"{PARENT}: missing or unreadable", f"{PROFILE}: missing or unreadable"],
            )

    def test_contract_or_profile_mutation_is_rejected_with_observed_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = {}
            for relative, data in ((PARENT, b"contract-v2\n"), (PROFILE, b"profile-v1\n")):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                expected[relative] = hashlib.sha256(data).hexdigest()
            for relative in (PARENT, PROFILE):
                path = root / relative
                original = path.read_bytes()
                path.write_bytes(original + b"modified\n")
                observed = hashlib.sha256(path.read_bytes()).hexdigest()
                self.assertEqual(
                    CHECK(root, expected),
                    [f"{relative}: expected sha256 {expected[relative]}, observed {observed}"],
                )
                path.write_bytes(original)

    def test_symlink_substitution_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "substitute"
            target.write_bytes(b"contract-v2\n")
            path = root / PARENT
            path.parent.mkdir(parents=True, exist_ok=True)
            path.symlink_to(target)
            self.assertEqual(
                CHECK(root, {PARENT: hashlib.sha256(target.read_bytes()).hexdigest()}),
                [f"{PARENT}: symlink is not a frozen file"],
            )


if __name__ == "__main__":
    unittest.main()

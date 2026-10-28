#!/usr/bin/env python3
"""Exact-output self-tests for the bounded safety mutation runner."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from run_safety_mutation_lane import (
    apply_mutation,
    baseline_passed,
    classify_mutant,
    render_summary,
    source_fingerprint_matches,
)


class SafetyMutationLaneTests(unittest.TestCase):
    def test_mutation_replaces_the_exact_source_block(self):
        case = {
            "id": "P1-EXAMPLE",
            "before": ["        let weight = left * right;", "        emit(weight);"],
            "after": ["        let weight = 2 * left * right;", "        emit(weight);"],
        }
        original = "fn example() {\n        let weight = left * right;\n        emit(weight);\n}\n"
        expected = "fn example() {\n        let weight = 2 * left * right;\n        emit(weight);\n}\n"
        self.assertEqual(apply_mutation(original, case), expected)

    def test_mutation_rejects_missing_or_ambiguous_source(self):
        case = {"id": "P1-EXAMPLE", "before": ["old"], "after": ["new"]}
        with self.assertRaisesRegex(RuntimeError, "exact mutation source is missing or ambiguous"):
            apply_mutation("other\n", case)
        with self.assertRaisesRegex(RuntimeError, "exact mutation source is missing or ambiguous"):
            apply_mutation("old\nold\n", case)

    def test_baseline_requires_the_exact_test_and_one_pass(self):
        output = (
            "running 1 test\n"
            "test example::detector ... ok\n\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.01s\n"
        )
        self.assertTrue(baseline_passed(0, output, "example::detector"))
        self.assertFalse(baseline_passed(0, output, "example::other"))
        self.assertFalse(baseline_passed(101, output, "example::detector"))

    def test_mutant_classification_uses_failure_identity_and_marker(self):
        case = {"test_id": "example::detector", "expected_failure_marker": "expected rows differ"}
        intended = (
            "test example::detector ... FAILED\n"
            "expected rows differ\n"
            "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 4 filtered out; finished in 0.01s\n"
        )
        self.assertEqual(classify_mutant(101, intended, case), "caught_by_intended_assertion")
        survived = (
            "test example::detector ... ok\n"
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"
        )
        self.assertEqual(classify_mutant(0, survived, case), "survivor")
        self.assertEqual(classify_mutant(0, "test result: ok. 0 passed; 0 failed; 1 filtered out;\n", case), "test_not_run")
        self.assertEqual(classify_mutant(101, intended.replace("expected rows differ", "linker failed"), case), "unintended_failure")
        self.assertEqual(classify_mutant(101, "error: could not compile\n", case), "build_or_harness_failure")
        self.assertEqual(classify_mutant(None, "", case), "timeout")

    def test_source_fingerprint_matches_exact_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "source.rs"
            path.write_bytes(b"fn source() {}\n")
            expected = hashlib.sha256(b"fn source() {}\n").hexdigest()
            self.assertTrue(source_fingerprint_matches(path, expected))
            self.assertFalse(source_fingerprint_matches(path, "0" * 64))

    def test_summary_is_complete_and_reports_uncovered_invariants(self):
        report = {
            "status": "partial_evidence_not_qualification",
            "source_sha": "abc123",
            "selected_count": 1,
            "run_seconds": 0,
            "tested_wall_seconds": 0,
            "setup_verification_seconds": 0,
            "mutant_wall_total_seconds": 0,
            "child_cpu_seconds": 0,
            "peak_child_rss_bytes": 0,
            "cases": [{
                "id": "P1-EXAMPLE",
                "invariant": "unknown writes are not acknowledged",
                "severity": "P0",
                "classification": "caught_by_intended_assertion",
                "test_id": "example::detector",
            }],
            "uncovered_invariants": [{
                "id": "P0-POWER-LOSS-AFTER-ACK",
                "severity": "P0",
                "status": "unsupported",
            }],
        }
        expected = (
            "# Bounded safety mutation run\n\n"
            "Status: partial_evidence_not_qualification\n"
            "Source SHA: abc123\n"
            "Cases selected: 1\n"
            "Wall seconds: 0\n"
            "Targeted test seconds: 0\n"
            "Setup and verification seconds: 0\n"
            "Mutant total/median seconds: 0 / n/a\n"
            "Child CPU seconds: 0\n"
            "Peak child RSS bytes: 0\n"
            "Candidate inventory: [candidate-inventory.json](candidate-inventory.json)\n\n"
            "| Mutation | Invariant | Severity | Test | Result | Mutant seconds | Diff | Log |\n"
            "| --- | --- | --- | --- | --- | ---: | --- | --- |\n"
            "| P1-EXAMPLE | unknown writes are not acknowledged | P0 | example::detector | caught_by_intended_assertion | n/a | [not-run](not-run) | [not-run](not-run) |\n"
            "| P0-POWER-LOSS-AFTER-ACK | P0-POWER-LOSS-AFTER-ACK | P0 | not selected | unsupported | n/a | n/a | n/a |\n\n"
            "This bounded result is not an end-to-end durability qualification.\n"
        )
        self.assertEqual(render_summary(report), expected)


if __name__ == "__main__":
    unittest.main()

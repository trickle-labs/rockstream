#!/usr/bin/env python3
"""Exercise paginated timing reports, including absent and misleading times."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("metrics", Path(__file__).with_name("ci-run-metrics.py"))
metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(metrics)


def job(id, conclusion="success", created="2026-11-25T09:00:00Z",
        started="2026-11-25T09:02:00Z", completed="2026-11-25T09:03:00Z"):
    return {"id": id, "name": f"job-{id}", "run_id": 7, "run_attempt": 1,
            "status": "completed", "conclusion": conclusion, "created_at": created,
            "started_at": started, "completed_at": completed}


class MetricsTests(unittest.TestCase):
    def test_paginated_report_exact_values(self):
        jobs = [job(1), job(2, "failure", started="2026-11-25T09:01:00Z",
                            completed="2026-11-25T09:03:00Z"),
                job(3, "cancelled"), job(4, "skipped"), job(5, started=None)]
        result = metrics.report([{"total_count": 5, "jobs": jobs[:2]},
                                 {"total_count": 5, "jobs": jobs[2:]}])
        self.assertEqual(result, {
            "job_count": 5,
            "outcomes": {"cancelled": 1, "failure": 1, "skipped": 1, "success": 2},
            "queue_seconds": {"count": 2, "sum": 180, "median": 90, "p90": 120, "max": 120},
            "execution_seconds": {"count": 2, "sum": 180, "median": 90, "p90": 120, "max": 120},
            "missing_or_invalid_timing_job_ids": [5],
            "jobs": [
                {"id": 1, "name": "job-1", "status": "success", "queue_seconds": 120, "execution_seconds": 60},
                {"id": 2, "name": "job-2", "status": "failure", "queue_seconds": 60, "execution_seconds": 120},
                {"id": 3, "name": "job-3", "status": "cancelled", "queue_seconds": None, "execution_seconds": None},
                {"id": 4, "name": "job-4", "status": "skipped", "queue_seconds": None, "execution_seconds": None},
                {"id": 5, "name": "job-5", "status": "success", "queue_seconds": None, "execution_seconds": None},
            ],
        })

    def test_reject_partial_duplicate_and_mixed_attempts(self):
        for payload, message in (
            ({"total_count": 2, "jobs": [job(1)]}, "incomplete or duplicate jobs; fetch every page of one attempt"),
            ({"total_count": 2, "jobs": [job(1), job(1)]}, "incomplete or duplicate jobs; fetch every page of one attempt"),
            ({"total_count": 2, "jobs": [job(1), {**job(2), "run_attempt": 2}]}, "mixed runs or attempts"),
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError) as ctx:
                    metrics.report(payload)
                self.assertEqual(str(ctx.exception), message)

    def test_pending_and_reversed_timestamps_are_not_credited(self):
        result = metrics.report({"total_count": 2, "jobs": [
            {**job(1), "status": "queued", "conclusion": None},
            job(2, started="2026-11-25T08:59:00Z"),
        ]})
        self.assertEqual(result["queue_seconds"],
                         {"count": 0, "sum": 0, "median": None, "p90": None, "max": None})
        self.assertEqual(result["execution_seconds"], result["queue_seconds"])
        self.assertEqual(result["missing_or_invalid_timing_job_ids"], [2])
        self.assertEqual(result["outcomes"], {"queued": 1, "success": 1})


if __name__ == "__main__":
    unittest.main()

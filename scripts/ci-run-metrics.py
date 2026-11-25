#!/usr/bin/env python3
"""Summarize one Actions attempt from complete paginated jobs JSON on stdin."""
import json
import math
import statistics
import sys
from collections import Counter
from datetime import datetime


def seconds(start, end):
    a, b = (datetime.fromisoformat(t.replace("Z", "+00:00")) for t in (start, end))
    if a.tzinfo is None or b.tzinfo is None or b < a:
        raise ValueError("invalid job timestamps")
    return int((b - a).total_seconds())


def stats(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "sum": 0, "median": None, "p90": None, "max": None}
    return {"count": len(values), "sum": sum(values),
            "median": statistics.median(values),
            "p90": values[math.ceil(0.9 * len(values)) - 1], "max": values[-1]}


def report(payload):
    pages = payload if isinstance(payload, list) else [payload]
    if not pages or any(not isinstance(p, dict) for p in pages):
        raise ValueError("expected one or more Actions jobs response objects")
    totals = {p["total_count"] for p in pages}
    jobs = [job for p in pages for job in p["jobs"]]
    if totals != {len(jobs)} or len({j["id"] for j in jobs}) != len(jobs):
        raise ValueError("incomplete or duplicate jobs; fetch every page of one attempt")
    for key in ("run_id", "run_attempt"):
        if len({j[key] for j in jobs if key in j}) > 1:
            raise ValueError("mixed runs or attempts")
    rows = []
    queue, runtime, missing = [], [], []
    counts = Counter()
    for job in jobs:
        status = job.get("conclusion") or job["status"]
        counts[status] += 1
        row = {"id": job["id"], "name": job["name"], "status": status,
               "queue_seconds": None, "execution_seconds": None}
        # Cancellations may never have received a runner; do not invent occupied time.
        if job["status"] == "completed" and status not in ("cancelled", "skipped"):
            try:
                q = seconds(job["created_at"], job["started_at"])
                r = seconds(job["started_at"], job["completed_at"])
            except (ValueError, TypeError, KeyError, AttributeError):
                missing.append(job["id"])
            else:
                row.update(queue_seconds=q, execution_seconds=r)
                queue.append(q)
                runtime.append(r)
        rows.append(row)
    return {
        "job_count": len(jobs),
        "outcomes": dict(sorted(counts.items())),
        "queue_seconds": stats(queue),
        "execution_seconds": stats(runtime),
        "missing_or_invalid_timing_job_ids": missing,
        "jobs": rows,
    }


def main():
    try:
        result = report(json.load(sys.stdin))
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        print(f"Invalid Actions jobs data: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

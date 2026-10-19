#!/usr/bin/env python3
"""Select conservative PR-only CI exclusions and validate required job results."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


PROSE_FILES = {
    "AGENTS.md", "CODE_OF_CONDUCT.md", "CONTRIBUTING.md",
    "ROADMAP.md", "NEW_ROADMAP.md", ".github/copilot-instructions.md",
}
PROSE_DIRS = (
    ".github/prompts/", ".github/skills/", "docs/agents/",
    "docs/implementation-plans/", "sign-offs/",
)
HEAVY_JOBS = (
    "check-tests", "gateway", "postgres-differential",
    "control-plane-ha", "formal-verify", "docker-build",
)


def scope_for_paths(paths: list[str]) -> str:
    # Unknown paths, including executable documentation and fixtures, require full CI.
    return "prose" if paths and all(
        path in PROSE_FILES or (path.startswith(PROSE_DIRS) and path.endswith(".md"))
        for path in paths
    ) else "full"


def scope_for_event(event_name: str, event: dict) -> str:
    if event_name != "pull_request":
        return "full"
    base = event["pull_request"]["base"]["sha"]
    head = event["pull_request"]["head"]["sha"]
    if not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        raise ValueError("invalid PR commit identity")
    # Disabling rename detection includes both old and new names; NULs preserve filenames.
    diff = subprocess.run(
        ["git", "diff", "--no-ext-diff", "--no-textconv", "--name-only",
         "--no-renames", "-z", f"{base}...{head}", "--"],
        check=True, capture_output=True,
    ).stdout
    if not diff or not diff.endswith(b"\0"):
        return "full"
    return scope_for_paths(os.fsdecode(diff).split("\0")[:-1])


def check_results(needs: dict) -> str:
    classification = needs.get("ci-policy", {})
    outputs = classification.get("outputs", {})
    scope = outputs.get("scope")
    expected_expensive = {"full": "true", "prose": "false"}.get(scope)
    if (classification.get("result") != "success"
            or expected_expensive is None
            or outputs.get("expensive") != expected_expensive):
        raise ValueError("CI classification did not succeed with a valid scope")
    expected = {"ci-policy", "check-quality", "verus-verify",
                "nexmark-regression-gate", "benchmark", *HEAVY_JOBS}
    if set(needs) != expected:
        raise ValueError("CI dependency set differs from the required jobs")
    for job in sorted(expected - {"ci-policy"}):
        allowed = {"success", "skipped"} if scope == "prose" and job in HEAVY_JOBS else {"success"}
        result = needs[job].get("result")
        if result not in allowed:
            raise ValueError(f"{job}: unexpected result {result!r} for {scope} CI")
    return f"Check passed ({scope} CI); excluded jobs are not execution evidence."


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("scope", "check"))
    args = parser.parse_args()
    if args.command == "scope":
        scope = "full"
        try:
            event_name = os.environ.get("GITHUB_EVENT_NAME", "")
            if event_name == "pull_request":
                event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
                scope = scope_for_event(event_name, event)
        except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
            print(f"Cannot classify changes; using full CI: {error}", file=sys.stderr)
        print(f"CI scope: {scope}", file=sys.stderr)
        print(f"scope={scope}")
        return 0
    try:
        print(check_results(json.loads(os.environ["CI_NEEDS"])))
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

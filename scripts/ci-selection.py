#!/usr/bin/env python3
"""Select expensive CI work; only an explicit documentation allowlist is cheap."""
import json
import os
from pathlib import Path
import subprocess
import importlib.util

_SCOPE = Path(__file__).with_name("ci-scope.py")
_SPEC = importlib.util.spec_from_file_location("ci_scope", _SCOPE)
_SCOPE_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_SCOPE_MODULE)


def selection(paths: list[str], event: str) -> dict:
    # One conservative allowlist is shared with the required-check gate.
    scope = _SCOPE_MODULE.scope_for_paths(paths) if event == "pull_request" else "full"
    excluded = scope == "prose"
    return {
        "expensive": not excluded,
        "scope": scope,
        "reason": "prose-only PR" if excluded else "full qualification required",
        "paths": paths,
    }

def main() -> None:
    event = os.environ["GITHUB_EVENT_NAME"]
    paths = []
    if event == "pull_request":
        payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
        base = payload["pull_request"]["base"]["sha"]
        proc = subprocess.run(
            ["git", "diff", "--name-only", "--no-renames", "-z", base, os.environ["GITHUB_SHA"]],
            check=True, capture_output=True, text=True,
        )
        paths = [path for path in proc.stdout.split("\0") if path]
    result = {"revision": os.environ["GITHUB_SHA"], **selection(paths, event)}
    Path("target").mkdir(exist_ok=True)
    Path("target/ci-selection.json").write_text(json.dumps(result, indent=2) + "\n")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"expensive={str(result['expensive']).lower()}\n")
        output.write(f"scope={result['scope']}\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()

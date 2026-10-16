"""Bounded scratch workspace. Requests and results are explicit, immutable Git commits.

Never updates main or a pull-request branch. Test results refer to one exact source
revision; successful controller operation is not a test or capacity verdict.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import subprocess
import time
import traceback

ROOT = Path.cwd().resolve()
RUN = os.environ["GITHUB_RUN_ID"]
REQUEST_REF = "refs/heads/codex/132-session"
RESULT_REF = "refs/heads/codex/132-session-result-" + RUN
STOP = time.monotonic() + 110 * 60
MAX_LOG = 2 * 1024 * 1024
MAX_REQUEST = 8 * 1024 * 1024
REPORTS: dict[str, bytes] = {}
RESULT_PARENT: str | None = None


def command(args: list[str], *, timeout: float = 120, data: bytes | None = None,
            check: bool = True, env: dict[str, str] | None = None) -> tuple[int, bytes]:
    remaining = STOP - time.monotonic()
    if remaining <= 15:
        raise TimeoutError("workspace lifetime reached")
    proc = subprocess.Popen(args, cwd=ROOT, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            env=env, start_new_session=True)
    try:
        output, _ = proc.communicate(data, timeout=min(timeout, remaining - 10))
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            output, _ = proc.communicate(timeout=3)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            output, _ = proc.communicate()
        output += b"\nWORKSPACE_COMMAND_TIMEOUT\n"
        if check:
            raise RuntimeError(output[-MAX_LOG:].decode("utf-8", "replace"))
        return 124, output[-MAX_LOG:]
    output = output[-MAX_LOG:]
    if check and proc.returncode:
        raise RuntimeError(output.decode("utf-8", "replace"))
    return proc.returncode, output


def git(*args: str, data: bytes | None = None) -> str:
    return command(["git", *args], data=data)[1].decode().strip()


def safe_path(value: str) -> Path:
    p = PurePosixPath(value)
    if p.is_absolute() or ".." in p.parts or ".git" in p.parts:
        raise ValueError("unsafe repository path")
    target = ROOT.joinpath(*p.parts)
    target.resolve().relative_to(ROOT)
    return target


def publish(record: dict, log: bytes = b"") -> None:
    global RESULT_PARENT
    encoded = (json.dumps(record, indent=2, sort_keys=True) + "\n").encode()
    REPORTS["reports/latest.json"] = encoded
    REPORTS["reports/" + record["id"] + ".json"] = encoded
    if log:
        REPORTS["reports/" + record["id"] + ".log"] = log[-MAX_LOG:]
    index = Path(os.environ["RUNNER_TEMP"]) / ("v072-results-index-" + RUN)
    index.unlink(missing_ok=True)
    env = dict(os.environ, GIT_INDEX_FILE=str(index))
    command(["git", "read-tree", "--empty"], env=env)
    for path, content in sorted(REPORTS.items()):
        digest = git("hash-object", "-w", "--stdin", data=content)
        command(["git", "update-index", "--add", "--cacheinfo", "100644", digest, path], env=env)
    tree = command(["git", "write-tree"], env=env)[1].decode().strip()
    args = ["commit-tree", tree]
    if RESULT_PARENT:
        args += ["-p", RESULT_PARENT]
    revision = git(*args, data=("v072 workspace " + record["id"] + " " + record["status"] + "\n").encode())
    git("push", "origin", revision + ":" + RESULT_REF)
    RESULT_PARENT = revision
    print(json.dumps({"request": record["id"], "status": record["status"],
                      "result_commit": revision, "source": record.get("source")}), flush=True)


def checkout(base: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", base):
        raise ValueError("base must be an exact commit")
    if git("status", "--porcelain"):
        raise RuntimeError("refusing to discard an uncommitted source change")
    git("fetch", "--no-tags", "origin", base)
    git("checkout", "--detach", base)


def prepare(request: dict) -> str:
    message = request.get("message", "")
    if not isinstance(message, str) or not 0 < len(message) < 500 or "\x00" in message:
        raise ValueError("invalid commit message")
    git("add", "-A")
    if git("diff", "--cached", "--name-only"):
        git("commit", "-F", "-", data=(message + "\n").encode())
    revision = git("rev-parse", "HEAD")
    actual = git("log", "-1", "--format=%B")
    if git("rev-parse", "HEAD^") == request.get("base") and actual != message:
        raise RuntimeError("commit message readback mismatch")
    branch = "refs/heads/codex/132-candidate-" + RUN + "-" + request["id"]
    git("push", "origin", revision + ":" + branch)
    return revision


def process(request: dict, request_commit: str) -> tuple[dict, bytes]:
    kind = request["kind"]
    log = b""
    record = {"id": request["id"], "request_commit": request_commit,
              "kind": kind, "status": "running", "source": git("rev-parse", "HEAD"),
              "workspace_run": RUN}
    if kind == "stop":
        record["status"] = "stopped"
        return record, log
    if request.get("base"):
        checkout(request["base"])
        record["source"] = git("rev-parse", "HEAD")
    publish(record)
    if kind in ("patch", "format"):
        if kind == "patch":
            patch = command(["git", "show", request_commit + ":session/request.patch"])[1]
            if not patch or len(patch) >= MAX_LOG:
                raise ValueError("empty or oversized patch")
            command(["git", "apply", "--check", "-"], data=patch)
            command(["git", "apply", "-"], data=patch)
        if request.get("harness", False):
            log += command(["cargo", "fmt", "--manifest-path", "tools/r1-local-harness/Cargo.toml", "--all"], timeout=300)[1]
        else:
            log += command(["cargo", "fmt", "--all"], timeout=300)[1]
        git("diff", "--check")
        record["source"] = prepare(request)
        record["tree"] = git("rev-parse", "HEAD^{tree}")
        record["changed_files"] = git("diff", "--name-only", request["base"], "HEAD").splitlines()
        log += command(["git", "diff", request["base"], "HEAD"])[1]
        record["status"] = "prepared"
    elif kind == "inspect":
        for item in request.get("files", []):
            path = safe_path(item["path"])
            content = path.read_text().splitlines()
            log += ("\nFILE " + item["path"] + "\n").encode()
            if item.get("signatures", False):
                pattern = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:fn |struct |enum |trait |impl(?:<|\s))")
                selected = [(i, line) for i, line in enumerate(content, 1) if pattern.match(line)]
            else:
                start = max(1, int(item.get("start", 1)))
                end = min(len(content), start + 999, int(item.get("end", start + 199)))
                selected = [(i, content[i - 1]) for i in range(start, end + 1)]
            log += "\n".join(f"{i}: {line}" for i, line in selected).encode() + b"\n"
        for query in request.get("searches", []):
            path = safe_path(query.get("path", "crates"))
            pattern = re.compile(query["pattern"])
            count = 0
            for file in sorted(path.rglob(query.get("glob", "*.rs"))):
                for i, line in enumerate(file.read_text().splitlines(), 1):
                    if pattern.search(line):
                        log += f"{file.relative_to(ROOT)}:{i}: {line}\n".encode()
                        count += 1
                        if count >= 300:
                            break
                if count >= 300:
                    break
        record["status"] = "inspected"
    elif kind == "test":
        commands = request.get("commands", [])
        if not commands or len(commands) > 20:
            raise ValueError("test needs one to twenty explicit commands")
        record["checks"] = []
        for args in commands:
            if not isinstance(args, list) or not args or not all(isinstance(a, str) for a in args):
                raise ValueError("commands must be argument arrays")
            if args[0] not in ("cargo", "bash", "python3", "rustc", "git"):
                raise ValueError("command executable not allowed")
            code, output = command(args, timeout=float(request.get("timeout_seconds", 1800)), check=False)
            digest = hashlib.sha256(output).hexdigest()
            record["checks"].append({"command": args, "exit_code": code, "log_sha256": digest})
            log += ("\nCOMMAND " + json.dumps(args) + "\n").encode() + output + b"\n"
            publish(record, log)
            if code:
                break
        record["status"] = "passed" if all(c["exit_code"] == 0 for c in record["checks"]) else "failed"
    elif kind == "reset":
        # An explicit reset applies only to this disposable runner-owned checkout.
        git("reset", "--hard", "HEAD")
        git("clean", "-fd")
        record["status"] = "reset"
    else:
        raise ValueError("unknown request kind")
    return record, log


def main() -> None:
    git("config", "user.name", "github-actions[bot]")
    git("config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    seen: set[str] = set()
    idle = time.monotonic()
    publish({"id": "startup", "status": "ready", "source": git("rev-parse", "HEAD"), "workspace_run": RUN},
            command(["rustc", "--version", "--verbose"])[1])
    while time.monotonic() < STOP - 30 and len(seen) < 128:
        git("fetch", "--no-tags", "origin", REQUEST_REF)
        revision = git("rev-parse", "FETCH_HEAD")
        payload = command(["git", "show", revision + ":session/request.json"])[1]
        if len(payload) > MAX_REQUEST:
            raise ValueError("request is too large")
        request = json.loads(payload)
        identifier = request.get("id", "")
        if not isinstance(identifier, str) or not re.fullmatch(r"[A-Za-z0-9-]{1,80}", identifier):
            raise ValueError("invalid request id")
        if identifier in seen:
            if time.monotonic() - idle > 20 * 60:
                break
            time.sleep(10)
            continue
        seen.add(identifier)
        idle = time.monotonic()
        try:
            record, log = process(request, revision)
        except Exception:
            record = {"id": identifier, "request_commit": revision, "status": "failed",
                      "source": git("rev-parse", "HEAD"), "workspace_run": RUN}
            log = traceback.format_exc().encode()
        publish(record, log)
        if request["kind"] == "stop":
            return
    publish({"id": "shutdown", "status": "bounded-lifetime-ended", "source": git("rev-parse", "HEAD"), "workspace_run": RUN})


if __name__ == "__main__":
    main()

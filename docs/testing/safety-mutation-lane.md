# Bounded safety mutation lane

This is a weekly scheduled and manually dispatched pilot for four human-reviewed candidates: uncertain physical write ACK, catalog replay identity, stale frontier generation, and weighted join multiplicity. It is not a pull-request gate and does not claim end-to-end safety qualification.

## Bound and execution

- One ubuntu-24.04 runner with Rust/Cargo 1.95 and at most two Cargo build jobs.
- Four fixed mutants per run; each exact detector command has a 600-second timeout, and the job has a 30-minute hard limit.
- The workflow fetches the locked dependency set before the runner's offline checks.
- For each candidate, require the unchanged exact detector to pass, apply one source replacement in a temporary source copy, verify the intended assertion failure, restore the source byte-for-byte, then rerun the unchanged detector.
- Reject a dirty checkout, non-descendant source revision, changed candidate or detector file hash, missing/ambiguous source text, zero or skipped tests, unrelated failures, stale output artifacts, and any mutant survivor.
- Retain the candidate inventory, exact mutation diffs, full command logs, JSON result, human-readable matrix, wall time, child CPU, and peak child RSS for 30 days.

## Disk budget and cleanup

- The lane owns `target/safety-mutation-lane` as its Cargo target and keeps that cache at the existing workspace target path. Before compiling, it records free/available bytes and sizes for that target, candidate scratch, and output artifacts on every involved filesystem.
- Preflight requires 10 GiB available. If space is lower, only the lane-owned Cargo target is pruned and preflight is repeated; the lane exits `not_qualified` before compilation if the reserve is still short.
- Filesystem usage is sampled every 5 seconds. The runner stops the current Cargo process group and reports `resource_limit_not_qualified` below a 2 GiB reserve. An `ENOSPC` or quota error is reported as `disk_full_not_qualified`; neither can count as a caught mutant.
- The source copy lives under `$RUNNER_TEMP/safety-mutation-work`, is removed on normal exit and exception, and has an `always()` workflow cleanup step for cancellation/timeout. The workflow-owned Cargo cache is retained and only pruned when preflight needs space.
- The exact preflight threshold and measured cold/warm disk growth are recorded with the qualification evidence; self-tests use an injected capacity snapshot and an actual timed child process rather than consuming the runner's disk.

The inventory pins the reviewed source revision and SHA-256 of each mutation and detector file. A documentation-only commit can follow that revision; any change to a pinned source or detector file makes the run not qualified until the inventory is reviewed again.

## Current matrix limits

The selected tests exercise an uncertain write response, in-memory catalog recovery, the Rust frontier adapter/kernel boundary, and one exact weighted join history. They do not simulate power loss after ACK, qualify a remote object store, or test a separate stale-owner lease mutant. These remain explicit unsupported or not-run entries in the inventory and run report.

The schedule runs Wednesdays at 02:30 UTC on `main`; `workflow_dispatch` remains available. Each run is capped at 30 minutes and retains artifacts for 30 days. It does not run on pull requests or change branch protection.

## Observed cost

- Hosted mutation runs used 12.62 runner-minutes (40673174392) and 18.13 runner-minutes (40682424016). The latest measured campaign took 1,044.838 seconds, used 1,632.72 child CPU seconds, and peaked at 2.59 GB RSS. It ran four mutants; the two hosted runs after the per-command timeout increased from 300 to 600 seconds succeeded. One earlier attempt hit the old timeout.
- In run 40682424016, the four mutant invocations took 168.074 seconds total and 52.346 seconds median. Inventory/setup verification took 0.624 seconds; the first unchanged baseline took 367.956 seconds, including the cold target build. One of three hosted attempts hit the old command timeout; neither of the two attempts under the 600-second limit timed out. Two post-fix runs are too few to estimate a flake rate.
- At the weekly cadence, the latest observed run implies about 15.7 runner-hours per year; the 30-minute hard cap bounds this at 26 hours per year.
- Adjacent standard CI runs each had 31 completed jobs: PR #271 run 40639005694 used 470.77 aggregate runner-minutes, and PR #272 run 40656388591 used 450.95. These are representative, non-controlled runs; the difference is not attributable to this lane. The mutation job is separate, so it adds no PR-gate jobs.

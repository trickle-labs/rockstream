# Bounded safety mutation lane

This is an on-demand pilot for four human-reviewed candidates: uncertain physical write ACK, catalog replay identity, stale frontier generation, and weighted join multiplicity. It is not a pull-request gate and does not claim end-to-end safety qualification.

## Bound and execution

- One ubuntu-24.04 runner with Rust/Cargo 1.88 and at most two Cargo build jobs.
- Four fixed mutants per run; each exact detector command has a 120-second timeout, and the job has a 30-minute hard limit.
- For each candidate, require the unchanged exact detector to pass, apply one source replacement in a temporary source copy, verify the intended assertion failure, restore the source byte-for-byte, then rerun the unchanged detector.
- Reject a dirty checkout, non-descendant source revision, changed candidate or detector file hash, missing/ambiguous source text, zero or skipped tests, unrelated failures, stale output artifacts, and any mutant survivor.
- Retain the candidate inventory, exact mutation diffs, full command logs, JSON result, human-readable matrix, wall time, child CPU, and peak child RSS for 30 days.

The inventory pins the reviewed source revision and SHA-256 of each mutation and detector file. A documentation-only commit can follow that revision; any change to a pinned source or detector file makes the run not qualified until the inventory is reviewed again.

## Current matrix limits

The selected tests exercise an uncertain write response, in-memory catalog recovery, the Rust frontier adapter/kernel boundary, and one exact weighted join history. They do not simulate power loss after ACK, qualify a remote object store, or test a separate stale-owner lease mutant. These remain explicit unsupported or not-run entries in the inventory and run report.

The first GitHub Actions run must supply total and median mutant time, setup time, CPU, peak RSS, and timeout behavior. The proposed hard cap is 30 minutes; a recurring schedule is not enabled until that run establishes the cost and the bound is approved. The manual workflow does not run on pull requests or change branch protection.

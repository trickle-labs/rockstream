# CI test target and mandatory-case accounting (#110)

This page defines the test safety-gate contract. The source of truth for assignment is [`scripts/ci-test-matrix.json`](../scripts/ci-test-matrix.json), validated against **live Cargo metadata**, not a checked-in target count.

## Assignment and execution

`python3 scripts/ci-test-inventory.py check --out target/ci-test-results/assignment.json` discovers workspace packages and testable Cargo lib, binary, and integration targets, checks ownership, verifies feature and service requirements, and rejects unowned or stale simulation targets. Simulation integration targets have **explicit names**; other workspace test targets use package ownership. An added simulation target without an explicit group fails the PR gate. All testable targets of a newly introduced workspace package require an assignment. Non-test Cargo artifacts (examples, benchmarks, build scripts, the fuzz harness) are outside this libtest inventory.

`python3 scripts/ci-test-inventory.test.py` exercises negative controls for missing assignment, missing feature/service, empty mandatory selection, ignored mandatory case, missing terminal event, and unexpected in-test skip.

`python3 scripts/ci-test-inventory.py run --job <job>` compiles selected Cargo test harnesses with declared features, executes them with machine-readable libtest output, compares `--list` IDs with terminal execution events, and writes `target/ci-test-results/<job>.json`. Every record contains the exact `package::kind/target::case` ID, outcome, job/tier, active package features, declared services, and a reason for any limitation or selection omission. A binary compiling or listing its tests is **not** a passing execution. A harness exit failure or missing terminal event is fatal. A test which prints a `SKIP` marker while returning success is not credited. A missing Docker daemon or configured image is a failed prerequisite, never success.

A known `#[ignore]` case may only be recorded as a **limitation** if individually identified with a meaningful reason under `known_limitations`. These do **not** count as passes or prove the claimed behavior. Unknown ignored cases fail. The R1 real-worker gateway-to-worker source-transport proof and the three empty MinIO placeholders in `lfs_catalog.rs` are currently limitations. They must be implemented and removed from the limitation ledger before they can be claimed as passing mandatory proofs. The ordinary mandatory tests in their targets remain required.

## Ownership and expensive jobs

| Job key | Trigger | Scope |
| --- | --- | --- |
| `pr-workspace` | CI PR/push | Default testable Cargo targets in lightweight workspace packages |
| `pr-crates` | CI PR/push | Connectors, control, ops, runtime, storage and SQL, with per-package features |
| `pr-gateway` | CI PR/push | Gateway and TestContainers feature, requires Docker |
| `pr-sim-unit` | CI PR/push | Simulation library |
| `pr-sim` | CI PR/push | Explicitly assigned ordinary simulation integration targets |
| `pr-ha` | CI PR/push | Separate HA suite; Docker hard prerequisite |
| `nightly-seeds` | daily schedule, dispatch | 100,000-seed chaos, SQL fuzz seed harness, longer simulation soaks |
| `nightly-docker` | daily schedule, dispatch | Docker simulation/MinIO/recovery and resource tests |
| `release-docker` | tag or explicit dispatch | Real rolling-upgrade cases; requires two configured distinct image references |
| `daily-resource` | separate daily resource-soak workflow | Four-hour MinIO resource-lifecycle gate, independently reported (the existing dedicated workflow owns it) |

The first six jobs are called in `ci.yml`. Seeded, Docker and release suites are called in `test-accounting-expensive.yml`; the existing specialized resource, real-cluster chaos, and fuzz workflows remain independent reports. Release image variables `ROCKSTREAM_ROLLING_N_IMAGE` and `ROCKSTREAM_ROLLING_N_PLUS_1_IMAGE` must reference distinct, available images. If absent, the release job fails and the release claim remains blocked. The specialized real-cluster chaos job retains its per-cell SLO gate; no retry or non-blocking bypass is added.

**Mandatory full-scope PR safety checks, collectively:** `Check` (quality + five test shards), `Control-plane HA smoke`, `Formal verify (M1–M4, M6–M7)`, both `Verus verify and qualify (VS6)` platform checks, `Nexmark regression gate`, `Performance-regression gate (ops/storage/runtime/control)`, and `Docker image (release binary)`. The `Check` aggregator depends on these jobs and the change classifier. Full-scope checks fail if any dependency is failed, skipped, or cancelled. Only the explicit prose-only exclusions below permit skipped jobs. **Do not use an older green `Check` run as evidence that independently scheduled checks have passed.** Check the individual job conclusions for the PR commit.

### PR change scope

[`scripts/ci-scope.py`](../scripts/ci-scope.py) selects lightweight CI only when every changed path is allowlisted prose: `AGENTS.md`, `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`, `ROADMAP.md`, `NEW_ROADMAP.md`, `.github/copilot-instructions.md`, or Markdown under `.github/prompts/`, `.github/skills/`, `docs/agents/`, `docs/implementation-plans/`, and `sign-offs/`. Documentation, capability, sign-off, source-policy, and checker self-tests still run in `Check quality`.

All other paths, mixed changes, empty diffs, unavailable change information, and all main/tag pushes use full CI. The classifier reads the complete Git diff with rename detection disabled, so both old and new paths count and filenames are NUL-delimited. Root `README.md`, executable documentation, generated references, fixtures, contracts, CI configuration, and the classifier itself require full CI.

Prose-only PRs omit Rust quality/build steps, Cargo target discovery, the five test shards, HA, PostgreSQL differential tests, formal models, benchmarks, and Docker builds. Both named Verus platform jobs still report, but explicitly exclude qualification steps and produce no qualification evidence. `Check` requires successful classification, quality, and both platform reports, and accepts skipped results only for the seven explicitly excluded job IDs. Failures, cancellations, missing jobs, and invalid classification remain fatal. A green prose-only PR is not test-execution or release-qualification evidence. Required check names and branch rules remain unchanged.

Run `python3 -B scripts/ci-scope.test.py` for classifier and aggregator regressions, including real Git renames, deletions, mixed changes beyond API file-list limits, and missing or unsuccessful required checks.

**Separate scheduled/release safety reports:** `Test Accounting — Expensive Suites` (`Nightly seeded simulation accounting`, `Nightly Docker test accounting`, `Release rolling-upgrade test accounting`); `Simulation Soak`; `Long-Duration Resource Soak`; `Real Cluster Chaos Soak`; `Coverage`; `Scheduled Fuzz Soak`. These may not run on every PR and are **not implied** by green PR checks. Their own red conclusions block any safety or release claim requiring them; record their run URLs and evidence artifacts explicitly in release sign-off. The issue does not authorize claiming formal, HA, or release proof from absent/skipped jobs.

## Boundaries

The libtest accounting report proves that tests were selected and reported results, **not** that a test's assertions faithfully exercise production behavior (see [test-suite audit](test-suite-audit-2026-09-21.md)). Benchmarks, doctests, cargo-fuzz engine execution, FizzBee, Verus, and long-lived soak reports are tracked by their separate existing tools; they cannot be inferred from this test-case manifest. A test that silently early-returns without printing a skip cannot be proven to execute its claimed scenario by libtest alone: such tests need explicit scenario assertions and fail-closed prerequisites. CI sets `ROCKSTREAM_REQUIRE_DOCKER=1` on Docker-owned test suites and preflights the daemon.

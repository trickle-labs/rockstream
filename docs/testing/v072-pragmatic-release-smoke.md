# v0.72.0 pragmatic release smoke

This smoke is **scope-specific** to the 2026-10-19 override in [issue #132](https://github.com/trickle-labs/rockstream/issues/132). It supplements but does not replace the normal required GitHub CI or the separate `scripts/run-release-qualification.sh` process. That broader script deliberately fails if unavailable full release-process observations are mistaken for live qualification: do not use `--fast` results as release proof.

## Candidate and prerequisites

Run from a clean checkout of the exact candidate on an environment with Rust 1.88, locked dependencies, sufficient RAM/disk, loopback binding, and available ports. Run real-cluster chaos/worker recovery in its dedicated Docker-enabled CI job; Docker-free in-memory tests are not a substitute for that real-cluster failure path. Set `V072_EXPECTED_SHA` to the exact planned tag SHA. Do not run against `main` while it advances.

```bash
git fetch origin main
git checkout <qualified-candidate-sha>
V072_EXPECTED_SHA=<qualified-candidate-sha> bash scripts/run-v072-pragmatic-smoke.sh
```

The script fails on a dirty tree, mismatched SHA, test process failure, zero executed tests, or ignored tests. It keeps per-command logs and the candidate SHA in `target/v072-release-smoke/<sha>/`. Record the compiler version, `Cargo.lock` hash, the finished status for each case and links to exact-commit CI/fuzz/chaos jobs before closing #132.

## Observable checks

| Requirement | Representative exercised path and expected result |
|---|---|
| Saturation/release | Group-commit byte/epoch/waiter bounds and error/retry rollback; quota lifecycle, source pressure and cancellation must fail closed and release reservations |
| Foreground fairness and ordered commits | A saturated, undrained operator chain cannot starve a separate valid chain; operator/frontier failures stop later epochs; bounded adaptive epochs do not change resulting data |
| Restart | Committed DML survives gateway task abort/restart, uncommitted work does not appear, and the independent expected query output matches recovery; real worker-loss/lease/fencing tests stay required in normal/chaos CI |
| Authorized interactive read | Unauthorized cursor/subscription operations are denied before producer work, cursor cancellation releases capacity, local SELECT scan failures remain errors, and recovered PgWire rows are exact |
| Complete bounded queries | DataFusion fallback rows/bytes and storage producer bounds reject overflow without passing partial results as complete |
| Committed subscriptions | Acknowledged epochs publish complete ordered data; failed durable flush cannot publish, slow consumers require resnapshot when replay is unavailable |
| Revisioned edits | Stale revisions and inadmissible memory-cap changes reject; requested vs effective owner state is truthful |
| Decoder | Both captured invalid CopyData/Execute frames and malformed SQL reject without a panic; a healthy PgWire client continues to return the correct result |

A separate report should include one overload/cancel/restart reproduction **on the same integrated candidate**. If a test is skipped because Docker, ports or external services are absent, record that as incomplete instead of treating it as passing. Tests are practical local correctness checks; do not claim Apple M5Pro latency, p99 service-level guarantees, cloud cost or broad distributed capacity from them.

## Release decision

- Confirm signed tag policy and required CI on the version-bumped candidate commit.
- Confirm decoder fuzz is green on the exact candidate (not an earlier PR head), and check any newer scheduled fuzz findings.
- Resolve the potential CLI checkpoint export false-success in [#221](https://github.com/trickle-labs/rockstream/issues/221) before publishing. Its targeted real-shard negative regression on [PR #231](https://github.com/trickle-labs/rockstream/pull/231) is pending; if it confirms a bug, require a fail-closed repair, no success marker, and a successful intact export/restore on the final candidate. An untested backup is not acceptance evidence.
- Close security issue #227 only when the known crashes and final integrated decoder checks are satisfied; close parent #132 only when this smoke and its limitations are recorded.
- Publish signed immutable `v0.72.0` tag via an authorized maintainer and release notes only after all gates pass. No retagging or branch-protection bypasses.

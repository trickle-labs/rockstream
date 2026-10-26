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

## Automated exact-commit execution

The [v0.72 pragmatic release smoke workflow](../../.github/workflows/v072-pragmatic-release-smoke.yml) runs the same 17-case script on a clean, pinned commit with Rust 1.88, and uploads the SHA, compiler version, lockfile digest, report and individual test logs **even on failure**. A passing job requires exactly 17 recorded PASS results, no skips, and zero failed processes. Pull-request execution tests the PR head; on merging this workflow into `main`, its push execution tests the resulting integrated commit. To qualify any later candidate without editing source, use **Actions → v0.72 pragmatic release smoke → Run workflow** on `main`, then verify the workflow's recorded `source-sha.txt` equals the immutable proposed tag commit.

This is a representative loopback/local supported-path smoke executed on a CI runner, **not** an Apple M5 Pro benchmark, an external cloud-capacity qualification, or proof of all distributed failures. Retain the exact-head CI, decoder fuzz and real Docker-enabled cluster-chaos links separately. Do not close #132, mark the roadmap Done or sign a release merely because the workflow was created: first inspect a successful executed report for the final integrated SHA, including an overload/cancel/restart path, and record its artifact and limitations.

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
- Confirm the decoder engine actually ran on the exact candidate SHA; inspect its per-target reports and runner failure/cancellation evidence, and check newer scheduled findings.
- Checkpoint export safety issue [#221](https://github.com/trickle-labs/rockstream/issues/221) is fixed and closed; security decoder issue [#227](https://github.com/trickle-labs/rockstream/issues/227) is closed following verified fixes. Reopen either only if new evidence identifies a regression.
- Close parent [#132](https://github.com/trickle-labs/rockstream/issues/132) only after its 2026-10-19 pragmatic acceptance outcomes, exact candidate SHA, commands, actual results, limitations, and retained-evidence links are recorded.
- Publish signed immutable `v0.72.0` only from the exact fully qualified `main` SHA and through an authorized maintainer. No retagging or branch-protection bypasses.

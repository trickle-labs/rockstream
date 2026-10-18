# Test commands

Run the checks that match the change. The commands below are the repository's
current command taxonomy.

```console
$ make build
$ make fmt
$ make clippy
$ make test
$ make error-codes
$ make exit-criteria
$ make documentation
$ make check
$ make verify-proof-contracts
$ bash scripts/check-verus-qualification.test.sh
$ cargo deny check
$ cargo audit
$ bash scripts/check-documentation.test.sh
$ cargo test -p rockstream-cli --test documentation_transcript_tests
```

The CI workflow runs the same Rust checks and the scheduled dependency audit.
Use the [dependency policy](../DEPENDENCY_POLICY.md) for license and advisory
rules.

Qualification execution checks:

```console
$ bash scripts/check-qualification-mutations.test.sh
$ bash scripts/check-qualification-mutations.sh --output target/qualification-mutations.json
$ bash scripts/run-release-qualification.sh --fast --output target/qualification-helpers.json
$ bash scripts/run-release-qualification.sh --suite recovery --output target/real-cluster-chaos/qualification.json
```

The first command tests the runner with compiled libtest fixtures, including
comment-only, failing negative-control, and ignored-test inputs. The other two
execute the named Rust assertions. `--suite` accepts `all`, `e2e`, `recovery`, or
`mutations`; recovery selections also execute the anti-cheat mutation checks.
An invalid selection exits before any prerequisite check or compilation with
`RS-0001: Unknown qualification suite: 'NAME' (expected: all, e2e, recovery, mutations)`.

Reports contain each executed scenario ID and status, each Cargo command and
exit, missing mandatory IDs, validation errors, and derived totals. They bind
the Git commit, tracked diff and untracked file digests, and lockfile digest to
the selected settings. Failed executions also write reports when `--output`
is supplied. Incomplete, malformed, duplicate, ignored, or failed results never
produce a passing report. There is no CLI build step: Cargo builds the selected
test targets, and the unused CLI binary cannot contribute qualification evidence.

`--fast`/`--quick`, `ROCKSTREAM_QUALIFY_FAST`, and
`ROCKSTREAM_QUALIFY_MOCK_ENV` select **helper evidence only**, recorded as
`evidence_kind: helper` and `release_qualified: false`. CI runs these helpers
separately from release qualification. The current qualification targets use
in-memory topology and supplied observations; their `test_helper_*` names
describe bookkeeping, never application recovery, Kafka atomicity, CDC, or PGWire
proof. Capability references must use the actual scenario for application claims.

Without fast mode, the runner requires Docker before compilation. For `recovery`
(or `mutations`), it provisions Kafka and MinIO through the fixed
[`worker-failure-kafka-minio-v1` scenario](testing/worker-failure-recovery.md).
The manifest embeds the entire observed view, sink, source offsets, authority
transition, first query, event timestamps, derived recovery durations, and exact
candidate identity. Each run uses a fresh artifact directory, and a candidate
change during execution fails qualification. Reachable ambient services and
old artifacts cannot satisfy the requirement.

The same executed Rust validator checks six complete corrupted records: missing
detection event, substituted constant durations, stale query frontier, changed
query value, duplicate consumed sink output, and a helper-only status transition
with no new worker process. The runner compares the complete records and their
specific failure reasons, rather than accepting a passing test name alone.

Successful `release_qualified: true` applies only to the listed
`qualification_scope: [worker-failure-kafka-minio-v1]`. `unsupported_claims`
explicitly excludes general Kafka recovery, PostgreSQL CDC, distributed control
HA, PGWire transport, and p99 recovery. `all` and `e2e` still fail with missing
`live::e2e` evidence; helper coverage cannot satisfy those mandatory claims.
The real-cluster chaos CI job runs the integrated recovery suite and uploads the
complete successful and corrupted records along with worker logs.

`--check-prerequisites` only reports prerequisite availability; it never runs
scenarios or qualifies a release.

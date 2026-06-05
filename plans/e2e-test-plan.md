# RockStream E2E Test Plan

This document defines the end-to-end test plan for RockStream after v0.52.0.
It is intentionally black-box: the suite must exercise the public binary,
the public pgwire protocol, and the practical SQL surface through external
clients. Internal Rust APIs can still have unit and property tests, but they do
not count as end-to-end proof for the public interface.

The suite must use TestContainers for the RockStream server, a `psql` client
container, and MinIO. The protocol-level client library used inside the test
harness should be `tokio-postgres`. AWS S3 and other external cloud services
are explicitly out of scope for this plan.

## Scope And Principles

The plan is built around the public surface that users actually touch:

- the `rockstream` binary commands: `start`, `bootstrap`, `explain`, `sql`,
  `version`, `describe`, `debug arrangement`, `support-bundle`, and `tune`;
- the pgwire startup, authentication, simple query, extended query, notices,
  catalog reflection, transaction, and error paths;
- the supported SQL surface, including DDL, DML, query planning, historical
  reads, subscribe semantics, system tables, admin statements, and explain
  output;
- the storage-backed paths that must work against a real object store, using
  MinIO rather than mocks.

The plan has four guiding rules.

1. Every user-visible path must be exercised from the outside, through a real
   process boundary.
2. `tokio-postgres` is the protocol oracle; `psql` is the compatibility
   witness. Neither one replaces the other.
3. Every object-store-backed behavior must be validated against MinIO.
4. No AWS-specific assumptions should appear in this plan. Those tests belong
   in a later roadmap once cloud services become part of the acceptance scope.

## Shared Test Harness

The recommended harness is a dedicated E2E test crate or a workspace-level test
package that owns all container orchestration. The harness should build the
release `rockstream` binary once, place it in a container image, and then run
the same binary in multiple topologies.

### Required fixtures

- A RockStream container image that can run the binary in each public role:
  `--role=all`, `--role=control`, `--role=worker`, `--role=gateway`, and
  `--role=frontier`.
- A `psql` container, ideally from an official Postgres image, used only as a
  client to run SQL scripts and validate interactive compatibility.
- A MinIO container with a disposable bucket namespace per test.
v0.52.1 through v0.52.5. The order matters. Earlier versions establish the harness
- A unique storage root per test so that no scenario can pass because of stale
  state from another scenario.

### Standard harness behavior
like a real service, not a CLI stub. This version should make the process
- Build the RockStream image from the current workspace revision, not from a
  cached image that might have an older binary.
- Assign stable container aliases for discovery, such as `control`,
- Treat every command line invocation as a contract. The test should validate
  storage.

### Minimum assertions for every scenario
- Exit code or SQLSTATE is correct.
- Result shape is correct.
- The relevant audit or support artifact exists when it should.
- The relevant container logs do not show a panic, crash loop, or silent
  protocol failure.
- Any object-store side effect is visible in MinIO, not only in memory.

### Surface inventory that must eventually be covered

- CLI parsing, help text, and command dispatch for all public subcommands.
- pgwire startup, authentication, simple query, extended query, and notices.
- Catalog reflection through `pg_catalog` and `information_schema`.
- DDL for views, indexes, sources, sinks, secrets, schemas, and admin DDL.
- DML for `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING`.
- Query semantics for joins, aggregates, windows, historical reads, and
  subscribe semantics.
  SCHEMA_EVOLUTION`, `EXPLAIN INCREMENTAL`, `EXPLAIN INDEX`, `describe`,
  `debug arrangement`, `support-bundle`, and `tune`.
- Storage-backed checkpoint, WAL, compaction, retention, and recovery paths.

## Roadmap Version Coverage
The plan is split into the five roadmap versions immediately after v0.52.0:
v0.52.1 through v0.52.5. The order matters. Earlier versions establish the harness
and the binary contract; later versions expand into SQL coverage, recovery,
MinIO-backed persistence, and catalog server behavior.
### v0.52.1 - Storage Format Gate, Rolling Upgrade, Migration Skeleton, Security Review

The purpose of the v0.52.1 E2E block is to prove that the public binary behaves
like a real service, not a CLI stub. This version should make the process
surface boring: the binary starts, stops, joins, bootstraps, rejects bad input,

#### Required topology

- One `rockstream` container in `--role=all` mode.
- One `rockstream` container in `--role=control` mode.
- One `rockstream` container in `--role=worker` mode.
- One `rockstream` container in `--role=gateway` mode.

#### Mandatory scenarios

- CLI help snapshots for every public command.
  - `rockstream --help`
  - `rockstream bootstrap --help`
  - `rockstream sql --help`
  - `rockstream describe --help`
  - `rockstream debug arrangement --help`
  - `rockstream support-bundle --help`
  - `rockstream tune --help`
- Start role matrix.
  - `--role=all` starts the combined control, worker, and gateway profile.
  - `--role=control` listens and stays alive.
  - `--role=worker` fails if `--control` is missing and succeeds when it is
    present.
  - `--role=gateway` and `--role=frontier` are explicitly smoke-tested as
- Bootstrap flow.
  - The same command fails cleanly when the control service is absent.
    sentinel registration completed.
- Error-path contract.
  - Invalid TLS flag combinations fail with the expected code and message.
    endpoints fail with stable error output.
  - Non-zero exit status is part of the contract and must be asserted.
- Artifact contract.
  - `audit.jsonl` is written for startup and shutdown.
  - `support-bundle` is written when requested and contains the expected JSON
    fields.
    key/value pairs in a controlled way.
- Rolling upgrade smoke.
  - Start the binary, stop it, restart it with the same storage root, and prove
    the service starts again without manual cleanup.
  - This scenario is still shallow in v0.52.1, but it must establish the

#### Required assertions

- Every command above exits with a deterministic status code.
- Every command above emits output that matches the docs and help text.
- No startup path can succeed while silently skipping audit logging.
  a role started; readiness must come from a port, a transcript, or a file
  artifact.

#### v0.52.1 pass criteria
- The binary contract is stable enough that later SQL and storage tests can use
  it as a fixture without per-test setup hacks.
- The help and command-dispatch surface is fully enumerated once and locked in
  as a snapshot.
- The basic upgrade and restart flow is proven against a real process.


The purpose of v0.52.2 is to make the gateway genuinely usable from `psql` and
`tokio-postgres`. This is the version where the test suite starts treating the
pgwire protocol as a first-class public interface instead of a narrow demo

#### Required topology

- A split cluster with at least one control container, one gateway container,
  and one worker container.
- Host-side `tokio-postgres` clients for protocol assertions that `psql` cannot
  express cleanly.
- MinIO-backed storage for any path that writes or reads object-store state.

#### Mandatory scenarios

- Startup handshake.
  - Verify that `tokio-postgres` can connect with database, user, and optional
    output.
  - Test authenticated and unauthenticated startup paths.
  - Test cross-tenant reads and writes if the configured public surface exposes
    them.
  - Assert that the session state survives normal query traffic but does not
- Simple query coverage.
  - `SHOW` and `SET` statements that are part of the supported SQL surface.
  - `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` succeeds.
  - `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns the documented
    rejection.
- Extended query coverage.
  - Parse/Bind/Execute round trips through `tokio-postgres`.
  - Prepared statements with parameters.
  - Reuse of prepared statements across multiple executions.
  - Error handling for bad parameter counts and malformed plans.
- Catalog reflection.
  - `pg_catalog.pg_type`.
  - `information_schema.columns`.
    placeholders.
  - `CREATE VIEW` and `DROP VIEW`.
  - `CREATE INDEX`, `DROP INDEX`, `REBUILD INDEX`, and `EXPLAIN INDEX`.
  - `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING`.
- Query semantics.
  - A join query.
  - An aggregate query.
  - A window query if it is in the supported SQL subset.
  - A subscribe query if that surface is already live.
- Notices and error codes.
  - `RS-2003` for unsupported isolation levels.
  - `RS-2004` for inline-view dependency violations.
  - `RS-2008` for optimistic write conflicts.
  - Any supported `RS-10xx` or `RS-20xx` code must be asserted with both code
    and message.
#### Required assertions

- `psql` must receive the same logical rows that `tokio-postgres` sees.
- Field OIDs must match the schema tags for all columns.
- Every supported SQL feature must have at least one positive path and one
  negative path where the negative path exists.
- The suite must never rely on textual row pretty-printing as the primary
  oracle.
#### v0.52.2 pass criteria

- The gateway can be driven from a real Postgres client and a native Rust
- The SQL surface is no longer a parser-only contract; it is observable through
  a live server.
- The test suite can detect row-shape, metadata, and notice regressions rather
  than only result-value regressions.

### v0.52.3 - Production Beta Handoff
The purpose of v0.52.3 is to prove that the binary can be operated by someone who
restart behavior, and admin surfaces become hard requirements.

- A split cluster with separate control, worker, and gateway containers.
- At least one test that uses the embedded `--role=all` mode to prove the
  single-binary developer story still works.

#### Mandatory scenarios

- Multi-process cluster lifecycle.
  - Start control first, then worker, then gateway.
  - Restart one role at a time and prove the others stay reachable.
  - Restart the full cluster against the same storage state and prove the
    service recovers cleanly.
- Public admin surface.
  - `debug arrangement` prints a stable arrangement header and diagnostic
  - `support-bundle` contains the expected operational artifacts.
  - `SHOW RESOURCE USAGE` and `SHOW SCHEMA_EVOLUTION` paths should be exercised
    through `psql` and the native protocol client.
- Query robustness under churn.
  - Keep one client connected through a worker restart.
  - Prove that reconnects after a control restart still preserve the expected
    SQL-visible state once the cluster comes back.
- Auth and policy surfaces if they are public by this point.
  - Unauthenticated requests are rejected.
  - Cross-tenant requests are rejected.
  - Admin users can still read across tenant boundaries when the policy allows
- Rate limiting and timeout surfaces.
  - Explicitly assert timeout behavior for a slow query.
  - Explicitly assert rate-limit behavior for a burst of short queries.
  - Make sure the error text is actionable and stable.
- Documentation parity.
  - If the docs claim a subcommand or flag exists, the binary must expose it in
    the same form.

#### Required assertions

- The admin surface must work against a live cluster, not stubbed in-memory
- Restart tests must prove the storage root is durable and that the cluster can
  reattach to it.
- `psql` must still be able to connect after the cluster is restarted.
- The suite must collect logs and artifacts on failure so production handoff
  issues are diagnosable from CI output.
#### v0.52.3 pass criteria

- The public binary feels like an operable service rather than a demo process.
- Restart, recovery, and admin behavior are all externally visible and stable.
- The same test harness can be used for pre-release operational rehearsals.

### v0.52.4 - Cold-Tier Parquet and Iceberg Sink With Law Metadata
and inspectable. This version should validate object-store behavior directly,

#### Required topology

- A RockStream cluster that uses MinIO-backed storage for the persistence paths
- Host-side `tokio-postgres` clients to compare logical results while MinIO is
  being exercised.

#### Mandatory scenarios

- WAL and checkpoint behavior.
  - Append data, force a checkpoint, restart the cluster, and prove the visible
    state is preserved.
  - Verify that WAL listing is not on the hot path once the listing cache is in
    place.
  - Ensure the test can observe real object-store artifacts in MinIO after the
    cleanup logic says they should.
  - Verify that a crash during flush or compaction does not leave the storage in
    a state that cannot be recovered.
- Cold-tier sink, if the version exposes it.
  - Wait for the snapshot interval to trigger real object writes.
  - Inspect the resulting files and manifests directly in MinIO.
  - Assert that finalized values are written, not raw intermediate operands.
  - Assert that the cold snapshot can be merged with the hot tail to produce the
- MinIO failure and restart behavior.
  - Stop MinIO mid-run and confirm the cluster reports a failure rather than a
    silent success.
  - The harness should check that retry behavior and error propagation are
    visible at the SQL or CLI layer.
- Object-store visibility.
  - Tests should list the bucket contents and assert on the presence or absence
  - No test should pass solely because an in-memory data structure still holds
    the expected value.


- Every storage-backed path must prove its effect against MinIO.
- The object layout must be stable enough to support later cold-tier and
  catalog tests.
- The cluster must remain queryable while the storage layer is being restarted
  or temporarily unavailable.
- MinIO is no longer just a fixture; it is part of the tested contract.
- Recovery, compaction, and retention are visible in the bucket contents.
- Storage regressions can be diagnosed from object listings and transcripts,
  not just from internal metrics.

surfaces are coherent with the SQL gateway. The HTTP catalog surface should be
tested locally and deterministically, without bringing in external query
engines or cloud catalog services.

#### Required topology


#### Mandatory scenarios

- Catalog registration.
  - After a write or DDL change, the catalog must reflect the new namespace,
    table, view, or snapshot state.
  - Restart the cluster and verify the catalog remains consistent with the
    durable state.
  - Assert the status code, schema, and payload shape.
- Metadata coherence.
  - If the catalog exposes namespaces, tables, or snapshots, the test must
- Auth and transport propagation.
  - If the endpoint is protected, assert the unauthenticated failure mode.
  - If mTLS or tokens are enabled, assert that the catalog layer sees the same
    identity that the gateway sees.
- Regression matrix.
  - Rerun the critical smoke tests from v0.52.1 through v0.52.4 in a single cluster
    so the catalog work cannot accidentally break the binary, SQL, or storage
    surfaces.

#### Required assertions

- HTTP catalog behavior must stay in sync with SQL-visible metadata.
- The catalog server must not become a second source of truth.
- The local HTTP tests must stay deterministic and must not depend on external
  catalog services.

#### v0.52.5 pass criteria

- The SQL gateway and the catalog endpoint describe the same world.
- Local interoperability is proven without requiring Spark, Trino, DuckDB, or
  AWS S3.
- The suite is now broad enough that a change to any public surface is likely to
  fail somewhere visible.

### v0.52.6 - SQL Plan Lowering and Exact Result Assertions

This version verifies SQL compilation, planning, and lowering to `PlanNode` IR end-to-end via the pgwire gateway.

#### Mandatory scenarios
- **SQL Explain API**
  - Execute `EXPLAIN <query>` for SELECT, JOIN, AGGREGATE, and WINDOW queries.
  - Verify that the gateway returns the correct lowered plan structures (e.g. `Join`, `Aggregate`, `Window`).
  - Verify that the plan contains correct merge law annotations (e.g. `WeightAdd/v1`, `MaxRegister/v1`) and `not_merge_safe_reason` strings.
- **Precise Query Value Assertions**
  - Verify that SELECT queries return exact column schemas (names, OIDs, and types).
  - Verify that query results are validated for exact values, row counts, and column types.

#### Required assertions
- `EXPLAIN SELECT * FROM a JOIN b ON a.id = b.id` returns a plan containing a `Join` operator.
- `EXPLAIN SELECT region, SUM(amount) FROM orders GROUP BY region` returns a plan containing `Aggregate` and merge law `WeightAdd/v1`.
- `EXPLAIN SELECT region, MAX(amount) FROM orders GROUP BY region` returns a plan containing `Aggregate`, `MaxRegister/v1`, and reason `extremum_requires_rmw`.
- SELECT results are matched exactly on row values rather than just asserting non-emptiness.

#### v0.52.6 pass criteria
- The SQL compiler/lowerer is exercised directly through public PG gateway connections.
- Query result assertions are strict, verifying value correctness, column metadata, and types.

### v0.52.7 - Advanced Diagnostics and Auto-Tuning CLI Validation

This version validates CLI diagnostic subcommands (`tune`, `describe`, `debug-arrangement`, and `support-bundle`) against active/simulated cluster states.

#### Mandatory scenarios
- **Pipeline Description**
  - Run `rockstream describe <pipeline>` and verify it prints a structured ASCII/Unicode DAG of the pipeline.
- **Arrangement Debugging**
  - Run `rockstream debug-arrangement <view> <op-id> <key>` and verify it decodes the arrangement key/metadata against registered merge laws and prints compaction metrics.
- **Auto-Tuning Configuration**
  - Run `rockstream tune --override` and verify it writes the overridden hysteresis configuration correctly into the storage directory.
- **Diagnostic Archive Compilation**
  - Run `rockstream support-bundle` and verify it compiles a valid `.tar.gz` bundle containing the real `audit.jsonl` log file.

#### Required assertions
- `rockstream describe` outputs expected DAG nodes (e.g. `Source`, `Filter`, `Project`, `Sink`).
- `rockstream debug-arrangement` resolves the target view in the catalog and prints the correct merge law.
- `rockstream tune` overrides write to `tuned_config.json` inside the storage root.
- The `support-bundle` tarball is generated and contains the correct files.

#### v0.52.7 pass criteria
- Diagnostic and tuning CLI tools are verified end-to-end against real/simulated storage states.


## What Is Deliberately Out Of Scope

The following are not part of this plan yet:

- AWS S3 or any other external cloud object store.
- Spark, Trino, DuckDB, or other external query engines as acceptance gates.
- Multi-region topologies.
- Performance-only benchmarks without correctness assertions.
- Internal Rust module coverage that cannot be observed through a public
  interface.

Those can come later, but they should not dilute the v0.52.1-v0.52.5 E2E plan.

## Final Exit Criteria For The Whole Plan

This plan is complete when all of the following are true:

- Every public `rockstream` subcommand has at least one external E2E test.
- Every supported pgwire path has both a `tokio-postgres` assertion and a
  `psql` compatibility check where applicable.
- Every supported SQL feature has at least one positive-path E2E test, plus a
  negative-path test when the feature is supposed to reject unsupported input.
- Every object-store-backed path is verified against MinIO.
- Every roadmap version from v0.52.1 to v0.52.5 has a dedicated, externally
  observable regression suite.
- The suite can be rerun from a clean checkout without manual repair steps.

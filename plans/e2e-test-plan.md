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
- A host-side `tokio-postgres` client in the Rust test process.
- A unique storage root per test so that no scenario can pass because of stale
  state from another scenario.

### Standard harness behavior

- Build the RockStream image from the current workspace revision, not from a
  cached image that might have an older binary.
- Assign stable container aliases for discovery, such as `control`,
  `worker`, `gateway`, `psql`, and `minio`.
- Capture stdout, stderr, container logs, and protocol transcripts for every
  failing test.
- Wait on externally observable readiness signals only. Do not rely on sleeps.
- Use fixed seeds for any randomized workload and persist the seed when a test
  fails.
- Treat every command line invocation as a contract. The test should validate
  exit status, stderr/stdout shape, and any side effects on disk or object
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
- Cluster bootstrap and multi-role startup flows.
- pgwire startup, authentication, simple query, extended query, and notices.
- Catalog reflection through `pg_catalog` and `information_schema`.
- DDL for views, indexes, sources, sinks, secrets, schemas, and admin DDL.
- DML for `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING`.
- Query semantics for joins, aggregates, windows, historical reads, and
  subscribe semantics.
- Admin and diagnostic surfaces such as `SHOW RESOURCE USAGE`, `SHOW
  SCHEMA_EVOLUTION`, `EXPLAIN INCREMENTAL`, `EXPLAIN INDEX`, `describe`,
  `debug arrangement`, `support-bundle`, and `tune`.
- Storage-backed checkpoint, WAL, compaction, retention, and recovery paths.

## Roadmap Version Coverage

The plan is split into the five roadmap versions immediately after v0.52.0:
v0.53 through v0.57. The order matters. Earlier versions establish the harness
and the binary contract; later versions expand into SQL coverage, recovery,
MinIO-backed persistence, and catalog server behavior.

### v0.53 - Storage Format Gate, Rolling Upgrade, Migration Skeleton, Security Review

The purpose of the v0.53 E2E block is to prove that the public binary behaves
like a real service, not a CLI stub. This version should make the process
surface boring: the binary starts, stops, joins, bootstraps, rejects bad input,
and writes expected artifacts every time.

#### Required topology

- One `rockstream` container in `--role=all` mode.
- One `rockstream` container in `--role=control` mode.
- One `rockstream` container in `--role=worker` mode.
- One `rockstream` container in `--role=gateway` mode.
- One `rockstream` container in `--role=frontier` mode.
- One `psql` client container, even if only used for future versions, so the
  harness shape stays stable.
- One MinIO container, even if only used as a fixture for later storage tests.

#### Mandatory scenarios

- CLI help snapshots for every public command.
  - `rockstream --help`
  - `rockstream start --help`
  - `rockstream bootstrap --help`
  - `rockstream explain --help`
  - `rockstream sql --help`
  - `rockstream version`
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
    first-class roles, not accidental aliases.
- Bootstrap flow.
  - `rockstream bootstrap --control <addr>` succeeds against a live control
    container.
  - The same command fails cleanly when the control service is absent.
  - Bootstrap output must prove the control service is reachable and that the
    sentinel registration completed.
- Error-path contract.
  - Invalid TLS flag combinations fail with the expected code and message.
  - Missing storage root, invalid bind addresses, and unreachable control
    endpoints fail with stable error output.
  - Non-zero exit status is part of the contract and must be asserted.
- Artifact contract.
  - `audit.jsonl` is written for startup and shutdown.
  - `support-bundle` is written when requested and contains the expected JSON
    fields.
  - `tune --override` writes a deterministic overrides file and rejects invalid
    key/value pairs in a controlled way.
- Rolling upgrade smoke.
  - Start the binary, stop it, restart it with the same storage root, and prove
    the service starts again without manual cleanup.
  - This scenario is still shallow in v0.53, but it must establish the
    plumbing needed for the later upgrade tests.

#### Required assertions

- Every command above exits with a deterministic status code.
- Every command above emits output that matches the docs and help text.
- No startup path can succeed while silently skipping audit logging.
- The harness must not need host-side shell parsing of the binary to know that
  a role started; readiness must come from a port, a transcript, or a file
  artifact.

#### v0.53 pass criteria

- The binary contract is stable enough that later SQL and storage tests can use
  it as a fixture without per-test setup hacks.
- The help and command-dispatch surface is fully enumerated once and locked in
  as a snapshot.
- The basic upgrade and restart flow is proven against a real process.

### v0.54 - Long Soak, Merge Laws, and Optimistic Transactions

The purpose of v0.54 is to make the gateway genuinely usable from `psql` and
`tokio-postgres`. This is the version where the test suite starts treating the
pgwire protocol as a first-class public interface instead of a narrow demo
surface.

#### Required topology

- A split cluster with at least one control container, one gateway container,
  and one worker container.
- A `psql` client container connected over the network to the gateway port.
- Host-side `tokio-postgres` clients for protocol assertions that `psql` cannot
  express cleanly.
- MinIO-backed storage for any path that writes or reads object-store state.

#### Mandatory scenarios

- Startup handshake.
  - Verify that `tokio-postgres` can connect with database, user, and optional
    application name fields.
  - Verify the same startup path from `psql` using shell-style environment
    variables and command-line flags.
  - Capture and assert the startup banner, server parameters, and failure
    output.
- Authentication and session semantics.
  - Test authenticated and unauthenticated startup paths.
  - Test cross-tenant reads and writes if the configured public surface exposes
    them.
  - Assert that the session state survives normal query traffic but does not
    leak across clients.
- Simple query coverage.
  - `SELECT 1`.
  - `SHOW` and `SET` statements that are part of the supported SQL surface.
  - Transaction blocks with `BEGIN`, `COMMIT`, and `ROLLBACK`.
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
  - `pg_catalog.pg_namespace`.
  - `information_schema.tables`.
  - `information_schema.columns`.
  - Result metadata must expose the correct Postgres OIDs, not generic
    placeholders.
- SQL DDL and DML.
  - `CREATE VIEW` and `DROP VIEW`.
  - `CREATE INDEX`, `DROP INDEX`, `REBUILD INDEX`, and `EXPLAIN INDEX`.
  - `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING`.
  - Any supported source or sink DDL that is part of the public API.
- Query semantics.
  - A join query.
  - An aggregate query.
  - A window query if it is in the supported SQL subset.
  - A historical query such as `AS OF EPOCH` or `AS OF TIMESTAMP` if exposed.
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
- Notice text must be captured and asserted, not ignored.
- Every supported SQL feature must have at least one positive path and one
  negative path where the negative path exists.
- The suite must never rely on textual row pretty-printing as the primary
  oracle.

#### v0.54 pass criteria

- The gateway can be driven from a real Postgres client and a native Rust
  client without behavior drift.
- The SQL surface is no longer a parser-only contract; it is observable through
  a live server.
- The test suite can detect row-shape, metadata, and notice regressions rather
  than only result-value regressions.

### v0.55 - Production Beta Handoff

The purpose of v0.55 is to prove that the binary can be operated by someone who
did not build it. This is the version where multi-process topology, recovery,
restart behavior, and admin surfaces become hard requirements.

#### Required topology

- A split cluster with separate control, worker, and gateway containers.
- At least one test that uses the embedded `--role=all` mode to prove the
  single-binary developer story still works.
- A `psql` container and at least two host-side clients so concurrency and
  failover can be exercised at once.
- MinIO with persistent state across a controlled restart.

#### Mandatory scenarios

- Multi-process cluster lifecycle.
  - Start control first, then worker, then gateway.
  - Restart one role at a time and prove the others stay reachable.
  - Restart the full cluster against the same storage state and prove the
    service recovers cleanly.
- Public admin surface.
  - `describe` returns a meaningful pipeline summary.
  - `debug arrangement` prints a stable arrangement header and diagnostic
    density information.
  - `support-bundle` contains the expected operational artifacts.
  - `tune --override` changes the visible tuning state and writes it to disk.
  - `SHOW RESOURCE USAGE` and `SHOW SCHEMA_EVOLUTION` paths should be exercised
    through `psql` and the native protocol client.
- Query robustness under churn.
  - Keep one client connected through a worker restart.
  - Keep a second client connected through a gateway restart.
  - Prove that reconnects after a control restart still preserve the expected
    SQL-visible state once the cluster comes back.
- Auth and policy surfaces if they are public by this point.
  - Unauthenticated requests are rejected.
  - Cross-tenant requests are rejected.
  - Admin users can still read across tenant boundaries when the policy allows
    it.
- Rate limiting and timeout surfaces.
  - Explicitly assert timeout behavior for a slow query.
  - Explicitly assert rate-limit behavior for a burst of short queries.
  - Make sure the error text is actionable and stable.
- Documentation parity.
  - Every public command used above must have matching help text and docs.
  - If the docs claim a subcommand or flag exists, the binary must expose it in
    the same form.

#### Required assertions

- The admin surface must work against a live cluster, not stubbed in-memory
  objects.
- Restart tests must prove the storage root is durable and that the cluster can
  reattach to it.
- `psql` must still be able to connect after the cluster is restarted.
- The suite must collect logs and artifacts on failure so production handoff
  issues are diagnosable from CI output.

#### v0.55 pass criteria

- The public binary feels like an operable service rather than a demo process.
- Restart, recovery, and admin behavior are all externally visible and stable.
- The same test harness can be used for pre-release operational rehearsals.

### v0.56 - Cold-Tier Parquet and Iceberg Sink With Law Metadata

The purpose of v0.56 is to prove that MinIO-backed storage is real, durable,
and inspectable. This version should validate object-store behavior directly,
not only through in-memory mocks or catalog-level abstractions.

#### Required topology

- A RockStream cluster that uses MinIO-backed storage for the persistence paths
  under test.
- A `psql` container to keep the SQL interface under concurrent load while the
  object store is being churned.
- Host-side `tokio-postgres` clients to compare logical results while MinIO is
  being exercised.

#### Mandatory scenarios

- WAL and checkpoint behavior.
  - Append data, force a checkpoint, restart the cluster, and prove the visible
    state is preserved.
  - Verify that WAL listing is not on the hot path once the listing cache is in
    place.
  - Ensure the test can observe real object-store artifacts in MinIO after the
    checkpoint.
- Compaction and cleanup.
  - Verify that old or superseded files disappear when the retention policy or
    cleanup logic says they should.
  - Verify that no orphan objects remain after a controlled shutdown.
  - Verify that a crash during flush or compaction does not leave the storage in
    a state that cannot be recovered.
- Cold-tier sink, if the version exposes it.
  - Create the sink against MinIO.
  - Wait for the snapshot interval to trigger real object writes.
  - Inspect the resulting files and manifests directly in MinIO.
  - Assert that finalized values are written, not raw intermediate operands.
  - Assert that the cold snapshot can be merged with the hot tail to produce the
    same logical result set as the live query.
- MinIO failure and restart behavior.
  - Stop MinIO mid-run and confirm the cluster reports a failure rather than a
    silent success.
  - Restart MinIO and prove recovery proceeds from durable state.
  - The harness should check that retry behavior and error propagation are
    visible at the SQL or CLI layer.
- Object-store visibility.
  - Tests should list the bucket contents and assert on the presence or absence
    of specific keys or prefixes.
  - No test should pass solely because an in-memory data structure still holds
    the expected value.

#### Required assertions

- Every storage-backed path must prove its effect against MinIO.
- The object layout must be stable enough to support later cold-tier and
  catalog tests.
- The cluster must remain queryable while the storage layer is being restarted
  or temporarily unavailable.

#### v0.56 pass criteria

- MinIO is no longer just a fixture; it is part of the tested contract.
- Recovery, compaction, and retention are visible in the bucket contents.
- Storage regressions can be diagnosed from object listings and transcripts,
  not just from internal metrics.

### v0.57 - Catalog Registration and REST Server

The purpose of v0.57 is to prove that RockStream's metadata and catalog-facing
surfaces are coherent with the SQL gateway. The HTTP catalog surface should be
tested locally and deterministically, without bringing in external query
engines or cloud catalog services.

#### Required topology

- A RockStream cluster with the HTTP catalog endpoint enabled.
- MinIO as the backing object store.
- A `psql` client container and a host-side `tokio-postgres` client so the SQL
  gateway remains part of the same scenario.
- A host-side HTTP client, such as `reqwest`, for direct catalog requests.

#### Mandatory scenarios

- Catalog registration.
  - After a write or DDL change, the catalog must reflect the new namespace,
    table, view, or snapshot state.
  - Restart the cluster and verify the catalog remains consistent with the
    durable state.
- REST catalog behavior.
  - Query the `/iceberg/v1/` routes that are publicly documented.
  - Assert the status code, schema, and payload shape.
  - Verify the server rejects invalid requests with stable errors.
- Metadata coherence.
  - Query the same logical object through `psql` and through the catalog API.
  - The names and versions must line up.
  - If the catalog exposes namespaces, tables, or snapshots, the test must
    verify that each one is discoverable and stable across restart.
- Auth and transport propagation.
  - If the endpoint is protected, assert the unauthenticated failure mode.
  - If mTLS or tokens are enabled, assert that the catalog layer sees the same
    identity that the gateway sees.
- Regression matrix.
  - Rerun the critical smoke tests from v0.53 through v0.56 in a single cluster
    so the catalog work cannot accidentally break the binary, SQL, or storage
    surfaces.

#### Required assertions

- HTTP catalog behavior must stay in sync with SQL-visible metadata.
- The catalog server must not become a second source of truth.
- The local HTTP tests must stay deterministic and must not depend on external
  catalog services.

#### v0.57 pass criteria

- The SQL gateway and the catalog endpoint describe the same world.
- Local interoperability is proven without requiring Spark, Trino, DuckDB, or
  AWS S3.
- The suite is now broad enough that a change to any public surface is likely to
  fail somewhere visible.

## What Is Deliberately Out Of Scope

The following are not part of this plan yet:

- AWS S3 or any other external cloud object store.
- Spark, Trino, DuckDB, or other external query engines as acceptance gates.
- Multi-region topologies.
- Performance-only benchmarks without correctness assertions.
- Internal Rust module coverage that cannot be observed through a public
  interface.

Those can come later, but they should not dilute the v0.53-v0.57 E2E plan.

## Final Exit Criteria For The Whole Plan

This plan is complete when all of the following are true:

- Every public `rockstream` subcommand has at least one external E2E test.
- Every supported pgwire path has both a `tokio-postgres` assertion and a
  `psql` compatibility check where applicable.
- Every supported SQL feature has at least one positive-path E2E test, plus a
  negative-path test when the feature is supposed to reject unsupported input.
- Every object-store-backed path is verified against MinIO.
- Every roadmap version from v0.53 to v0.57 has a dedicated, externally
  observable regression suite.
- The suite can be rerun from a clean checkout without manual repair steps.

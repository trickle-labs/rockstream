# RockStream Pre-v1.0 Product Polish
## Formal Feature Specification and Implementation Plan

**Status:** Proposed  
**Target:** Complete before `v1.0.0` if the project accepts the scope freeze described below  
**Repository baseline reviewed:** `main` at `aa5f03b091f581230603a336473ce2d87f176ae1`  
**Baseline package version:** `0.59.3`  
**Baseline roadmap state:** `v0.59.3` being implemented  
**Primary owners:** RockStream maintainers across `rockstream-cli`, `rockstream-gateway`, `rockstream-sql`, `rockstream-plan`, `rockstream-ops`, `rockstream-types`, and `rockstream-control`

---

## 1. Purpose

This document specifies a focused pre-v1 product-polish program for the following user-facing improvements:

1. `rockstream demo`
2. `rockstream doctor`
3. `rockstream config validate`
4. `rockstream config print-effective`
5. Uniform `--output json`
6. Shell completions for Bash, Zsh, and Fish
7. `SHOW ROCKSTREAM CAPABILITIES`
8. `SELECT rockstream_version()`
9. Richer `SHOW VIEW STATUS`
10. Generated documentation for every `RS-XXXX` error
11. `UPDATE ... RETURNING`
12. `DELETE ... RETURNING`
13. Consistent `IF EXISTS` and `IF NOT EXISTS`
14. Common string, null-handling, and date/time functions
15. Read-only system catalog tables for nodes, sources, views, checkpoints, and capabilities

The intent is not to broaden RockStream into a general database. The intent is to make the product that already exists easier to evaluate, automate, understand, diagnose, and use correctly.

The work should be admitted as **pre-v1 product completeness**, not as a new strategic pillar. No new connector family, distributed protocol, transaction model, lakehouse integration, or general-purpose SQL execution path is introduced.

---

## 2. Baseline assessment

The current repository already contains several useful foundations:

- The CLI is a single `clap`-based binary in `crates/rockstream-cli/src/main.rs`.
- A global `--json` flag and `OutputFormat::{Text, Json}` already exist.
- `CandidateIdentity` already centralizes semantic version, commit SHA, build timestamp, compiler version, lockfile digest, and feature metadata.
- `RockstreamConfig` already supports TOML deserialization, defaults, and serialization.
- `capabilities.toml` and `scripts/generate-capability-matrix.py` already provide a machine-readable capability contract.
- `SHOW VIEW STATUS` already reports stage-lag breakdown, degradation reason, reason code, dominant contributor, and progress fields.
- `UPDATE ... RETURNING` and `DELETE ... RETURNING` are already implemented in the gateway.
- The canonical `RS-XXXX` constants and `next_steps()` mapping already live in `rockstream-types`.
- The gateway catalog already contains view, source, workload, resource-usage, and dead-letter-queue state.

The proposed work must reuse and consolidate those foundations rather than creating parallel systems.

### 2.1 Current-status matrix

| ID | Feature | Baseline state | Required work |
|---|---|---|---|
| UX-01 | `rockstream demo` | Missing | New command and deterministic embedded scenario |
| UX-02 | `rockstream doctor` | Missing | New diagnostic framework and command |
| CFG-01 | `config validate` | Parsing only | Add unknown-key detection and semantic validation |
| CFG-02 | `config print-effective` | Defaults and ad hoc CLI overrides exist | Add one shared resolver, source precedence, and origin reporting |
| CLI-01 | `--output json` | Partial: global `--json` exists | Standardize syntax, errors, streaming behavior, and command coverage |
| CLI-02 | Shell completions | Missing | Add `clap_complete` and generated completions |
| OBS-01 | `SHOW ROCKSTREAM CAPABILITIES` | Machine-readable file exists; no SQL surface | Add embedded runtime registry, SQL command, and catalog table |
| OBS-02 | `rockstream_version()` | CLI version command exists; SQL function missing | Add pgwire-visible function backed by `CandidateIdentity` |
| OBS-03 | Richer view status | Substantial partial implementation | Append runtime frontier, checkpoint, state, spill, recovery, and action fields |
| DOC-01 | Generated error docs | Constants/comments and helper mappings exist | Create structured catalog and generated Markdown/reference checks |
| SQL-01 | `UPDATE ... RETURNING` | Implemented | Harden contract, tests, docs, aliases, prepared statements, and transactions |
| SQL-02 | `DELETE ... RETURNING` | Implemented | Harden contract, tests, docs, prepared statements, and no-match behavior |
| SQL-03 | `IF EXISTS` / `IF NOT EXISTS` | Not consistently implemented | Add shared DDL modifiers and no-op semantics |
| SQL-04 | Common functions | Narrow scalar-UDF support exists | Add typed, null-preserving scalar-function support |
| CAT-01 | Read-only system catalogs | Resource and DLQ tables only | Add five coherent catalog tables and snapshot provider |

---

## 3. Goals

The program is complete when:

- A new user can run a useful RockStream demonstration without external infrastructure.
- An operator can diagnose configuration, storage, control-plane, TLS, and system problems from one command.
- The configuration used by `rockstream start` is exactly the configuration displayed by `config print-effective`.
- Every finite CLI command can emit machine-readable JSON through one stable flag.
- Users can discover CLI syntax through their shell.
- A running cluster can describe its version, capabilities, nodes, sources, views, checkpoints, and view health.
- Common PostgreSQL-style DDL and DML ergonomics work consistently.
- A deliberately limited set of common scalar functions has correct null, type, and incremental semantics.
- Every public `RS-XXXX` code has a generated reference entry.
- The machine-readable capability contract, generated docs, SQL introspection, and tests cannot drift independently.

---

## 4. Non-goals

The following are explicitly out of scope:

- New source or sink families.
- DuckLake, Iceberg, Delta, or external catalog support.
- New distributed coordination protocols.
- Multi-region operation.
- General OLTP semantics.
- Full PostgreSQL compatibility.
- Arbitrary `RETURNING` expressions, subqueries, or aggregates.
- Arbitrary timezone databases or locale-dependent collation.
- YAML, CSV, or custom CLI output formats.
- A new web administration UI.
- Automatic repair by `rockstream doctor`.
- Exposing secrets, inline connector options, certificates, private keys, or credentials through system catalogs.
- Replacing the existing `capabilities.toml` contract.
- A second configuration system or a second CLI binary.

---

## 5. Cross-cutting engineering rules

Every feature in this plan must follow these rules.

### 5.1 One implementation path

There must be one canonical implementation for each concept:

- One configuration resolver for `start`, `doctor`, `validate`, and `print-effective`.
- One `OutputFormat` implementation for all CLI commands.
- One runtime capability registry backed by `capabilities.toml`.
- One view-status snapshot model shared by SQL and CLI.
- One structured error catalog used by code and documentation.
- One DDL modifier parser shared by all applicable object families.

### 5.2 Error contract

Every user-visible failure must:

- Carry an `RS-XXXX` code.
- Include a concise message.
- Include actionable next steps.
- Indicate whether the operation may be retried where meaningful.
- Produce structured JSON when JSON output is active.
- Never silently fall back to a materially different behavior.

### 5.3 Boundedness

New collections and outputs must be bounded:

- Demo result sets: at most 100 rows per step.
- Doctor checks: at most 64 checks and a default 30-second total deadline.
- CLI finite outputs: existing `CLI_OUTPUT_MAX_ROWS` remains enforced.
- Catalog snapshots: bounded by configured cluster/view/source/checkpoint limits.
- Capability registry: immutable and bounded by the embedded contract.
- Error registry: immutable and bounded at build time.
- Scalar-function argument count: at most 16 unless a smaller limit is specified.
- No command may accumulate an unbounded streaming result in memory.

### 5.4 Security

- No command prints secret values.
- Config output redacts values designated sensitive.
- Doctor is read-only by default.
- Deep storage probes use an isolated key prefix and always attempt cleanup.
- System catalogs expose status metadata, not credentials or raw secret options.
- Namespace and RBAC filtering must be applied consistently with existing gateway rules.

### 5.5 Documentation and capability contract

Any new SQL, CLI, catalog, or configuration surface must update:

- `capabilities.toml`
- `docs/capability-matrix.md` through the generator
- `docs/language-features.md`
- `docs/cli.md`
- `docs/configuration.md` where applicable
- `scripts/check-dispatch-wiring.py` where applicable
- Raw pgwire reachability tests
- Negative tests with structured errors

### 5.6 Compatibility

- New SQL and catalog surfaces are additive.
- New `SHOW VIEW STATUS` columns are appended, not inserted in the middle.
- Existing `--json` remains accepted as a deprecated alias for `--output json` through at least v1.0.
- Existing error-code constants remain source-compatible after error-catalog generation.
- Existing `UPDATE` and `DELETE` behavior without `RETURNING` remains unchanged.
- Existing DDL without `IF EXISTS` or `IF NOT EXISTS` retains current error behavior.

---

## 6. Proposed pre-v1 delivery sequence

The work is best delivered in four ordered milestones.

| Milestone | Theme | Features |
|---|---|---|
| **v0.59.4** | CLI and configuration usability | UX-01, UX-02, CFG-01, CFG-02, CLI-01, CLI-02 |
| **v0.59.5** | Runtime introspection and operator clarity | OBS-01, OBS-02, OBS-03, CAT-01 |
| **v0.59.6** | SQL ergonomics and common expression completeness | SQL-01, SQL-02, SQL-03, SQL-04 |
| **v0.59.7** | Error-reference generation and contract closure | DOC-01 plus final docs/conformance sweep |

This sequencing is intentional:

1. Shared CLI output and configuration resolution are needed by demo and doctor.
2. Capability and status models should be established before exposing catalog tables.
3. DML and scalar-function work should land after the introspection contract is stable.
4. The error catalog should absorb every new code introduced by the other milestones before the final documentation gate.

The milestones may be developed in parallel, but they should merge in dependency order.

---

# 7. Detailed specifications

## 7.1 UX-01 — `rockstream demo`

### 7.1.1 Objective

Provide a zero-dependency demonstration that proves the basic RockStream value proposition:

> Changes are written through PostgreSQL-compatible SQL, a materialized view is maintained incrementally, and updated results are read back through the same public interface.

The command must use the real gateway, planner, operator pipeline, storage layer, and pgwire client path. It must not call private catalog mutation helpers to fake the result.

### 7.1.2 User interface

```text
rockstream demo
rockstream demo --scenario orders
rockstream demo --storage ./demo-data --keep
rockstream demo --listen 127.0.0.1:0
rockstream demo --output json
```

Proposed arguments:

| Argument | Default | Meaning |
|---|---:|---|
| `--scenario <name>` | `orders` | Select a built-in deterministic scenario |
| `--storage <path>` | Temporary directory | Use a caller-specified local storage directory |
| `--listen <addr>` | `127.0.0.1:0` | Bind the embedded gateway; port `0` requests an ephemeral port |
| `--keep` | `false` | Retain generated data after the command exits |
| `--step-delay-ms <n>` | `0` | Optional presentation delay; capped at 5,000 ms |
| `--output <text|json>` | `text` | Use the common CLI output contract |

### 7.1.3 Built-in `orders` scenario

The initial scenario must use only proven Core-compatible types and operations.

Recommended schema:

```sql
CREATE TABLE orders (
    order_id BIGINT,
    store_id BIGINT,
    amount BIGINT
);

CREATE MATERIALIZED VIEW sales_by_store AS
SELECT store_id, SUM(amount) AS total_amount
FROM orders
GROUP BY store_id;
```

Execution steps:

1. Start one embedded `role=all` process over local-filesystem storage.
2. Connect using `tokio-postgres`.
3. Create the table and materialized view.
4. Insert three deterministic rows.
5. Query the view and assert expected totals.
6. Update one order and query again.
7. Delete one order and query again.
8. Display:
   - SQL executed.
   - Returned command tag.
   - View result after each mutation.
   - Final pgwire endpoint.
   - Storage path if retained.
9. Shut down cleanly.
10. Remove the temporary directory unless `--keep` was supplied.

### 7.1.4 Output contract

Text mode should be narrative but deterministic.

JSON mode should return:

```json
{
  "schema_version": "1",
  "command": "demo",
  "scenario": "orders",
  "endpoint": "127.0.0.1:54321",
  "storage_path": "/tmp/...",
  "retained": false,
  "steps": [
    {
      "name": "insert_initial_orders",
      "sql": "INSERT ...",
      "command_tag": "INSERT 0 3",
      "rows": []
    }
  ],
  "final_rows": [
    {"store_id": 1, "total_amount": 120}
  ],
  "cleanup": "completed"
}
```

### 7.1.5 Implementation

Add:

- `crates/rockstream-cli/src/demo.rs`
- `DemoOptions`
- `DemoStep`
- `DemoOutcome`
- `run_demo()`

Modify:

- `crates/rockstream-cli/src/main.rs`
- `crates/rockstream-cli/src/lib.rs`
- `crates/rockstream-cli/src/output.rs`
- `crates/rockstream-cli/Cargo.toml` only if an additional client feature is required
- `docs/quickstart.md`
- `docs/cli.md`
- `README.md`

Reuse:

- `start_gateway_with_shard`
- `RockstreamConfig::default()`
- local `ShardDb`
- `tokio-postgres`
- common output rendering

The SQL for each scenario should live in a visible, reviewable fixture such as:

```text
examples/demo/orders.sql
```

Do not bury the demonstration SQL in a large Rust string if it can be shared with documentation.

### 7.1.6 Failure behavior

- Gateway startup failure: existing storage/listen error code.
- Scenario assertion mismatch: `RS-0001`, because the built-in demonstration is internally inconsistent.
- Unsupported scenario: `RS-0002`.
- Cleanup failure after successful demo: warning in the outcome plus non-zero exit only when a caller-specified temporary path cannot be safely handled.
- A failed step stops the scenario. Later steps must not continue.

### 7.1.7 Tests

Add `crates/rockstream-cli/tests/demo_tests.rs`:

- `demo_orders_proves_insert_update_delete_view_maintenance`
- `demo_uses_ephemeral_port`
- `demo_json_is_valid_and_stable`
- `demo_temp_storage_is_removed`
- `demo_keep_retains_storage`
- `demo_unknown_scenario_fails_rs0002`
- `demo_result_is_reproducible`

At least one test must connect through pgwire and independently query the final view.

### 7.1.8 Acceptance criteria

- A clean machine with the RockStream binary can run `rockstream demo` without Docker, Kafka, PostgreSQL, or MinIO.
- The command exits successfully in under 30 seconds on a normal development machine.
- The final result is produced by the real maintained-view path.
- No private mutation helper is used to synthesize the result.
- Text and JSON output both pass snapshot or schema tests.

### 7.1.9 Estimated effort

**3–5 engineer-days.**

---

## 7.2 UX-02 — `rockstream doctor`

### 7.2.1 Objective

Provide one bounded, non-destructive diagnostic command that answers:

- Is this binary internally consistent?
- Is the configuration valid?
- Is storage usable?
- Is the control plane reachable?
- Are listener addresses usable?
- Is TLS material readable and coherent?
- Are basic host limits obviously unsafe?
- Is the optional local integration environment available?

### 7.2.2 User interface

```text
rockstream doctor
rockstream doctor --config ./rockstream.toml
rockstream doctor --storage ./data
rockstream doctor --control http://127.0.0.1:8000
rockstream doctor --gateway 127.0.0.1:5432
rockstream doctor --deep
rockstream doctor --include-docker
rockstream doctor --output json
```

Proposed arguments:

| Argument | Meaning |
|---|---|
| `--config <path>` | Resolve and validate a specific config file |
| `--storage <path-or-url>` | Probe a storage target |
| `--control <url>` | Probe control-plane connectivity |
| `--gateway <host:port>` | Probe pgwire reachability |
| `--deep` | Permit temporary write/read/delete storage probes |
| `--include-docker` | Check Docker only when evaluation/qualification requires it |
| `--timeout <duration>` | Per-check timeout, default 5 seconds, maximum 30 seconds |
| `--output <text|json>` | Common output format |

### 7.2.3 Check model

```rust
pub enum DiagnosticStatus {
    Pass,
    Warn,
    Fail,
    Skip,
}

pub struct DiagnosticCheckResult {
    pub id: String,
    pub category: String,
    pub status: DiagnosticStatus,
    pub summary: String,
    pub details: Option<String>,
    pub code: Option<String>,
    pub next_steps: Option<String>,
    pub duration_ms: u64,
}
```

The final report contains:

- Candidate identity.
- Start and completion timestamps.
- All check results.
- Counts by status.
- Overall status.
- Redaction confirmation.

### 7.2.4 Required checks

#### Always run

- `binary.candidate_identity`
- `config.parse`
- `config.semantic`
- `system.clock_monotonic`
- `system.temp_directory`
- `system.file_descriptor_limit`
- `system.memory_available`
- `system.os_arch_supported`

#### Run when storage is supplied or resolved

- `storage.path_or_url_valid`
- `storage.readable`
- `storage.local_permissions`
- `storage.object_store_credentials_present`
- `storage.deep_roundtrip` only with `--deep`

#### Run when control is supplied or resolved

- `control.dns`
- `control.tcp_connect`
- `control.protocol_handshake`
- `control.read_only_status`

#### Run when gateway is supplied

- `gateway.tcp_connect`
- `gateway.startup_handshake`
- `gateway.version_query`

#### Run when TLS paths are configured

- `tls.gateway_files_readable`
- `tls.gateway_cert_key_match`
- `tls.gateway_ca_parse`
- `tls.internal_files_readable`
- `tls.internal_cert_key_match`
- `tls.internal_ca_parse`
- `tls.certificate_expiry`

#### Optional

- `docker.daemon` only with `--include-docker`
- `docker.rockstream_test_image` only when explicitly requested by a future flag

### 7.2.5 Safety and boundedness

- Default mode performs no remote mutation.
- `--deep` uses a unique key such as `_rockstream_doctor/<uuid>/probe`.
- The probe writes a small fixed payload, reads it, compares the digest, and deletes it.
- Cleanup runs in a guard/finalizer even after intermediate failure.
- Each check has a timeout.
- Maximum checks: 64.
- Default total command deadline: 30 seconds.
- No recursive DNS, port scanning, or broad network discovery.

### 7.2.6 Exit status

| Outcome | Exit |
|---|---:|
| No `Fail` results | `0` |
| One or more `Fail` results | `1` |
| CLI usage/parsing error | `2` |

Warnings do not fail the command.

### 7.2.7 Implementation

Add:

- `crates/rockstream-cli/src/doctor.rs`
- `DiagnosticCheck` trait or a table-driven async check runner
- `DoctorOptions`
- `DoctorReport`

Refactor reusable checks into small modules rather than one large function.

Use existing abstractions:

- `ControlClient`
- `StorageClient`
- `CatalogClient`
- TLS loading logic
- `CandidateIdentity`
- the shared configuration resolver defined in CFG-02

### 7.2.8 Tests

Add `crates/rockstream-cli/tests/doctor_tests.rs`:

- valid local config and storage pass
- malformed config fails with path-specific diagnostics
- invalid semantic values produce all relevant errors
- inaccessible control endpoint fails within deadline
- missing TLS key is reported without panic
- expired certificate warns or fails according to policy
- deep local storage roundtrip cleans up
- JSON output contains no private-key or credential material
- Docker is skipped unless requested
- injected slow check is cancelled at deadline

Mock transport traits should be used for deterministic negative tests. One integration test should exercise a live local gateway.

### 7.2.9 Acceptance criteria

- The command never hangs indefinitely.
- Every failing check has an `RS-XXXX` code or maps clearly to an existing code.
- No secret values are present in text or JSON output.
- Default mode is non-mutating.
- A real local gateway and local storage pass.
- At least one injected failure for every check category is covered.

### 7.2.10 Estimated effort

**6–9 engineer-days.**

---

## 7.3 CFG-01 — `rockstream config validate`

### 7.3.1 Objective

Validate both the syntax and meaning of `rockstream.toml` before startup.

Current deserialization proves that values have parseable types. It does not provide a complete semantic diagnostic report and may not identify all unknown keys clearly.

### 7.3.2 User interface

```text
rockstream config validate
rockstream config validate --file ./rockstream.toml
rockstream config validate --strict
rockstream config validate --check-files
rockstream config validate --output json
```

Arguments:

| Argument | Default | Meaning |
|---|---:|---|
| `--file <path>` | Resolved config path | Validate a specific file |
| `--strict` | `true` for pre-v1 startup compatibility | Treat unknown keys and deprecated keys as errors |
| `--check-files` | `false` | Validate referenced certificate/key paths |
| `--output` | `text` | Common output format |

### 7.3.3 Validation layers

#### Layer 1 — TOML syntax

Report line and column for malformed TOML.

#### Layer 2 — unknown and deprecated fields

Use `serde_ignored` or an equivalent deterministic mechanism to report every ignored field.

Unknown-field diagnostics must include:

- Full dotted path.
- Closest known key when edit-distance matching is unambiguous.
- `RS-0002`.
- Suggested correction.

Deprecated/removed keys must point to the relevant migration documentation, including fail-closed `RS-4017` behavior where applicable.

#### Layer 3 — semantic validation

At minimum enforce:

- `recursion_max_iterations > 0`
- `cluster.min_epoch_ms > 0`
- `cluster.checkpoint_retention_count > 0`
- `cluster.state_budget_gb > 0`
- `0.0 < index_prefer_selectivity_threshold <= 1.0`
- `cluster.index_max_lag_ms > 0`
- autotuner values satisfy:
  - `min_parallelism > 0`
  - `min_parallelism <= default_parallelism <= max_parallelism`
  - hysteresis windows are non-zero
- skew:
  - factor is finite and greater than `1.0`
  - bucket count is non-zero
- worker:
  - cache bytes > 0
  - rows per quantum > 0
- connector:
  - retention days > 0
- exchange:
  - thresholds, capacities, and timeouts are non-zero
  - direct threshold does not exceed the spill threshold in bytes
  - retry count is bounded by an explicit maximum
- gateway TLS:
  - certificate and key are both set or both absent
- internal TLS:
  - certificate, key, and CA are all set or all absent
- pricing values are non-negative
- spot mix is within `0.0..=1.0`
- removed storage-tiering fields are absent

### 7.3.4 Diagnostic model

```rust
pub struct ConfigDiagnostic {
    pub path: String,
    pub severity: ConfigDiagnosticSeverity,
    pub code: ErrorCode,
    pub message: String,
    pub suggestion: Option<String>,
}
```

Validation returns all diagnostics in stable path order, not only the first failure.

### 7.3.5 Implementation

Add to `crates/rockstream-types/src/config.rs` or a new `config_validation.rs`:

- `RockstreamConfig::validate() -> Vec<ConfigDiagnostic>`
- `ConfigValidationReport`
- helper validation functions per section

Add to CLI:

- `ConfigCommand::Validate`
- `run_config_validate()`

Add workspace dependency only if required:

```toml
serde_ignored = "0.1"
```

### 7.3.6 Tests

- table-driven boundary tests for every rule
- multiple simultaneous errors are all returned
- unknown nested key reports full path
- typo suggestion is deterministic
- removed key returns migration guidance
- valid default configuration passes
- generated default TOML passes
- file checks handle missing/unreadable TLS paths
- JSON schema test
- property test: serializing any semantically valid config and reloading it remains valid

### 7.3.7 Acceptance criteria

- `rockstream start` calls the same semantic validator.
- Unknown keys do not disappear silently.
- All diagnostics are stable and path-specific.
- Validation itself performs no network operations.
- `docs/configuration.md` documents every rule.

### 7.3.8 Estimated effort

**4–6 engineer-days.**

---

## 7.4 CFG-02 — `rockstream config print-effective`

### 7.4.1 Objective

Display the exact configuration that the process would use after applying defaults, file values, environment variables, and CLI overrides.

The command is valuable only if `rockstream start` uses the same resolver.

### 7.4.2 User interface

```text
rockstream config print-effective
rockstream config print-effective --file ./rockstream.toml
rockstream config print-effective --show-origins
rockstream config print-effective --output json
```

### 7.4.3 Source precedence

The binding precedence is:

1. Compiled defaults.
2. Configuration file.
3. `ROCKSTREAM__<SECTION>__<KEY>` environment variables.
4. Explicit CLI flags.

The path to the configuration file is resolved in this order:

1. `--config` / `--file`
2. `ROCKSTREAM_CONFIG`
3. `./rockstream.toml` if it exists
4. No file; defaults only

Environment examples:

```text
ROCKSTREAM__CLUSTER__MIN_EPOCH_MS=25
ROCKSTREAM__WORKER__SEGMENT_CACHE_BYTES=1073741824
ROCKSTREAM__EXCHANGE__RPC_TIMEOUT_MS=15000
```

Environment values are parsed as TOML scalar values, not as untyped strings.

### 7.4.4 Shared resolver

Introduce:

```rust
pub enum ConfigOrigin {
    Default,
    File(PathBuf),
    Environment(String),
    Cli(String),
}

pub struct ResolvedConfig {
    pub config: RockstreamConfig,
    pub origins: BTreeMap<String, ConfigOrigin>,
    pub diagnostics: Vec<ConfigDiagnostic>,
}
```

`rockstream start`, `doctor`, `config validate`, and `config print-effective` must all use:

```rust
ConfigResolver::resolve(...)
```

No command may independently reproduce precedence logic.

### 7.4.5 CLI refactor

Extract reusable `clap::Args` structures from the large `Start` variant:

- `CommonConfigArgs`
- `RuntimeOverrideArgs`
- `TlsOverrideArgs`
- `ExchangeOverrideArgs`

This allows `start` and `config print-effective` to resolve the same overrides without duplicating fields.

### 7.4.6 Output

Text mode emits canonical TOML.

`--show-origins` adds comments in text mode:

```toml
# source: environment ROCKSTREAM__CLUSTER__MIN_EPOCH_MS
min_epoch_ms = 25
```

JSON mode emits:

```json
{
  "schema_version": "1",
  "config": { "...": "..." },
  "origins": {
    "cluster.min_epoch_ms": {
      "kind": "environment",
      "source": "ROCKSTREAM__CLUSTER__MIN_EPOCH_MS"
    }
  },
  "diagnostics": []
}
```

Sensitive values must be redacted. At present most configuration fields are paths and numeric settings, but the redaction mechanism must be generic for future secret-bearing fields.

### 7.4.7 Tests

- defaults only
- file overrides defaults
- environment overrides file
- CLI overrides environment
- printed effective config equals `StartOptions.config`
- origin map is correct
- invalid environment scalar is rejected
- unknown environment path is rejected
- output ordering is deterministic
- sensitive test field is redacted
- no config file is a valid defaults-only case

### 7.4.8 Acceptance criteria

- One resolver is used by every process-start path.
- The printed configuration round-trips through `RockstreamConfig`.
- The displayed configuration exactly matches the configuration passed into `run_start`.
- No secret material appears in output.
- Precedence and environment naming are documented.

### 7.4.9 Estimated effort

**5–8 engineer-days.**

---

## 7.5 CLI-01 — Uniform `--output json`

### 7.5.1 Objective

Replace the special-purpose boolean experience with one consistent, discoverable output contract while preserving compatibility.

### 7.5.2 User interface

```text
rockstream view status --output text
rockstream view status --output json
rockstream --output json cluster status
rockstream --json cluster status
```

`--json` remains a deprecated alias for `--output json`.

### 7.5.3 Command-line model

Change `OutputFormat` to a `clap::ValueEnum`:

```rust
#[derive(ValueEnum, Serialize, Deserialize)]
pub enum OutputFormat {
    Text,
    Json,
}
```

Global argument:

```rust
#[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
output: OutputFormat
```

Compatibility alias:

```rust
#[arg(long, global = true, hide = true)]
json: bool
```

If both are supplied incompatibly, return a usage error.

### 7.5.4 Output rules

#### Finite commands

Emit one JSON document to stdout.

#### Streaming commands

`audit tail` and `view subscribe` emit one JSON object per line. This behavior is documented as JSON Lines while still selected by `--output json`.

#### Diagnostics

- Normal data: stdout.
- Logs and progress: stderr.
- No ANSI escapes in JSON.
- No human table headings in JSON.
- No success prose after the JSON document.

#### Errors

Structured JSON errors are emitted to stderr:

```json
{
  "error": {
    "code": "RS-2001",
    "message": "view `missing` was not found",
    "retryable": false,
    "next_steps": "Run `rockstream view list`...",
    "documentation": "docs/errors/RS-2001.md"
  }
}
```

### 7.5.5 Schema stability

Every JSON-producing model must be serializable and documented.

A common envelope is recommended for new commands:

```rust
pub struct CliEnvelope<T> {
    pub schema_version: &'static str,
    pub command: &'static str,
    pub data: T,
    pub warnings: Vec<CliWarning>,
}
```

Existing raw JSON shapes may remain for v1 if changing them would create unnecessary churn, but every shape must have a named test fixture and versioning statement. The project should choose one policy and document it before v1.

### 7.5.6 Implementation

Modify:

- `crates/rockstream-cli/src/main.rs`
- `crates/rockstream-cli/src/output.rs`
- `CliError` rendering
- every command path that bypasses `render_output`

Add a CI/static check that rejects direct user-facing `println!` calls in command implementations unless explicitly allowlisted for streaming output.

### 7.5.7 Tests

- every top-level command accepts `--output json`
- `--json` produces the same data
- JSON stdout parses successfully
- errors parse successfully from stderr
- no ANSI escapes
- no trailing prose
- streaming commands emit valid JSON per line
- destructive confirmation errors remain structured
- output row bounds still apply

### 7.5.8 Acceptance criteria

- All finite commands support text and JSON.
- All streaming commands define JSON-line behavior.
- `--json` remains compatible.
- No command mixes human text into JSON stdout.
- JSON errors contain code and next steps.

### 7.5.9 Estimated effort

**3–5 engineer-days.**

---

## 7.6 CLI-02 — Shell completions

### 7.6.1 Objective

Generate completions directly from the `clap` command tree so they cannot drift from actual syntax.

### 7.6.2 User interface

```text
rockstream completions bash
rockstream completions zsh
rockstream completions fish
```

Output is written to stdout.

Installation examples:

```bash
rockstream completions bash > ~/.local/share/bash-completion/completions/rockstream
rockstream completions zsh > ~/.zfunc/_rockstream
rockstream completions fish > ~/.config/fish/completions/rockstream.fish
```

### 7.6.3 Implementation

Add workspace dependency:

```toml
clap_complete = "4"
```

Add:

- `Command::Completions`
- `CompletionShell::{Bash, Zsh, Fish}`
- direct use of `clap_complete::generate`

The command must generate from `Cli::command()` rather than a hand-written list.

### 7.6.4 Tests

- each shell output is non-empty
- generated output contains representative nested commands
- generated output contains `--output`
- generated output contains `config validate`, `doctor`, and `demo`
- no deprecated hidden alias is suggested unless intentionally retained
- generation writes only to stdout

### 7.6.5 Acceptance criteria

- Bash, Zsh, and Fish are supported.
- Completions are generated from the live CLI model.
- Installation instructions are in `docs/cli.md`.
- Release packaging may optionally install completion files, but packaging automation is not required by this feature.

### 7.6.6 Estimated effort

**1–2 engineer-days.**

---

## 7.7 OBS-01 — `SHOW ROCKSTREAM CAPABILITIES`

### 7.7.1 Objective

Allow a connected client to discover the exact capability contract of the running binary.

The runtime result must come from the same `capabilities.toml` that generates the documentation.

### 7.7.2 SQL interface

```sql
SHOW ROCKSTREAM CAPABILITIES;

SHOW ROCKSTREAM CAPABILITIES FOR 'language.query-read';
```

The `FOR` form is optional but recommended because it is easy to support once the registry exists.

### 7.7.3 Result columns

| Column | Type | Meaning |
|---|---|---|
| `contract_version` | `TEXT` | Capability-contract version |
| `capability_id` | `TEXT` | Stable machine-readable ID |
| `kind` | `TEXT` | `language`, `connector`, or `sink` |
| `name` | `TEXT` | Human-readable name |
| `tier` | `TEXT` | `Core`, `Maintain`, or `Experimental` |
| `reachability` | `TEXT` | Public entry surface |
| `proof` | `TEXT` nullable | Named proof test |
| `documentation` | `TEXT` | Documentation path |

Rows are sorted by `(kind, capability_id)`.

### 7.7.4 Runtime registry

Add `crates/rockstream-types/src/capability.rs`:

```rust
pub struct CapabilityContract {
    pub version: String,
    pub promise: String,
    pub capabilities: Vec<CapabilityDescriptor>,
}

pub struct CapabilityDescriptor {
    pub id: String,
    pub kind: CapabilityKind,
    pub name: String,
    pub tier: CapabilityTier,
    pub reachability: String,
    pub proof: Option<String>,
    pub documentation: String,
}
```

Embed the source:

```rust
const CAPABILITIES_TOML: &str = include_str!("../../../capabilities.toml");
```

Parse it once through `OnceLock`.

The existing generator and validator remain authoritative. Runtime parsing failure is an internal build defect and must return an internal structured error rather than silently returning zero rows.

### 7.7.5 SQL dispatch

Implement `SHOW` as syntactic sugar over:

```sql
SELECT ...
FROM rockstream_catalog.capabilities
ORDER BY kind, capability_id;
```

The table and `SHOW` command must share the same row builder.

### 7.7.6 Permissions

Capability metadata is safe for every authenticated user and for auth-off evaluation mode.

### 7.7.7 Tests

- embedded file parses
- result count equals `capabilities.toml`
- every row matches the generator input
- stable ordering
- `FOR` finds an exact ID
- unknown ID returns zero rows or a clearly documented error; choose one behavior and test it
- simple and extended pgwire protocols
- capability contract mutation causes tests to fail until docs and runtime result update

### 7.7.8 Acceptance criteria

- Runtime capability output and generated Markdown come from one source.
- No hand-written capability list exists in the gateway.
- `SHOW` and catalog-table results are identical.
- The command is included in dispatch-wiring audits and language documentation.

### 7.7.9 Estimated effort

**4–6 engineer-days**, shared with CAT-01.

---

## 7.8 OBS-02 — `SELECT rockstream_version()`

### 7.8.1 Objective

Return the semantic version of the server process over pgwire.

### 7.8.2 SQL contract

```sql
SELECT rockstream_version();
```

Result:

| Column | Type |
|---|---|
| `rockstream_version` | `TEXT` |

The value is exactly `CandidateIdentity::current().semantic_version`.

Example:

```text
0.59.5
```

### 7.8.3 Scope

`rockstream_version()` is an introspection function, not an incremental data function.

It must be:

- Allowed in standalone `SELECT`.
- Allowed through simple and extended query protocols.
- Allowed in prepared statements.
- Rejected inside `CREATE MATERIALIZED VIEW` and other persisted incremental definitions, because its value changes when the software is upgraded.

Use an actionable `RS-1013` unsupported-plan error for persisted-plan usage.

### 7.8.4 Implementation options

Preferred implementation:

- Add a small special introspection expression path in gateway query dispatch.
- Reuse `CandidateIdentity`.
- Do not create a second version source.

Alternative:

- Register a stable zero-argument DataFusion UDF for ad hoc queries.
- Add a planner guard preventing it from entering incremental PlanIR.

The preferred choice is whichever requires less special-casing while preserving the persisted-plan restriction.

### 7.8.5 Tests

- returns package version
- simple query
- extended query
- prepared statement
- text format
- binary request receives a valid supported representation or an explicit fallback
- rejected in materialized view
- CLI version and SQL version agree

### 7.8.6 Acceptance criteria

- The SQL value equals the CLI’s semantic version.
- It does not duplicate build metadata logic.
- It cannot be accidentally persisted into a maintained view.
- It is documented in the SQL feature matrix.

### 7.8.7 Estimated effort

**1–2 engineer-days.**

---

## 7.9 OBS-03 — Richer `SHOW VIEW STATUS`

### 7.9.1 Objective

Make one query sufficient to understand whether a view is healthy, how fresh it is, how much state it uses, whether it is spilling, where it is in backfill/recovery, and what an operator should do next.

### 7.9.2 Compatibility rule

Existing columns and order remain unchanged. New columns are appended.

### 7.9.3 New columns

Append:

| Column | Type | Nullable | Meaning |
|---|---|---:|---|
| `frontier_epoch` | `BIGINT` | yes | Latest committed view frontier |
| `checkpoint_id` | `BIGINT` | yes | Latest checkpoint containing the view |
| `backfill_progress_pct` | `DOUBLE PRECISION` | yes | `0.0..=100.0` while backfilling |
| `state_bytes` | `BIGINT` | no | Total durable operator state attributed to the view |
| `memory_bytes` | `BIGINT` | no | Current in-memory state |
| `spill_bytes` | `BIGINT` | no | Current spilled state |
| `storage_pressure_state` | `TEXT` | no | Stable enum such as `NORMAL`, `THROTTLED`, `SHEDDING`, `BLOCKED` |
| `last_recovery_at_ms` | `BIGINT` | yes | Unix timestamp of most recent recovery transition |
| `last_recovery_reason` | `TEXT` | yes | Stable recovery reason |
| `recommended_action` | `TEXT` | no | Human-readable next action |

Existing `source_lag_ms` in the stage-lag breakdown remains the source-lag field. Do not duplicate it under a second name.

### 7.9.4 Shared status model

Add a single normalized model in `rockstream-types`:

```rust
pub struct ViewRuntimeStatusSnapshot {
    pub frontier_epoch: Option<u64>,
    pub checkpoint_id: Option<u64>,
    pub stage_lag: StageLagBreakdown,
    pub degradation: DegradationStatus,
    pub backfill: Option<BackfillStatus>,
    pub state_bytes: u64,
    pub memory_bytes: u64,
    pub spill_bytes: u64,
    pub storage_pressure: StoragePressureState,
    pub last_recovery: Option<RecoveryStatus>,
}
```

Both CLI and SQL convert from this model.

### 7.9.5 Recommended-action mapping

Add a pure, exhaustive function:

```rust
pub fn recommended_action(reason: DegradationReason) -> &'static str
```

Examples:

| Reason | Action |
|---|---|
| Healthy | `No action required.` |
| WaitingOnSource | `Check source connectivity and source lag.` |
| Backfilling | `Wait for backfill or add capacity if ETA exceeds the target.` |
| Recovering | `Monitor checkpoint recovery; inspect the last recovery reason.` |
| StoragePressure | `Reduce ingestion, increase the state budget, or restore object-store performance.` |
| OverBudgetRelaxed | `Increase the workload memory/state budget or reduce view state.` |
| Blocked | `Inspect the reason code and unblock the named dependency.` |

The mapping must be exhaustive so adding a new reason fails compilation or tests until an action is supplied.

### 7.9.6 Data ownership

Avoid reading unrelated globals independently in each formatter. Introduce one status registry/provider that composes:

- existing stage-lag metrics
- existing state-byte metrics
- spill metrics
- checkpoint metadata
- frontier metadata
- backfill progress
- latest recovery audit/status event

The latest recovery record is bounded to one record per view.

### 7.9.7 Tests

- healthy view
- source-lag degradation
- backfill progress at 0, mid-point, and 100
- spill state
- storage-pressure states
- recovery fields
- recommended action for every degradation enum
- no integer overflow in progress calculation
- SQL and CLI produce equivalent data
- appended-column compatibility test
- namespace filter
- `SHOW VIEW STATUS FOR <name>`
- `SHOW VIEW STATUS FOR NAMESPACE <name>`

### 7.9.8 Acceptance criteria

- One view-status query provides enough information for first-response diagnosis.
- No status field is fabricated from static defaults when runtime data is available.
- Missing data is nullable and explicit.
- State remains bounded by the number of registered views.
- CLI and SQL share the same model and action mapping.

### 7.9.9 Estimated effort

**6–9 engineer-days.**

---

## 7.10 DOC-01 — Generated `RS-XXXX` error documentation

### 7.10.1 Objective

Make every public error discoverable and consistent across code, CLI JSON, pgwire hints, and documentation.

### 7.10.2 Canonical source

Create:

```text
error-catalog.toml
```

Example:

```toml
[[error]]
code = "RS-2001"
constant = "RS_2001"
slug = "view-not-found"
subsystem = "gateway"
title = "View not found"
severity = "error"
retryable = false
cause = "The requested view is not present in the visible namespace."
next_steps = [
  "Run `rockstream view list` or query the catalog.",
  "Check the namespace and spelling."
]
```

The catalog must contain every public constant.

### 7.10.3 Generated artifacts

Generate:

- `crates/rockstream-types/src/error_code_generated.rs`
- `docs/errors/index.md`
- `docs/errors/RS-XXXX.md` for every code
- optional machine-readable `docs/errors/catalog.json`

`error_code.rs` reexports generated constants and descriptors so existing imports remain valid.

### 7.10.4 Descriptor API

```rust
pub struct ErrorDescriptor {
    pub code: ErrorCode,
    pub constant: &'static str,
    pub slug: &'static str,
    pub subsystem: &'static str,
    pub title: &'static str,
    pub severity: Severity,
    pub retryable: bool,
    pub cause: &'static str,
    pub next_steps: &'static [&'static str],
}
```

Provide:

```rust
pub fn descriptor(code: ErrorCode) -> Option<&'static ErrorDescriptor>;
pub fn next_steps(code: ErrorCode) -> &'static str;
pub fn documentation_path(code: ErrorCode) -> Option<&'static str>;
```

### 7.10.5 Generator

Add:

```text
scripts/generate-error-reference.py
scripts/check-error-reference.sh
scripts/check-error-reference.test.sh
```

The generator validates:

- Code format.
- Numeric uniqueness.
- Constant-name uniqueness.
- Non-empty title/cause/next steps.
- Valid severity.
- Documentation filename agreement.
- Every Rust constant is represented.
- No catalog record lacks a generated constant.
- Stable numeric ranges by subsystem where existing policy applies.

### 7.10.6 Migration strategy

1. Inventory all existing constants and `next_steps()` entries.
2. Populate `error-catalog.toml`.
3. Generate code that preserves current constant names.
4. Compare generated severity and next steps against current behavior.
5. Switch callers to generated helpers.
6. Remove duplicate hand-maintained match arms only after parity tests pass.

### 7.10.7 User-facing integration

CLI JSON errors include:

- code
- title/message
- retryable
- next steps
- documentation path

Pgwire errors should use PostgreSQL’s detail/hint fields where supported:

- primary message
- detail with RockStream code/slug
- hint with next steps

### 7.10.8 Tests

- every constant has a descriptor
- every descriptor has a generated Markdown file
- no duplicate code
- no empty next steps
- existing code formatting remains `RS-XXXX`
- CLI error JSON includes descriptor metadata
- pgwire error includes code and hint
- mutation self-test catches missing catalog entries
- generated tree is clean under `--check`

### 7.10.9 Acceptance criteria

- Every public code has one canonical structured record.
- Generated docs are reproducible.
- Existing imports do not require a mass caller rewrite.
- CI fails on drift.
- New error codes cannot be merged without documentation metadata.

### 7.10.10 Estimated effort

**7–12 engineer-days**, depending on registry size.

---

## 7.11 SQL-01 — `UPDATE ... RETURNING` conformance hardening

### 7.11.1 Baseline

The gateway already contains a true read-modify-write `UPDATE` handler and a shared returning-response builder.

This item is primarily a contract-completion and conformance task.

### 7.11.2 v1 syntax contract

Required:

```sql
UPDATE table_name
SET column = value [, ...]
WHERE primary_key_column = value
RETURNING *;

UPDATE table_name
SET column = value
WHERE primary_key_column = value
RETURNING column [, ...];

UPDATE table_name
SET column = value
WHERE primary_key_column = value
RETURNING column AS alias;
```

Out of scope:

- arbitrary expressions in `RETURNING`
- subqueries
- aggregates
- multi-table updates
- `UPDATE ... FROM`
- non-key full-table predicates unless already supported independently

### 7.11.3 Semantics

- Returned row is the post-update image.
- No matching row produces zero returned rows and `UPDATE 0`.
- One matching row produces one row and `UPDATE 1`.
- Column order follows the `RETURNING` list.
- `RETURNING *` follows declared table-column order.
- Alias names become row-description field names.
- Duplicate requested columns are allowed only if PostgreSQL-compatible field naming is deterministic; otherwise reject clearly.
- Within an explicit transaction, returned values represent the transaction-local post-update state.
- A later rollback does not invalidate the already returned statement result, but durable table/view state must revert.
- View maintenance receives a `-1` old row and `+1` new row exactly once.

### 7.11.4 Implementation tasks

- Extract or formalize a parsed `ReturningProjection`.
- Add alias support if absent.
- Ensure parser rejects malformed clauses with `RS-2022`.
- Ensure command tags contain exact affected-row counts.
- Ensure text and binary row-description paths use correct OIDs.
- Update `capabilities.toml` and language docs to name `UPDATE ... RETURNING`.
- Reuse `build_returning_response`; do not create a second response encoder.

### 7.11.5 Tests

Add or expand gateway tests for:

- `RETURNING *`
- single column
- multiple columns
- aliases
- null values
- no matching row
- prepared statement
- simple query
- extended query
- explicit transaction commit
- explicit transaction rollback
- read-your-writes
- updated materialized view equals batch recomputation
- restart after commit preserves post-image
- malformed clause
- unknown column
- duplicate assignment
- authorization failure

### 7.11.6 Acceptance criteria

- The feature is documented as implemented, not merely present in code.
- Simple and extended pgwire paths agree.
- Command tags and row counts are exact.
- View deltas remain correct through commit and rollback.
- No new storage scan is introduced beyond the existing bounded key lookup.

### 7.11.7 Estimated effort

**3–5 engineer-days.**

---

## 7.12 SQL-02 — `DELETE ... RETURNING` conformance hardening

### 7.12.1 Baseline

The gateway already captures the pre-delete image and uses it for both `RETURNING` and correct view retraction.

### 7.12.2 v1 syntax contract

Required:

```sql
DELETE FROM table_name
WHERE primary_key_column = value
RETURNING *;

DELETE FROM table_name
WHERE primary_key_column = value
RETURNING column [, ...];

DELETE FROM table_name
WHERE primary_key_column = value
RETURNING column AS alias;
```

Out of scope:

- arbitrary expressions
- subqueries
- `DELETE ... USING`
- non-key full-table deletion unless independently supported

### 7.12.3 Semantics

- Returned row is the pre-delete image.
- No matching row produces zero returned rows and `DELETE 0`.
- One matching row produces one returned row and `DELETE 1`.
- The pre-image is captured before buffering/committing the delete.
- Explicit transaction rollback restores the row and maintained-view contribution.
- `RETURNING *` follows declared table-column order.

### 7.12.4 Implementation tasks

Share `ReturningProjection` with SQL-01.

Ensure:

- no-match behavior is not reported as `DELETE 1`
- returned pre-image and view retraction use the same captured row
- aliases and row-description names are correct
- malformed projection returns `RS-2022`
- documentation and capability records are updated

### 7.12.5 Tests

- `RETURNING *`
- selected columns
- aliases
- nulls
- no match
- prepared statement
- explicit commit and rollback
- view result after delete
- restart after commit
- malformed clause
- unknown column
- authorization
- source-epoch/idempotency interaction where relevant

### 7.12.6 Acceptance criteria

- The returned row is exactly the row retracted from maintained views.
- No second table read is required after deletion.
- Command tags and row counts are correct.
- SQL and docs explicitly include the feature.

### 7.12.7 Estimated effort

**2–4 engineer-days**, sharing work with SQL-01.

---

## 7.13 SQL-03 — Consistent `IF EXISTS` and `IF NOT EXISTS`

### 7.13.1 Objective

Make idempotent setup and teardown scripts safe and predictable.

### 7.13.2 Required object matrix

#### `CREATE ... IF NOT EXISTS`

- `CREATE TABLE`
- `CREATE VIEW`
- `CREATE MATERIALIZED VIEW`
- `CREATE INDEX`
- `CREATE WORKLOAD`
- `CREATE SOURCE`
- `CREATE SECRET`

#### `DROP ... IF EXISTS`

- `DROP TABLE`
- `DROP VIEW`
- `DROP MATERIALIZED VIEW`
- `DROP INDEX`
- `DROP WORKLOAD`
- `DROP SOURCE`
- `DROP SECRET`

Only object families already implemented without the modifier are included.

### 7.13.3 Semantics

#### Existing object with `IF NOT EXISTS`

- No mutation.
- Return the normal command tag.
- Emit a PostgreSQL `NOTICE`.
- Write an audit event with `outcome=noop`.
- Do not silently accept a conflicting object kind under the same name.
- Do not compare definitions for equivalence; PostgreSQL-style `IF NOT EXISTS` does not guarantee that the existing definition matches.

#### Missing object with `IF EXISTS`

- No mutation.
- Return the normal command tag.
- Emit a `NOTICE`.
- Write a bounded audit event with `outcome=noop`.

#### Without modifier

Existing duplicate/missing-object errors remain unchanged.

### 7.13.4 Parsing design

Introduce a shared model:

```rust
pub struct DdlExistenceModifier {
    pub if_exists: bool,
    pub if_not_exists: bool,
}
```

Custom DDL parsers should normalize into a common `DdlCommand` instead of each handler searching raw substrings.

For DataFusion/sqlparser-supported statements, read the AST flags.

For RockStream-specific statements, use one token-aware helper that handles:

- quoted identifiers
- whitespace
- optional `MATERIALIZED`
- semicolons
- case-insensitivity

Do not implement this by unconstrained `contains("if exists")`.

### 7.13.5 Notices and SQLSTATE

Use PostgreSQL-compatible notice classes where practical.

The exact notice text must be stable enough for tests but should not be treated as a permanent API. The `RS-XXXX` error contract applies to failures; successful no-op notices do not require a new error code.

### 7.13.6 Tests

Create a matrix test across every object family:

- create absent object
- create existing object without modifier fails
- create existing object with modifier succeeds/no-op
- drop existing object
- drop missing object without modifier fails
- drop missing object with modifier succeeds/no-op
- quoted names
- mixed case
- simple and extended query protocol
- audit event
- no mutation on no-op
- conflicting object kind
- dependency constraints remain enforced when object exists and a real drop is attempted

### 7.13.7 Acceptance criteria

- The full object matrix is implemented or explicitly reduced before coding begins.
- All families share one modifier model.
- No-op behavior is observable through notice and audit.
- Existing destructive/dependency protections remain intact.
- Documentation contains a concise matrix.

### 7.13.8 Estimated effort

**5–8 engineer-days.**

---

## 7.14 SQL-04 — Common scalar functions

### 7.14.1 Objective

Support a deliberately limited set of high-frequency scalar functions with correct type, null, incremental, and pgwire behavior.

### 7.14.2 Required functions

#### Null and comparison

- `COALESCE`
- `NULLIF`
- `GREATEST`
- `LEAST`

#### Text

- `LOWER`
- `UPPER`
- `CONCAT`

#### Date/time

- `DATE_TRUNC`
- `EXTRACT`
- `TO_TIMESTAMP`
- `TO_TIMESTAMP_MILLIS`
- `TO_TIMESTAMP_MICROS`

### 7.14.3 Why this item needs an IR improvement

The existing expression evaluator supports a narrow set of named scalar UDFs and still contains untyped literal-byte behavior. Correct `NULL`, timestamp, and multi-type function semantics should not be implemented by adding more string-name branches over ambiguous bytes.

Introduce typed scalar expression support before adding the functions.

### 7.14.4 PlanIR additions

Recommended additive types:

```rust
pub enum ScalarType {
    Int64,
    Float64,
    Boolean,
    Utf8,
    Date32,
    TimestampMillis,
    TimestampMicros,
    TimestampMillisUtc,
}

pub enum ScalarLiteral {
    Null(ScalarType),
    Int64(i64),
    Float64(u64), // canonical bits
    Boolean(bool),
    Utf8(String),
    Date32(i32),
    TimestampMillis(i64),
    TimestampMicros(i64),
    TimestampMillisUtc(i64),
}

pub enum ScalarFunction {
    Coalesce,
    NullIf,
    Greatest,
    Least,
    Lower,
    Upper,
    Concat,
    DateTrunc,
    Extract,
    ToTimestamp,
    ToTimestampMillis,
    ToTimestampMicros,
}

pub enum Expr {
    // existing variants retained
    TypedLiteral(ScalarLiteral),
    ScalarFunction {
        function: ScalarFunction,
        args: Vec<Expr>,
        return_type: ScalarType,
    },
}
```

Existing `Expr::Literal(Vec<u8>)` and `Expr::ScalarUdf` can remain for compatibility during migration, but new functions must use typed variants.

### 7.14.5 Type matrix

| Function | Accepted input types | Return type |
|---|---|---|
| `COALESCE` | Same/coercible Core scalar type; 2–16 args | Common coerced type |
| `NULLIF` | Two values of same/coercible Core scalar type | Left/common type |
| `GREATEST`, `LEAST` | `BIGINT`, `TEXT`, `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`; 2–16 args | Input type |
| `LOWER`, `UPPER` | `TEXT` | `TEXT` |
| `CONCAT` | Core scalar types; 1–16 args | `TEXT` |
| `DATE_TRUNC` | Supported field plus `TIMESTAMP`/`TIMESTAMPTZ` | Same timestamp family |
| `EXTRACT` | Supported field plus `DATE`/`TIMESTAMP`/`TIMESTAMPTZ` | `DOUBLE PRECISION` for the v1 RockStream subset |
| `TO_TIMESTAMP` | `DOUBLE PRECISION` Unix seconds | `TIMESTAMPTZ` |
| `TO_TIMESTAMP_MILLIS` | `BIGINT` | `TIMESTAMPTZ` |
| `TO_TIMESTAMP_MICROS` | `BIGINT` | `TIMESTAMPTZ` |

`GREATEST`/`LEAST` over floating-point values are excluded from the first v1 implementation to avoid an undocumented NaN ordering contract.

### 7.14.6 Null semantics

- `COALESCE`: first non-null, null if all null.
- `NULLIF(a, b)`: null when `a = b`; otherwise `a`; null `a` remains null.
- `GREATEST`/`LEAST`: PostgreSQL behavior—ignore null arguments and return null only when all arguments are null.
- `LOWER`/`UPPER`: null-preserving.
- `CONCAT`: null arguments are ignored; all-null input produces an empty string.
- Date/time functions are null-preserving.

### 7.14.7 Text semantics

- Use Unicode, locale-independent Rust case conversion.
- No locale parameter.
- No collation support.
- `CONCAT` textual conversion must be deterministic and match the pgwire text representation for the supported Core types.
- Result-size bounds must be enforced. A per-row concatenated value may not exceed the existing row/value byte budget.

### 7.14.8 Date/time semantics

- All `TIMESTAMPTZ` values are normalized to UTC.
- No named timezone database is introduced.
- `DATE_TRUNC` fields:
  - `second`
  - `minute`
  - `hour`
  - `day`
  - `week` using ISO Monday
  - `month`
  - `quarter`
  - `year`
- `EXTRACT` fields:
  - `epoch`
  - `second`
  - `minute`
  - `hour`
  - `day`
  - `dow`
  - `isodow`
  - `doy`
  - `week`
  - `month`
  - `quarter`
  - `year`
- Invalid field names fail with an actionable SQL error.
- Timestamp conversion checks overflow and invalid floating-point input. It must not saturate silently.

### 7.14.9 Lowering

Modify `crates/rockstream-sql/src/lower.rs`:

- Map DataFusion function names to `ScalarFunction`.
- Validate argument count.
- Resolve/coerce supported types.
- Produce typed literals.
- Reject unsupported type combinations with `RS-1013`.
- Preserve function aliases and output schema.

### 7.14.10 Evaluation

Modify `crates/rockstream-ops/src/expr.rs`:

- Evaluate through Arrow arrays while preserving null bitmaps.
- Use Arrow kernels where available.
- Use `chrono` for UTC date/time decomposition and truncation.
- Enforce per-value and per-batch bounds.
- Never convert null to zero or empty bytes accidentally.
- Return typed arrays and correct Arrow data types.

### 7.14.11 Oracle and incremental correctness

For every function:

```text
incremental(materialized view using function, changes)
==
batch(DataFusion query using function, accumulated rows)
```

Test sequences must include inserts, updates, deletes, nulls, duplicates, and empty inputs.

### 7.14.12 Tests

#### Unit

- each function/type combination
- null matrix
- invalid argument count
- invalid type
- overflow
- Unicode case conversion
- ISO week boundaries
- leap year
- month/quarter/year boundaries
- negative Unix timestamps
- microsecond/millisecond precision

#### Planner

- SQL parses and lowers to the expected typed PlanIR
- unsupported type combinations fail
- output schema and pgwire OID are correct

#### End-to-end

- maintained views using each function
- update/delete propagation
- restart and checkpoint recovery of materialized outputs
- simple and extended query protocols
- prepared statements where parameters are relevant

#### Property tests

- `LOWER(LOWER(x)) = LOWER(x)`
- `UPPER(UPPER(x)) = UPPER(x)`
- `DATE_TRUNC(unit, DATE_TRUNC(unit, x)) = DATE_TRUNC(unit, x)`
- `COALESCE(x, y)` returns one of the inputs according to null order
- `NULLIF(x, x)` is null for supported equality types
- `LEAST(args) <= every non-null arg`
- `GREATEST(args) >= every non-null arg`

#### Performance

Add a Criterion benchmark over at least one million scalar values for:

- `LOWER`
- `COALESCE`
- `DATE_TRUNC`
- `CONCAT`

Document the accepted baseline. No per-row regex compilation or heap allocation in the hot path where avoidable.

### 7.14.13 Capability classification

These functions should initially be a distinct capability record, for example:

```text
language.common-scalar-functions
```

Promotion to Core requires:

- public pgwire reachability
- incremental/batch proof
- negative tests
- explicit state-growth statement: stateless and bounded by one output batch/value budget
- failure semantics

### 7.14.14 Acceptance criteria

- Every listed function has a documented type and null matrix.
- The evaluator preserves nulls.
- Incremental results equal the batch oracle.
- Unsupported combinations fail clearly.
- No arbitrary timezone or collation behavior is implied.
- Function output OIDs are tested through pgwire.

### 7.14.15 Estimated effort

**12–18 engineer-days.** This is the largest item in the “small features” group.

---

## 7.15 CAT-01 — Read-only RockStream system catalog

### 7.15.1 Objective

Expose stable, queryable metadata through ordinary SQL.

Required tables:

- `rockstream_catalog.nodes`
- `rockstream_catalog.sources`
- `rockstream_catalog.views`
- `rockstream_catalog.checkpoints`
- `rockstream_catalog.capabilities`

### 7.15.2 Architecture

Introduce a coherent system-catalog snapshot layer rather than adding more string-substring branches to `CatalogStubs`.

Recommended model:

```rust
pub struct SystemCatalogSnapshot {
    pub nodes: Vec<NodeCatalogRow>,
    pub sources: Vec<SourceCatalogRow>,
    pub views: Vec<ViewCatalogRow>,
    pub checkpoints: Vec<CheckpointCatalogRow>,
    pub capabilities: Vec<CapabilityCatalogRow>,
    pub captured_at_ms: u64,
}

pub trait SystemCatalogSnapshotProvider: Send + Sync {
    fn snapshot(&self) -> Arc<SystemCatalogSnapshot>;
}
```

The snapshot is immutable once published and replaced atomically. Readers never hold a mutation lock while encoding rows.

For a standalone or `role=all` gateway:

- local node and in-process catalog state populate the snapshot directly.

For a distributed gateway:

- a bounded background refresh task reads control-plane state at a configured interval.
- stale snapshots remain queryable with an explicit `captured_at_ms`.
- refresh failures do not erase the last successful snapshot.
- staleness is exposed rather than hidden.

### 7.15.3 Query support

These should behave as actual read-only relations, not merely fixed `SELECT *` strings.

Preferred implementation:

- Register a `rockstream_catalog` DataFusion schema.
- Provide dynamic read-only `TableProvider` implementations backed by the snapshot.
- Permit ordinary projection, filtering, ordering, and limits through the existing query path.

Writes, DDL, and `COPY` against these tables fail clearly.

If a full dynamic `TableProvider` is too large for the milestone, the minimum acceptable first implementation must support:

- projection
- simple equality filters
- `ORDER BY`
- `LIMIT`

through parsed SQL, not unbounded substring matching.

### 7.15.4 Table contracts

#### `rockstream_catalog.nodes`

| Column | Type | Notes |
|---|---|---|
| `node_id` | `TEXT` | Stable node identifier |
| `role` | `TEXT` | `control`, `worker`, `gateway`, `frontier`, or `all` |
| `status` | `TEXT` | Stable lifecycle state |
| `host_id` | `TEXT` nullable | Same-host identity |
| `availability_zone` | `TEXT` nullable | Placement metadata |
| `software_version` | `TEXT` | Candidate semantic version |
| `last_heartbeat_ms` | `BIGINT` nullable | Last observed heartbeat |
| `assigned_shards` | `BIGINT` | Count only; no unbounded array |
| `is_control_leader` | `BOOLEAN` | Current control leader |
| `captured_at_ms` | `BIGINT` | Snapshot capture time |

#### `rockstream_catalog.sources`

| Column | Type |
|---|---|
| `namespace` | `TEXT` |
| `source_name` | `TEXT` |
| `source_type` | `TEXT` |
| `table_name` | `TEXT` nullable |
| `status` | `TEXT` |
| `current_offset` | `TEXT` nullable |
| `lag_ms` | `BIGINT` nullable |
| `committed_checkpoint_id` | `BIGINT` nullable |
| `buffer_fill` | `BIGINT` |
| `blocked_reason` | `TEXT` nullable |
| `captured_at_ms` | `BIGINT` |

Connector options and credentials are not exposed.

#### `rockstream_catalog.views`

| Column | Type |
|---|---|
| `namespace` | `TEXT` |
| `view_name` | `TEXT` |
| `view_kind` | `TEXT` |
| `state` | `TEXT` |
| `workload_name` | `TEXT` nullable |
| `frontier_epoch` | `BIGINT` nullable |
| `checkpoint_id` | `BIGINT` nullable |
| `freshness_slo_ms` | `BIGINT` nullable |
| `freshness_lag_ms` | `BIGINT` nullable |
| `state_bytes` | `BIGINT` |
| `memory_bytes` | `BIGINT` |
| `spill_bytes` | `BIGINT` |
| `degradation_reason` | `TEXT` |
| `reason_code` | `TEXT` |
| `captured_at_ms` | `BIGINT` |

The full SQL definition may remain available through existing PostgreSQL-compatible metadata views. Avoid duplicating potentially sensitive definitions unless RBAC filtering is explicit.

#### `rockstream_catalog.checkpoints`

| Column | Type |
|---|---|
| `checkpoint_id` | `BIGINT` |
| `status` | `TEXT` |
| `created_at_ms` | `BIGINT` |
| `committed_frontier_epoch` | `BIGINT` nullable |
| `shard_count` | `BIGINT` |
| `object_count` | `BIGINT` nullable |
| `byte_count` | `BIGINT` nullable |
| `export_status` | `TEXT` nullable |
| `captured_at_ms` | `BIGINT` |

The number of rows is bounded by checkpoint retention.

#### `rockstream_catalog.capabilities`

Use the columns specified in OBS-01.

### 7.15.5 RBAC and namespaces

- Capabilities: visible to everyone.
- Nodes and checkpoints: visible to authenticated viewers unless existing policy requires admin-only exposure.
- Views and sources: filter by visible namespace.
- No row may reveal a secret reference value or connector credential.
- Authorization is applied before row encoding.

### 7.15.6 Integration with `SHOW`

- `SHOW ROCKSTREAM CAPABILITIES` reads the capabilities table.
- `SHOW VIEW STATUS` reads the same normalized view-status model used by the views table.
- CLI source/view/checkpoint/node commands should be able to reuse row models where practical.

### 7.15.7 Tests

#### Table registration

- all five tables appear in schema metadata
- correct column names and OIDs
- stable row ordering when no explicit order is requested, or explicit documentation that order is unspecified

#### Query behavior

- `SELECT *`
- projection
- equality filter
- order
- limit
- prepared statement
- simple and extended protocols

#### State behavior

- node joins/leaves
- source pause/resume
- view backfill/recovery
- checkpoint creation/retention
- capability contract update

#### Security

- namespace filtering
- no options/credentials/private-key paths
- unauthorized mutation fails
- snapshot remains bounded

#### Staleness

- distributed refresh failure retains last snapshot
- `captured_at_ms` exposes age
- stale data is not silently represented as current

### 7.15.8 Acceptance criteria

- The tables are actual read-only query surfaces.
- All data comes from normalized runtime snapshots or the embedded capability contract.
- Catalogs do not disclose secrets.
- Result size is bounded.
- CLI, `SHOW`, and catalog output do not maintain contradictory models.
- Documentation includes a schema reference and examples.

### 7.15.9 Estimated effort

**9–14 engineer-days**, partly shared with OBS-01 and OBS-03.

---

# 8. Cross-feature architecture changes

## 8.1 New or refactored modules

Recommended file layout:

```text
crates/rockstream-cli/src/
  config_resolver.rs
  config_commands.rs
  demo.rs
  doctor.rs
  output.rs

crates/rockstream-gateway/src/
  system_catalog.rs
  introspection.rs

crates/rockstream-types/src/
  capability.rs
  config_validation.rs
  error_code_generated.rs
  system_catalog.rs
  view_status.rs

scripts/
  generate-error-reference.py
  check-error-reference.sh
  check-error-reference.test.sh

docs/
  quickstart.md
  errors/
    index.md
    RS-0001.md
    ...
  system-catalog.md
```

The exact names may change, but responsibilities should remain separated.

## 8.2 Avoid further growth of large dispatch modules

`crates/rockstream-cli/src/main.rs` and `crates/rockstream-gateway/src/server.rs` are already broad dispatch points.

The new features should route quickly into focused modules:

- CLI parsing in `main.rs`; command logic elsewhere.
- SQL dispatch recognition in `server.rs`; introspection/catalog implementation elsewhere.
- View-status composition in a shared provider, not in output formatting.
- Scalar-function evaluation in `expr.rs` helpers or a dedicated scalar module.

## 8.3 Capability records to add or update

Suggested additions:

- `cli.demo`
- `cli.doctor`
- `cli.config-introspection`
- `cli.structured-output`
- `language.runtime-introspection`
- `language.idempotent-ddl`
- `language.common-scalar-functions`
- `catalog.system-introspection`

Suggested DML update:

- Expand the existing DML capability proof and documentation to include all three `RETURNING` forms.

CLI capabilities may require extending the generator’s current allowed `kind` set beyond `language`, `connector`, and `sink`. If the team does not want to broaden `capabilities.toml` to CLI surfaces, keep CLI conformance in a separate `cli-capabilities.toml`. Prefer extending the existing source if it remains conceptually clean.

---

# 9. Test strategy

## 9.1 Required layers

| Feature class | Unit | CLI integration | Raw pgwire | Oracle | LFS/MinIO | Negative |
|---|---:|---:|---:|---:|---:|---:|
| Demo | yes | yes | yes | final result | LFS | yes |
| Doctor | yes | yes | optional gateway | no | local + optional object store | yes |
| Config | yes | yes | no | no | no | yes |
| Output/completions | yes | yes | no | no | no | yes |
| Capabilities/version | yes | optional | yes | no | no | yes |
| View status/catalog | yes | yes | yes | no | runtime snapshot tests | yes |
| Error docs | generator tests | CLI error | pgwire error | no | no | mutation tests |
| `RETURNING` | parser/unit | no | yes | view result | LFS and existing durable path | yes |
| DDL modifiers | parser/unit | no | yes | no | catalog persistence where relevant | yes |
| Scalar functions | yes | no | yes | mandatory | LFS; MinIO for persisted-view recovery sample | yes |

## 9.2 Conformance suites

Create focused suites rather than placing every test in existing giant files:

```text
crates/rockstream-cli/tests/product_polish_cli_tests.rs
crates/rockstream-gateway/tests/runtime_introspection_pgwire_tests.rs
crates/rockstream-gateway/tests/returning_conformance_tests.rs
crates/rockstream-gateway/tests/ddl_existence_modifiers_tests.rs
crates/rockstream-gateway/tests/common_scalar_functions_pgwire_tests.rs
crates/rockstream-gateway/tests/system_catalog_tests.rs
crates/rockstream-ops/tests/common_scalar_functions_oracle_tests.rs
```

## 9.3 No-vacuous-proof rules

- A demo test must query the real view.
- Doctor negative tests must observe the expected failed check.
- Config precedence tests must compare against the actual `StartOptions.config`.
- Catalog tests must mutate the backing snapshot and observe changed SQL rows.
- Scalar-function oracle tests must include at least one mutation after initial materialization.
- `RETURNING` tests must compare returned rows with stored/view state.
- DDL no-op tests must verify no catalog mutation occurred.

---

# 10. Documentation deliverables

## 10.1 New documents

- `docs/quickstart.md`
- `docs/system-catalog.md`
- `docs/errors/index.md`
- generated `docs/errors/RS-XXXX.md`

## 10.2 Existing documents to update

- `README.md`
- `docs/cli.md`
- `docs/configuration.md`
- `docs/language-features.md`
- `docs/capability-matrix.md` through generation
- `docs/diagnostics.md`
- `docs/sre-operations.md`
- `docs/pgwire-conformance.md`
- `capabilities.toml`

## 10.3 Required examples

Documentation must show:

```bash
rockstream demo
rockstream doctor --config ./rockstream.toml
rockstream config validate --file ./rockstream.toml
rockstream config print-effective --show-origins
rockstream completions zsh
rockstream view status --output json
```

And:

```sql
SELECT rockstream_version();
SHOW ROCKSTREAM CAPABILITIES;
SHOW VIEW STATUS;
SELECT * FROM rockstream_catalog.nodes;
SELECT * FROM rockstream_catalog.sources;
SELECT * FROM rockstream_catalog.views;
SELECT * FROM rockstream_catalog.checkpoints;
SELECT * FROM rockstream_catalog.capabilities;

UPDATE orders SET amount = 50 WHERE order_id = 1 RETURNING *;
DELETE FROM orders WHERE order_id = 1 RETURNING *;

CREATE TABLE IF NOT EXISTS orders (...);
DROP VIEW IF EXISTS sales_by_store;

SELECT COALESCE(name, 'unknown') FROM customers;
SELECT LOWER(name) FROM customers;
SELECT DATE_TRUNC('hour', event_time) FROM events;
```

---

# 11. CI and repository gates

Add or extend these checks:

1. **CLI output gate**
   - Every finite command has JSON coverage.
   - No unapproved direct stdout prose in JSON paths.

2. **Config conformance gate**
   - Docs default table matches `RockstreamConfig::default()`.
   - Unknown/deprecated fields are tested.
   - `print-effective` equals actual startup config.

3. **Capability runtime gate**
   - Embedded registry equals `capabilities.toml`.
   - Generated matrix is clean.
   - SQL capabilities rows equal registry.

4. **Error-reference gate**
   - Catalog, generated Rust, and generated docs are clean.
   - Every code has descriptor metadata.

5. **Dispatch-wiring gate**
   - Add new SQL/SHOW/catalog surfaces.

6. **Catalog mutation gate**
   - Read-only tables reject writes.

7. **Scalar-function matrix gate**
   - Every supported function/type cell has a named test.
   - Every unsupported cell has a negative test or a documented category rule.

8. **DML conformance gate**
   - INSERT, UPDATE, and DELETE returning paths remain reachable.

9. **DDL modifier matrix gate**
   - Every admitted object family has all four create/drop present/missing cases.

No test may be weakened, ignored, or converted into a text-presence assertion in place of execution.

---

# 12. Backward compatibility and migration

## 12.1 CLI

- `--json` remains accepted.
- New `--output json` is the documented form.
- New subcommands are additive.
- Existing command names remain unchanged.

## 12.2 Configuration

- Introduce strict unknown-field behavior before v1.
- Document every formerly ignored or removed key.
- `config validate` provides a migration path before `start` becomes strict.
- Environment-variable precedence is new and must be documented.

## 12.3 SQL

- New functions and modifiers are additive.
- Existing duplicate/missing-object errors remain unchanged when modifiers are absent.
- Existing `RETURNING` behavior is hardened, not replaced.
- New system-catalog tables are read-only.
- New `SHOW VIEW STATUS` columns are appended.

## 12.4 Error codes

- Preserve existing numeric codes and Rust constant names.
- Generate source-compatible reexports.
- Do not renumber codes to create prettier ranges.

---

# 13. Dependency and execution order

```text
CLI-01 structured output
        │
        ├── UX-01 demo
        ├── UX-02 doctor
        ├── CFG-01 validate
        ├── CFG-02 print-effective
        └── CLI-02 completions

CFG-01 validate
        └── CFG-02 shared resolver
                ├── start integration
                └── doctor integration

OBS-01 capability registry
        ├── SHOW ROCKSTREAM CAPABILITIES
        └── CAT-01 capabilities table

OBS-03 shared view status
        ├── SHOW VIEW STATUS
        └── CAT-01 views table

CandidateIdentity
        └── OBS-02 rockstream_version()

Shared ReturningProjection
        ├── SQL-01 UPDATE RETURNING
        └── SQL-02 DELETE RETURNING

Typed scalar IR
        └── SQL-04 common functions

All new errors
        └── DOC-01 final error catalog generation
```

---

# 14. Suggested pull-request breakdown

To keep reviewable diffs, use approximately these PRs:

1. `cli: introduce --output and structured error rendering`
2. `config: add semantic validation and unknown-key diagnostics`
3. `config: add shared resolver and print-effective`
4. `cli: add deterministic embedded demo`
5. `cli: add bounded doctor diagnostics`
6. `cli: add generated shell completions`
7. `types: add embedded capability registry`
8. `gateway: add capabilities SHOW command and catalog table`
9. `gateway: add rockstream_version()`
10. `types/gateway: normalize and enrich view runtime status`
11. `gateway: add nodes, sources, views, and checkpoints catalog providers`
12. `gateway: harden UPDATE and DELETE RETURNING conformance`
13. `sql/gateway: add shared IF EXISTS/IF NOT EXISTS handling`
14. `plan/sql: add typed scalar literals and function enum`
15. `ops: add null-preserving common scalar evaluation`
16. `gateway/oracle: add scalar function pgwire and incremental conformance`
17. `docs: generate complete RS error reference`
18. `docs: final capability, SQL, CLI, and catalog reconciliation`

PRs 14–16 may be split further into null/text and date/time work if review size becomes excessive.

---

# 15. Effort and staffing

These features are small from a product perspective, but together they are not a tiny patch.

Approximate total:

| Workstream | Effort |
|---|---:|
| CLI/config/demo/doctor/completions | 19–30 engineer-days |
| Runtime introspection and catalogs | 20–31 engineer-days |
| DML/DDL ergonomics | 10–17 engineer-days |
| Scalar functions and typed expression work | 12–18 engineer-days |
| Error catalog and final docs | 7–12 engineer-days |
| **Total** | **68–108 engineer-days** |

With three engineers working in parallel and a strict scope freeze, this is roughly a **five-to-eight-week engineering program**, excluding unrelated release qualification and long-duration gates.

The major uncertainty is SQL-04. If null-preserving typed expression support reveals wider assumptions in PlanIR or operator evaluation, split it into:

1. typed/null expression foundation,
2. null/text functions,
3. date/time functions.

Do not bypass that foundation with more ambiguous string-named UDF branches merely to shorten the schedule.

---

# 16. Global definition of done

The program is complete only when all of the following are true:

- [ ] `rockstream demo` proves a real incremental workflow through pgwire.
- [ ] `rockstream doctor` is bounded, non-destructive by default, and redaction-tested.
- [ ] `config validate` reports syntax, unknown keys, deprecated keys, and semantic errors.
- [ ] `config print-effective` uses the same resolver as `start`.
- [ ] Every finite CLI command supports `--output json`.
- [ ] Streaming JSON behavior is documented and tested.
- [ ] Bash, Zsh, and Fish completions are generated from the live command tree.
- [ ] `SHOW ROCKSTREAM CAPABILITIES` and `rockstream_catalog.capabilities` share one embedded registry.
- [ ] `rockstream_version()` agrees with `rockstream version`.
- [ ] `SHOW VIEW STATUS` includes frontier, checkpoint, state, spill, recovery, and recommended action.
- [ ] Every `RS-XXXX` code has generated documentation.
- [ ] `UPDATE ... RETURNING` and `DELETE ... RETURNING` pass the complete conformance matrix.
- [ ] The admitted DDL object families consistently support `IF EXISTS` and `IF NOT EXISTS`.
- [ ] Every common scalar function has a documented type/null matrix.
- [ ] Incremental function results equal the batch oracle.
- [ ] All five new catalog tables are read-only, bounded, and free of secrets.
- [ ] `capabilities.toml`, generated docs, SQL introspection, and proof tests agree.
- [ ] Dispatch-wiring, error-code, capability, and documentation checks pass.
- [ ] `cargo fmt`, `cargo clippy --workspace --all-targets -- -D warnings`, and the complete workspace test matrix pass.
- [ ] No test is ignored, weakened, or replaced by a source-text assertion.
- [ ] No new connector, protocol, lakehouse, or general database scope entered the work.

---

# 17. Final recommendation

Implementing all fifteen items before v1.0 is reasonable **only with a feature freeze elsewhere**.

The highest-value order is:

1. Configuration and structured CLI output.
2. Demo and doctor.
3. Runtime capability/version/status introspection.
4. Read-only catalogs.
5. DML/DDL ergonomics.
6. Typed common scalar functions.
7. Generated error reference and final contract reconciliation.

The project should treat `UPDATE ... RETURNING` and `DELETE ... RETURNING` as existing features that need formal closure, not new implementation work. The largest genuinely new technical item is the typed, null-preserving scalar-function layer.

Completed together, this program would make RockStream v1 materially better without changing what RockStream is: a focused, cloud-native incremental view maintenance system with a PostgreSQL-compatible access layer.

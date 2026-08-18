# RockStream Pre-v1.0 Product Experience and Quality Program
## Formal Description and Implementation Plan

**Status:** Proposed  
**Target:** Complete before `v1.0.0`  
**Repository baseline reviewed:** `main` at `aa5f03b091f581230603a336473ce2d87f176ae1`  
**Baseline package version:** `0.59.3`  
**Baseline roadmap state:** `v0.59.3` being implemented  
**Companion plan:** `rockstream-pre-v1-small-features-plan.md`  
**Proposed roadmap range:** `v0.59.8` through `v0.59.13`  
**Primary owners:** maintainers of `rockstream-cli`, `rockstream-gateway`, `rockstream-sql`, `rockstream-types`, `rockstream-control`, `rockstream-sim`, `rockstream-oracle`, and `rockstream-test-support`

---

## 1. Purpose

This document specifies four product-level initiatives that should be completed before RockStream declares `v1.0.0`:

1. **Make the documentation simple, current, and generated from one normalized product-surface source.**
2. **Create a very smooth golden path from installation or checkout to a useful maintained view.**
3. **Improve the quality of high-level tests so public product claims are exercised through public interfaces and independent oracles.**
4. **Make error messages excellent and consistent across pgwire, CLI, logs, metrics, support bundles, and documentation.**

These initiatives are broader than the fifteen small features in the companion plan. They build on those features rather than replacing them. In particular, this program assumes the companion milestones provide:

- `rockstream demo`
- `rockstream doctor`
- `rockstream config validate`
- `rockstream config print-effective`
- uniform structured CLI output
- shell completions
- runtime capability introspection
- `rockstream_version()`
- richer view status
- a generated error-code reference
- consistent DML and DDL ergonomics
- common scalar functions
- coherent read-only system catalogs

The purpose of the present program is to turn those individual features into one understandable, testable, and polished product experience.

---

## 2. Strategic classification

This program is **pre-v1 product completeness**, not new strategic breadth.

It does not create another connector family, execution engine, transaction model, catalog product, or distributed protocol. It strengthens the product RockStream already claims to be:

> A cloud-native incremental view maintenance system that ingests changing data, maintains durable SQL materialized views, and serves committed results through PostgreSQL-compatible clients.

All work in this plan therefore belongs to one of four accepted v1 concerns:

- usability
- contract accuracy
- correctness evidence
- operability

No item in this document should be used as a reason to reopen previously rejected scope such as new lakehouse sinks, general OLTP semantics, multi-region active/active operation, or a connector marketplace.

---

## 3. Baseline assessment

The reviewed repository already contains important foundations:

- `capabilities.toml` is a machine-readable capability contract.
- `scripts/generate-capability-matrix.py` generates `docs/capability-matrix.md`.
- `RockstreamConfig` supplies typed configuration defaults and TOML round-tripping.
- `docs/configuration.md` has a conformance test against those defaults.
- `CandidateIdentity` centralizes build and version identity.
- the CLI is one `clap`-based binary and already supports structured output in several paths.
- `SHOW VIEW STATUS` already exposes detailed lag and degradation information.
- `rockstream-types::error_code` provides stable `RS-XXXX` identifiers and next-step text.
- `GatewayError` maps several errors to PostgreSQL SQLSTATE values.
- `rockstream-sim::qualification` contains a qualification topology model, workload components, an oracle auditor, a recovery observer, and metrics collection.
- the workspace contains a dedicated `rockstream-test-support` crate, although it is currently small and mostly focused on Docker/PKI helpers.

The current baseline also has product-experience fragmentation that this plan must remove:

- README, roadmap, generated references, source comments, and historical plans can describe different moments in the project.
- generated documentation exists for selected surfaces, but there is no normalized manifest covering the whole public product surface.
- no dedicated, executable documentation information architecture makes the shortest path obvious to a new user.
- the qualification modules and numerous integration tests do not yet share one reusable, typed scenario framework.
- capability records link to named proofs, but the contract does not yet express required proof level, backend, public entry point, or lifecycle coverage.
- user-visible errors are split across registry constants, `thiserror` display strings, manually constructed pgwire errors, CLI errors, and ad hoc `String` errors.
- some generic gateway variants still have no `RS-XXXX` code in their display text.
- SQLSTATE, retryability, hint text, diagnostic context, and documentation links are not represented by one descriptor.

The implementation must consolidate these foundations. It must not create parallel registries that can drift.

---

## 4. Program goals

The program is complete when all of the following are true.

### 4.1 Documentation goals

- A new reader can determine what RockStream is, what it supports now, and what it does not support without reading historical roadmap documents.
- README is short enough to scan and contains a tested quickstart.
- All generated reference pages are produced from one normalized `ProductSurfaceManifest`.
- Public capability, CLI, configuration, SQL, function, catalog, metric, error, and limitation claims cannot drift independently.
- Historical plans remain accessible but are visually and structurally separated from current product documentation.
- Every command and SQL transcript in the primary getting-started path runs in automated tests.

### 4.2 Golden-path goals

- A new user can see a correct maintained view with one command through `rockstream demo`.
- A project-based local workflow requires no undocumented flags or internal setup.
- `rockstream init` can scaffold supported local, Kafka, and PostgreSQL CDC examples.
- A maintained Docker Compose environment demonstrates the supported external boundary.
- Every golden-path template is tested from a clean directory.
- Errors along the golden path point directly to corrective action.

### 4.3 High-level testing goals

- Every `Core` capability is connected to at least one public-path proof with an explicit proof level.
- Stateful `Core` capabilities have backfill, incremental update, checkpoint/restart, boundedness, and failure tests through the public path.
- Distributed and connector claims have multi-process or real-backend tests rather than only local models.
- Differential and metamorphic suites cover mixed inserts, updates, deletes, nulls, duplicates, empty inputs, and boundary values.
- High-level tests share one scenario format, one transcript representation, and one artifact format.
- Documentation and golden-path examples are ordinary test scenarios, not a separate unverified class of prose.

### 4.4 Error-experience goals

- Every user-visible failure is represented by one structured diagnostic descriptor.
- Every descriptor includes code, stable key, SQLSTATE where applicable, severity, retry class, summary, detail policy, hint, documentation slug, and redaction policy.
- pgwire uses PostgreSQL error fields instead of packing all information into one message string.
- CLI text and JSON errors share one stable schema.
- logs and support bundles include correlation IDs and safe structured context.
- metrics expose bounded error counts by registered code.
- no production path constructs a user-facing `[RS-XXXX]` string manually.
- users can look up any error through both documentation and the CLI.

---

## 5. Non-goals

The following are explicitly outside this program:

- a graphical administration console
- a hosted documentation service requirement
- a second CLI binary
- a new SQL client or replacement for `psql`
- a general-purpose project build system
- automatic destructive repair by `rockstream doctor`
- arbitrary plugin-defined error codes
- localized error-message translations
- exposing secret values or raw connector credentials in diagnostics
- using line coverage alone as proof of product behavior
- making test retries hide nondeterministic failures
- rewriting all architecture documents from scratch
- deleting historical evidence or sign-off files
- adding unsupported connectors to make examples look broader

---

## 6. Definitions

### 6.1 Public surface

A **public surface** is any behavior users or operators can depend on through:

- pgwire SQL
- the `rockstream` CLI
- configuration files or environment variables
- system catalog tables
- metrics
- documented connector behavior
- published error codes
- documented file formats or recovery commands

### 6.2 Normalized product-surface source

The phrase **generated from one source** does not mean manually duplicating every command, field, error, and SQL feature into one enormous TOML file.

It means:

1. Each subsystem exposes a typed canonical registry from its implementation.
2. A deterministic builder merges those registries and the hand-maintained product contract into one normalized `ProductSurfaceManifest`.
3. Every generated public reference consumes that manifest and no other handwritten feature list.

The normalized manifest is the one documentation source. Its contributors remain the canonical implementation sources.

### 6.3 High-level proof levels

| Level | Name | Minimum boundary exercised |
|---|---|---|
| `L0` | Unit/model | Pure function, operator, state machine, or formal model |
| `L1` | Component integration | Real component with storage or protocol adapter |
| `L2` | Public single-process | Real `rockstream` binary or gateway through CLI/pgwire |
| `L3` | Public multi-process | Multiple RockStream processes with real coordination |
| `L4` | External-system | Real Kafka, PostgreSQL, MinIO/S3-compatible storage, or external client driver |

A higher level does not replace lower-level tests. It adds evidence at the boundary where users experience the product.

### 6.4 Diagnostic

A **diagnostic** is a structured instance of a registered error or warning descriptor plus request-specific context. Diagnostic codes and keys are stable contracts. Human wording may improve without becoming a machine-parsing API.

---

## 7. Cross-cutting engineering rules

### 7.1 One source per concept

The implementation must preserve these ownership rules:

- `capabilities.toml` owns capability tier and semantic-proof requirements.
- the `clap` command tree owns CLI syntax.
- `RockstreamConfig` and its typed metadata own configuration fields.
- the scalar-function registry owns supported function signatures.
- the catalog schema registry owns public system catalog columns.
- the structured error catalog owns error descriptors.
- the metric descriptor registry owns public metrics.
- `ProductSurfaceManifest` is the normalized output used to generate documentation.

### 7.2 Determinism

All generators must be deterministic:

- stable sorting
- normalized newlines
- no current wall-clock time in generated references
- no host-specific paths
- no nondeterministic map iteration
- a schema version and content hash in generated artifacts

Running the generator twice on the same commit must produce byte-identical output.

### 7.3 Boundedness

- Documentation generators must cap captured command output and reject unbounded transcripts.
- Scenario histories must have configured maximum steps and artifact sizes.
- Diagnostic context must use a fixed allowlist of keys.
- Metrics may label by registered error code and subsystem, never arbitrary messages or correlation IDs.
- Golden-path data sets must remain small and deterministic.
- Test artifact retention must be bounded per scenario.

### 7.4 Security and redaction

The following must never appear in generated docs, error JSON, logs, support bundles, or test artifacts:

- secret values
- private keys
- authentication tokens
- full connection strings containing passwords
- raw SASL credentials
- unredacted environment variables classified as sensitive
- arbitrary row payloads from production paths unless explicitly enabled in a test fixture

### 7.5 Compatibility

- Existing `RS-XXXX` codes remain stable.
- Existing SQLSTATE mappings remain stable unless demonstrably incorrect; changes require a compatibility note and tests.
- New pgwire detail and hint fields are additive.
- Existing CLI text may improve, but JSON diagnostic schema is versioned.
- Existing documentation URLs receive redirect stubs when files move.
- Existing capability IDs remain stable.
- Generated README sections are delimited so the narrative can remain hand-edited.

### 7.6 No test-only product behavior

Golden-path and high-level tests must not rely on product behavior available only under `#[cfg(test)]`. Test drivers may inject faults, but normal DDL, DML, querying, status, recovery, and configuration must use the same public path as users.

---

## 8. Proposed roadmap

The companion small-feature plan occupies `v0.59.4` through `v0.59.7`. This program continues with six ordered roadmap versions.

| Version | Name | Primary result |
|---|---|---|
| **v0.59.8** | Single-Source Product Surface | One normalized manifest and deterministic documentation generator cover every public surface. |
| **v0.59.9** | Current, Simple, Executable Documentation | README and docs are reorganized around current users; all primary snippets and transcripts execute in CI. |
| **v0.59.10** | Golden Path Complete | `rockstream init`, local and Compose templates, reference workflows, and clean-start tests make evaluation straightforward. |
| **v0.59.11** | Structured Diagnostics Everywhere | One diagnostic contract powers pgwire, CLI, logs, metrics, support bundles, and error lookup. |
| **v0.59.12** | Public-Path Scenario and Differential Framework | A typed scenario runner, independent transcript oracle, capability proof levels, differential testing, and metamorphic testing are established. |
| **v0.59.13** | Lifecycle, Client, Documentation, and Backend Test Closure | Core lifecycle matrices, real clients, real backends, golden-path tests, documentation tests, and product-surface coverage close the pre-v1 program. |

The v1 tag should be scheduled after `v0.59.13`, not after `v0.59.7`, if the complete scope in this document is accepted.

---

# Part I — Single-source, current documentation

## 9. DOC-001 — Product surface manifest

### 9.1 Objective

Create a deterministic normalized representation of the public RockStream product surface.

### 9.2 Required type

Add a new test/tooling crate:

```text
crates/rockstream-docgen/
```

It must define, at minimum:

```rust
pub struct ProductSurfaceManifest {
    pub schema_version: u32,
    pub product: ProductIdentitySpec,
    pub capabilities: Vec<CapabilitySpec>,
    pub cli: CliSurfaceSpec,
    pub configuration: ConfigSurfaceSpec,
    pub sql: SqlSurfaceSpec,
    pub functions: Vec<FunctionSpec>,
    pub catalogs: Vec<CatalogTableSpec>,
    pub metrics: Vec<MetricSpec>,
    pub errors: Vec<ErrorDescriptorSpec>,
    pub connectors: Vec<ConnectorSpec>,
    pub limitations: Vec<LimitationSpec>,
    pub examples: Vec<ExampleSpec>,
    pub documentation: DocumentationNavigationSpec,
    pub source_fingerprint: String,
}
```

The manifest must serialize to deterministic JSON at:

```text
docs/generated/product-surface.json
```

### 9.3 Contributor architecture

Each implementation subsystem contributes typed data through a small stable API. Suggested APIs:

```rust
pub trait SurfaceContributor {
    fn contribute(&self, builder: &mut ProductSurfaceBuilder) -> Result<(), SurfaceError>;
}
```

Concrete contributors:

- `CapabilityContributor` reads and validates `capabilities.toml`.
- `CliContributor` reads the real `clap::Command` tree.
- `ConfigContributor` reads typed field metadata and defaults from `RockstreamConfig`.
- `SqlContributor` reads parser/dispatch capability descriptors.
- `FunctionContributor` reads the scalar-function registry.
- `CatalogContributor` reads the catalog schema registry.
- `MetricContributor` reads metric descriptors.
- `ErrorContributor` reads the structured error catalog created by the companion plan.
- `ConnectorContributor` reads supported connector guarantees.
- `StaticProductContributor` reads small hand-maintained metadata such as product summary, documentation navigation, example ordering, and limitation explanations.

### 9.4 CLI refactor required

The `clap` command definition currently lives primarily in `rockstream-cli/src/main.rs`. Refactor it so tooling can inspect the exact command tree without executing the binary:

```text
crates/rockstream-cli/src/command.rs
```

Expose:

```rust
pub fn command() -> clap::Command;
pub fn parse_from<I, T>(args: I) -> Result<Cli, clap::Error>;
```

`main.rs` becomes a thin execution adapter.

### 9.5 Configuration metadata

Serde alone does not preserve descriptions, sensitivity, environment variable names, or replacement/deprecation metadata. Add explicit field descriptors adjacent to the types:

```rust
pub struct ConfigFieldDescriptor {
    pub path: &'static str,
    pub value_type: ConfigValueType,
    pub default: ConfigDefault,
    pub description: &'static str,
    pub env_var: Option<&'static str>,
    pub cli_flag: Option<&'static str>,
    pub sensitive: bool,
    pub status: SurfaceStatus,
    pub replacement: Option<&'static str>,
}
```

The configuration resolver and documentation generator must consume the same descriptors.

### 9.6 Static product metadata

Add:

```text
product-surface.toml
```

This file must contain only information that does not already have an implementation registry, such as:

- product one-line promise
- supported deployment modes
- documentation navigation order
- example IDs and descriptions
- known limitation explanations
- strategic tier explanations
- current support policy text

It must not hand-list CLI commands, configuration fields, errors, functions, or catalog columns.

### 9.7 Validation rules

The builder fails when:

- duplicate public IDs exist
- a capability references a missing command, SQL surface, proof, or documentation anchor
- a `Core` capability has no required public proof level
- an error code lacks descriptor metadata
- a catalog column lacks a type or sensitivity classification
- a configuration field lacks a description
- a limitation references an unknown capability
- a documentation navigation entry points to a missing file
- generated IDs are unstable or unsorted

### 9.8 Commands

Add:

```bash
cargo run -p rockstream-docgen -- manifest
cargo run -p rockstream-docgen -- generate
cargo run -p rockstream-docgen -- check
```

`check` regenerates into a temporary directory and fails on any diff.

### 9.9 Tests

- manifest serialization is deterministic
- all contributors produce stable ordering
- duplicate IDs fail
- missing references fail
- sensitive fields are marked
- manifest schema version is present
- manifest hash changes when a public descriptor changes
- manifest hash does not change for private implementation-only changes
- checked-in generated file matches current source

### 9.10 Acceptance criteria

- Every generated public reference consumes only `ProductSurfaceManifest`.
- `scripts/generate-capability-matrix.py` is either replaced by or delegated to `rockstream-docgen`; it must not remain a separate rendering implementation.
- `docs/generated/product-surface.json` is deterministic and checked in.
- CI fails when generated docs drift.

---

## 10. DOC-002 — Documentation information architecture

### 10.1 Objective

Separate current user documentation from internals, project history, and planning material.

### 10.2 Required tree

Adopt this structure:

```text
docs/
  index.md
  getting-started/
    quickstart.md
    installation.md
    first-project.md
    docker-compose.md
  guides/
    direct-sql.md
    kafka-source.md
    postgres-cdc.md
    materialized-views.md
    subscriptions.md
    backfill-and-refresh.md
  operations/
    deployment.md
    configuration.md
    observability.md
    troubleshooting.md
    backup-and-restore.md
    rolling-upgrades.md
    security.md
  reference/
    cli.md
    sql.md
    functions.md
    configuration.md
    system-catalogs.md
    errors.md
    metrics.md
    capabilities.md
    connectors.md
    limitations.md
  internals/
    architecture.md
    ivm.md
    storage.md
    coordination.md
    formal-verification.md
  project/
    roadmap.md
    contributing.md
    design-decisions.md
  history/
    README.md
    plans/
    sign-offs/
    reviews/
  generated/
    product-surface.json
    transcripts/
```

Existing authoritative files such as `DESIGN.md`, `IVM.md`, and `NEW_ROADMAP.md` may remain at the repository root for compatibility, but the current documentation index must route readers through the new structure. Files that move require compatibility stubs or a checked redirect map.

### 10.3 Persona routing

`docs/index.md` must begin with four explicit entry points:

- evaluating RockStream
- building an application
- operating a cluster
- understanding or contributing to the engine

### 10.4 Current-state rule

Primary user and operator documentation must describe **current implemented behavior only**.

Planned behavior belongs in the roadmap. Historical behavior belongs in `docs/history/`. The reference pages may include a compact `Unsupported or Experimental` section generated from the manifest, but they must not interleave aspirational prose with current instructions.

### 10.5 README contract

Rewrite README with these sections and no more than approximately 300 lines:

1. one-sentence product promise
2. supported use cases
3. ten-minute quickstart
4. current supported boundary
5. one architecture diagram
6. reliability and correctness summary
7. links to documentation personas
8. status and support policy
9. contributing and license

The following README blocks should be generated between markers:

```html
<!-- BEGIN GENERATED STATUS -->
<!-- END GENERATED STATUS -->

<!-- BEGIN GENERATED QUICKSTART -->
<!-- END GENERATED QUICKSTART -->

<!-- BEGIN GENERATED CAPABILITIES -->
<!-- END GENERATED CAPABILITIES -->

<!-- BEGIN GENERATED LIMITATIONS -->
<!-- END GENERATED LIMITATIONS -->
```

### 10.6 Glossary and terminology

Create one generated `docs/reference/glossary.md`. Terms such as epoch, frontier, checkpoint, arrangement, backfill, source offset, committed result, workload, and spill must use the same wording throughout the docs.

Add a terminology linter that rejects known conflicting terms where feasible, for example:

- `pipeline` versus `view` when the public object is a view
- `watermark` versus `frontier` when the distinction matters
- `database` claims that imply general OLTP support
- removed connector names presented as supported

### 10.7 Documentation style rules

Add `docs/STYLE.md` with enforceable rules:

- lead with user outcome
- show exact commands
- state prerequisites
- state destructive effects
- mark local-only credentials
- avoid undocumented placeholders
- distinguish Core, Maintain, Experimental, and Unsupported
- prefer one canonical example per concept
- do not repeat generated feature lists manually
- use absolute object and command names
- include expected output only when generated from a test transcript

### 10.8 Acceptance criteria

- A reader can find the quickstart in one click from README.
- Current references contain no roadmap version narratives.
- Historical plans are clearly labeled and excluded from current support searches.
- Every old public doc path either remains or has a redirect stub.
- Documentation navigation is generated from the product manifest.

---

## 11. DOC-003 — Executable documentation

### 11.1 Objective

Make commands, SQL, configuration, and expected outputs in primary documentation mechanically verifiable.

### 11.2 Fenced-block metadata

Adopt metadata on executable blocks:

````markdown
```bash test=quickstart-local step=10
rockstream config validate --file rockstream.toml
```

```sql test=quickstart-local step=20 connection=pgwire
CREATE TABLE orders (...);
```

```text generated=quickstart-local:20
CREATE TABLE 0
```
````

The generator must reject duplicate scenario/step IDs and generated-output blocks with no producer.

### 11.3 Snippet runner

Add a `rockstream-docgen test` subcommand that:

1. extracts annotated snippets
2. builds a scenario specification
3. runs the snippets in an isolated temporary directory
4. captures normalized output
5. compares it to generated transcript blocks
6. writes mismatch artifacts
7. fails on unexecuted primary snippets

### 11.4 Normalization

Output normalization may remove only unstable data explicitly declared by the scenario:

- temporary paths
- allocated local ports
- timestamps
- generated correlation IDs
- build SHA abbreviations

It must not normalize away row values, error codes, SQLSTATE, command tags, result counts, frontier values, or failure status.

### 11.5 Link and anchor checking

`rockstream-docgen check` must verify:

- internal links
- relative file links
- generated anchors
- capability proof links
- error-code links
- configuration field anchors
- CLI command anchors
- no links from current docs into historical plans as normative instructions

### 11.6 Documentation test tiers

- `docs-fast`: parse, manifest, links, anchors, generated diff
- `docs-local`: embedded demo and local quickstart
- `docs-compose`: Kafka/PostgreSQL/MinIO Compose paths

### 11.7 Acceptance criteria

- Every command in README quickstart executes in `docs-local`.
- Every command in `getting-started/` either executes or is explicitly marked `illustrative` with a reason.
- Expected output is generated, not manually maintained.
- Docs tests use the same scenario runner introduced later in this plan; an initial adapter may be used until `v0.59.12` lands.

---

## 12. DOC-004 — Generated reference pages

Generate these pages entirely from `ProductSurfaceManifest` plus limited hand-authored introductions:

- CLI command reference
- configuration reference
- SQL capability reference
- scalar-function reference
- system catalog reference
- error reference
- metric reference
- connector guarantee matrix
- capability matrix
- known limitations
- glossary

Each generated entry must include source ownership and stability tier.

### 12.1 SQL reference requirements

For each statement or expression family, show:

- syntax
- tier
- supported type matrix
- null semantics
- incremental semantics
- state-growth bound where applicable
- failure behavior
- named proof(s)
- known limitations

### 12.2 CLI reference requirements

For each command, show:

- command path
- synopsis
- options
- defaults
- environment variables
- required role
- mutating or read-only classification
- output formats
- relevant errors
- examples

### 12.3 Error reference requirements

For each code, show:

- code
- stable key
- subsystem
- severity
- SQLSTATE
- retry class
- summary
- likely causes
- safe next steps
- diagnostic context keys
- related metrics
- related commands
- version introduced

### 12.4 Acceptance criteria

There is no second hand-maintained table of the same public surface. CI detects any duplicated table introduced in current docs where a generated block is required.

---

# Part II — Smooth golden path

## 13. GP-001 — Formal user journeys

The golden path consists of four supported journeys.

### 13.1 Journey A: zero-dependency demonstration

Prerequisite: the `rockstream` binary only.

```bash
rockstream demo
```

Outcome:

- starts the real embedded `role=all` path
- creates a table and materialized view through pgwire
- writes deterministic changes
- queries the view through pgwire
- displays the changing result
- exits cleanly unless `--keep-running` is supplied

This journey is defined in the companion plan and is a prerequisite here.

### 13.2 Journey B: first local project

Prerequisites: `rockstream` and optionally `psql`.

```bash
rockstream init sales-demo --template local
cd sales-demo
rockstream config validate
rockstream start --storage ./data
```

A second terminal runs the generated commands or `psql` script. The user sees a maintained view and `SHOW VIEW STATUS` without setting private session variables.

### 13.3 Journey C: Kafka evaluation

Prerequisites: Docker Compose.

```bash
rockstream init kafka-demo --template kafka
cd kafka-demo
./run.sh
```

Outcome:

- starts Redpanda/Kafka, MinIO, and RockStream
- creates the supported Kafka source
- produces fixture events
- maintains and queries a view
- verifies source status and committed offsets

### 13.4 Journey D: PostgreSQL CDC evaluation

Prerequisites: Docker Compose.

```bash
rockstream init cdc-demo --template postgres-cdc
cd cdc-demo
./run.sh
```

Outcome:

- starts PostgreSQL configured for logical replication, MinIO, and RockStream
- creates a source table and publication
- connects RockStream through the supported CDC path
- applies insert/update/delete changes
- verifies the maintained view
- shows source LSN and view frontier

### 13.5 Journey requirements

All four journeys must:

- use supported public interfaces
- have deterministic fixture data
- have bounded runtime and output
- clean up idempotently
- work twice in the same directory after cleanup
- include failure messages that point to `rockstream doctor`
- produce a machine-readable result summary

---

## 14. GP-002 — `rockstream init`

### 14.1 Objective

Create a supported project skeleton without asking the user to copy files from documentation manually.

### 14.2 CLI

```text
rockstream init [DIRECTORY]
  --template <local|kafka|postgres-cdc>
  --name <PROJECT_NAME>
  --force
  --output <text|json>
```

Defaults:

- directory: current directory
- template: `local`
- name: directory basename
- no overwrite without `--force`

### 14.3 Generated structure

```text
<project>/
  rockstream.toml
  README.md
  .gitignore
  .env.example
  sql/
    schema.sql
    views.sql
    seed.sql
    verify.sql
  scripts/
    start.sh
    verify.sh
    stop.sh
  fixtures/
    events.jsonl
  compose.yaml              # external templates only
  template.lock
```

`template.lock` contains:

```toml
schema_version = 1
template = "kafka"
rockstream_version = "0.59.x"
content_digest = "sha256:..."
```

### 14.4 Template source

Templates live in:

```text
crates/rockstream-cli/templates/
```

They are embedded at compile time and described in `ProductSurfaceManifest`.

### 14.5 Safety

- never overwrite an existing nonempty directory without `--force`
- show the exact files that would be overwritten
- generated `.env.example` contains placeholders, not secrets
- local credentials in Compose are clearly labeled local-only
- shell scripts use `set -euo pipefail`
- cleanup scripts remove only resources labeled with the generated project ID

### 14.6 Tests

- snapshot test for each template
- path traversal rejection
- overwrite rejection
- deterministic generation
- JSON output schema
- generated configuration passes `config validate`
- every generated SQL file parses
- generated README snippets execute

---

## 15. GP-003 — Local quickstart quality

### 15.1 Default behavior

A generated local project must not require the user to understand:

- control-plane addresses
- shard directories
- frontier roles
- idempotency settings
- internal TLS
- query-time shard topology
- operator IDs

The default local profile uses one process and one storage directory.

### 15.2 Startup output

On the first successful local start, print a concise readiness block:

```text
RockStream is ready.
PostgreSQL endpoint: postgresql://rockstream@127.0.0.1:5432/rockstream
Storage: ./data
Next:
  psql postgresql://rockstream@127.0.0.1:5432/rockstream -f sql/schema.sql
  psql postgresql://rockstream@127.0.0.1:5432/rockstream -f sql/views.sql
  rockstream view status
```

Structured output exposes the same fields.

### 15.3 Readiness

The local start path must distinguish:

- process started
- pgwire listener bound
- storage opened
- catalog ready
- first write accepted

The quickstart waits for product readiness rather than sleeping a fixed number of seconds.

### 15.4 Acceptance criteria

- a clean local journey reaches the first correct view result with at most ten user-entered commands
- no step requires editing generated files
- the project runs from paths containing spaces
- failed startup leaves no misleading readiness message
- `rockstream doctor` detects common local failures

---

## 16. GP-004 — Maintained Docker Compose environment

### 16.1 Location

```text
deploy/compose/quickstart/
```

### 16.2 Services

The complete profile contains:

- `rockstream`
- `minio`
- `minio-init`
- `redpanda`
- `postgres`
- optional `grafana`
- optional workload producer

Templates may enable only the services they need.

### 16.3 Requirements

- pinned image versions
- health checks for every service
- named volumes
- deterministic network aliases
- logical replication enabled for PostgreSQL
- MinIO bucket initialization
- local-only credentials in `.env.example`
- no host-network dependency
- Linux and macOS Docker Desktop compatibility
- bounded resource settings suitable for developer machines
- one cleanup command

### 16.4 Profiles

```bash
docker compose --profile local up
docker compose --profile kafka up
docker compose --profile postgres-cdc up
docker compose --profile observability up
```

### 16.5 Verification container

Add a short-lived `verify` service that exits nonzero unless:

- pgwire accepts a query
- the expected materialized-view rows exist
- source status is healthy
- MinIO is reachable
- the relevant source offset or LSN advanced

### 16.6 Acceptance criteria

- Compose does not report healthy before RockStream is query-ready
- `docker compose run --rm verify` is deterministic
- all profiles run in CI with Docker required
- cleanup leaves no unlabeled containers or volumes

---

## 17. GP-005 — Canonical examples and reference application

### 17.1 Canonical examples

Maintain exactly three primary examples matching supported ingestion modes:

```text
examples/local-sql/
examples/kafka-materialized-view/
examples/postgres-cdc-materialized-view/
```

Each example must contain:

- one clear business question
- schema
- view definition
- fixture changes including update and delete where supported
- verification query
- expected result generated from tests
- troubleshooting section linked to errors and doctor

### 17.2 Reference application

Provide one primary application example, preferably Python with `psycopg`, because it is concise and widely understandable:

```text
examples/reference-app-python/
```

The application must:

- connect through pgwire
- create or verify required objects
- perform writes
- query a materialized view
- reconnect
- use prepared statements
- handle at least one structured error
- expose no RockStream-private protocol

Other client libraries remain in the driver compatibility matrix but do not each need a full tutorial.

### 17.3 Example versioning

Examples declare the minimum RockStream version and capability IDs they require. The product manifest validates those IDs.

---

## 18. GP-006 — Golden-path observability and support

The golden path must teach only the primary operational controls:

- `rockstream doctor`
- `rockstream view status`
- `SHOW VIEW STATUS`
- `rockstream source show`
- `rockstream support bundle`
- relevant `RS-XXXX` lookup

Do not introduce internal operator debugging in the first tutorial. Link to advanced diagnostics after the first successful result.

### 18.1 Golden-path failure cases

Automated scenarios must cover:

- port already in use
- unwritable storage directory
- invalid configuration
- Docker unavailable
- Kafka not ready
- PostgreSQL replication not enabled
- MinIO credentials rejected
- source table missing
- unsupported SQL in view definition
- view not ready yet

Each case must produce one clear primary diagnosis and safe next steps.

---

# Part III — Excellent error messages

## 19. ERR-001 — Canonical diagnostic descriptor

### 19.1 Objective

Extend the structured error catalog from the companion plan into the canonical runtime diagnostic contract.

### 19.2 Descriptor

Define in `rockstream-types`:

```rust
pub struct ErrorDescriptor {
    pub code: ErrorCode,
    pub key: &'static str,
    pub subsystem: ErrorSubsystem,
    pub severity: DiagnosticSeverity,
    pub sqlstate: Option<&'static str>,
    pub retry_class: RetryClass,
    pub summary: &'static str,
    pub default_hint: &'static str,
    pub docs_slug: &'static str,
    pub introduced_in: &'static str,
    pub context_schema: &'static [ContextFieldDescriptor],
    pub redaction: RedactionPolicy,
}
```

Required enums:

```rust
pub enum RetryClass {
    Never,
    Immediate,
    Backoff,
    AfterUserAction,
    AfterOperatorAction,
}

pub enum DiagnosticSeverity {
    Info,
    Notice,
    Warning,
    Error,
    Fatal,
}
```

### 19.3 Stable key

Every code receives a stable dotted key such as:

```text
catalog.view_not_found
query.statement_timeout
write.shard_backpressure
auth.invalid_password
storage.object_store_unavailable
```

The key is a machine contract. Human message text is not.

### 19.4 SQLSTATE rules

- every pgwire-visible error has an explicit SQLSTATE
- no pgwire-visible error defaults silently to `XX000`
- internal defects may use `XX000`
- unsupported features use `0A000`
- parse/syntax errors use PostgreSQL-compatible classes
- resource limits use the relevant `53xxx` or `54xxx` class
- object names and transaction errors use compatible PostgreSQL classes where possible

A validator checks five-character format and approved mappings.

---

## 20. ERR-002 — Diagnostic instance

### 20.1 Type

```rust
pub struct Diagnostic {
    pub descriptor: &'static ErrorDescriptor,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub context: DiagnosticContext,
    pub correlation_id: DiagnosticId,
    pub source_chain: Vec<SafeSourceCause>,
}
```

### 20.2 Context

Context uses an allowlisted enum rather than arbitrary string keys:

```rust
pub enum DiagnosticContextKey {
    View,
    Source,
    Workload,
    Table,
    Column,
    Shard,
    Worker,
    Epoch,
    Frontier,
    Checkpoint,
    Limit,
    Actual,
    Operation,
    ConfigPath,
    Peer,
}
```

Values are typed and classified for redaction.

### 20.3 Correlation ID

- one correlation ID per CLI command or pgwire request
- propagated through tracing spans and internal adapters
- returned to users on errors
- included in support bundles and structured logs
- never used as a metric label
- random or monotonic uniqueness must not affect deterministic test snapshots; tests inject a fixed ID generator

### 20.4 Source chain

Internal source causes may be captured for debug logs and support bundles, but user output includes only sanitized causes explicitly allowed by the descriptor.

---

## 21. ERR-003 — Construction API

### 21.1 No manual strings

Introduce constructors or macros that require a registered descriptor:

```rust
Diagnostic::new(RS_2001)
    .message(format!("view `{name}` does not exist"))
    .context(View, name)
    .finish()
```

or:

```rust
diagnostic!(RS_2001,
    message = "view `{view}` does not exist",
    context = { View => view }
)
```

### 21.2 Type integration

Subsystem error enums may remain `thiserror` enums, but conversion to public output must produce `Diagnostic`, not parse the enum's `Display` string.

Recommended pattern:

```rust
pub trait IntoDiagnostic {
    fn into_diagnostic(self, request: &DiagnosticRequestContext) -> Diagnostic;
}
```

### 21.3 Migration targets

Migrate all of the following:

- `GatewayError`
- `CliError`
- `SqlError`
- `OpError`
- `StorageError`
- control-plane errors
- connector errors
- manually created `pgwire::ErrorInfo`
- HTTP/webhook errors that remain supported
- qualification/preflight errors

Generic variants such as `ViewNotFound`, `NotSupported`, `ParseError`, `Storage`, `Io`, and `PgWire` must either map to registered diagnostics or be made explicitly internal-only.

---

## 22. ERR-004 — Renderers

### 22.1 CLI text renderer

Human output format:

```text
RS-2001 catalog.view_not_found: view `sales_by_region` does not exist

Cause:
  The referenced view is not present in namespace `public`.

Next step:
  Run `rockstream view list` to inspect available views, or create the view first.

Correlation ID: 01J... 
More: docs/errors/RS-2001
```

Rules:

- first line stays concise
- detail and hint are separate
- no stack trace by default
- `--verbose` may include safe source causes
- noninteractive output does not use color unless requested

### 22.2 CLI JSON renderer

Create a versioned schema:

```json
{
  "schema_version": 1,
  "error": {
    "code": "RS-2001",
    "key": "catalog.view_not_found",
    "severity": "error",
    "sqlstate": "42P01",
    "retry_class": "after_user_action",
    "message": "view `sales_by_region` does not exist",
    "detail": "The referenced view is not present in namespace `public`.",
    "hint": "Run `rockstream view list` ...",
    "context": {
      "view": "sales_by_region",
      "namespace": "public"
    },
    "correlation_id": "01J...",
    "docs_slug": "errors/RS-2001"
  }
}
```

### 22.3 Pgwire renderer

Populate PostgreSQL protocol fields correctly:

- severity
- SQLSTATE
- primary message
- detail
- hint
- position when parser information exists
- schema/table/column fields when safe
- constraint name when applicable

Keep the RockStream code visible in the primary message or detail for compatibility, but do not concatenate hint and context into the message.

### 22.4 Log renderer

Structured logging fields:

```text
error.code
error.key
error.severity
error.retry_class
error.sqlstate
error.correlation_id
error.subsystem
```

Context fields are emitted only through the redaction policy.

### 22.5 Metrics renderer

Expose:

```text
rockstream_diagnostics_total{code="RS-2001",subsystem="catalog",severity="error"}
```

Label values come only from the bounded registry.

### 22.6 Support bundles

Support bundles include:

- correlation ID
- descriptor metadata
- safe context
- sanitized source-chain summaries
- surrounding trace events within a bounded window

---

## 23. ERR-005 — Notices and warnings

Use the same descriptor and rendering system for:

- degraded-but-continuing query notices
- staleness warnings
- index fallback notices
- source lag warnings
- configuration deprecations
- maintenance-tier feature notices where appropriate

A notice must not use an error code whose severity is registered as fatal or error.

---

## 24. ERR-006 — Error lookup surfaces

### 24.1 CLI

Add:

```text
rockstream errors list
rockstream errors show RS-2001
rockstream errors search <text>
```

Output is available in text and JSON.

### 24.2 SQL catalog

Add the read-only table:

```sql
SELECT * FROM rockstream_catalog.error_codes;
```

Columns:

- code
- key
- subsystem
- severity
- sqlstate
- retry_class
- summary
- hint
- docs_slug
- introduced_in

### 24.3 Documentation

Generate one anchor per code and optionally one file per code. Error lookup in a deployed binary must not depend on network access.

---

## 25. ERR-007 — Redaction and safety

### 25.1 Sensitive classifications

Context fields support:

- public
- identifier
- path
- network address
- secret
- row data
- internal

Default policy is deny. Secret values are always replaced with `<redacted>`.

### 25.2 Path handling

Production paths may be reduced to a basename or logical config key unless verbose local mode is explicitly requested.

### 25.3 Row payloads

Errors should identify row number, partition, offset, column, and schema without printing full row content by default.

### 25.4 Tests

- passwords never appear
- bearer tokens never appear
- private keys never appear
- connector options marked sensitive never appear
- JSON and text renderers apply identical redaction
- support bundles apply identical redaction

---

## 26. ERR-008 — Diagnostic enforcement

Add:

```text
scripts/check-diagnostics.py
scripts/check-diagnostics.sh
scripts/check-diagnostics.test.sh
```

The checker fails on:

- direct user-facing `[RS-` string literals outside generated code/tests
- direct `ErrorInfo::new` outside the canonical pgwire renderer
- literal `next_steps:` in production errors
- public error enum variants with no `IntoDiagnostic` mapping
- missing SQLSTATE for pgwire-visible codes
- missing retry class
- missing docs slug
- unregistered context keys
- duplicate codes or keys
- sensitive context rendered by an unsafe formatter
- metric labels derived from message text

Mutation self-tests deliberately introduce each class of violation.

### 26.1 Snapshot tests

Maintain golden snapshots for:

- CLI text
- CLI JSON
- pgwire fields
- notice response
- structured log record
- error-catalog row
- redaction

Snapshots use fixed correlation IDs and paths.

---

# Part IV — High-level test quality

## 27. TST-001 — Public-path scenario framework

### 27.1 Objective

Create one typed framework for user journeys, documentation examples, capability proofs, lifecycle sequences, fault tests, and external-client workflows.

### 27.2 Crates

Expand:

```text
crates/rockstream-test-support/
```

Add a test-only workspace crate:

```text
crates/rockstream-e2e/
```

No production crate may depend on `rockstream-e2e`.

### 27.3 Scenario specification

```rust
pub struct ScenarioSpec {
    pub id: String,
    pub description: String,
    pub capability_ids: Vec<String>,
    pub proof_level: ProofLevel,
    pub topology: TopologySpec,
    pub backend: BackendSpec,
    pub seed: u64,
    pub budgets: ScenarioBudgets,
    pub steps: Vec<ScenarioStep>,
    pub oracle: OracleSpec,
    pub artifacts: ArtifactPolicy,
}
```

Supported steps include:

```rust
pub enum ScenarioStep {
    StartCluster,
    StopCluster,
    Sql { connection: String, statement: String },
    SqlFile { connection: String, path: PathBuf },
    KafkaProduce { topic: String, records: Vec<TestRecord> },
    PostgresChange { transaction: Vec<PostgresMutation> },
    WaitForFrontier { view: String, at_least: u64 },
    WaitForState { object: String, state: String },
    Query { sql: String, capture: String },
    AssertRows { capture: String, expected: ExpectedRows },
    AssertOracle { capture: String },
    RestartNode { selector: NodeSelector },
    KillNode { selector: NodeSelector },
    PauseNode { selector: NodeSelector },
    NetworkPartition { from: NodeSelector, to: NodeSelector },
    ObjectStoreBrownout { duration: Duration },
    Checkpoint,
    ExportCheckpoint,
    RestoreFreshCluster,
    UpgradeNode { selector: NodeSelector, image: String },
    AssertDiagnostic { code: ErrorCode, fields: DiagnosticExpectation },
    RunCommand { command: Vec<String>, capture: String },
}
```

### 27.4 Scenario files

Support checked-in TOML scenario files under:

```text
crates/rockstream-e2e/scenarios/
```

The Rust types are authoritative. Generate and check a JSON schema for editor/tooling support.

### 27.5 Drivers

Define interfaces:

```rust
pub trait ClusterDriver;
pub trait SqlDriver;
pub trait KafkaDriver;
pub trait PostgresDriver;
pub trait ObjectStoreDriver;
pub trait FaultDriver;
pub trait ClientDriver;
```

Implement:

- embedded single-process driver
- real child-process driver
- Docker/TestContainers driver
- optional simulation adapter for paired scenarios

### 27.6 Readiness and cleanup

Drivers must:

- wait on real readiness conditions
- reserve ports safely
- label owned resources
- capture stdout/stderr
- terminate all child tasks
- remove owned containers and networks
- preserve artifacts on failure
- fail when required infrastructure is absent

---

## 28. TST-002 — Canonical typed transcript

### 28.1 Problem

String or TSV comparisons can hide type, null, ordering, and duplicate-row differences.

### 28.2 Type

```rust
pub struct QueryTranscript {
    pub columns: Vec<TranscriptColumn>,
    pub rows: Vec<TranscriptRow>,
    pub command_tag: Option<String>,
    pub notices: Vec<DiagnosticTranscript>,
    pub observed_frontier: Option<u64>,
}

pub enum TranscriptValue {
    Null,
    Bool(bool),
    Int(i128),
    Decimal { unscaled: i128, scale: u32 },
    FloatBits(u64),
    Text(String),
    Bytes(Vec<u8>),
    TimestampMicros(i64),
    DateDays(i32),
    Interval { months: i32, days: i32, micros: i64 },
    Array(Vec<TranscriptValue>),
}
```

### 28.3 Comparison modes

- ordered rows
- bag/multiset rows
- set rows
- approximate float with explicitly declared tolerance
- exact float bits

Default is exact typed comparison. Approximate comparison is never implicit.

### 28.4 Mismatch artifact

On mismatch, emit:

- schema diff
- missing rows
- extra rows
- differing values
- multiplicity differences
- frontier information
- source seed
- exact scenario step

---

## 29. TST-003 — Independent oracle strategy

### 29.1 Oracle modes

```rust
pub enum OracleSpec {
    PostgreSql,
    DataFusionBatch,
    RockstreamBatchOracle,
    Explicit(ExpectedRows),
    Composite(Vec<OracleSpec>),
}
```

### 29.2 Independence rules

- Public SQL semantic tests should prefer PostgreSQL when the supported subset has PostgreSQL-compatible behavior.
- Incremental algebra tests use `rockstream-oracle` and DataFusion batch.
- No scenario may create the expected result by reading the same RockStream storage path it is testing.
- The code that submits input must not also mark those inputs committed without observing RockStream.
- Composite oracles must agree before the expected transcript is accepted.

### 29.3 PostgreSQL adaptation

The oracle adapter may rewrite only explicitly documented RockStream syntax such as materialized-view lifecycle statements. Query bodies should run unchanged whenever possible.

### 29.4 Persistent regression seeds

Every differential mismatch stores:

- scenario ID
- seed
- generated schema
- generated SQL
- input mutation sequence
- expected transcript
- actual transcript

The minimized case is checked in before the defect is considered closed.

---

## 30. TST-004 — Capability proof levels

Extend `capabilities.toml` from a single proof string to a proof ledger:

```toml
[[capability.proof]]
test = "crates/rockstream-e2e/scenarios/query-read.toml"
level = "L2"
entry_point = "pgwire"
backend = "lfs"
behaviors = ["incremental", "backfill", "failure"]

[[capability.proof]]
test = "crates/rockstream-e2e/scenarios/query-read-restart.toml"
level = "L2"
entry_point = "pgwire"
backend = "minio"
behaviors = ["checkpoint_recovery", "state_growth"]
```

### 30.1 Minimum rules

- Every `Core` language capability: at least `L2` reachability and failure proof.
- Every stateful `Core` language capability: at least `L2` incremental, backfill, restart, boundedness, and failure proof.
- Every distributed `Core` capability: at least one `L3` proof.
- Every release-gated connector or sink: at least one `L4` proof with the real external system.
- CLI-only capabilities: real binary process test.
- Configuration surfaces: process startup proof using the resolved configuration.

### 30.2 Generated report

Generate:

```text
target/capability-proof-coverage.json
docs/generated/capability-proof-coverage.md
```

A missing required level fails CI.

---

## 31. TST-005 — Differential SQL suite

### 31.1 Input dimensions

Generate bounded combinations of:

- empty relations
- one row
- duplicate rows
- null values
- negative and zero values
- integer boundaries
- decimal scales
- text including Unicode and delimiters
- timestamps around bucket boundaries
- updates changing grouping or join keys
- deletes of present and absent rows
- out-of-order event time
- multi-row transactions

### 31.2 Query dimensions

For the supported subset:

- filters
- projections
- arithmetic
- casts
- `CASE`
- common scalar functions
- aggregates
- inner and supported outer/semi/anti joins
- distinct and set operations
- supported windows
- view-on-view DAGs
- subscriptions where a batch comparison is meaningful

### 31.3 Mutation sequence

A generated case must compare after each committed step, not only at the end:

1. initial backfill
2. insert batch
3. update batch
4. delete batch
5. restart when applicable
6. additional update after restart

### 31.4 Bounds

PR profile:

- fixed corpus
- bounded schemas and rows
- deterministic seeds

Nightly profile:

- larger seed count
- larger mutation sequences
- multiple backends

No unbounded random test runs.

---

## 32. TST-006 — Metamorphic SQL suite

Test logically equivalent forms, including:

- commuted conjunction predicates
- reordered projections with corresponding expected schema
- equivalent `IN` and disjunction forms
- join input reordering where semantics permit
- `UNION ALL` associativity
- filter pushdown equivalence
- `CAST` normalization
- equivalent `CASE` forms
- aggregate decomposition where exact semantics permit
- view expansion versus direct query

Metamorphic transformations must be type-aware and preserve null semantics. Each transformation records its proof precondition.

---

## 33. TST-007 — Lifecycle sequence matrix

### 33.1 Required sequences

- create table, write, create materialized view, backfill, write again
- create view while writes continue
- restart during backfill
- refresh while writes continue
- replace a view and verify dependent views
- spill state, checkpoint, restart, continue
- pause/resume source during backfill
- source disconnect and resume from committed offset/LSN
- create/drop/recreate object with prepared statements present
- concurrent DDL and DML where supported
- worker failure during update/delete flow
- control failover during view lifecycle transition
- checkpoint interruption
- fresh-cluster restore and continued ingestion
- mixed-version node replacement where applicable

### 33.2 Assertions

Every lifecycle scenario must assert:

- exact output
- object state transitions
- frontier monotonicity
- no duplicate committed deltas
- no lost committed deltas
- bounded resource indicators
- expected diagnostic on rejected operations
- successful progress after recovery

---

## 34. TST-008 — External client workflows

At minimum, retain or add complete workflows for:

- `psql`
- `tokio-postgres`
- Psycopg 3
- PgJDBC
- node-postgres
- SQLAlchemy

A workflow must do more than connect. It must:

- perform session startup
- use prepared statements
- create or inspect objects
- write rows
- query a materialized view
- consume a structured error
- reconnect
- verify transaction state after error

ORM-specific tests may limit DDL to what the ORM genuinely emits.

---

## 35. TST-009 — Documentation and golden-path tests

Every golden-path project template is represented as a scenario.

Required tests:

- `rockstream demo`
- local quickstart from clean directory
- local quickstart repeated after cleanup
- Kafka Compose template
- PostgreSQL CDC Compose template
- reference Python application
- README quickstart snippets
- configuration examples
- error lookup examples
- troubleshooting examples with deliberate failures

Documentation tests consume generated project templates rather than maintaining separate fixture copies.

---

## 36. TST-010 — Test execution policy

### 36.1 Profiles

Define explicit commands or nextest profiles:

```text
unit
component
public-local
public-docker
public-clients
differential-fast
differential-nightly
docs-fast
docs-local
docs-compose
qualification
```

### 36.2 No hidden skips

- required CI infrastructure absence is failure
- local developer skips are explicit and reported
- skipped count is emitted in a machine-readable summary
- release-gated profiles require zero skips

### 36.3 Retry policy

- automatic retries are disabled for correctness tests
- a flaky test is treated as a defect
- temporary quarantine requires an owner, issue, expiry date, and non-gating replacement signal
- quarantine may not satisfy a capability proof requirement

### 36.4 Artifacts

Every failed high-level scenario uploads:

- scenario spec
- seed
- candidate identity
- process/container inventory
- stdout/stderr
- normalized and raw transcripts
- diagnostics
- relevant metrics
- fault timeline
- cleanup report

### 36.5 Time budgets

Each scenario declares a maximum duration. A timeout produces a registered diagnostic and artifacts, not a generic killed process.

---

# Part V — Version-by-version implementation plan

## 37. v0.59.8 — Single-Source Product Surface

### 37.1 Scope

Implement DOC-001 and the generation foundation for DOC-004.

### 37.2 Vertical slices

#### Slice 1: `rockstream-docgen` and manifest schema

- add crate
- define manifest types
- deterministic JSON serialization
- schema version and fingerprint

#### Slice 2: CLI and config contributors

- refactor CLI command tree out of `main.rs`
- add config descriptors
- generate CLI/config sections

#### Slice 3: capabilities, SQL, functions, catalogs, errors, and metrics contributors

- adapt existing capability generator
- expose typed registries
- merge into manifest

#### Slice 4: generator and drift gate

- generate checked-in manifest
- add `generate`, `check`, and self-tests
- wire CI and Makefile

### 37.3 Required repository changes

```text
crates/rockstream-docgen/
crates/rockstream-cli/src/command.rs
product-surface.toml
docs/generated/product-surface.json
scripts/check-product-surface.sh
scripts/check-product-surface.test.sh
```

### 37.4 Proof commands

```bash
cargo test -p rockstream-docgen
cargo run -p rockstream-docgen -- manifest
cargo run -p rockstream-docgen -- check
bash scripts/check-product-surface.test.sh
cargo test -p rockstream-cli --test cli_command_contract_tests
cargo test -p rockstream-types --test product_surface_config_tests
```

### 37.5 Exit criteria

- one normalized manifest covers every public registry
- generator is deterministic
- old capability generator delegates to or is replaced by the new generator
- public registry drift fails CI

---

## 38. v0.59.9 — Current, Simple, Executable Documentation

### 38.1 Scope

Implement DOC-002, DOC-003, and complete DOC-004.

### 38.2 Vertical slices

#### Slice 1: information architecture and navigation

- create current docs tree
- add persona index
- add redirect map/stubs
- classify historical material

#### Slice 2: README and getting-started rewrite

- compact README
- generated status/capability/limitation blocks
- local tested quickstart

#### Slice 3: generated references

- CLI
- configuration
- SQL/functions
- catalogs
- metrics
- connectors
- errors
- limitations
- glossary

#### Slice 4: executable snippets and link checks

- fenced metadata parser
- snippet runner
- transcript generation
- link/anchor/terminology checks

### 38.3 Proof commands

```bash
cargo run -p rockstream-docgen -- generate
cargo run -p rockstream-docgen -- check
cargo run -p rockstream-docgen -- test --profile docs-fast
cargo run -p rockstream-docgen -- test --profile docs-local
cargo test -p rockstream-docgen --test link_tests
cargo test -p rockstream-docgen --test snippet_parser_tests
```

### 38.4 Exit criteria

- README quickstart executes
- all primary reference pages are generated
- current docs contain no unsupported claims not present in the manifest
- every moved public path has compatibility handling
- historical plans are clearly separated

---

## 39. v0.59.10 — Golden Path Complete

### 39.1 Scope

Implement GP-001 through GP-006, using companion-plan demo/doctor/config features.

### 39.2 Vertical slices

#### Slice 1: `rockstream init`

- template engine
- three templates
- deterministic generation
- safety and JSON output

#### Slice 2: local project path

- first-run readiness output
- generated SQL
- local verification script
- clean shutdown and rerun

#### Slice 3: Docker Compose paths

- local, Kafka, CDC, observability profiles
- health checks
- verifier service
- cleanup

#### Slice 4: examples and reference app

- three canonical examples
- Python reference app
- generated transcripts
- troubleshooting paths

#### Slice 5: golden-path CI

- clean-directory tests
- Compose tests
- deliberate failure tests
- artifact capture

### 39.3 Proof commands

```bash
cargo test -p rockstream-cli --test init_template_tests
cargo test -p rockstream-cli --test demo_tests
cargo test -p rockstream-cli --test golden_path_local_tests
cargo test -p rockstream-e2e --test golden_path_compose_tests --features docker_tests
bash scripts/test-quickstart.sh
bash scripts/test-examples.sh
```

### 39.4 Exit criteria

- every template validates and runs
- local and external journeys reach exact expected results
- no private setup step exists
- failure cases point to actionable diagnostics
- all examples are part of the tested documentation corpus

---

## 40. v0.59.11 — Structured Diagnostics Everywhere

### 40.1 Scope

Implement ERR-001 through ERR-008.

### 40.2 Vertical slices

#### Slice 1: descriptor and diagnostic types

- extend error catalog
- stable keys
- retry classes
- SQLSTATE validation
- context schema

#### Slice 2: renderers

- CLI text/JSON
- pgwire fields
- logs
- metrics
- support bundles

#### Slice 3: correlation and redaction

- request context
- tracing propagation
- redaction policy
- deterministic test IDs

#### Slice 4: subsystem migration

- gateway
- CLI
- SQL
- operators
- storage
- control
- connectors

#### Slice 5: lookup surfaces and enforcement

- CLI errors commands
- error catalog table
- generated docs
- diagnostic linter and mutation tests

### 40.3 Proof commands

```bash
cargo test -p rockstream-types --test diagnostic_catalog_tests
cargo test -p rockstream-gateway --test pgwire_diagnostic_tests
cargo test -p rockstream-cli --test cli_diagnostic_tests
cargo test -p rockstream-cli --test diagnostic_redaction_tests
cargo test -p rockstream-control --test diagnostic_propagation_tests
bash scripts/check-diagnostics.sh
bash scripts/check-diagnostics.test.sh
```

### 40.4 Exit criteria

- no direct user-facing diagnostic strings outside canonical renderers
- every public error has complete descriptor metadata
- pgwire detail/hint fields are tested
- text and JSON outputs share one diagnostic instance
- secrets are redacted in every renderer
- lookup surfaces contain every code

---

## 41. v0.59.12 — Public-Path Scenario and Differential Framework

### 41.1 Scope

Implement TST-001 through TST-006 and the proof-level extension of `capabilities.toml`.

### 41.2 Vertical slices

#### Slice 1: scenario types and local/process drivers

- scenario schema
- transcript model
- readiness/cleanup
- artifact format

#### Slice 2: Docker and external-system drivers

- Kafka
- PostgreSQL
- MinIO
- process fault driver

#### Slice 3: oracle adapters

- PostgreSQL
- DataFusion batch
- RockStream batch oracle
- composite agreement

#### Slice 4: capability proof ledger

- schema extension
- generated coverage report
- CI minimum-level gate

#### Slice 5: differential and metamorphic suites

- deterministic generators
- mutation sequences
- minimization
- regression corpus

### 41.3 Proof commands

```bash
cargo test -p rockstream-test-support
cargo test -p rockstream-e2e --test scenario_schema_tests
cargo test -p rockstream-e2e --test transcript_tests
cargo test -p rockstream-e2e --test capability_coverage_tests
cargo test -p rockstream-e2e --test differential_fast_tests
cargo test -p rockstream-e2e --test metamorphic_tests
bash scripts/check-capability-contract.sh
```

### 41.4 Exit criteria

- one scenario runner serves docs, examples, and product tests
- typed transcripts preserve null/type/multiplicity semantics
- every Core capability meets its minimum proof level
- differential failures produce reproducible checked artifacts
- no oracle derives expectations from RockStream's tested storage path

---

## 42. v0.59.13 — Lifecycle, Client, Documentation, and Backend Test Closure

### 42.1 Scope

Implement TST-007 through TST-010 and close the integrated program.

### 42.2 Vertical slices

#### Slice 1: lifecycle matrix

- backfill/write races
- restart/spill/checkpoint
- source lifecycle
- DDL/DML concurrency
- recovery and continued progress

#### Slice 2: external client workflows

- psql
- tokio-postgres
- Psycopg
- PgJDBC
- node-postgres
- SQLAlchemy

#### Slice 3: docs and golden-path integration

- all snippets through scenario runner
- all project templates through scenario runner
- deliberate troubleshooting failures

#### Slice 4: backend/fault matrix

- LFS
- MinIO
- Kafka
- PostgreSQL CDC
- multi-process control/worker paths

#### Slice 5: execution policy and final product-surface gate

- profiles
- zero hidden skips
- no retry masking
- artifact validation
- complete capability proof report
- complete docs/error/golden-path consistency check

### 42.3 Proof commands

```bash
cargo test -p rockstream-e2e --test lifecycle_matrix_tests
cargo test -p rockstream-e2e --test client_matrix_tests --features docker_tests
cargo test -p rockstream-e2e --test backend_matrix_tests --features docker_tests
cargo test -p rockstream-e2e --test docs_scenarios_tests
cargo test -p rockstream-e2e --test golden_path_scenarios_tests
cargo run -p rockstream-docgen -- check
bash scripts/check-diagnostics.sh
bash scripts/check-capability-contract.sh
bash scripts/check-dispatch-wiring.sh
```

### 42.4 Exit criteria

- every Core capability has required proof levels and behaviors
- every golden path is tested from a clean environment
- all current primary docs are executable or generated
- all public errors use structured diagnostics
- high-level suites report zero hidden skips
- failed scenarios produce complete reproducibility artifacts
- the public product surface, docs, tests, diagnostics, and examples agree

---

# Part VI — Pull request decomposition

## 43. Recommended PR sequence

The program should be delivered through reviewable pull requests rather than one long-lived branch.

### v0.59.8

1. Add `rockstream-docgen` manifest types and deterministic serializer.
2. Refactor CLI command tree for introspection.
3. Add configuration descriptors.
4. Add SQL/function/catalog/metric contributors.
5. Consolidate capability and error contributors.
6. Add generated-manifest drift gate.

### v0.59.9

7. Add docs navigation and redirect validator.
8. Rewrite README and getting-started docs.
9. Generate CLI/config/SQL/catalog/metric references.
10. Generate glossary/limitations/capability references.
11. Add executable snippet parser and local runner.
12. Migrate historical material and preserve links.

### v0.59.10

13. Add `rockstream init` core and local template.
14. Add Kafka template and Compose profile.
15. Add PostgreSQL CDC template and Compose profile.
16. Add verifier/cleanup services.
17. Add canonical examples and Python reference app.
18. Add golden-path tests and documentation transcripts.

### v0.59.11

19. Add diagnostic descriptor and instance types.
20. Add CLI and pgwire renderers.
21. Add correlation IDs and redaction.
22. Migrate gateway and SQL errors.
23. Migrate CLI/control/storage/operator/connector errors.
24. Add lookup surfaces, metrics, docs, and linter.

### v0.59.12

25. Expand `rockstream-test-support` with scenario and driver interfaces.
26. Add typed transcript and artifacts.
27. Add PostgreSQL/DataFusion/oracle adapters.
28. Extend capability proof ledger.
29. Add differential suite.
30. Add metamorphic suite and seed minimization.

### v0.59.13

31. Add lifecycle scenarios.
32. Add real-backend and fault matrix.
33. Add external client workflows.
34. Route docs and golden paths through scenario runner.
35. Add test profiles, zero-skip gate, and artifact validator.
36. Run final product-surface reconciliation and sign-off evidence.

Parallel PRs are acceptable when they do not duplicate ownership of the same registry.

---

## 44. Dependency graph

```text
Companion v0.59.4–v0.59.7
        |
        v
v0.59.8 ProductSurfaceManifest
        |
        +------------------+
        |                  |
        v                  v
v0.59.9 Docs          v0.59.11 Diagnostics
        |                  |
        v                  |
v0.59.10 Golden Path       |
        |                  |
        +---------+--------+
                  v
        v0.59.12 Scenario Framework
                  |
                  v
        v0.59.13 Test and Product Closure
```

`v0.59.11` may be developed in parallel with `v0.59.9` and `v0.59.10`, but it should merge before final high-level error assertions are frozen.

---

## 45. Effort sizing

This program is approximately six normal RockStream roadmap units.

| Version | Relative size | Main concentration |
|---|---:|---|
| v0.59.8 | 1.0 unit | Rust tooling, registries, generation |
| v0.59.9 | 1.0 unit | Documentation, doc tooling, current-state audit |
| v0.59.10 | 1.0 unit | CLI, templates, Compose, examples |
| v0.59.11 | 1.0–1.25 units | Cross-crate error migration |
| v0.59.12 | 1.0–1.25 units | Test framework, oracles, generators |
| v0.59.13 | 1.0–1.25 units | Lifecycle/client/backend scenario coverage |

The work is practical only with a strict freeze on unrelated feature expansion. The greatest schedule risk is cross-crate migration, not algorithmic novelty.

---

## 46. Risk register

### 46.1 Manifest becomes a duplicate source

**Risk:** contributors manually repeat implementation details.  
**Control:** contributors introspect typed registries; `product-surface.toml` is limited to metadata with no implementation owner.

### 46.2 Documentation rewrite breaks historical links

**Risk:** external links and sign-offs become invalid.  
**Control:** checked redirect map, compatibility stubs, link crawler, no destructive history deletion.

### 46.3 Golden path becomes a second deployment product

**Risk:** Compose templates diverge from real configuration.  
**Control:** templates use the same config resolver, generated references, binary, and readiness checks as production paths.

### 46.4 Scenario DSL becomes too general

**Risk:** a large testing framework consumes roadmap capacity.  
**Control:** only model actions required by accepted Core capabilities and golden paths; no arbitrary scripting language.

### 46.5 Oracle shares the same bug

**Risk:** DataFusion-based expected results mirror frontend defects.  
**Control:** PostgreSQL oracle for compatible SQL, composite oracles, explicit expected rows for special surfaces, and no reading tested RockStream state as expectation.

### 46.6 Error migration changes compatibility

**Risk:** SQLSTATE or text changes surprise clients.  
**Control:** stable code/key contract, snapshot tests, additive pgwire fields, compatibility ledger for any SQLSTATE correction.

### 46.7 Correlation IDs leak cardinality into metrics

**Risk:** unbounded metric label cardinality.  
**Control:** correlation IDs only in logs, output, and support bundles; never metric labels.

### 46.8 High-level tests become slow and flaky

**Risk:** developers avoid running them.  
**Control:** explicit profiles, deterministic seeds, readiness instead of sleeps, no retries, strict time budgets, failure artifacts, PR-fast subset.

### 46.9 Generated docs become unreadable

**Risk:** exhaustive references overwhelm users.  
**Control:** generated reference pages are comprehensive; hand-authored guides remain outcome-focused and link to detail.

---

## 47. Program-wide definition of done

Every roadmap version must satisfy the existing common definition of done plus the following program rules.

### 47.1 Documentation

- generated output is clean
- current docs use current product status
- no duplicate hand-maintained public surface tables
- links and anchors pass
- primary snippets execute
- limitations are explicit

### 47.2 Golden path

- clean environment test passes
- repeated run passes after cleanup
- readiness is observed, not slept
- no private flags
- no secret leakage
- verification result is exact

### 47.3 Diagnostics

- code/key/SQLSTATE/retry class present
- detail and hint separated
- correlation ID present
- context redacted
- text/JSON/pgwire snapshots pass
- descriptor appears in generated docs and catalog

### 47.4 High-level tests

- scenario declares capability IDs and proof level
- public path is used
- oracle is independent
- typed transcript is compared
- time and artifact bounds exist
- infrastructure absence fails when required
- no hidden skip or retry satisfies a proof

### 47.5 Contract reconciliation

Any public surface change updates or regenerates:

- `ProductSurfaceManifest`
- generated docs
- `capabilities.toml`
- proof coverage
- dispatch wiring where applicable
- diagnostics where applicable
- golden-path examples where applicable

---

## 48. Proposed roadmap rows

The following condensed rows can be adapted directly into `NEW_ROADMAP.md`.

| Version | Focus | Scope | Proof |
|---|---|---|---|
| `v0.59.8` | Single-Source Product Surface | Add `rockstream-docgen`, normalized `ProductSurfaceManifest`, CLI/config/function/catalog/metric/error contributors, deterministic generated JSON, and drift gates. | Manifest is deterministic; all public IDs resolve; generated output is clean; mutations to any public registry fail the drift gate. |
| `v0.59.9` | Current, Simple, Executable Documentation | Reorganize docs by user persona, shorten README, separate history, generate references, add executable snippets/transcripts, and validate links and terminology. | README and getting-started paths execute; generated references match manifest; old public links resolve; no current doc claims unsupported behavior. |
| `v0.59.10` | Golden Path Complete | Add `rockstream init`, local/Kafka/PostgreSQL-CDC templates, maintained Compose profiles, verifier and cleanup services, canonical examples, and a reference application. | Every template runs from a clean directory, produces exact maintained-view results, fails clearly under common setup errors, cleans up, and runs again. |
| `v0.59.11` | Structured Diagnostics Everywhere | Create canonical descriptor/diagnostic types, stable keys, retry classes, SQLSTATE validation, correlation IDs, redaction, renderers, lookup surfaces, and migrate every public error path. | No manual user-facing error strings; pgwire/CLI/JSON/log/support outputs agree; every code is documented; redaction mutation tests pass. |
| `v0.59.12` | Public-Path Scenario and Differential Framework | Add typed scenario DSL, process/Docker drivers, typed transcripts, independent oracles, capability proof levels, differential and metamorphic suites, and reproducibility artifacts. | Every Core capability meets minimum proof level; differential corpus passes; injected mismatches produce minimized reproducible artifacts. |
| `v0.59.13` | Lifecycle, Client, Documentation, and Backend Test Closure | Add lifecycle race/restart/recovery matrix, external client workflows, real Kafka/PostgreSQL/MinIO paths, route docs/golden paths through scenarios, and enforce zero hidden skips. | Core behavior ledger is complete across required levels/backends; all golden paths and current docs run; all public surfaces agree; no required high-level test is skipped or retried. |

---

## 49. Final recommendation

Accepting this program means deliberately extending the pre-v1 roadmap beyond the current small-feature polish sequence.

That is justified because these four initiatives improve the reliability of every existing feature rather than adding another product pillar:

- one current description of the product
- one obvious path to first value
- one reusable framework for public behavior proofs
- one excellent diagnostic contract

The implementation rule should be:

> **Do not add breadth while this program is open. Make the existing RockStream product easy to understand, easy to start, easy to verify, and easy to debug.**

When `v0.59.13` is complete, RockStream will not merely have more documentation and tests. It will have a coherent product contract connecting implementation, documentation, examples, diagnostics, and public-path evidence.

# RockStream

RockStream is a cloud-native incremental view maintenance engine. It ingests
changing data, updates durable materialized views, and serves committed results
to PostgreSQL-compatible clients.

## Start here

| If you are... | Start with |
| --- | --- |
| Evaluating RockStream | [Getting started](docs/getting-started.md) |
| Operating RockStream | [Operator guide](docs/operator.md) |
| Contributing code | [Contributor guide](docs/contributors.md) |
| Maintaining the project | [Documentation index](docs/README.md) |

The [getting-started guide](docs/getting-started.md) includes the checked-in
demo and local-project transcripts.

## Try it

RockStream currently builds with Rust 1.88. From a clone of this repository:

```bash
cargo build -p rockstream-cli
./target/debug/rockstream demo
```

The demo creates an `orders` table and a materialized sales view, then proves
that inserts, updates, and deletes produce the expected incremental result. To
create a persistent local project instead:

```bash
./target/debug/rockstream init my-project --template local
cd my-project
../target/debug/rockstream start --storage ./storage
```

The generated project contains its own verification and cleanup scripts. See
[Getting started](docs/getting-started.md) for the complete transcript and the
Kafka and PostgreSQL CDC templates.

> RockStream ingests changing data, continuously maintains a documented SQL subset as durable materialized views, and serves globally committed results to PostgreSQL-compatible clients while surviving ordinary distributed-system failures without losing or silently corrupting committed state.

---

## What it does

Traditional materialized views often recompute a query over its full input.
RockStream compiles supported SQL into an incremental dataflow instead. Each
insert, update, or delete becomes a delta, and operators apply that delta only
to affected state.

```text
Kafka, PostgreSQL CDC, or SQL writes
      |
      v
      incremental operators
      |
      v
     SlateDB-backed arrangements and views
      |
      v
   PostgreSQL wire protocol clients
```

State can live on a local filesystem for evaluation or in object storage for a
cluster. Clients use PostgreSQL drivers and tools; RockStream remains a
streaming SQL system, not a general-purpose OLTP replacement.

## Why RockStream

### Incremental SQL

RockStream maintains supported queries as data changes instead of rerunning the
full query. A basic materialized view looks like this:

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

Clients can write with `INSERT`, `UPDATE`, and `DELETE`, read tables and views,
or use `SUBSCRIBE` for a snapshot followed by committed changes.

### Durable state

[SlateDB](https://slatedb.io/) backs arrangements, materialized views, and
checkpoints. A local process can use filesystem storage; clustered deployments
can place durable state in object storage. Frontiers and epoch commits let a
worker resume from committed state after a failure.

### PostgreSQL clients

The gateway speaks the PostgreSQL wire protocol, including the extended query
flow used by common drivers. You can connect with `psql`, BI tools, or an ORM:

```bash
psql -h 127.0.0.1 -p 5432 -U app -d analytics \
  -c "SELECT * FROM sales_by_store;"
```

RockStream also supports inline `CREATE VIEW` definitions. The compiler expands
these reusable query fragments without materializing separate operator state.
See the [pgwire conformance matrix](docs/pgwire-conformance.md) for tested
clients and protocol behavior.

### Reproducible failure testing

The deterministic simulator replaces time, storage, and networking so crash,
reordering, and partial-failure scenarios can be replayed from a seed. FizzBee
models cover the coordination protocols. Completed milestones link to their
tests, artifacts, and sign-offs instead of relying on prose claims.

## Capability contract

RockStream labels every public capability by its compatibility commitment. The
[generated capability matrix](docs/capability-matrix.md) is authoritative.

| Tier | Meaning | Current examples |
| --- | --- | --- |
| Core | Release-gated with named proof tests | Reads, scalar expressions, DML, transaction semantics, views and view DAGs, `SUBSCRIBE`, freshness controls, Kafka source, PostgreSQL CDC source, Kafka sink |
| Maintain | Supported and regression-tested, but not a growth area | Secondary indexes, workload quotas, resource visibility, query diagnostics, system catalog SQL |
| Experimental | Available subsets with no continuity guarantee | Aggregates, joins and set operations, window and time operators |

### Important boundaries

- RockStream is in v1 release qualification and is scoped to single-region
  deployments. It is not a multi-region consensus system.
- The SQL frontend implements a documented subset. `ORDER BY` and `LIMIT`,
  recursive CTEs, `LATERAL`, and several uncommon expressions remain gaps.
- Aggregate, join, and window support has type-specific restrictions. In
  particular, floating-point keys and some text-key combinations are rejected.
- User-defined merge laws and user-visible CRDT column types are post-v1 work.
- The v1 connector set is Kafka source, PostgreSQL CDC source, and Kafka sink.
  Iceberg, Delta, object-store sink, S3 source, and webhook source frontends are
  removed and fail closed.

Read [language features](docs/language-features.md), the
[SQL support reference](docs/reference/sql-support.md), and
[known limitations](docs/known-limitations.md) before evaluating a production
workload. Data-lake exports should use the Kafka sink with a downstream writer;
see [connector migration](docs/connector-migration.md).

## Inspiration

RockStream is inspired by production systems and implementation research:

| System | What it does |
|---|---|
| **[DBSP](https://github.com/vmware-research/database-stream-processor)** | Mathematical theory that guarantees incremental results are always identical to what a full re-computation would produce |
| **[pg_trickle](https://github.com/trickle-labs/pg-trickle)** | Shows how to turn SQL views into practical per-operator delta rules, with many hard correctness cases worked through in PostgreSQL |
| **[SlateDB](https://slatedb.io/)** | Provides the cloud-native object-storage-backed LSM that RockStream uses as its durable shard and arrangement store |

RockStream brings these ideas to an open, cloud-native storage foundation.

## Key concepts

| Term | Meaning |
|---|---|
| **Materialized view** | A saved, pre-computed answer to a query |
| **Delta / change** | Only the new or removed records since the last update |
| **Epoch** | A small batch of changes processed together, like a transaction |
| **Worker** | A process that handles one slice of the data |
| **Frontier** | A marker that says no future change before this point is expected |
| **Checkpoint** | A saved snapshot of progress used for recovery |
| **CDC** | Change data capture, which streams source database changes into RockStream |
| **Inline view** | A reusable query fragment expanded at compile time without separate IVM state |
| **Merge law** | A named algebraic rule that permits safe combination of partial results |

## Project status

The current workspace version is **v0.59.15**. Engineering work toward the v1
release qualification is active; RockStream does not yet claim a `v1.0.0`
release. The checked-in [capability matrix](docs/capability-matrix.md),
[evidence manifest](docs/evidence-manifest.json), and [version sign-offs](sign-offs/)
record what has shipped and how each milestone was verified.

| Read this | For |
| --- | --- |
| [Architecture](ARCHITECTURE.md) | A short map of runtime components and dependencies |
| [Design](DESIGN.md) | Storage layout, coordination, fault tolerance, and deployment details |
| [IVM internals](IVM.md) | Differentiation, operators, circuits, and the correctness oracle |
| [Roadmap](NEW_ROADMAP.md) | Version history, remaining qualification work, and proof obligations |
| [Project focus](ROCKSTREAM_PROJECT_FOCUS.md) | Core scope, maintained features, and admission rules for new work |
| [Release governance](docs/release-governance.md) | Candidate identity, evidence, and release gates |

Run `rockstream version --json` to inspect the binary version and candidate
identity.

## Workspace map

The project is a Cargo workspace of purpose-built crates:

| Crate | Purpose |
|---|---|
| `rockstream-types` | Shared types: timestamps, frontiers, Z-set rows, schemas |
| `rockstream-storage` | SlateDB wrappers, key encoders, merge operator registry, checkpoint helpers |
| `rockstream-plan` | `PlanNode` IR and physical `OpNode` graph |
| `rockstream-diff` | `DiffCtx` differentiation pass that turns SQL plans into incremental delta plans |
| `rockstream-ops` | `Operator` trait and per-operator implementations |
| `rockstream-sql` | SQL frontend built on DataFusion |
| `rockstream-runtime` | Worker process, circuit executor, async scheduler, exchange subsystem |
| `rockstream-control` | Control-plane service (topology, shard leasing, placement) |
| `rockstream-gateway` | Postgres wire protocol gateway |
| `rockstream-connectors` | Connector implementations; removed connector frontends remain compiled for catalog compatibility. |
| `rockstream-oracle` | Batch reference engine and property-test harness (DBSP soundness tests) |
| `rockstream-sim` | Deterministic simulation harness: `SimRuntime`, `buggify!()`, fault model |
| `rockstream-cli` | Single operator binary for nodes, views, sources, schemas, workloads, shards, checkpoints, diagnostics, and release qualification |
| `rockstream-docgen` | Generated CLI, configuration, SQL, catalog, metric, and error-code references |

## Run and operate

There is one `rockstream` binary. Node roles are flags, not separate
executables.

```bash
# Local evaluation or a single host
rockstream start --role all --storage ./data

# A cluster uses the same binary with explicit roles
rockstream start --role control --storage ./control-data
rockstream start --role worker --control http://control:8000 --storage ./worker-data
```

Configuration, object-storage setup, authentication, and multi-node examples
live in the [configuration reference](docs/reference/configuration.md) and
[operator guide](docs/operator.md).

Useful diagnostics include:

```bash
rockstream doctor --storage ./data
rockstream cluster status
rockstream explain sales_by_store --estimate
rockstream resource usage
rockstream audit tail
rockstream support bundle
```

The [CLI reference](docs/reference/cli.md) documents every argument and exit
code. Prometheus metrics and the checked-in Grafana dashboard are documented in
[metrics](docs/metrics.md); recovery and upgrade procedures live in
[disaster recovery](docs/disaster-recovery.md) and
[rolling upgrades](docs/rolling-upgrades.md).

## Development

The common checks are:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`make check` runs the complete local CI suite, including contract and formal
verification checks. See [test commands](docs/test-commands.md) for focused and
release-qualification commands.

## Contributing

Read the [contributor guide](docs/contributors.md) for repository conventions,
tests, dependency policy, and pull request expectations.

## License

Apache 2.0

# Cold-Tier Sinks — Iceberg & Delta Lake

RockStream can sink a materialized view's finalized rows to an object-store-
backed lakehouse table in either Apache Iceberg or Delta Lake format, so
external query engines (Spark, Trino, DuckDB, Athena, etc.) can read
RockStream's incrementally-maintained output without going through pgwire.
This document covers the full `CREATE SINK ... TO ICEBERG|DELTA` option
surface, partitioning, catalog registration, retention/GC, and current
implementation caveats.

## Syntax

```sql
CREATE SINK <sink_name>
  FOR VIEW <view_name>
  TO ICEBERG | DELTA '<path>'
  WITH (
    snapshot_interval_epochs = <int>,      -- flush every N committed epochs
    snapshot_interval_ms     = <int>,      -- flush every N milliseconds
    parquet_row_group_bytes  = <int>,      -- target Parquet row-group size
    format_version            = <int>,     -- Iceberg format-version hint
    partition_by               = ARRAY['col', ...],  -- see "Partitioning" below
    catalog                     = filesystem | glue | rest | hive | ducklake
  );
```

Example:

```sql
CREATE SINK orders_cold
  FOR VIEW orders_view
  TO ICEBERG 's3://warehouse/orders'
  WITH (
    snapshot_interval_epochs = 128,
    parquet_row_group_bytes  = 134217728,
    partition_by             = ARRAY['region'],
    catalog                  = filesystem
  );
```

`FOR VIEW` must reference an existing view (`CREATE VIEW`/`CREATE MATERIALIZED
VIEW`); `CREATE SINK` against an unknown view returns `RS-4007`. All `WITH`
options are optional except `catalog`, which must be one of `filesystem`,
`glue`, `rest`, `hive`, or `ducklake` — anything else also returns `RS-4007`.

## Partitioning

`partition_by` accepts a list of either bare column names or
`date_trunc('unit', column)` expressions, where `unit` is one of `year`,
`month`, `day`, or `hour`:

```sql
WITH (partition_by = ARRAY['region', 'date_trunc(''day'', event_time)'])
```

Each committed epoch's output batch is split into one data file per distinct
partition-key tuple, written under a `field=value/...` directory layout (e.g.
`region=eu/day=2026-01-01/...`), matching the Hive-style partition path
convention both Iceberg and Delta Lake tooling expect. Rows with the same
`partition_by` tuple always land in the same file for a given epoch; readers
scanning by partition therefore only need to open the relevant subdirectory.
Omitting `partition_by` (or leaving it empty) preserves the original
unpartitioned single-file-per-epoch layout.

## Catalog registration

Cold-tier sinks always write valid Iceberg/Delta metadata and data files
directly to the configured `path`, regardless of `catalog`. The `catalog`
option additionally controls whether — and how — each successful snapshot
commit is registered with an external table catalog so query engines can
discover the table by name instead of by raw path:

| `catalog`     | Behavior |
|---------------|----------|
| `filesystem`  | No external registration; the table is only reachable by path. Always succeeds. |
| `rest`        | Registers via a minimal Iceberg/Delta REST catalog `POST /v1/tables/<name>` call. |
| `glue`        | Registers via a pluggable transport standing in for AWS Glue Data Catalog. |
| `hive`        | Registers via a pluggable transport standing in for the Hive Metastore Thrift API. |
| `ducklake`    | Appends a line-delimited JSON record to a local, self-contained catalog file (no external service), matching DuckLake's own "no separate metastore service" design. |

**Failure isolation**: a catalog-registration failure (unreachable REST
endpoint, Glue/Hive transport error, unwritable DuckLake catalog path) never
fails the sink's commit or blocks incremental view maintenance. Instead it
sets the sink's state to `CATALOG_WARN`, which is visible via `EXPLAIN` (see
below), and is retried automatically on the sink's next successful flush.
This mirrors DESIGN.md §13.6.5's failure-isolation rule: the cold tier is a
best-effort export, and a catalog outage must never stall the primary
streaming path.

## `EXPLAIN` sink-target annotation

`EXPLAIN <query>` over a view with a registered sink includes a
`sink_target:` line naming the sink, its format, path, last committed
snapshot epoch, and catalog-registration state:

```
Plan: SeqScan → partial_pushdown: false
sink_target: name='orders_cold' format=ICEBERG path='s3://warehouse/orders' last_snapshot_epoch=512 state=OK
Query: SELECT * FROM orders_view
```

`state` is `OK` normally, or `CATALOG_WARN` if the last catalog-registration
attempt failed (see above). A view with no registered sink has no
`sink_target:` line. This annotation is intentionally lightweight — it does
not attempt to wire the full `EXPLAIN INCREMENTAL` plan library into sink
metadata; that remains later-roadmap scope.

## Cold-snapshot garbage collection

Cold snapshots accumulate over time; without GC, object-store costs grow
unboundedly. After each successful snapshot commit, a sink evaluates
retention against two independent bounds — expiry happens on whichever bound
is reached first:

- **`cold_snapshot_retention_count`** — keep at most N snapshots (default
  **32**).
- **`cold_snapshot_retention_duration`** — keep snapshots for at most this
  long (default **7 days**).

At the default `snapshot_interval_epochs`/`snapshot_interval_ms`, 32
snapshots covers roughly a few hours of history; adjust based on how far
behind external readers may lag.

GC never expires the single newest snapshot, regardless of count/duration, so
a sink always has at least one readable snapshot. For each expired snapshot,
GC scan-and-deletes (never a range delete) any data file it references that
is **not** also referenced by a still-retained snapshot, then rewrites the
sink's metadata to drop the expired snapshot entries, and emits
`cold_gc_bytes_reclaimed` / `cold_gc_last_run_epoch` metrics.

**Safety guarantees**:

- **Idempotent.** GC durably stages its delete list before deleting anything
  or rewriting metadata; if the process crashes mid-delete, the next GC run
  resumes the same delete list and finishes it. Deleting an already-gone file
  is treated as a no-op (0 bytes reclaimed), so replaying the same delete list
  twice never deletes anything extra or errors.
- **Never deletes a shared file.** A data file referenced by any retained
  snapshot is never deleted, even if an expired snapshot also references it.
- **Never races a commit.** GC acquires the same lock the flush/commit path
  holds around the sink's catalog state, so a GC pass structurally cannot
  interleave with a snapshot commit on the same sink.
- External readers mid-scan of an expired snapshot may see 404s on deleted
  data files — an accepted tradeoff. Raise `cold_snapshot_retention_count`
  if external readers need a longer window.

## Implementation notes and caveats (current status)

- **Fallback wire format.** The `iceberg` and `deltalake` crates declared as
  workspace dependencies have API/version mismatches with RockStream's
  `object_store`-based fault-injecting store layer, so today's Iceberg and
  Delta sinks write a RockStream-owned JSON metadata format
  (`metadata.json` for Iceberg, `_delta_log/*.json` for Delta) rather than
  the wire-exact Iceberg manifest/Avro or Delta Lake transaction-log
  encodings external engines expect byte-for-byte. Both sinks still produce
  real, valid Parquet data files with Hive-style partition paths; only the
  metadata/log encoding is RockStream's own JSON rather than the upstream
  binary/Avro format.
- **`glue`/`hive` catalog backends** register through a pluggable transport
  rather than the real AWS Glue SDK or Hive Thrift client — no live
  Glue/Hive service is exercised in CI, and no Proof claim requires
  round-tripping against a real Glue/Hive service, only the DDL surface and
  the shared failure-isolation/retry contract every backend implements.
- **`ducklake`** uses a local append-only JSON catalog file rather than the
  `ducklake` crate (an immature, low-adoption crate at the time of writing),
  again satisfying the same "no external service" catalog character DuckLake
  itself documents.
- **No automatic sink wiring from DDL to the commit loop yet.** `CREATE SINK`
  registers a catalog entry describing the sink's configuration; actually
  constructing an `IcebergSink`/`DeltaSink` and driving it from a view's
  epoch-commit loop is not yet wired into the gateway (this matches the
  gateway's existing precedent — no other `SinkConnector` implementation is
  wired into the commit loop today either). `IcebergSink`/`DeltaSink` are
  fully functional, tested `SinkConnector` implementations usable directly
  by connector code; end-to-end DDL-to-object-store wiring is later-roadmap
  scope.

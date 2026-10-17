# v0.72 eight-workload independent-result oracle (structural verifier)

The `verify-v072-oracles` tool computes a complete **independent SQLite
result multiset** from each scenario's full source/dimension/change corpus,
and compares it with the observed result rows. It rejects missing or duplicate
scenarios, changed or extra rows, wrong duplicate multiplicities, stale epoch
numbers, missing files, unrecognized transports, and paths that escape the
manifest directory. It pins the exact frozen profile digest before evaluating
anything. The SQL is selected from the eight immutable workload kinds below;
manifest authors cannot submit a different query to lower the oracle bar.

**This tool does not execute RockStream, PostgreSQL CDC, Kafka, or a worker
exchange.** A transport label or committed epoch in the input is self-reported
and never authenticates actual process ownership, durable commits or frontier.
Only complete release-process measurements with real transport and independent
frontier provenance can qualify #132 v2:R11/R12. The successful output status
is always `independent_multiset_oracle_only_not_capacity_qualified`.

## Inputs and CLI

```sh
rtk cargo run --manifest-path tools/r1-local-harness/Cargo.toml \
  --bin verify-v072-oracles --locked -- \
  --profile benchmarks/r1-local/profile-v072.toml \
  --manifest /path/to/observed-run/oracle-manifest.json \
  --output /path/to/observed-run/oracle-check.json
```

Every manifest must have `schema_version=1`, `profile_sha256` equal to the
frozen profile hash
`d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a`,
and **exactly one** of each of the eight `runs` objects. Each run has:

```json
{
  "workload": "filter_projection",
  "input_path": "filter_projection/input-corpus.json",
  "observed_rows_path": "filter_projection/complete-result-rows.json",
  "reported_transport": "pg_wire",
  "committed_epoch": 42,
  "observed_epoch": 42
}
```

Both paths must be regular files located below the manifest's parent directory
(with no absolute, dot-relative or symlink-escaped paths). The full input
corpus has the repository `Corpus` JSON shape (source rows, dimension rows,
complete insert/update/delete changes with exact `before` and `after` rows).
The observed result file must be the **entire** materialized result multiset as
JSON `[["column1", "column2"], ...]` (not count-only or a sample). Each
scenario must have its own result file; input files may be shared if they
really describe the same full input sequence. The tool fails nonzero without
writing a success report if any oracle mismatches.

Required `workload` values and `reported_transport` values:

| Workload | Declared transport | Independent SQLite semantics |
| --- | --- | --- |
| `filter_projection` | `pg_wire` | Active-row ID/value projection |
| `group_aggregate` | `pg_wire` | Per-group count and signed sum |
| `high_cardinality_aggregate` | `pg_wire` | Per-ID count and sum |
| `two_way_join` | `pg_wire` | Source-to-dimension inner join |
| `window` | `pg_wire` | Partitioned deterministic `ROW_NUMBER` |
| `postgres_cdc` | `postgres_cdc` | Filtered CDC source-state result |
| `kafka_ingestion` | `kafka` | Per-group aggregate of active Kafka-ingested state |
| `distributed_exchange` | `exchange` | Bucket-aggregated source/dimension join |

See `src/v072_oracles.rs` for the exact immutable SQLite queries. The
`reported_transport` field is a **claim under test**, not evidence it ran.

## Required follow-up to qualify the milestone

A release run must independently supply eight actual public-interface source
feeds, source and output digest bindings, correct durable read tokens, real
worker/process identities, queue/CPU/RSS samples, no hidden data truncation,
and complete raw measurements for all applicable section 5.2 matrix cells.
`verify-v072-run-bundle`, the strict matrix validator, and the process-capture
checks are separate preflight stages. This oracle alone cannot prove their
input provenance, source ordering, or benchmark performance.

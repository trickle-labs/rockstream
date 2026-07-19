# RockStream Configuration Reference

This document provides a comprehensive reference for the system-wide configuration knobs migrated to `rockstream.toml`.

This table is generated from — and locked to — `RockstreamConfig` (and its
sub-structs `ClusterConfig`, `AutotunerConfig`, `WorkerConfig`,
`ConnectorConfig`, `StorageConfig`/`StorageTieringConfig`, and the optional
`PricingConfig`) in `crates/rockstream-types/src/config.rs`,
`crates/rockstream-types/src/tiering.rs`, and
`crates/rockstream-types/src/cost.rs`. A test,
`crates/rockstream-types/tests/docs_conformance_tests.rs::
test_configuration_doc_matches_rockstream_config_defaults`, parses every
`(key, default value)` pair below and diffs it against
`RockstreamConfig::default()`'s real field values at test time — it fails if
this document and the struct's defaults ever drift apart.

## Configuration File Structure

The `rockstream.toml` configuration file contains settings for the cluster,
worker, connector, and storage-tiering parameters, plus an optional
cost-estimation pricing table.

```toml
# rockstream.toml example

recursion_max_iterations = 1024

[cluster]
min_epoch_ms = 10
checkpoint_retention_count = 128
state_budget_gb = 10
index_prefer_selectivity_threshold = 0.01
index_max_lag_ms = 1000

[cluster.autotuner]
enabled = true
hysteresis_scale_up_windows = 3
hysteresis_scale_down_windows = 12
default_parallelism = 4
min_parallelism = 1
max_parallelism = 32

[worker]
segment_cache_bytes = 536870912 # 512 MiB
max_rows_per_quantum = 1000

[connector]
dlq_warn_threshold = 100
dlq_retention_days = 7

# [storage.tiering] and [pricing] are both optional; omitted entirely, the
# node runs with tiering disabled and no cost-estimation pricing table.
```

## Knobs & Parameters Reference

### Root-level Settings

- **`recursion_max_iterations`** (integer, default: `1024`): Safety cap on recursive fixed-point iterations per epoch before the compiler/runtime fails the epoch conservatively.

### `[cluster]` Section

- **`min_epoch_ms`** (integer, default: `10`): The minimum duration of an epoch window in milliseconds. Epoch coordinator sizes epoch windows dynamically based on SLO metrics, but will not flush faster than this limit.
- **`checkpoint_retention_count`** (integer, default: `128`): The maximum number of historical checkpoints to retain in storage before GC deletes old files.
- **`state_budget_gb`** (integer, default: `10`): Total state memory limit in gigabytes across all active pipelines. Surpassing the threshold emits `RS-3604` (`OVER_BUDGET_RELAXED`) warning. Lives under `[cluster]`, not a separate `[memory]` section — `RockstreamConfig` has no `[memory]` table.
- **`index_prefer_selectivity_threshold`** (float, default: `0.01`): Selectivity threshold below which the planner prefers an available secondary index over a full scan.
- **`index_max_lag_ms`** (integer, default: `1000`): Maximum acceptable staleness, in milliseconds, of a secondary index before the planner falls back to a full scan.

There is no `checkpoint_retention_duration_sec` key — `ClusterConfig` has no
corresponding field; retention is governed solely by
`checkpoint_retention_count`.

#### `[cluster.autotuner]` Sub-section

- **`enabled`** (bool, default: `true`): Whether the workload autotuner is active.
- **`hysteresis_scale_up_windows`** (integer, default: `3`): Consecutive over-threshold windows required before the autotuner scales parallelism up.
- **`hysteresis_scale_down_windows`** (integer, default: `12`): Consecutive under-threshold windows required before the autotuner scales parallelism down (4x the scale-up window count, to avoid flapping).
- **`default_parallelism`** (integer, default: `4`): Initial parallelism assigned to a new pipeline before the autotuner has collected any windows.
- **`min_parallelism`** (integer, default: `1`): Floor the autotuner will never scale a pipeline's parallelism below.
- **`max_parallelism`** (integer, default: `32`): Ceiling the autotuner will never scale a pipeline's parallelism above.

### `[worker]` Section

- **`segment_cache_bytes`** (integer, default: `536870912`): Total memory allocated for the arrangement segment cache on each worker node (512 MiB). LRU eviction is used when limit is reached.
- **`max_rows_per_quantum`** (integer, default: `1000`): The maximum number of rows processed in a single cooperative scheduler quantum before yielding.

### `[connector]` Section

- **`dlq_warn_threshold`** (integer, default: `100`): The hourly rate threshold of dead-letter-queue inserts before triggering proactive `RS-1004` warnings.
- **`dlq_retention_days`** (integer, default: `7`): The number of days dead-letter-queue entries are retained before being eligible for GC.

### `[storage.tiering]` Section (optional)

Omitted by default (`StorageConfig::default()`'s `tiering` field is
`StorageTieringConfig::default()`, all three sub-fields `None`) — cold-tier
storage backends are disabled unless explicitly configured.

- **`shard_meta_backend`** (string, default: `None`): Object-store backend class for shard metadata (e.g. `"s3express"`). Unset disables the backend override.
- **`cold_sst_backend`** (string, default: `None`): Object-store backend class for cold SSTs (e.g. `"standard-ia"`). Unset disables cold-tiering.
- **`cold_sst_age_threshold`** (integer, default: `None`): Age, in seconds, after which an SST becomes eligible for the cold backend. Unset disables age-based tiering.

### `[pricing]` Section (optional)

Omitted by default (`RockstreamConfig::default()`'s `pricing` field is
`None`) — cost-estimation diagnostics are disabled unless explicitly
configured. When present, every sub-field below is required:

- `object_store_request_per_1k` (float): Cost per 1,000 object-store requests.
- `object_store_standard_gb_month` (float): Cost per GB-month of standard-tier object storage.
- `object_store_standard_ia_gb_month` (float, optional): Cost per GB-month of infrequent-access-tier object storage.
- `object_store_egress_gb` (float): Cost per GB of object-store egress.
- `compute_on_demand_core_hour` (float): On-demand compute cost per core-hour.
- `compute_spot_core_hour` (float, optional): Spot compute cost per core-hour.
- `compute_spot_mix` (float, optional): Fraction (0.0–1.0) of compute assumed to run on spot instances for blended cost estimates.

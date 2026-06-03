# RockStream Configuration Reference

This document provides a comprehensive reference for the system-wide configuration knobs migrated to `rockstream.toml`.

## Configuration File Structure

The `rockstream.toml` configuration file contains settings for the coordinator, worker, memory, and engine parameters.

```toml
# rockstream.toml example

[cluster]
min_epoch_ms = 100
checkpoint_retention_count = 3
checkpoint_retention_duration_sec = 3600

[worker]
segment_cache_bytes = 1073741824 # 1 GB
max_rows_per_quantum = 10000

[memory]
state_budget_gb = 8

[connector]
dlq_warn_threshold = 100
```

## Knobs & Parameters Reference

### `[cluster]` Section

- **`min_epoch_ms`** (integer, default: `100`): The minimum duration of an epoch window in milliseconds. Epoch coordinator sizes epoch windows dynamically based on SLO metrics, but will not flush faster than this limit.
- **`checkpoint_retention_count`** (integer, default: `3`): The maximum number of historical checkpoints to retain in storage before GC deletes old files.
- **`checkpoint_retention_duration_sec`** (integer, default: `3600`): The duration in seconds to keep checkpoints in storage.

### `[worker]` Section

- **`segment_cache_bytes`** (integer, default: `1073741824`): Total memory allocated for the arrangement segment cache on each worker node. LRU eviction is used when limit is reached.
- **`max_rows_per_quantum`** (integer, default: `10000`): The maximum number of rows processed in a single cooperative scheduler quantum before yielding.

### `[memory]` Section

- **`state_budget_gb`** (integer, default: `8`): Total state memory limit in gigabytes across all active pipelines. Surpassing the threshold emits `RS-3604` (`OVER_BUDGET_RELAXED`) warning.

### `[connector]` Section

- **`dlq_warn_threshold`** (integer, default: `100`): The hourly rate threshold of dead-letter-queue inserts before triggering proactive `RS-1004` warnings.

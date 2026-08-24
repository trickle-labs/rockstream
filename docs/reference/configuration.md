# Configuration reference

| Key | Type | Default | Environment | Deprecated | Source | Description |
| --- | --- | --- | --- | --- | --- | --- |
| autotuner.compression_disable_hysteresis_windows | usize | 2 | ROCKSTREAM_AUTOTUNER_COMPRESSION_DISABLE_HYSTERESIS_WINDOWS | false | rockstream-types::config::AutotunerConfig | Windows of high CPU before disabling direct compression |
| autotuner.compression_reenable_hysteresis_windows | usize | 4 | ROCKSTREAM_AUTOTUNER_COMPRESSION_REENABLE_HYSTERESIS_WINDOWS | false | rockstream-types::config::AutotunerConfig | Windows of low CPU before re-enabling direct compression |
| autotuner.default_parallelism | usize | 4 | ROCKSTREAM_AUTOTUNER_DEFAULT_PARALLELISM | false | rockstream-types::config::AutotunerConfig | Default operator execution parallelism |
| autotuner.direct_compression_cpu_budget_ms | u64 | 5 | ROCKSTREAM_AUTOTUNER_DIRECT_COMPRESSION_CPU_BUDGET_MS | false | rockstream-types::config::AutotunerConfig | CPU time budget in milliseconds allocated for direct compression |
| autotuner.enabled | bool | true | ROCKSTREAM_AUTOTUNER_ENABLED | false | rockstream-types::config::AutotunerConfig | Enable dynamic pipeline autotuning |
| autotuner.hysteresis_scale_down_windows | usize | 12 | ROCKSTREAM_AUTOTUNER_HYSTERESIS_SCALE_DOWN_WINDOWS | false | rockstream-types::config::AutotunerConfig | Number of consecutive underloaded windows before scale-down |
| autotuner.hysteresis_scale_up_windows | usize | 3 | ROCKSTREAM_AUTOTUNER_HYSTERESIS_SCALE_UP_WINDOWS | false | rockstream-types::config::AutotunerConfig | Number of consecutive overloaded windows before scale-up |
| autotuner.max_parallelism | usize | 32 | ROCKSTREAM_AUTOTUNER_MAX_PARALLELISM | false | rockstream-types::config::AutotunerConfig | Maximum allowed operator parallelism |
| autotuner.min_parallelism | usize | 1 | ROCKSTREAM_AUTOTUNER_MIN_PARALLELISM | false | rockstream-types::config::AutotunerConfig | Minimum allowed operator parallelism |
| scatter_pruning.shard_bloom_budget_bytes | usize | 65536 | ROCKSTREAM_SCATTER_PRUNING_SHARD_BLOOM_BUDGET_BYTES | false | rockstream-types::config::ScatterPruningConfig | Memory budget in bytes allocated for shard bloom filters |
| scatter_pruning.shard_stats_max_age_checkpoints | u64 | 5 | ROCKSTREAM_SCATTER_PRUNING_SHARD_STATS_MAX_AGE_CHECKPOINTS | false | rockstream-types::config::ScatterPruningConfig | Maximum checkpoint age for shard key statistics |
| server.listen_addr | String | 127.0.0.1:5432 | ROCKSTREAM_LISTEN_ADDR | false | rockstream-runtime::server | PostgreSQL wire protocol server listen address |
| server.max_connections | usize | 1024 | ROCKSTREAM_MAX_CONNECTIONS | false | rockstream-runtime::server | Maximum concurrent pgwire client connections |
| skew_split.enabled | bool | true | ROCKSTREAM_SKEW_SPLIT_ENABLED | false | rockstream-types::config::SkewSplitConfig | Enable automatic hot-key skew splitting |
| skew_split.hot_key_factor | f64 | 20.0 | ROCKSTREAM_SKEW_SPLIT_HOT_KEY_FACTOR | false | rockstream-types::config::SkewSplitConfig | Skew threshold multiplier relative to mean key frequency |
| skew_split.max_skew_buckets | u16 | 16 | ROCKSTREAM_SKEW_SPLIT_MAX_SKEW_BUCKETS | false | rockstream-types::config::SkewSplitConfig | Maximum number of sub-buckets per skewed key |
| storage.base_dir | String | ./data | ROCKSTREAM_STORAGE_DIR | false | rockstream-storage | Local filesystem storage directory root |
| storage.object_store_url | Option<String> | None | ROCKSTREAM_OBJECT_STORE_URL | false | rockstream-storage | Object store bucket URL (e.g. s3://bucket or minio://bucket) |


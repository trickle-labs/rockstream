# RockStream Metrics Reference
 
RockStream exports OpenMetrics / Prometheus metrics over HTTP via the `--metrics-addr` endpoint or gateway `/metrics` handler.
 
## Freshness Lag & Attributable Stage Lag Metrics
 
All lag metrics are measured in **milliseconds (`ms`)**, are strictly non-negative (`u64`), and satisfy the empirical published summation tolerance ($\le 5\text{ms}$).
 
| Metric | Type | Unit | Description |
|---|---|---|---|
| `view_freshness_lag_source_ms` | Gauge | ms | Source watermark/event lag (source event time to ingestion arrival). |
| `view_freshness_lag_decode_ms` | Gauge | ms | Record decoding, deserialization, and ingest buffer time. |
| `view_freshness_lag_compute_ms` | Gauge | ms | IVM operator delta evaluation / differential compute execution time. |
| `view_freshness_lag_checkpoint_alignment_ms` | Gauge | ms | Checkpoint barrier alignment wait time across shards. |
| `view_freshness_lag_sink_commit_ms` | Gauge | ms | Transactional sink staging and batch commit latency. |
| `view_freshness_lag_spill_ms` | Gauge | ms | Spill activity / storage paging delay. |
| `view_freshness_lag_storage_pressure_ms` | Gauge | ms | Object store and L0 compaction backpressure delay. |
| `view_freshness_lag_end_to_end_ms` | Gauge | ms | Total end-to-end freshness lag ($\sum \text{stage\_lags} \pm 5\text{ms}$). |
 
## Checkpoint & Barrier Flight Time Metrics
 
| Metric | Type | Unit | Description |
|---|---|---|---|
| `checkpoint_barrier_flight_time_ms` | Gauge | ms | Elapsed time from coordinator barrier injection until all shard operators receive the barrier. |
| `checkpoint_completion_time_ms` | Gauge | ms | Total elapsed time from barrier injection until all shard SlateDB checkpoints confirm and the manifest commits. |
| `checkpoint_alignment_buffer_credits_used` | Gauge | count | Number of in-flight alignment credits currently held by the checkpoint coordinator. |
 
## Merge Law & Storage Metrics
 
| Metric | Type | Description |
|---|---|---|
| `merge_law_applied_total` | Counter | Total number of merge law evaluations on state merges. |
| `merge_law_fallback_total` | Counter | Total fallback reads copying raw bytes. |
| `merge_law_rmw_avoided_total` | Counter | Total hot-path read-modify-write operations avoided via abelian merge laws. |
| `merge_law_rmw_required_total` | Counter | Total read-modify-write operations required. |
| `manifest_write_total` | Counter | Total epoch-level manifest commits. |

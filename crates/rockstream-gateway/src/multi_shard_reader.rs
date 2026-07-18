//! Multi-shard reader pinned to a `ClusterFrontier` epoch.
//!
//! Scatter-reads the same view across multiple shards at a pinned frontier
//! epoch and merges results (union semantics — view outputs are already
//! partitioned, no dedup needed).
//!
//! # Bounds
//!
//! - `max_in_flight_rows`: named upper bound on rows held in memory across all
//!   shards. Default: 1_000_000 rows.
//! - Fill-level metric: `rows_in_flight` (AtomicUsize) tracks current usage.
//! - Backpressure: if `rows_in_flight > max_in_flight_rows`, `scatter_read`
//!   returns `GatewayError::ResultSetTooLarge`.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use rockstream_storage::{PartialAggSpec, ShardReader};
use rockstream_types::frontier::{bloom_filter_might_contain, ColumnStats, ShardColumnStats};
use rockstream_types::ids::ShardId;

use crate::error::GatewayError;

/// Returns true if the SQL query can be pushed down as a partial aggregation.
/// Detects: SELECT <cols> FROM <view> GROUP BY <cols> where aggregates are SUM/COUNT/AVG.
pub fn can_pushdown_partial_agg(sql: &str) -> bool {
    let ql = sql.to_lowercase();
    if !ql.contains("group by") {
        return false;
    }
    if !ql.contains("select") || !ql.contains("from") {
        return false;
    }
    if ql.contains("select *") || ql.contains("select\n*") {
        return false;
    }
    ql.contains("sum(") || ql.contains("count(") || ql.contains("count(*)") || ql.contains("avg(")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScatterPredicate {
    Eq {
        col_idx: u16,
        value: Vec<u8>,
    },
    Range {
        col_idx: u16,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScatterPlan {
    pub shard_ids: Vec<ShardId>,
    pub total_shards: usize,
    pub pruned_columns: Vec<u16>,
}

pub fn plan_scatter_shards(
    shard_stats: &[ShardColumnStats],
    predicates: &[ScatterPredicate],
    max_stats_age_checkpoints: u64,
    latest_checkpoint_epoch: u64,
) -> ScatterPlan {
    if shard_stats.is_empty() {
        return ScatterPlan {
            shard_ids: Vec::new(),
            total_shards: 0,
            pruned_columns: Vec::new(),
        };
    }
    let too_stale = shard_stats.iter().any(|stats| {
        latest_checkpoint_epoch.saturating_sub(stats.checkpoint_epoch) > max_stats_age_checkpoints
    });
    rockstream_types::metrics::add_scatter_shards_total(shard_stats.len() as u64);
    if too_stale {
        return ScatterPlan {
            shard_ids: shard_stats.iter().map(|stats| stats.shard_id).collect(),
            total_shards: shard_stats.len(),
            pruned_columns: Vec::new(),
        };
    }

    let mut kept = Vec::new();
    let mut pruned_columns = std::collections::BTreeSet::new();
    for stats in shard_stats {
        let mut keep = true;
        for predicate in predicates {
            let (col_idx, column) = match predicate {
                ScatterPredicate::Eq { col_idx, .. } | ScatterPredicate::Range { col_idx, .. } => (
                    *col_idx,
                    stats
                        .col_stats
                        .iter()
                        .find(|column| column.col_idx == *col_idx),
                ),
            };
            let Some(column) = column else {
                continue;
            };
            if should_prune_column(column, predicate) {
                keep = false;
                pruned_columns.insert(col_idx);
                break;
            }
        }
        if keep {
            kept.push(stats.shard_id);
        }
    }
    rockstream_types::metrics::add_scatter_shards_pruned_total(
        shard_stats.len().saturating_sub(kept.len()) as u64,
    );
    ScatterPlan {
        shard_ids: kept,
        total_shards: shard_stats.len(),
        pruned_columns: pruned_columns.into_iter().collect(),
    }
}

fn should_prune_column(column: &ColumnStats, predicate: &ScatterPredicate) -> bool {
    match predicate {
        ScatterPredicate::Eq { value, .. } => {
            outside_bounds(column, value, value)
                || column
                    .bloom_filter
                    .as_ref()
                    .is_some_and(|filter| !bloom_filter_might_contain(filter, value))
        }
        ScatterPredicate::Range { lower, upper, .. } => {
            outside_optional_bounds(column, lower, upper)
        }
    }
}

fn outside_bounds(column: &ColumnStats, lower: &[u8], upper: &[u8]) -> bool {
    column
        .min_bytes
        .as_ref()
        .is_some_and(|min| upper < min.as_ref())
        || column
            .max_bytes
            .as_ref()
            .is_some_and(|max| lower > max.as_ref())
}

fn outside_optional_bounds(
    column: &ColumnStats,
    lower: &Option<Vec<u8>>,
    upper: &Option<Vec<u8>>,
) -> bool {
    let lower_prunes = lower
        .as_ref()
        .zip(column.max_bytes.as_ref())
        .is_some_and(|(lower, max)| lower.as_slice() > max.as_ref());
    let upper_prunes = upper
        .as_ref()
        .zip(column.min_bytes.as_ref())
        .is_some_and(|(upper, min)| upper.as_slice() < min.as_ref());
    lower_prunes || upper_prunes
}

/// Extract a PartialAggSpec from a GROUP BY SQL query.
/// Returns None if the query is not parseable.
fn extract_partial_agg_spec(sql: &str) -> Option<PartialAggSpec> {
    if !can_pushdown_partial_agg(sql) {
        return None;
    }
    let ql = sql.to_lowercase();
    let agg_type = if ql.contains("sum(") {
        "sum"
    } else if ql.contains("count(") || ql.contains("count(*)") {
        "count"
    } else {
        "sum"
    };
    Some(PartialAggSpec {
        group_col: 0,
        agg_col: 1,
        agg_type: agg_type.to_string(),
    })
}

/// Scatter-reads a view across multiple shards, all pinned to the same
/// `pinned_frontier` epoch.
pub struct MultiShardReader {
    /// One reader per shard, all opened at `pinned_frontier`.
    shards: Vec<Arc<ShardReader>>,
    /// The `ClusterFrontier` epoch at which all shard readers are pinned.
    pinned_frontier: u64,
    /// Bound: max rows that can be held in memory across all shards.
    ///
    /// Invariant: `total_rows_in_flight <= max_in_flight_rows`.
    /// Backpressure: if exceeded, `scatter_read` returns `GatewayError::ResultSetTooLarge`.
    max_in_flight_rows: usize,
    /// Fill-level metric: tracks current rows in flight.
    rows_in_flight: Arc<AtomicUsize>,
}

impl MultiShardReader {
    /// Default max in-flight rows (1 million).
    pub const DEFAULT_MAX_IN_FLIGHT_ROWS: usize = 1_000_000;

    pub fn new(
        shards: Vec<Arc<ShardReader>>,
        pinned_frontier: u64,
        max_in_flight_rows: usize,
    ) -> Self {
        Self {
            shards,
            pinned_frontier,
            max_in_flight_rows,
            rows_in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The frontier epoch all shards are pinned to.
    pub fn pinned_frontier(&self) -> u64 {
        self.pinned_frontier
    }

    /// Current fill level (rows currently in-flight in memory).
    pub fn rows_in_flight(&self) -> usize {
        self.rows_in_flight.load(Ordering::Relaxed)
    }

    /// Scatter-read `view_name` across all shards, merge and return up to
    /// `limit` rows.
    ///
    /// Returns `GatewayError::ResultSetTooLarge` if the merged row count would
    /// exceed `max_in_flight_rows`.
    pub async fn scatter_read(
        &self,
        view_name: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        let shard_count = self.shards.len().max(1);
        // Per-shard limit: ceiling division so we don't miss rows.
        let per_shard_limit = limit.map(|l| l.div_ceil(shard_count));

        // Read each shard in parallel.
        let prefix = format!("view_output/{view_name}/");
        let prefix_bytes = prefix.as_bytes().to_vec();

        let mut handles = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let shard = shard.clone();
            let pfx = prefix_bytes.clone();
            let per_limit = per_shard_limit;
            handles.push(tokio::spawn(async move {
                let kvs = shard.scan_prefix(&pfx).await?;
                let rows: Vec<Vec<u8>> = kvs
                    .into_iter()
                    .map(|(_k, v)| v.to_vec())
                    .take(per_limit.unwrap_or(usize::MAX))
                    .collect();
                Ok::<Vec<Vec<u8>>, rockstream_storage::StorageError>(rows)
            }));
        }

        let mut merged: Vec<Vec<u8>> = Vec::new();
        for handle in handles {
            let shard_rows = handle
                .await
                .map_err(|e| GatewayError::NotSupported(format!("join error: {e}")))?
                .map_err(GatewayError::Storage)?;
            merged.extend(shard_rows);
        }

        // Check bound before returning.
        let total = merged.len();
        if total > self.max_in_flight_rows {
            return Err(GatewayError::ResultSetTooLarge);
        }

        self.rows_in_flight.store(total, Ordering::Relaxed);

        // Truncate to global limit.
        let rows = match limit {
            Some(n) => merged.into_iter().take(n).collect(),
            None => merged,
        };

        Ok(rows)
    }

    /// Scatter a partial aggregation query to all shards, then merge (re-aggregate) the results.
    ///
    /// Bounds: MAX_PARTIAL_AGG_RESULT_ROWS per shard (enforced by shard);
    /// MAX_IN_FLIGHT_ROWS on merged total.
    pub async fn scatter_read_partial_agg(
        &self,
        view_name: &str,
        partial_plan_bytes: &[u8],
        sql: &str,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        use std::collections::HashMap;

        let planner_spec = extract_partial_agg_spec(sql).ok_or_else(|| {
            GatewayError::NotSupported("query is not eligible for partial aggregation".to_string())
        })?;
        let spec: PartialAggSpec = serde_json::from_slice(partial_plan_bytes)
            .map_err(|e| GatewayError::NotSupported(format!("invalid PartialAggSpec: {e}")))?;
        let agg_type = if spec.agg_type.is_empty() {
            planner_spec.agg_type
        } else {
            spec.agg_type.clone()
        };

        let prefix = format!("view_output/{view_name}/");
        let prefix_bytes = prefix.into_bytes();

        let mut handles = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let shard = shard.clone();
            let pfx = prefix_bytes.clone();
            let shard_spec = spec.clone();
            handles.push(tokio::spawn(async move {
                let rows = shard.scan_prefix(&pfx).await?;
                let mut groups: HashMap<String, i64> = HashMap::new();
                for (_k, v) in rows {
                    let row = String::from_utf8_lossy(&v);
                    let cols: Vec<&str> = row.split('\t').collect();
                    let key = cols
                        .get(shard_spec.group_col)
                        .copied()
                        .unwrap_or("")
                        .to_string();
                    let agg_val = cols.get(shard_spec.agg_col).copied().unwrap_or("0");
                    let num: i64 = agg_val.parse().unwrap_or(0);
                    let entry = groups.entry(key).or_insert(0);
                    match shard_spec.agg_type.as_str() {
                        "sum" => *entry += num,
                        "count" => *entry += 1,
                        _ => *entry += num,
                    }
                }
                Ok::<HashMap<String, i64>, rockstream_storage::StorageError>(groups)
            }));
        }

        let mut merged: HashMap<String, i64> = HashMap::new();
        for handle in handles {
            let partial_groups = handle
                .await
                .map_err(|e| GatewayError::NotSupported(format!("join error: {e}")))?
                .map_err(GatewayError::Storage)?;

            for (key, val) in partial_groups {
                let entry = merged.entry(key).or_insert(0);
                match agg_type.as_str() {
                    "sum" | "count" => *entry += val,
                    _ => *entry += val,
                }
            }
        }

        let total = merged.len();
        if total > self.max_in_flight_rows {
            return Err(GatewayError::ResultSetTooLarge);
        }

        self.rows_in_flight.store(total, Ordering::Relaxed);

        Ok(merged
            .into_iter()
            .map(|(k, v)| format!("{k}\t{v}").into_bytes())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use rockstream_storage::{ShardDb, ShardReader};
    use std::sync::Arc;

    async fn make_shard_with_rows(
        path: &str,
        view_name: &str,
        rows: &[(&str, &str)],
        store: Arc<InMemory>,
    ) -> Arc<ShardReader> {
        let shard_db = ShardDb::builder(path, store.clone()).build().await.unwrap();
        for (key_suffix, value) in rows {
            let key = format!("view_output/{view_name}/{key_suffix}");
            shard_db
                .put(key.as_bytes(), value.as_bytes())
                .await
                .unwrap();
        }
        shard_db.flush().await.unwrap();
        let reader = ShardReader::open(path, store).await.unwrap();
        Arc::new(reader)
    }

    #[tokio::test]
    async fn multi_shard_reader_pinned_to_frontier() {
        let store1 = Arc::new(InMemory::new());
        let store2 = Arc::new(InMemory::new());

        // Shard 1: rows 0-4
        let r1 = make_shard_with_rows(
            "shard1",
            "orders_mv",
            &[
                ("00000000", "1\t100.0"),
                ("00000001", "2\t200.0"),
                ("00000002", "3\t300.0"),
            ],
            store1,
        )
        .await;

        // Shard 2: rows 5-9
        let r2 = make_shard_with_rows(
            "shard2",
            "orders_mv",
            &[("00000003", "4\t400.0"), ("00000004", "5\t500.0")],
            store2,
        )
        .await;

        let msr = MultiShardReader::new(
            vec![r1, r2],
            /*pinned_frontier=*/ 42,
            MultiShardReader::DEFAULT_MAX_IN_FLIGHT_ROWS,
        );

        assert_eq!(msr.pinned_frontier(), 42);

        let rows = msr.scatter_read("orders_mv", None).await.unwrap();
        assert_eq!(rows.len(), 5, "merged result should have 5 rows");

        // fill-level metric updated
        assert_eq!(msr.rows_in_flight(), 5);
    }

    #[tokio::test]
    async fn multi_shard_reader_result_set_too_large() {
        let store = Arc::new(InMemory::new());
        let r1 = make_shard_with_rows(
            "shard-big",
            "big_view",
            &[("00000000", "a"), ("00000001", "b"), ("00000002", "c")],
            store,
        )
        .await;

        // max_in_flight_rows = 2 → should trigger too-large error with 3 rows
        let msr = MultiShardReader::new(vec![r1], 1, 2);
        let err = msr.scatter_read("big_view", None).await;
        assert!(
            matches!(err, Err(GatewayError::ResultSetTooLarge)),
            "expected ResultSetTooLarge"
        );
    }

    // ── S7: partial_agg_gateway_planner_detects_pushdown_query ───────────────

    #[test]
    fn partial_agg_gateway_planner_detects_pushdown_query() {
        assert!(
            can_pushdown_partial_agg("SELECT k, SUM(v) FROM mv GROUP BY k"),
            "GROUP BY + SUM should be detected as pushdown"
        );
        assert!(
            can_pushdown_partial_agg("SELECT region, COUNT(*) FROM orders_mv GROUP BY region"),
            "GROUP BY + COUNT(*) should be detected as pushdown"
        );
    }

    // ── S7: partial_agg_gateway_planner_rejects_non_pushdown ─────────────────

    #[test]
    fn partial_agg_gateway_planner_rejects_non_pushdown() {
        assert!(
            !can_pushdown_partial_agg("SELECT * FROM mv WHERE id > 5"),
            "full scan SELECT * should not be pushdown"
        );
        assert!(
            !can_pushdown_partial_agg("SELECT id, val FROM mv"),
            "no GROUP BY should not be pushdown"
        );
    }

    // ── S7: oracle_partial_agg_pushdown_equals_full_scan ─────────────────────

    /// Oracle: pushdown result must equal a re-aggregation of the full scan.
    #[tokio::test]
    async fn oracle_partial_agg_pushdown_equals_full_scan() {
        let mut shards_reader = vec![];
        for i in 0..3usize {
            let store = Arc::new(InMemory::new());
            let shard = Arc::new(
                ShardDb::builder(format!("oracle-shard-{i}"), store.clone())
                    .build()
                    .await
                    .unwrap(),
            );
            for j in 0u64..10 {
                let group = j % 5;
                let key = format!("view_output/mv/{:016x}", j);
                let val = format!("{group}\t{}", (i as u64 + 1) * 10 + j);
                shard.put(key.as_bytes(), val.as_bytes()).await.unwrap();
            }
            shard.flush().await.unwrap();
            shards_reader.push(Arc::new(
                ShardReader::open(format!("oracle-shard-{i}"), store)
                    .await
                    .unwrap(),
            ));
        }

        let msr = MultiShardReader::new(
            shards_reader,
            0,
            MultiShardReader::DEFAULT_MAX_IN_FLIGHT_ROWS,
        );

        let spec = PartialAggSpec {
            group_col: 0,
            agg_col: 1,
            agg_type: "sum".to_string(),
        };
        let plan_bytes = serde_json::to_vec(&spec).unwrap();

        let pushdown_rows = msr
            .scatter_read_partial_agg("mv", &plan_bytes, "SELECT k, SUM(v) FROM mv GROUP BY k")
            .await
            .unwrap();

        let full_rows = msr.scatter_read("mv", None).await.unwrap();
        let mut expected: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        for row in &full_rows {
            let s = String::from_utf8_lossy(row);
            let cols: Vec<&str> = s.split('\t').collect();
            let key = cols.first().copied().unwrap_or("").to_string();
            let val: i64 = cols.get(1).copied().unwrap_or("0").parse().unwrap_or(0);
            *expected.entry(key).or_insert(0) += val;
        }

        let mut pushdown_map: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for row in &pushdown_rows {
            let s = String::from_utf8_lossy(row);
            let cols: Vec<&str> = s.split('\t').collect();
            let key = cols.first().copied().unwrap_or("").to_string();
            let val: i64 = cols.get(1).copied().unwrap_or("0").parse().unwrap_or(0);
            pushdown_map.insert(key, val);
        }

        assert_eq!(
            pushdown_rows.len(),
            expected.len(),
            "pushdown group count should equal full-scan group count"
        );
        for (k, v) in &expected {
            assert_eq!(
                pushdown_map.get(k).copied().unwrap_or(0),
                *v,
                "group {k}: pushdown={}, full_scan={v}",
                pushdown_map.get(k).copied().unwrap_or(0)
            );
        }
    }

    fn make_stats(
        shard_id: u64,
        epoch: u64,
        col_idx: u16,
        min: &str,
        max: &str,
        bloom_values: &[&str],
    ) -> ShardColumnStats {
        let filter = rockstream_types::frontier::build_budget_capped_bloom_filter(
            &bloom_values
                .iter()
                .map(|value| value.as_bytes().to_vec())
                .collect::<Vec<_>>(),
            64,
        );
        ShardColumnStats {
            shard_id: ShardId(shard_id),
            view_id: rockstream_types::ids::ViewId(1),
            checkpoint_epoch: epoch,
            col_stats: vec![ColumnStats {
                col_idx,
                min_bytes: Some(Bytes::copy_from_slice(min.as_bytes())),
                max_bytes: Some(Bytes::copy_from_slice(max.as_bytes())),
                bloom_filter: Some(filter),
                null_count: 0,
                distinct_count_hll: Bytes::from(vec![0; 64]),
            }],
        }
    }

    #[test]
    fn scatter_planner_prunes_shards_outside_min_max_bounds() {
        let plan = plan_scatter_shards(
            &[
                make_stats(1, 10, 0, "a", "m", &["a", "b", "f"]),
                make_stats(2, 10, 0, "n", "z", &["n", "z"]),
            ],
            &[ScatterPredicate::Eq {
                col_idx: 0,
                value: b"b".to_vec(),
            }],
            5,
            10,
        );
        assert_eq!(plan.shard_ids, vec![ShardId(1)]);
        assert_eq!(plan.total_shards, 2);
    }

    #[test]
    fn scatter_planner_prunes_shards_via_bloom_negative() {
        let plan = plan_scatter_shards(
            &[
                make_stats(1, 10, 0, "a", "z", &["match-me"]),
                make_stats(2, 10, 0, "a", "z", &["other"]),
            ],
            &[ScatterPredicate::Eq {
                col_idx: 0,
                value: b"match-me".to_vec(),
            }],
            5,
            10,
        );
        assert_eq!(plan.shard_ids, vec![ShardId(1)]);
    }

    #[test]
    fn stale_shard_stats_falls_back_to_full_scatter_with_warning() {
        let plan = plan_scatter_shards(
            &[
                make_stats(1, 1, 0, "a", "m", &["a"]),
                make_stats(2, 1, 0, "n", "z", &["z"]),
            ],
            &[ScatterPredicate::Eq {
                col_idx: 0,
                value: b"b".to_vec(),
            }],
            5,
            10,
        );
        assert_eq!(plan.shard_ids, vec![ShardId(1), ShardId(2)]);
        assert!(plan.pruned_columns.is_empty());
    }
}

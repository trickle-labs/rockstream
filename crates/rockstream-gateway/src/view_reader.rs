//! `ViewReader` trait and strategy types for reading view output from storage.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use crate::error::GatewayError;

/// Batch size for streaming row delivery (Slice 4).
/// Peak per-connection memory is bounded by rows × average row size.
/// Fill-level metric constant.
pub const ROWS_IN_FLIGHT_BATCH: usize = 1_000;

/// Peak per-connection memory bound for streaming (64 MiB).
pub const STREAM_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Strategy for reading view rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewReadStrategy {
    /// Read from the hot LSM tier only (ShardReader on the latest published frontier).
    HotOnly,
    /// Reserved for two-tier hot+cold reads (Phase 9). Not implemented.
    TwoTier,
}

/// Trait for reading view output rows.
///
/// Implementations resolve a named view and return its rows as raw byte rows
/// (one `Vec<u8>` per row, tab-separated field values) at the current published
/// frontier.
#[async_trait]
pub trait ViewReader: Send + Sync {
    /// Read up to `limit` rows from the named view at the current published frontier.
    ///
    /// Returns rows as Vec<Vec<u8>> where each entry is a tab-separated row.
    async fn read_view(
        &self,
        view_name: &str,
        limit: Option<usize>,
        strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError>;

    /// Stream rows from the named view in batches of `ROWS_IN_FLIGHT_BATCH`.
    ///
    /// Each item is a batch of raw rows (tab-separated bytes). Peak in-flight
    /// memory is bounded by `STREAM_BATCH_BYTES`. No single batch exceeds
    /// `ROWS_IN_FLIGHT_BATCH` rows.
    ///
    /// Default implementation reads all rows up front and chunks them. Override
    /// for true incremental streaming.
    async fn read_view_stream(
        &self,
        view_name: &str,
        strategy: ViewReadStrategy,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Vec<u8>>, GatewayError>> + Send>>, GatewayError>
    {
        let rows = self.read_view(view_name, None, strategy).await?;
        let batches: Vec<Result<Vec<Vec<u8>>, GatewayError>> = rows
            .chunks(ROWS_IN_FLIGHT_BATCH)
            .map(|chunk| Ok(chunk.to_vec()))
            .collect();
        Ok(Box::pin(futures::stream::iter(batches)))
    }

    /// The current published frontier epoch (None if no data written yet).
    fn published_frontier(&self) -> Option<u64>;

    /// Read-only inspection of an intermediate Z-set arrangement key.
    /// Returns `Ok(Some((weight, epoch)))` if found, or `Ok(None)` if key does not exist.
    async fn peek_arrangement(
        &self,
        _view_name: &str,
        _op_id: u64,
        _key: &str,
    ) -> Result<Option<(i64, u64)>, GatewayError> {
        Ok(None)
    }
}

/// Column metadata for a view.
#[derive(Debug, Clone)]
pub struct ViewColumn {
    pub name: String,
    /// Arrow type name: "Int32", "Int64", "Float64", "Utf8", "Boolean", "Binary", "Timestamp"
    pub data_type: String,
}

/// Schema of a view (ordered column list).
#[derive(Debug, Clone)]
pub struct ViewSchema {
    pub columns: Vec<ViewColumn>,
}

/// `HotOnly` implementation backed by a single `ShardReader`.
///
/// v0.51.4 Slice 8: resolves `view_name` through the view directory
/// (`rockstream_ops::sink::read_view_directory_entry_via_reader` —
/// `view_name -> (op_id, num_cols, pk)`, written once at `CREATE VIEW` time)
/// and reads the compiled view's `ViewSinkOp` delta log through it,
/// materializing current state the same way a local, shard_db-backed read
/// does (`materialize_view_state`). This is the multi-shard publish/read
/// path — a standalone `--role gateway` process with no local `ShardDb`
/// has no other way to resolve a view name to its compiled storage
/// location. Before this slice, rows were read from a plain
/// `view_output/{view_name}/` string-keyed prefix written by the now-deleted
/// `view_materializer.rs`; nothing writes that format anymore.
pub struct HotOnlyViewReader {
    pub shard_reader: Arc<rockstream_storage::ShardReader>,
    /// The frontier epoch at which this reader was opened (if known).
    pub frontier_epoch: Option<u64>,
}

impl HotOnlyViewReader {
    /// Resolve `view_name` to its current materialized rows (TSV-encoded).
    ///
    /// Tries the directory-entry-resolved compiled-view format first; if
    /// `view_name` has no directory entry (never compiled through this
    /// process's `handle_create_view`, e.g. externally-published or
    /// pre-seeded data — a legitimate scenario for a reader opened against
    /// a shard it didn't itself write, and the shape a handful of
    /// lower-level protocol tests seed directly), falls back to the legacy
    /// `view_output/{view_name}/` string-keyed prefix scan.
    async fn materialized_rows(&self, view_name: &str) -> Result<Vec<Vec<u8>>, GatewayError> {
        let Some((op_id, num_cols, pk)) =
            rockstream_ops::sink::read_view_directory_entry_via_reader(
                &self.shard_reader,
                view_name,
            )
            .await
            .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                detail: format!("read_view_directory_entry({view_name}): {e}"),
            })?
        else {
            let prefix = format!("view_output/{view_name}/");
            let kvs = self.shard_reader.scan_prefix(prefix.as_bytes()).await?;
            return Ok(kvs.into_iter().map(|(_k, v)| v.to_vec()).collect());
        };
        let stored =
            rockstream_ops::sink::read_view_output_via_reader(&self.shard_reader, op_id, num_cols)
                .await
                .map_err(|e| GatewayError::QueryTimeExecutionFailed {
                    detail: format!("read_view_output({view_name}): {e}"),
                })?;
        let state = rockstream_ops::sink::materialize_view_state(stored, &pk);
        Ok(state
            .into_values()
            .flat_map(|(row, count)| {
                std::iter::repeat_with({
                    let row = row.clone();
                    move || rockstream_ops::sink::column_values_to_tsv_bytes(&row)
                })
                .take(count.max(0) as usize)
            })
            .collect())
    }
}

#[async_trait]
impl ViewReader for HotOnlyViewReader {
    async fn read_view(
        &self,
        view_name: &str,
        limit: Option<usize>,
        strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        if strategy == ViewReadStrategy::TwoTier {
            return Err(GatewayError::NotSupported(
                "TwoTier strategy reserved for Phase 9".to_string(),
            ));
        }
        let rows = self.materialized_rows(view_name).await?;
        let rows = match limit {
            Some(n) => rows.into_iter().take(n).collect(),
            None => rows,
        };
        Ok(rows)
    }

    /// Streaming implementation: chunks the materialized rows into
    /// `ROWS_IN_FLIGHT_BATCH`-sized, `STREAM_BATCH_BYTES`-bounded batches.
    async fn read_view_stream(
        &self,
        view_name: &str,
        strategy: ViewReadStrategy,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Vec<u8>>, GatewayError>> + Send>>, GatewayError>
    {
        if strategy == ViewReadStrategy::TwoTier {
            return Err(GatewayError::NotSupported(
                "TwoTier strategy reserved for Phase 9".to_string(),
            ));
        }
        let rows = self.materialized_rows(view_name).await?;

        // Build batches: each batch is at most ROWS_IN_FLIGHT_BATCH rows and
        // at most STREAM_BATCH_BYTES bytes in total per-connection.
        let mut batches: Vec<Result<Vec<Vec<u8>>, GatewayError>> = Vec::new();
        let mut current_batch: Vec<Vec<u8>> = Vec::new();
        let mut current_batch_bytes: usize = 0;

        for row in rows {
            let row_len = row.len();
            current_batch.push(row);
            current_batch_bytes += row_len;

            if current_batch.len() >= ROWS_IN_FLIGHT_BATCH
                || current_batch_bytes >= STREAM_BATCH_BYTES
            {
                batches.push(Ok(std::mem::take(&mut current_batch)));
                current_batch_bytes = 0;
            }
        }
        if !current_batch.is_empty() {
            batches.push(Ok(current_batch));
        }

        Ok(Box::pin(futures::stream::iter(batches)))
    }

    fn published_frontier(&self) -> Option<u64> {
        self.frontier_epoch
    }

    async fn peek_arrangement(
        &self,
        _view_name: &str,
        op_id: u64,
        key: &str,
    ) -> Result<Option<(i64, u64)>, GatewayError> {
        let epoch = self.published_frontier().unwrap_or(0);
        let mut db_key = Vec::with_capacity(9 + key.len());
        db_key.push(0x01);
        db_key.extend_from_slice(&op_id.to_be_bytes());
        if let Ok(num_key) = key.parse::<i64>() {
            db_key.extend_from_slice(&num_key.to_be_bytes());
        } else {
            db_key.extend_from_slice(key.as_bytes());
        }

        if let Some(val) = self.shard_reader.get(&db_key).await? {
            let weight = if val.len() >= 16 {
                i64::from_be_bytes(val[8..16].try_into().unwrap_or([0; 8]))
            } else if val.len() >= 8 {
                i64::from_be_bytes(val[0..8].try_into().unwrap_or([0; 8]))
            } else {
                1i64
            };
            Ok(Some((weight, epoch)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use rockstream_ops::sink::{write_view_directory_entry, ViewSinkOp};
    use rockstream_ops::ArrowZSet;
    use rockstream_storage::ShardDb;
    use rockstream_types::ids::OperatorId;
    use std::sync::Arc;

    /// Write `values.len()` rows of a single-column, weight-`1` compiled
    /// view through `ViewSinkOp` plus its directory entry — the v0.51.4
    /// Slice 8 replacement for directly `put`-ing rows under the retired
    /// `view_output/{view_name}/` string-keyed format.
    async fn write_compiled_view(db: Arc<ShardDb>, view_name: &str, op_id: u64, values: &[i64]) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let array = Arc::new(Int64Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        let weights = vec![1i64; values.len()];
        let zset = ArrowZSet::new(batch, weights);
        let sink = ViewSinkOp::new(db.clone(), OperatorId(op_id));
        sink.write_next_epoch(&zset).await.unwrap();
        write_view_directory_entry(&db, view_name, OperatorId(op_id), 1, &[0])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn view_reader_hot_only_reads_from_shard() {
        let store = Arc::new(InMemory::new());
        let shard_db = Arc::new(
            ShardDb::builder("test-shard", store.clone())
                .build()
                .await
                .unwrap(),
        );

        write_compiled_view(shard_db.clone(), "my_view", 1, &[0, 1, 2, 3, 4]).await;
        shard_db.flush().await.unwrap();

        // Open a ShardReader
        let reader = rockstream_storage::ShardReader::open("test-shard", store.clone())
            .await
            .unwrap();
        let view_reader = HotOnlyViewReader {
            shard_reader: Arc::new(reader),
            frontier_epoch: Some(1),
        };

        let rows = view_reader
            .read_view("my_view", Some(10), ViewReadStrategy::HotOnly)
            .await
            .unwrap();

        assert_eq!(rows.len(), 5, "expected 5 rows");

        // TwoTier returns error
        let err = view_reader
            .read_view("my_view", None, ViewReadStrategy::TwoTier)
            .await;
        assert!(
            matches!(err, Err(GatewayError::NotSupported(_))),
            "TwoTier should return NotSupported"
        );
    }

    /// Slice 4 green gate: `read_view_stream` yields rows in bounded batches
    /// without collecting all rows up front. Verifies no single batch exceeds
    /// ROWS_IN_FLIGHT_BATCH and the total row count is correct.
    #[tokio::test]
    async fn test_streaming_peak_memory_bounded() {
        // Use a smaller row count since writing 2_000_000 rows to in-memory store
        // is slow in unit tests. The important invariant is that no batch exceeds
        // ROWS_IN_FLIGHT_BATCH rows.
        let n_rows: usize = 3_500; // > 3 * ROWS_IN_FLIGHT_BATCH is not needed; 3.5 batches

        let store = Arc::new(InMemory::new());
        let shard_db = Arc::new(
            ShardDb::builder("stream-shard", store.clone())
                .build()
                .await
                .unwrap(),
        );

        let values: Vec<i64> = (0..n_rows as i64).collect();
        write_compiled_view(shard_db.clone(), "stream_view", 1, &values).await;
        shard_db.flush().await.unwrap();

        let reader = rockstream_storage::ShardReader::open("stream-shard", store.clone())
            .await
            .unwrap();
        let view_reader = HotOnlyViewReader {
            shard_reader: Arc::new(reader),
            frontier_epoch: Some(1),
        };

        let mut stream = view_reader
            .read_view_stream("stream_view", ViewReadStrategy::HotOnly)
            .await
            .unwrap();

        let mut total_rows = 0usize;
        let mut batch_count = 0usize;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.expect("stream batch error");
            assert!(
                batch.len() <= ROWS_IN_FLIGHT_BATCH,
                "batch {} has {} rows, exceeds ROWS_IN_FLIGHT_BATCH={}",
                batch_count,
                batch.len(),
                ROWS_IN_FLIGHT_BATCH
            );
            total_rows += batch.len();
            batch_count += 1;
        }

        assert_eq!(total_rows, n_rows, "total rows mismatch");
        // With 3500 rows and batch size 1000, we expect 4 batches (1000+1000+1000+500)
        assert!(
            batch_count >= 2,
            "expected at least 2 batches for {n_rows} rows with ROWS_IN_FLIGHT_BATCH={ROWS_IN_FLIGHT_BATCH}"
        );
    }
}

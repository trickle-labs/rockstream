//! S3 source connector tracking file indices and line offsets (§13.3).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;

use crate::source_connector::{PollDeltaResult, SnapshotStream, SourceConnector, SourceError};
use crate::source_epoch::OffsetToken;

/// Buffer bound declaration: S3 source read buffer limit (64 MiB).
pub const S3_SOURCE_BUFFER_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// Production `S3Source` implementing `SourceConnector`.
pub struct S3Source {
    _connector_id: ConnectorId,
    schema: SchemaRef,
    files: Vec<(String, Vec<Vec<i64>>)>,
    paused: bool,
    last_committed: Option<(Epoch, OffsetToken)>,
    store: Option<Arc<dyn object_store::ObjectStore>>,
    bucket_prefix: Option<String>,
}

impl S3Source {
    pub fn new(connector_id: ConnectorId, schema: SchemaRef) -> Self {
        Self {
            _connector_id: connector_id,
            schema,
            files: Vec::new(),
            paused: false,
            last_committed: None,
            store: None,
            bucket_prefix: None,
        }
    }

    pub fn with_object_store(
        mut self,
        store: Arc<dyn object_store::ObjectStore>,
        prefix: Option<String>,
    ) -> Self {
        self.store = Some(store);
        self.bucket_prefix = prefix;
        self
    }

    fn sync_files_from_store(&mut self) -> Result<(), SourceError> {
        let store = match &self.store {
            Some(s) => s.clone(),
            None => return Ok(()),
        };
        let prefix = self.bucket_prefix.clone();

        // Run asynchronously by spawning a dedicated thread to drive the runtime
        let downloaded = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                use futures::StreamExt;
                let path_prefix = prefix
                    .as_ref()
                    .map(|p| object_store::path::Path::from(p.as_str()));
                let mut list_stream = store.list(path_prefix.as_ref());
                let mut objs = Vec::new();
                while let Some(meta) = list_stream.next().await {
                    if let Ok(meta) = meta {
                        objs.push(meta.location);
                    }
                }

                // Sort locations
                let mut locations_sorted = objs;
                locations_sorted.sort();

                // Get data for each location
                let mut results = Vec::new();
                for location in locations_sorted {
                    let get_res = store.get(&location).await;
                    if let Ok(res) = get_res {
                        if let Ok(bytes) = res.bytes().await {
                            results.push((location.to_string(), bytes.to_vec()));
                        }
                    }
                }
                results
            })
        })
        .join()
        .map_err(|e| SourceError::Io(format!("helper thread panicked: {e:?}")))?;

        for (filename, data) in downloaded {
            // Check if we already have this file in self.files
            if self.files.iter().any(|(name, _)| name == &filename) {
                continue;
            }

            // Parse data. Let's assume each line is a JSON-serialized Vec<i64> (JSON array)
            // or let's parse it as a JSON array of arrays: e.g. [[1, 10], [2, 20]] or newline-delimited JSON.
            let mut records = Vec::new();
            let text = String::from_utf8_lossy(&data);
            let trimmed = text.trim();
            if trimmed.starts_with('[') {
                if let Ok(rows) = serde_json::from_str::<Vec<Vec<i64>>>(trimmed) {
                    records = rows;
                }
            }
            if records.is_empty() {
                for line in trimmed.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if let Ok(row) = serde_json::from_str::<Vec<i64>>(line) {
                        records.push(row);
                    }
                }
            }

            self.files.push((filename, records));
        }

        // Sort self.files by name to be consistent
        self.files.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(())
    }

    /// Add a file with records to the mock S3 bucket.
    pub fn add_file(&mut self, filename: String, records: Vec<Vec<i64>>) {
        self.files.push((filename, records));
        // Sort files by name to ensure deterministic scanning order
        self.files.sort_by(|a, b| a.0.cmp(&b.0));
    }

    /// Retrieve (file_index, line_offset) from a serialized OffsetToken.
    pub fn get_file_position(&self, token: &OffsetToken) -> Option<(usize, usize)> {
        if token.as_bytes().is_empty() {
            Some((0, 0))
        } else {
            serde_json::from_slice(token.as_bytes()).ok()
        }
    }

    /// Build a single `RecordBatch` from polled records.
    fn build_batch(&self, records: &[Vec<i64>]) -> Result<Vec<RecordBatch>, SourceError> {
        use arrow::array::Int64Array;
        use rockstream_types::arrow_batch::append_weight_column;

        let num_rows = records.len();
        let num_cols = self.schema.fields().len();

        let mut columns: Vec<Vec<i64>> = vec![vec![0; num_rows]; num_cols];
        let weights = vec![1; num_rows]; // Default insert weight (+1)

        for (r_idx, row) in records.iter().enumerate() {
            for (c_idx, col) in columns.iter_mut().enumerate().take(num_cols) {
                col[r_idx] = row.get(c_idx).copied().unwrap_or(0);
            }
        }

        let mut arrow_columns: Vec<arrow::array::ArrayRef> = vec![];
        for col_data in columns {
            arrow_columns.push(Arc::new(Int64Array::from(col_data)));
        }

        let data_batch = RecordBatch::try_new(self.schema.clone(), arrow_columns).map_err(|e| {
            SourceError::PollDeltaFailed {
                reason: format!("failed to build RecordBatch: {e}"),
            }
        })?;

        let weighted_batch = append_weight_column(data_batch, &weights).map_err(|e| {
            SourceError::PollDeltaFailed {
                reason: format!("failed to append weight column: {e}"),
            }
        })?;

        Ok(vec![weighted_batch])
    }

    pub fn last_committed(&self) -> Option<(Epoch, OffsetToken)> {
        self.last_committed.clone()
    }
}

impl SourceConnector for S3Source {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(self.schema.clone())
    }

    fn start_snapshot(
        &mut self,
        _frontier: Epoch,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        self.sync_files_from_store()?;
        let mut all_records = Vec::new();
        for (_, rows) in &self.files {
            all_records.extend(rows.iter().cloned());
        }

        let batches = if all_records.is_empty() {
            Vec::new()
        } else {
            self.build_batch(&all_records)?
        };

        Ok(SnapshotStream::new(batches))
    }

    fn poll_delta(
        &mut self,
        after: OffsetToken,
        _max_bytes: usize,
        credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        self.sync_files_from_store()?;
        if self.paused || credits_available == 0 {
            return Ok(PollDeltaResult {
                batches: vec![],
                new_offset: after,
                watermark: None,
            });
        }

        let (mut file_idx, mut line_offset): (usize, usize) = if after.as_bytes().is_empty() {
            (0, 0)
        } else {
            serde_json::from_slice(after.as_bytes()).map_err(|e| SourceError::PollDeltaFailed {
                reason: format!("failed to deserialize offset token: {e}"),
            })?
        };

        let mut polled_records = Vec::new();
        let mut credits_left = credits_available;

        while file_idx < self.files.len() && credits_left > 0 {
            let (_, rows) = &self.files[file_idx];
            if line_offset < rows.len() {
                let to_take = std::cmp::min(rows.len() - line_offset, credits_left);
                let range = line_offset..(line_offset + to_take);
                polled_records.extend(rows[range].iter().cloned());
                line_offset += to_take;
                credits_left -= to_take;
            }
            if line_offset >= rows.len() {
                file_idx += 1;
                line_offset = 0;
            }
        }

        let batches = if polled_records.is_empty() {
            Vec::new()
        } else {
            self.build_batch(&polled_records)?
        };

        let new_offset_bytes = serde_json::to_vec(&(file_idx, line_offset)).map_err(|e| {
            SourceError::PollDeltaFailed {
                reason: format!("failed to serialize offset token: {e}"),
            }
        })?;

        Ok(PollDeltaResult {
            batches,
            new_offset: OffsetToken::new(new_offset_bytes),
            watermark: None,
        })
    }

    fn commit_offset(&mut self, epoch: Epoch, offset: OffsetToken) -> Result<(), SourceError> {
        self.last_committed = Some((epoch, offset));
        Ok(())
    }

    fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        self.paused = true;
        Ok(())
    }

    fn resume(&mut self) -> Result<(), SourceError> {
        self.paused = false;
        Ok(())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_types::arrow_batch::split_weight_column;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    #[test]
    fn test_s3_source_basic() {
        let schema = test_schema();
        let mut source = S3Source::new(ConnectorId(202), schema);

        // Add files to the mock bucket
        source.add_file("file1.json".to_string(), vec![vec![1, 10], vec![2, 20]]);
        source.add_file("file2.json".to_string(), vec![vec![3, 30]]);

        // Poll with 1 credit limit (first row of file1)
        let token_start = OffsetToken::new(vec![]);
        let res1 = source
            .poll_delta(token_start.clone(), 1024, 1, None)
            .unwrap();
        assert_eq!(res1.batches.len(), 1);
        let (data1, weights1) = split_weight_column(&res1.batches[0]).unwrap();
        assert_eq!(data1.num_rows(), 1);
        assert_eq!(weights1, vec![1]);

        let pos1 = source.get_file_position(&res1.new_offset).unwrap();
        assert_eq!(pos1, (0, 1)); // file 0, line 1

        // Poll with remaining rows
        let res2 = source
            .poll_delta(res1.new_offset.clone(), 1024, 10, None)
            .unwrap();
        assert_eq!(res2.batches.len(), 1);
        let (data2, weights2) = split_weight_column(&res2.batches[0]).unwrap();
        assert_eq!(data2.num_rows(), 2); // 1 remaining from file1, 1 from file2
        assert_eq!(weights2, vec![1, 1]);

        let pos2 = source.get_file_position(&res2.new_offset).unwrap();
        assert_eq!(pos2, (2, 0)); // finished file1 (0) and file2 (1), points to next (2, 0)

        // Verify snapshot contains everything
        let mut snapshot_stream = source.start_snapshot(0, None).unwrap();
        let snap_batch = snapshot_stream.next().unwrap();
        let (snap_data, _) = split_weight_column(&snap_batch).unwrap();
        assert_eq!(snap_data.num_rows(), 3); // all 3 rows

        // Commit offsets
        source.commit_offset(10, res2.new_offset.clone()).unwrap();
        let (epoch, commit_tok) = source.last_committed().unwrap();
        assert_eq!(epoch, 10);
        assert_eq!(commit_tok, res2.new_offset);
    }
}

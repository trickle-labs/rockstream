//! S3 source connector tracking file indices and line offsets (§13.3).

use async_trait::async_trait;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;

use crate::source_connector::{
    PollDeltaResult, SnapshotBatch, SnapshotStream, SourceConnector, SourceError,
};
use crate::source_epoch::{OffsetToken, SnapshotDeltaFence};
use crate::source_json::{json_rows_to_batch, JsonRow};

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
enum SourceRow {
    Values(JsonRow),
    Change {
        values: JsonRow,
        #[serde(default = "default_weight")]
        weight: i64,
    },
}

impl SourceRow {
    fn values(&self) -> &JsonRow {
        match self {
            Self::Values(values) | Self::Change { values, .. } => values,
        }
    }

    fn weight(&self) -> i64 {
        match self {
            Self::Values(_) => 1,
            Self::Change { weight, .. } => *weight,
        }
    }
}

const fn default_weight() -> i64 {
    1
}

/// Buffer bound declaration: S3 source read buffer limit (64 MiB).
pub const S3_SOURCE_BUFFER_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const S3_SNAPSHOT_BATCH_MAX_ROWS: usize = 10_000;

/// Production `S3Source` implementing `SourceConnector`.
pub struct S3Source {
    _connector_id: ConnectorId,
    schema: SchemaRef,
    files: Vec<(String, Vec<SourceRow>)>,
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

    pub async fn sync_files_from_store(&mut self) -> Result<(), SourceError> {
        // Kept for callers that explicitly validate a configured source.  Do
        // not cache object contents here: snapshot and delta reads fetch one
        // bounded object at a time.
        if self.store.is_some() {
            self.current_manifest().await?;
        }
        Ok(())
    }

    /// Add a file with records to the mock S3 bucket.
    pub fn add_file(&mut self, filename: String, records: Vec<Vec<i64>>) {
        self.files.push((
            filename,
            records
                .into_iter()
                .map(|row| {
                    SourceRow::Values(row.into_iter().map(serde_json::Value::from).collect())
                })
                .collect(),
        ));
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
    fn build_batch(&self, records: &[SourceRow]) -> Result<Vec<RecordBatch>, SourceError> {
        use rockstream_types::arrow_batch::append_weight_column;

        let rows = records
            .iter()
            .map(|record| record.values().clone())
            .collect::<Vec<_>>();
        let weighted_batch = append_weight_column(
            json_rows_to_batch(&self.schema, &rows, "S3")?,
            &records.iter().map(SourceRow::weight).collect::<Vec<_>>(),
        )
        .map_err(|e| SourceError::PollDeltaFailed {
            reason: format!("failed to append weight column: {e}"),
        })?;

        Ok(vec![weighted_batch])
    }

    pub fn last_committed(&self) -> Option<(Epoch, OffsetToken)> {
        self.last_committed.clone()
    }

    fn parse_rows(data: &[u8]) -> Result<Vec<SourceRow>, SourceError> {
        let text = String::from_utf8_lossy(data);
        let trimmed = text.trim();
        if trimmed.starts_with('[') {
            return serde_json::from_str(trimmed).map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("S3 JSON array decoding failed: {error}"),
            });
        }
        trimmed
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line.trim()).map_err(|error| SourceError::PollDeltaFailed {
                    reason: format!("S3 JSON line decoding failed: {error}"),
                })
            })
            .collect()
    }

    async fn object_locations(&self) -> Result<Vec<String>, SourceError> {
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| SourceError::Io("S3 object store is not configured".to_string()))?;
        use futures::StreamExt;
        let path_prefix = self
            .bucket_prefix
            .as_ref()
            .map(|prefix| object_store::path::Path::from(prefix.as_str()));
        let mut locations = Vec::new();
        let mut listed = store.list(path_prefix.as_ref());
        while let Some(meta) = listed.next().await {
            locations.push(
                meta.map_err(|error| {
                    SourceError::Io(format!("S3 object listing failed: {error}"))
                })?
                .location
                .to_string(),
            );
        }
        locations.sort();
        Ok(locations)
    }

    async fn rows_for_object(&self, name: &str) -> Result<Vec<SourceRow>, SourceError> {
        if self.store.is_none() {
            return self
                .files
                .iter()
                .find(|(candidate, _)| candidate == name)
                .map(|(_, rows)| rows.clone())
                .ok_or_else(|| SourceError::Io(format!("S3 object '{name}' is missing")));
        }
        let store = self.store.as_ref().expect("checked above");
        let bytes = store
            .get(&object_store::path::Path::from(name))
            .await
            .map_err(|error| SourceError::Io(format!("S3 object '{name}' fetch failed: {error}")))?
            .bytes()
            .await
            .map_err(|error| SourceError::Io(format!("S3 object '{name}' read failed: {error}")))?;
        if bytes.len() > S3_SOURCE_BUFFER_LIMIT_BYTES {
            return Err(SourceError::PollDeltaFailed {
                reason: format!(
                    "S3 object '{name}' exceeds S3_SOURCE_BUFFER_LIMIT_BYTES={S3_SOURCE_BUFFER_LIMIT_BYTES}"
                ),
            });
        }
        Self::parse_rows(&bytes)
    }

    async fn current_manifest(&self) -> Result<Vec<(String, usize)>, SourceError> {
        let names = if self.store.is_some() {
            self.object_locations().await?
        } else {
            self.files.iter().map(|(name, _)| name.clone()).collect()
        };
        let mut manifest = Vec::with_capacity(names.len());
        for name in names {
            manifest.push((name.clone(), self.rows_for_object(&name).await?.len()));
        }
        Ok(manifest)
    }
}

#[async_trait]
impl SourceConnector for S3Source {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(self.schema.clone())
    }

    async fn capture_snapshot_delta_fence(
        &mut self,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotDeltaFence, SourceError> {
        let manifest = self.current_manifest().await?;
        let token = OffsetToken::new(
            serde_json::to_vec(&manifest)
                .map_err(|error| SourceError::Io(format!("S3 fence encoding failed: {error}")))?,
        );
        Ok(SnapshotDeltaFence::new(token.clone(), token))
    }

    async fn start_snapshot(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        self.start_snapshot_bounded(fence, after, None, S3_SNAPSHOT_BATCH_MAX_ROWS)
            .await
    }

    async fn start_snapshot_bounded(
        &mut self,
        fence: &SnapshotDeltaFence,
        after: Option<OffsetToken>,
        _partition_filter: Option<PartitionFilter>,
        max_rows: usize,
    ) -> Result<SnapshotStream, SourceError> {
        let manifest: Vec<(String, usize)> = serde_json::from_slice(fence.snapshot.as_bytes())
            .map_err(|error| {
                SourceError::Io(format!("S3 snapshot fence decoding failed: {error}"))
            })?;
        let total_rows = manifest.iter().map(|(_, count)| *count).sum::<usize>();
        let next_row = match after {
            None => 0,
            Some(after) if after == fence.snapshot => total_rows,
            Some(after) if after.as_bytes().is_empty() => 0,
            Some(after) => serde_json::from_slice::<usize>(after.as_bytes()).map_err(|error| {
                SourceError::PollDeltaFailed {
                    reason: format!("S3 snapshot cursor decoding failed: {error}"),
                }
            })?,
        };
        if next_row > total_rows {
            return Err(SourceError::PollDeltaFailed {
                reason: format!(
                    "S3 snapshot cursor {next_row} exceeds fenced snapshot length {}",
                    total_rows
                ),
            });
        }
        if next_row == total_rows {
            return Ok(SnapshotStream::with_remaining(Vec::new(), 0));
        }
        let max_rows = max_rows.clamp(1, S3_SNAPSHOT_BATCH_MAX_ROWS);
        let mut skipped = 0usize;
        let mut records = Vec::with_capacity(max_rows);
        for (name, count) in manifest {
            if skipped.saturating_add(count) <= next_row {
                skipped += count;
                continue;
            }
            let rows = self.rows_for_object(&name).await?;
            if rows.len() < count {
                return Err(SourceError::Io(format!(
                    "S3 snapshot fence references changed object '{name}'"
                )));
            }
            let start = next_row.saturating_sub(skipped);
            let take = (max_rows - records.len()).min(count.saturating_sub(start));
            records.extend(rows[start..start + take].iter().cloned());
            skipped += count;
            if records.len() == max_rows {
                break;
            }
        }
        let end = next_row + records.len();
        let resume_offset = if end == total_rows {
            fence.snapshot.clone()
        } else {
            OffsetToken::new(serde_json::to_vec(&end).map_err(|error| {
                SourceError::Io(format!("S3 snapshot cursor encoding failed: {error}"))
            })?)
        };
        let batches = self
            .build_batch(&records)?
            .into_iter()
            .map(|batch| SnapshotBatch {
                batch,
                resume_offset: resume_offset.clone(),
            })
            .collect();
        Ok(SnapshotStream::with_remaining(
            batches,
            total_rows - next_row,
        ))
    }

    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        if self.paused || credits_available == 0 {
            return Ok(PollDeltaResult {
                batches: vec![],
                new_offset: after,
                watermark: None,
            });
        }
        if max_bytes == 0 {
            return Ok(PollDeltaResult {
                batches: vec![],
                new_offset: after,
                watermark: None,
            });
        }

        if let Ok(known) = serde_json::from_slice::<Vec<(String, usize)>>(after.as_bytes()) {
            let known = known
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>();
            let mut updated = known.clone();
            let mut records = Vec::new();
            let mut credits_left = credits_available;
            let mut used_bytes = 0usize;
            for (name, count) in self.current_manifest().await? {
                let rows = self.rows_for_object(&name).await?;
                let start = known.get(&name).copied().unwrap_or(0).min(count);
                let mut take = 0;
                for row in &rows[start..] {
                    if take == credits_left {
                        break;
                    }
                    let row_bytes = serde_json::to_vec(row)
                        .map_err(|error| SourceError::PollDeltaFailed {
                            reason: format!("S3 record sizing failed: {error}"),
                        })?
                        .len();
                    if row_bytes > max_bytes && records.is_empty() {
                        return Err(SourceError::PollDeltaFailed {
                            reason: format!(
                                "S3 record exceeds max_bytes={max_bytes}; next_steps: increase BACKFILL_LIVE_DELTA_MAX_BYTES"
                            ),
                        });
                    }
                    if used_bytes.saturating_add(row_bytes) > max_bytes {
                        break;
                    }
                    records.push(row.clone());
                    used_bytes += row_bytes;
                    take += 1;
                }
                if take > 0 {
                    updated.insert(name, start + take);
                    credits_left -= take;
                }
                if credits_left == 0 || used_bytes >= max_bytes {
                    break;
                }
            }
            let batches = if records.is_empty() {
                Vec::new()
            } else {
                self.build_batch(&records)?
            };
            let mut next = updated.into_iter().collect::<Vec<_>>();
            next.sort_by(|a, b| a.0.cmp(&b.0));
            return Ok(PollDeltaResult {
                batches,
                new_offset: OffsetToken::new(serde_json::to_vec(&next).map_err(|error| {
                    SourceError::PollDeltaFailed {
                        reason: format!("S3 live cursor encoding failed: {error}"),
                    }
                })?),
                watermark: None,
            });
        }

        if self.store.is_some() {
            let (mut file_idx, mut line_offset): (usize, usize) = if after.as_bytes().is_empty() {
                (0, 0)
            } else {
                serde_json::from_slice(after.as_bytes()).map_err(|error| {
                    SourceError::PollDeltaFailed {
                        reason: format!("failed to deserialize offset token: {error}"),
                    }
                })?
            };
            let manifest = self.current_manifest().await?;
            let mut records = Vec::new();
            let mut credits_left = credits_available;
            while file_idx < manifest.len() && credits_left > 0 {
                let (name, count) = &manifest[file_idx];
                let rows = self.rows_for_object(name).await?;
                let available = count.saturating_sub(line_offset);
                let take = available.min(credits_left);
                records.extend(rows[line_offset..line_offset + take].iter().cloned());
                line_offset += take;
                credits_left -= take;
                if line_offset == *count {
                    file_idx += 1;
                    line_offset = 0;
                }
            }
            let batches = if records.is_empty() {
                Vec::new()
            } else {
                self.build_batch(&records)?
            };
            return Ok(PollDeltaResult {
                batches,
                new_offset: OffsetToken::new(
                    serde_json::to_vec(&(file_idx, line_offset)).map_err(|error| {
                        SourceError::PollDeltaFailed {
                            reason: format!("failed to serialize offset token: {error}"),
                        }
                    })?,
                ),
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

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        self.last_committed = Some((epoch, offset));
        Ok(())
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        self.paused = true;
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.paused = false;
        Ok(())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_types::arrow_batch::split_weight_column;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    #[tokio::test]
    async fn test_s3_source_basic() {
        let schema = test_schema();
        let mut source = S3Source::new(ConnectorId(202), schema);

        // Add files to the mock bucket
        source.add_file("file1.json".to_string(), vec![vec![1, 10], vec![2, 20]]);
        source.add_file("file2.json".to_string(), vec![vec![3, 30]]);

        let fence = SnapshotDeltaFence::new(
            OffsetToken::new(
                serde_json::to_vec(&vec![
                    ("file1.json".to_string(), 2_usize),
                    ("file2.json".to_string(), 1_usize),
                ])
                .unwrap(),
            ),
            OffsetToken::new(
                serde_json::to_vec(&vec![
                    ("file1.json".to_string(), 2_usize),
                    ("file2.json".to_string(), 1_usize),
                ])
                .unwrap(),
            ),
        );
        assert_eq!(
            source.capture_snapshot_delta_fence(None).await.unwrap(),
            fence
        );

        // Poll with 1 credit limit (first row of file1)
        let token_start = OffsetToken::new(vec![]);
        let res1 = source
            .poll_delta(token_start.clone(), 1024, 1, None)
            .await
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
            .await
            .unwrap();
        assert_eq!(res2.batches.len(), 1);
        let (data2, weights2) = split_weight_column(&res2.batches[0]).unwrap();
        assert_eq!(data2.num_rows(), 2); // 1 remaining from file1, 1 from file2
        assert_eq!(weights2, vec![1, 1]);

        let pos2 = source.get_file_position(&res2.new_offset).unwrap();
        assert_eq!(pos2, (2, 0)); // finished file1 (0) and file2 (1), points to next (2, 0)

        // Verify snapshot contains everything
        let mut snapshot_stream = source.start_snapshot(&fence, None, None).await.unwrap();
        let snap_batch = snapshot_stream.next().unwrap();
        let (snap_data, _) = split_weight_column(&snap_batch.batch).unwrap();
        assert_eq!(snap_data.num_rows(), 3); // all 3 rows
        assert_eq!(
            source
                .start_snapshot(&fence, Some(snap_batch.resume_offset.clone()), None)
                .await
                .unwrap()
                .count(),
            0,
            "a committed snapshot cursor must not replay its completed chunk"
        );

        // Commit offsets
        source
            .commit_offset(10, res2.new_offset.clone())
            .await
            .unwrap();
        let (epoch, commit_tok) = source.last_committed().unwrap();
        assert_eq!(epoch, 10);
        assert_eq!(commit_tok, res2.new_offset);
    }

    #[tokio::test]
    async fn json_rows_preserve_bound_text_and_decimal_types() {
        use arrow::array::{Decimal128Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("amount", DataType::Decimal128(12, 2), false),
        ]));
        let mut source = S3Source::new(ConnectorId(206), schema);
        source.files.push((
            "typed.json".to_string(),
            vec![SourceRow::Values(vec![
                serde_json::json!("a"),
                serde_json::json!("10.25"),
            ])],
        ));
        let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
        let batch = source
            .start_snapshot(&fence, None, None)
            .await
            .unwrap()
            .next()
            .unwrap()
            .batch;
        let (data, weights) = split_weight_column(&batch).unwrap();
        assert_eq!(
            (
                data.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                data.column(1)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(0),
                weights,
            ),
            ("a", 1025, vec![1])
        );
    }

    #[tokio::test]
    async fn json_rows_reject_values_that_do_not_match_the_bound_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "enabled",
            DataType::Boolean,
            false,
        )]));
        let mut source = S3Source::new(ConnectorId(207), schema);
        source.files.push((
            "invalid.json".to_string(),
            vec![SourceRow::Values(vec![serde_json::json!("true")])],
        ));
        let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
        let error = match source.start_snapshot(&fence, None, None).await {
            Ok(_) => panic!("malformed boolean source row must be rejected"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "RS-4004: source poll failed: S3 value '\"true\"' is not a boolean"
        );
    }

    #[tokio::test]
    async fn json_change_rows_preserve_explicit_delete_weights() {
        let mut source = S3Source::new(ConnectorId(208), test_schema());
        source.files.push((
            "changes.json".to_string(),
            vec![
                SourceRow::Values(vec![serde_json::json!(1), serde_json::json!(10)]),
                SourceRow::Change {
                    values: vec![serde_json::json!(1), serde_json::json!(10)],
                    weight: -1,
                },
            ],
        ));
        let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
        let batch = source
            .start_snapshot(&fence, None, None)
            .await
            .unwrap()
            .next()
            .unwrap()
            .batch;
        let (data, weights) = split_weight_column(&batch).unwrap();
        assert_eq!(
            (
                data.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                weights,
            ),
            (&[1_i64, 1][..], vec![1, -1])
        );
    }

    #[tokio::test]
    async fn snapshot_uses_captured_manifest_not_later_files() {
        let mut source = S3Source::new(ConnectorId(203), test_schema());
        source.add_file("before.json".to_string(), vec![vec![1, 10]]);
        let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
        source.add_file("after.json".to_string(), vec![vec![2, 20]]);

        let snapshot = source
            .start_snapshot(&fence, None, None)
            .await
            .unwrap()
            .next()
            .unwrap();
        let (data, weights) = split_weight_column(&snapshot.batch).unwrap();
        let ids = data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((ids.value(0), weights), (1_i64, vec![1]));
        assert_eq!(
            source
                .poll_delta(fence.live, 1024, 10, None)
                .await
                .unwrap()
                .batches[0]
                .num_rows(),
            1,
            "the post-fence file must be delivered only as a live delta"
        );
    }

    #[tokio::test]
    async fn bounded_snapshot_resumes_from_the_committed_chunk_cursor() {
        let mut source = S3Source::new(ConnectorId(204), test_schema());
        source.add_file(
            "input.json".to_string(),
            vec![vec![1, 10], vec![2, 20], vec![3, 30]],
        );
        let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
        let mut snapshot = source
            .start_snapshot_bounded(&fence, None, None, 2)
            .await
            .unwrap();
        let first = snapshot.next().unwrap();
        assert_eq!(snapshot.by_ref().count(), 0);
        let (first_data, first_weights) = split_weight_column(&first.batch).unwrap();
        assert_eq!(
            (
                first_data
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                first_weights,
                first.resume_offset.clone(),
                snapshot.remaining_rows(),
            ),
            (
                &[1_i64, 2][..],
                vec![1_i64, 1],
                OffsetToken::new(serde_json::to_vec(&2_usize).unwrap()),
                1,
            )
        );

        let resumed = source
            .start_snapshot_bounded(&fence, Some(first.resume_offset.clone()), None, 2)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        let (resumed_data, resumed_weights) = split_weight_column(&resumed[0].batch).unwrap();
        assert_eq!(
            (
                resumed_data
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                resumed_weights,
                resumed[0].resume_offset.clone(),
            ),
            (&[3_i64][..], vec![1_i64], fence.snapshot)
        );
    }

    #[tokio::test]
    async fn manifest_live_cursor_delivers_a_later_object_that_sorts_before_the_fence() {
        let mut source = S3Source::new(ConnectorId(205), test_schema());
        source.add_file("z-before.json".to_string(), vec![vec![1, 10]]);
        let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
        source.add_file("a-after.json".to_string(), vec![vec![2, 20]]);

        let delta = source.poll_delta(fence.live, 1024, 10, None).await.unwrap();
        let (data, weights) = split_weight_column(&delta.batches[0]).unwrap();
        assert_eq!(
            (
                data.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                weights,
                delta.new_offset,
            ),
            (
                &[2_i64][..],
                vec![1_i64],
                OffsetToken::new(
                    serde_json::to_vec(&vec![
                        ("a-after.json".to_string(), 1_usize),
                        ("z-before.json".to_string(), 1_usize),
                    ])
                    .unwrap(),
                ),
            )
        );
    }

    #[tokio::test]
    async fn manifest_live_cursor_rejects_an_oversize_record_without_advancing() {
        let mut source = S3Source::new(ConnectorId(206), test_schema());
        source.add_file("before.json".to_string(), vec![vec![1, 10]]);
        let fence = source.capture_snapshot_delta_fence(None).await.unwrap();
        source.add_file("after.json".to_string(), vec![vec![2, 20]]);

        let error = source
            .poll_delta(fence.live.clone(), 1, 1, None)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "RS-4004: source poll failed: S3 record exceeds max_bytes=1; next_steps: increase BACKFILL_LIVE_DELTA_MAX_BYTES"
        );
        let delta = source.poll_delta(fence.live, 1024, 1, None).await.unwrap();
        let (data, weights) = split_weight_column(&delta.batches[0]).unwrap();
        assert_eq!(
            (
                data.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                weights,
            ),
            (&[2_i64][..], vec![1_i64])
        );
    }
}

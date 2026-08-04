//! Kafka source connector supporting multi-partition consumer groups (§13.3).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::{Epoch, EventTimeWatermark};

use crate::source_connector::{PollDeltaResult, SnapshotStream, SourceConnector, SourceError};
use crate::source_epoch::OffsetToken;

/// Buffer bound declaration: Kafka source buffer limit.
pub const KAFKA_SOURCE_BUFFER_LIMIT: usize = 50_000;

#[derive(Debug, Clone)]
pub struct KafkaRecord {
    pub offset: u64,
    pub timestamp: i64,
    pub values: Vec<i64>,
    pub weight: i64,
}

#[derive(Debug, Clone)]
pub struct KafkaPartition {
    pub partition_id: u64,
    pub records: Vec<KafkaRecord>,
    pub watermark: i64,
}

impl KafkaPartition {
    pub fn new(partition_id: u64) -> Self {
        Self {
            partition_id,
            records: Vec::new(),
            watermark: i64::MIN,
        }
    }

    pub fn poll_from(&self, start_offset: u64, limit: usize) -> Vec<KafkaRecord> {
        let mut results = Vec::new();
        for rec in &self.records {
            if rec.offset >= start_offset {
                results.push(rec.clone());
                if results.len() >= limit {
                    break;
                }
            }
        }
        results
    }
}

/// A mock `KafkaSource` implementing `SourceConnector`.
pub struct KafkaSource {
    _connector_id: ConnectorId,
    schema: SchemaRef,
    partitions: BTreeMap<u64, KafkaPartition>,
    paused: bool,
    last_committed: Option<(Epoch, OffsetToken)>,
}

impl KafkaSource {
    pub fn new(connector_id: ConnectorId, schema: SchemaRef, partition_ids: &[u64]) -> Self {
        let mut partitions = BTreeMap::new();
        for &pid in partition_ids {
            partitions.insert(pid, KafkaPartition::new(pid));
        }
        Self {
            _connector_id: connector_id,
            schema,
            partitions,
            paused: false,
            last_committed: None,
        }
    }

    /// Add a record to the given partition.
    pub fn add_record(&mut self, partition_id: u64, timestamp: i64, values: Vec<i64>) {
        let partition = self
            .partitions
            .entry(partition_id)
            .or_insert_with(|| KafkaPartition::new(partition_id));
        let next_offset = partition.records.len() as u64;
        partition.records.push(KafkaRecord {
            offset: next_offset,
            timestamp,
            values,
            weight: 1,
        });
    }

    /// Direct skew injection helper to explicitly set partition watermarks.
    pub fn set_partition_watermark(&mut self, partition_id: u64, watermark: i64) {
        if let Some(partition) = self.partitions.get_mut(&partition_id) {
            partition.watermark = watermark;
        }
    }

    /// Retrieve the partition's current offset from serialized OffsetToken.
    pub fn get_partition_offset(&self, token: &OffsetToken, partition_id: u64) -> Option<u64> {
        if token.as_bytes().is_empty() {
            return Some(0);
        }
        let map: BTreeMap<u64, u64> = serde_json::from_slice(token.as_bytes()).ok()?;
        Some(map.get(&partition_id).copied().unwrap_or(0))
    }

    /// Current global watermark is the minimum watermark across all partitions.
    fn current_global_watermark(&self) -> Option<EventTimeWatermark> {
        if self.partitions.is_empty() {
            return None;
        }
        let min_wm = self
            .partitions
            .values()
            .map(|p| p.watermark)
            .min()
            .unwrap_or(i64::MIN);
        if min_wm == i64::MIN {
            None
        } else {
            Some(min_wm as u64)
        }
    }

    /// Build a single `RecordBatch` from polled records.
    fn build_batch(&self, records: &[KafkaRecord]) -> Result<Vec<RecordBatch>, SourceError> {
        use arrow::array::Int64Array;
        use rockstream_types::arrow_batch::append_weight_column;

        let num_rows = records.len();
        let num_cols = self.schema.fields().len();

        let mut columns: Vec<Vec<i64>> = vec![vec![0; num_rows]; num_cols];
        let mut weights = vec![0; num_rows];

        for (r_idx, rec) in records.iter().enumerate() {
            weights[r_idx] = rec.weight;
            for (c_idx, col) in columns.iter_mut().enumerate().take(num_cols) {
                col[r_idx] = rec.values.get(c_idx).copied().unwrap_or(0);
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

impl SourceConnector for KafkaSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(self.schema.clone())
    }

    fn start_snapshot(
        &mut self,
        _frontier: Epoch,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        // Mock Kafka source starts with an empty snapshot stream by default
        Ok(SnapshotStream::new(vec![]))
    }

    fn poll_delta(
        &mut self,
        after: OffsetToken,
        _max_bytes: usize,
        credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        if self.paused || credits_available == 0 {
            return Ok(PollDeltaResult {
                batches: vec![],
                new_offset: after,
                watermark: self.current_global_watermark(),
            });
        }

        let mut current_offsets: BTreeMap<u64, u64> = if after.as_bytes().is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_slice(after.as_bytes()).map_err(|e| SourceError::PollDeltaFailed {
                reason: format!("failed to deserialize offset token: {e}"),
            })?
        };

        let mut polled_records = Vec::new();
        let mut credits_left = credits_available;

        for (&part_id, partition) in &mut self.partitions {
            if credits_left == 0 {
                break;
            }
            let start_offset = current_offsets.get(&part_id).copied().unwrap_or(0);
            let part_records = partition.poll_from(start_offset, credits_left);
            if !part_records.is_empty() {
                let next_offset = start_offset + part_records.len() as u64;
                current_offsets.insert(part_id, next_offset);
                credits_left -= part_records.len();

                // Watermark advances to the maximum of polled timestamps
                let max_ts = part_records.iter().map(|r| r.timestamp).max().unwrap();
                partition.watermark = partition.watermark.max(max_ts);

                polled_records.extend(part_records);
            }
        }

        let batches = if polled_records.is_empty() {
            Vec::new()
        } else {
            self.build_batch(&polled_records)?
        };

        let new_offset_bytes =
            serde_json::to_vec(&current_offsets).map_err(|e| SourceError::PollDeltaFailed {
                reason: format!("failed to serialize offset token: {e}"),
            })?;

        Ok(PollDeltaResult {
            batches,
            new_offset: OffsetToken::new(new_offset_bytes),
            watermark: self.current_global_watermark(),
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
            Field::new("t", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    #[test]
    fn test_kafka_source_basic() {
        let schema = test_schema();
        let mut source = KafkaSource::new(ConnectorId(101), schema, &[0, 1]);

        // Add records to partitions
        source.add_record(0, 100, vec![100, 10]);
        source.add_record(0, 200, vec![200, 20]);
        source.add_record(1, 150, vec![150, 15]);

        // Poll with 1 credit limit (respecting backpressure / credit limit)
        let token_start = OffsetToken::new(vec![]);
        let res1 = source
            .poll_delta(token_start.clone(), 1024, 1, None)
            .unwrap();
        assert_eq!(res1.batches.len(), 1);
        let (data1, weights1) = split_weight_column(&res1.batches[0]).unwrap();
        assert_eq!(data1.num_rows(), 1);
        assert_eq!(weights1, vec![1]);

        // Verify partition 0 offset advanced, partition 1 is 0
        let p0_off = source.get_partition_offset(&res1.new_offset, 0).unwrap();
        let p1_off = source.get_partition_offset(&res1.new_offset, 1).unwrap();
        assert_eq!(p0_off, 1);
        assert_eq!(p1_off, 0);

        // Watermark should be None because partition 1 hasn't polled anything (watermark is i64::MIN)
        assert!(res1.watermark.is_none());

        // Poll remaining records
        let res2 = source
            .poll_delta(res1.new_offset.clone(), 1024, 10, None)
            .unwrap();
        assert_eq!(res2.batches.len(), 1);
        let (data2, weights2) = split_weight_column(&res2.batches[0]).unwrap();
        assert_eq!(data2.num_rows(), 2); // 1 from p0, 1 from p1
        assert_eq!(weights2, vec![1, 1]);

        // Verify both partitions advanced
        let p0_off2 = source.get_partition_offset(&res2.new_offset, 0).unwrap();
        let p1_off2 = source.get_partition_offset(&res2.new_offset, 1).unwrap();
        assert_eq!(p0_off2, 2);
        assert_eq!(p1_off2, 1);

        // Watermark should now be the minimum of watermarks of partitions:
        // p0 watermark is 200 (max of 100, 200)
        // p1 watermark is 150 (max of 150)
        // min(200, 150) = 150
        assert_eq!(res2.watermark, Some(150));

        // Test offset commit
        source.commit_offset(5, res2.new_offset.clone()).unwrap();
        let (epoch, commit_tok) = source.last_committed().unwrap();
        assert_eq!(epoch, 5);
        assert_eq!(commit_tok, res2.new_offset);
    }
}

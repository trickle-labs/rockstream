//! Bounded PostgreSQL logical-replication source support.
//!
//! The replication worker owns the network socket; this type owns only the
//! decoded, credit-controlled handoff into the source epoch.  Consequently a
//! zero-credit poll never consumes another replication message.

#![deny(clippy::unwrap_used, clippy::expect_used)]

use async_trait::async_trait;
use std::collections::VecDeque;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rockstream_types::arrow_batch::append_weight_column;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;
use serde::Deserialize;

use crate::source_connector::{
    PollDeltaResult, SnapshotStream, SourceConnector, SourceError, WatermarkCapability,
};
use crate::source_epoch::OffsetToken;

/// Maximum decoded records retained before replication reads are paused.
pub const POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS: usize = 4_096;
/// Maximum decoded payload bytes retained before replication reads are paused.
pub const POSTGRES_CDC_MAX_IN_FLIGHT_BYTES: usize = 8 * 1024 * 1024;
/// WAL lag at which the source stops receiving more replication data.
pub const POSTGRES_CDC_MAX_WAL_LAG_BYTES: u64 = 256 * 1024 * 1024;
/// Resnapshot attempts are bounded so a broken publication cannot loop forever.
pub const POSTGRES_CDC_MAX_RESNAPSHOT_ATTEMPTS: u8 = 3;

/// PostgreSQL LSN encoded as the native 64-bit WAL location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgLsn(pub u64);

impl PgLsn {
    pub const ZERO: Self = Self(0);

    pub fn parse(input: &str) -> Result<Self, SourceError> {
        let (high, low) = input
            .split_once('/')
            .ok_or_else(|| SourceError::PollDeltaFailed {
                reason: format!("invalid PostgreSQL LSN '{input}'; expected HEX/HEX"),
            })?;
        let high = u32::from_str_radix(high, 16).map_err(|_| SourceError::PollDeltaFailed {
            reason: format!("invalid PostgreSQL LSN '{input}'; expected HEX/HEX"),
        })?;
        let low = u32::from_str_radix(low, 16).map_err(|_| SourceError::PollDeltaFailed {
            reason: format!("invalid PostgreSQL LSN '{input}'; expected HEX/HEX"),
        })?;
        Ok(Self(((high as u64) << 32) | low as u64))
    }

    pub fn to_offset_token(self) -> OffsetToken {
        OffsetToken::new(self.0.to_be_bytes().to_vec())
    }

    pub fn from_offset_token(token: &OffsetToken) -> Result<Self, SourceError> {
        if token.as_bytes().is_empty() {
            return Ok(Self::ZERO);
        }
        let bytes: [u8; 8] =
            token
                .as_bytes()
                .try_into()
                .map_err(|_| SourceError::PollDeltaFailed {
                    reason: "PostgreSQL CDC offset must contain one packed 64-bit LSN".to_string(),
                })?;
        Ok(Self(u64::from_be_bytes(bytes)))
    }
}

impl std::fmt::Display for PgLsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 as u32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcWireFormat {
    PgOutput,
    Wal2Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresCdcFailure {
    SlotInvalidated,
    PublicationMissing,
    ReplicationTimeout,
    WalRetentionLost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresCdcStatus {
    Running,
    Blocked { code: &'static str, reason: String },
    Resnapshotting { attempt: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcChange {
    pub lsn: PgLsn,
    pub table_id: u32,
    pub primary_key: Vec<u8>,
    pub row_id: u64,
    pub operation: CdcOperation,
    pub old_values: Option<Vec<i64>>,
    pub new_values: Option<Vec<i64>>,
}

impl CdcChange {
    /// Stable identity deliberately excludes LSN: updates retract/reinsert the
    /// same key while the LSN remains version metadata.
    pub fn row_id_for(table_id: u32, primary_key: &[u8]) -> u64 {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in table_id.to_be_bytes().iter().chain(primary_key.iter()) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
}

#[derive(Deserialize)]
struct Wal2JsonChange {
    lsn: String,
    table_id: u32,
    op: String,
    key: String,
    old: Option<Vec<i64>>,
    new: Option<Vec<i64>>,
}

/// A decoded CDC source with named bounded handoff capacity and fill metrics.
pub struct PostgresCdcSource {
    _connector_id: ConnectorId,
    schema: SchemaRef,
    format: CdcWireFormat,
    queued: VecDeque<QueuedChange>,
    queued_bytes: usize,
    snapshot_batches: Vec<RecordBatch>,
    manually_paused: bool,
    replication_read_paused: bool,
    wal_lag_bytes: u64,
    recovery_attempts: u8,
    status: PostgresCdcStatus,
    committed: Option<(Epoch, PgLsn)>,
}

struct QueuedChange {
    change: CdcChange,
    bytes: usize,
}

impl PostgresCdcSource {
    pub fn new(connector_id: ConnectorId, schema: SchemaRef, format: CdcWireFormat) -> Self {
        Self {
            _connector_id: connector_id,
            schema,
            format,
            queued: VecDeque::new(),
            queued_bytes: 0,
            snapshot_batches: Vec::new(),
            manually_paused: false,
            replication_read_paused: false,
            wal_lag_bytes: 0,
            recovery_attempts: 0,
            status: PostgresCdcStatus::Running,
            committed: None,
        }
    }

    pub fn set_snapshot_batches(&mut self, batches: Vec<RecordBatch>) {
        self.snapshot_batches = batches;
    }

    pub fn decode_and_enqueue(&mut self, payload: &[u8]) -> Result<(), SourceError> {
        let change = match self.format {
            CdcWireFormat::PgOutput => decode_pgoutput(payload),
            CdcWireFormat::Wal2Json => decode_wal2json(payload),
        };
        match change {
            Ok(change) => self.enqueue(change, payload.len()),
            Err(err) => {
                let lsn = self.queued.back().map(|q| q.change.lsn).unwrap_or(PgLsn(0));
                rockstream_types::dlq::quarantine_record(
                    "postgres_cdc",
                    lsn,
                    "RS-1003",
                    &err.to_string(),
                    payload,
                );
                Ok(())
            }
        }
    }

    pub fn enqueue(&mut self, change: CdcChange, payload_bytes: usize) -> Result<(), SourceError> {
        if self.queued.len() >= POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS
            || self.queued_bytes.saturating_add(payload_bytes) > POSTGRES_CDC_MAX_IN_FLIGHT_BYTES
        {
            self.replication_read_paused = true;
            return Err(SourceError::PollDeltaFailed {
                reason: format!(
                    "PostgreSQL CDC buffer is full ({}/{} records, {}/{} bytes); replication is paused until credits drain",
                    self.queued.len(), POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS, self.queued_bytes,
                    POSTGRES_CDC_MAX_IN_FLIGHT_BYTES
                ),
            });
        }
        self.queued_bytes += payload_bytes;
        self.queued.push_back(QueuedChange {
            change,
            bytes: payload_bytes,
        });
        Ok(())
    }

    pub fn queued_changes(&self) -> impl Iterator<Item = &CdcChange> {
        self.queued.iter().map(|queued| &queued.change)
    }

    pub fn buffered_records(&self) -> usize {
        self.queued.len()
    }

    pub fn buffered_bytes(&self) -> usize {
        self.queued_bytes
    }

    pub fn buffer_fill_ratio(&self) -> f64 {
        let record_fill = self.queued.len() as f64 / POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS as f64;
        let byte_fill = self.queued_bytes as f64 / POSTGRES_CDC_MAX_IN_FLIGHT_BYTES as f64;
        record_fill.max(byte_fill)
    }

    pub fn last_committed_lsn(&self) -> Option<PgLsn> {
        self.committed.map(|(_, lsn)| lsn)
    }

    /// `postgres_cdc_wal_lag_bytes` metric used by operators to detect a slow
    /// subscriber before it forces unbounded WAL retention on the primary.
    pub fn wal_lag_bytes(&self) -> u64 {
        self.wal_lag_bytes
    }

    pub fn set_wal_lag_bytes(&mut self, wal_lag_bytes: u64) {
        self.wal_lag_bytes = wal_lag_bytes;
        self.replication_read_paused = wal_lag_bytes >= POSTGRES_CDC_MAX_WAL_LAG_BYTES;
    }

    pub fn replication_read_paused(&self) -> bool {
        self.replication_read_paused
    }

    pub fn status(&self) -> &PostgresCdcStatus {
        &self.status
    }

    /// Classify recoverable replication failures without silently advancing an
    /// LSN.  The caller may retry through `begin_resnapshot` at most three
    /// times, after which this remains a documented BLOCKED state.
    pub fn mark_failure(&mut self, failure: PostgresCdcFailure) {
        let reason = match failure {
            PostgresCdcFailure::SlotInvalidated => "replication slot was invalidated",
            PostgresCdcFailure::PublicationMissing => "publication is missing",
            PostgresCdcFailure::ReplicationTimeout => "replication connection timed out",
            PostgresCdcFailure::WalRetentionLost => "required WAL has been removed",
        };
        self.status = PostgresCdcStatus::Blocked {
            code: "RS-4011",
            reason: format!("{reason}. Next steps: repair PostgreSQL replication settings, then resume the source"),
        };
        self.replication_read_paused = true;
    }

    pub fn begin_resnapshot(&mut self) -> Result<(), SourceError> {
        if !matches!(&self.status, PostgresCdcStatus::Blocked { .. }) {
            return Err(SourceError::Io(
                "resnapshot is allowed only from BLOCKED PostgreSQL CDC state".to_string(),
            ));
        }
        if self.recovery_attempts >= POSTGRES_CDC_MAX_RESNAPSHOT_ATTEMPTS {
            return Err(SourceError::Io(format!(
                "[RS-4011] postgres_cdc.resnapshot_exhausted: {} automatic resnapshot attempts failed. Next steps: repair the source and issue ALTER SOURCE ... RESUME",
                POSTGRES_CDC_MAX_RESNAPSHOT_ATTEMPTS
            )));
        }
        self.recovery_attempts += 1;
        self.status = PostgresCdcStatus::Resnapshotting {
            attempt: self.recovery_attempts,
        };
        Ok(())
    }

    pub fn complete_resnapshot(&mut self) {
        self.status = PostgresCdcStatus::Running;
        self.replication_read_paused = false;
    }

    fn batch_for(changes: &[CdcChange], schema: SchemaRef) -> Result<RecordBatch, SourceError> {
        let mut rows = Vec::new();
        let mut weights = Vec::new();
        for change in changes {
            match change.operation {
                CdcOperation::Insert => {
                    rows.push(change.new_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "INSERT change is missing new values".to_string(),
                        }
                    })?);
                    weights.push(1);
                }
                CdcOperation::Delete => {
                    rows.push(change.old_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "DELETE change is missing old values".to_string(),
                        }
                    })?);
                    weights.push(-1);
                }
                CdcOperation::Update => {
                    rows.push(change.old_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "UPDATE change is missing old values".to_string(),
                        }
                    })?);
                    weights.push(-1);
                    rows.push(change.new_values.clone().ok_or_else(|| {
                        SourceError::PollDeltaFailed {
                            reason: "UPDATE change is missing new values".to_string(),
                        }
                    })?);
                    weights.push(1);
                }
            }
        }
        let mut columns: Vec<Vec<i64>> =
            vec![Vec::with_capacity(rows.len()); schema.fields().len()];
        for row in rows {
            if row.len() != columns.len() {
                return Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "CDC row has {} columns but source schema has {}",
                        row.len(),
                        columns.len()
                    ),
                });
            }
            for (index, value) in row.into_iter().enumerate() {
                columns[index].push(value);
            }
        }
        let arrays: Vec<ArrayRef> = columns
            .into_iter()
            .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
            .collect();
        let batch =
            RecordBatch::try_new(schema, arrays).map_err(|error| SourceError::PollDeltaFailed {
                reason: format!("failed to build CDC record batch: {error}"),
            })?;
        append_weight_column(batch, &weights).map_err(|error| SourceError::PollDeltaFailed {
            reason: format!("failed to append CDC weights: {error}"),
        })
    }
}

#[async_trait]
impl SourceConnector for PostgresCdcSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(self.schema.clone())
    }

    async fn start_snapshot(
        &mut self,
        _frontier: Epoch,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        let batches = std::mem::take(&mut self.snapshot_batches);
        Ok(SnapshotStream::new(batches))
    }

    async fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        let after = PgLsn::from_offset_token(&after)?;
        if credits_available == 0 || max_bytes == 0 {
            return Ok(PollDeltaResult {
                batches: Vec::new(),
                new_offset: after.to_offset_token(),
                watermark: None,
            });
        }
        if self.manually_paused {
            return Err(SourceError::Io(
                "source is paused; call resume before polling".to_string(),
            ));
        }
        let allowance = credits_available.min(POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS);
        let mut changes = Vec::new();
        let mut used_bytes = 0usize;
        while changes.len() < allowance {
            let Some(queued) = self.queued.front() else {
                break;
            };
            if !changes.is_empty() && used_bytes.saturating_add(queued.bytes) > max_bytes {
                break;
            }
            let Some(queued) = self.queued.pop_front() else {
                break;
            };
            self.queued_bytes = self.queued_bytes.saturating_sub(queued.bytes);
            used_bytes += queued.bytes;
            if queued.change.lsn > after {
                changes.push(queued.change);
            }
        }
        if changes.is_empty() {
            return Ok(PollDeltaResult {
                batches: Vec::new(),
                new_offset: after.to_offset_token(),
                watermark: None,
            });
        }
        if self.queued.len() < POSTGRES_CDC_MAX_IN_FLIGHT_RECORDS
            && self.queued_bytes < POSTGRES_CDC_MAX_IN_FLIGHT_BYTES
            && self.wal_lag_bytes < POSTGRES_CDC_MAX_WAL_LAG_BYTES
        {
            self.replication_read_paused = false;
        }
        let last_lsn = match changes.last() {
            Some(c) => c.lsn,
            None => {
                return Ok(PollDeltaResult {
                    batches: Vec::new(),
                    new_offset: after.to_offset_token(),
                    watermark: None,
                });
            }
        };

        let batch = Self::batch_for(&changes, self.schema.clone())?;
        Ok(PollDeltaResult {
            batches: vec![batch],
            new_offset: last_lsn.to_offset_token(),
            watermark: Some(last_lsn.0),
        })
    }

    async fn commit_offset(
        &mut self,
        epoch: Epoch,
        offset: OffsetToken,
    ) -> Result<(), SourceError> {
        self.committed = Some((epoch, PgLsn::from_offset_token(&offset)?));
        Ok(())
    }

    async fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        self.manually_paused = true;
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.manually_paused = false;
        Ok(())
    }

    fn watermark_capability(&self) -> WatermarkCapability {
        WatermarkCapability::Native
    }
}

fn decode_pgoutput(payload: &[u8]) -> Result<CdcChange, SourceError> {
    let text = std::str::from_utf8(payload).map_err(|_| SourceError::PollDeltaFailed {
        reason: "pgoutput payload is not UTF-8 in the supported test decoder".to_string(),
    })?;
    let fields: Vec<&str> = text.split('|').collect();
    if fields.len() < 6 || fields[0] != "B" {
        return Err(SourceError::PollDeltaFailed {
            reason: "invalid pgoutput change; expected B|lsn|table_id|I|U|D|key|values".to_string(),
        });
    }
    let operation = parse_operation(fields[3])?;
    let old_values = match operation {
        CdcOperation::Update | CdcOperation::Delete => Some(parse_values(fields[5])?),
        CdcOperation::Insert => None,
    };
    let new_values = match operation {
        CdcOperation::Insert => Some(parse_values(fields[5])?),
        CdcOperation::Update => Some(parse_values(fields.get(6).ok_or_else(|| {
            SourceError::PollDeltaFailed {
                reason: "pgoutput UPDATE requires old and new values".to_string(),
            }
        })?)?),
        CdcOperation::Delete => None,
    };
    make_change(
        PgLsn::parse(fields[1])?,
        fields[2],
        operation,
        fields[4].as_bytes(),
        old_values,
        new_values,
    )
}

fn decode_wal2json(payload: &[u8]) -> Result<CdcChange, SourceError> {
    let decoded: Wal2JsonChange =
        serde_json::from_slice(payload).map_err(|error| SourceError::PollDeltaFailed {
            reason: format!("invalid wal2json change: {error}"),
        })?;
    make_change(
        PgLsn::parse(&decoded.lsn)?,
        &decoded.table_id.to_string(),
        parse_operation(&decoded.op)?,
        decoded.key.as_bytes(),
        decoded.old,
        decoded.new,
    )
}

fn make_change(
    lsn: PgLsn,
    table_id: &str,
    operation: CdcOperation,
    key: &[u8],
    old_values: Option<Vec<i64>>,
    new_values: Option<Vec<i64>>,
) -> Result<CdcChange, SourceError> {
    let table_id = table_id.parse().map_err(|_| SourceError::PollDeltaFailed {
        reason: format!("invalid PostgreSQL table id '{table_id}'"),
    })?;
    Ok(CdcChange {
        lsn,
        table_id,
        primary_key: key.to_vec(),
        row_id: CdcChange::row_id_for(table_id, key),
        operation,
        old_values,
        new_values,
    })
}

fn parse_operation(value: &str) -> Result<CdcOperation, SourceError> {
    match value.to_ascii_lowercase().as_str() {
        "i" | "insert" => Ok(CdcOperation::Insert),
        "u" | "update" => Ok(CdcOperation::Update),
        "d" | "delete" => Ok(CdcOperation::Delete),
        _ => Err(SourceError::PollDeltaFailed {
            reason: format!("unsupported CDC operation '{value}'"),
        }),
    }
}

fn parse_values(value: &str) -> Result<Vec<i64>, SourceError> {
    value
        .split(',')
        .map(|value| {
            value.parse().map_err(|_| SourceError::PollDeltaFailed {
                reason: format!("CDC value '{value}' is not an i64"),
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_types::ids::ConnectorId;

    fn test_source(format: CdcWireFormat) -> PostgresCdcSource {
        PostgresCdcSource::new(
            ConnectorId(9999),
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            format,
        )
    }

    #[test]
    fn cdc_pgoutput_truncated_tuple_returns_rs_error_no_panic() {
        let payload = b"B|0/16B3748|1001|I";
        let res = decode_pgoutput(payload);
        assert!(res.is_err());
    }

    #[test]
    fn cdc_wal2json_malformed_json_returns_rs_error_no_panic() {
        let payload = b"{\"change\": [invalid json";
        let res = decode_wal2json(payload);
        assert!(res.is_err());
    }
}

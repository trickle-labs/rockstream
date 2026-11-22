//! Checkpointed snapshot and change delivery from a production `ViewSinkOp` log.

use std::sync::Arc;

use rockstream_connectors::{KafkaSink, SinkConnector, SinkError};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::{
    ids::OperatorId,
    sink::{RecoveryAction, SinkState},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::sink::{materialize_view_state, read_view_output, ColumnValue};

/// Default maximum JSON envelope size per Kafka transaction (900 KiB,
/// comfortably under Kafka's 1 MiB default `message.max.bytes`).
pub const DEFAULT_MAX_ENVELOPE_BYTES: usize = 900 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    view: u64,
    columns: usize,
    topic: String,
    snapshot_complete: bool,
    view_epoch: Option<u64>,
    transaction: u64,
    pending: Option<SinkState>,
    #[serde(default)]
    snapshot_chunk_offset: usize,
    #[serde(default)]
    snapshot_epoch: Option<u64>,
    #[serde(default)]
    change_chunk_offset: usize,
}

/// One owner exports one append-only, monotonically committed view log.
/// Keep the log until delivery succeeds. Use a dedicated topic and connector ID;
/// recreating the view or changing its schema requires a new connector ID.
pub struct KafkaViewSink {
    sink: KafkaSink,
    db: Arc<ShardDb>,
    view: OperatorId,
    columns: usize,
    max_envelope_bytes: usize,
}

impl KafkaViewSink {
    pub fn new(sink: KafkaSink, db: Arc<ShardDb>, view: OperatorId, columns: usize) -> Self {
        Self {
            sink,
            db,
            view,
            columns,
            max_envelope_bytes: DEFAULT_MAX_ENVELOPE_BYTES,
        }
    }

    /// Override the per-transaction envelope byte ceiling (useful for tests
    /// and brokers with custom `message.max.bytes`).
    pub fn with_max_envelope_bytes(mut self, max_envelope_bytes: usize) -> Self {
        self.max_envelope_bytes = max_envelope_bytes.max(256);
        self
    }

    fn key(&self) -> Vec<u8> {
        format!("\x06kafka-view/{}", self.sink.connector_id().0).into_bytes()
    }

    async fn progress(&self) -> Result<Progress, SinkError> {
        let stored = self.db.get(&self.key()).await.map_err(io)?;
        let progress = match stored {
            Some(bytes) => serde_json::from_slice::<Progress>(&bytes).map_err(io)?,
            None => Progress {
                view: self.view.0,
                columns: self.columns,
                topic: self.sink.topic()?.to_owned(),
                snapshot_complete: false,
                view_epoch: None,
                transaction: 0,
                pending: None,
                snapshot_chunk_offset: 0,
                snapshot_epoch: None,
                change_chunk_offset: 0,
            },
        };
        if progress.view != self.view.0
            || progress.columns != self.columns
            || progress.topic != self.sink.topic()?
        {
            return Err(io("Kafka view checkpoint identity mismatch"));
        }
        Ok(progress)
    }

    async fn persist(&self, progress: &Progress) -> Result<(), SinkError> {
        let mut batch = WriteBatch::new();
        batch.put(&self.key(), &serde_json::to_vec(progress).map_err(io)?);
        self.db.write_batch(batch).await.map_err(io)?;
        self.db.flush().await.map_err(io)
    }

    /// Stage the initial snapshot or the next nonempty view epoch and durably
    /// save both the full envelope and its cursor before external commit.
    /// A restart must reconnect the producer, fencing its old transaction.
    pub async fn stage_next(&mut self) -> Result<bool, SinkError> {
        let mut progress = self.progress().await?;
        if progress.pending.is_some() {
            return Err(io("Kafka view transaction requires recovery"));
        }
        let mut output = read_view_output(&self.db, self.view, self.columns)
            .await
            .map_err(io)?;
        let kind;
        let all_rows: Vec<(Vec<ColumnValue>, i64)>;
        let chunk_offset: usize;
        let envelope_view_epoch: Option<u64>;
        let is_snapshot = !progress.snapshot_complete;
        if is_snapshot {
            kind = "snapshot";
            let snap_epoch = if progress.snapshot_chunk_offset > 0 {
                progress.snapshot_epoch
            } else {
                let latest = output.last().map(|row| row.0);
                progress.snapshot_epoch = latest;
                latest
            };
            if let Some(limit_epoch) = snap_epoch {
                output.retain(|row| row.0 <= limit_epoch);
            }
            all_rows = materialize_view_state(output, &[])
                .into_values()
                .collect::<Vec<_>>();
            if all_rows.iter().any(|(_, weight)| *weight < 0) {
                return Err(io("Kafka snapshot contains a negative row multiplicity"));
            }
            chunk_offset = progress.snapshot_chunk_offset;
            envelope_view_epoch = snap_epoch;
        } else {
            output.retain(|row| progress.view_epoch.is_none_or(|epoch| row.0 > epoch));
            let Some(epoch) = output.first().map(|row| row.0) else {
                return Ok(false);
            };
            kind = "changes";
            all_rows = output
                .into_iter()
                .take_while(|row| row.0 == epoch)
                .map(|(_, _, row, weight)| (row, weight))
                .collect();
            chunk_offset = progress.change_chunk_offset;
            envelope_view_epoch = Some(epoch);
        }

        let total_rows = all_rows.len();
        let remaining = all_rows.get(chunk_offset..).unwrap_or(&[]);
        let metadata = EnvelopeMetadata {
            connector_id: self.sink.connector_id().0,
            view_id: self.view.0,
            transaction: progress.transaction,
            view_epoch: envelope_view_epoch,
            kind,
        };
        let (envelope, chunk_rows_len) = build_bounded_envelope(
            &metadata,
            self.max_envelope_bytes,
            remaining,
            chunk_offset,
            total_rows,
        )?;
        let next_offset = chunk_offset + chunk_rows_len;
        if is_snapshot {
            if next_offset >= total_rows {
                progress.snapshot_complete = true;
                progress.view_epoch = envelope_view_epoch;
                progress.snapshot_chunk_offset = 0;
                progress.snapshot_epoch = None;
            } else {
                progress.snapshot_chunk_offset = next_offset;
            }
        } else if next_offset >= total_rows {
            progress.view_epoch = envelope_view_epoch;
            progress.change_chunk_offset = 0;
        } else {
            progress.change_chunk_offset = next_offset;
        }

        progress.transaction = progress
            .transaction
            .checked_add(1)
            .ok_or_else(|| io("Kafka view transaction counter exhausted"))?;
        progress.pending = Some(
            self.sink
                .pre_commit_payload(progress.transaction - 1, chunk_rows_len, envelope)
                .await?,
        );
        self.persist(&progress).await?;
        Ok(true)
    }

    /// Deliver all pending snapshot chunks and committed view epochs until caught up.
    /// Returns the number of Kafka transactions committed.
    pub async fn drain_all(&mut self) -> Result<usize, SinkError> {
        let mut delivered = 0usize;
        while self.deliver_next().await? {
            delivered += 1;
        }
        Ok(delivered)
    }

    /// Recover a durable pending transaction, or deliver one new transaction.
    /// Returns false when the snapshot is complete and no new rows are present.
    pub async fn deliver_next(&mut self) -> Result<bool, SinkError> {
        let mut progress = self.progress().await?;
        if let Some(SinkState::PreCommitted { pending_handle, .. }) = &progress.pending {
            let epoch = progress
                .transaction
                .checked_sub(1)
                .ok_or_else(|| io("invalid Kafka view transaction cursor"))?;
            self.sink.set_cluster_committed(epoch);
            self.sink
                .recover(RecoveryAction::RerunCommit {
                    epoch,
                    profile: self.sink.idempotency_profile(),
                    pending_handle: pending_handle.clone(),
                })
                .await?;
        } else if progress.pending.is_some() {
            return Err(io("invalid Kafka view pending state"));
        } else {
            if !self.stage_next().await? {
                return Ok(false);
            }
            progress = self.progress().await?;
            let epoch = progress.transaction - 1;
            self.sink.set_cluster_committed(epoch);
            self.sink
                .commit(epoch, progress.pending.as_ref().unwrap())
                .await?;
        }
        #[cfg(feature = "test-hooks")]
        {
            let committed_epoch = progress
                .transaction
                .checked_sub(1)
                .ok_or_else(|| io("invalid Kafka view transaction cursor"))?;
            pause_after_commit_for_test(committed_epoch).await?;
        }
        progress.pending = None;
        self.persist(&progress).await?;
        Ok(true)
    }
}

#[cfg(feature = "test-hooks")]
async fn pause_after_commit_for_test(epoch: u64) -> Result<(), SinkError> {
    let Some(root) = std::env::var_os("ROCKSTREAM_TEST_SINK_CURSOR_GAP_ROOT") else {
        return Ok(());
    };
    let selected_epoch = std::env::var("ROCKSTREAM_TEST_SINK_CURSOR_GAP_EPOCH")
        .map_err(io)?
        .parse::<u64>()
        .map_err(io)?;
    if epoch != selected_epoch {
        return Ok(());
    }
    let root = std::path::PathBuf::from(root);
    std::fs::create_dir_all(&root).map_err(io)?;
    let committed_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io)?
        .as_millis() as u64;
    let ready =
        json!({"epoch":epoch,"process_id":std::process::id(),"committed_at_ms":committed_at_ms});
    let marker = root.join("sink-transaction-committed.json");
    let temporary = root.join("sink-transaction-committed.json.tmp");
    std::fs::write(&temporary, serde_json::to_vec(&ready).map_err(io)?).map_err(io)?;
    std::fs::rename(temporary, marker).map_err(io)?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(180);
    while !root.join("release-sink-cursor").exists() {
        if tokio::time::Instant::now() >= deadline {
            return Err(io("sink cursor gap test hook timed out"));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Ok(())
}

struct EnvelopeMetadata<'a> {
    connector_id: u64,
    view_id: u64,
    transaction: u64,
    view_epoch: Option<u64>,
    kind: &'a str,
}

fn build_bounded_envelope(
    metadata: &EnvelopeMetadata<'_>,
    max_envelope_bytes: usize,
    remaining: &[(Vec<ColumnValue>, i64)],
    chunk_offset: usize,
    total_rows: usize,
) -> Result<(String, usize), SinkError> {
    let format_envelope = |rows: &[(Vec<ColumnValue>, i64)], is_chunked: bool| -> String {
        let encoded_rows = rows
            .iter()
            .map(|(row, weight)| {
                json!({
                    "values": row.iter().map(column).collect::<Vec<_>>(),
                    "weight": weight,
                })
            })
            .collect::<Vec<_>>();
        if !is_chunked {
            json!({
                "version": 1,
                "connector_id": metadata.connector_id,
                "view": metadata.view_id,
                "epoch": metadata.transaction,
                "view_epoch": metadata.view_epoch,
                "kind": metadata.kind,
                "rows": encoded_rows,
            })
            .to_string()
        } else {
            let chunk_end = chunk_offset + rows.len();
            json!({
                "version": 1,
                "connector_id": metadata.connector_id,
                "view": metadata.view_id,
                "epoch": metadata.transaction,
                "view_epoch": metadata.view_epoch,
                "kind": metadata.kind,
                "chunk_offset": chunk_offset,
                "chunk_end": chunk_end,
                "total_rows": total_rows,
                "chunk_complete": chunk_end >= total_rows,
                "rows": encoded_rows,
            })
            .to_string()
        }
    };

    if chunk_offset == 0 {
        let full = format_envelope(remaining, false);
        if full.len() <= max_envelope_bytes {
            return Ok((full, remaining.len()));
        }
    }

    if remaining.is_empty() {
        return Ok((format_envelope(&[], chunk_offset > 0), 0));
    }

    let mut lo = 1usize;
    let mut hi = remaining.len();
    let mut best: Option<(String, usize)> = None;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let candidate = format_envelope(&remaining[..mid], true);
        if candidate.len() <= max_envelope_bytes {
            best = Some((candidate, mid));
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }
    best.ok_or_else(|| {
        io(format!(
            "single Kafka row exceeds the {} byte envelope limit",
            max_envelope_bytes
        ))
    })
}

fn io(error: impl std::fmt::Display) -> SinkError {
    SinkError::Io(error.to_string())
}

fn column(value: &ColumnValue) -> Value {
    match value {
        ColumnValue::Null => json!("Null"),
        ColumnValue::Int64(value) => json!({"Int64": value}),
        ColumnValue::Utf8(value) => json!({"Utf8": value}),
        ColumnValue::Boolean(value) => json!({"Boolean": value}),
        ColumnValue::Float64(value) => json!({"Float64": format!("{:016x}", value.to_bits())}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_tags_preserve_null_text_and_float_bits() {
        let values = [
            ColumnValue::Null,
            ColumnValue::Int64(i64::MIN),
            ColumnValue::Utf8("a\n\"b".into()),
            ColumnValue::Boolean(false),
            ColumnValue::Float64(-0.0),
            ColumnValue::Float64(f64::from_bits(0x7ff8000000000001)),
        ];
        assert_eq!(
            values.iter().map(column).collect::<Vec<_>>(),
            vec![
                json!("Null"),
                json!({"Int64":-9223372036854775808_i64}),
                json!({"Utf8":"a\n\"b"}),
                json!({"Boolean":false}),
                json!({"Float64":"8000000000000000"}),
                json!({"Float64":"7ff8000000000001"}),
            ]
        );
    }

    #[test]
    fn chunked_snapshot_splits_deterministically_under_envelope_limit() {
        let rows: Vec<(Vec<ColumnValue>, i64)> = (0..12i64)
            .map(|i| {
                (
                    vec![
                        ColumnValue::Int64(i),
                        ColumnValue::Utf8(format!("payload-row-{i:03}")),
                    ],
                    1,
                )
            })
            .collect();

        let mut offset = 0usize;
        let mut chunks = Vec::new();
        let mut tx = 0u64;
        while offset < rows.len() {
            let metadata = EnvelopeMetadata {
                connector_id: 404,
                view_id: 404,
                transaction: tx,
                view_epoch: Some(1),
                kind: "snapshot",
            };
            let (env, count) = build_bounded_envelope(
                &metadata,
                420,
                &rows[offset..],
                offset,
                rows.len(),
            )
            .unwrap();
            assert!(
                env.len() <= 420,
                "chunk envelope {} exceeded 420 bytes",
                env.len()
            );
            assert!(count >= 1);
            let parsed: Value = serde_json::from_str(&env).unwrap();
            assert_eq!(parsed["kind"], json!("snapshot"));
            assert_eq!(parsed["chunk_offset"], json!(offset));
            assert_eq!(parsed["chunk_end"], json!(offset + count));
            assert_eq!(parsed["total_rows"], json!(rows.len()));
            assert_eq!(
                parsed["chunk_complete"],
                json!(offset + count == rows.len())
            );
            chunks.push((offset, count, parsed));
            offset += count;
            tx += 1;
        }
        assert!(
            chunks.len() >= 2,
            "expected multiple chunks, got {}",
            chunks.len()
        );
        let reassembled: Vec<i64> = chunks
            .iter()
            .flat_map(|(_, _, p)| {
                p["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r["values"][0]["Int64"].as_i64().unwrap())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(reassembled, (0..12i64).collect::<Vec<_>>());
    }
}

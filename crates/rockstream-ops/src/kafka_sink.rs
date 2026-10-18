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
}

/// One owner exports one append-only, monotonically committed view log.
/// Keep the log until delivery succeeds. Use a dedicated topic and connector ID;
/// recreating the view or changing its schema requires a new connector ID.
pub struct KafkaViewSink {
    sink: KafkaSink,
    db: Arc<ShardDb>,
    view: OperatorId,
    columns: usize,
}

impl KafkaViewSink {
    pub fn new(sink: KafkaSink, db: Arc<ShardDb>, view: OperatorId, columns: usize) -> Self {
        Self {
            sink,
            db,
            view,
            columns,
        }
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
        let rows;
        if !progress.snapshot_complete {
            kind = "snapshot";
            progress.view_epoch = output.last().map(|row| row.0);
            rows = materialize_view_state(output, &[])
                .into_values()
                .collect::<Vec<_>>();
            if rows.iter().any(|(_, weight)| *weight < 0) {
                return Err(io("Kafka snapshot contains a negative row multiplicity"));
            }
            progress.snapshot_complete = true;
        } else {
            output.retain(|row| progress.view_epoch.is_none_or(|epoch| row.0 > epoch));
            let Some(epoch) = output.first().map(|row| row.0) else {
                return Ok(false);
            };
            kind = "changes";
            progress.view_epoch = Some(epoch);
            rows = output
                .into_iter()
                .take_while(|row| row.0 == epoch)
                .map(|(_, _, row, weight)| (row, weight))
                .collect();
        }
        let envelope = json!({
            "version": 1,
            "connector_id": self.sink.connector_id().0,
            "view": self.view.0,
            "epoch": progress.transaction,
            "view_epoch": progress.view_epoch,
            "kind": kind,
            "rows": rows.iter().map(|(row, weight)| json!({
                "values": row.iter().map(column).collect::<Vec<_>>(), "weight": weight,
            })).collect::<Vec<_>>(),
        })
        .to_string();
        // shortcut: one bounded envelope per epoch, add chunked transactions for larger views.
        if envelope.len() > 900 * 1024 {
            return Err(io("Kafka row envelope exceeds the 900 KiB delivery limit"));
        }
        progress.transaction = progress
            .transaction
            .checked_add(1)
            .ok_or_else(|| io("Kafka view transaction counter exhausted"))?;
        progress.pending = Some(
            self.sink
                .pre_commit_payload(progress.transaction - 1, rows.len(), envelope)
                .await?,
        );
        self.persist(&progress).await?;
        Ok(true)
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
        progress.pending = None;
        self.persist(&progress).await?;
        Ok(true)
    }
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
}

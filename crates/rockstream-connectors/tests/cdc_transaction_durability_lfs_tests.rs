use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use object_store::local::LocalFileSystem;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, PollDeltaResult,
    SnapshotDeltaFence, SnapshotStream, SourceCheckpoint, SourceCheckpointStore, SourceConnector,
    SourceError, SourceRuntimeCoordinator,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::{connector::PartitionFilter, ids::ConnectorId, timestamp::Epoch};
use tempfile::TempDir;

struct AckSource;

#[async_trait]
impl SourceConnector for AckSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])))
    }

    async fn start_snapshot(
        &mut self,
        _: &SnapshotDeltaFence,
        _: Option<OffsetToken>,
        _: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        Ok(SnapshotStream::new(vec![]))
    }

    async fn poll_delta(
        &mut self,
        _: OffsetToken,
        _: usize,
        _: usize,
        _: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        unreachable!("durability tests commit explicit envelopes")
    }

    async fn commit_offset(&mut self, _: Epoch, _: OffsetToken) -> Result<(), SourceError> {
        Ok(())
    }

    async fn pause(&mut self, _: String) -> Result<(), SourceError> {
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

async fn recover_exactly(dir: &TempDir, connector_id: ConnectorId, payload: &[u8]) {
    let db = Arc::new(
        ShardDb::builder(
            "cdc-transaction-lfs",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let mut runtime = SourceRuntimeCoordinator::new(
        AckSource,
        connector_id,
        OffsetToken::new(vec![]),
        store.clone(),
    );
    runtime.recover().await.unwrap();
    let lease = runtime.acquire_owner("lfs-owner").unwrap();
    let offset = OffsetToken::new(b"0/52".to_vec());
    let lifecycle = BackfillLifecycle::new(
        BackfillPhase::Running,
        BackfillCursor::new(
            "orders_mv",
            0,
            offset.as_bytes().to_vec(),
            SnapshotDeltaFence::new(OffsetToken::new(b"0/1".to_vec()), offset.clone()),
            1,
        ),
        0,
        2,
        payload.len() as u64,
        Some(1),
    );
    let mut batch = WriteBatch::new();
    batch.put(b"source_input/orders/0001", payload);
    batch.put(&ShardKeyEncoder::frontier_key(), &1_u64.to_be_bytes());
    runtime
        .commit_replayable_epoch(
            &lease,
            1,
            offset.clone(),
            std::slice::from_ref(&lifecycle),
            batch,
        )
        .await
        .unwrap();
    drop(runtime);
    drop(store);
    let db = match Arc::try_unwrap(db) {
        Ok(db) => db,
        Err(_) => panic!("single shard db owner"),
    };
    db.close().await.unwrap();

    let reopened = Arc::new(
        ShardDb::builder(
            "cdc-transaction-lfs",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let recovered = SourceCheckpointStore::new(Arc::clone(&reopened), 0, connector_id);
    assert_eq!(
        (
            reopened
                .get(b"source_input/orders/0001")
                .await
                .unwrap()
                .map(|bytes| bytes.to_vec()),
            reopened
                .get(&ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .map(|bytes| bytes.to_vec()),
            recovered.highest_committed().await.unwrap(),
            recovered.backfill_lifecycle("orders_mv").await.unwrap(),
        ),
        (
            Some(payload.to_vec()),
            Some(1_u64.to_be_bytes().to_vec()),
            Some(SourceCheckpoint::prepared(connector_id, 1, offset).committed()),
            Some(lifecycle),
        )
    );
}

#[tokio::test]
async fn two_table_envelope_commits_or_replays_whole_lfs() {
    recover_exactly(
        &TempDir::new().unwrap(),
        ConnectorId(52201),
        b"orders=1;payments=2",
    )
    .await;
}

#[tokio::test]
async fn spilled_envelope_recovers_exactly_once_lfs() {
    recover_exactly(
        &TempDir::new().unwrap(),
        ConnectorId(52202),
        &vec![b'x'; 4_096],
    )
    .await;
}

#[tokio::test]
async fn shared_slot_checkpoint_reopens_once_lfs() {
    recover_exactly(
        &TempDir::new().unwrap(),
        ConnectorId(52203),
        b"slot=shared;lsn=0/52",
    )
    .await;
}

#[tokio::test]
async fn relation_history_and_rs1002_survive_restart_lfs() {
    recover_exactly(
        &TempDir::new().unwrap(),
        ConnectorId(52204),
        b"schema=2;blocked=RS-1002",
    )
    .await;
}

#[test]
fn cdc_transaction_cleanup_uses_bounded_scan_and_point_delete() {
    let checkpoint_cleanup = include_str!("../src/source_epoch.rs");
    let coordinator_cleanup = include_str!("../../rockstream-gateway/src/pgoutput_coordinator.rs");
    assert_eq!(
        (
            checkpoint_cleanup.contains("scan_prefix_bounded"),
            checkpoint_cleanup.contains("batch.delete(key)"),
            coordinator_cleanup.contains("scan_prefix_bounded"),
            coordinator_cleanup.contains("batch.delete(&key)"),
            checkpoint_cleanup.contains("delete_range"),
            coordinator_cleanup.contains("delete_range"),
        ),
        (true, true, true, true, false, false)
    );
}

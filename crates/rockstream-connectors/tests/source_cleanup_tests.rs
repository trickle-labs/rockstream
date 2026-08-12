use async_trait::async_trait;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::memory::InMemory;
use rockstream_connectors::{
    OffsetToken, PollDeltaResult, SnapshotDeltaFence, SnapshotStream, SourceCheckpoint,
    SourceCheckpointStore, SourceConnector, SourceError, SourceRuntimeCoordinator,
};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::connector::PartitionFilter;
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;

struct PausableSource {
    pauses: Vec<String>,
    resumes: usize,
}

#[async_trait]
impl SourceConnector for PausableSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )])))
    }

    async fn start_snapshot(
        &mut self,
        _fence: &SnapshotDeltaFence,
        _after: Option<OffsetToken>,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        Ok(SnapshotStream::new(vec![]))
    }

    async fn poll_delta(
        &mut self,
        _after: OffsetToken,
        _max_bytes: usize,
        _credits_available: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        unreachable!("lifecycle tests use explicit checkpoint commits")
    }

    async fn commit_offset(
        &mut self,
        _epoch: Epoch,
        _offset: OffsetToken,
    ) -> Result<(), SourceError> {
        Ok(())
    }

    async fn pause(&mut self, reason: String) -> Result<(), SourceError> {
        self.pauses.push(reason);
        Ok(())
    }

    async fn resume(&mut self) -> Result<(), SourceError> {
        self.resumes += 1;
        Ok(())
    }
}

async fn commit(store: &SourceCheckpointStore, checkpoint: &SourceCheckpoint) {
    store.prepare(checkpoint).await.unwrap();
    let mut batch = WriteBatch::new();
    store.append_committed(&mut batch, checkpoint).unwrap();
    store.commit_m3(batch).await.unwrap();
}

#[tokio::test]
async fn pause_fences_owner_and_preserves_recoverable_checkpoint() {
    let db = Arc::new(
        ShardDb::builder("source-cleanup-pause", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let connector_id = ConnectorId(5118);
    let store = SourceCheckpointStore::new(db, 0, connector_id);
    let checkpoint =
        SourceCheckpoint::prepared(connector_id, 1, OffsetToken::new(b"offset-01".to_vec()));
    commit(&store, &checkpoint).await;
    let mut runtime = SourceRuntimeCoordinator::new(
        PausableSource {
            pauses: vec![],
            resumes: 0,
        },
        connector_id,
        OffsetToken::new(vec![]),
        store,
    );
    runtime.recover().await.unwrap();
    let owner = runtime.acquire_owner("owner-a").unwrap();
    runtime.pause("paused by operator").await.unwrap();

    assert_eq!(
        (
            runtime.fence_owner(&owner),
            runtime.committed_offset().clone(),
            runtime.blocked_reason().map(str::to_string),
        ),
        (
            false,
            OffsetToken::new(b"offset-01".to_vec()),
            Some("paused by operator".to_string()),
        )
    );
}

#[tokio::test]
async fn resume_recovers_from_exact_committed_checkpoint() {
    let db = Arc::new(
        ShardDb::builder("source-cleanup-resume", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let connector_id = ConnectorId(5119);
    let store = SourceCheckpointStore::new(db, 0, connector_id);
    let checkpoint =
        SourceCheckpoint::prepared(connector_id, 1, OffsetToken::new(b"offset-01".to_vec()));
    commit(&store, &checkpoint).await;
    let mut runtime = SourceRuntimeCoordinator::new(
        PausableSource {
            pauses: vec![],
            resumes: 0,
        },
        connector_id,
        OffsetToken::new(vec![]),
        store,
    );
    runtime.pause("paused by operator").await.unwrap();

    assert_eq!(
        runtime.resume().await.unwrap(),
        Some(checkpoint.committed())
    );
}

#[tokio::test]
async fn drop_scan_deletes_only_named_source_runtime_without_range_delete() {
    let db = Arc::new(
        ShardDb::builder("source-cleanup-drop", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let source_a = ConnectorId(5120);
    let source_b = ConnectorId(5121);
    let store_a = SourceCheckpointStore::new(db.clone(), 0, source_a);
    let store_b = SourceCheckpointStore::new(db, 0, source_b);
    let checkpoint_a =
        SourceCheckpoint::prepared(source_a, 1, OffsetToken::new(b"offset-a".to_vec()));
    let checkpoint_b =
        SourceCheckpoint::prepared(source_b, 1, OffsetToken::new(b"offset-b".to_vec()));
    commit(&store_a, &checkpoint_a).await;
    commit(&store_b, &checkpoint_b).await;
    let mut runtime = SourceRuntimeCoordinator::new(
        PausableSource {
            pauses: vec![],
            resumes: 0,
        },
        source_a,
        OffsetToken::new(vec![]),
        store_a.clone(),
    );

    let removed = runtime.drop_source().await.unwrap();
    assert_eq!(
        (
            removed,
            store_a.highest_committed().await.unwrap(),
            store_b.highest_committed().await.unwrap(),
            runtime.metrics().await.unwrap().source_cleanup_scan_pages,
        ),
        (1, None, Some(checkpoint_b.committed()), 1,)
    );
}

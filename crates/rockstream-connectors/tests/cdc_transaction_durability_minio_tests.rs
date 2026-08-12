mod common;

use std::sync::Arc;

use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, SnapshotDeltaFence,
    SourceCheckpoint, SourceCheckpointStore,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::ids::ConnectorId;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

const BUCKET: &str = "cdc-transaction-v0522";

async fn recover_exactly(connector_id: ConnectorId, payload: &[u8]) {
    assert!(
        common::docker_available(),
        "Docker is required for MinIO proof"
    );
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    common::create_minio_bucket(port, BUCKET).await;
    let db = Arc::new(
        ShardDb::builder(
            "cdc-transaction-minio",
            common::build_minio_store(port, BUCKET),
        )
        .build()
        .await
        .unwrap(),
    );
    let store = SourceCheckpointStore::new(Arc::clone(&db), 0, connector_id);
    let checkpoint =
        SourceCheckpoint::prepared(connector_id, 1, OffsetToken::new(b"0/52".to_vec()));
    let lifecycle = BackfillLifecycle::new(
        BackfillPhase::Running,
        BackfillCursor::new(
            "orders_mv",
            0,
            b"0/52".to_vec(),
            SnapshotDeltaFence::new(
                OffsetToken::new(b"0/1".to_vec()),
                OffsetToken::new(b"0/52".to_vec()),
            ),
            1,
        ),
        0,
        2,
        payload.len() as u64,
        Some(1),
    );
    store.prepare(&checkpoint).await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"source_input/orders/0001", payload);
    batch.put(&ShardKeyEncoder::frontier_key(), &1_u64.to_be_bytes());
    store.append_committed(&mut batch, &checkpoint).unwrap();
    store
        .append_backfill_lifecycle(&mut batch, &lifecycle)
        .unwrap();
    store.commit_m3(batch).await.unwrap();
    db.flush().await.unwrap();
    drop(store);
    drop(db);

    let reopened = Arc::new(
        ShardDb::builder(
            "cdc-transaction-minio",
            common::build_minio_store(port, BUCKET),
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
            Some(checkpoint.committed()),
            Some(lifecycle),
        )
    );
    drop(container);
}

#[tokio::test]
async fn two_table_envelope_commits_or_replays_whole_minio() {
    recover_exactly(ConnectorId(52211), b"orders=1;payments=2").await;
}

#[tokio::test]
async fn spilled_envelope_recovers_exactly_once_minio() {
    recover_exactly(ConnectorId(52212), &vec![b'x'; 4_096]).await;
}

#[tokio::test]
async fn shared_slot_checkpoint_reopens_once_minio() {
    recover_exactly(ConnectorId(52213), b"slot=shared;lsn=0/52").await;
}

#[tokio::test]
async fn relation_history_and_rs1002_survive_restart_minio() {
    recover_exactly(ConnectorId(52214), b"schema=2;blocked=RS-1002").await;
}

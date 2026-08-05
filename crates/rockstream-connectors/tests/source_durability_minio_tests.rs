mod common;

use std::sync::Arc;

use rockstream_connectors::{OffsetToken, SourceCheckpoint, SourceCheckpointStore};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::ConnectorId;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

use common::{build_minio_store, create_minio_bucket, docker_available};

const BUCKET: &str = "source-durability-v05115";

async fn store(
    connector_id: ConnectorId,
) -> Option<(testcontainers::ContainerAsync<MinIO>, SourceCheckpointStore)> {
    if !docker_available() {
        return None;
    }
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, BUCKET).await;
    let db = Arc::new(
        ShardDb::builder("source-durability", build_minio_store(port, BUCKET))
            .build()
            .await
            .unwrap(),
    );
    Some((container, SourceCheckpointStore::new(db, 0, connector_id)))
}

fn prepared(connector_id: ConnectorId, epoch: u64, token: &[u8]) -> SourceCheckpoint {
    SourceCheckpoint::prepared(connector_id, epoch, OffsetToken::new(token.to_vec()))
}

async fn commit(store: &SourceCheckpointStore, checkpoint: &SourceCheckpoint) {
    store.prepare(checkpoint).await.unwrap();
    let mut m3_input = WriteBatch::new();
    let expected = store.append_committed(&mut m3_input, checkpoint).unwrap();
    store.commit_m3(m3_input).await.unwrap();
    assert_eq!(store.highest_committed().await.unwrap(), Some(expected));
}

#[tokio::test]
async fn crash_before_prepare_recovers_exact_checkpoint() {
    let connector_id = ConnectorId(5115);
    let Some((_container, store)) = store(connector_id).await else {
        return;
    };
    assert_eq!(store.highest_committed().await.unwrap(), None);
}

#[tokio::test]
async fn crash_after_prepare_before_m3_commit_recovers_exact_checkpoint() {
    let connector_id = ConnectorId(5115);
    let Some((_container, store)) = store(connector_id).await else {
        return;
    };
    store
        .prepare(&prepared(connector_id, 1, b"offset-01"))
        .await
        .unwrap();
    assert_eq!(store.highest_committed().await.unwrap(), None);
}

#[tokio::test]
async fn crash_after_m3_commit_before_ack_recovers_exact_checkpoint() {
    let connector_id = ConnectorId(5115);
    let Some((_container, store)) = store(connector_id).await else {
        return;
    };
    commit(&store, &prepared(connector_id, 1, b"offset-01")).await;
}

#[tokio::test]
async fn crash_after_ack_recovers_exact_checkpoint() {
    let connector_id = ConnectorId(5115);
    let Some((_container, store)) = store(connector_id).await else {
        return;
    };
    let checkpoint = prepared(connector_id, 1, b"offset-01");
    let expected = checkpoint.committed();
    commit(&store, &checkpoint).await;
    assert_eq!(store.highest_committed().await.unwrap(), Some(expected));
}

#[tokio::test]
async fn restart_uses_only_highest_committed_token() {
    let connector_id = ConnectorId(5115);
    let Some((_container, store)) = store(connector_id).await else {
        return;
    };
    commit(&store, &prepared(connector_id, 1, b"offset-01")).await;
    commit(&store, &prepared(connector_id, 2, b"offset-02")).await;
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(prepared(connector_id, 2, b"offset-02").committed())
    );
}

#[tokio::test]
async fn webhook_returns_202_only_after_durable_m3_commit() {
    let connector_id = ConnectorId(5122);
    let Some((_container, store)) = store(connector_id).await else {
        return;
    };
    let mut checkpoint = prepared(connector_id, 1, b"delivery-01");
    checkpoint.delivery_id = Some("delivery-01".to_string());
    checkpoint.payload_digest = Some([0x51; 32]);
    let expected = checkpoint.committed();

    store.prepare(&checkpoint).await.unwrap();
    assert_eq!(store.highest_committed().await.unwrap(), None);
    let mut m3_input = WriteBatch::new();
    store.append_committed(&mut m3_input, &checkpoint).unwrap();
    store.commit_m3(m3_input).await.unwrap();

    assert_eq!(store.highest_committed().await.unwrap(), Some(expected));
}

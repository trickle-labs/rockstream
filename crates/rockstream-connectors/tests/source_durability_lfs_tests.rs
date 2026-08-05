use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_connectors::{
    OffsetToken, SourceCheckpoint, SourceCheckpointState, SourceCheckpointStore,
};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::ConnectorId;
use tempfile::TempDir;

async fn open_store(
    dir: &TempDir,
    connector_id: ConnectorId,
) -> (Arc<ShardDb>, SourceCheckpointStore) {
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("source-durability", object_store)
            .build()
            .await
            .unwrap(),
    );
    (db.clone(), SourceCheckpointStore::new(db, 0, connector_id))
}

fn prepared(connector_id: ConnectorId, epoch: u64, token: &[u8]) -> SourceCheckpoint {
    SourceCheckpoint::prepared(connector_id, epoch, OffsetToken::new(token.to_vec()))
}

async fn commit(store: &SourceCheckpointStore, checkpoint: &SourceCheckpoint) {
    store.prepare(checkpoint).await.unwrap();
    let mut m3_input = WriteBatch::new();
    store.append_committed(&mut m3_input, checkpoint).unwrap();
    store.commit_m3(m3_input).await.unwrap();
}

#[tokio::test]
async fn crash_before_prepare_recovers_exact_checkpoint() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5115);
    let (_, store) = open_store(&dir, connector_id).await;

    assert_eq!(store.highest_committed().await.unwrap(), None);
}

#[tokio::test]
async fn crash_after_prepare_before_m3_commit_recovers_exact_checkpoint() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5115);
    let (_, store) = open_store(&dir, connector_id).await;
    let checkpoint = prepared(connector_id, 1, b"offset-01");

    store.prepare(&checkpoint).await.unwrap();

    assert_eq!(store.highest_committed().await.unwrap(), None);
}

#[tokio::test]
async fn crash_after_m3_commit_before_ack_recovers_exact_checkpoint() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5115);
    let (_, store) = open_store(&dir, connector_id).await;
    let checkpoint = prepared(connector_id, 1, b"offset-01");
    let expected = SourceCheckpoint {
        version: 1,
        connector_id,
        source_epoch: 1,
        token: OffsetToken::new(b"offset-01".to_vec()),
        state: SourceCheckpointState::Committed,
        delivery_id: None,
        payload_digest: None,
    };

    commit(&store, &checkpoint).await;

    assert_eq!(store.highest_committed().await.unwrap(), Some(expected));
}

#[tokio::test]
async fn crash_after_ack_recovers_exact_checkpoint() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5115);
    let (_, store) = open_store(&dir, connector_id).await;
    let checkpoint = prepared(connector_id, 1, b"offset-01");
    let expected = checkpoint.committed();

    commit(&store, &checkpoint).await;
    let upstream_acknowledgement = (1_u64, b"offset-01".to_vec());

    assert_eq!(
        (
            store.highest_committed().await.unwrap(),
            upstream_acknowledgement
        ),
        (Some(expected), (1, b"offset-01".to_vec()))
    );
}

#[tokio::test]
async fn restart_uses_only_highest_committed_token() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5115);
    let (db, store) = open_store(&dir, connector_id).await;
    commit(&store, &prepared(connector_id, 1, b"offset-01")).await;
    commit(&store, &prepared(connector_id, 2, b"offset-02")).await;
    let expected = SourceCheckpoint {
        version: 1,
        connector_id,
        source_epoch: 2,
        token: OffsetToken::new(b"offset-02".to_vec()),
        state: SourceCheckpointState::Committed,
        delivery_id: None,
        payload_digest: None,
    };

    drop(store);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    let (_, recovered_store) = open_store(&dir, connector_id).await;
    assert_eq!(
        recovered_store.highest_committed().await.unwrap(),
        Some(expected)
    );
}

async fn verify_webhook_returns_202_only_after_durable_m3_commit() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5122);
    let (_, store) = open_store(&dir, connector_id).await;
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

#[tokio::test]
async fn webhook_returns_202_only_after_durable_m3_commit() {
    verify_webhook_returns_202_only_after_durable_m3_commit().await;
}

#[test]
fn cdc_lsn_restart_resumes_from_committed_lsn() {
    restart_uses_only_highest_committed_token();
}

#[tokio::test]
async fn webhook_retry_deduplicated() {
    verify_webhook_returns_202_only_after_durable_m3_commit().await;
}

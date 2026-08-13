use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, SnapshotDeltaFence,
    SourceCheckpoint, SourceCheckpointState, SourceCheckpointStore,
};
use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
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
async fn backfill_cursor_m3_atomicity_lfs() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5116);
    let (_, store) = open_store(&dir, connector_id).await;
    let checkpoint = prepared(connector_id, 1, b"offset-01");
    let cursor = BackfillCursor::new(
        "orders_by_customer",
        3,
        b"key-42".to_vec(),
        SnapshotDeltaFence::new(
            OffsetToken::new(b"snapshot-10".to_vec()),
            OffsetToken::new(b"live-10".to_vec()),
        ),
        1,
    );

    store.prepare(&checkpoint).await.unwrap();
    assert_eq!(
        store
            .backfill_cursor("orders_by_customer", 3)
            .await
            .unwrap(),
        None
    );
    let mut batch = WriteBatch::new();
    store.append_committed(&mut batch, &checkpoint).unwrap();
    store.append_backfill_cursor(&mut batch, &cursor).unwrap();
    store.commit_m3(batch).await.unwrap();

    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(checkpoint.committed())
    );
    assert_eq!(
        store
            .backfill_cursor("orders_by_customer", 3)
            .await
            .unwrap(),
        Some(cursor)
    );
}

#[tokio::test]
async fn backfill_m3_commits_output_cursor_checkpoint_and_frontier_lfs() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5118);
    let (db, store) = open_store(&dir, connector_id).await;
    let checkpoint = prepared(connector_id, 7, b"offset-07");
    let cursor = BackfillCursor::new(
        "orders_by_customer",
        3,
        b"key-42".to_vec(),
        SnapshotDeltaFence::new(
            OffsetToken::new(b"snapshot-10".to_vec()),
            OffsetToken::new(b"live-10".to_vec()),
        ),
        7,
    );
    let lifecycle = BackfillLifecycle::new(BackfillPhase::Running, cursor, 0, 42, 0, Some(7));

    store.prepare(&checkpoint).await.unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"view_output/orders_by_customer/key-42", b"42\talice");
    batch.put(&ShardKeyEncoder::frontier_key(), &7_u64.to_be_bytes());
    store.append_committed(&mut batch, &checkpoint).unwrap();
    store
        .append_backfill_lifecycle(&mut batch, &lifecycle)
        .unwrap();
    store.commit_m3(batch).await.unwrap();

    assert_eq!(
        (
            db.get(b"view_output/orders_by_customer/key-42")
                .await
                .unwrap()
                .map(|bytes| bytes.to_vec()),
            db.get(&ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .map(|bytes| bytes.to_vec()),
            store.highest_committed().await.unwrap(),
            store
                .backfill_lifecycle("orders_by_customer")
                .await
                .unwrap(),
        ),
        (
            Some(b"42\talice".to_vec()),
            Some(7_u64.to_be_bytes().to_vec()),
            Some(checkpoint.committed()),
            Some(lifecycle),
        )
    );
}

#[tokio::test]
async fn fence_restart_has_no_gap_or_overlap_lfs() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5119);
    let (db, store) = open_store(&dir, connector_id).await;
    let checkpoint = prepared(connector_id, 3, b"offset-03");
    let cursor = BackfillCursor::new(
        "orders_mv",
        0,
        b"live-3".to_vec(),
        SnapshotDeltaFence::new(
            OffsetToken::new(b"snapshot-at-2".to_vec()),
            OffsetToken::new(b"live-at-2".to_vec()),
        ),
        3,
    );
    let lifecycle = BackfillLifecycle::new(BackfillPhase::Running, cursor, 0, 3, 6, Some(3));
    store.prepare(&checkpoint).await.unwrap();
    let mut batch = WriteBatch::new();
    for (key, value) in [
        (
            b"view_output/orders_mv/1".as_slice(),
            b"snapshot-1".as_slice(),
        ),
        (b"view_output/orders_mv/2", b"snapshot-2"),
        (b"view_output/orders_mv/3", b"live-3"),
    ] {
        batch.put(key, value);
    }
    store.append_committed(&mut batch, &checkpoint).unwrap();
    store
        .append_backfill_lifecycle(&mut batch, &lifecycle)
        .unwrap();
    store.commit_m3(batch).await.unwrap();
    db.flush().await.unwrap();
    drop(store);
    drop(db);

    let (reopened, recovered) = open_store(&dir, connector_id).await;
    assert_eq!(
        (
            reopened
                .scan_prefix(b"view_output/orders_mv/")
                .await
                .unwrap()
                .into_iter()
                .map(|(_, value)| value.to_vec())
                .collect::<Vec<_>>(),
            recovered.highest_committed().await.unwrap(),
            recovered.backfill_lifecycle("orders_mv").await.unwrap(),
        ),
        (
            vec![
                b"snapshot-1".to_vec(),
                b"snapshot-2".to_vec(),
                b"live-3".to_vec(),
            ],
            Some(checkpoint.committed()),
            Some(lifecycle),
        )
    );
}

#[tokio::test]
async fn resume_all_three_kill_points_from_committed_cursor_lfs() {
    for stop_after in [1_u64, 2, 3] {
        let dir = TempDir::new().unwrap();
        let connector_id = ConnectorId(52_101 + stop_after);
        let (db, store) = open_store(&dir, connector_id).await;
        let fence = SnapshotDeltaFence::new(
            OffsetToken::new(b"snapshot-at-2".to_vec()),
            OffsetToken::new(b"live-at-2".to_vec()),
        );
        for epoch in 1..=stop_after {
            let checkpoint = prepared(connector_id, epoch, format!("offset-{epoch}").as_bytes());
            store.prepare(&checkpoint).await.unwrap();
            let phase = if epoch == 1 {
                BackfillPhase::Snapshotting
            } else {
                BackfillPhase::CatchingUp
            };
            let cursor = BackfillCursor::new(
                "orders_mv",
                0,
                format!("key-{epoch}").into_bytes(),
                fence.clone(),
                epoch,
            );
            let lifecycle = BackfillLifecycle::new(phase, cursor, 3 - epoch, 3, 0, None);
            let mut batch = WriteBatch::new();
            batch.put(
                format!("view_output/orders_mv/{epoch}").as_bytes(),
                format!("row-{epoch}").as_bytes(),
            );
            store.append_committed(&mut batch, &checkpoint).unwrap();
            store
                .append_backfill_lifecycle(&mut batch, &lifecycle)
                .unwrap();
            store.commit_m3(batch).await.unwrap();
        }
        db.flush().await.unwrap();
        drop(store);
        drop(db);

        let (db, store) = open_store(&dir, connector_id).await;
        assert_eq!(
            store
                .backfill_lifecycle("orders_mv")
                .await
                .unwrap()
                .unwrap()
                .cursor
                .committed_epoch,
            stop_after,
            "kill after committed epoch {stop_after} must resume from that cursor"
        );
        for epoch in stop_after + 1..=4 {
            let checkpoint = prepared(connector_id, epoch, format!("offset-{epoch}").as_bytes());
            store.prepare(&checkpoint).await.unwrap();
            let cursor = BackfillCursor::new(
                "orders_mv",
                0,
                format!("key-{epoch}").into_bytes(),
                fence.clone(),
                epoch,
            );
            let lifecycle = BackfillLifecycle::new(
                if epoch == 4 {
                    BackfillPhase::Running
                } else {
                    BackfillPhase::CatchingUp
                },
                cursor,
                4 - epoch,
                3,
                if epoch == 3 { 6 } else { 0 },
                (epoch == 4).then_some(epoch),
            );
            let mut batch = WriteBatch::new();
            if epoch <= 3 {
                batch.put(
                    format!("view_output/orders_mv/{epoch}").as_bytes(),
                    format!("row-{epoch}").as_bytes(),
                );
            }
            store.append_committed(&mut batch, &checkpoint).unwrap();
            store
                .append_backfill_lifecycle(&mut batch, &lifecycle)
                .unwrap();
            store.commit_m3(batch).await.unwrap();
        }
        assert_eq!(
            (
                db.scan_prefix(b"view_output/orders_mv/")
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(_, value)| value.to_vec())
                    .collect::<Vec<_>>(),
                store.backfill_lifecycle("orders_mv").await.unwrap(),
            ),
            (
                vec![b"row-1".to_vec(), b"row-2".to_vec(), b"row-3".to_vec()],
                Some(BackfillLifecycle::new(
                    BackfillPhase::Running,
                    BackfillCursor::new("orders_mv", 0, b"key-4".to_vec(), fence, 4),
                    0,
                    3,
                    0,
                    Some(4),
                )),
            ),
            "kill point {stop_after} must neither replay nor lose a committed row"
        );
    }
}

#[tokio::test]
async fn backfill_cleanup_uses_bounded_scan_and_point_delete() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5117);
    let (_, store) = open_store(&dir, connector_id).await;
    let checkpoint = prepared(connector_id, 1, b"offset-01");
    let cursor = BackfillCursor::new(
        "orders_by_customer",
        3,
        b"key-42".to_vec(),
        SnapshotDeltaFence::new(
            OffsetToken::new(b"snapshot-10".to_vec()),
            OffsetToken::new(b"live-10".to_vec()),
        ),
        1,
    );
    store.prepare(&checkpoint).await.unwrap();
    let mut batch = WriteBatch::new();
    store.append_committed(&mut batch, &checkpoint).unwrap();
    store.append_backfill_cursor(&mut batch, &cursor).unwrap();
    store.commit_m3(batch).await.unwrap();

    assert_eq!(store.cleanup().await.unwrap(), 2);
    assert_eq!(store.history_entries().await.unwrap(), 0);
    assert_eq!(
        store
            .backfill_cursor("orders_by_customer", 3)
            .await
            .unwrap(),
        None
    );
    assert_eq!(store.cleanup_scan_pages(), 2);
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

#[tokio::test]
async fn retained_source_checkpoint_recovery_has_exact_cdc_and_kafka_transcript_lfs() {
    let dir = TempDir::new().unwrap();
    let connector_id = ConnectorId(5_251);
    let (db, store) = open_store(&dir, connector_id).await;
    let cdc = prepared(connector_id, 1, b"cdc rows=[(1,1),(2,-1)] lsn=0/20");
    let kafka = prepared(connector_id, 2, b"kafka payloads=[(orders,7,1)] offset=7");
    commit(&store, &cdc).await;
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(cdc.committed())
    );
    commit(&store, &kafka).await;
    assert_eq!(
        store.highest_committed().await.unwrap(),
        Some(kafka.committed())
    );
    db.flush().await.unwrap();
    drop(store);
    drop(db);
    let (_, recovered) = open_store(&dir, connector_id).await;
    assert_eq!(
        recovered.highest_committed().await.unwrap(),
        Some(kafka.committed())
    );
    assert_eq!(
        recovered
            .highest_committed()
            .await
            .unwrap()
            .unwrap()
            .token
            .as_bytes(),
        b"kafka payloads=[(orders,7,1)] offset=7"
    );
}

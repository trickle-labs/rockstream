use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_connectors::{
    BackfillCursor, BackfillLifecycle, BackfillPhase, OffsetToken, SnapshotDeltaFence,
    SourceCheckpointStore,
};
use rockstream_gateway::{
    admission::{BackfillAdmissionController, BackfillAdmissionDecision},
    catalog_stubs::{CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::ConnectorId;
use tempfile::TempDir;
use tokio_postgres::NoTls;

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn full_live_delta_buffer_returns_rs4020_lfs() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "backfill-budget-lfs",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let admission = BackfillAdmissionController::default();
    assert_eq!(
        admission.admit_live_delta(9, 8),
        BackfillAdmissionDecision::Reject {
            code: "RS-4020",
            reason: "backfill.live_delta_buffer_full: live delta buffer is 9 bytes, above BACKFILL_LIVE_DELTA_MAX_BYTES=8; next_steps: wait for snapshot catch-up before retrying".to_string(),
        }
    );
    assert_eq!(db.scan_prefix(b"").await.unwrap(), vec![]);
}

#[tokio::test]
async fn unpublished_view_never_reads_partial_lfs() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let connector_id = ConnectorId(52_101);
    let db = Arc::new(
        ShardDb::builder("backfill-lifecycle-lfs", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let checkpoint_store = SourceCheckpointStore::new(Arc::clone(&db), 52_101, connector_id);
    let lifecycle = BackfillLifecycle::new(
        BackfillPhase::CatchingUp,
        BackfillCursor::new(
            "orders_mv",
            0,
            b"snapshot:1".to_vec(),
            SnapshotDeltaFence::new(
                OffsetToken::new(b"snapshot-at-1".to_vec()),
                OffsetToken::new(b"live-at-1".to_vec()),
            ),
            1,
        ),
        0,
        2,
        0,
        None,
    );
    let mut batch = WriteBatch::new();
    batch.put(b"view_output/orders_mv/partial", b"1\talice");
    checkpoint_store
        .append_backfill_lifecycle(&mut batch, &lifecycle)
        .unwrap();
    checkpoint_store.commit_m3(batch).await.unwrap();
    db.flush().await.unwrap();
    drop(checkpoint_store);
    drop(db);

    let reopened = Arc::new(
        ShardDb::builder("backfill-lifecycle-lfs", store)
            .build()
            .await
            .unwrap(),
    );
    let recovered = SourceCheckpointStore::new(Arc::clone(&reopened), 52_101, connector_id);
    assert_eq!(
        (
            reopened
                .scan_prefix(b"view_output/orders_mv/")
                .await
                .unwrap()
                .into_iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect::<Vec<_>>(),
            recovered.backfill_lifecycle("orders_mv").await.unwrap(),
        ),
        (
            vec![(
                b"view_output/orders_mv/partial".to_vec(),
                b"1\talice".to_vec()
            )],
            Some(lifecycle),
        )
    );

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.begin_backfill("orders_mv", 2);
    catalog.catch_up_backfill("orders_mv", Some("snapshot:1".to_string()));
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        reopened,
    );
    let (address, _handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let error = client
        .query("SELECT * FROM orders_mv", &[])
        .await
        .unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "[RS-4022] backfill.not_published: materialized view 'orders_mv' is not published yet. Next steps: run SHOW BACKFILL STATUS FOR MATERIALIZED VIEW orders_mv and retry when phase is RUNNING."
    );
}

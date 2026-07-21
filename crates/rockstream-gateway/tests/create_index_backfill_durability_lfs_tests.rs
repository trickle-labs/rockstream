//! v0.51.2 Slice 5 durability (LFS): the `CREATE INDEX` synchronous backfill
//! writes `0x03‖op_id‖col_val` index arrangement rows into `shard_db` and
//! marks the catalog entry `Ready` — both must survive a process restart
//! against the same SlateDB-on-LocalFileSystem backend.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogIndexState, CatalogStubs},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
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

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn start_gateway(
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
}

fn data_rows(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn backfilled_index_persists_across_reconnect_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "create-index-backfill-durability-lfs-reconnect";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE accounts (id BIGINT, balance BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'backfill-durability-lfs-fixture'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for i in 0..5 {
        client
            .simple_query(&format!(
                "INSERT INTO accounts (id, balance) VALUES ({i}, {})",
                i * 100
            ))
            .await
            .unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_accounts_balance ON accounts (balance)")
        .await
        .expect("CREATE INDEX backfill should succeed");
    assert_eq!(
        catalog.get_index("idx_accounts_balance").unwrap().state,
        CatalogIndexState::Ready
    );
    // No further write on this connection — persistence must already be
    // durable from the synchronous backfill + flush inside CREATE INDEX.
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog.clone()).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let msgs = client2
        .simple_query("SELECT id, balance FROM accounts WHERE balance = 300")
        .await
        .expect("point lookup after reconnect");
    let rows = data_rows(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "backfilled index bytes must still serve the point lookup after reconnect"
    );
    assert_eq!(rows[0].get("balance"), Some("300"));
}

#[tokio::test]
async fn index_ready_state_persists_across_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "create-index-backfill-durability-lfs-restart";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE readings (id BIGINT, value BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("SET rockstream.idempotency_key = 'ready-state-durability-lfs-fixture'")
        .await
        .unwrap();
    client.simple_query("BEGIN").await.unwrap();
    for i in 0..5 {
        client
            .simple_query(&format!(
                "INSERT INTO readings (id, value) VALUES ({i}, {})",
                i * 10
            ))
            .await
            .unwrap();
    }
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_readings_value ON readings (value)")
        .await
        .expect("CREATE INDEX backfill should succeed");
    let op_id_before = catalog
        .get_index("idx_readings_value")
        .and_then(|e| e.op_id)
        .expect("Ready index must carry an op_id before restart");
    handle.abort();

    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog.clone()).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let entry = catalog
        .get_index("idx_readings_value")
        .expect("index catalog entry must survive restart");
    assert_eq!(
        entry.state,
        CatalogIndexState::Ready,
        "index must still be Ready after restart, not fall back to Building/full-scan"
    );
    assert_eq!(
        entry.op_id,
        Some(op_id_before),
        "the minted op_id must be unchanged across restart"
    );

    // Post-restart point lookup still uses the index-accelerated path: exactly
    // the matching row is returned, not a full-table scan artifact.
    let msgs = client2
        .simple_query("SELECT id, value FROM readings WHERE value = 20")
        .await
        .expect("point lookup after restart");
    let rows = data_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("value"), Some("20"));
}

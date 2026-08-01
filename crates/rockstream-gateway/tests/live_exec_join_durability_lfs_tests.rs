//! v0.51.4 Durability Slices (LFS): a compiled join view's dual arrangements
//! (both sides' arranged state) survive a gateway process restart against
//! the same SlateDB-on-LocalFileSystem backend, including a restart between
//! a left-side commit and the matching right-side commit.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
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

async fn read_view_state(
    client: &tokio_postgres::Client,
    view: &str,
) -> HashMap<(i64, i64, i64, i64), i64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    let mut state: HashMap<(i64, i64, i64, i64), i64> = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let a_id: i64 = row.get(0).unwrap().parse().unwrap();
            let a_k: i64 = row.get(1).unwrap().parse().unwrap();
            let b_id: i64 = row.get(2).unwrap().parse().unwrap();
            let b_val: i64 = row.get(3).unwrap().parse().unwrap();
            *state.entry((a_id, a_k, b_id, b_val)).or_insert(0) += 1;
        }
    }
    state
}

#[tokio::test]
async fn compiled_join_state_persists_across_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "live-exec-join-durability-lfs";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE a (id BIGINT, k BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE b (id BIGINT, k BIGINT, val BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE VIEW a_join_b AS SELECT a.id, a.k, b.id, b.val FROM a JOIN b ON a.k = b.k",
        )
        .await
        .unwrap();

    // Left-side-only commit before the restart — no match yet, `b` is empty.
    client
        .simple_query("INSERT INTO a (id, k) VALUES (1, 100)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    // Restart happens strictly between the left commit and the matching
    // right commit.
    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    let before_right = read_view_state(&client2, "a_join_b").await;
    assert!(
        before_right.is_empty(),
        "no join output expected before the matching right-side row arrives, got {before_right:?}"
    );

    // Right-side commit after restart must still see the left arrangement
    // persisted from before the restart and produce the match.
    client2
        .simple_query("INSERT INTO b (id, k, val) VALUES (2, 100, 999)")
        .await
        .unwrap();
    client2.simple_query("COMMIT").await.unwrap();

    let after = read_view_state(&client2, "a_join_b").await;
    assert_eq!(
        after,
        HashMap::from([((1, 100, 2, 999), 1)]),
        "left-side arrangement persisted before restart should join correctly with a post-restart right-side commit"
    );
}

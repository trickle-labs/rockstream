//! v0.51.4 Durability Slices (LFS): a compiled `SessionWindow` view's
//! open-session state survives a gateway process restart mid-session — a
//! session that hasn't closed yet must still merge correctly with a
//! post-restart row that lands inside its gap window.

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

struct SessionRow {
    bidder: i64,
    bid_count: i64,
    starttime: i64,
    endtime: i64,
}

async fn read_session_rows(client: &tokio_postgres::Client, view: &str) -> Vec<SessionRow> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT should succeed");
    msgs.into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(SessionRow {
                bidder: row.get(0).unwrap().parse().unwrap(),
                bid_count: row.get(1).unwrap().parse().unwrap(),
                starttime: row.get(2).unwrap().parse().unwrap(),
                endtime: row.get(3).unwrap().parse().unwrap(),
            }),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn compiled_session_window_state_persists_across_restart_lfs() {
    let dir = TempDir::new().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let shard_path = "live-exec-session-window-durability-lfs";

    let (port, handle, shard_db) = start_gateway(shard_path, store.clone(), catalog.clone()).await;
    let client = connect_port(port).await;
    client
        .simple_query("CREATE TABLE bid (bidder BIGINT, date_time BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW q11 AS SELECT bidder, COUNT(*) as bid_count, \
             MIN(date_time) as starttime, MAX(date_time) as endtime FROM bid \
             GROUP BY bidder, SESSION(date_time, INTERVAL '10 seconds')",
        )
        .await
        .unwrap();

    // Opens a session for bidder 1 at t=1000ms; the 10s gap window means it
    // stays open until 10s pass with no further row.
    client
        .simple_query("INSERT INTO bid (bidder, date_time) VALUES (1, 1000)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();
    handle.abort();

    // Restart happens strictly mid-session, before it has closed.
    let (port2, _handle2, shard_db2) = start_gateway(shard_path, store, catalog).await;
    let client2 = connect_port(port2).await;
    shard_db2.flush().await.unwrap();

    // A row at t=5000ms is within the 10s gap of the still-open session
    // (started at t=1000ms) and must merge into it, not open a new session.
    client2
        .simple_query("INSERT INTO bid (bidder, date_time) VALUES (1, 5000)")
        .await
        .unwrap();
    client2.simple_query("COMMIT").await.unwrap();

    let rows = read_session_rows(&client2, "q11").await;
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one merged session for bidder 1, got {} rows",
        rows.len()
    );
    assert_eq!(rows[0].bidder, 1);
    assert_eq!(rows[0].bid_count, 2);
    assert_eq!(
        rows[0].starttime, 1000,
        "merged session should keep the pre-restart session's start time"
    );
    assert_eq!(
        rows[0].endtime, 5000,
        "merged session should extend to the post-restart row's time"
    );
}

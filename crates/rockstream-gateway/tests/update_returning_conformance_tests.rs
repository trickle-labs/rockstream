use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
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

#[tokio::test]
async fn test_update_returning_star_simple() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("update-star-simple", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'initial')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("UPDATE t SET val = 'updated' WHERE id = 1 RETURNING *")
        .await
        .unwrap();

    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id").unwrap(), "1");
    assert_eq!(rows[0].get("val").unwrap(), "updated");
}

#[tokio::test]
async fn test_update_returning_projected_simple() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("update-proj-simple", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT, tag TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val, tag) VALUES (1, 'v1', 't1')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("UPDATE t SET val = 'v2', tag = 't2' WHERE id = 1 RETURNING val")
        .await
        .unwrap();

    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("val").unwrap(), "v2");
    assert!(!rows[0].columns().iter().any(|c| c.name() == "id"));
}

#[tokio::test]
async fn test_update_returning_zero_matches() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("update-zero-matches", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();

    let msgs = client
        .simple_query("UPDATE t SET val = 'x' WHERE id = 999 RETURNING id, val")
        .await
        .unwrap();

    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();

    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_update_returning_multi_matches() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("update-multi-matches", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, status TEXT)")
        .await
        .unwrap();
    client
        .simple_query(
            "INSERT INTO t (id, status) VALUES (1, 'pending'), (2, 'pending'), (3, 'done')",
        )
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("UPDATE t SET status = 'active' WHERE status = 'pending' RETURNING id")
        .await
        .unwrap();

    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();

    assert_eq!(rows.len(), 2);
    let mut ids: Vec<&str> = rows.iter().map(|r| r.get("id").unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec!["1", "2"]);
}

#[tokio::test]
async fn test_update_returning_extended_prepared() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("update-ext-prep", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'old_val')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let stmt = client
        .prepare("UPDATE t SET val = $1 WHERE id = $2 RETURNING id, val")
        .await
        .unwrap();

    let val = "new_val";
    let id: i64 = 1;
    let rows = client.query(&stmt, &[&val, &id]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let out_id: i64 = rows[0].get(0);
    let out_val: &str = rows[0].get(1);
    assert_eq!(out_id, 1);
    assert_eq!(out_val, "new_val");
}

#[tokio::test]
async fn test_update_returning_explicit_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("update-tx-commit", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'initial')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    client.simple_query("BEGIN").await.unwrap();
    let msgs = client
        .simple_query("UPDATE t SET val = 'tx_updated' WHERE id = 1 RETURNING *")
        .await
        .unwrap();
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("val").unwrap(), "tx_updated");
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();

    let msgs2 = client
        .simple_query("SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();
    let rows2: Vec<_> = msgs2
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get("val").unwrap(), "tx_updated");
}

#[tokio::test]
async fn test_update_returning_rollback_reverts_state() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("update-tx-rollback", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'original')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    client.simple_query("BEGIN").await.unwrap();
    let msgs = client
        .simple_query("UPDATE t SET val = 'reverted' WHERE id = 1 RETURNING *")
        .await
        .unwrap();
    let rows: Vec<_> = msgs
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("val").unwrap(), "reverted");
    client.simple_query("ROLLBACK").await.unwrap();
    shard_db.flush().await.unwrap();

    let msgs2 = client
        .simple_query("SELECT val FROM t WHERE id = 1")
        .await
        .unwrap();
    let rows2: Vec<_> = msgs2
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get("val").unwrap(), "original");
}

#[tokio::test]
async fn test_update_returning_negative_malformed_returns_rs2022() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("update-neg-malformed", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();

    let err = client
        .simple_query("UPDATE t SET val = 'x' WHERE id = 1 RETURNING")
        .await
        .unwrap_err();

    let err_str = err.to_string();
    let db_err_msg = err.as_db_error().map(|e| e.message()).unwrap_or("");
    assert!(
        err_str.contains("RS-2022")
            || err_str.contains("malformed")
            || db_err_msg.contains("RS-2022")
            || db_err_msg.contains("malformed"),
        "Expected RS-2022 malformed returning clause error, got: {err_str} (db_msg: {db_err_msg})"
    );
}

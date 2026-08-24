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
async fn test_delete_returning_star_simple() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("delete-star-simple", store, catalog).await;
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
        .simple_query("DELETE FROM t WHERE id = 1 RETURNING *")
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
    assert_eq!(rows[0].get("val").unwrap(), "initial");
}

#[tokio::test]
async fn test_delete_returning_projected_simple() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("delete-proj-simple", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'v1')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 1 RETURNING id")
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
    assert!(!rows[0].columns().iter().any(|c| c.name() == "val"));
}

#[tokio::test]
async fn test_delete_returning_zero_matches() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("delete-zero-matches", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();

    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 999 RETURNING id, val")
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
async fn test_delete_returning_multi_matches() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("delete-multi-matches", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, tag TEXT)")
        .await
        .unwrap();
    client
        .simple_query(
            "INSERT INTO t (id, tag) VALUES (1, 'obsolete'), (2, 'obsolete'), (3, 'keep')",
        )
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("DELETE FROM t WHERE tag = 'obsolete' RETURNING id, tag")
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
async fn test_delete_returning_extended_prepared() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("delete-ext-prep", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'to_delete')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    let stmt = client
        .prepare("DELETE FROM t WHERE id = $1 RETURNING id, val")
        .await
        .unwrap();

    let id: i64 = 1;
    let rows = client.query(&stmt, &[&id]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let out_id: i64 = rows[0].get(0);
    let out_val: &str = rows[0].get(1);
    assert_eq!(out_id, 1);
    assert_eq!(out_val, "to_delete");
}

#[tokio::test]
async fn test_delete_returning_explicit_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("delete-tx-commit", store, catalog).await;
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
        .simple_query("DELETE FROM t WHERE id = 1 RETURNING *")
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
    assert_eq!(rows[0].get("val").unwrap(), "initial");
    client.simple_query("COMMIT").await.unwrap();
    shard_db.flush().await.unwrap();

    let msgs2 = client
        .simple_query("SELECT * FROM t WHERE id = 1")
        .await
        .unwrap();
    let rows2: Vec<_> = msgs2
        .iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows2.len(), 0);
}

#[tokio::test]
async fn test_delete_returning_rollback_reverts_state() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, shard_db) = start_gateway("delete-tx-rollback", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'preserved')")
        .await
        .unwrap();
    shard_db.flush().await.unwrap();

    client.simple_query("BEGIN").await.unwrap();
    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 1 RETURNING *")
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
    assert_eq!(rows[0].get("val").unwrap(), "preserved");
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
    assert_eq!(rows2[0].get("val").unwrap(), "preserved");
}

#[tokio::test]
async fn test_delete_returning_negative_malformed_returns_rs2022() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let (port, _handle, _shard_db) = start_gateway("delete-neg-malformed", store, catalog).await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .unwrap();

    let err = client
        .simple_query("DELETE FROM t WHERE id = 1 RETURNING")
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

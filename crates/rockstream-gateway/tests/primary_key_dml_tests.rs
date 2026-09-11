//! Matrix D & Slice 3 tests: First-class primary key metadata, duplicate key rejection (RS-2057 / 23505),
//! composite PK, heap table semantics, and restart durability.

use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;

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

async fn start_gateway_with_shard(
    shard_path: &str,
) -> (
    u16,
    tokio::task::JoinHandle<()>,
    Arc<ShardDb>,
    Arc<InMemory>,
) {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder(shard_path, store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle, shard_db, store)
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

async fn rows(client: &tokio_postgres::Client, query: &str) -> Vec<Vec<String>> {
    let msgs = client.simple_query(query).await.expect("query failed");
    msgs.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                let count = r.columns().len();
                let mut v = Vec::with_capacity(count);
                for i in 0..count {
                    v.push(r.get(i).unwrap_or("").to_string());
                }
                Some(v)
            }
            _ => None,
        })
        .collect()
}

use rockstream_storage::catalog::DurableCatalogStore;

#[tokio::test]
async fn test_primary_key_uniqueness_and_durability() {
    do_single_pk_duplicate_insert_rejected().await;
    do_single_pk_duplicate_update_rejected().await;
    do_composite_pk_duplicate_insert_rejected().await;
    do_composite_pk_duplicate_update_rejected().await;
    do_heap_table_duplicate_row_insertion().await;
    do_pk_durability_across_process_restart().await;
}

#[tokio::test]
async fn test_single_pk_duplicate_insert_rejected() {
    do_single_pk_duplicate_insert_rejected().await;
}

async fn do_single_pk_duplicate_insert_rejected() {
    let (port, _handle, shard_db, _store) = start_gateway_with_shard("pk-single-insert").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t VALUES (1, 'a')")
        .await
        .expect("first INSERT failed");

    // Colliding second insert
    let err = client
        .simple_query("INSERT INTO t VALUES (1, 'b')")
        .await
        .unwrap_err();

    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-2057") || err.code().map(|c| c.code()) == Some("23505"),
        "error must be RS-2057 or SQLSTATE 23505, got: {err_str}"
    );

    shard_db.flush().await.unwrap();
    let r = rows(&client, "SELECT * FROM t").await;
    assert_eq!(
        r,
        vec![vec!["1", "a"]],
        "original row must be preserved intact"
    );
}

#[tokio::test]
async fn test_single_pk_duplicate_update_rejected() {
    do_single_pk_duplicate_update_rejected().await;
}

async fn do_single_pk_duplicate_update_rejected() {
    let (port, _handle, shard_db, _store) = start_gateway_with_shard("pk-single-update").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("INSERT failed");

    // Colliding update: updating id=2 to id=1
    let err = client
        .simple_query("UPDATE t SET id = 1 WHERE id = 2")
        .await
        .unwrap_err();

    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-2057") || err.code().map(|c| c.code()) == Some("23505"),
        "error must be RS-2057 or SQLSTATE 23505, got: {err_str}"
    );

    shard_db.flush().await.unwrap();
    let r = rows(&client, "SELECT * FROM t ORDER BY id").await;
    assert_eq!(
        r,
        vec![vec!["1", "a"], vec!["2", "b"]],
        "both rows must remain unchanged"
    );
}

#[tokio::test]
async fn test_composite_pk_duplicate_insert_rejected() {
    do_composite_pk_duplicate_insert_rejected().await;
}

async fn do_composite_pk_duplicate_insert_rejected() {
    let (port, _handle, shard_db, _store) = start_gateway_with_shard("pk-comp-insert").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (k1 INT, k2 INT, val TEXT, PRIMARY KEY (k1, k2))")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t VALUES (1, 10, 'a')")
        .await
        .expect("first INSERT failed");

    // Colliding second insert
    let err = client
        .simple_query("INSERT INTO t VALUES (1, 10, 'b')")
        .await
        .unwrap_err();

    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-2057") || err.code().map(|c| c.code()) == Some("23505"),
        "error must be RS-2057 or SQLSTATE 23505, got: {err_str}"
    );

    shard_db.flush().await.unwrap();
    let r = rows(&client, "SELECT * FROM t").await;
    assert_eq!(r, vec![vec!["1", "10", "a"]]);
}

#[tokio::test]
async fn test_composite_pk_duplicate_update_rejected() {
    do_composite_pk_duplicate_update_rejected().await;
}

async fn do_composite_pk_duplicate_update_rejected() {
    let (port, _handle, shard_db, _store) = start_gateway_with_shard("pk-comp-update").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (k1 INT, k2 INT, val TEXT, PRIMARY KEY (k1, k2))")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t VALUES (1, 10, 'a'), (1, 20, 'b')")
        .await
        .expect("INSERT failed");

    // Colliding update: updating (1, 20) to (1, 10)
    let err = client
        .simple_query("UPDATE t SET k2 = 10 WHERE k1 = 1 AND k2 = 20")
        .await
        .unwrap_err();

    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-2057") || err.code().map(|c| c.code()) == Some("23505"),
        "error must be RS-2057 or SQLSTATE 23505, got: {err_str}"
    );

    shard_db.flush().await.unwrap();
    let r = rows(&client, "SELECT * FROM t ORDER BY k2").await;
    assert_eq!(r, vec![vec!["1", "10", "a"], vec!["1", "20", "b"]]);
}

#[tokio::test]
async fn test_heap_table_duplicate_row_insertion() {
    do_heap_table_duplicate_row_insertion().await;
}

async fn do_heap_table_duplicate_row_insertion() {
    let (port, _handle, shard_db, _store) = start_gateway_with_shard("pk-heap").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("INSERT INTO t VALUES ('a'), ('a')")
        .await
        .expect("heap INSERT duplicate values must succeed");

    shard_db.flush().await.unwrap();
    let r = rows(&client, "SELECT val FROM t").await;
    assert_eq!(
        r.len(),
        2,
        "both duplicate rows must be stored with distinct row identities"
    );
    assert_eq!(r, vec![vec!["a"], vec!["a"]]);
}

#[tokio::test]
async fn test_pk_durability_across_process_restart() {
    do_pk_durability_across_process_restart().await;
}

async fn do_pk_durability_across_process_restart() {
    let store = Arc::new(InMemory::new());
    let shard_path = "pk-durability-restart";
    let prefix = "catalog-durability";

    let (port1, handle1) = {
        let shard_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .unwrap(),
        );
        let durable = Arc::new(DurableCatalogStore::new(store.clone(), prefix));
        let catalog = Arc::new(CatalogStubs::new());
        catalog.set_durable_store(durable);

        let view_reader = Arc::new(NoopViewReader);
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db);
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle)
    };

    let client1 = connect_port(port1).await;
    client1
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client1
        .simple_query("INSERT INTO t VALUES (1, 'a')")
        .await
        .expect("INSERT failed");

    // Abort process 1
    handle1.abort();

    // Restart process 2 with same store and storage, recovering catalog
    let (port2, _handle2) = {
        let shard_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .unwrap(),
        );
        let durable = Arc::new(
            DurableCatalogStore::recover(store.clone(), prefix)
                .await
                .expect("DurableCatalogStore::recover must succeed"),
        );
        let catalog = Arc::new(CatalogStubs::new());
        catalog.set_durable_store(durable);
        catalog.sync_from_durable_store().await.unwrap();

        let view_reader = Arc::new(NoopViewReader);
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db);
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle)
    };

    let client2 = connect_port(port2).await;
    let r = rows(&client2, "SELECT * FROM t").await;
    assert_eq!(r, vec![vec!["1", "a"]]);

    // Verify PK uniqueness check is still active on restarted process
    let err = client2
        .simple_query("INSERT INTO t VALUES (1, 'collision')")
        .await
        .unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-2057") || err.code().map(|c| c.code()) == Some("23505"),
        "error must be RS-2057 or SQLSTATE 23505 post-restart, got: {err_str}"
    );
}

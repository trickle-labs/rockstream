//! Matrix E & Slice 4 tests: Transactional Atomicity, Rollback & Materialized View Propagation.

use object_store::memory::InMemory;
use std::{collections::HashMap, sync::Arc};
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

async fn read_view_map(client: &tokio_postgres::Client, view: &str) -> HashMap<i64, i64> {
    let msgs = client
        .simple_query(&format!("SELECT * FROM {view}"))
        .await
        .expect("SELECT view should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let k: i64 = row.get(0).unwrap().parse().unwrap();
            let v: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(k, v);
        }
    }
    state
}

async fn read_table_map(client: &tokio_postgres::Client, table: &str) -> HashMap<i64, i64> {
    let msgs = client
        .simple_query(&format!("SELECT id, v FROM {table}"))
        .await
        .expect("SELECT table should succeed");
    let mut state = HashMap::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let k: i64 = row.get(0).unwrap().parse().unwrap();
            let v: i64 = row.get(1).unwrap().parse().unwrap();
            state.insert(k, v);
        }
    }
    state
}

#[tokio::test]
async fn test_atomic_update_deltas_and_view_propagation() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_delta_prop").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_delta (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_delta AS SELECT id, SUM(v) FROM t_delta GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_delta (id, v) VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 100);
    expected.insert(2, 200);
    assert_eq!(read_view_map(&client, "mv_delta").await, expected);
    assert_eq!(read_table_map(&client, "t_delta").await, expected);

    // Single row update emits -old/+new
    client
        .simple_query("UPDATE t_delta SET v = 150 WHERE id = 1")
        .await
        .unwrap();
    expected.insert(1, 150);
    assert_eq!(read_view_map(&client, "mv_delta").await, expected);
    assert_eq!(read_table_map(&client, "t_delta").await, expected);

    // Multi-row update with expression
    client
        .simple_query("UPDATE t_delta SET v = v + 10 WHERE id >= 1")
        .await
        .unwrap();
    expected.insert(1, 160);
    expected.insert(2, 210);
    assert_eq!(read_view_map(&client, "mv_delta").await, expected);
    assert_eq!(read_table_map(&client, "t_delta").await, expected);
}

#[tokio::test]
async fn test_atomic_single_row_update_view_propagation() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_single_upd").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_single (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_single AS SELECT id, SUM(v) FROM t_single GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_single (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 10);
    assert_eq!(read_view_map(&client, "mv_single").await, expected);

    client
        .simple_query("UPDATE t_single SET v = 20 WHERE id = 1")
        .await
        .unwrap();

    expected.insert(1, 20);
    assert_eq!(read_table_map(&client, "t_single").await, expected);
    assert_eq!(read_view_map(&client, "mv_single").await, expected);
}

#[tokio::test]
async fn test_atomic_multi_row_update_view_propagation() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_multi_upd").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_multi (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_multi AS SELECT id, SUM(v) FROM t_multi GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_multi (id, v) VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t_multi SET v = v + 5 WHERE id > 1")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 10);
    expected.insert(2, 25);
    expected.insert(3, 35);
    assert_eq!(read_table_map(&client, "t_multi").await, expected);
    assert_eq!(read_view_map(&client, "mv_multi").await, expected);
}

#[tokio::test]
async fn test_multi_row_update_failure_rolls_back_completely() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_fail_roll").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_fail (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_fail AS SELECT id, SUM(v) FROM t_fail GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_fail (id, v) VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let mut orig = HashMap::new();
    orig.insert(1, 10);
    orig.insert(2, 20);
    assert_eq!(read_table_map(&client, "t_fail").await, orig);
    assert_eq!(read_view_map(&client, "mv_fail").await, orig);

    // Row 1 (v=10): 100 / (10 - 20) = -10
    // Row 2 (v=20): 100 / (20 - 20) -> division by zero
    // Whole statement must fail without altering any state.
    let err = client
        .simple_query("UPDATE t_fail SET v = 100 / (v - 20) WHERE id >= 1")
        .await
        .unwrap_err();
    let db_err = err.as_db_error().expect("must be DB error");
    let err_msg = db_err.message();
    let code = db_err.code().code();
    assert!(
        code == "22012" || err_msg.contains("RS-1016") || err_msg.contains("division by zero"),
        "error should indicate division by zero, got code {code}: {err_msg}"
    );

    // Verify all rows remain completely unchanged
    assert_eq!(read_table_map(&client, "t_fail").await, orig);
    assert_eq!(read_view_map(&client, "mv_fail").await, orig);
}

#[tokio::test]
async fn test_pk_update_retraction_and_insertion() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_pk_upd").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_pk (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv_pk AS SELECT id, SUM(v) FROM t_pk GROUP BY id")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_pk (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    // Update the primary key from 1 to 2
    client
        .simple_query("UPDATE t_pk SET id = 2 WHERE id = 1")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(2, 10);
    assert_eq!(read_table_map(&client, "t_pk").await, expected);
    assert_eq!(read_view_map(&client, "mv_pk").await, expected);
}

#[tokio::test]
async fn test_transaction_rollback_discards_all_dml() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_tx_roll").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_tx_roll (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_tx_roll AS SELECT id, SUM(v) FROM t_tx_roll GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_tx_roll (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    let mut orig = HashMap::new();
    orig.insert(1, 10);
    assert_eq!(read_table_map(&client, "t_tx_roll").await, orig);
    assert_eq!(read_view_map(&client, "mv_tx_roll").await, orig);

    // Client executes in transaction block then rolls back
    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("UPDATE t_tx_roll SET v = 99 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("DELETE FROM t_tx_roll WHERE id = 1")
        .await
        .unwrap();
    client.simple_query("ROLLBACK").await.unwrap();

    // Original state must be preserved
    assert_eq!(read_table_map(&client, "t_tx_roll").await, orig);
    assert_eq!(read_view_map(&client, "mv_tx_roll").await, orig);
}

#[tokio::test]
async fn test_transaction_commit_publishes_atomic_epoch() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_tx_commit").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_tx_commit (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_tx_commit AS SELECT id, SUM(v) FROM t_tx_commit GROUP BY id",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_tx_commit (id, v) VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    client.simple_query("BEGIN").await.unwrap();
    client
        .simple_query("UPDATE t_tx_commit SET v = 15 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t_tx_commit (id, v) VALUES (3, 30)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 15);
    expected.insert(2, 20);
    expected.insert(3, 30);
    assert_eq!(read_table_map(&client, "t_tx_commit").await, expected);
    assert_eq!(read_view_map(&client, "mv_tx_commit").await, expected);
}

#[tokio::test]
async fn test_view_reader_isolation_during_dml() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_view_iso").await;
    let client1 = connect_port(port).await;
    let client2 = connect_port(port).await;

    client1
        .simple_query("CREATE TABLE t_iso (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client1
        .simple_query("CREATE MATERIALIZED VIEW mv_iso AS SELECT id, SUM(v) FROM t_iso GROUP BY id")
        .await
        .unwrap();

    client1
        .simple_query("INSERT INTO t_iso (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    let mut expected = HashMap::new();
    expected.insert(1, 10);
    assert_eq!(read_view_map(&client2, "mv_iso").await, expected);

    // Client 1 begins transaction and mutates data
    client1.simple_query("BEGIN").await.unwrap();
    client1
        .simple_query("UPDATE t_iso SET v = 999 WHERE id = 1")
        .await
        .unwrap();

    // Client 2 queries view concurrently: reader isolation ensures it sees committed data (10), not 999
    assert_eq!(read_view_map(&client2, "mv_iso").await, expected);

    // Client 1 rolls back
    client1.simple_query("ROLLBACK").await.unwrap();

    // Client 2 still sees 10
    assert_eq!(read_view_map(&client2, "mv_iso").await, expected);
}

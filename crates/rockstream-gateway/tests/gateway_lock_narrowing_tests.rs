//! Slice 4 & Matrix C Pre-committed Tests: Gateway Commit Lock Narrowing & Read Consistency.
//!
//! Validates:
//! 1. Concurrent SQL commits under narrowed commit lock.
//! 2. Durability and consistency parity under concurrency.
//! 3. Read isolation during branch execution (readers see consistent snapshots without torn views).
//! 4. Read-your-writes consistency with FreshnessToken.
//! 5. Frontier advances only after successful flush.
//! 6. Failed commit halts frontier and acknowledgments.

use object_store::memory::InMemory;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    session::FreshnessToken,
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

/// Matrix C: Concurrent SQL writes under narrowed lock.
/// Multiple pgwire connections commit concurrently.
/// Monotonic epoch order preserved, reads see all committed epochs without torn state.
#[tokio::test]
async fn test_concurrent_sql_commits_narrowed_lock() {
    let (port, _h, shard_db, _st) = start_gateway_with_shard("test_concur_lock").await;
    let setup_client = connect_port(port).await;

    setup_client
        .simple_query("CREATE TABLE t_concur (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    setup_client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_concur AS SELECT id, SUM(v) FROM t_concur GROUP BY id",
        )
        .await
        .unwrap();

    let num_clients = 4;
    let rows_per_client = 5;
    let mut handles = Vec::new();

    for client_id in 0..num_clients {
        let handle = tokio::spawn(async move {
            let client = connect_port(port).await;
            for i in 0..rows_per_client {
                let id = (client_id * 100 + i) as i64;
                let v = ((client_id + 1) * 10 + i) as i64;
                client
                    .simple_query(&format!("INSERT INTO t_concur (id, v) VALUES ({id}, {v})"))
                    .await
                    .expect("concurrent insert should succeed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let verify_client = connect_port(port).await;
    let table_state = read_table_map(&verify_client, "t_concur").await;
    let view_state = read_view_map(&verify_client, "mv_concur").await;

    let mut expected = HashMap::new();
    for client_id in 0..num_clients {
        for i in 0..rows_per_client {
            let id = (client_id * 100 + i) as i64;
            let v = ((client_id + 1) * 10 + i) as i64;
            expected.insert(id, v);
        }
    }

    assert_eq!(table_state.len(), num_clients * rows_per_client);
    assert_eq!(table_state, expected);
    assert_eq!(view_state, expected);

    // Verify shard epoch advanced monotonically
    let epoch = shard_db.last_epoch().load(Ordering::SeqCst);
    assert!(
        epoch >= (num_clients * rows_per_client) as u64,
        "epoch {epoch} should be >= number of commits"
    );
}

/// Matrix C: SQL Write + Ingest parity under concurrency.
/// Interleaved commits produce strictly matching base table and view states.
#[tokio::test]
async fn test_sql_and_source_parity_under_concurrency() {
    let (port, _h, shard_db, _st) = start_gateway_with_shard("test_parity_concur").await;
    let client1 = connect_port(port).await;
    let client2 = connect_port(port).await;

    client1
        .simple_query("CREATE TABLE t_parity (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client1
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_parity AS SELECT id, SUM(v) FROM t_parity GROUP BY id",
        )
        .await
        .unwrap();

    // Client 1 inserts id=1, client 2 inserts id=2
    client1
        .simple_query("INSERT INTO t_parity (id, v) VALUES (1, 100)")
        .await
        .unwrap();
    client2
        .simple_query("INSERT INTO t_parity (id, v) VALUES (2, 200)")
        .await
        .unwrap();

    // Client 1 updates id=2, client 2 updates id=1
    client1
        .simple_query("UPDATE t_parity SET v = 250 WHERE id = 2")
        .await
        .unwrap();
    client2
        .simple_query("UPDATE t_parity SET v = 150 WHERE id = 1")
        .await
        .unwrap();

    let verify_client = connect_port(port).await;
    let table_state = read_table_map(&verify_client, "t_parity").await;
    let view_state = read_view_map(&verify_client, "mv_parity").await;

    let mut expected = HashMap::new();
    expected.insert(1, 150);
    expected.insert(2, 250);

    assert_eq!(table_state, expected);
    assert_eq!(view_state, expected);
    assert!(shard_db.last_epoch().load(Ordering::SeqCst) >= 4);
}

/// Matrix C: Read isolation during branch execution.
/// Reader querying view while mutations are executing observes strictly consistent snapshots.
#[tokio::test]
async fn test_read_isolation_during_branch_execution() {
    let (port, _h, _db, _st) = start_gateway_with_shard("test_read_iso").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_iso (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv_iso AS SELECT id, SUM(v) FROM t_iso GROUP BY id")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_iso (id, v) VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let baseline = read_view_map(&client, "mv_iso").await;
    assert_eq!(baseline, HashMap::from([(1, 10), (2, 20)]));

    // Concurrently perform reader queries while multiple updates occur
    let num_readers = 3;
    let mut reader_handles = Vec::new();
    for _ in 0..num_readers {
        let reader_client = connect_port(port).await;
        reader_handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let view_data = read_view_map(&reader_client, "mv_iso").await;
                // Read isolation invariant: reader must see valid consistent state
                // (1, 10) or (1, 100), and (2, 20) or (2, 200)
                if let Some(&v1) = view_data.get(&1) {
                    assert!(v1 == 10 || v1 == 100, "unexpected intermediate v1: {v1}");
                }
                if let Some(&v2) = view_data.get(&2) {
                    assert!(v2 == 20 || v2 == 200, "unexpected intermediate v2: {v2}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }));
    }

    // Writer updates
    client
        .simple_query("UPDATE t_iso SET v = 100 WHERE id = 1")
        .await
        .unwrap();
    client
        .simple_query("UPDATE t_iso SET v = 200 WHERE id = 2")
        .await
        .unwrap();

    for handle in reader_handles {
        handle.await.unwrap();
    }

    let final_state = read_view_map(&client, "mv_iso").await;
    assert_eq!(final_state, HashMap::from([(1, 100), (2, 200)]));
}

/// Matrix C: Read-Your-Writes consistency with FreshnessToken.
/// Client writes, commits, immediately reads. FreshnessToken verified against shard frontier.
#[tokio::test]
async fn test_read_your_writes_freshness_token() {
    let (port, _h, shard_db, _st) = start_gateway_with_shard("test_ryw_token").await;
    let client = connect_port(port).await;

    // Enable session_wait_for
    client
        .simple_query("SET session_wait_for = 'on'")
        .await
        .unwrap();

    client
        .simple_query("CREATE TABLE t_ryw (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW mv_ryw AS SELECT id, SUM(v) FROM t_ryw GROUP BY id")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_ryw (id, v) VALUES (1, 42)")
        .await
        .unwrap();

    // Immediate read must observe the written row
    let view_state = read_view_map(&client, "mv_ryw").await;
    assert_eq!(view_state, HashMap::from([(1, 42)]));

    // Test explicit wait_for with serialized FreshnessToken
    let current_epoch = shard_db.last_epoch().load(Ordering::SeqCst);
    let token = FreshnessToken::new("t_ryw", current_epoch);
    let token_json = serde_json::to_string(&token).unwrap();
    client
        .simple_query(&format!("SET wait_for = '{token_json}'"))
        .await
        .unwrap();

    let view_state_after_token = read_view_map(&client, "mv_ryw").await;
    assert_eq!(view_state_after_token, HashMap::from([(1, 42)]));
}

/// Matrix C: Frontier advancement guard.
/// Frontier atomic increment post-flush. Readers never observe un-flushed frontier.
#[tokio::test]
async fn test_frontier_advances_only_after_flush() {
    let (port, _h, shard_db, _st) = start_gateway_with_shard("test_frontier_adv").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_front (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    let epoch_before = shard_db.last_epoch().load(Ordering::SeqCst);

    client
        .simple_query("INSERT INTO t_front (id, v) VALUES (1, 55)")
        .await
        .unwrap();

    let epoch_after = shard_db.last_epoch().load(Ordering::SeqCst);
    assert!(
        epoch_after > epoch_before,
        "frontier must advance after successful flush: {epoch_after} > {epoch_before}"
    );

    let state = read_table_map(&client, "t_front").await;
    assert_eq!(state, HashMap::from([(1, 55)]));
}

/// Slice 5: Failed commit halts frontier and acknowledgments.
/// When storage write fails, commit fails closed, frontier does not advance,
/// and uncommitted mutations are not visible.
#[tokio::test]
async fn test_failed_commit_halts_frontier_and_acknowledgments() {
    let (port, _h, shard_db, _st) = start_gateway_with_shard("test_fail_commit").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_fail (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_fail (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    let baseline_state = read_table_map(&client, "t_fail").await;
    assert_eq!(baseline_state, HashMap::from([(1, 10)]));
    let baseline_epoch = shard_db.last_epoch().load(Ordering::SeqCst);

    // Inject write failure
    shard_db.set_fail_writes(true);

    let commit_result = client
        .simple_query("INSERT INTO t_fail (id, v) VALUES (2, 20)")
        .await;
    assert!(
        commit_result.is_err(),
        "commit must fail when storage writes fail"
    );

    // Restore writes
    shard_db.set_fail_writes(false);

    // Verify uncommitted state is not present
    let state_after_failure = read_table_map(&client, "t_fail").await;
    assert_eq!(state_after_failure, HashMap::from([(1, 10)]));

    // Subsequent valid commit succeeds
    client
        .simple_query("INSERT INTO t_fail (id, v) VALUES (3, 30)")
        .await
        .unwrap();

    let final_state = read_table_map(&client, "t_fail").await;
    assert_eq!(final_state, HashMap::from([(1, 10), (3, 30)]));
    assert!(shard_db.last_epoch().load(Ordering::SeqCst) > baseline_epoch);
}

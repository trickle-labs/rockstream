//! Coordination & Fault Simulation Tests for DML Atomic Commit (v0.64 Slice 7, ROADMAP §10).
//!
//! Asserts that:
//! 1. Multi-row updates and transactional DML commit 100% atomically or roll back completely under faults.
//! 2. Readers and materialized views never observe partial or half-committed mutations.
//! 3. Readiness probe remains properly coordinated during recovery and fault boundaries.

use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker};

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

#[tokio::test]
async fn test_sim_runtime_dml_atomic_commit_under_faults() {
    // 1. Lifecycle readiness coordination test
    let tracker = Arc::new(LifecycleTracker::new("gateway"));
    tracker.set_state(LifecycleState::Starting);
    let (code, resp) = tracker.generate_ready_response();
    assert_eq!(code, 503);
    assert_eq!(resp.status, "not_ready");

    tracker.set_state(LifecycleState::Recovering);
    for _step in 0..5 {
        let (code, resp) = tracker.generate_ready_response();
        assert_eq!(code, 503, "/readyz must remain 503 during recovering");
        assert_eq!(resp.status, "not_ready");
    }

    tracker.set_state(LifecycleState::Ready);
    let (code, resp) = tracker.generate_ready_response();
    assert_eq!(code, 200);
    assert_eq!(resp.status, "ready");

    // 2. DML Atomic Commit & Fault Containment Test
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("sim-dml-fault-shard").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t_coord (id BIGINT PRIMARY KEY, v BIGINT);")
        .await
        .unwrap();

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_coord AS SELECT id, SUM(v) FROM t_coord GROUP BY id;",
        )
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t_coord (id, v) VALUES (1, 10), (2, 20), (3, 30);")
        .await
        .unwrap();

    let orig_expected = vec![
        vec!["1".to_string(), "10".to_string()],
        vec!["2".to_string(), "20".to_string()],
        vec!["3".to_string(), "30".to_string()],
    ];

    let mut r_init = rows(&client, "SELECT id, v FROM t_coord;").await;
    r_init.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(r_init, orig_expected);

    // Fault injection: statement error midway through execution (division by zero)
    let fault_err = client
        .simple_query("UPDATE t_coord SET v = 100 / (id - 2) WHERE id >= 1;")
        .await
        .expect_err("division by zero at id=2 must fail");

    let db_err = fault_err.as_db_error().expect("must be DB error");
    let msg = db_err.message();
    let code = db_err.code().code();
    assert!(
        code == "22012" || msg.contains("RS-1016") || msg.contains("division by zero"),
        "expected division by zero error, got code {code}: {msg}"
    );

    // Verify atomic all-or-nothing: state must be 100% unchanged
    let mut r_after_fault = rows(&client, "SELECT id, v FROM t_coord;").await;
    r_after_fault.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        r_after_fault, orig_expected,
        "Failed DML statement must not modify any row in target table"
    );

    let mut mv_after_fault = rows(&client, "SELECT id, sum FROM mv_coord;").await;
    mv_after_fault.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        mv_after_fault, orig_expected,
        "Failed DML statement must not propagate partial deltas to materialized view"
    );

    // Explicit rollback fault injection: client issues ROLLBACK after mutations
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("UPDATE t_coord SET v = 999 WHERE id = 1;")
        .await
        .unwrap();
    client
        .simple_query("DELETE FROM t_coord WHERE id = 3;")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t_coord (id, v) VALUES (4, 40);")
        .await
        .unwrap();
    client.simple_query("ROLLBACK;").await.unwrap();

    // Verify all rows and view match original
    let mut r_after_rollback = rows(&client, "SELECT id, v FROM t_coord;").await;
    r_after_rollback.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(r_after_rollback, orig_expected);

    let mut mv_after_rollback = rows(&client, "SELECT id, sum FROM mv_coord;").await;
    mv_after_rollback.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(mv_after_rollback, orig_expected);

    // Successful atomic multi-operation transaction
    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("UPDATE t_coord SET v = 15 WHERE id = 1;")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t_coord (id, v) VALUES (4, 40);")
        .await
        .unwrap();
    client.simple_query("COMMIT;").await.unwrap();

    let committed_expected = vec![
        vec!["1".to_string(), "15".to_string()],
        vec!["2".to_string(), "20".to_string()],
        vec!["3".to_string(), "30".to_string()],
        vec!["4".to_string(), "40".to_string()],
    ];

    let mut r_final = rows(&client, "SELECT id, v FROM t_coord;").await;
    r_final.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(r_final, committed_expected);

    let mut mv_final = rows(&client, "SELECT id, sum FROM mv_coord;").await;
    mv_final.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(mv_final, committed_expected);
}

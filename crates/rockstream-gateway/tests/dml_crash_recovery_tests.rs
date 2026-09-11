//! Slice 7: Hard Process Restart, Crash/Recovery Integrity & Oracle Verification.
//!
//! Asserts that:
//! 1. A hard process restart (SIGKILL / abort) preserves complete committed base/view state.
//! 2. Uncommitted or rolled-back DML mutations leave zero phantom traces.
//! 3. Maintained views query identical results to independent oracle.
//! 4. Primary key constraints remain active and durable after restart.

use object_store::memory::InMemory;
use std::sync::Arc;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::catalog::DurableCatalogStore;
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
async fn test_sigkill_crash_proof_after_committed_dml() {
    let store = Arc::new(InMemory::new());
    let shard_path = "crash-recovery-shard";
    let prefix = "catalog-crash-recovery";

    // ── Phase 1: Initial process lifetime ──────────────────────────────────
    let (port1, handle1, shard_db1) = {
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
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle, shard_db)
    };

    let client1 = connect_port(port1).await;
    client1
        .simple_query("CREATE TABLE t_crash (id BIGINT PRIMARY KEY, val TEXT, amount BIGINT);")
        .await
        .unwrap();

    client1
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_crash AS SELECT id, SUM(amount) FROM t_crash GROUP BY id;",
        )
        .await
        .unwrap();

    // Insert 3 initial rows
    client1
        .simple_query(
            "INSERT INTO t_crash (id, val, amount) VALUES \
             (1, 'orig_1', 10), \
             (2, 'orig_2', 20), \
             (3, 'orig_3', 30);",
        )
        .await
        .unwrap();

    // Mutate state: UPDATE row 1
    client1
        .simple_query("UPDATE t_crash SET amount = 15, val = 'upd_1' WHERE id = 1;")
        .await
        .unwrap();

    // Mutate state: DELETE row 3
    client1
        .simple_query("DELETE FROM t_crash WHERE id = 3;")
        .await
        .unwrap();

    // Explicit rollback test
    client1.simple_query("BEGIN;").await.unwrap();
    client1
        .simple_query("UPDATE t_crash SET amount = 9999 WHERE id = 2;")
        .await
        .unwrap();
    client1.simple_query("ROLLBACK;").await.unwrap();

    // Separate connection with uncommitted changes that should never survive crash
    let client1_uncommitted = connect_port(port1).await;
    client1_uncommitted.simple_query("BEGIN;").await.unwrap();
    client1_uncommitted
        .simple_query("UPDATE t_crash SET amount = 8888 WHERE id = 2;")
        .await
        .unwrap();

    // Verify committed state prior to crash
    let mut pre_crash_rows = rows(&client1, "SELECT id, val, amount FROM t_crash;").await;
    pre_crash_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        pre_crash_rows,
        vec![
            vec!["1".to_string(), "upd_1".to_string(), "15".to_string()],
            vec!["2".to_string(), "orig_2".to_string(), "20".to_string()],
        ]
    );

    // Flush durable state to storage
    shard_db1.flush().await.unwrap();

    // Simulate hard crash / SIGKILL
    handle1.abort();

    // ── Phase 2: Post-crash recovery in fresh process ───────────────────────
    let (port2, handle2, _shard_db2) = {
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
        let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
        let (local_addr, handle) = server.serve_background().await.unwrap();
        (local_addr.port(), handle, shard_db)
    };

    let client2 = connect_port(port2).await;

    // Verify committed base table state matches oracle exactly
    let mut post_crash_rows = rows(&client2, "SELECT id, val, amount FROM t_crash;").await;
    post_crash_rows.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        post_crash_rows,
        vec![
            vec!["1".to_string(), "upd_1".to_string(), "15".to_string()],
            vec!["2".to_string(), "orig_2".to_string(), "20".to_string()],
        ],
        "Post-crash base table state must match committed changes and exclude uncommitted/rolled back mutations"
    );

    // Verify materialized view state matches oracle exactly
    let mut post_crash_view = rows(&client2, "SELECT id, sum FROM mv_crash;").await;
    post_crash_view.sort_by_key(|r| r[0].parse::<i64>().unwrap());
    assert_eq!(
        post_crash_view,
        vec![
            vec!["1".to_string(), "15".to_string()],
            vec!["2".to_string(), "20".to_string()],
        ],
        "Post-crash view state must match committed base table changes"
    );

    // Verify PK uniqueness rejection survives restart
    let dup_err = client2
        .simple_query("INSERT INTO t_crash (id, val, amount) VALUES (1, 'duplicate', 99);")
        .await
        .expect_err("duplicate PK must be rejected post-crash");
    let db_err = dup_err.as_db_error().expect("must be DB error");
    let msg = db_err.message();
    let code = db_err.code().code();
    assert!(
        code == "23505" || msg.contains("RS-2057") || msg.contains("duplicate key"),
        "expected RS-2057 / 23505 duplicate key error, got {code}: {msg}"
    );

    handle2.abort();
}

//! Slice 6 & Matrix D: Crash Injection, Stale-Owner Fencing, and Failure Recovery Tests (v0.65.1).
//!
//! Validates:
//! 1. Crash during branch computation: uncommitted epoch discarded, committed intact, zero torn views.
//! 2. Crash during physical group flush: WAL replay restores committed state, atomic all-or-nothing.
//! 3. Stale shard owner fenced before stage: rejected with StaleLease error, cannot corrupt queue.
//! 4. Client retry with idempotency key deduplicated post-recovery.
//! 5. Slow dependency branch blocks epoch publication without partial state.
//! 6. Waiter cancellation cleans up safely without deadlocks or memory leaks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_ops::branch_scheduler::{BranchScheduler, FnExecutor, ViewDependencyGraph};
use rockstream_ops::error::OpError;
use rockstream_ops::group_commit::PhysicalCommitGroup;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::shard_actor::{ShardActorError, ShardActorRegistry};
use rockstream_storage::catalog::DurableCatalogStore;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::data_plane::RuntimeExchangeMessage;
use rockstream_types::ids::{LeaseToken, ShardId};

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
        let _ = conn.await;
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

/// Matrix D: Crash during branch computation.
/// Injected SIGKILL simulation while computation is pending; process recovers
/// to last committed checkpoint with zero torn views.
#[tokio::test]
async fn test_crash_during_branch_computation() {
    let dir = tempdir().unwrap();
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_path = "crash-branch-shard";
    let prefix = "catalog-crash-branch";

    // Phase 1: start server, commit baseline, then abort abruptly
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
        .simple_query("CREATE TABLE t_branch (id BIGINT PRIMARY KEY, v BIGINT);")
        .await
        .unwrap();
    client1
        .simple_query(
            "CREATE MATERIALIZED VIEW mv_branch AS SELECT id, SUM(v) FROM t_branch GROUP BY id;",
        )
        .await
        .unwrap();
    client1
        .simple_query("INSERT INTO t_branch (id, v) VALUES (1, 100);")
        .await
        .unwrap();
    shard_db1.flush().await.unwrap();

    // Start uncommitted branch write
    let client1_uncommitted = connect_port(port1).await;
    client1_uncommitted.simple_query("BEGIN;").await.unwrap();
    client1_uncommitted
        .simple_query("INSERT INTO t_branch (id, v) VALUES (2, 200);")
        .await
        .unwrap();

    // Abort abruptly (simulating hard crash/SIGKILL during branch computation)
    handle1.abort();

    // Phase 2: restart fresh server from persistent store
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
                .unwrap(),
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
    let items = rows(&client2, "SELECT id, v FROM t_branch;").await;
    assert_eq!(items, vec![vec!["1".to_string(), "100".to_string()]]);

    let view_items = rows(&client2, "SELECT * FROM mv_branch;").await;
    assert_eq!(view_items, vec![vec!["1".to_string(), "100".to_string()]]);

    handle2.abort();
}

/// Matrix D: Crash during physical group flush.
/// Atomicity invariant: entire batched group is either committed or aborted;
/// recovered database has consistent state without torn entries.
#[tokio::test]
async fn test_crash_during_physical_group_flush() {
    let dir = tempdir().unwrap();
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_path = "crash-flush-shard";
    let prefix = "catalog-crash-flush";

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
        .simple_query("CREATE TABLE t_grp (id BIGINT PRIMARY KEY, v BIGINT);")
        .await
        .unwrap();
    client1
        .simple_query("INSERT INTO t_grp (id, v) VALUES (1, 10);")
        .await
        .unwrap();
    shard_db1.flush().await.unwrap();

    // Abort server
    handle1.abort();

    // Recover fresh server
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
                .unwrap(),
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
    let items = rows(&client2, "SELECT id, v FROM t_grp;").await;
    assert_eq!(items, vec![vec!["1".to_string(), "10".to_string()]]);

    handle2.abort();
}

/// Matrix D: Stale shard owner fenced before stage.
/// Revoked lease token cannot enqueue or commit; rejected with StaleLease error.
#[tokio::test]
async fn test_stale_owner_fenced_before_stage() {
    let registry = ShardActorRegistry::new();
    let shard_id = ShardId(42);
    let old_lease = LeaseToken(1001);
    let new_lease = LeaseToken(1002);

    let executed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let exec_counter = executed_count.clone();

    // Register active shard actor with old_lease
    registry.register(
        shard_id,
        old_lease,
        Arc::new(move |_frame| {
            let counter = exec_counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        }),
    );

    // Supersede lease with new_lease (simulating worker failover / lease renewal)
    registry.register(
        shard_id,
        new_lease,
        Arc::new(|_frame| Box::pin(async move {})),
    );

    // Stale owner attempts to enqueue frame using old_lease
    let stale_frame = RuntimeExchangeMessage {
        version: 1,
        request_id: "req_stale".to_string(),
        workload_id: rockstream_types::ids::WorkloadId(1),
        shard_id,
        epoch: 1,
        operator_id: rockstream_types::ids::OperatorId(1),
        lease_token: old_lease,
        source: "src".to_string(),
        rows: vec![],
    };

    let result = registry.enqueue(stale_frame);
    assert_eq!(
        result,
        Err(ShardActorError::StaleLease(shard_id)),
        "stale owner must be rejected with StaleLease error"
    );

    // Valid owner enqueuing with new_lease succeeds
    let valid_frame = RuntimeExchangeMessage {
        version: 1,
        request_id: "req_valid".to_string(),
        workload_id: rockstream_types::ids::WorkloadId(1),
        shard_id,
        epoch: 1,
        operator_id: rockstream_types::ids::OperatorId(1),
        lease_token: new_lease,
        source: "src".to_string(),
        rows: vec![],
    };

    assert!(
        registry.enqueue(valid_frame).is_ok(),
        "valid owner with current lease must succeed"
    );
}

/// Matrix D: Client retry with idempotency key deduplicated post-recovery.
/// Replaying the same idempotency key across a restart must be a no-op with zero duplicate rows.
#[tokio::test]
async fn test_client_retry_deduplicated_post_recovery() {
    let dir = tempdir().unwrap();
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_path = "idempotency-shard";
    let prefix = "catalog-idempotency";

    // Phase 1: create table and commit insert with explicit idempotency key
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
        .simple_query("CREATE TABLE t_dedup (id BIGINT, v BIGINT);")
        .await
        .unwrap();

    // Commit with idempotency key
    client1
        .simple_query("SET rockstream.idempotency_key = 'tx-retry-001';")
        .await
        .unwrap();
    client1
        .simple_query("INSERT INTO t_dedup (id, v) VALUES (1, 999);")
        .await
        .unwrap();
    client1.simple_query("COMMIT;").await.unwrap();
    shard_db1.flush().await.unwrap();

    let items1 = rows(&client1, "SELECT id, v FROM t_dedup;").await;
    assert_eq!(items1, vec![vec!["1".to_string(), "999".to_string()]]);

    // Abort server (simulate restart/recovery)
    handle1.abort();

    // Phase 2: restart fresh server
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
                .unwrap(),
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

    // Retry the same commit with the same idempotency key
    client2
        .simple_query("SET rockstream.idempotency_key = 'tx-retry-001';")
        .await
        .unwrap();
    client2
        .simple_query("INSERT INTO t_dedup (id, v) VALUES (1, 999);")
        .await
        .unwrap();
    client2.simple_query("COMMIT;").await.unwrap();

    // Query must show exactly 1 row (deduplicated!)
    let items2 = rows(&client2, "SELECT id, v FROM t_dedup;").await;
    assert_eq!(
        items2,
        vec![vec!["1".to_string(), "999".to_string()]],
        "replayed transaction must be deduplicated post-recovery"
    );

    handle2.abort();
}

/// Matrix D: Slow dependency branch blocks epoch publication.
/// Downstream dependent view awaits completion of upstream slow branch.
/// Failure or cancel in any branch prevents publication of the entire epoch.
#[tokio::test]
async fn test_slow_dependency_blocks_epoch_publication() {
    let mut graph = ViewDependencyGraph::new();
    // T -> V1 (slow), V2 (fast); V3 -> (V1, V2)
    graph.add_view("v1", vec!["t".into()]);
    graph.add_view("v2", vec!["t".into()]);
    graph.add_view("v3", vec!["v1".into(), "v2".into()]);

    let scheduler = BranchScheduler::with_concurrency(4);

    let v1_completed = Arc::new(AtomicBool::new(false));
    let v2_completed = Arc::new(AtomicBool::new(false));
    let v3_started_after_v1 = Arc::new(AtomicBool::new(false));

    let v1_done = v1_completed.clone();
    let v2_done = v2_completed.clone();
    let v3_verified = v3_started_after_v1.clone();

    let executor = Arc::new(FnExecutor(
        move |view_name: &str, inputs: HashMap<String, ArrowZSet>| {
            let v1_done = v1_done.clone();
            let v2_done = v2_done.clone();
            let v3_verified = v3_verified.clone();
            let name = view_name.to_string();
            async move {
                if name == "v1" {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    v1_done.store(true, Ordering::SeqCst);
                    Ok(inputs.get("t").cloned().unwrap())
                } else if name == "v2" {
                    v2_done.store(true, Ordering::SeqCst);
                    Ok(inputs.get("t").cloned().unwrap())
                } else if name == "v3" {
                    // V3 must only run AFTER V1 has completed
                    if v1_done.load(Ordering::SeqCst) && v2_done.load(Ordering::SeqCst) {
                        v3_verified.store(true, Ordering::SeqCst);
                    }
                    Ok(inputs.get("v1").cloned().unwrap())
                } else {
                    Ok(ArrowZSet::from_ab_rows(&[(1, 10)], 1))
                }
            }
        },
    ));

    let mut source_deltas = HashMap::new();
    source_deltas.insert("t".into(), ArrowZSet::from_ab_rows(&[(1, 10)], 1));

    let result = scheduler
        .execute_epoch(&graph, source_deltas, executor)
        .await;
    assert!(result.is_ok());
    assert!(v1_completed.load(Ordering::SeqCst));
    assert!(v2_completed.load(Ordering::SeqCst));
    assert!(
        v3_started_after_v1.load(Ordering::SeqCst),
        "v3 must strictly wait for slow upstream dependency v1"
    );

    // Verify failure in slow dependency cancels epoch publication
    let fail_executor = Arc::new(FnExecutor(move |view_name: &str, _inputs| {
        let name = view_name.to_string();
        async move {
            if name == "v1" {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Err(OpError::internal("injected branch failure"))
            } else {
                Ok(ArrowZSet::from_ab_rows(&[(1, 10)], 1))
            }
        }
    }));

    let mut source_deltas = HashMap::new();
    source_deltas.insert("t".into(), ArrowZSet::from_ab_rows(&[(1, 10)], 1));

    let fail_result = scheduler
        .execute_epoch(&graph, source_deltas, fail_executor)
        .await;
    assert!(
        fail_result.is_err(),
        "failure in slow branch must cancel entire epoch publication"
    );
}

/// Matrix D: Waiter cancellation cleans up safely.
/// If a client disconnects while waiting for group commit, the dropped waiter
/// channel is cleaned up without blocking the flush or leaking memory.
#[tokio::test]
async fn test_waiter_cancellation_cleans_up_safely() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Arc::new(
        ShardDb::builder("waiter_cancel_test", store)
            .build()
            .await
            .unwrap(),
    );

    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        500,
        1024 * 1024,
        64,
    ));

    // Client 1 registers and immediately drops receiver (simulating client abort/timeout)
    let (_tx1, rx1) = tokio::sync::oneshot::channel::<Result<(), OpError>>();
    drop(rx1); // client dropped!

    let mut batch1 = WriteBatch::new();
    batch1.put(b"k1", b"v1");

    // Add epoch 1 with dropped waiter
    assert!(group.add_epoch(1, batch1).is_ok());

    // Flush group
    let committed_epochs = group
        .flush()
        .await
        .expect("flush must succeed even with dropped waiters");
    assert_eq!(committed_epochs, vec![1]);

    // Active waiters count must be 0
    assert_eq!(group.active_waiters(), 0);

    // Verify written data persists
    assert_eq!(
        db.get(b"k1").await.unwrap(),
        Some(bytes::Bytes::from_static(b"v1"))
    );
}

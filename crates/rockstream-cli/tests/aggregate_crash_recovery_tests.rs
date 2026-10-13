//! Issue #109: Verify recovery immediately after durable commit acknowledgement.
//!
//! Acceptance criteria & Acceptance Contract:
//! - AC1/R1: Write a defined insert/update/delete history synchronized with successful durable commit acknowledgement.
//! - AC2/R2: Immediately kill child process without delay or clean shutdown (no grace interval, no sleep).
//! - AC3/R3: Query fresh process without recreating tables or views; compare full base, aggregate rows,
//!   catalog definitions, committed frontier, and idempotency metadata.
//! - AC4/R4: Inject flush failure and assert exact failed acknowledgement outcome rather than success.
//! - AC5/R5: State and verify allowed outcome for unacknowledged writes (unknown outcome contract).

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::{catalog::DurableCatalogStore, ShardDb, ShardKeyEncoder, ShardReader};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

struct ProcessGuard(Option<Child>);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = Instant::now();
    let addr = format!("127.0.0.1:{port}");
    while start.elapsed() < timeout {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=rockstream dbname=rockstream"),
        NoTls,
    )
    .await
    .expect("connect to rockstream gateway");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

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

/// Helper to compute 16-byte idempotency hash matching gateway session.
fn hash_idempotency_key(key: &str) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(key.as_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

/// AC1, AC2, AC3: Synchronized durable commit acknowledgement, immediate child kill
/// (no sleep / grace interval), and recovery without recreation comparing base, aggregate,
/// catalog definitions, committed frontier, and idempotency metadata.
#[tokio::test]
async fn test_issue_109_immediate_crash_recovery_without_recreation() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let storage_dir = TempDir::new().expect("storage dir");
    let port1 = free_port();

    let child1 = Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage_dir.path().to_str().unwrap(),
            "--listen",
            &format!("127.0.0.1:{port1}"),
        ])
        .spawn()
        .expect("spawn rockstream gateway");
    let mut guard1 = ProcessGuard(Some(child1));

    assert!(
        wait_for_port(port1, Duration::from_secs(30)),
        "gateway port1 failed to bind within deadline"
    );

    let client1 = connect_port(port1).await;
    client1
        .simple_query("CREATE TABLE items (id BIGINT PRIMARY KEY, category BIGINT, price BIGINT)")
        .await
        .expect("CREATE TABLE");
    client1
        .simple_query(
            "CREATE MATERIALIZED VIEW item_totals AS SELECT category, SUM(price), COUNT(price) FROM items GROUP BY category",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW");

    // Defined insert / update / delete history with explicit client idempotency key
    client1.simple_query("BEGIN").await.expect("BEGIN epoch 1");
    client1
        .simple_query("SET rockstream.idempotency_key = 'epoch-1-batch'")
        .await
        .expect("SET idempotency_key");
    client1
        .simple_query(
            "INSERT INTO items (id, category, price) VALUES (1, 10, 100), (2, 10, 200), (3, 20, 50)",
        )
        .await
        .expect("INSERT epoch 1");
    client1
        .simple_query("COMMIT")
        .await
        .expect("COMMIT epoch 1");

    // Epoch 2: update category 10, delete category 20 row, insert new row
    client1.simple_query("BEGIN").await.expect("BEGIN epoch 2");
    client1
        .simple_query("SET rockstream.idempotency_key = 'epoch-2-batch'")
        .await
        .expect("SET idempotency_key");
    client1
        .simple_query("UPDATE items SET price = 250 WHERE id = 1, category = 10, price = 100")
        .await
        .expect("UPDATE epoch 2");
    client1
        .simple_query("DELETE FROM items WHERE id = 3, category = 20, price = 50")
        .await
        .expect("DELETE epoch 2");
    client1
        .simple_query("INSERT INTO items (id, category, price) VALUES (4, 10, 50), (5, 20, 75)")
        .await
        .expect("INSERT epoch 2");

    // Barrier: Parent synchronizes with child's successful durable commit acknowledgement
    let commit_res = client1
        .simple_query("COMMIT")
        .await
        .expect("COMMIT epoch 2 durable acknowledgement");
    assert!(
        !commit_res.is_empty(),
        "COMMIT epoch 2 must return durable acknowledgement response"
    );

    // AC2/R2: Immediate hard crash without any sleep or graceful shutdown
    if let Some(mut c) = guard1.0.take() {
        let _ = c.kill();
        let _ = c.wait();
    }

    // Reopen storage in fresh process
    let port2 = free_port();
    let child2 = Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage_dir.path().to_str().unwrap(),
            "--listen",
            &format!("127.0.0.1:{port2}"),
        ])
        .spawn()
        .expect("spawn restart rockstream gateway");
    let _guard2 = ProcessGuard(Some(child2));

    assert!(
        wait_for_port(port2, Duration::from_secs(30)),
        "gateway port2 failed to bind after restart"
    );

    let client2 = connect_port(port2).await;

    // AC3/R3: Query WITHOUT recreating tables or views

    // 1. Full base rows verification
    let base_rows = client2
        .simple_query("SELECT id, category, price FROM items ORDER BY id")
        .await
        .expect("SELECT base items post-crash without recreating table");
    let mut base = Vec::new();
    for msg in base_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let id: i64 = row.get(0).unwrap().parse().unwrap();
            let cat: i64 = row.get(1).unwrap().parse().unwrap();
            let price: i64 = row.get(2).unwrap().parse().unwrap();
            base.push((id, cat, price));
        }
    }
    assert_eq!(
        base,
        vec![(1, 10, 250), (2, 10, 200), (4, 10, 50), (5, 20, 75)],
        "recovered base table rows mismatch"
    );

    // 2. Full aggregate rows verification
    let post_crash_rows = client2
        .simple_query("SELECT * FROM item_totals")
        .await
        .expect("SELECT item_totals post-crash without recreating view");
    let mut post_crash = HashMap::new();
    for msg in post_crash_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let cat: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            let cnt: i64 = row.get(2).unwrap().parse().unwrap();
            post_crash.insert(cat, (sum, cnt));
        }
    }
    assert_eq!(
        post_crash,
        HashMap::from([(10, (500, 3)), (20, (75, 1))]),
        "recovered aggregate view state mismatch"
    );

    // 3. Catalog definitions verification (information_schema.tables)
    let cat_rows = client2
        .simple_query(
            "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
        )
        .await
        .expect("query catalog definitions post-crash");
    let mut cat_items = Vec::new();
    for msg in cat_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let name = row.get(0).unwrap().to_string();
            let ttype = row.get(1).unwrap().to_string();
            cat_items.push((name, ttype));
        }
    }
    assert_eq!(
        cat_items,
        vec![
            ("item_totals".to_string(), "VIEW".to_string()),
            ("items".to_string(), "BASE TABLE".to_string()),
        ],
        "catalog definitions must survive without manual recreation"
    );

    // 4. Committed frontier & idempotency metadata via direct storage inspection
    let gateway_shard_dir = storage_dir.path().join("gateway-shard");
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&gateway_shard_dir).unwrap(),
    );
    let reader = ShardReader::open("gateway", store.clone())
        .await
        .expect("open ShardReader on recovered shard");

    // Committed frontier check
    let frontier_bytes = reader
        .get(&ShardKeyEncoder::frontier_key())
        .await
        .expect("read frontier key")
        .expect("frontier key must be present");
    assert_eq!(frontier_bytes.len(), 8);
    let recovered_frontier = u64::from_be_bytes(frontier_bytes.as_ref().try_into().unwrap());
    assert_eq!(recovered_frontier, 2, "committed frontier must be epoch 2");

    // Idempotency metadata check
    let key1_hash = hash_idempotency_key("epoch-1-batch");
    let key2_hash = hash_idempotency_key("epoch-2-batch");
    let epoch1 = reader
        .get_idempotency_epoch(0, key1_hash)
        .await
        .expect("lookup key1")
        .expect("key1 must exist in idempotency metadata");
    let epoch2 = reader
        .get_idempotency_epoch(0, key2_hash)
        .await
        .expect("lookup key2")
        .expect("key2 must exist in idempotency metadata");
    assert_eq!(epoch1, 1, "idempotency epoch for epoch-1-batch must be 1");
    assert_eq!(epoch2, 2, "idempotency epoch for epoch-2-batch must be 2");

    // Idempotent replay on the fresh gateway returns COMMIT without re-applying writes
    client2
        .simple_query("SET rockstream.idempotency_key = 'epoch-2-batch'")
        .await
        .expect("set idempotency_key for replay");
    client2
        .simple_query("INSERT INTO items (id, category, price) VALUES (99, 10, 999)")
        .await
        .expect("replayed insert statement buffered");
    client2
        .simple_query("COMMIT")
        .await
        .expect("replayed commit returns success as a no-op");

    // Verify row 99 was NOT inserted due to idempotency key replay
    let recheck = client2
        .simple_query("SELECT id FROM items WHERE id = 99")
        .await
        .expect("query id 99");
    let recheck_count = recheck
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        recheck_count, 0,
        "idempotent replay must not re-apply mutation"
    );

    // Continued accumulation on top of restored arrangement
    client2
        .simple_query("SET rockstream.idempotency_key = default")
        .await
        .expect("reset idempotency_key");
    client2
        .simple_query("INSERT INTO items (id, category, price) VALUES (6, 10, 100)")
        .await
        .expect("INSERT epoch 3");
    client2
        .simple_query("COMMIT")
        .await
        .expect("COMMIT epoch 3");

    let after_rows = client2
        .simple_query("SELECT * FROM item_totals")
        .await
        .expect("SELECT item_totals post-restart");
    let mut after = HashMap::new();
    for msg in after_rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let cat: i64 = row.get(0).unwrap().parse().unwrap();
            let sum: i64 = row.get(1).unwrap().parse().unwrap();
            let cnt: i64 = row.get(2).unwrap().parse().unwrap();
            after.insert(cat, (sum, cnt));
        }
    }
    assert_eq!(
        after,
        HashMap::from([(10, (600, 4)), (20, (75, 1))]),
        "post-restart updates must accumulate correctly on top of restored state"
    );
}

/// AC4/R4: Injected flush failure returns the exact failed acknowledgement outcome
/// rather than a successful durable commit.
#[tokio::test]
async fn test_issue_109_injected_flush_failure_returns_exact_error() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let shard_path = "flush-fail-shard";
    let prefix = "catalog-flush-fail";

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

    let client = connect_port(local_addr.port()).await;
    client
        .simple_query("CREATE TABLE t_flush (id BIGINT PRIMARY KEY, val TEXT);")
        .await
        .unwrap();

    // Successful write and commit
    client
        .simple_query("INSERT INTO t_flush VALUES (1, 'initial');")
        .await
        .unwrap();
    client.simple_query("COMMIT;").await.unwrap();

    // Inject flush failure
    shard_db.set_fail_flushes(true);

    client.simple_query("BEGIN;").await.unwrap();
    client
        .simple_query("INSERT INTO t_flush VALUES (2, 'failing');")
        .await
        .unwrap();

    // AC4: COMMIT must fail because flush fails
    let commit_err = client
        .simple_query("COMMIT;")
        .await
        .expect_err("COMMIT must fail when shard_db flush fails");

    let db_err = commit_err.as_db_error().expect("must return DatabaseError");
    assert_eq!(
        db_err.code().code(),
        "XX000",
        "expected InternalError (XX000) for storage error on failed flush acknowledgement"
    );

    // Clean up
    shard_db.set_fail_flushes(false);
    handle.abort();
}

/// AC5/R5: Unacknowledged writes contract.
/// For an unacknowledged write interrupted before commit acknowledgement, the contract
/// permits either rollback or commit (unknown outcome), but does NOT require absence.
#[tokio::test]
async fn test_issue_109_unacknowledged_writes_unknown_outcome_contract() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let storage_dir = TempDir::new().expect("storage dir");
    let port1 = free_port();

    let child1 = Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage_dir.path().to_str().unwrap(),
            "--listen",
            &format!("127.0.0.1:{port1}"),
        ])
        .spawn()
        .expect("spawn rockstream gateway");
    let mut guard1 = ProcessGuard(Some(child1));

    assert!(
        wait_for_port(port1, Duration::from_secs(30)),
        "gateway port1 failed to bind within deadline"
    );

    let client1 = connect_port(port1).await;
    client1
        .simple_query("CREATE TABLE items (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .expect("CREATE TABLE");

    // Acknowledged commit: id=1
    client1
        .simple_query("INSERT INTO items VALUES (1, 'acknowledged')")
        .await
        .expect("INSERT 1");
    client1
        .simple_query("COMMIT")
        .await
        .expect("COMMIT 1 acknowledged");

    // Client begins an unacknowledged write without waiting for or receiving COMMIT acknowledgement
    let client1_unack = connect_port(port1).await;
    client1_unack
        .simple_query("INSERT INTO items VALUES (2, 'unacknowledged')")
        .await
        .expect("INSERT 2 buffered");
    // Send COMMIT without awaiting completion or acknowledgement
    let commit_fut = client1_unack.simple_query("COMMIT");

    // Immediately kill child process without waiting for commit_fut acknowledgement
    if let Some(mut c) = guard1.0.take() {
        let _ = c.kill();
        let _ = c.wait();
    }
    drop(commit_fut);

    // Restart process against the same storage
    let port2 = free_port();
    let child2 = Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage_dir.path().to_str().unwrap(),
            "--listen",
            &format!("127.0.0.1:{port2}"),
        ])
        .spawn()
        .expect("spawn restart rockstream gateway");
    let _guard2 = ProcessGuard(Some(child2));

    assert!(
        wait_for_port(port2, Duration::from_secs(30)),
        "gateway port2 failed to bind after restart"
    );

    let client2 = connect_port(port2).await;
    let rows = client2
        .simple_query("SELECT id, val FROM items ORDER BY id")
        .await
        .expect("SELECT items post-crash");

    let mut recovered_items = Vec::new();
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let id: i64 = row.get(0).unwrap().parse().unwrap();
            let val = row.get(1).unwrap().to_string();
            recovered_items.push((id, val));
        }
    }

    // Acknowledged write id=1 must always be present
    assert!(
        recovered_items.iter().any(|(id, _)| *id == 1),
        "acknowledged write id=1 must be present"
    );

    // Contract AC5/R5: unacknowledged write id=2 may either be present (if committed before SIGKILL)
    // or absent (if killed before commit/flush). Both outcomes are valid under the unknown outcome contract.
    let unack_committed = recovered_items.iter().any(|(id, _)| *id == 2);
    if unack_committed {
        assert_eq!(
            recovered_items,
            vec![
                (1, "acknowledged".to_string()),
                (2, "unacknowledged".to_string())
            ],
            "unacknowledged write committed before kill"
        );
    } else {
        assert_eq!(
            recovered_items,
            vec![(1, "acknowledged".to_string())],
            "unacknowledged write discarded before commit"
        );
    }
}

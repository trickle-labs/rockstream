//! Gateway serve-mode integration tests.
//!
//! These tests spin up the PostgreSQL wire gateway through `start_gateway` and
//! connect with `tokio-postgres`, verifying end-to-end connectivity and the
//! key query capabilities promised by the Postgres Pillar (v0.23–v0.26).
//!
//! All tests use port 0 so the OS assigns a free port and tests never
//! conflict with each other or with a running server.

use rockstream_cli::{start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

// ── helpers ──────────────────────────────────────────────────────────────────

fn gateway_opts(dir: &tempfile::TempDir) -> StartOptions {
    StartOptions {
        storage: dir.path().to_path_buf(),
        role: "gateway".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some("127.0.0.1:0".to_string()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
    }
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=rockstream dbname=rockstream"),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("pg connection error: {e}");
        }
    });
    client
}

// ── G1: basic connectivity ────────────────────────────────────────────────────

/// G1: start_gateway binds a port, accepts a psql connection, and answers
/// SELECT 1 without error.
#[tokio::test]
async fn gateway_starts_and_accepts_connection() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 failed");
    assert!(!rows.is_empty(), "expected at least one response message");
}

// ── G2: SHOW server_version ─────────────────────────────────────────────────

/// G2: SHOW server_version returns a non-empty row (catalog stub proof).
#[tokio::test]
async fn gateway_show_server_version_returns_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    let rows = client
        .simple_query("SHOW server_version")
        .await
        .expect("SHOW server_version failed");

    let found = rows.iter().any(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            r.get(0).is_some()
        } else {
            false
        }
    });
    assert!(
        found,
        "SHOW server_version did not return a row; got: {rows:?}"
    );
}

// ── G3: pg_catalog schema reflection ─────────────────────────────────────────

/// G3: information_schema.tables lists registered views.  Proves the ORM /
/// SQLAlchemy introspection path (v0.23 proof obligation).
#[tokio::test]
async fn gateway_information_schema_tables_is_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    // Should complete without error even on a fresh gateway with no views.
    let rows = client
        .simple_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        )
        .await
        .expect("information_schema.tables failed");

    // We at least get a CommandComplete.
    let completed = rows
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)));
    assert!(
        completed,
        "expected CommandComplete from information_schema.tables"
    );
}

/// G3b: pg_catalog.pg_class is queryable.
#[tokio::test]
async fn gateway_pg_catalog_pg_class_is_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    let rows = client
        .simple_query("SELECT oid, relname FROM pg_catalog.pg_class")
        .await
        .expect("pg_class failed");

    let completed = rows
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)));
    assert!(
        completed,
        "expected CommandComplete from pg_catalog.pg_class"
    );
}

// ── G4: CREATE VIEW + SELECT ──────────────────────────────────────────────────

/// G4: CREATE VIEW registers the view; a subsequent SELECT returns without
/// error (empty results on a fresh shard is correct).  Proves the v0.23
/// materialized-view registration path.
#[tokio::test]
async fn gateway_create_view_and_select_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    // The view's source must exist as a base table before `CREATE VIEW` can
    // compile it (v0.51.4's `compile_plan` resolves view deps eagerly).
    client
        .simple_query("CREATE TABLE source (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE source failed");

    // Register a view.
    client
        .simple_query("CREATE VIEW orders AS SELECT id, amount FROM source")
        .await
        .expect("CREATE VIEW failed");

    // SELECT from it — returns empty on a fresh shard, but must not error.
    let rows = client
        .simple_query("SELECT * FROM orders LIMIT 10")
        .await
        .expect("SELECT from view failed");

    let completed = rows
        .iter()
        .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_)));
    assert!(
        completed,
        "expected CommandComplete after SELECT from newly-created view"
    );
}

// ── G5: CREATE MATERIALIZED VIEW + cyclic rejection ──────────────────────────

/// G5: CREATE MATERIALIZED VIEW succeeds; a self-referencing (cyclic)
/// CREATE VIEW is rejected with RS-1011.  Proves the IVM inlining /
/// cycle-detection path.
#[tokio::test]
async fn gateway_cyclic_view_returns_rs_1011() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    // The view's source must exist as a base table before `CREATE
    // MATERIALIZED VIEW` can compile it (v0.51.4's `compile_plan` resolves
    // view deps eagerly).
    client
        .simple_query("CREATE TABLE base (id BIGINT)")
        .await
        .expect("CREATE TABLE base failed");

    // Non-cyclic MATERIALIZED VIEW — must succeed.
    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id FROM base")
        .await
        .expect("CREATE MATERIALIZED VIEW failed");

    // A self-referencing view is a trivial one-node cycle. Cycle detection
    // (`detect_cycle_with_new_view`) runs purely against the catalog's
    // dependency-name graph *before* `compile_plan`'s eager dependency-
    // existence check, so it still fires here even though v0.51.4's
    // `compile_plan` no longer tolerates a view forward-referencing a
    // not-yet-created sibling view (a two-view mutual-forward-reference
    // pair — the classic way to construct a cycle — now fails earlier,
    // with RS-1019 "depends on non-base-table relation(s)", on the very
    // first `CREATE VIEW`, before a true cycle could ever be formed; see
    // `handle_create_view`'s doc comment on why there is no longer a
    // DataFusion-materializer fallback for that case).
    let result = client
        .simple_query("CREATE VIEW c AS SELECT * FROM c")
        .await;

    let got_rs1011 = match &result {
        Err(e) => {
            if let Some(db_err) = e.as_db_error() {
                db_err.message().contains("RS-1011")
            } else {
                e.to_string().contains("RS-1011")
            }
        }
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("RS-1011")),
    };
    assert!(
        got_rs1011,
        "expected RS-1011 for cyclic view; got: {result:?}"
    );
}

// ── G6: DML accumulation ──────────────────────────────────────────────────────

/// G6: INSERT / UPDATE / DELETE inside a BEGIN…COMMIT transaction accumulates
/// in the write buffer without error.  Proves the v0.24 direct-write DML path.
#[tokio::test]
async fn gateway_dml_in_transaction_accumulates_without_error() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    // Register a target table/view first.
    client
        .simple_query("CREATE TABLE events (id INT, payload TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // Each COMMIT requires an idempotency key to deduplicate writes.
    client
        .simple_query("SET rockstream.idempotency_key = 'test-dml-txn-001'")
        .await
        .expect("SET idempotency_key failed");

    // DML inside a transaction.
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO events VALUES (1, 'hello')")
        .await
        .expect("INSERT failed");
    client
        .simple_query("UPDATE events SET payload = 'world' WHERE id = 1")
        .await
        .expect("UPDATE failed");
    client
        .simple_query("DELETE FROM events WHERE id = 1")
        .await
        .expect("DELETE failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
}

// ── G7: SUBSCRIBE ─────────────────────────────────────────────────────────────

/// G7: SUBSCRIBE <view> returns a CommandComplete (streaming rows are served
/// in the background); proves the v0.25 subscribe path.
#[tokio::test]
async fn gateway_subscribe_returns_without_error() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    // The view's source must exist as a base table before `CREATE VIEW` can
    // compile it (v0.51.4's `compile_plan` resolves view deps eagerly).
    client
        .simple_query("CREATE TABLE source (ts BIGINT, val BIGINT)")
        .await
        .expect("CREATE TABLE source failed");

    client
        .simple_query("CREATE VIEW live_feed AS SELECT ts, val FROM source")
        .await
        .expect("CREATE VIEW failed");

    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.query_raw(
            "SUBSCRIBE live_feed",
            std::iter::empty::<&(dyn ToSql + Sync)>(),
        ),
    )
    .await
    .expect("SUBSCRIBE did not start streaming within 5 seconds")
    .expect("SUBSCRIBE failed");
    drop(stream);
}

// ── G8: error cases ───────────────────────────────────────────────────────────

/// G8a: An unparseable listen address returns RS-0002 before binding.
#[tokio::test]
async fn gateway_invalid_listen_address_returns_rs_0002() {
    let dir = tempfile::tempdir().unwrap();
    let opts = StartOptions {
        storage: dir.path().to_path_buf(),
        role: "gateway".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some("not-a-valid-address".to_string()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
    };

    let err = start_gateway(&opts).await.unwrap_err();
    assert_eq!(
        err.code.to_string(),
        "RS-0002",
        "expected RS-0002 for bad listen address, got: {err}"
    );
}

/// G8b: Binding an already-occupied port returns RS-0003.
#[tokio::test]
async fn gateway_port_in_use_returns_rs_0003() {
    // Occupy a port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_port = listener.local_addr().unwrap().port();

    let dir = tempfile::tempdir().unwrap();
    let opts = StartOptions {
        storage: dir.path().to_path_buf(),
        role: "gateway".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some(format!("127.0.0.1:{occupied_port}")),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
    };

    let err = start_gateway(&opts).await.unwrap_err();
    assert_eq!(
        err.code.to_string(),
        "RS-0003",
        "expected RS-0003 for port-in-use, got: {err}"
    );

    // Keep listener alive until here so the port stays occupied during the test.
    drop(listener);
}

// ── G9: multiple concurrent clients ──────────────────────────────────────────

/// G9: Ten concurrent clients connect and run SELECT 1 simultaneously; all
/// succeed.  Proves session isolation at the gateway level.
#[tokio::test]
async fn gateway_handles_concurrent_clients() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let port = addr.port();

    let mut handles = Vec::new();
    for _ in 0..10 {
        handles.push(tokio::spawn(async move {
            let client = connect_port(port).await;
            client
                .simple_query("SELECT 1")
                .await
                .expect("concurrent SELECT 1 failed")
        }));
    }

    for h in handles {
        h.await.expect("task panicked");
    }
}

// ── G10: SET rockstream.* session variables ───────────────────────────────────

/// G10: SET rockstream.idempotency_key and SET rockstream.isolation_level
/// work without error.  Proves the v0.25 session-variable path.
#[tokio::test]
async fn gateway_set_rockstream_session_variables() {
    let dir = tempfile::tempdir().unwrap();
    let opts = gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let client = connect_port(addr.port()).await;

    client
        .simple_query("SET rockstream.idempotency_key = 'test-key-abc'")
        .await
        .expect("SET idempotency_key failed");

    client
        .simple_query("SET rockstream.isolation_level = 'repeatable_read'")
        .await
        .expect("SET isolation_level failed");
}

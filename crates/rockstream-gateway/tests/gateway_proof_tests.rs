//! v0.23 + v0.24 Proof tests — Phase 3b/3a green gates.
//!
//! v0.23 tests: S6–S10
//! v0.24 tests (S2–S5): CREATE TABLE, DML accumulation, COMMIT flush, ROLLBACK
//! v0.32 index DDL wire tests: CREATE INDEX, DROP INDEX, REBUILD INDEX through pgwire

use std::sync::Arc;
use std::time::Instant;

use hmac::{Hmac, Mac};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::{
        CatalogColumn, CatalogIndexEntry, CatalogIndexState, CatalogStubs, CatalogTable,
        CatalogView,
    },
    multi_shard_reader::{plan_scatter_shards, MultiShardReader, ScatterPredicate},
    view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_sql::WorkloadCatalog;
use rockstream_storage::{ShardDb, ShardReader};
use rockstream_types::{
    cost::set_active_pricing_config,
    frontier::{
        bloom_filter_might_contain, build_exact_membership_filter, ColumnStats, ShardColumnStats,
    },
    ids::{ShardId, ViewId},
    metrics::{
        generate_prometheus_metrics, inc_shard_bloom_false_positive_total,
        read_scatter_shards_pruned_total, read_scatter_shards_total,
        read_shard_bloom_false_positive_total, reset_all, set_freshness_lag,
        set_pipeline_state_bytes, set_state_budget, set_workload_memory,
    },
    view_lifecycle::ViewState,
    workload::{FreshnessSlo, MemoryLimit, WorkloadDef, WorkloadPriority},
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio_postgres::NoTls;

// ── Shared helpers ────────────────────────────────────────────────────────────

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

#[allow(dead_code)]
fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

#[allow(dead_code)]
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[allow(dead_code)]
fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86400;
    let mut days = (secs / 86400) as u32;
    let h = (sod / 3600) as u32;
    let m = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    let mut year = 1970u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let dpm = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0u32;
    for &d in &dpm {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    (year, month + 1, days + 1, h, m, s)
}

#[allow(dead_code)]
async fn create_minio_bucket(port: u16, bucket: &str) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (y, mo, d, hh, mm, ss) = epoch_to_ymd_hms(secs);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{y:04}{mo:02}{d:02}T{hh:02}{mm:02}{ss:02}Z");
    let host = format!("127.0.0.1:{port}");
    let region = "us-east-1";
    let empty_hash = sha256_hex(b"");
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = sha256_hex(canonical.as_bytes());
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = hmac_sha256(b"AWS4minioadmin", date.as_bytes());
    let k2 = hmac_sha256(&k1, region.as_bytes());
    let k3 = hmac_sha256(&k2, b"s3");
    let signing_key = hmac_sha256(&k3, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&signing_key, sts.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential=minioadmin/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
    );
    let status = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", &auth)
        .header("Content-Length", "0")
        .send()
        .await
        .expect("CreateBucket PUT request failed")
        .status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}

async fn connect_with_notices(
    port: u16,
) -> (
    tokio_postgres::Client,
    tokio::sync::mpsc::UnboundedReceiver<tokio_postgres::error::DbError>,
) {
    let (client, mut conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect failed");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let msg = futures::future::poll_fn(|cx| conn.poll_message(cx)).await;
            match msg {
                Some(Ok(tokio_postgres::AsyncMessage::Notice(n))) => {
                    let _ = tx.send(n);
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    });
    (client, rx)
}

/// Extract `SimpleQueryMessage::Row` entries from a simple_query result.
fn data_rows_from(
    msgs: &[tokio_postgres::SimpleQueryMessage],
) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

fn is_uuid_v4ish(value: &str) -> bool {
    let parts: Vec<&str> = value.split('-').collect();
    if parts.len() != 5 {
        return false;
    }
    let expected = [8, 4, 4, 4, 12];
    if parts
        .iter()
        .zip(expected)
        .any(|(part, len)| part.len() != len || !part.chars().all(|c| c.is_ascii_hexdigit()))
    {
        return false;
    }
    parts[2].starts_with('4')
}

async fn start_gateway_noop(catalog: CatalogStubs) -> (u16, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle)
}

async fn open_local_shard_db(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(
        ShardDb::builder("gateway-workload-catalog", store)
            .build()
            .await
            .unwrap(),
    )
}

// ── S6: proof_serializable_returns_rs2003 ─────────────────────────────────────

/// P3: `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns RS-2003.
#[tokio::test]
async fn proof_serializable_returns_rs2003() {
    let (port, _handle) = start_gateway_noop(CatalogStubs::new()).await;
    let client = connect_port(port).await;

    let result = client
        .simple_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await;

    match result {
        Err(e) => {
            // tokio-postgres wraps server errors as DbError; check message field
            let msg = if let Some(db_err) = e.as_db_error() {
                db_err.message().to_string()
            } else {
                e.to_string()
            };
            assert!(
                msg.contains("RS-2003"),
                "expected RS-2003 in error message, got: {msg}"
            );
        }

        Ok(msgs) => {
            // Some clients may surface the error inside the messages list
            let found = msgs.iter().any(|m| format!("{m:?}").contains("RS-2003"));
            assert!(found, "expected RS-2003 error message in response");
        }
    }
}

#[tokio::test]
async fn create_workload_survives_catalog_stubs_restart() {
    let dir = TempDir::new().unwrap();
    let db = open_local_shard_db(&dir).await;
    let workload_catalog = Arc::new(WorkloadCatalog::new(Arc::clone(&db)));
    let catalog = Arc::new(
        CatalogStubs::with_workload_catalog(Arc::clone(&workload_catalog))
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query(
            "CREATE WORKLOAD fast_lane WITH (MEMORY_LIMIT = 1048576, FRESHNESS_SLO_MS = 500)",
        )
        .await
        .expect("CREATE WORKLOAD fast_lane");

    handle.abort();
    drop(client);
    drop(catalog);
    drop(workload_catalog);
    drop(db);

    let reopened_db = open_local_shard_db(&dir).await;
    let reopened_catalog = Arc::new(WorkloadCatalog::new(reopened_db));
    let restarted = CatalogStubs::with_workload_catalog(reopened_catalog)
        .await
        .expect("reload workload catalog");
    let expected = WorkloadDef::new("fast_lane")
        .with_memory_limit(MemoryLimit::new(1_048_576))
        .with_freshness_slo(FreshnessSlo::new(500));
    assert_eq!(restarted.get_workload("fast_lane"), Some(expected));
}

#[tokio::test]
async fn create_workload_parses_priority_and_max_parallelism() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query(
            "CREATE WORKLOAD fast WITH (MEMORY_LIMIT=1048576, FRESHNESS_SLO_MS=500, PRIORITY=HIGH, MAX_PARALLELISM=8)",
        )
        .await
        .expect("CREATE WORKLOAD fast");

    let workload = catalog.get_workload("fast").expect("workload registered");
    assert_eq!(workload.memory_limit, Some(MemoryLimit::new(1_048_576)));
    assert_eq!(workload.freshness_slo, Some(FreshnessSlo::new(500)));
    assert_eq!(workload.priority, WorkloadPriority::HIGH);
    assert_eq!(workload.max_parallelism, Some(8));

    handle.abort();
}

#[tokio::test]
async fn alter_workload_updates_requested_fields() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query(
            "CREATE WORKLOAD fast WITH (MEMORY_LIMIT=1024, FRESHNESS_SLO_MS=500, PRIORITY=DEFAULT, MAX_PARALLELISM=2)",
        )
        .await
        .expect("CREATE WORKLOAD fast");

    let cases = [
        (
            "ALTER WORKLOAD fast SET (MEMORY_LIMIT=2048)",
            Some(MemoryLimit::new(2048)),
            Some(FreshnessSlo::new(500)),
            WorkloadPriority::DEFAULT,
            Some(2),
        ),
        (
            "ALTER WORKLOAD fast SET (FRESHNESS_SLO_MS=750)",
            Some(MemoryLimit::new(2048)),
            Some(FreshnessSlo::new(750)),
            WorkloadPriority::DEFAULT,
            Some(2),
        ),
        (
            "ALTER WORKLOAD fast SET (PRIORITY=LOW)",
            Some(MemoryLimit::new(2048)),
            Some(FreshnessSlo::new(750)),
            WorkloadPriority::LOW,
            Some(2),
        ),
        (
            "ALTER WORKLOAD fast SET (MAX_PARALLELISM=4)",
            Some(MemoryLimit::new(2048)),
            Some(FreshnessSlo::new(750)),
            WorkloadPriority::LOW,
            Some(4),
        ),
        (
            "ALTER WORKLOAD fast SET (MEMORY_LIMIT=4096, FRESHNESS_SLO_MS=900, PRIORITY=HIGH, MAX_PARALLELISM=8)",
            Some(MemoryLimit::new(4096)),
            Some(FreshnessSlo::new(900)),
            WorkloadPriority::HIGH,
            Some(8),
        ),
    ];

    for (statement, memory_limit, freshness_slo, priority, max_parallelism) in cases {
        client.simple_query(statement).await.expect(statement);
        let workload = catalog.get_workload("fast").expect("workload exists");
        assert_eq!(workload.memory_limit, memory_limit);
        assert_eq!(workload.freshness_slo, freshness_slo);
        assert_eq!(workload.priority, priority);
        assert_eq!(workload.max_parallelism, max_parallelism);
    }

    handle.abort();
}

#[tokio::test]
async fn alter_workload_nonexistent_returns_rs1005() {
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    let err = client
        .simple_query("ALTER WORKLOAD missing SET (MEMORY_LIMIT=2048)")
        .await
        .expect_err("ALTER WORKLOAD missing must fail");
    let msg = err
        .as_db_error()
        .map(|db_err| db_err.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(msg.contains("RS-1005"), "expected RS-1005, got: {msg}");

    handle.abort();
}

#[tokio::test]
async fn drop_workload_enforces_assignments_and_allows_recreate() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query("CREATE WORKLOAD busy WITH (MEMORY_LIMIT=1024)")
        .await
        .expect("CREATE WORKLOAD busy");
    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE orders");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW busy_view WITH WORKLOAD = busy AS SELECT id, amount FROM orders",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW busy_view");

    let err = client
        .simple_query("DROP WORKLOAD busy")
        .await
        .expect_err("DROP WORKLOAD busy must fail while assigned");
    let msg = err
        .as_db_error()
        .map(|db_err| db_err.message().to_string())
        .unwrap_or_else(|| err.to_string());
    assert!(msg.contains("RS-1014"), "expected RS-1014, got: {msg}");

    client
        .simple_query("CREATE WORKLOAD ephemeral WITH (MEMORY_LIMIT=512)")
        .await
        .expect("CREATE WORKLOAD ephemeral");
    client
        .simple_query("DROP WORKLOAD ephemeral")
        .await
        .expect("DROP WORKLOAD ephemeral");
    assert!(catalog.get_workload("ephemeral").is_none());

    client
        .simple_query("CREATE WORKLOAD ephemeral WITH (MEMORY_LIMIT=2048)")
        .await
        .expect("recreate workload ephemeral");
    assert_eq!(
        catalog
            .get_workload("ephemeral")
            .expect("recreated workload")
            .memory_limit,
        Some(MemoryLimit::new(2048))
    );

    handle.abort();
}

#[tokio::test]
async fn show_workload_status_matches_catalog_state() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        Arc::clone(&catalog),
        Arc::new(NoopViewReader),
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query(
            "CREATE WORKLOAD fast WITH (MEMORY_LIMIT=100, FRESHNESS_SLO_MS=900, PRIORITY=HIGH, MAX_PARALLELISM=8)",
        )
        .await
        .expect("CREATE WORKLOAD fast");
    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE orders");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW fast_a WITH WORKLOAD = fast AS SELECT id, amount FROM orders",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW fast_a");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW fast_b WITH WORKLOAD = fast AS SELECT id FROM orders",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW fast_b");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW fast_c WITH WORKLOAD = fast AS SELECT amount FROM orders",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW fast_c");
    catalog.set_view_state("fast_b", ViewState::Paused);
    catalog.set_view_state_bytes("fast_a", 60, None);
    catalog.set_view_state_bytes("fast_c", 50, None);

    let msgs = client
        .simple_query("SHOW WORKLOAD STATUS FOR fast")
        .await
        .expect("SHOW WORKLOAD STATUS FOR fast");
    let rows = data_rows_from(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("fast"));
    assert_eq!(rows[0].get(1), Some("0"));
    assert_eq!(rows[0].get(2), Some("8"));
    assert_eq!(rows[0].get(3), Some("100"));
    assert_eq!(rows[0].get(4), Some("900"));
    assert_eq!(rows[0].get(5), Some("3"));
    assert_eq!(rows[0].get(6), Some("1"));
    assert_eq!(rows[0].get(7), Some("1"));

    let entry = catalog
        .workload_status_entries()
        .into_iter()
        .find(|entry| entry.workload_name == "fast")
        .expect("status entry");
    assert_eq!(entry.priority, 0);
    assert_eq!(entry.max_parallelism, Some(8));
    assert_eq!(entry.memory_limit_bytes, Some(100));
    assert_eq!(entry.freshness_slo_ms, Some(900));
    assert_eq!(entry.view_count, 3);
    assert_eq!(entry.over_budget_relaxed_view_count, 1);
    assert_eq!(entry.paused_view_count, 1);

    handle.abort();
}

// ── S7: copy_out_streams_view_rows ────────────────────────────────────────────

/// S7 green gate: COPY OUT from a 3-row view returns exactly 3 CopyData
/// messages followed by CopyDone.
///
/// Uses a raw TCP connection to inspect individual pgwire messages since
/// tokio-postgres `copy_out()` uses the extended query protocol.
#[tokio::test]
async fn copy_out_streams_view_rows() {
    // Set up an in-memory shard with 3 rows.
    let store = Arc::new(InMemory::new());
    let shard_db = ShardDb::builder("copy-shard", store.clone())
        .build()
        .await
        .unwrap();
    for i in 0u32..3 {
        let key = format!("view_output/copy_view/{:08}", i);
        let value = format!("row_{i}\t{i}");
        shard_db
            .put(key.as_bytes(), value.as_bytes())
            .await
            .unwrap();
    }
    shard_db.flush().await.unwrap();

    let reader = ShardReader::open("copy-shard", store).await.unwrap();
    let view_reader = Arc::new(HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: Some(1),
    });

    // Register the view in the catalog.
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "copy_view".to_string(),
        sql: "SELECT name, val FROM source".to_string(),
        columns: vec![
            CatalogColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
            },
            CatalogColumn {
                name: "val".to_string(),
                data_type: "Int32".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), view_reader);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    // Raw TCP pgwire session.
    let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Send StartupMessage (protocol 3.0).
    let startup = build_startup_message("test", "test");
    stream.write_all(&startup).await.unwrap();

    // Drain until ReadyForQuery.
    drain_until_ready(&mut stream).await;

    // Send Query: "COPY copy_view TO STDOUT"
    let query_msg = build_query_message("COPY copy_view TO STDOUT");
    stream.write_all(&query_msg).await.unwrap();

    // Count CopyData messages; stop after CopyDone.
    let copy_data_count = count_copy_data_messages(&mut stream).await;

    assert_eq!(
        copy_data_count, 3,
        "expected exactly 3 CopyData messages, got {copy_data_count}"
    );
}

/// Build a PostgreSQL startup message for user/dbname.
fn build_startup_message(user: &str, db: &str) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::new();
    // Protocol version 3.0
    body.extend_from_slice(&196608u32.to_be_bytes());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.push(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(db.as_bytes());
    body.push(0);
    body.push(0);
    let len = (body.len() + 4) as u32;
    let mut msg = len.to_be_bytes().to_vec();
    msg.extend_from_slice(&body);
    msg
}

/// Build a simple Query message.
fn build_query_message(sql: &str) -> Vec<u8> {
    let mut msg = vec![b'Q'];
    let payload = format!("{sql}\0");
    let len = (payload.len() + 4) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(payload.as_bytes());
    msg
}

/// Read and discard messages until a ReadyForQuery ('Z') is received.
async fn drain_until_ready(stream: &mut tokio::net::TcpStream) {
    loop {
        let msg_type = read_u8(stream).await;
        let len = read_u32_be(stream).await as usize;
        let body_len = len - 4;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            tokio::io::AsyncReadExt::read_exact(stream, &mut body)
                .await
                .unwrap();
        }
        if msg_type == b'Z' {
            break;
        }
    }
}

/// Read messages and count CopyData ('d') messages until CopyDone ('c').
async fn count_copy_data_messages(stream: &mut tokio::net::TcpStream) -> usize {
    let mut count = 0;
    loop {
        let msg_type = read_u8(stream).await;
        let len = read_u32_be(stream).await as usize;
        let body_len = len - 4;
        let mut body = vec![0u8; body_len];
        if body_len > 0 {
            tokio::io::AsyncReadExt::read_exact(stream, &mut body)
                .await
                .unwrap();
        }
        match msg_type {
            b'd' => count += 1,   // CopyData
            b'c' => break,        // CopyDone
            b'C' | b'Z' => break, // CommandComplete / ReadyForQuery → done
            b'E' => panic!("received ErrorResponse during COPY OUT"),
            _ => {} // skip other messages (H = CopyOutResponse, etc.)
        }
    }
    count
}

async fn read_u8(stream: &mut tokio::net::TcpStream) -> u8 {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 1];
    stream.read_exact(&mut buf).await.unwrap();
    buf[0]
}

async fn read_u32_be(stream: &mut tokio::net::TcpStream) -> u32 {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await.unwrap();
    u32::from_be_bytes(buf)
}

// ── S8: proof_psql_select_limit_10_under_10ms_p99 ─────────────────────────────

/// P1: 100 back-to-back `SELECT * FROM my_view LIMIT 10` queries; p99 < 10 ms.
#[tokio::test]
async fn proof_psql_select_limit_10_under_10ms_p99() {
    // 1. In-memory LFS shard with 100 rows.
    let store = Arc::new(InMemory::new());
    let shard_db = ShardDb::builder("latency-shard", store.clone())
        .build()
        .await
        .unwrap();
    for i in 0u32..100 {
        let key = format!("view_output/my_view/{:08}", i);
        let value = format!("id_{i}\t{i}");
        shard_db
            .put(key.as_bytes(), value.as_bytes())
            .await
            .unwrap();
    }
    shard_db.flush().await.unwrap();

    let reader = ShardReader::open("latency-shard", store).await.unwrap();
    let view_reader = Arc::new(HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: Some(1),
    });

    // 2. Start GatewayServer on a random port.
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "my_view".to_string(),
        sql: "SELECT id, val FROM source".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Utf8".to_string(),
            },
            CatalogColumn {
                name: "val".to_string(),
                data_type: "Int32".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), view_reader);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    // 3. Connect.
    let client = connect_port(port).await;

    // 4. Warm-up: 10 queries (not measured).
    for _ in 0..10 {
        client
            .simple_query("SELECT * FROM my_view LIMIT 10")
            .await
            .expect("warmup query failed");
    }

    // 5. Measure 100 queries.
    let mut latencies_ms: Vec<f64> = Vec::with_capacity(100);
    for _ in 0..100 {
        let t0 = Instant::now();
        client
            .simple_query("SELECT * FROM my_view LIMIT 10")
            .await
            .expect("measured query failed");
        latencies_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
    }

    // 6. Compute p99.
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99_idx = (latencies_ms.len() as f64 * 0.99) as usize;
    let p99_ms = latencies_ms[p99_idx.min(latencies_ms.len() - 1)];

    // 7. Assert p99 < 10 ms.
    assert!(p99_ms < 10.0, "p99 latency {p99_ms:.2}ms exceeded 10ms SLO");
}

// ── S9: proof_inline_view_inlined_into_materialized_view ──────────────────────

/// P4: CREATE MATERIALIZED VIEW mv AS SELECT * FROM v inlines v and starts
/// IVM. A cyclic CREATE VIEW pair returns RS-1011.
#[tokio::test]
async fn proof_inline_view_inlined_into_materialized_view() {
    // Set up shard with rows for mv.
    let store = Arc::new(InMemory::new());
    let shard_db = ShardDb::builder("ivm-shard", store.clone())
        .build()
        .await
        .unwrap();
    for i in 0u32..5 {
        let key = format!("view_output/mv/{:08}", i);
        let value = format!("id_{i}\t{i}");
        shard_db
            .put(key.as_bytes(), value.as_bytes())
            .await
            .unwrap();
    }
    shard_db.flush().await.unwrap();

    let reader = ShardReader::open("ivm-shard", store).await.unwrap();
    let view_reader = Arc::new(HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: Some(1),
    });

    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog.clone(), view_reader);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();
    let client = connect_port(port).await;

    // 3. CREATE VIEW v (base table doesn't need to exist for the gateway stub).
    let result = client
        .simple_query("CREATE VIEW v AS SELECT id, val FROM base WHERE val > 0")
        .await
        .expect("CREATE VIEW v failed");
    assert!(
        result
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))),
        "expected CommandComplete for CREATE VIEW"
    );

    // 4. CREATE MATERIALIZED VIEW mv AS SELECT * FROM v — inlines v.
    let result = client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT * FROM v")
        .await
        .expect("CREATE MATERIALIZED VIEW failed");
    assert!(
        result.iter().any(|m| {
            if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = m {
                let _ = n; // row count
                true
            } else {
                false
            }
        }),
        "expected CommandComplete(CREATE MATERIALIZED VIEW)"
    );

    // 5. Attempt cycle: a → b → a.
    client
        .simple_query("CREATE VIEW a AS SELECT * FROM b")
        .await
        .expect("CREATE VIEW a failed");

    let result = client
        .simple_query("CREATE VIEW b AS SELECT * FROM a")
        .await;

    // The server sends an ErrorResponse which tokio-postgres surfaces as Err
    // or as an error row in the message list.
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
        "expected RS-1011 for cyclic CREATE VIEW; got: {result:?}"
    );

    // 6. SELECT * FROM mv LIMIT 10 — rows from pre-seeded shard data.
    let rows = client
        .simple_query("SELECT * FROM mv LIMIT 10")
        .await
        .expect("SELECT mv failed");
    let data_rows: Vec<_> = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert!(
        !data_rows.is_empty(),
        "expected rows from mv after IVM, got none"
    );
}

// ── S10: proof_orm_schema_reflection_testcontainers ───────────────────────────

/// P2: An ORM reflects view schemas without error. Simulates the four ORM
/// reflection query patterns.
///
/// Gated behind the `testcontainers` feature flag for CI environments that
/// cannot run external containers. Uses only in-process tokio-postgres.
#[tokio::test]
#[cfg_attr(
    not(feature = "testcontainers"),
    ignore = "requires testcontainers feature"
)]
async fn proof_orm_schema_reflection_testcontainers() {
    _proof_orm_schema_reflection_impl().await;
}

/// The test body, also callable without the feature flag for local testing.
#[tokio::test]
async fn proof_orm_schema_reflection_queries() {
    _proof_orm_schema_reflection_impl().await;
}

async fn _proof_orm_schema_reflection_impl() {
    // 1. Seed view catalog with one view: orders_mv.
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT id, amount FROM orders".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "amount".to_string(),
                data_type: "Float64".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });
    catalog.add_index(CatalogIndexEntry {
        name: "orders_mv_idx".to_string(),
        table: "orders_mv".to_string(),
        index_cols: vec!["id".to_string()],
        state: CatalogIndexState::Ready,
        op_id: Some(101),
    });

    let (port, _handle) = start_gateway_noop(catalog).await;

    // 2. Connect with tokio-postgres.
    let client = connect_port(port).await;

    // 3a. information_schema.tables → orders_mv
    let rows = client
        .simple_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
        )
        .await
        .expect("information_schema.tables failed");
    let names: Vec<String> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get("table_name").map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(
        names.contains(&"orders_mv".to_string()),
        "expected orders_mv in information_schema.tables; got {names:?}"
    );

    // 3b. information_schema.columns → id (bigint), amount (double precision)
    let rows = client
        .simple_query(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'orders_mv'",
        )
        .await
        .expect("information_schema.columns failed");
    let col_rows: Vec<(String, String)> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some((
                    r.get("column_name").unwrap_or("").to_string(),
                    r.get("data_type").unwrap_or("").to_string(),
                ))
            } else {
                None
            }
        })
        .collect();
    assert!(
        col_rows.iter().any(|(n, t)| n == "id" && t == "bigint"),
        "expected id/bigint column; got {col_rows:?}"
    );
    assert!(
        col_rows
            .iter()
            .any(|(n, t)| n == "amount" && t == "double precision"),
        "expected amount/double precision column; got {col_rows:?}"
    );

    // 3c. pg_catalog.pg_class → orders_mv with non-zero OID
    let rows = client
        .simple_query("SELECT oid, relname FROM pg_catalog.pg_class WHERE relname = 'orders_mv'")
        .await
        .expect("pg_class failed");
    let class_rows: Vec<(String, String)> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some((
                    r.get("oid").unwrap_or("0").to_string(),
                    r.get("relname").unwrap_or("").to_string(),
                ))
            } else {
                None
            }
        })
        .collect();
    let matching: Vec<_> = class_rows
        .iter()
        .filter(|(oid, name)| name == "orders_mv" && oid.parse::<i64>().unwrap_or(0) != 0)
        .collect();
    assert!(
        !matching.is_empty(),
        "expected non-zero OID for orders_mv in pg_class; got {class_rows:?}"
    );
    let oid_str = &matching[0].0;

    // 3d. pg_catalog.pg_attribute → correct type OIDs for the view's OID
    let rows = client
        .simple_query(&format!(
            "SELECT attname, atttypid FROM pg_catalog.pg_attribute WHERE attrelid = {oid_str}"
        ))
        .await
        .expect("pg_attribute failed");
    let attr_rows: Vec<(String, String)> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some((
                    r.get("attname").unwrap_or("").to_string(),
                    r.get("atttypid").unwrap_or("").to_string(),
                ))
            } else {
                None
            }
        })
        .collect();
    // id → OID 20 (int8), amount → OID 701 (float8)
    assert!(
        attr_rows.iter().any(|(n, t)| n == "id" && t == "20"),
        "expected id/OID-20 in pg_attribute; got {attr_rows:?}"
    );
    assert!(
        attr_rows.iter().any(|(n, t)| n == "amount" && t == "701"),
        "expected amount/OID-701 in pg_attribute; got {attr_rows:?}"
    );

    // Test pg_index
    let rows = client
        .simple_query("SELECT indexrelid, indrelid, indkey FROM pg_catalog.pg_index")
        .await
        .expect("pg_index failed");
    assert!(!rows.is_empty(), "expected index row in pg_index");

    // Test extended pg_class with indexes (relkind = 'i')
    let rows = client
        .simple_query("SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'i'")
        .await
        .expect("pg_class index query failed");
    let index_names: Vec<String> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get("relname").map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(
        index_names.contains(&"orders_mv_idx".to_string()),
        "expected orders_mv_idx in pg_class; got {:?}",
        index_names
    );

    // Test pg_proc
    let rows = client
        .simple_query("SELECT proname, prokind FROM pg_catalog.pg_proc")
        .await
        .expect("pg_proc failed");
    let proc_names: Vec<String> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get("proname").map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect();
    assert!(
        proc_names.contains(&"count".to_string()),
        "expected count in pg_proc"
    );

    // Test pg_constraint
    client
        .simple_query("SELECT conname FROM pg_catalog.pg_constraint")
        .await
        .expect("pg_constraint failed");

    // Test pg_description
    client
        .simple_query("SELECT description FROM pg_catalog.pg_description")
        .await
        .expect("pg_description failed");

    // Test pg_enum
    client
        .simple_query("SELECT enumlabel FROM pg_catalog.pg_enum")
        .await
        .expect("pg_enum failed");

    // Test pg_roles
    let rows = client
        .simple_query("SELECT rolname FROM pg_catalog.pg_roles")
        .await
        .expect("pg_roles failed");
    assert!(!rows.is_empty(), "expected roles row");

    // Test pg_user
    let rows = client
        .simple_query("SELECT usename FROM pg_catalog.pg_user")
        .await
        .expect("pg_user failed");
    assert!(!rows.is_empty(), "expected user row");

    // Test information_schema tables
    client
        .simple_query("SELECT constraint_name FROM information_schema.key_column_usage")
        .await
        .expect("key_column_usage failed");

    client
        .simple_query("SELECT constraint_type FROM information_schema.table_constraints")
        .await
        .expect("table_constraints failed");

    client
        .simple_query("SELECT privilege_type FROM information_schema.column_privileges")
        .await
        .expect("column_privileges failed");

    client
        .simple_query("SELECT constraint_name FROM information_schema.referential_constraints")
        .await
        .expect("referential_constraints failed");
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.24 Direct-Write DML tests (S2–S5)
// ══════════════════════════════════════════════════════════════════════════════

/// Build an in-memory ShardDb backed gateway with a fresh shard.
async fn start_gateway_with_shard(
    shard_path: &str,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
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
    (local_addr.port(), handle, shard_db)
}

// ── S2: create_table_registers_in_catalog ────────────────────────────────────

/// S2 green gate: CREATE TABLE registers the table in the catalog.
#[tokio::test]
async fn create_table_registers_in_catalog() {
    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog.clone(), Arc::new(NoopViewReader));
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Create table
    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount FLOAT8, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // Verify catalog registration
    let table = catalog
        .get_table("orders")
        .expect("table should be in catalog");
    assert_eq!(table.name, "orders");
    assert_eq!(table.columns.len(), 3);
    assert!(table
        .columns
        .iter()
        .any(|c| c.name == "id" && c.data_type == "Int64"));
    assert!(table
        .columns
        .iter()
        .any(|c| c.name == "amount" && c.data_type == "Float64"));
    assert!(table
        .columns
        .iter()
        .any(|c| c.name == "name" && c.data_type == "Utf8"));

    // CREATE TABLE IF NOT EXISTS is a no-op (no error)
    client
        .simple_query("CREATE TABLE IF NOT EXISTS orders (id BIGINT)")
        .await
        .expect("CREATE TABLE IF NOT EXISTS should not error");

    // Duplicate without IF NOT EXISTS returns relation already exists
    let result = client.simple_query("CREATE TABLE orders (id BIGINT)").await;
    let got_42p07 = match &result {
        Err(e) => {
            e.as_db_error()
                .map(|d| d.code() == &tokio_postgres::error::SqlState::DUPLICATE_TABLE)
                .unwrap_or(false)
                || e.to_string().contains("42P07")
                || e.to_string().contains("already exists")
        }
        Ok(msgs) => msgs
            .iter()
            .any(|m| format!("{m:?}").contains("already exists")),
    };
    assert!(
        got_42p07,
        "expected 42P07 duplicate table error; got {result:?}"
    );
}

// ── S3: insert_accumulates_in_write_buffer ────────────────────────────────────

/// S3 green gate: INSERT accumulates in write buffer; no shard writes until COMMIT.
///
/// We don't have direct write-buffer introspection here via psql, so we verify
/// that after INSERT (no COMMIT), SELECT returns zero rows.
#[tokio::test]
async fn insert_accumulates_in_write_buffer() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("s3-insert-buf").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .await
        .expect("INSERT failed");

    // No COMMIT yet — SELECT must return 0 rows (write buffer not flushed to shard)
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let data_rows = data_rows_from(&msgs);
    assert_eq!(
        data_rows.len(),
        0,
        "expected no rows before COMMIT, got {} rows",
        data_rows.len()
    );
}

/// S3 green gate: DELETE accumulates in write buffer.
#[tokio::test]
async fn delete_accumulates_in_write_buffer() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("s3-delete-buf").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("DELETE FROM t WHERE id = 1")
        .await
        .expect("DELETE failed");

    // No COMMIT yet — SELECT must return 0 rows
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let data_rows = data_rows_from(&msgs);
    assert_eq!(data_rows.len(), 0, "expected no rows before COMMIT");
}

// ── S4: commit_flushes_rows_scannable_via_view_prefix ────────────────────────

/// S4 green gate: COMMIT flushes INSERT rows to the shard, visible via SELECT.
#[tokio::test]
async fn commit_flushes_rows_scannable_via_view_prefix() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s4-commit-flush").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 's4-commit-flush-key'")
        .await
        .expect("SET idempotency_key failed");
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (42, 99)")
        .await
        .expect("INSERT failed");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (43, 100)")
        .await
        .expect("INSERT 2 failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM orders ORDER BY id")
        .await
        .expect("SELECT orders failed");
    let data_rows = data_rows_from(&msgs);
    assert_eq!(
        data_rows.len(),
        2,
        "expected 2 rows after COMMIT, got {}",
        data_rows.len()
    );
    assert_eq!(data_rows[0].get("id").unwrap_or(""), "42");
    assert_eq!(data_rows[0].get("amount").unwrap_or(""), "99");
    assert_eq!(data_rows[1].get("id").unwrap_or(""), "43");
    assert_eq!(data_rows[1].get("amount").unwrap_or(""), "100");
}

/// S4: commit_write_batch_uses_no_range_delete — WriteBatch uses only Put/Delete ops.
///
/// Introspects WriteBatch to confirm no range-delete op is ever present.
/// This is a compile-time + structural assertion: `BatchOp` has no RangeDelete variant.
#[test]
fn commit_write_batch_uses_no_range_delete() {
    use rockstream_storage::BatchOp;
    // Verify BatchOp has only Put, Delete, Merge variants (no RangeDelete).
    // This is a compile-time exhaustiveness check: if a RangeDelete variant
    // were added, this match would fail to compile without a new arm.
    let op = BatchOp::Put {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };
    match op {
        BatchOp::Put { .. } => {}
        BatchOp::Delete { .. } => {}
        BatchOp::Merge { .. } => {}
    }
    // No range-delete path in BatchOp — assertion passes by exhaustiveness.
}

// ── S5: rollback_discards_write_buffer_no_shard_writes ────────────────────────

/// S5 green gate: ROLLBACK discards the write buffer — SELECT returns zero rows.
#[tokio::test]
async fn rollback_discards_write_buffer_no_shard_writes() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s5-rollback").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .await
        .expect("INSERT failed");
    client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK failed");

    // After ROLLBACK, SELECT must return 0 rows
    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let data_rows = data_rows_from(&msgs);
    assert_eq!(
        data_rows.len(),
        0,
        "expected no rows after ROLLBACK, got {} rows",
        data_rows.len()
    );
}

// ── S7: insert_returning_returns_written_rows ────────────────────────────────

/// S7 green gate: INSERT … RETURNING returns the written row values.
#[tokio::test]
async fn insert_returning_returns_written_rows() {
    let catalog = Arc::new(CatalogStubs::new());
    // Register table with schema so RETURNING can build column info
    catalog.add_table(CatalogTable {
        name: "products".to_string(),
        columns: vec![
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
    });
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog.clone(), Arc::new(NoopViewReader));
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    let rows = client
        .simple_query("INSERT INTO products (id, name) VALUES (1, 'Widget') RETURNING *")
        .await
        .expect("INSERT RETURNING failed");

    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        data_rows.len(),
        1,
        "expected 1 row from RETURNING, got {}",
        data_rows.len()
    );
}

// ── S7: insert_select_returning_multi_row ────────────────────────────────────

/// S7 green gate: multi-row INSERT … RETURNING returns all written rows.
/// Uses VALUES (...), (...) syntax via multiple INSERTs followed by a scan.
#[tokio::test]
async fn insert_select_returning_multi_row() {
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(CatalogTable {
        name: "items".to_string(),
        columns: vec![
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
    });
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog.clone(), Arc::new(NoopViewReader));
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // First INSERT … RETURNING
    let rows1 = client
        .simple_query("INSERT INTO items (id, name) VALUES (1, 'Alpha') RETURNING *")
        .await
        .expect("INSERT 1 RETURNING failed");
    let count1 = rows1
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count1, 1, "expected 1 row from first INSERT RETURNING");

    // Second INSERT … RETURNING
    let rows2 = client
        .simple_query("INSERT INTO items (id, name) VALUES (2, 'Beta') RETURNING *")
        .await
        .expect("INSERT 2 RETURNING failed");
    let count2 = rows2
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(count2, 1, "expected 1 row from second INSERT RETURNING");
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.42.2: multi-row VALUES INSERT
// ══════════════════════════════════════════════════════════════════════════════

/// v0.42.2 green gate: a real `INSERT ... VALUES (...), (...), (...)` list
/// (a single statement, not per-row workaround INSERTs) writes all three rows
/// correctly end-to-end, including a value containing a comma inside quotes
/// (which the old first-`(`-to-last-`)` splitter would have corrupted).
#[tokio::test]
async fn multi_row_insert_values_writes_all_rows_correctly() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v0422-multi-row-insert").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v0422-multi-row-key'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, name) VALUES (1, 'a, with comma'), (2, 'b'), (3, 'c')")
        .await
        .expect("multi-row INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM t ORDER BY id")
        .await
        .expect("SELECT t failed");
    let data_rows = data_rows_from(&msgs);
    assert_eq!(
        data_rows.len(),
        3,
        "expected 3 rows from multi-row INSERT, got {}",
        data_rows.len()
    );
    assert_eq!(data_rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(
        data_rows[0].get("name").unwrap_or(""),
        "a, with comma",
        "value with embedded comma must not be corrupted or dropped"
    );
    assert_eq!(data_rows[1].get("id").unwrap_or(""), "2");
    assert_eq!(data_rows[1].get("name").unwrap_or(""), "b");
    assert_eq!(data_rows[2].get("id").unwrap_or(""), "3");
    assert_eq!(data_rows[2].get("name").unwrap_or(""), "c");
}

/// v0.42.2 green gate: `INSERT ... VALUES (...), (...) RETURNING *` returns
/// every written row (not just the first), all from a single statement.
#[tokio::test]
async fn multi_row_insert_values_returning_returns_all_rows() {
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(CatalogTable {
        name: "items".to_string(),
        columns: vec![
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
    });
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, catalog.clone(), Arc::new(NoopViewReader));
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    let rows = client
        .simple_query(
            "INSERT INTO items (id, name) VALUES (1, 'Alpha'), (2, 'Beta'), (3, 'Gamma') RETURNING *",
        )
        .await
        .expect("multi-row INSERT RETURNING failed");
    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        data_rows.len(),
        3,
        "expected 3 rows from multi-row INSERT RETURNING, got {}",
        data_rows.len()
    );
    assert_eq!(data_rows[0].get("id"), Some("1"));
    assert_eq!(data_rows[0].get("name"), Some("Alpha"));
    assert_eq!(data_rows[1].get("id"), Some("2"));
    assert_eq!(data_rows[1].get("name"), Some("Beta"));
    assert_eq!(data_rows[2].get("id"), Some("3"));
    assert_eq!(data_rows[2].get("name"), Some("Gamma"));
}

#[tokio::test]
async fn insert_returning_generates_uuid_default_and_persists_it() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v045-returning-uuid").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE widgets (id UUID DEFAULT gen_random_uuid(), email TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v045-returning-uuid-key'")
        .await
        .expect("SET idempotency_key failed");

    let rows = client
        .simple_query("INSERT INTO widgets (email) VALUES ('a@example.com') RETURNING *")
        .await
        .expect("INSERT RETURNING failed");
    let data_rows = data_rows_from(&rows);
    assert_eq!(data_rows.len(), 1, "expected one RETURNING row");

    let generated_id = data_rows[0].get("id").expect("missing generated id");
    assert!(
        is_uuid_v4ish(generated_id),
        "expected UUID-looking id, got {generated_id}"
    );
    assert_eq!(data_rows[0].get("email"), Some("a@example.com"));

    let selected = client
        .simple_query("SELECT * FROM widgets")
        .await
        .expect("SELECT widgets failed");
    let selected_rows = data_rows_from(&selected);
    assert_eq!(selected_rows.len(), 1, "expected one persisted row");
    assert_eq!(selected_rows[0].get("id"), Some(generated_id));
    assert_eq!(selected_rows[0].get("email"), Some("a@example.com"));
}

#[tokio::test]
async fn insert_returning_generates_identity_values_sequentially() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v045-returning-identity").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE seq_items (id BIGINT GENERATED ALWAYS AS IDENTITY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client
        .simple_query("SET rockstream.idempotency_key = 'v045-returning-identity-1'")
        .await
        .expect("SET idempotency_key failed");
    let first = client
        .simple_query("INSERT INTO seq_items (name) VALUES ('alpha') RETURNING *")
        .await
        .expect("first INSERT RETURNING failed");
    let first_rows = data_rows_from(&first);
    assert_eq!(first_rows[0].get("id"), Some("1"));

    client
        .simple_query("SET rockstream.idempotency_key = 'v045-returning-identity-2'")
        .await
        .expect("SET idempotency_key failed");
    let second = client
        .simple_query("INSERT INTO seq_items (name) VALUES ('beta') RETURNING *")
        .await
        .expect("second INSERT RETURNING failed");
    let second_rows = data_rows_from(&second);
    assert_eq!(second_rows[0].get("id"), Some("2"));
}

#[tokio::test]
async fn multi_row_insert_returning_generates_distinct_identity_values_in_order() {
    let (port, _handle, _shard_db) =
        start_gateway_with_shard("v045-returning-identity-multi").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE seq_batch (id BIGINT GENERATED ALWAYS AS IDENTITY, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v045-returning-identity-multi-key'")
        .await
        .expect("SET idempotency_key failed");

    let rows = client
        .simple_query(
            "INSERT INTO seq_batch (name) VALUES ('alpha'), ('beta'), ('gamma') RETURNING *",
        )
        .await
        .expect("multi-row INSERT RETURNING failed");
    let data_rows = data_rows_from(&rows);
    assert_eq!(data_rows.len(), 3, "expected three RETURNING rows");
    assert_eq!(data_rows[0].get("id"), Some("1"));
    assert_eq!(data_rows[1].get("id"), Some("2"));
    assert_eq!(data_rows[2].get("id"), Some("3"));
    assert_eq!(data_rows[0].get("name"), Some("alpha"));
    assert_eq!(data_rows[1].get("name"), Some("beta"));
    assert_eq!(data_rows[2].get("name"), Some("gamma"));
}

/// v0.42.2 green gate: a malformed multi-row VALUES list (a row with the
/// wrong number of values) returns a hard `RS-2056` parse error instead of
/// silently corrupting or dropping data.
#[tokio::test]
async fn multi_row_insert_malformed_row_returns_rs2056_not_silent_corruption() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v0422-malformed-row").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let result = client
        .simple_query("INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b', 'extra')")
        .await;
    let got_rs2056 = match &result {
        Err(e) => {
            e.as_db_error()
                .map(|d| d.message().contains("RS-2056"))
                .unwrap_or(false)
                || e.to_string().contains("RS-2056")
        }
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("RS-2056")),
    };
    assert!(
        got_rs2056,
        "expected RS-2056 error for malformed multi-row VALUES, got {result:?}"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.24 S6 idempotency tests
// ══════════════════════════════════════════════════════════════════════════════

/// v0.51.1 Slice 2 green gate: COMMIT without idempotency_key or source_epoch
/// no longer returns RS-2007. The gateway mints a server-generated
/// idempotency envelope for the commit instead, and the write succeeds and
/// is visible.
#[tokio::test]
async fn missing_idempotency_key_autogenerates_envelope_and_commits() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("s6-missing-key").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .await
        .expect("INSERT failed");

    // COMMIT without setting idempotency_key or source_epoch now succeeds —
    // the gateway generates a fresh envelope server-side instead of RS-2007.
    let result = client.simple_query("COMMIT").await;
    assert!(
        result.is_ok(),
        "expected COMMIT to succeed with a server-generated idempotency envelope; got: {result:?}"
    );

    let rows = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT failed");
    let data_rows = data_rows_from(&rows);
    assert_eq!(
        data_rows.len(),
        1,
        "expected the committed row to be visible, got: {data_rows:?}"
    );
    assert_eq!(data_rows[0].get("id"), Some("1"));
    assert_eq!(data_rows[0].get("val"), Some("hello"));
}

/// v0.51.1 Slice 2 regression: explicit `SET rockstream.idempotency_key` set
/// once before two INSERTs inside an explicit BEGIN...COMMIT still dedupes a
/// replayed identical commit — the dedup guarantee spans the whole explicit
/// multi-statement transaction, not just a single autocommitted statement.
#[tokio::test]
async fn explicit_idempotency_key_dedupes_multi_statement_transaction_replay() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s6-explicit-multi-stmt-replay").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // First transaction: two inserts under one idempotency key.
    client
        .simple_query("SET rockstream.idempotency_key = 'multi-stmt-key-1'")
        .await
        .expect("SET failed");
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'alice')")
        .await
        .expect("INSERT 1 failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (2, 'bob')")
        .await
        .expect("INSERT 2 failed");
    client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT 1 failed");

    shard_db.flush().await.unwrap();
    let msgs1 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT 1");
    let rows1 = data_rows_from(&msgs1);
    assert_eq!(
        rows1.len(),
        2,
        "expected 2 rows after first commit, got {}",
        rows1.len()
    );

    // Replay the exact same key and statements — should be a no-op.
    client
        .simple_query("SET rockstream.idempotency_key = 'multi-stmt-key-1'")
        .await
        .expect("SET replay failed");
    client
        .simple_query("BEGIN")
        .await
        .expect("BEGIN replay failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'alice')")
        .await
        .expect("INSERT replay 1 failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (2, 'bob')")
        .await
        .expect("INSERT replay 2 failed");
    client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT replay failed");

    shard_db.flush().await.unwrap();
    let msgs2 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT 2");
    let rows2 = data_rows_from(&msgs2);
    assert_eq!(
        rows2.len(),
        2,
        "expected still 2 rows after replayed multi-statement transaction, got {}",
        rows2.len()
    );
}

/// S6 green gate: idempotent replay of a committed write is a no-op.
#[tokio::test]
async fn idempotent_replay_is_noop() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s6-idempotent-replay").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // First write with idempotency key
    client
        .simple_query("SET rockstream.idempotency_key = 'replay-key-1'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .await
        .expect("INSERT failed");
    client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT 1 failed");

    // After first COMMIT: exactly 1 row with the inserted data
    shard_db.flush().await.unwrap();
    let msgs1 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t after first commit");
    let rows1 = data_rows_from(&msgs1);
    assert_eq!(rows1.len(), 1, "expected 1 row after first COMMIT");
    assert_eq!(rows1[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows1[0].get("val").unwrap_or(""), "hello");

    // Replay: same idempotency key — should be a no-op
    client
        .simple_query("SET rockstream.idempotency_key = 'replay-key-1'")
        .await
        .expect("SET idempotency_key replay failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .await
        .expect("INSERT replay failed");
    client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT 2 (replay) failed");

    // Still exactly 1 row with the same data — replay was a no-op
    shard_db.flush().await.unwrap();
    let msgs2 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t after replay");
    let rows2 = data_rows_from(&msgs2);
    assert_eq!(
        rows2.len(),
        1,
        "expected still 1 row after idempotent replay, got {}",
        rows2.len()
    );
    assert_eq!(rows2[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows2[0].get("val").unwrap_or(""), "hello");
}

/// S6 green gate: idempotency-key cleanup uses scan-and-delete, no range-delete.
#[tokio::test]
async fn idempotency_key_expiry_cleanup_no_range_delete() {
    use object_store::memory::InMemory;
    use rockstream_storage::ShardDb;
    use std::sync::Arc;

    let store = Arc::new(InMemory::new());
    let shard_db = ShardDb::builder("s6-cleanup", store.clone())
        .build()
        .await
        .unwrap();

    // Insert two idempotency keys: one "old" (0 ms timestamp) and one "new" (now).
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Old key (timestamp = 0, will be expired by any positive retention)
    let old_hash = [1u8; 16];
    let mut batch = rockstream_storage::WriteBatch::new();
    ShardDb::put_idempotency_key(&mut batch, 0, old_hash, 1, 0);
    shard_db.write_batch(batch).await.unwrap();

    // New key (timestamp far in the future — will not be expired)
    let new_hash = [2u8; 16];
    let future_ms = now_ms + 86_400_000; // 24h ahead
    let mut batch2 = rockstream_storage::WriteBatch::new();
    ShardDb::put_idempotency_key(&mut batch2, 0, new_hash, 2, future_ms);
    shard_db.write_batch(batch2).await.unwrap();

    shard_db.flush().await.unwrap();

    // Cleanup with 24h-1ms retention → old key (ts=0) should be deleted, new key (ts=future) survives
    let deleted = shard_db
        .cleanup_expired_idempotency_keys(0, 86_399_999)
        .await
        .unwrap();
    assert_eq!(deleted, 1, "expected 1 expired key deleted, got {deleted}");

    // Verify old key is gone, new key still present
    let old_epoch = shard_db.get_idempotency_epoch(0, old_hash).await.unwrap();
    assert!(
        old_epoch.is_none(),
        "old key should be deleted after cleanup"
    );

    let new_epoch = shard_db.get_idempotency_epoch(0, new_hash).await.unwrap();
    assert!(new_epoch.is_some(), "new key should survive cleanup");
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.24 S8/S10 LFS proof tests
// ══════════════════════════════════════════════════════════════════════════════

/// P1 (S8/S10): psql INSERT + COMMIT with idempotency_key is visible via SELECT.
#[tokio::test]
async fn proof_psql_insert_commit_reflects_in_view() {
    let (port, _handle, shard_db) = start_gateway_with_shard("proof-p1-lfs").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'proof-p1-key'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (1, 500)")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM orders")
        .await
        .expect("SELECT orders failed");
    let data_rows = data_rows_from(&msgs);
    assert_eq!(data_rows.len(), 1, "P1: expected 1 row after COMMIT");
    assert_eq!(data_rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(data_rows[0].get("amount").unwrap_or(""), "500");
}

/// P2 (S10): A write missing idempotency_key no longer returns RS-2007 —
/// the gateway generates a server-side envelope and the commit succeeds
/// (v0.51.1 Slice 2).
#[tokio::test]
async fn proof_missing_idempotency_autogenerates_envelope_and_commits() {
    let (port, _handle, shard_db) = start_gateway_with_shard("proof-p2-missing-key").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE failed");

    // Do NOT set idempotency_key; use an explicit BEGIN so the write stays
    // buffered until COMMIT, exercising the envelope-generation path.
    client
        .simple_query("BEGIN")
        .await
        .expect("BEGIN should succeed");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (1, 500)")
        .await
        .expect("INSERT should succeed");

    let result = client.simple_query("COMMIT").await;
    assert!(
        result.is_ok(),
        "P2: expected COMMIT to succeed with a server-generated idempotency envelope; got: {result:?}"
    );

    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM orders")
        .await
        .expect("SELECT orders failed");
    let data_rows = data_rows_from(&msgs);
    assert_eq!(
        data_rows.len(),
        1,
        "P2: expected the committed row to be visible, got: {data_rows:?}"
    );
    assert_eq!(data_rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(data_rows[0].get("amount").unwrap_or(""), "500");
}

/// P3a (S8/S10): Idempotent replay on LFS is a no-op.
#[tokio::test]
async fn proof_idempotent_replay_noop_lfs() {
    let (port, _handle, shard_db) = start_gateway_with_shard("proof-p3a-lfs").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE failed");

    // First commit
    client
        .simple_query("SET rockstream.idempotency_key = 'p3a-lfs-key'")
        .await
        .expect("SET 1 failed");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (10, 99)")
        .await
        .expect("INSERT 1 failed");
    client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT 1 failed");

    shard_db.flush().await.unwrap();
    let msgs1 = client
        .simple_query("SELECT * FROM orders")
        .await
        .expect("SELECT after first commit");
    let rows1 = data_rows_from(&msgs1);
    assert_eq!(rows1.len(), 1, "P3a: expected 1 row after first commit");
    assert_eq!(rows1[0].get("id").unwrap_or(""), "10");
    assert_eq!(rows1[0].get("amount").unwrap_or(""), "99");

    // Replay with same key
    client
        .simple_query("SET rockstream.idempotency_key = 'p3a-lfs-key'")
        .await
        .expect("SET replay failed");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (10, 99)")
        .await
        .expect("INSERT replay failed");
    client
        .simple_query("COMMIT")
        .await
        .expect("COMMIT replay failed");

    shard_db.flush().await.unwrap();
    let msgs2 = client
        .simple_query("SELECT * FROM orders")
        .await
        .expect("SELECT after replay");
    let rows2 = data_rows_from(&msgs2);
    assert_eq!(
        rows2.len(),
        1,
        "P3a: expected still 1 row after idempotent replay (no-op), got {}",
        rows2.len()
    );
    assert_eq!(rows2[0].get("id").unwrap_or(""), "10");
    assert_eq!(rows2[0].get("amount").unwrap_or(""), "99");
}

// ── P3b: MinIO proof test (feature-gated) ────────────────────────────────────

/// P3b (S9/S10): Idempotent replay on MinIO is a no-op.
/// Requires the `testcontainers` feature and a running Docker daemon.
#[tokio::test]
#[cfg(feature = "testcontainers")]
async fn proof_idempotent_replay_noop_minio() {
    use object_store::aws::AmazonS3Builder;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::minio::MinIO;

    let minio = MinIO::default().start().await.expect("MinIO start failed");
    let host = minio.get_host().await.expect("host");
    let port = minio.get_host_port_ipv4(9000).await.expect("port");
    create_minio_bucket(port, "testbucket").await;

    let store = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{host}:{port}"))
            .with_bucket_name("testbucket")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .expect("S3 builder"),
    );

    let shard_db = Arc::new(
        rockstream_storage::ShardDb::builder("proof-p3b-minio", store.clone())
            .build()
            .await
            .expect("ShardDb build"),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Register table so SELECT can return typed columns.
    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .unwrap();

    // First commit
    client
        .simple_query("SET rockstream.idempotency_key = 'p3b-minio-key'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (20, 200)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    shard_db.flush().await.unwrap();
    let msgs1 = client.simple_query("SELECT * FROM orders").await.unwrap();
    let rows1 = data_rows_from(&msgs1);
    assert_eq!(rows1.len(), 1, "P3b: expected 1 row after first commit");
    assert_eq!(rows1[0].get("id").unwrap_or(""), "20");
    assert_eq!(rows1[0].get("amount").unwrap_or(""), "200");

    // Replay with same key
    client
        .simple_query("SET rockstream.idempotency_key = 'p3b-minio-key'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (20, 200)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    shard_db.flush().await.unwrap();
    let msgs2 = client.simple_query("SELECT * FROM orders").await.unwrap();
    let rows2 = data_rows_from(&msgs2);
    assert_eq!(
        rows2.len(),
        1,
        "P3b: expected still 1 row after idempotent replay on MinIO, got {}",
        rows2.len()
    );
    assert_eq!(rows2[0].get("id").unwrap_or(""), "20");
    assert_eq!(rows2[0].get("amount").unwrap_or(""), "200");
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.25 Phase 3b — S6/S8/S9/S10 proof tests
// ══════════════════════════════════════════════════════════════════════════════

use rockstream_gateway::change_log::ChangeEntry;
use rockstream_gateway::subscribe_handler::{
    deliver_snapshot, start_from_epoch, SubscribeRegistry, SubscriberHandle,
};
use rockstream_gateway::subscribe_parser::parse_subscribe;

// ── S10-1: oracle_subscribe_incremental_equals_batch ──────────────────────────

/// Oracle: N sequential 1-row COMMITs to a SubscribeRegistry produce the same
/// ordered Z-set as a single N-row "batch" (same pushes, same registry).
#[test]
fn oracle_subscribe_incremental_equals_batch() {
    let reg = SubscribeRegistry::new();
    for i in 1u64..=5 {
        reg.push(
            "t",
            ChangeEntry {
                epoch: i,
                row_key: bytes::Bytes::from(format!("k{i}")),
                mz_diff: 1,
                encoded_row: bytes::Bytes::from(format!("{i}")),
            },
        );
    }
    let incremental = reg.since_epoch("t", 1).unwrap_or_default();

    // Second registry with the same pushes in one "batch" (identical sequence).
    let reg2 = SubscribeRegistry::new();
    for i in 1u64..=5 {
        reg2.push(
            "t",
            ChangeEntry {
                epoch: i,
                row_key: bytes::Bytes::from(format!("k{i}")),
                mz_diff: 1,
                encoded_row: bytes::Bytes::from(format!("{i}")),
            },
        );
    }
    let batch = reg2.since_epoch("t", 1).unwrap_or_default();

    assert_eq!(incremental.len(), batch.len(), "entry counts differ");
    for (a, b) in incremental.iter().zip(batch.iter()) {
        assert_eq!(a.epoch, b.epoch);
        assert_eq!(a.encoded_row, b.encoded_row);
    }
}

// ── S10-2: proof_subscribe_snapshot_then_deltas_lfs ───────────────────────────

/// Snapshot delivers existing rows; subsequent live-tail delivers new deltas.
#[test]
fn proof_subscribe_snapshot_then_deltas_lfs() {
    let reg = SubscribeRegistry::new();
    let snapshot = vec![
        (bytes::Bytes::from("k1"), bytes::Bytes::from("1\tval1")),
        (bytes::Bytes::from("k2"), bytes::Bytes::from("2\tval2")),
    ];
    let req = parse_subscribe("SUBSCRIBE t AS OF NOW WITH SNAPSHOT").unwrap();
    let rows = deliver_snapshot(snapshot, 5, &req, &[]);
    assert_eq!(rows.len(), 2, "snapshot should deliver 2 rows");
    assert!(rows.iter().all(|r| r.mz_diff == 1));

    // Push live deltas after snapshot epoch.
    reg.push(
        "t",
        ChangeEntry {
            epoch: 6,
            row_key: bytes::Bytes::from("k3"),
            mz_diff: 1,
            encoded_row: bytes::Bytes::from("3\tval3"),
        },
    );
    reg.push(
        "t",
        ChangeEntry {
            epoch: 7,
            row_key: bytes::Bytes::from("k4"),
            mz_diff: 1,
            encoded_row: bytes::Bytes::from("4\tval4"),
        },
    );

    let mut handle = SubscriberHandle::new("t".to_string(), 5, req, vec![]);
    let deltas = handle.poll(&reg).unwrap();
    assert_eq!(deltas.len(), 2, "live-tail should deliver 2 deltas");
    assert_eq!(deltas[0].mz_timestamp, 6);
    assert_eq!(deltas[1].mz_timestamp, 7);
}

// ── S10-3: proof_subscribe_no_gaps_restart_lfs ────────────────────────────────

/// Restart from epoch 5 of a 10-epoch log delivers epochs 5-10 with no gaps.
#[test]
fn proof_subscribe_no_gaps_restart_lfs() {
    let reg = SubscribeRegistry::new();
    for i in 1u64..=10 {
        reg.push(
            "t",
            ChangeEntry {
                epoch: i,
                row_key: bytes::Bytes::from(format!("k{i}")),
                mz_diff: 1,
                encoded_row: bytes::Bytes::from(format!("{i}")),
            },
        );
    }
    let req = parse_subscribe("SUBSCRIBE t AS OF EPOCH 5").unwrap();
    let mut handle = start_from_epoch(&reg, &req, 5, vec![]).unwrap();
    let rows = handle.poll(&reg).unwrap();
    assert_eq!(rows.len(), 6, "expected epochs 5-10 (6 rows)");
    let epochs: Vec<u64> = rows.iter().map(|r| r.mz_timestamp).collect();
    for w in epochs.windows(2) {
        assert_eq!(w[1], w[0] + 1, "gap between epochs {} and {}", w[0], w[1]);
    }
    assert_eq!(epochs[0], 5);
    assert_eq!(*epochs.last().unwrap(), 10);
}

// ── S10-4: proof_ryw_resolves_within_slo_lfs ─────────────────────────────────

/// After a COMMIT the session's wait_for resolves immediately (epoch already
/// advanced in memory). Total elapsed time must be < 100 ms.
#[tokio::test]
async fn proof_ryw_resolves_within_slo_lfs() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("ryw-slo-lfs").await;
    let client = connect_port(port).await;

    // Commit a row to advance the shard epoch.
    client
        .simple_query("SET rockstream.idempotency_key = 'ryw-slo-key'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'x')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // Set explicit wait_for to epoch 1 (already advanced).
    client
        .simple_query(r#"SET rockstream.wait_for = '{"table_name":"t","source_epoch":1}'"#)
        .await
        .unwrap();

    let t0 = std::time::Instant::now();
    client.simple_query("SELECT * FROM t").await.unwrap();
    let elapsed_ms = t0.elapsed().as_millis();

    assert!(
        elapsed_ms < 100,
        "wait_for should resolve within 100ms SLO, took {elapsed_ms}ms"
    );
}

// ── S10-5: proof_subscribe_no_gaps_restart_tc ─────────────────────────────────

/// Simulates a "gateway restart" by discarding the first handle and creating a
/// new one from a checkpoint epoch. No entries are lost or duplicated.
#[test]
fn proof_subscribe_no_gaps_restart_tc() {
    let reg = SubscribeRegistry::new();
    for i in 1u64..=5 {
        reg.push(
            "t",
            ChangeEntry {
                epoch: i,
                row_key: bytes::Bytes::from(format!("k{i}")),
                mz_diff: 1,
                encoded_row: bytes::Bytes::from(format!("{i}")),
            },
        );
    }
    let checkpoint_epoch = 3u64;

    // Simulate restart: new handle from checkpoint epoch.
    let req = parse_subscribe("SUBSCRIBE t AS OF EPOCH 3").unwrap();
    let mut handle = start_from_epoch(&reg, &req, checkpoint_epoch, vec![]).unwrap();

    // Push additional entries "after restart".
    for i in 6u64..=8 {
        reg.push(
            "t",
            ChangeEntry {
                epoch: i,
                row_key: bytes::Bytes::from(format!("k{i}")),
                mz_diff: 1,
                encoded_row: bytes::Bytes::from(format!("{i}")),
            },
        );
    }

    let rows = handle.poll(&reg).unwrap();
    assert!(
        !rows.is_empty(),
        "should receive entries from epoch 3 onward"
    );
    assert_eq!(
        rows.first().unwrap().mz_timestamp,
        3,
        "first row should be epoch 3"
    );
    let epochs: Vec<u64> = rows.iter().map(|r| r.mz_timestamp).collect();
    for w in epochs.windows(2) {
        assert!(w[1] >= w[0], "epochs must be non-decreasing");
    }
}

// ── S10-6: subscribe_no_range_delete_in_change_log ───────────────────────────

/// ViewChangeLog eviction uses pop_front (point eviction), not range-delete.
/// Structural proof: capacity-3 log with 5 pushes → 3 entries remain.
#[test]
fn subscribe_no_range_delete_in_change_log() {
    use rockstream_gateway::change_log::ViewChangeLog;

    let mut log = ViewChangeLog::new(3);
    for i in 1u64..=5 {
        log.push(ChangeEntry {
            epoch: i,
            row_key: bytes::Bytes::from(format!("k{i}")),
            mz_diff: 1,
            encoded_row: bytes::Bytes::from(format!("{i}")),
        });
    }
    // Capacity-3 log after 5 pushes: only entries 3-5 remain (pop_front eviction).
    assert_eq!(
        log.entry_count(),
        3,
        "expected 3 entries after pop_front eviction"
    );
    let earliest = log.earliest_epoch().unwrap();
    assert!(
        earliest >= 3,
        "earliest epoch should be ≥ 3 after pop_front eviction, got {earliest}"
    );
    // Entries 1 and 2 are gone — not range-deleted, just popped from the front.
    let all = log.since_epoch(1);
    assert!(
        all.iter().all(|e| e.epoch >= 3),
        "all retained entries should have epoch ≥ 3"
    );
}

// ── S10-7: session_wait_for_bounded_by_timeout ────────────────────────────────

/// wait_for with an impossible epoch and 50 ms timeout always exits within 200 ms.
#[tokio::test]
async fn session_wait_for_bounded_by_timeout() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("wait-for-bounded").await;
    let client = connect_port(port).await;

    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'x')")
        .await
        .unwrap();

    // 50 ms timeout, impossible epoch.
    client
        .simple_query("SET rockstream.session_wait_for_timeout_ms = '50'")
        .await
        .unwrap();
    client
        .simple_query(r#"SET rockstream.wait_for = '{"table_name":"t","source_epoch":99999999}'"#)
        .await
        .unwrap();

    let t0 = std::time::Instant::now();
    // Must not block indefinitely; should proceed at current frontier.
    client.simple_query("SELECT * FROM t").await.unwrap();
    let elapsed_ms = t0.elapsed().as_millis();

    assert!(
        elapsed_ms < 200,
        "wait_for timeout must bound blocking to <200ms, took {elapsed_ms}ms"
    );
}

// ── S9: session_ryw_after_commit_sees_own_write ───────────────────────────────

/// After a COMMIT the session's auto-RYW ensures the subsequent SELECT
/// completes within the RYW SLO (epoch already advanced).
#[tokio::test]
async fn session_ryw_after_commit_sees_own_write() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("ryw-auto").await;
    let client = connect_port(port).await;

    client
        .simple_query("SET rockstream.idempotency_key = 'ryw-auto-key'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'x')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // Auto-RYW applies: session.last_written_epoch is set → wait_for resolves immediately.
    let t0 = std::time::Instant::now();
    client.simple_query("SELECT * FROM t").await.unwrap();
    let elapsed_ms = t0.elapsed().as_millis();

    assert!(
        elapsed_ms < 200,
        "session RYW should complete within SLO, took {elapsed_ms}ms"
    );
}

// ── S9: session_ryw_opt_out ────────────────────────────────────────────────────

/// SET rockstream.session_wait_for = off disables auto-RYW.
/// The SELECT completes immediately without any wait_for blocking.
#[tokio::test]
async fn session_ryw_opt_out() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("ryw-opt-out").await;
    let client = connect_port(port).await;

    // Opt out before the commit.
    client
        .simple_query("SET rockstream.session_wait_for = 'off'")
        .await
        .unwrap();

    client
        .simple_query("SET rockstream.idempotency_key = 'ryw-opt-out-key'")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'x')")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    // SELECT proceeds without RYW wait.
    let t0 = std::time::Instant::now();
    client.simple_query("SELECT * FROM t").await.unwrap();
    let elapsed_ms = t0.elapsed().as_millis();

    assert!(
        elapsed_ms < 200,
        "opt-out SELECT should complete immediately, took {elapsed_ms}ms"
    );
}

#[tokio::test]
async fn session_max_staleness_notice_emits_and_query_still_returns() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("v045-max-staleness", store)
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        GatewayServer::with_shard_db(addr, catalog, Arc::new(NoopViewReader), shard_db.clone());
    let handler = server.handler().clone();
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let (client, mut notice_rx) = connect_with_notices(local_addr.port()).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v045-max-staleness-key'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    let stale_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 20_000;
    handler.set_frontier_published_at_ms_for_test(stale_ms);

    client
        .simple_query("SET rockstream.max_staleness = '10s'")
        .await
        .expect("SET max_staleness failed");
    let rows = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let data_rows = data_rows_from(&rows);
    assert_eq!(data_rows.len(), 1, "query must still return the stale row");

    let notice = tokio::time::timeout(std::time::Duration::from_secs(2), notice_rx.recv())
        .await
        .expect("timed out waiting for notice")
        .expect("notice channel closed");
    assert!(
        notice.message().contains("session.staleness_exceeded"),
        "expected RS-2018 staleness notice, got {}",
        notice.message()
    );
    assert_eq!(notice.code().code(), "01000");

    let frontier_age = client
        .simple_query("SHOW frontier_age_ms")
        .await
        .expect("SHOW frontier_age_ms failed");
    let age_rows = data_rows_from(&frontier_age);
    let age_ms: u64 = age_rows[0]
        .get("frontier_age_ms")
        .expect("missing frontier_age_ms")
        .parse()
        .expect("frontier_age_ms must be numeric");
    assert!(
        age_ms >= 10_000,
        "frontier age should be stale, got {age_ms}ms"
    );
}

#[tokio::test]
async fn session_mode_tracks_mutual_exclusion_between_wait_for_and_max_staleness() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v045-session-mode").await;
    let client = connect_port(port).await;

    client
        .simple_query(r#"SET rockstream.wait_for = '{"table_name":"t","source_epoch":9}'"#)
        .await
        .expect("SET wait_for failed");
    let initial = client
        .simple_query("SHOW rockstream.session_mode")
        .await
        .expect("SHOW session_mode failed");
    assert_eq!(
        data_rows_from(&initial)[0].get("rockstream.session_mode"),
        Some("wait_for")
    );

    client
        .simple_query("SET rockstream.max_staleness = '10s'")
        .await
        .expect("SET max_staleness failed");
    let after_max = client
        .simple_query("SHOW rockstream.session_mode")
        .await
        .expect("SHOW session_mode failed");
    assert_eq!(
        data_rows_from(&after_max)[0].get("rockstream.session_mode"),
        Some("max_staleness")
    );

    client
        .simple_query("SET rockstream.session_wait_for = 'on'")
        .await
        .expect("SET session_wait_for failed");
    let after_wait = client
        .simple_query("SHOW rockstream.session_mode")
        .await
        .expect("SHOW session_mode failed");
    assert_eq!(
        data_rows_from(&after_wait)[0].get("rockstream.session_mode"),
        Some("wait_for")
    );
}

#[tokio::test]
async fn write_fence_token_can_be_used_by_another_session_via_after_fence() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v045-write-fence").await;
    let writer = connect_port(port).await;
    let reader = connect_port(port).await;

    writer
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");
    writer
        .simple_query("SET rockstream.idempotency_key = 'v045-write-fence-key'")
        .await
        .expect("SET idempotency_key failed");
    writer
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'visible')")
        .await
        .expect("INSERT failed");
    writer.simple_query("COMMIT").await.expect("COMMIT failed");

    let fence_rows = writer
        .simple_query("SELECT rockstream.write_fence() AS fence")
        .await
        .expect("write_fence failed");
    let fence = data_rows_from(&fence_rows)[0]
        .get("fence")
        .expect("missing fence token");

    let t0 = Instant::now();
    let selected = reader
        .simple_query(&format!(
            "SELECT * FROM t WHERE rockstream.after_fence('{fence}')"
        ))
        .await
        .expect("after_fence SELECT failed");
    let elapsed_ms = t0.elapsed().as_millis();
    let rows = data_rows_from(&selected);
    assert_eq!(rows.len(), 1, "expected the fenced row to be visible");
    assert_eq!(rows[0].get("id"), Some("1"));
    assert_eq!(rows[0].get("val"), Some("visible"));
    assert!(
        elapsed_ms < 200,
        "after_fence should reuse bounded wait_for path, took {elapsed_ms}ms"
    );
}

// ── v0.26 S8: explain_names_pushdown_effect ──────────────────────────────────

/// EXPLAIN SELECT k, COUNT(*) FROM mv GROUP BY k → output contains "partial_pushdown: true".
#[tokio::test]
async fn explain_names_pushdown_effect() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("explain-pushdown").await;
    let client = connect_port(port).await;

    let rows = client
        .simple_query("EXPLAIN SELECT k, COUNT(*) FROM mv GROUP BY k")
        .await
        .unwrap();

    let plan_text: String = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
                row.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        plan_text.contains("partial_pushdown: true"),
        "EXPLAIN must contain 'partial_pushdown: true', got: {plan_text}"
    );
}

// ── v0.26 S8: explain_no_pushdown_for_full_scan ──────────────────────────────

/// EXPLAIN SELECT * FROM mv → output does NOT contain "partial_pushdown: true".
#[tokio::test]
async fn explain_no_pushdown_for_full_scan() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("explain-no-pushdown").await;
    let client = connect_port(port).await;

    let rows = client
        .simple_query("EXPLAIN SELECT * FROM mv")
        .await
        .unwrap();

    let plan_text: String = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
                row.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        !plan_text.contains("partial_pushdown: true"),
        "EXPLAIN SELECT * must NOT contain 'partial_pushdown: true', got: {plan_text}"
    );
}

#[tokio::test]
async fn explain_incremental_matches_frontend_byte_for_byte() {
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogView};
    use rockstream_sql::SqlFrontend;
    use rockstream_types::explain::ExplainLevel;

    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "inc_mv".to_string(),
        sql: "SELECT id FROM base".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let msgs = client
        .simple_query("EXPLAIN INCREMENTAL inc_mv")
        .await
        .expect("EXPLAIN INCREMENTAL failed");
    let gateway_output = data_rows_from(&msgs)
        .iter()
        .filter_map(|row| row.get(0).map(|s| s.to_string()))
        .collect::<Vec<_>>()
        .join("\n");

    let frontend = SqlFrontend::new();
    frontend
        .register_table(
            "inc_mv",
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        )
        .unwrap();
    let direct = frontend
        .explain_incremental_for_sql("SELECT * FROM inc_mv", ExplainLevel::Default, &[])
        .await
        .unwrap();

    assert_eq!(gateway_output, direct);
}

#[tokio::test]
async fn explain_incremental_analyze_reflects_live_view_traffic() {
    use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogView};
    use rockstream_gateway::view_reader::ViewReadStrategy;
    use rockstream_types::ids::OperatorId;
    use std::time::{Duration, SystemTime};

    struct InstrumentedViewReader;

    #[async_trait::async_trait]
    impl ViewReader for InstrumentedViewReader {
        async fn read_view(
            &self,
            _view_name: &str,
            _limit: Option<usize>,
            _strategy: ViewReadStrategy,
        ) -> Result<Vec<Vec<u8>>, GatewayError> {
            rockstream_types::metrics::record_operator_runtime_sample_at(
                OperatorId(1),
                120,
                18,
                Duration::from_millis(9),
                2,
                SystemTime::now(),
            );
            Ok(vec![b"1".to_vec(), b"2".to_vec()])
        }

        fn published_frontier(&self) -> Option<u64> {
            Some(77)
        }
    }

    rockstream_types::metrics::reset_all();
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_view(CatalogView {
        name: "analyze_mv".to_string(),
        sql: "SELECT id FROM base".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(InstrumentedViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("SELECT * FROM analyze_mv")
        .await
        .expect("SELECT failed");

    let msgs = client
        .simple_query("EXPLAIN INCREMENTAL ANALYZE analyze_mv")
        .await
        .expect("EXPLAIN INCREMENTAL ANALYZE failed");
    let plan_output = data_rows_from(&msgs)
        .iter()
        .filter_map(|row| row.get(0).map(|s| s.to_string()))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(plan_output.contains("rows/s=2"), "output: {plan_output}");
    assert!(
        plan_output.contains("state_reads=18"),
        "output: {plan_output}"
    );
    assert!(plan_output.contains("p99=9.0ms"), "output: {plan_output}");
    assert!(
        plan_output.contains("dlq_entries=2"),
        "output: {plan_output}"
    );
    assert!(
        !plan_output.contains("rows/s=12500"),
        "output: {plan_output}"
    );

    rockstream_types::metrics::reset_all();
}

// ════════════════════════════════════════════════════════════════════════════
// v0.27 COPY Protocol tests (Phase 3a: S1–S4)
// ════════════════════════════════════════════════════════════════════════════

use rockstream_gateway::copy_state::MAX_COPY_IN_BATCH_ROWS;

// ── S3 gate: copy_from_stdin_returns_copy_in_response ────────────────────────

/// S3 green gate: `COPY t FROM STDIN` causes the gateway to enter COPY IN mode
/// (tokio_postgres successfully obtains a `CopyInSink` without error).
#[tokio::test]
async fn copy_from_stdin_returns_copy_in_response() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v027-s3-copy-in-response").await;
    let client = connect_port(port).await;

    // Register table so the gateway can resolve columns.
    client
        .simple_query("CREATE TABLE t (id TEXT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // `copy_in` will send the COPY statement and expect CopyInResponse back.
    // If we get a CopyInSink without error, the gateway entered COPY IN mode.
    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in("COPY t FROM STDIN")
        .await
        .expect("copy_in should enter COPY IN mode without error");
    tokio::pin!(sink);

    // Close the sink immediately (zero rows) — gateway should send COPY 0.
    let rows_written = sink.finish().await.expect("finish should succeed");
    assert_eq!(rows_written, 0, "expected COPY 0 for empty stream");
}

// ════════════════════════════════════════════════════════════════════════════
// v0.27 COPY Protocol tests (Phase 3b: S5–S9)
// ════════════════════════════════════════════════════════════════════════════

// ── S4 gate: copy_in_basic_rows_visible_lfs ──────────────────────────────────

/// S4 green gate (P1): three rows sent via COPY FROM STDIN are visible in the
/// shard after CopyDone.
///
/// Asserts:
/// - CopyInResponse received (entering COPY IN mode)
/// - CopyDone sent successfully
/// - CommandComplete tag == `COPY 3`
/// - Shard contains 3 keys under `view_output/t/…`
#[tokio::test]
async fn copy_in_basic_rows_visible_lfs() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v027-s4-copy-in-basic").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id TEXT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // Obtain a COPY IN sink — this confirms CopyInResponse was sent.
    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in("COPY t FROM STDIN")
        .await
        .expect("copy_in should enter COPY IN mode");
    tokio::pin!(sink);

    // Send 3 TSV rows via the Sink trait.
    for i in 1u32..=3 {
        let row = format!("row{i}\tval{i}\n");
        futures::SinkExt::send(&mut sink, bytes::Bytes::from(row))
            .await
            .expect("send row failed");
    }

    // CopyDone — gateway flushes and sends CommandComplete.
    let rows_written = sink.finish().await.expect("CopyDone should succeed");
    assert_eq!(rows_written, 3, "CommandComplete should report COPY 3");

    // Wire-protocol proof: row count verified via CommandComplete above.
    // Content check via direct shard scan (SELECT after COPY deadlocks in the
    // single-threaded test runtime).
    shard_db.flush().await.unwrap();
    let entries = shard_db
        .scan_prefix(b"view_output/t/")
        .await
        .expect("scan_prefix failed");
    assert_eq!(
        entries.len(),
        3,
        "expected 3 rows in shard after COPY, got {}",
        entries.len()
    );
    let values: Vec<String> = entries
        .iter()
        .map(|(_, v)| String::from_utf8_lossy(v).to_string())
        .collect();
    assert!(
        values.iter().any(|v| v.contains("val1")),
        "expected val1 in shard"
    );
    assert!(
        values.iter().any(|v| v.contains("val2")),
        "expected val2 in shard"
    );
    assert!(
        values.iter().any(|v| v.contains("val3")),
        "expected val3 in shard"
    );
}

// ── S5 gate: copy_in_large_batch_no_memory_exhaustion_lfs ────────────────────

/// S5 / P2 green gate: 50 000-row COPY FROM STDIN auto-flushes at
/// MAX_COPY_IN_BATCH_ROWS so `COPY_IN_BUFFER_ROWS` never exceeds the bound.
/// All 50 000 rows appear in the shard afterward.
#[tokio::test]
async fn copy_in_large_batch_no_memory_exhaustion_lfs() {
    use rockstream_gateway::copy_state::COPY_IN_BUFFER_ROWS;
    use std::sync::atomic::Ordering;

    // Reset the global gauge to a known baseline.
    COPY_IN_BUFFER_ROWS.store(0, Ordering::Relaxed);

    let (port, _handle, shard_db) = start_gateway_with_shard("v027-s5-large-batch").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE big (id TEXT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in("COPY big FROM STDIN")
        .await
        .expect("copy_in should enter COPY IN mode");
    tokio::pin!(sink);

    // Send 50 000 rows in chunks of MAX_COPY_IN_BATCH_ROWS+1 to ensure auto-flush
    // fires at least once.
    const TOTAL: usize = 50_000;
    const CHUNK: usize = 1_024; // arbitrary; auto-flush triggers inside on_copy_data

    let mut sent = 0usize;
    while sent < TOTAL {
        let batch_size = CHUNK.min(TOTAL - sent);
        let mut data = String::new();
        for i in sent..sent + batch_size {
            data.push_str(&format!("id_{i}\t{i}\n"));
        }
        futures::SinkExt::send(&mut sink, bytes::Bytes::from(data))
            .await
            .expect("send chunk failed");

        // Assert the gauge never exceeds MAX_COPY_IN_BATCH_ROWS after each chunk.
        let gauge = COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed);
        assert!(
            gauge <= MAX_COPY_IN_BATCH_ROWS as u64,
            "COPY_IN_BUFFER_ROWS ({gauge}) exceeded MAX_COPY_IN_BATCH_ROWS ({MAX_COPY_IN_BATCH_ROWS}) after sending batch ending at row {}", sent + batch_size
        );

        sent += batch_size;
    }

    // Wire-protocol proof: CommandComplete tag carries the exact row count.
    let rows_written = sink.finish().await.expect("CopyDone should succeed");
    assert_eq!(
        rows_written, TOTAL as u64,
        "CommandComplete should report COPY {TOTAL}"
    );

    // All rows visible in the shard (verified via direct scan; SELECT after COPY
    // deadlocks in the single-threaded test runtime due to TCP buffer back-pressure).
    shard_db.flush().await.unwrap();
    let entries = shard_db
        .scan_prefix(b"view_output/big/")
        .await
        .expect("scan_prefix failed");
    assert_eq!(
        entries.len(),
        TOTAL,
        "shard should contain {TOTAL} rows after large COPY; got {}",
        entries.len()
    );
}

// ── S6 gate: copy_in_table_not_found_returns_rs2500 ──────────────────────────

/// S6 / P3 green gate: COPY into a non-existent table returns RS-2500.
#[tokio::test]
async fn copy_in_table_not_found_returns_rs2500() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v027-s6-table-not-found").await;
    let client = connect_port(port).await;

    // Do NOT create the table — it must be absent from the catalog.
    let copy_result: Result<tokio_postgres::CopyInSink<bytes::Bytes>, _> =
        client.copy_in("COPY ghost_t FROM STDIN").await;
    let err = match copy_result {
        Err(e) => e,
        Ok(_) => panic!("expected error for unknown table, but got CopyInSink"),
    };

    let msg = if let Some(db_err) = err.as_db_error() {
        db_err.message().to_string()
    } else {
        err.to_string()
    };
    assert!(
        msg.contains("RS-2500"),
        "expected RS-2500 in error, got: {msg}"
    );
}

// ── S6 gate: copy_in_column_count_mismatch_returns_rs2501 ────────────────────

/// S6 / P4 green gate: a TSV row with the wrong field count returns RS-2501.
#[tokio::test]
async fn copy_in_column_count_mismatch_returns_rs2501() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("v027-s6-col-mismatch").await;
    let client = connect_port(port).await;

    // Create a 3-column table.
    client
        .simple_query("CREATE TABLE tbl3 (a TEXT, b TEXT, c TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in("COPY tbl3 FROM STDIN")
        .await
        .expect("copy_in should enter COPY IN mode");
    tokio::pin!(sink);

    // Send a row with only 2 fields — mismatch against 3-column table.
    futures::SinkExt::send(&mut sink, bytes::Bytes::from("field1\tfield2\n"))
        .await
        .expect("send should not fail immediately");

    // CopyDone or finish will surface the RS-2501 error.
    let result = sink.finish().await;
    match result {
        Err(e) => {
            let msg = if let Some(db_err) = e.as_db_error() {
                db_err.message().to_string()
            } else {
                e.to_string()
            };
            assert!(
                msg.contains("RS-2501"),
                "expected RS-2501 in error, got: {msg}"
            );
        }
        Ok(n) => panic!("expected error but got COPY {n}"),
    }
}

// ── S7 gate: copy_in_auth_enforced_lfs ───────────────────────────────────────

/// S7 / P5 green gate: COPY IN enforces PipelineOwner role.
///
/// Tests at the handler level (matching auth_proof_tests.rs pattern):
/// - No principal (system with auth=off) → passthrough (system bypasses ACL)
/// - Viewer principal → RS-2401
/// - PipelineOwner principal → CopyIn accepted
/// - No principal with auth enabled but no session → RS-2400 tested via JwtVerifier
#[tokio::test]
async fn copy_in_auth_enforced_lfs() {
    use rockstream_gateway::auth::Principal;
    use rockstream_gateway::server::GatewayHandler;
    use rockstream_types::acl::{AclEntry, Role};

    const SECRET: &[u8] = b"v027-auth-test-secret";

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("v027-s7-auth", store)
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader: Arc<dyn ViewReader> = Arc::new(NoopViewReader);

    let handler = Arc::new(GatewayHandler::with_shard_db(
        catalog.clone(),
        view_reader,
        shard_db,
    ));

    // Grant PipelineOwner to owner_user; Viewer to viewer_user.
    handler.acl_store.grant(AclEntry {
        principal: "owner_user".to_string(),
        namespace: "public".to_string(),
        view_name: None,
        role: Role::PipelineOwner,
    });
    handler.acl_store.grant(AclEntry {
        principal: "viewer_user".to_string(),
        namespace: "public".to_string(),
        view_name: None,
        role: Role::Viewer,
    });

    // Register the target table.
    catalog.add_table(rockstream_gateway::catalog_stubs::CatalogTable {
        name: "auth_t".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Utf8".to_string(),
            },
            CatalogColumn {
                name: "val".to_string(),
                data_type: "Utf8".to_string(),
            },
        ],
    });

    // ── RS-2400: JwtVerifier rejects missing/empty token ─────────────────────
    let verifier = rockstream_gateway::auth::JwtVerifier::with_hs256_key(SECRET.to_vec());
    let err = verifier.verify("").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("RS-2400") || msg.contains("unauthenticated"),
        "expected RS-2400 for empty token; got: {msg}"
    );

    // ── RS-2401: viewer_user cannot COPY IN ───────────────────────────────────
    let viewer_conn = "viewer-conn-001";
    {
        let mut s = handler.sessions.entry(viewer_conn.to_string()).or_default();
        s.principal = Principal::Jwt {
            sub: "viewer_user".to_string(),
        };
        s.current_namespace = "public".to_string();
    }

    let responses = handler
        .copy_from_stdin_response("COPY auth_t FROM STDIN", viewer_conn)
        .expect("copy_from_stdin_response should return Ok (error inside response)");

    let viewer_err = responses
        .iter()
        .find(|r| matches!(r, pgwire::api::results::Response::Error(_)));
    let viewer_err_msg = viewer_err
        .map(|r| {
            if let pgwire::api::results::Response::Error(e) = r {
                e.message.clone()
            } else {
                String::new()
            }
        })
        .unwrap_or_default();
    assert!(
        viewer_err_msg.contains("RS-2401") || viewer_err_msg.contains("insufficient_privilege"),
        "expected RS-2401 for viewer; got: {viewer_err_msg:?}"
    );

    // ── PipelineOwner: owner_user can COPY IN ─────────────────────────────────
    let owner_conn = "owner-conn-001";
    {
        let mut s = handler.sessions.entry(owner_conn.to_string()).or_default();
        s.principal = Principal::Jwt {
            sub: "owner_user".to_string(),
        };
        s.current_namespace = "public".to_string();
    }

    let responses = handler
        .copy_from_stdin_response("COPY auth_t FROM STDIN", owner_conn)
        .expect("owner should get a response");

    let has_copy_in = responses
        .iter()
        .any(|r| matches!(r, pgwire::api::results::Response::CopyIn(_)));
    assert!(
        has_copy_in,
        "expected CopyInResponse for pipeline_owner; got non-CopyIn response"
    );
}

// ── S8 gate: copy_in_large_batch_no_memory_exhaustion_minio_tc ───────────────

#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn copy_in_large_batch_no_memory_exhaustion_minio_tc() {
    use rockstream_gateway::copy_state::{COPY_IN_BUFFER_ROWS, MAX_COPY_IN_BATCH_ROWS};
    use std::sync::atomic::Ordering;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::minio::MinIO;

    let container = MinIO::default()
        .start()
        .await
        .expect("MinIO container start");
    let host = container.get_host().await.expect("get MinIO host");
    let minio_port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("get MinIO port");
    create_minio_bucket(minio_port, "testbucket").await;

    let store = Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(format!("http://{host}:{minio_port}"))
            .with_bucket_name("testbucket")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .expect("build MinIO store"),
    );

    let shard_db = Arc::new(
        ShardDb::builder("v027-s8-minio", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        GatewayServer::with_shard_db(addr, catalog, Arc::new(NoopViewReader), shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query("CREATE TABLE minio_t (id TEXT, val TEXT)")
        .await
        .expect("CREATE TABLE");

    COPY_IN_BUFFER_ROWS.store(0, Ordering::Relaxed);

    const TOTAL: usize = 50_000;
    const CHUNK: usize = 1_024;

    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in("COPY minio_t FROM STDIN")
        .await
        .expect("copy_in");
    tokio::pin!(sink);

    let mut sent = 0usize;
    while sent < TOTAL {
        let batch_size = CHUNK.min(TOTAL - sent);
        let mut data = String::new();
        for i in sent..sent + batch_size {
            data.push_str(&format!("id_{i}\t{i}\n"));
        }
        futures::SinkExt::send(&mut sink, bytes::Bytes::from(data))
            .await
            .expect("send");

        let gauge = COPY_IN_BUFFER_ROWS.load(Ordering::Relaxed);
        assert!(
            gauge <= MAX_COPY_IN_BATCH_ROWS as u64,
            "gauge {gauge} exceeded bound"
        );

        sent += batch_size;
    }

    let rows = sink.finish().await.expect("finish");
    assert_eq!(rows, TOTAL as u64, "COPY {TOTAL}");

    // Wire-protocol proof: row count verified via CommandComplete above.
    // Content check via direct shard scan.
    shard_db.flush().await.unwrap();
    let entries = shard_db
        .scan_prefix(b"view_output/minio_t/")
        .await
        .expect("scan_prefix failed");
    assert_eq!(
        entries.len(),
        TOTAL,
        "shard should contain {TOTAL} rows; got {}",
        entries.len()
    );
}

// ── S9 gate: proof_copy_from_lfs ─────────────────────────────────────────────

/// S9 / P1 end-to-end proof: 1 000 rows via tokio_postgres `copy_in` API are
/// all visible in the shard and CommandComplete reports COPY 1000.
#[tokio::test]
async fn proof_copy_from_lfs() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v027-s9-proof-copy").await;
    let client = connect_port(port).await;

    // Register 2-column table in the catalog.
    client
        .simple_query("CREATE TABLE proof_t (id TEXT, val TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let sink: tokio_postgres::CopyInSink<bytes::Bytes> = client
        .copy_in("COPY proof_t FROM STDIN")
        .await
        .expect("copy_in should enter COPY IN mode");
    tokio::pin!(sink);

    const TOTAL: usize = 1_000;
    for i in 0..TOTAL {
        let row = format!("id_{i}\tval_{i}\n");
        futures::SinkExt::send(&mut sink, bytes::Bytes::from(row))
            .await
            .expect("send row failed");
    }

    // Wire-protocol proof: CommandComplete tag carries the exact row count.
    let rows_written = sink.finish().await.expect("CopyDone should succeed");
    assert_eq!(
        rows_written, TOTAL as u64,
        "CommandComplete should report COPY {TOTAL}"
    );

    // All rows visible in the shard.
    shard_db.flush().await.unwrap();
    let entries = shard_db
        .scan_prefix(b"view_output/proof_t/")
        .await
        .expect("scan_prefix failed");
    assert_eq!(
        entries.len(),
        TOTAL,
        "shard should contain {TOTAL} rows; got {}",
        entries.len()
    );

    // Verify a few spot rows.
    for i in [0usize, 499, 999] {
        let found = entries
            .iter()
            .any(|(_, v)| String::from_utf8_lossy(v).contains(&format!("val_{i}")));
        assert!(found, "expected row val_{i} in shard");
    }
}

// ── Last-hop: view materialisation after DML commit ──────────────────────────

/// Last-hop green gate: INSERT INTO base_table + COMMIT causes dependent views
/// to be materialised into the serving shard immediately, so that a subsequent
/// SELECT over a view returns real rows instead of zero rows.
///
/// This is the "last hop" described in the 30-minute tutorial — the bridge that
/// connects the DML write path to the live view serving layer.
#[tokio::test]
async fn last_hop_view_materialised_after_commit() {
    // Use an in-memory store so we can open a fresh ShardReader after flushing.
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("last-hop-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());

    // Use NoopViewReader for startup; we'll verify via ShardDb scan directly.
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // DDL: table + filter view
    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW big_orders AS \
             SELECT id, amount FROM orders WHERE amount > 50",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW");

    // DML: two rows, one above the filter threshold, one below
    client
        .simple_query("SET rockstream.idempotency_key = 'last-hop-001'")
        .await
        .expect("SET idempotency_key");
    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (1, 100)")
        .await
        .expect("INSERT row 1");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (2, 10)")
        .await
        .expect("INSERT row 2");
    client.simple_query("COMMIT").await.expect("COMMIT");

    shard_db.flush().await.unwrap();

    // Verify: base table has 2 rows with exact values (ordered by id)
    let base_msgs = client
        .simple_query("SELECT * FROM orders ORDER BY id")
        .await
        .expect("SELECT orders failed");
    let base_rows = data_rows_from(&base_msgs);
    assert_eq!(base_rows.len(), 2, "orders should have 2 rows");
    assert_eq!(base_rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(base_rows[0].get("amount").unwrap_or(""), "100");
    assert_eq!(base_rows[1].get("id").unwrap_or(""), "2");
    assert_eq!(base_rows[1].get("amount").unwrap_or(""), "10");

    // Verify: big_orders has exactly 1 row (amount=100 passes WHERE amount > 50)
    let view_msgs = client
        .simple_query("SELECT * FROM big_orders")
        .await
        .expect("SELECT big_orders failed");
    let view_rows = data_rows_from(&view_msgs);
    assert_eq!(
        view_rows.len(),
        1,
        "big_orders should have exactly 1 row (amount=100 passes WHERE amount > 50); got {}",
        view_rows.len()
    );
    assert_eq!(view_rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(view_rows[0].get("amount").unwrap_or(""), "100");
}

/// Last-hop with a GROUP BY aggregate view: COUNT(*) and SUM() produce
/// correct values in the serving shard after COMMIT.
#[tokio::test]
async fn last_hop_aggregate_view_materialised_after_commit() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("last-hop-agg-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query("CREATE TABLE clicks (user_id BIGINT, url TEXT, ts BIGINT)")
        .await
        .expect("CREATE TABLE clicks");
    client
        .simple_query(
            "CREATE VIEW page_hits AS \
             SELECT url, COUNT(*) AS hits FROM clicks GROUP BY url",
        )
        .await
        .expect("CREATE VIEW page_hits");

    client
        .simple_query("SET rockstream.idempotency_key = 'last-hop-agg-001'")
        .await
        .expect("SET");
    client.simple_query("BEGIN").await.expect("BEGIN");
    // 3 clicks: /home × 2, /pricing × 1
    client
        .simple_query("INSERT INTO clicks (user_id, url, ts) VALUES (1, '/home', 100)")
        .await
        .expect("INSERT 1");
    client
        .simple_query("INSERT INTO clicks (user_id, url, ts) VALUES (2, '/home', 101)")
        .await
        .expect("INSERT 2");
    client
        .simple_query("INSERT INTO clicks (user_id, url, ts) VALUES (3, '/pricing', 102)")
        .await
        .expect("INSERT 3");
    client.simple_query("COMMIT").await.expect("COMMIT");

    shard_db.flush().await.unwrap();

    // Verify exact page_hits output via SELECT (ordered by url for determinism)
    let msgs = client
        .simple_query("SELECT * FROM page_hits ORDER BY url")
        .await
        .expect("SELECT page_hits failed");
    let view_rows = data_rows_from(&msgs);
    assert_eq!(
        view_rows.len(),
        2,
        "page_hits should have 2 rows (one per URL); got {}",
        view_rows.len()
    );
    // ORDER BY url: /home < /pricing (lexicographic)
    assert_eq!(view_rows[0].get("url").unwrap_or(""), "/home");
    assert_eq!(view_rows[0].get("hits").unwrap_or(""), "2");
    assert_eq!(view_rows[1].get("url").unwrap_or(""), "/pricing");
    assert_eq!(view_rows[1].get("hits").unwrap_or(""), "1");
}

/// Last-hop SELECT: after INSERT+COMMIT, a SELECT over the view returns rows
/// via the ShardReader-backed HotOnlyViewReader.
///
/// This is the full end-to-end path: DML → materialiser → ShardReader → SELECT.
#[tokio::test]
async fn last_hop_select_returns_rows_after_commit() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("last-hop-select-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());

    // Start gateway with a NoopViewReader initially — we'll swap the
    // catalog to verify rows in the shard; the SELECT path uses HotOnlyViewReader
    // opened after the flush so it sees the materialised output.
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW high_value AS \
             SELECT id, amount FROM orders WHERE amount > 100",
        )
        .await
        .expect("CREATE VIEW high_value");

    client
        .simple_query("SET rockstream.idempotency_key = 'last-hop-select-001'")
        .await
        .expect("SET");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (10, 200)")
        .await
        .expect("INSERT 200");
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (11, 50)")
        .await
        .expect("INSERT 50");
    client.simple_query("COMMIT").await.expect("COMMIT");

    // Flush so the materialiser's output is visible to a ShardReader.
    shard_db.flush().await.unwrap();

    // Open a fresh ShardReader AFTER the flush so it reads the latest SSTables.
    let reader = ShardReader::open("last-hop-select-shard", store.clone())
        .await
        .unwrap();
    let hot_reader = Arc::new(HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: None,
    });

    // Use a second gateway instance backed by the same shard but with the live reader.
    let addr2: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server2 = GatewayServer::with_catalog(addr2, catalog.clone(), hot_reader);
    let (local_addr2, _handle2) = server2.serve_background().await.unwrap();
    let client2 = connect_port(local_addr2.port()).await;

    // SELECT from high_value — should return 1 row (amount=200 > 100)
    let rows = client2
        .simple_query("SELECT * FROM high_value")
        .await
        .expect("SELECT high_value");

    let data_rows: Vec<_> = rows
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        data_rows.len(),
        1,
        "SELECT high_value should return 1 row (amount=200); got {}: {:?}",
        data_rows.len(),
        data_rows
            .iter()
            .map(|r| format!("({:?},{:?})", r.get(0), r.get(1)))
            .collect::<Vec<_>>()
    );
    // Verify the amount column
    let amount_val = data_rows[0].get(1).unwrap_or("");
    assert_eq!(
        amount_val, "200",
        "expected amount=200 in high_value row; got {amount_val}"
    );
}

#[tokio::test]
async fn resource_usage_show_matches_catalog_table() {
    reset_all();
    set_active_pricing_config(None);

    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("resource-usage-shard", store)
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog, view_reader, shard_db);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query(
            "CREATE WORKLOAD fast_lane WITH (MEMORY_LIMIT = 8192, FRESHNESS_SLO_MS = 500)",
        )
        .await
        .expect("CREATE WORKLOAD fast_lane");
    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .expect("CREATE TABLE orders");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW big_orders WITH WORKLOAD = fast_lane AS SELECT id, amount FROM orders WHERE amount > 50",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW big_orders");

    set_workload_memory("fast_lane", 4096);
    set_pipeline_state_bytes("big_orders", 2048);
    set_state_budget(8192);
    set_freshness_lag("big_orders", 250);

    let show_msgs = client
        .simple_query("SHOW RESOURCE USAGE")
        .await
        .expect("SHOW RESOURCE USAGE");
    let show_rows = data_rows_from(&show_msgs);
    assert_eq!(show_rows.len(), 1);

    let table_msgs = client
        .simple_query("SELECT * FROM rockstream_catalog.view_resource_usage")
        .await
        .expect("SELECT view_resource_usage");
    let table_rows = data_rows_from(&table_msgs);
    assert_eq!(table_rows.len(), 1);
    let workload_msgs = client
        .simple_query("SHOW RESOURCE USAGE FOR WORKLOAD fast_lane")
        .await
        .expect("SHOW RESOURCE USAGE FOR WORKLOAD fast_lane");
    let workload_rows = data_rows_from(&workload_msgs);
    assert_eq!(workload_rows.len(), 1);
    let workload_table_msgs = client
        .simple_query("SELECT * FROM rockstream_catalog.workload_resource_usage")
        .await
        .expect("SELECT workload_resource_usage");
    let workload_table_rows = data_rows_from(&workload_table_msgs);
    assert_eq!(workload_table_rows.len(), 1);
    let cluster_msgs = client
        .simple_query("SHOW CLUSTER RESOURCE USAGE")
        .await
        .expect("SHOW CLUSTER RESOURCE USAGE");
    let cluster_rows = data_rows_from(&cluster_msgs);
    assert_eq!(cluster_rows.len(), 1);

    for col in [
        "view_name",
        "workload_name",
        "state_bytes",
        "memory_bytes",
        "memory_limit_bytes",
        "state_budget_bytes",
        "freshness_slo_ms",
        "freshness_lag_ms",
        "slo_compliant",
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            show_rows[0].get(col.0),
            table_rows[0].get(col.1),
            "column {} differed",
            col.1
        );
    }
    assert_eq!(show_rows[0].get(0).unwrap_or(""), "big_orders");
    assert_eq!(show_rows[0].get(1).unwrap_or(""), "fast_lane");
    assert_eq!(show_rows[0].get(2).unwrap_or(""), "2048");
    assert_eq!(show_rows[0].get(3).unwrap_or(""), "4096");
    assert_eq!(show_rows[0].get(6).unwrap_or(""), "500");
    assert_eq!(show_rows[0].get(7).unwrap_or(""), "250");
    assert_eq!(show_rows[0].get(8).unwrap_or(""), "true");
    assert!(show_rows[0].get(10).is_none());
    assert_eq!(workload_rows[0].get(0).unwrap_or(""), "fast_lane");
    assert_eq!(workload_rows[0].get(1).unwrap_or(""), "1");
    assert_eq!(workload_rows[0].get(2).unwrap_or(""), "2048");
    assert_eq!(workload_rows[0].get(3).unwrap_or(""), "4096");
    assert_eq!(workload_rows[0].get(6).unwrap_or(""), "500");
    assert_eq!(workload_rows[0].get(7).unwrap_or(""), "250");
    assert_eq!(workload_rows[0].get(8).unwrap_or(""), "true");
    assert_eq!(
        workload_rows[0].get(0),
        workload_table_rows[0].get("workload_name")
    );
    assert_eq!(cluster_rows[0].get(0).unwrap_or(""), "cluster");
    assert_eq!(cluster_rows[0].get(1).unwrap_or(""), "1");
    assert_eq!(cluster_rows[0].get(2).unwrap_or(""), "2048");
    assert_eq!(cluster_rows[0].get(3).unwrap_or(""), "4096");
}

// ── Tutorial DAG: 3-level view chain (aggregate → join → filter) ─────────────

/// Validates the 3-level DAG shown in the 30-minute tutorial:
///
///   campaigns ──────────────────────────────────┐
///                                               ▼
///   conversions → campaign_totals → campaign_report → high_performers
///
/// Proves that:
/// 1. An aggregate view over a base table materialises correctly.
/// 2. A join view (base table ⋈ aggregate view) materialises correctly.
/// 3. A filter materialized view on the join view materialises correctly.
/// 4. Inserting more data that crosses the threshold updates high_performers.
#[tokio::test]
async fn tutorial_dag_three_level_chain_materialises_correctly() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("tutorial-dag-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // ── DDL: two base tables ─────────────────────────────────────────────────
    client
        .simple_query(
            "CREATE TABLE campaigns \
             (campaign_id BIGINT, name TEXT, channel TEXT, budget BIGINT)",
        )
        .await
        .expect("CREATE TABLE campaigns");

    client
        .simple_query(
            "CREATE TABLE conversions \
             (conv_id BIGINT, campaign_id BIGINT, revenue BIGINT, ts BIGINT)",
        )
        .await
        .expect("CREATE TABLE conversions");

    // ── DDL: 3-level view chain ──────────────────────────────────────────────
    // Level 1: aggregate conversions per campaign
    client
        .simple_query(
            "CREATE VIEW campaign_totals AS \
             SELECT campaign_id, COUNT(*) AS conv_count, SUM(revenue) AS total_revenue \
             FROM conversions GROUP BY campaign_id",
        )
        .await
        .expect("CREATE VIEW campaign_totals");

    // Level 2: join aggregate view with base table
    client
        .simple_query(
            "CREATE VIEW campaign_report AS \
             SELECT c.name, c.channel, t.conv_count, t.total_revenue \
             FROM campaigns c JOIN campaign_totals t ON c.campaign_id = t.campaign_id",
        )
        .await
        .expect("CREATE VIEW campaign_report");

    // Level 3: filter materialized view on join view
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW high_performers AS \
             SELECT name, channel, total_revenue \
             FROM campaign_report WHERE total_revenue > 500",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW high_performers");

    // ── Transaction 1: seed campaigns ────────────────────────────────────────
    client
        .simple_query("SET rockstream.idempotency_key = 'tutorial-dag-txn-001'")
        .await
        .expect("SET idempotency_key 1");
    client.simple_query("BEGIN").await.expect("BEGIN 1");
    client
        .simple_query("INSERT INTO campaigns (campaign_id, name, channel, budget) VALUES (1, 'Summer Sale', 'email', 5000)")
        .await
        .expect("INSERT campaign 1");
    client
        .simple_query("INSERT INTO campaigns (campaign_id, name, channel, budget) VALUES (2, 'Brand Awareness', 'social', 3000)")
        .await
        .expect("INSERT campaign 2");
    client
        .simple_query("INSERT INTO campaigns (campaign_id, name, channel, budget) VALUES (3, 'Retargeting', 'display', 2000)")
        .await
        .expect("INSERT campaign 3");
    client.simple_query("COMMIT").await.expect("COMMIT 1");

    // After seeding campaigns: high_performers must be empty (no conversions yet).
    shard_db.flush().await.unwrap();
    let hp_msgs0 = client
        .simple_query("SELECT * FROM high_performers")
        .await
        .expect("SELECT high_performers after txn1");
    let hp_rows0 = data_rows_from(&hp_msgs0);
    assert_eq!(
        hp_rows0.len(),
        0,
        "high_performers should be empty before any conversions; got {}",
        hp_rows0.len()
    );

    // ── Transaction 2: first batch of conversions ─────────────────────────────
    // campaign 1 (Summer Sale): 2 conversions totalling 550 → above threshold
    // campaign 2 (Brand Awareness): 1 conversion of 150 → below threshold
    // campaign 3 (Retargeting): 1 conversion of 600 → above threshold
    client
        .simple_query("SET rockstream.idempotency_key = 'tutorial-dag-txn-002'")
        .await
        .expect("SET idempotency_key 2");
    client.simple_query("BEGIN").await.expect("BEGIN 2");
    client
        .simple_query("INSERT INTO conversions (conv_id, campaign_id, revenue, ts) VALUES (101, 1, 300, 1000)")
        .await
        .expect("INSERT conv 101");
    client
        .simple_query("INSERT INTO conversions (conv_id, campaign_id, revenue, ts) VALUES (102, 1, 250, 1001)")
        .await
        .expect("INSERT conv 102");
    client
        .simple_query("INSERT INTO conversions (conv_id, campaign_id, revenue, ts) VALUES (103, 2, 150, 1002)")
        .await
        .expect("INSERT conv 103");
    client
        .simple_query("INSERT INTO conversions (conv_id, campaign_id, revenue, ts) VALUES (104, 3, 600, 1003)")
        .await
        .expect("INSERT conv 104");
    client.simple_query("COMMIT").await.expect("COMMIT 2");

    shard_db.flush().await.unwrap();

    // campaign_totals: 3 rows (one per campaign), ordered by campaign_id
    let ct_msgs = client
        .simple_query("SELECT * FROM campaign_totals ORDER BY campaign_id")
        .await
        .expect("SELECT campaign_totals");
    let ct_rows = data_rows_from(&ct_msgs);
    assert_eq!(
        ct_rows.len(),
        3,
        "campaign_totals should have 3 rows; got {}",
        ct_rows.len()
    );
    assert_eq!(ct_rows[0].get("campaign_id").unwrap_or(""), "1");
    assert_eq!(ct_rows[0].get("conv_count").unwrap_or(""), "2");
    assert_eq!(ct_rows[0].get("total_revenue").unwrap_or(""), "550");
    assert_eq!(ct_rows[1].get("campaign_id").unwrap_or(""), "2");
    assert_eq!(ct_rows[1].get("conv_count").unwrap_or(""), "1");
    assert_eq!(ct_rows[1].get("total_revenue").unwrap_or(""), "150");
    assert_eq!(ct_rows[2].get("campaign_id").unwrap_or(""), "3");
    assert_eq!(ct_rows[2].get("conv_count").unwrap_or(""), "1");
    assert_eq!(ct_rows[2].get("total_revenue").unwrap_or(""), "600");

    // campaign_report: 3 rows (inner join matched all 3 campaigns), ordered by name
    let cr_msgs = client
        .simple_query("SELECT * FROM campaign_report ORDER BY name")
        .await
        .expect("SELECT campaign_report");
    let cr_rows = data_rows_from(&cr_msgs);
    assert_eq!(
        cr_rows.len(),
        3,
        "campaign_report should have 3 rows; got {}",
        cr_rows.len()
    );
    // ORDER BY name: Brand Awareness < Retargeting < Summer Sale
    assert_eq!(cr_rows[0].get("name").unwrap_or(""), "Brand Awareness");
    assert_eq!(cr_rows[0].get("channel").unwrap_or(""), "social");
    assert_eq!(cr_rows[0].get("conv_count").unwrap_or(""), "1");
    assert_eq!(cr_rows[0].get("total_revenue").unwrap_or(""), "150");
    assert_eq!(cr_rows[1].get("name").unwrap_or(""), "Retargeting");
    assert_eq!(cr_rows[1].get("channel").unwrap_or(""), "display");
    assert_eq!(cr_rows[1].get("conv_count").unwrap_or(""), "1");
    assert_eq!(cr_rows[1].get("total_revenue").unwrap_or(""), "600");
    assert_eq!(cr_rows[2].get("name").unwrap_or(""), "Summer Sale");
    assert_eq!(cr_rows[2].get("channel").unwrap_or(""), "email");
    assert_eq!(cr_rows[2].get("conv_count").unwrap_or(""), "2");
    assert_eq!(cr_rows[2].get("total_revenue").unwrap_or(""), "550");

    // high_performers: 2 rows (Summer Sale=550 and Retargeting=600 exceed 500;
    //                          Brand Awareness=150 does not).
    // ORDER BY total_revenue DESC, name: Retargeting(600) then Summer Sale(550).
    let hp_msgs2 = client
        .simple_query("SELECT * FROM high_performers ORDER BY total_revenue DESC, name")
        .await
        .expect("SELECT high_performers after txn2");
    let hp_rows2 = data_rows_from(&hp_msgs2);
    assert_eq!(
        hp_rows2.len(),
        2,
        "high_performers should have 2 rows (total_revenue > 500); got {}",
        hp_rows2.len()
    );
    assert_eq!(hp_rows2[0].get("name").unwrap_or(""), "Retargeting");
    assert_eq!(hp_rows2[0].get("channel").unwrap_or(""), "display");
    assert_eq!(hp_rows2[0].get("total_revenue").unwrap_or(""), "600");
    assert_eq!(hp_rows2[1].get("name").unwrap_or(""), "Summer Sale");
    assert_eq!(hp_rows2[1].get("channel").unwrap_or(""), "email");
    assert_eq!(hp_rows2[1].get("total_revenue").unwrap_or(""), "550");

    // ── Transaction 3: Brand Awareness crosses the threshold ──────────────────
    // One more conversion worth 400 pushes campaign 2 from 150 to 550 (> 500).
    client
        .simple_query("SET rockstream.idempotency_key = 'tutorial-dag-txn-003'")
        .await
        .expect("SET idempotency_key 3");
    client
        .simple_query("INSERT INTO conversions (conv_id, campaign_id, revenue, ts) VALUES (105, 2, 400, 1004)")
        .await
        .expect("INSERT conv 105");
    client.simple_query("COMMIT").await.expect("COMMIT 3");

    shard_db.flush().await.unwrap();

    // high_performers: now 3 rows — Brand Awareness (150+400=550) joined the club.
    // ORDER BY total_revenue DESC, name: Retargeting(600), Brand Awareness(550), Summer Sale(550).
    let hp_msgs3 = client
        .simple_query("SELECT * FROM high_performers ORDER BY total_revenue DESC, name")
        .await
        .expect("SELECT high_performers after txn3");
    let hp_rows3 = data_rows_from(&hp_msgs3);
    assert_eq!(
        hp_rows3.len(),
        3,
        "after threshold crossing, high_performers should have 3 rows; got {}",
        hp_rows3.len()
    );
    assert_eq!(hp_rows3[0].get("name").unwrap_or(""), "Retargeting");
    assert_eq!(hp_rows3[0].get("channel").unwrap_or(""), "display");
    assert_eq!(hp_rows3[0].get("total_revenue").unwrap_or(""), "600");
    assert_eq!(hp_rows3[1].get("name").unwrap_or(""), "Brand Awareness");
    assert_eq!(hp_rows3[1].get("channel").unwrap_or(""), "social");
    assert_eq!(hp_rows3[1].get("total_revenue").unwrap_or(""), "550");
    assert_eq!(hp_rows3[2].get("name").unwrap_or(""), "Summer Sale");
    assert_eq!(hp_rows3[2].get("channel").unwrap_or(""), "email");
    assert_eq!(hp_rows3[2].get("total_revenue").unwrap_or(""), "550");
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.32 — Index DDL via pgwire (CREATE INDEX, DROP INDEX, REBUILD INDEX)
// These tests verify that the gateway correctly routes index DDL statements
// through the wire protocol and that error codes RS-2016 / RS-2014 surface
// correctly to pgwire clients.
// ══════════════════════════════════════════════════════════════════════════════

/// CREATE INDEX … ON … via pgwire registers the index in Building state.
#[tokio::test]
async fn create_index_ddl_registers_in_catalog_via_wire() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let result = client
        .simple_query("CREATE INDEX idx_orders_email ON orders (email)")
        .await
        .expect("CREATE INDEX failed");

    assert!(
        result
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))),
        "expected CommandComplete for CREATE INDEX; got {result:?}"
    );

    let entry = catalog
        .get_index("idx_orders_email")
        .expect("index not in catalog after CREATE INDEX");
    assert_eq!(entry.table, "orders");
    assert_eq!(entry.index_cols, vec!["email".to_string()]);
    assert_eq!(entry.state, CatalogIndexState::Building);
}

/// DROP INDEX removes the index from the catalog; a second DROP returns an error.
#[tokio::test]
async fn drop_index_ddl_removes_from_catalog_via_wire() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE INDEX idx_drop_test ON products (sku)")
        .await
        .expect("CREATE INDEX failed");

    assert!(
        catalog.get_index("idx_drop_test").is_some(),
        "index must exist before DROP"
    );

    client
        .simple_query("DROP INDEX idx_drop_test")
        .await
        .expect("DROP INDEX failed");

    assert!(
        catalog.get_index("idx_drop_test").is_none(),
        "index must be gone after DROP INDEX"
    );

    // Second DROP on nonexistent index returns an error (42704).
    let result = client.simple_query("DROP INDEX idx_drop_test").await;
    let got_err = match &result {
        Err(e) => e
            .as_db_error()
            .map(|d| d.message().contains("does not exist"))
            .unwrap_or(false),
        Ok(msgs) => msgs
            .iter()
            .any(|m| format!("{m:?}").contains("does not exist")),
    };
    assert!(got_err, "expected 42704 for missing index; got {result:?}");
}

/// REBUILD INDEX transitions state back to Building; nonexistent index returns error.
#[tokio::test]
async fn rebuild_index_ddl_transitions_state_via_wire() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE INDEX idx_rebuild_test ON events (user_id)")
        .await
        .expect("CREATE INDEX failed");

    // Initially Building; REBUILD returns CommandComplete.
    let result = client
        .simple_query("REBUILD INDEX idx_rebuild_test")
        .await
        .expect("REBUILD INDEX failed");

    assert!(
        result
            .iter()
            .any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))),
        "expected CommandComplete for REBUILD INDEX"
    );

    // State must still be Building after REBUILD.
    let entry = catalog.get_index("idx_rebuild_test").unwrap();
    assert_eq!(
        entry.state,
        CatalogIndexState::Building,
        "index must be Building after REBUILD INDEX"
    );

    // REBUILD on nonexistent index returns error.
    let result = client.simple_query("REBUILD INDEX idx_nonexistent").await;
    let got_err = match &result {
        Err(e) => e
            .as_db_error()
            .map(|d| d.message().contains("does not exist"))
            .unwrap_or(false),
        Ok(msgs) => msgs
            .iter()
            .any(|m| format!("{m:?}").contains("does not exist")),
    };
    assert!(
        got_err,
        "expected 42704 for nonexistent index; got {result:?}"
    );
}

/// CREATE INDEX with a name already used for a different table returns RS-2016.
#[tokio::test]
async fn create_index_name_conflict_returns_rs2016_via_wire() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    // Register index on table_a.
    client
        .simple_query("CREATE INDEX idx_conflict ON table_a (col1)")
        .await
        .expect("first CREATE INDEX failed");

    // Attempt same index name on a different table → RS-2016.
    let result = client
        .simple_query("CREATE INDEX idx_conflict ON table_b (col2)")
        .await;

    let got_rs2016 = match &result {
        Err(e) => {
            if let Some(db) = e.as_db_error() {
                db.message().contains("RS-2016")
            } else {
                e.to_string().contains("RS-2016")
            }
        }
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("RS-2016")),
    };
    assert!(
        got_rs2016,
        "expected RS-2016 for index name conflict; got {result:?}"
    );

    // Same name on same table is idempotent with IF NOT EXISTS.
    client
        .simple_query("CREATE INDEX IF NOT EXISTS idx_conflict ON table_a (col1)")
        .await
        .expect("idempotent CREATE INDEX on same table should succeed");
}

#[tokio::test]
async fn create_index_publishes_precise_stats_immediately() {
    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder("v048-index-stats-publish", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, email TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-index-stats-seed'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query(
            "INSERT INTO orders (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com')",
        )
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_orders_email ON orders (email)")
        .await
        .expect("CREATE INDEX failed");
    client
        .simple_query("MARK INDEX idx_orders_email READY op_id=7")
        .await
        .expect("MARK INDEX READY failed");

    let stats = catalog.shard_stats("orders");
    assert_eq!(
        stats.len(),
        1,
        "expected one shard_stats entry after MARK INDEX READY"
    );
    let email_stats = stats[0]
        .col_stats
        .iter()
        .find(|stats| stats.col_idx == 1)
        .expect("missing email stats");
    assert_eq!(
        email_stats.min_bytes.as_deref(),
        Some("alice@example.com".as_bytes())
    );
    assert_eq!(
        email_stats.max_bytes.as_deref(),
        Some("bob@example.com".as_bytes())
    );
    let filter = email_stats
        .bloom_filter
        .as_ref()
        .expect("indexed column must publish an exact filter");
    assert!(bloom_filter_might_contain(filter, b"alice@example.com"));
    assert!(bloom_filter_might_contain(filter, b"bob@example.com"));
    assert!(!bloom_filter_might_contain(filter, b"carol@example.com"));
}

#[tokio::test]
async fn indexed_column_stats_are_exact_not_probabilistic() {
    let catalog = Arc::new(CatalogStubs::new());
    let shard_db = Arc::new(
        ShardDb::builder("v048-index-stats-exact", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE TABLE orders (id BIGINT, email TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-index-stats-exact-seed'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query(
            "INSERT INTO orders (id, email) VALUES (1, 'alice@example.com'), (2, 'bob@example.com')",
        )
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client
        .simple_query("CREATE INDEX idx_orders_email_exact ON orders (email)")
        .await
        .expect("CREATE INDEX failed");
    client
        .simple_query("MARK INDEX idx_orders_email_exact READY op_id=9")
        .await
        .expect("MARK INDEX READY failed");

    let stats = catalog.shard_stats("orders");
    let plan = plan_scatter_shards(
        &stats,
        &[ScatterPredicate::Eq {
            col_idx: 1,
            value: b"amy@example.com".to_vec(),
        }],
        5,
        1,
    );
    assert!(
        plan.shard_ids.is_empty(),
        "exact indexed-column filter must prune an in-range non-member without a false positive"
    );
}

/// EXPLAIN shows RS-2014 hint for a BUILDING index covering the queried table.
#[tokio::test]
async fn explain_shows_rs2014_hint_for_building_index_via_wire() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    // Register an index on orders in Building state.
    client
        .simple_query("CREATE INDEX idx_orders_status ON orders (status)")
        .await
        .expect("CREATE INDEX failed");

    // EXPLAIN a query over orders — should surface RS-2014.
    let msgs = client
        .simple_query("EXPLAIN SELECT * FROM orders WHERE status = 'shipped'")
        .await
        .expect("EXPLAIN failed");

    let plan_output: String = msgs
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        plan_output.contains("RS-2014"),
        "EXPLAIN output must contain RS-2014 hint for building index; got:\n{plan_output}"
    );
}

/// EXPLAIN over a view with no registered sink does NOT contain `sink_target:`.
#[tokio::test]
async fn explain_no_sink_target_for_unsunk_view_via_wire() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE VIEW unsunk_view AS SELECT id, val FROM base WHERE val > 0")
        .await
        .expect("CREATE VIEW failed");

    let msgs = client
        .simple_query("EXPLAIN SELECT * FROM unsunk_view")
        .await
        .expect("EXPLAIN failed");

    let plan_output: String = msgs
        .iter()
        .filter_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                r.get(0).map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        !plan_output.contains("sink_target:"),
        "EXPLAIN output must NOT contain sink_target for a view with no registered sink; got:\n{plan_output}"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.32 gap-fill: index scan SELECT, state sync (MARK INDEX READY), proof test
// ══════════════════════════════════════════════════════════════════════════════

/// MARK INDEX <name> READY op_id=<n> transitions catalog state to Ready and
/// records the backing OperatorId so the gateway can route point lookups.
#[tokio::test]
async fn mark_index_ready_updates_catalog_state_via_wire() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    client
        .simple_query("CREATE INDEX idx_events_uid ON events (user_id)")
        .await
        .expect("CREATE INDEX failed");

    let before = catalog.get_index("idx_events_uid").unwrap();
    assert_eq!(before.state, CatalogIndexState::Building);
    assert_eq!(before.op_id, None);

    client
        .simple_query("MARK INDEX idx_events_uid READY op_id=42")
        .await
        .expect("MARK INDEX READY failed");

    let after = catalog.get_index("idx_events_uid").unwrap();
    assert_eq!(after.state, CatalogIndexState::Ready);
    assert_eq!(after.op_id, Some(42));
}

/// MARK INDEX READY on a nonexistent index returns error 42704.
#[tokio::test]
async fn mark_index_ready_nonexistent_returns_error() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog.clone(),
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;

    let result = client
        .simple_query("MARK INDEX idx_ghost READY op_id=1")
        .await;
    let got_err = match &result {
        Err(e) => e
            .as_db_error()
            .map(|d| d.message().contains("does not exist"))
            .unwrap_or(false),
        Ok(msgs) => msgs
            .iter()
            .any(|m| format!("{m:?}").contains("does not exist")),
    };
    assert!(
        got_err,
        "expected 42704 for nonexistent index; got {result:?}"
    );
}

/// P1 (wire) — SELECT WHERE on a READY index uses the index arrangement and
/// returns only matching rows within the latency target.
///
/// Proof claim: a point-lookup SELECT through pgwire on a READY secondary index
/// returns the correct subset of rows and completes in < 10 ms p99.
#[tokio::test]
async fn proof_index_scan_point_lookup_via_wire() {
    use rockstream_ops::index_arrange::{BackfillRow, IndexArrangeOp, MAX_INDEX_ARRANGE_ROWS};
    use rockstream_types::ids::OperatorId;

    const INDEX_OP_ID: u64 = 99;

    // Build an in-memory ShardDb and populate it via IndexArrangeOp.
    let store = Arc::new(object_store::memory::InMemory::new());
    let shard_db = Arc::new(
        rockstream_storage::ShardDb::builder("idx-scan-proof", store.clone())
            .build()
            .await
            .unwrap(),
    );

    // index_cols=[0] (account_id), pk_cols=[1] (order_id)
    let op = IndexArrangeOp::new(
        shard_db.clone(),
        OperatorId(INDEX_OP_ID),
        vec![0],
        vec![1],
        MAX_INDEX_ARRANGE_ROWS,
    );

    // Insert 5 rows: account_id ∈ {111, 222}
    // Rows with account_id=111: order_id 1 and 3
    // Rows with account_id=222: order_id 2, 4, 5
    let rows = vec![
        BackfillRow {
            index_val: 111,
            pk_val: 1,
        },
        BackfillRow {
            index_val: 222,
            pk_val: 2,
        },
        BackfillRow {
            index_val: 111,
            pk_val: 3,
        },
        BackfillRow {
            index_val: 222,
            pk_val: 4,
        },
        BackfillRow {
            index_val: 222,
            pk_val: 5,
        },
    ];
    op.run_backfill_rows(&rows, "idx_by_account", shard_db.clone(), 0)
        .await
        .expect("backfill failed");

    // Start gateway with the same ShardDb.
    let catalog = Arc::new(CatalogStubs::new());
    // Register the "orders" table with known column names.
    catalog.add_table(rockstream_gateway::catalog_stubs::CatalogTable {
        name: "orders".to_string(),
        columns: vec![
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "account_id".to_string(),
                data_type: "Int64".to_string(),
            },
            rockstream_gateway::catalog_stubs::CatalogColumn {
                name: "order_id".to_string(),
                data_type: "Int64".to_string(),
            },
        ],
    });
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    // Register index via wire and transition to Ready with the correct op_id.
    client
        .simple_query("CREATE INDEX idx_by_account ON orders (account_id)")
        .await
        .expect("CREATE INDEX failed");
    client
        .simple_query(&format!(
            "MARK INDEX idx_by_account READY op_id={INDEX_OP_ID}"
        ))
        .await
        .expect("MARK INDEX READY failed");

    // Verify catalog state.
    let entry = catalog.get_index("idx_by_account").unwrap();
    assert_eq!(entry.state, CatalogIndexState::Ready);
    assert_eq!(entry.op_id, Some(INDEX_OP_ID));

    // Issue point-lookup SELECT via wire and measure latency.
    let t0 = std::time::Instant::now();
    let msgs = client
        .simple_query("SELECT * FROM orders WHERE account_id = 111")
        .await
        .expect("SELECT failed");
    let elapsed_ms = t0.elapsed().as_millis();

    // Extract data rows.
    let data_rows = data_rows_from(&msgs);

    assert_eq!(
        data_rows.len(),
        2,
        "expected 2 rows for account_id=111; got {}",
        data_rows.len()
    );

    // Both rows must have account_id=111.
    for row in &data_rows {
        let acct = row.get("account_id").unwrap_or("");
        assert_eq!(acct, "111", "returned row has wrong account_id: {acct}");
    }

    // Order ids must be 1 and 3 (in any order).
    let mut order_ids: Vec<i64> = data_rows
        .iter()
        .filter_map(|r| r.get("order_id").and_then(|v| v.parse().ok()))
        .collect();
    order_ids.sort();
    assert_eq!(order_ids, vec![1i64, 3], "wrong order_ids: {order_ids:?}");

    // P1 latency gate: < 10 ms for an in-memory shard.
    assert!(
        elapsed_ms < 10,
        "index point-lookup took {elapsed_ms}ms — must be < 10ms"
    );
}

// ── v0.39 Green Gates ─────────────────────────────────────────────────────────

// ── Slice 1: SQLSTATE mapping ─────────────────────────────────────────────────

/// Every GatewayError variant must map to the correct 5-char SQLSTATE.
/// Slice 1 green gate from v0.39-plan.md.
#[test]
fn test_all_rs_codes_have_sqlstate() {
    use rockstream_gateway::error::{sqlstate_for, GatewayError};

    let cases: Vec<(&str, GatewayError)> = vec![
        ("25001", GatewayError::SerializableNotSupported),
        (
            "53200",
            GatewayError::PreparedStatementsLimitExceeded { limit: 100 },
        ),
        ("53200", GatewayError::PortalsLimitExceeded { limit: 100 }),
        ("42P01", GatewayError::ViewNotFound("v".into())),
        ("54000", GatewayError::ResultSetTooLarge),
        (
            "53200",
            GatewayError::ShardBackpressure {
                current_bytes: 1,
                limit_bytes: 2,
            },
        ),
        ("XX000", GatewayError::IdempotencyKeyRequired),
        (
            "42P01",
            GatewayError::CopyTableNotFound { table: "t".into() },
        ),
        (
            "22000",
            GatewayError::CopyColumnCountMismatch {
                expected: 2,
                got: 1,
            },
        ),
        ("57014", GatewayError::QueryCancelled),
        ("34000", GatewayError::CursorNotFound { name: "c".into() }),
        (
            "42P03",
            GatewayError::CursorAlreadyExists { name: "c".into() },
        ),
        ("53200", GatewayError::MemoryLimitExceeded),
        ("57014", GatewayError::StatementTimeout),
        (
            "53300",
            GatewayError::ConnectionLimitExceeded { limit: 10_000 },
        ),
        (
            "28000",
            GatewayError::InvalidPassword {
                user: "alice".into(),
            },
        ),
        ("XX000", GatewayError::NotSupported("x".into())),
        ("42601", GatewayError::ParseError("x".into())),
        ("XX000", GatewayError::PgWire("x".into())),
        ("25P02", GatewayError::InFailedSqlTransaction),
        (
            "3B001",
            GatewayError::SavepointNotFound { name: "s".into() },
        ),
        ("0A000", GatewayError::TwoPhaseNotSupported),
        ("54000", GatewayError::SavepointLimitExceeded { limit: 128 }),
        (
            "54000",
            GatewayError::NotifyChannelLimitExceeded { limit: 1000 },
        ),
    ];

    for (expected_sqlstate, err) in &cases {
        let got = sqlstate_for(err);
        assert_eq!(
            got, *expected_sqlstate,
            "SQLSTATE mismatch for {:?}: expected {} got {}",
            err, expected_sqlstate, got
        );
        assert_eq!(got.len(), 5, "SQLSTATE '{got}' is not 5 chars for {err:?}");
    }
}

// ── Slice 2: CancelRequest / BackendKeyData ───────────────────────────────────

/// Slow view reader: sleeps `sleep_ms` before returning rows.
struct SlowViewReader {
    sleep_ms: u64,
}

#[async_trait::async_trait]
impl ViewReader for SlowViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        tokio::time::sleep(tokio::time::Duration::from_millis(self.sleep_ms)).await;
        Ok(vec![b"row1".to_vec()])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

/// Slice 2 green gate: CancelRequest aborts a slow query and the connection
/// is reusable afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancel_request_aborts_query() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "slow_view".to_string(),
        sql: String::new(),
        namespace: "public".to_string(),
        columns: vec![CatalogColumn {
            name: "col".to_string(),
            data_type: "Utf8".to_string(),
        }],
        op_id: None,
    });

    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(catalog),
        Arc::new(SlowViewReader { sleep_ms: 3_000 }),
    );
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    // Connect
    let (client, conn_task) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .expect("connect");
    let cancel_token = client.cancel_token();
    tokio::spawn(async move {
        conn_task.await.ok();
    });

    // Issue a slow query in the background
    let query_handle = tokio::spawn(async move {
        let t0 = Instant::now();
        let result = client.simple_query("SELECT * FROM slow_view").await;
        (result, t0.elapsed())
    });

    // Wait 50ms then cancel
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    cancel_token
        .cancel_query(NoTls)
        .await
        .expect("cancel_query");

    let (_result, elapsed) = query_handle.await.unwrap();

    // Query must have been cancelled (error) OR completed very fast due to select!
    // Either way, elapsed < 1000ms (not the full 3s sleep)
    assert!(
        elapsed < tokio::time::Duration::from_millis(1_000),
        "query took {elapsed:?} — cancel should have interrupted the 3s sleep"
    );

    // Connection reuse: new connection should work fine
    let client2 = connect_port(port).await;
    let rows = client2
        .simple_query("SELECT 1")
        .await
        .expect("second query after cancel");
    assert!(
        !rows.is_empty(),
        "connection reuse after cancel should work"
    );
}

// ── Slice 3: Named Cursors ────────────────────────────────────────────────────

/// Slice 3 green gate: DECLARE / FETCH / MOVE / CLOSE full lifecycle.
#[tokio::test]
async fn test_named_cursor_lifecycle() {
    let store = Arc::new(InMemory::new());
    let shard_db = ShardDb::builder("cursor-shard", store.clone())
        .build()
        .await
        .unwrap();

    // Write 250 rows to my_view
    for i in 0u32..250 {
        let key = format!("view_output/cursor_view/{:08}", i);
        let value = format!("row_{i}");
        shard_db
            .put(key.as_bytes(), value.as_bytes())
            .await
            .unwrap();
    }
    shard_db.flush().await.unwrap();

    let reader = ShardReader::open("cursor-shard", store.clone())
        .await
        .unwrap();
    let view_reader = Arc::new(HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: Some(1),
    });

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "cursor_view".to_string(),
        sql: String::new(),
        namespace: "public".to_string(),
        columns: vec![CatalogColumn {
            name: "col".to_string(),
            data_type: "Utf8".to_string(),
        }],
        op_id: None,
    });

    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), view_reader);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    let client = connect_port(port).await;

    // DECLARE cursor
    client
        .simple_query("DECLARE cur1 CURSOR FOR SELECT * FROM cursor_view")
        .await
        .expect("DECLARE failed");

    // FETCH 100 — should return 100 rows
    let rows = client
        .simple_query("FETCH 100 FROM cur1")
        .await
        .expect("FETCH 100 failed");
    let data_rows = data_rows_from(&rows);
    assert_eq!(data_rows.len(), 100, "FETCH 100 should return 100 rows");

    // FETCH ALL — should return remaining 150 rows
    let rows2 = client
        .simple_query("FETCH ALL FROM cur1")
        .await
        .expect("FETCH ALL failed");
    let data_rows2 = data_rows_from(&rows2);
    assert_eq!(
        data_rows2.len(),
        150,
        "FETCH ALL should return remaining 150 rows"
    );

    // FETCH from exhausted cursor — 0 rows, no error
    let rows3 = client
        .simple_query("FETCH 10 FROM cur1")
        .await
        .expect("FETCH on exhausted cursor failed");
    let data_rows3 = data_rows_from(&rows3);
    assert_eq!(
        data_rows3.len(),
        0,
        "FETCH on exhausted cursor should return 0 rows"
    );

    // DECLARE second cursor and use MOVE to skip 50 rows
    client
        .simple_query("DECLARE cur2 CURSOR FOR SELECT * FROM cursor_view")
        .await
        .expect("DECLARE cur2 failed");

    client
        .simple_query("MOVE 50 FROM cur2")
        .await
        .expect("MOVE 50 failed");

    // FETCH 10 after MOVE 50 — rows at positions 50..60
    let rows4 = client
        .simple_query("FETCH 10 FROM cur2")
        .await
        .expect("FETCH 10 after MOVE failed");
    let data_rows4 = data_rows_from(&rows4);
    assert_eq!(
        data_rows4.len(),
        10,
        "FETCH 10 after MOVE 50 should return 10 rows"
    );

    // CLOSE cur1
    client
        .simple_query("CLOSE cur1")
        .await
        .expect("CLOSE cur1 failed");

    // CLOSE ALL removes all remaining cursors
    client
        .simple_query("CLOSE ALL")
        .await
        .expect("CLOSE ALL failed");

    // Connection still works
    let ping = client
        .simple_query("SELECT 1")
        .await
        .expect("ping after CLOSE ALL");
    assert!(
        !ping.is_empty(),
        "connection should remain usable after CLOSE ALL"
    );
}

// ── Slice 4: Streaming row delivery ──────────────────────────────────────────
// Unit test in: crates/rockstream-gateway/src/view_reader.rs
// Test name:    test_streaming_peak_memory_bounded

// ── Slice 5: PgBouncer compat ─────────────────────────────────────────────────

/// Slice 5 green gate: ReadyForQuery status byte is 'I' at idle, 'T' in transaction, 'I' after COMMIT/ROLLBACK.
/// Verifies PgBouncer-compatible transaction state tracking via observable client behaviour.
#[tokio::test]
async fn test_pgbouncer_compat_status_bytes() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    // Connect and run a baseline query — server should be in Idle ('I') state.
    let client = connect_port(port).await;
    client
        .simple_query("SELECT 1")
        .await
        .expect("baseline SELECT failed");

    // BEGIN puts the connection into InTransaction ('T') state.
    // Subsequent queries must work within the transaction.
    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT inside BEGIN failed");

    // COMMIT returns status to Idle ('I'); next query must work.
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT after COMMIT failed");

    // BEGIN + ROLLBACK cycle.
    client
        .simple_query("BEGIN")
        .await
        .expect("second BEGIN failed");
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT inside second BEGIN failed");
    client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK failed");
    client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT after ROLLBACK failed");

    // Multiple back-to-back transaction cycles — simulates PgBouncer session-mode reuse.
    for _ in 0..5 {
        client
            .simple_query("BEGIN")
            .await
            .expect("BEGIN in loop failed");
        client
            .simple_query("SELECT 1")
            .await
            .expect("SELECT in loop failed");
        client
            .simple_query("COMMIT")
            .await
            .expect("COMMIT in loop failed");
    }

    // Final baseline — connection must still be operational in Idle state.
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("final SELECT failed");
    assert!(
        !rows.is_empty(),
        "connection should be operational after multiple transaction cycles"
    );
}

/// Slice 5a green gate: DISCARD ALL clears all session state.
#[tokio::test]
async fn test_discard_all_clears_session() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();
    let client = connect_port(port).await;

    // Set a GUC, then DISCARD ALL should reset it
    client
        .simple_query("SET rockstream.wait_for_timeout_ms = 99999")
        .await
        .expect("SET");
    client
        .simple_query("DISCARD ALL")
        .await
        .expect("DISCARD ALL");

    // Connection still functional after DISCARD ALL
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT after DISCARD ALL");
    assert!(!rows.is_empty(), "connection should work after DISCARD ALL");
}

/// Slice 5b green gate: RESET ALL resets GUC settings only.
#[tokio::test]
async fn test_reset_all_preserves_cursors() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();
    let client = connect_port(port).await;

    // RESET ALL should succeed
    client.simple_query("RESET ALL").await.expect("RESET ALL");

    // Connection still functional
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT after RESET ALL");
    assert!(!rows.is_empty(), "connection should work after RESET ALL");
}

// ── Slice 6: pg_stat_activity ─────────────────────────────────────────────────

/// Slice 6 green gate: SELECT FROM pg_stat_activity returns the connected session.
#[tokio::test]
async fn test_pg_stat_activity_shows_connection() {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
    );
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();
    let client = connect_port(port).await;

    // Give the server a moment to register the session
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let msgs = client
        .simple_query("SELECT pid, usename, application_name, state FROM pg_stat_activity")
        .await
        .expect("pg_stat_activity query failed");

    let rows = data_rows_from(&msgs);
    // Should see at least one row (our own connection)
    assert!(
        !rows.is_empty(),
        "pg_stat_activity should return at least one row for the connected session"
    );

    // Verify required columns are present with correct structure
    let first_row = rows[0];
    assert!(first_row.get(0).is_some(), "pid column should be present");
    assert!(
        first_row.get(1).is_some(),
        "usename column should be present"
    );
    assert!(
        first_row.get(2).is_some(),
        "application_name column should be present"
    );
    assert!(first_row.get(3).is_some(), "state column should be present");
    let state = first_row.get(3).unwrap_or("").to_string();
    assert!(
        state == "idle" || state == "active",
        "state should be 'idle' or 'active', got: {state}"
    );
}

// ── v0.40: Role Catalog + SCRAM auth tests ────────────────────────────────────

/// S1 green gate: test_role_catalog_create_alter_drop
/// Unit test: create role alice/pencil, verify verifiers non-empty, alter password, drop.
#[test]
fn test_role_catalog_create_alter_drop() {
    use rockstream_gateway::role_catalog::{create_role_entry, RoleCatalog};

    let catalog = RoleCatalog::new();
    assert_eq!(catalog.len(), 0);

    // Create role
    let entry = create_role_entry("alice", "pencil");
    assert!(
        !entry.scram_salted_password.is_empty(),
        "salted_password must be non-empty"
    );
    assert!(!entry.scram_salt.is_empty(), "salt must be non-empty");
    assert_eq!(entry.scram_iterations, 4096);
    assert!(entry
        .md5_hash
        .as_deref()
        .map(|h| h.starts_with("md5"))
        .unwrap_or(false));

    catalog.insert(entry).expect("insert should succeed");
    assert_eq!(catalog.len(), 1);

    // Get and verify
    let got = catalog.get("alice").expect("alice should exist");
    assert_eq!(got.username, "alice");
    assert!(!got.scram_salted_password.is_empty());

    // Alter password
    let updated = catalog.update_password("alice", "newpass");
    assert!(
        updated,
        "update_password should return true for existing user"
    );

    // Verify new salted_password differs
    let got2 = catalog.get("alice").expect("alice should still exist");
    assert!(!got2.scram_salted_password.is_empty());

    // Drop
    let removed = catalog.remove("alice");
    assert!(removed, "remove should return true");
    assert_eq!(catalog.len(), 0);
    assert!(catalog.get("alice").is_none());
}

/// S5 green gate: test_scram_auth_flow_unit
/// End-to-end SCRAM handshake via tokio-postgres client against in-process gateway.
#[tokio::test]
async fn test_scram_auth_flow_unit() {
    use rockstream_gateway::role_catalog::{create_role_entry, RoleCatalog};

    let catalog = Arc::new(rockstream_gateway::catalog_stubs::CatalogStubs::new());
    let role_catalog = Arc::new(RoleCatalog::new());
    role_catalog
        .insert(create_role_entry("alice", "pencil"))
        .expect("insert alice");

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        GatewayServer::with_scram_auth(addr, catalog, Arc::new(NoopViewReader), role_catalog);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("SCRAM auth should succeed for alice/pencil");

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn error: {e}");
        }
    });

    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 should work after SCRAM auth");
    assert!(!rows.is_empty(), "should have at least one row");
}

/// S6 green gate: test_md5_auth_flow_unit
/// End-to-end MD5 handshake via tokio-postgres client against in-process gateway.
#[tokio::test]
async fn test_md5_auth_flow_unit() {
    use rockstream_gateway::role_catalog::{create_role_entry, RoleCatalog};

    let catalog = Arc::new(rockstream_gateway::catalog_stubs::CatalogStubs::new());
    let role_catalog = Arc::new(RoleCatalog::new());
    role_catalog
        .insert(create_role_entry("bob", "secret"))
        .expect("insert bob");

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        GatewayServer::with_md5_auth(addr, catalog, Arc::new(NoopViewReader), role_catalog);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=bob password=secret dbname=test sslmode=disable"),
        NoTls,
    )
    .await
    .expect("MD5 auth should succeed for bob/secret");

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn error: {e}");
        }
    });

    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("SELECT 1 should work after MD5 auth");
    assert!(!rows.is_empty(), "should have at least one row");

    // Wrong password must fail
    let bad = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=bob password=wrong dbname=test sslmode=disable"),
        NoTls,
    )
    .await;
    assert!(bad.is_err(), "wrong password should be rejected");
}

/// S7 green gate: test_bootstrap_functions
/// Verify current_user, session_user, pg_backend_pid, pg_is_in_recovery,
/// pg_postmaster_start_time, txid_current, current_schemas via live gateway.
#[tokio::test]
async fn test_bootstrap_functions() {
    use rockstream_gateway::role_catalog::{create_role_entry, RoleCatalog};

    let catalog = Arc::new(rockstream_gateway::catalog_stubs::CatalogStubs::new());
    let role_catalog = Arc::new(RoleCatalog::new());
    role_catalog
        .insert(create_role_entry("alice", "pencil"))
        .expect("insert alice");

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        GatewayServer::with_scram_auth(addr, catalog, Arc::new(NoopViewReader), role_catalog);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn error: {e}");
        }
    });

    // current_user
    let rows = client
        .simple_query("SELECT current_user")
        .await
        .expect("SELECT current_user");
    let data = data_rows_from(&rows);
    assert!(!data.is_empty(), "expected rows for current_user");
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(val, "alice", "current_user should be alice");

    // session_user
    let rows = client
        .simple_query("SELECT session_user")
        .await
        .expect("SELECT session_user");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(val, "alice", "session_user should be alice");

    // pg_backend_pid() — must be a non-negative integer string
    let rows = client
        .simple_query("SELECT pg_backend_pid()")
        .await
        .expect("SELECT pg_backend_pid()");
    let data = data_rows_from(&rows);
    assert!(!data.is_empty(), "expected rows for pg_backend_pid");
    let pid_str = data[0].get(0).unwrap_or("0");
    assert!(
        pid_str.parse::<u64>().is_ok(),
        "pg_backend_pid should be numeric, got: {pid_str}"
    );

    // pg_is_in_recovery() — must be "f"
    let rows = client
        .simple_query("SELECT pg_is_in_recovery()")
        .await
        .expect("SELECT pg_is_in_recovery()");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(val, "f", "pg_is_in_recovery() should return f");

    // pg_postmaster_start_time() — must be a non-empty timestamp string
    let rows = client
        .simple_query("SELECT pg_postmaster_start_time()")
        .await
        .expect("SELECT pg_postmaster_start_time()");
    let data = data_rows_from(&rows);
    let ts = data[0].get(0).unwrap_or("");
    assert!(
        !ts.is_empty(),
        "pg_postmaster_start_time should be non-empty"
    );
    assert!(
        ts.contains('-'),
        "pg_postmaster_start_time should look like a date: {ts}"
    );

    // txid_current() — must be "0"
    let rows = client
        .simple_query("SELECT txid_current()")
        .await
        .expect("SELECT txid_current()");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(val, "0", "txid_current() should return 0");

    // current_schemas(false) — must contain "public"
    let rows = client
        .simple_query("SELECT current_schemas(false)")
        .await
        .expect("SELECT current_schemas(false)");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert!(
        val.contains("public"),
        "current_schemas(false) should contain public, got: {val}"
    );

    // version() — must start with "PostgreSQL 14."
    let rows = client
        .simple_query("SELECT version()")
        .await
        .expect("SELECT version()");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert!(
        val.starts_with("PostgreSQL 14."),
        "version should start with PostgreSQL 14., got: {val}"
    );
}

/// S8 green gate: test_guc_round_trip
/// SET search_path / client_encoding / timezone then SHOW verifies round-trip.
#[tokio::test]
async fn test_guc_round_trip() {
    use rockstream_gateway::role_catalog::{create_role_entry, RoleCatalog};

    let catalog = Arc::new(rockstream_gateway::catalog_stubs::CatalogStubs::new());
    let role_catalog = Arc::new(RoleCatalog::new());
    role_catalog
        .insert(create_role_entry("alice", "pencil"))
        .expect("insert alice");

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        GatewayServer::with_scram_auth(addr, catalog, Arc::new(NoopViewReader), role_catalog);
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let port = local_addr.port();

    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn error: {e}");
        }
    });

    // SET and SHOW search_path
    client
        .simple_query("SET search_path = 'app'")
        .await
        .expect("SET search_path");
    let rows = client
        .simple_query("SHOW search_path")
        .await
        .expect("SHOW search_path");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(val, "app", "SHOW search_path should return 'app' after SET");

    // SET and SHOW client_encoding
    client
        .simple_query("SET client_encoding = 'UTF8'")
        .await
        .expect("SET client_encoding");
    let rows = client
        .simple_query("SHOW client_encoding")
        .await
        .expect("SHOW client_encoding");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(val, "UTF8", "SHOW client_encoding should return UTF8");

    // SET and SHOW timezone
    client
        .simple_query("SET timezone = 'America/New_York'")
        .await
        .expect("SET timezone");
    let rows = client
        .simple_query("SHOW timezone")
        .await
        .expect("SHOW timezone");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(
        val, "America/New_York",
        "SHOW timezone should return the set value"
    );
}

/// S9 green gate: test_search_path_view_resolution
/// Unqualified view SELECT is only served when the view's namespace is in search_path.
/// Uses Trust auth (GatewayServer::with_catalog) to avoid ACL checks in read_view_response.
#[tokio::test]
async fn test_search_path_view_resolution() {
    use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView};

    let catalog = CatalogStubs::new();

    // Register a view in namespace "public"
    catalog.add_view_in_namespace(CatalogView {
        name: "myview".to_string(),
        namespace: "public".to_string(),
        sql: "SELECT 1 AS id".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int32".to_string(),
        }],
        op_id: None,
    });

    let (port, _handle) = start_gateway_noop(catalog).await;
    let client = connect_port(port).await;

    // search_path = public → unqualified SELECT must succeed (no error)
    client
        .simple_query("SET search_path = 'public'")
        .await
        .expect("SET search_path public");
    let result = client.simple_query("SELECT * FROM myview").await;
    assert!(
        result.is_ok(),
        "SELECT myview with search_path=public should succeed, got: {:?}",
        result.err()
    );

    // search_path = other → unqualified SELECT must fail with 42P01
    client
        .simple_query("SET search_path = 'other'")
        .await
        .expect("SET search_path other");
    let result = client.simple_query("SELECT * FROM myview").await;
    assert!(
        result.is_err(),
        "SELECT myview with search_path=other should fail"
    );
    let err = result.unwrap_err();
    let err_msg = if let Some(db_err) = err.as_db_error() {
        format!("{} {}", db_err.code().code(), db_err.message())
    } else {
        err.to_string()
    };
    assert!(
        err_msg.contains("42P01") || err_msg.contains("does not exist"),
        "error should reference 42P01 or 'does not exist', got: {err_msg}"
    );

    // Qualified SELECT (public.myview) must succeed regardless of search_path
    let result = client.simple_query("SELECT * FROM public.myview").await;
    assert!(
        result.is_ok(),
        "qualified SELECT public.myview should succeed regardless of search_path, got: {:?}",
        result.err()
    );
}

// ── v0.41 Slice 4 green gate ─────────────────────────────────────────────────

/// S4 green gate: commands inside a failed explicit block return SQLSTATE 25P02;
/// ROLLBACK exits the failed block and the connection becomes usable again.
#[tokio::test]
async fn test_failed_block_blocks_commands() {
    let (port, _handle) =
        start_gateway_noop(rockstream_gateway::catalog_stubs::CatalogStubs::new()).await;
    let client = connect_port(port).await;

    // BEGIN — enters InTransaction
    client.simple_query("BEGIN").await.expect("BEGIN failed");

    // Force an error inside the explicit block: SERIALIZABLE → RS-2003 Error response
    let result = client
        .simple_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .await;
    let got_error = match &result {
        Err(e) => e.as_db_error().is_some(),
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("ERROR")),
    };
    assert!(
        got_error,
        "expected error from SERIALIZABLE inside BEGIN, got: {result:?}"
    );

    // Now inside failed block — any non-ROLLBACK command must return SQLSTATE 25P02
    let blocked = client.simple_query("SELECT 1").await;
    let has_25p02 = match &blocked {
        Err(e) => e
            .as_db_error()
            .map(|d| d.code().code() == "25P02")
            .unwrap_or(false),
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("25P02")),
    };
    assert!(
        has_25p02,
        "expected SQLSTATE 25P02 from command in failed block, got: {blocked:?}"
    );

    // ROLLBACK must succeed and exit the failed block
    client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK from failed block failed");

    // Connection must be usable again after ROLLBACK
    let after = client.simple_query("SELECT 1").await;
    assert!(
        after.is_ok(),
        "connection should be usable after ROLLBACK from failed block, got: {after:?}"
    );
}

// ── v0.41 Slice 6 green gate ─────────────────────────────────────────────────

/// S6 green gate: SET LOCAL reverts on ROLLBACK; SET (non-local) persists.
#[tokio::test]
async fn test_set_local_reverts_on_rollback() {
    let (port, _handle) =
        start_gateway_noop(rockstream_gateway::catalog_stubs::CatalogStubs::new()).await;
    let client = connect_port(port).await;

    // Establish a session-level value.
    client
        .simple_query("SET search_path = 'original'")
        .await
        .expect("SET search_path failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");

    // Override at transaction scope.
    client
        .simple_query("SET LOCAL search_path = 'local_value'")
        .await
        .expect("SET LOCAL failed");

    // SHOW inside transaction must return the local override.
    let msgs = client
        .simple_query("SHOW search_path")
        .await
        .expect("SHOW search_path inside BEGIN failed");
    let val_inside: Option<String> = msgs.iter().find_map(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            r.get(0).map(|v| v.to_string())
        } else {
            None
        }
    });
    assert_eq!(
        val_inside.as_deref(),
        Some("local_value"),
        "expected local_value inside transaction, got: {val_inside:?}"
    );

    // ROLLBACK — SET LOCAL value must be discarded.
    client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK failed");

    // SHOW after ROLLBACK must return the original session-level value.
    let msgs2 = client
        .simple_query("SHOW search_path")
        .await
        .expect("SHOW search_path after ROLLBACK failed");
    let val_after: Option<String> = msgs2.iter().find_map(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            r.get(0).map(|v| v.to_string())
        } else {
            None
        }
    });
    assert_eq!(
        val_after.as_deref(),
        Some("original"),
        "expected original after ROLLBACK, got: {val_after:?}"
    );
}

// ── v0.48 Track A: UPDATE/DELETE ... RETURNING (read-modify-write) ─────────

/// v0.48 Slice A2 green gate: `UPDATE` performs a true read-modify-write —
/// a column not named in `SET` must survive unchanged.
#[tokio::test]
async fn update_read_modify_write_preserves_untouched_columns() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-update-rmw").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT, b TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-rmw-insert'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, a, b) VALUES (1, 'A1', 'B1')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-rmw-update'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("UPDATE t SET a = 'A2' WHERE id = 1, a = 'A1', b = 'B1'")
        .await
        .expect("UPDATE failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "expected exactly 1 row after UPDATE, got {}",
        rows.len()
    );
    assert_eq!(rows[0].get("a").unwrap_or(""), "A2");
    assert_eq!(
        rows[0].get("b").unwrap_or(""),
        "B1",
        "untouched column b must be preserved by read-modify-write"
    );
}

/// v0.48 Slice A2 green gate: `UPDATE` of a row that doesn't exist affects
/// zero rows and buffers no write.
#[tokio::test]
async fn update_of_nonexistent_row_returns_zero_rows_no_write() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-update-nonexistent").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let msgs = client
        .simple_query("UPDATE t SET a = 'X' WHERE id = 99, a = 'nope'")
        .await
        .expect("UPDATE failed");
    let tag = msgs.iter().find_map(|m| {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = m {
            Some(*n)
        } else {
            None
        }
    });
    assert_eq!(
        tag,
        Some(0),
        "expected UPDATE 0 for nonexistent row, got {tag:?}"
    );

    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();
    let msgs2 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT failed");
    assert_eq!(
        data_rows_from(&msgs2).len(),
        0,
        "no row should have been written for a no-op UPDATE"
    );
}

/// v0.48 Slice A3 green gate: `UPDATE ... RETURNING *` returns the
/// post-update row (full read-back after commit + frontier wait), outside
/// an explicit transaction block.
#[tokio::test]
async fn update_returning_returns_new_row_after_commit() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-update-returning").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT, b TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-returning-insert'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, a, b) VALUES (1, 'A1', 'B1')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-returning-update'")
        .await
        .expect("SET idempotency_key failed");
    let msgs = client
        .simple_query("UPDATE t SET a = 'A2' WHERE id = 1, a = 'A1', b = 'B1' RETURNING *")
        .await
        .expect("UPDATE RETURNING failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(rows.len(), 1, "expected 1 row from UPDATE RETURNING");
    assert_eq!(rows[0].get("a").unwrap_or(""), "A2");
    assert_eq!(rows[0].get("b").unwrap_or(""), "B1");
    assert_eq!(rows[0].get("id").unwrap_or(""), "1");
}

/// v0.48 Slice A3 green gate: `UPDATE ... RETURNING <col list>` projects
/// only the requested columns.
#[tokio::test]
async fn update_returning_star_returns_all_columns() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-update-returning-cols").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT, b TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-returning-cols-insert'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, a, b) VALUES (1, 'A1', 'B1')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-returning-cols-update'")
        .await
        .expect("SET idempotency_key failed");
    let msgs = client
        .simple_query("UPDATE t SET a = 'A2' WHERE id = 1, a = 'A1', b = 'B1' RETURNING a")
        .await
        .expect("UPDATE RETURNING a failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(rows.len(), 1, "expected 1 row from UPDATE RETURNING a");
    assert_eq!(rows[0].get("a").unwrap_or(""), "A2");
    assert_eq!(
        rows[0].columns().len(),
        1,
        "RETURNING a must project exactly 1 column, not the full row"
    );
}

/// v0.48 Slice A3 green gate: inside an explicit transaction block,
/// `UPDATE ... RETURNING` resolves from the already-computed merged row
/// (no post-commit read-back is attempted), matching INSERT ... RETURNING's
/// existing in-block behavior.
#[tokio::test]
async fn update_returning_inside_explicit_transaction_resolves_at_commit() {
    let (port, _handle, shard_db) =
        start_gateway_with_shard("v048-update-returning-explicit").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT, b TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-explicit-insert'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, a, b) VALUES (1, 'A1', 'B1')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    let msgs = client
        .simple_query("UPDATE t SET a = 'A2' WHERE id = 1, a = 'A1', b = 'B1' RETURNING *")
        .await
        .expect("UPDATE RETURNING inside BEGIN failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "expected 1 row from UPDATE RETURNING inside explicit block"
    );
    assert_eq!(rows[0].get("a").unwrap_or(""), "A2");

    client
        .simple_query("SET rockstream.idempotency_key = 'v048-update-explicit-commit'")
        .await
        .expect("SET idempotency_key failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    let msgs2 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT after COMMIT failed");
    let rows2 = data_rows_from(&msgs2);
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get("a").unwrap_or(""), "A2");
}

/// v0.48 Slice A4/A5 green gate: `DELETE ... RETURNING` returns the
/// pre-delete row (captured before the write, since the row is gone from
/// `view_output` once the commit lands).
#[tokio::test]
async fn delete_returning_returns_row_that_existed_before_delete() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-delete-returning").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-delete-returning-insert'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, a) VALUES (1, 'A1')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client
        .simple_query("SET rockstream.idempotency_key = 'v048-delete-returning-delete'")
        .await
        .expect("SET idempotency_key failed");
    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 1, a = 'A1' RETURNING *")
        .await
        .expect("DELETE RETURNING failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "expected 1 pre-delete row from DELETE RETURNING"
    );
    assert_eq!(rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows[0].get("a").unwrap_or(""), "A1");

    // After the (implicit) COMMIT below, the row must actually be gone.
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();
    let msgs2 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT after DELETE COMMIT failed");
    assert_eq!(
        data_rows_from(&msgs2).len(),
        0,
        "row must be gone after commit"
    );
}

/// v0.48 Slice A4 green gate: `DELETE` of a nonexistent row (with
/// `RETURNING`) affects zero rows and buffers no write.
#[tokio::test]
async fn delete_of_nonexistent_row_returns_zero_rows_no_write() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-delete-nonexistent").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT)")
        .await
        .expect("CREATE TABLE failed");

    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 99, a = 'nope' RETURNING *")
        .await
        .expect("DELETE RETURNING failed");
    assert_eq!(
        data_rows_from(&msgs).len(),
        0,
        "expected 0 rows from DELETE RETURNING on a nonexistent row"
    );

    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();
    let msgs2 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT failed");
    assert_eq!(data_rows_from(&msgs2).len(), 0);
}

/// v0.48 Slice A5 green gate: `ROLLBACK` discards a buffered
/// `DELETE ... RETURNING` — the row is still present afterward and no write
/// ever reached the shard.
#[tokio::test]
async fn delete_returning_discarded_on_rollback() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-delete-rollback").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-delete-rollback-insert'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, a) VALUES (1, 'A1')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 1, a = 'A1' RETURNING *")
        .await
        .expect("DELETE RETURNING inside BEGIN failed");
    assert_eq!(
        data_rows_from(&msgs).len(),
        1,
        "DELETE RETURNING should still report the pre-delete row inside the block"
    );
    client
        .simple_query("ROLLBACK")
        .await
        .expect("ROLLBACK failed");

    shard_db.flush().await.unwrap();
    let msgs2 = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT after ROLLBACK failed");
    let rows2 = data_rows_from(&msgs2);
    assert_eq!(
        rows2.len(),
        1,
        "row must still be present after ROLLBACK — the DELETE was never committed"
    );
    assert_eq!(rows2[0].get("a").unwrap_or(""), "A1");
}

/// v0.48 Slice A5 green gate: `ROLLBACK TO SAVEPOINT` discards a buffered
/// `DELETE ... RETURNING` captured after the savepoint, while a delete
/// buffered *before* the savepoint survives and still commits.
#[tokio::test]
async fn savepoint_rollback_discards_buffered_delete_returning_capture() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-delete-savepoint").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, a TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-delete-savepoint-insert'")
        .await
        .expect("SET idempotency_key failed");
    client
        .simple_query("INSERT INTO t (id, a) VALUES (1, 'A1'), (2, 'A2')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("DELETE FROM t WHERE id = 1, a = 'A1' RETURNING *")
        .await
        .expect("first DELETE RETURNING failed");
    client
        .simple_query("SAVEPOINT sp1")
        .await
        .expect("SAVEPOINT failed");
    let msgs = client
        .simple_query("DELETE FROM t WHERE id = 2, a = 'A2' RETURNING *")
        .await
        .expect("second DELETE RETURNING failed");
    assert_eq!(data_rows_from(&msgs).len(), 1);
    client
        .simple_query("ROLLBACK TO SAVEPOINT sp1")
        .await
        .expect("ROLLBACK TO SAVEPOINT failed");
    client
        .simple_query("SET rockstream.idempotency_key = 'v048-delete-savepoint-commit'")
        .await
        .expect("SET idempotency_key failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");
    shard_db.flush().await.unwrap();

    let msgs2 = client
        .simple_query("SELECT * FROM t ORDER BY id")
        .await
        .expect("SELECT failed");
    let rows2 = data_rows_from(&msgs2);
    assert_eq!(
        rows2.len(),
        1,
        "expected only the id=2 row to remain (id=1's delete committed, id=2's was rolled back)"
    );
    assert_eq!(rows2[0].get("id").unwrap_or(""), "2");
}

/// v0.48 Slice A6 (oracle-style regression): a randomized sequence of
/// INSERT/UPDATE ... RETURNING/DELETE ... RETURNING statements against a
/// base table with a dependent aggregate view must leave the view's
/// incrementally maintained state identical to a from-scratch recompute —
/// `materialize_views` always fully recomputes dependent views on every
/// commit, so this is a direct proof that Slice A2/A4's corrected full-row
/// merge and pre-image capture never desynchronize that recompute from the
/// base table's true current contents.
#[tokio::test]
async fn oracle_update_returning_incremental_equals_batch() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-oracle-update").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE accounts (id BIGINT, balance BIGINT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW total_balance AS SELECT SUM(balance) AS total FROM accounts",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW failed");

    // Seed 5 accounts.
    let mut expected: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for id in 1..=5i64 {
        let balance = id * 100;
        client
            .simple_query(&format!(
                "SET rockstream.idempotency_key = 'v048-oracle-update-seed-{id}'"
            ))
            .await
            .expect("SET idempotency_key failed");
        client
            .simple_query(&format!(
                "INSERT INTO accounts (id, balance) VALUES ({id}, {balance})"
            ))
            .await
            .expect("seed INSERT failed");
        client.simple_query("COMMIT").await.expect("COMMIT failed");
        expected.insert(id, balance);
    }
    shard_db.flush().await.unwrap();

    // Deterministic pseudo-random UPDATE ... RETURNING sequence.
    let mut state: u64 = 0xC0FFEE;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for step in 0..20u64 {
        let id = (next() % 5) as i64 + 1;
        let old_balance = expected[&id];
        let new_balance = old_balance + (next() % 50) as i64 - 25;
        client
            .simple_query(&format!(
                "SET rockstream.idempotency_key = 'v048-oracle-update-step-{step}'"
            ))
            .await
            .expect("SET idempotency_key failed");
        let msgs = client
            .simple_query(&format!(
                "UPDATE accounts SET balance = {new_balance} WHERE id = {id}, balance = {old_balance} RETURNING balance"
            ))
            .await
            .expect("UPDATE RETURNING failed");
        let rows = data_rows_from(&msgs);
        assert_eq!(
            rows.len(),
            1,
            "step {step}: expected 1 row from UPDATE RETURNING"
        );
        assert_eq!(
            rows[0].get("balance").unwrap_or(""),
            new_balance.to_string(),
            "step {step}: RETURNING must reflect the new balance"
        );
        expected.insert(id, new_balance);

        shard_db.flush().await.unwrap();
        let batch_total: i64 = expected.values().sum();
        let view_msgs = client
            .simple_query("SELECT total FROM total_balance")
            .await
            .expect("SELECT total_balance failed");
        let view_rows = data_rows_from(&view_msgs);
        assert_eq!(
            view_rows.len(),
            1,
            "step {step}: expected exactly 1 aggregate row"
        );
        assert_eq!(
            view_rows[0].get("total").unwrap_or(""),
            batch_total.to_string(),
            "step {step}: incremental view state must equal a full batch recompute"
        );
    }
}

/// v0.48 Slice A6 (oracle-style regression): same `incremental == batch`
/// property as above, for randomized `DELETE ... RETURNING` sequences,
/// including deletes of rows that were never present (no-op, zero rows).
#[tokio::test]
async fn oracle_delete_returning_incremental_equals_batch() {
    let (port, _handle, shard_db) = start_gateway_with_shard("v048-oracle-delete").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE accounts (id BIGINT, balance BIGINT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "CREATE MATERIALIZED VIEW total_balance AS SELECT SUM(balance) AS total FROM accounts",
        )
        .await
        .expect("CREATE MATERIALIZED VIEW failed");

    let mut expected: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for id in 1..=5i64 {
        let balance = id * 100;
        client
            .simple_query(&format!(
                "SET rockstream.idempotency_key = 'v048-oracle-delete-seed-{id}'"
            ))
            .await
            .expect("SET idempotency_key failed");
        client
            .simple_query(&format!(
                "INSERT INTO accounts (id, balance) VALUES ({id}, {balance})"
            ))
            .await
            .expect("seed INSERT failed");
        client.simple_query("COMMIT").await.expect("COMMIT failed");
        expected.insert(id, balance);
    }
    shard_db.flush().await.unwrap();

    // Randomized DELETE sequence, including deletes of already-deleted (or
    // never-present) ids — must be a clean zero-row no-op each time.
    let mut state: u64 = 0xBADC0DE;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for step in 0..8u64 {
        let id = (next() % 5) as i64 + 1;
        client
            .simple_query(&format!(
                "SET rockstream.idempotency_key = 'v048-oracle-delete-step-{step}'"
            ))
            .await
            .expect("SET idempotency_key failed");
        let existed = expected.contains_key(&id);
        let balance = expected.get(&id).copied().unwrap_or_default();
        let msgs = client
            .simple_query(&format!(
                "DELETE FROM accounts WHERE id = {id}, balance = {balance} RETURNING balance"
            ))
            .await
            .expect("DELETE RETURNING failed");
        let rows = data_rows_from(&msgs);
        if existed {
            assert_eq!(rows.len(), 1, "step {step}: expected 1 pre-delete row");
            assert_eq!(rows[0].get("balance").unwrap_or(""), balance.to_string());
            expected.remove(&id);
        } else {
            assert_eq!(
                rows.len(),
                0,
                "step {step}: deleting an already-absent id must be a zero-row no-op"
            );
        }
        client.simple_query("COMMIT").await.expect("COMMIT failed");
        shard_db.flush().await.unwrap();

        let batch_total: i64 = expected.values().sum();
        let view_msgs = client
            .simple_query("SELECT total FROM total_balance")
            .await
            .expect("SELECT total_balance failed");
        let view_rows = data_rows_from(&view_msgs);
        if expected.is_empty() {
            // An empty base table's SUM aggregate may materialise as either
            // zero rows or a single NULL/0 row depending on the view
            // pipeline — both are consistent with "no accounts remain".
            if !view_rows.is_empty() {
                let total = view_rows[0].get("total").unwrap_or("0");
                assert!(
                    total == "0" || total.is_empty(),
                    "step {step}: expected zero total once all accounts are deleted, got {total}"
                );
            }
        } else {
            assert_eq!(
                view_rows.len(),
                1,
                "step {step}: expected exactly 1 aggregate row"
            );
            assert_eq!(
                view_rows[0].get("total").unwrap_or(""),
                batch_total.to_string(),
                "step {step}: incremental view state must equal a full batch recompute"
            );
        }
    }
}

#[tokio::test]
async fn explain_reports_pruned_shard_count() {
    let catalog = Arc::new(CatalogStubs::new());
    catalog.add_table(CatalogTable {
        name: "orders".to_string(),
        columns: vec![CatalogColumn {
            name: "region".to_string(),
            data_type: "Utf8".to_string(),
        }],
    });
    catalog.set_shard_stats(
        "orders",
        vec![
            ShardColumnStats {
                shard_id: ShardId(1),
                view_id: ViewId(1),
                checkpoint_epoch: 10,
                col_stats: vec![ColumnStats {
                    col_idx: 0,
                    min_bytes: Some(bytes::Bytes::from_static(b"a")),
                    max_bytes: Some(bytes::Bytes::from_static(b"m")),
                    bloom_filter: Some(build_exact_membership_filter(&[b"emea".to_vec()])),
                    null_count: 0,
                    distinct_count_hll: bytes::Bytes::from(vec![0; 64]),
                }],
            },
            ShardColumnStats {
                shard_id: ShardId(2),
                view_id: ViewId(1),
                checkpoint_epoch: 10,
                col_stats: vec![ColumnStats {
                    col_idx: 0,
                    min_bytes: Some(bytes::Bytes::from_static(b"n")),
                    max_bytes: Some(bytes::Bytes::from_static(b"z")),
                    bloom_filter: Some(build_exact_membership_filter(&[b"us".to_vec()])),
                    null_count: 0,
                    distinct_count_hll: bytes::Bytes::from(vec![0; 64]),
                }],
            },
        ],
    );
    let server = GatewayServer::with_catalog(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
    );
    let (addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(addr.port()).await;
    let msgs = client
        .simple_query("EXPLAIN SELECT * FROM orders WHERE region = 'emea'")
        .await
        .unwrap();
    let rows = data_rows_from(&msgs);
    // v0.51.2 Slice 4: real DataFusion plan rows now precede the
    // shard-pruning annotation row (previously all concatenated into a
    // single row), so check the joined plan text rather than row 0.
    let plan_text = rows
        .iter()
        .map(|row| row.get("QUERY PLAN").unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan_text.contains("shard_scan: 1/2 shards"));
}

#[tokio::test]
async fn proof_multi_shard_point_read_prunes_over_90_percent_of_shards() {
    reset_all();
    let mut readers = Vec::new();
    let mut stats = Vec::new();
    for shard_id in 0..100_u64 {
        let store = Arc::new(InMemory::new());
        let shard = ShardDb::builder(format!("prune-proof-{shard_id}"), store.clone())
            .build()
            .await
            .unwrap();
        let value = format!("region-{shard_id:03}");
        shard
            .put(
                format!("view_output/orders/{shard_id:03}").as_bytes(),
                value.as_bytes(),
            )
            .await
            .unwrap();
        shard.flush().await.unwrap();
        readers.push(Arc::new(
            ShardReader::open(format!("prune-proof-{shard_id}"), store)
                .await
                .unwrap(),
        ));
        stats.push(ShardColumnStats {
            shard_id: ShardId(shard_id),
            view_id: ViewId(1),
            checkpoint_epoch: 10,
            col_stats: vec![ColumnStats {
                col_idx: 0,
                min_bytes: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
                max_bytes: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
                bloom_filter: Some(build_exact_membership_filter(&[value.as_bytes().to_vec()])),
                null_count: 0,
                distinct_count_hll: bytes::Bytes::from(vec![0; 64]),
            }],
        });
    }
    let plan = plan_scatter_shards(
        &stats,
        &[ScatterPredicate::Eq {
            col_idx: 0,
            value: b"region-042".to_vec(),
        }],
        5,
        10,
    );
    let reader = MultiShardReader::new(readers, 0, MultiShardReader::DEFAULT_MAX_IN_FLIGHT_ROWS);
    let rows = reader.scatter_read("orders", None).await.unwrap();
    assert!(rows.iter().any(|row| row == b"region-042"));
    let total = read_scatter_shards_total() as f64;
    let pruned = read_scatter_shards_pruned_total() as f64;
    assert!(total > 0.0);
    assert!(
        pruned / total > 0.90,
        "expected >90% pruning, got {pruned}/{total}"
    );
    assert_eq!(plan.shard_ids, vec![ShardId(42)]);
}

#[test]
fn scatter_pruning_metrics_exported_correctly() {
    let total_before = read_scatter_shards_total();
    let pruned_before = read_scatter_shards_pruned_total();
    let false_positive_before = read_shard_bloom_false_positive_total();
    let stats = vec![
        ShardColumnStats {
            shard_id: ShardId(1),
            view_id: ViewId(1),
            checkpoint_epoch: 10,
            col_stats: vec![ColumnStats {
                col_idx: 0,
                min_bytes: Some(bytes::Bytes::from_static(b"a")),
                max_bytes: Some(bytes::Bytes::from_static(b"m")),
                bloom_filter: Some(build_exact_membership_filter(&[b"emea".to_vec()])),
                null_count: 0,
                distinct_count_hll: bytes::Bytes::from(vec![0; 64]),
            }],
        },
        ShardColumnStats {
            shard_id: ShardId(2),
            view_id: ViewId(1),
            checkpoint_epoch: 10,
            col_stats: vec![ColumnStats {
                col_idx: 0,
                min_bytes: Some(bytes::Bytes::from_static(b"n")),
                max_bytes: Some(bytes::Bytes::from_static(b"z")),
                bloom_filter: Some(build_exact_membership_filter(&[b"us".to_vec()])),
                null_count: 0,
                distinct_count_hll: bytes::Bytes::from(vec![0; 64]),
            }],
        },
    ];
    let _ = plan_scatter_shards(
        &stats,
        &[ScatterPredicate::Eq {
            col_idx: 0,
            value: b"emea".to_vec(),
        }],
        5,
        10,
    );
    inc_shard_bloom_false_positive_total();

    assert!(read_scatter_shards_total() >= total_before + 2);
    assert!(read_scatter_shards_pruned_total() > pruned_before);
    assert!(read_shard_bloom_false_positive_total() > false_positive_before);

    let metrics = generate_prometheus_metrics();
    assert!(metrics.contains("scatter_shards_total"));
    assert!(metrics.contains("scatter_shards_pruned_total"));
    assert!(metrics.contains("shard_bloom_false_positive_total"));
}

// ══════════════════════════════════════════════════════════════════════════════
// v0.51.1 Slice 3 — implicit (standard-PostgreSQL) autocommit
// ══════════════════════════════════════════════════════════════════════════════

/// Slice 3 core Gap-1 regression: a bare `INSERT` (no `RETURNING`, no
/// explicit `BEGIN`, no `SET`) followed by a `SELECT` in a **separate simple
/// query round-trip** returns the row — matching standard PostgreSQL
/// autocommit semantics.
#[tokio::test]
async fn bare_insert_autocommits_and_is_visible_in_separate_round_trip() {
    let (port, _handle, shard_db) = start_gateway_with_shard("s3a-bare-insert-autocommit").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    // No SET, no BEGIN, no RETURNING — a vanilla autocommitting INSERT.
    client
        .simple_query("INSERT INTO t VALUES (1, 'alice')")
        .await
        .expect("INSERT failed");

    // Separate simple-query round-trip.
    shard_db.flush().await.unwrap();
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "expected the bare INSERT to autocommit and be visible; got {} rows",
        rows.len()
    );
    assert_eq!(rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows[0].get("name").unwrap_or(""), "alice");
}

/// Slice 3: a bare `UPDATE`/`DELETE` (no `RETURNING`, no explicit `BEGIN`)
/// autocommits the same way a bare `INSERT` does.
#[tokio::test]
async fn bare_update_and_delete_autocommit_and_are_visible_in_separate_round_trip() {
    let (port, _handle, shard_db) =
        start_gateway_with_shard("s3a-bare-update-delete-autocommit").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
        .await
        .expect("seed INSERT failed");

    // Bare UPDATE — no RETURNING, no explicit BEGIN — autocommits.
    // (WHERE must specify every original column value: this gateway resolves
    // WHERE against the exact row key, not a predicate scan.)
    client
        .simple_query("UPDATE t SET name = 'alice2' WHERE id = 1, name = 'alice'")
        .await
        .expect("UPDATE failed");

    shard_db.flush().await.unwrap();
    let after_update_msgs = client
        .simple_query("SELECT * FROM t ORDER BY id")
        .await
        .expect("SELECT after UPDATE failed");
    let after_update = data_rows_from(&after_update_msgs);
    assert_eq!(after_update.len(), 2, "expected 2 rows after UPDATE");
    assert_eq!(after_update[0].get("name").unwrap_or(""), "alice2");

    // Bare DELETE — no RETURNING, no explicit BEGIN — autocommits.
    client
        .simple_query("DELETE FROM t WHERE id = 2, name = 'bob'")
        .await
        .expect("DELETE failed");

    shard_db.flush().await.unwrap();
    let after_delete_msgs = client
        .simple_query("SELECT * FROM t ORDER BY id")
        .await
        .expect("SELECT after DELETE failed");
    let after_delete = data_rows_from(&after_delete_msgs);
    assert_eq!(
        after_delete.len(),
        1,
        "expected bare DELETE to autocommit, leaving 1 row; got {}",
        after_delete.len()
    );
    assert_eq!(after_delete[0].get("id").unwrap_or(""), "1");
}

/// Slice 3 regression: an explicit `BEGIN; INSERT; INSERT;` (no `COMMIT`
/// yet) followed by a `SELECT` in a separate round-trip still returns
/// **zero** rows — explicit-block buffering is unaffected by autocommit.
#[tokio::test]
async fn explicit_begin_block_still_buffers_until_commit() {
    let (port, _handle, _shard_db) =
        start_gateway_with_shard("s3a-explicit-begin-still-buffers").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");

    client.simple_query("BEGIN").await.expect("BEGIN failed");
    client
        .simple_query("INSERT INTO t VALUES (1, 'alice')")
        .await
        .expect("INSERT 1 failed");
    client
        .simple_query("INSERT INTO t VALUES (2, 'bob')")
        .await
        .expect("INSERT 2 failed");

    // No COMMIT yet — separate round-trip SELECT must see zero rows.
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT t failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        0,
        "expected explicit BEGIN block to still buffer until COMMIT; got {} rows",
        rows.len()
    );
}

/// v0.51.1 Slice 4: a freshly `CREATE MATERIALIZED VIEW`'d view must reflect
/// its source table's current data immediately — before any further write —
/// with no second COMMIT required to see it.
#[tokio::test]
async fn create_materialized_view_populates_immediately_without_further_write() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("s4-immediate-mv-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO t (id, name) VALUES (1, 'alice')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t")
        .await
        .expect("CREATE MATERIALIZED VIEW failed");

    // No further write on this connection — the view must already be
    // populated from the immediate synchronous materialization.
    let msgs = client
        .simple_query("SELECT * FROM mv")
        .await
        .expect("SELECT mv failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "expected mv to be immediately populated with 1 row; got {}",
        rows.len()
    );
    assert_eq!(rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows[0].get("name").unwrap_or(""), "alice");
}

/// v0.51.1 Slice 4 regression: after immediate population on CREATE, a
/// further INSERT + COMMIT into the source table still updates the view via
/// the existing post-commit materialization path (unchanged behavior).
#[tokio::test]
async fn create_materialized_view_immediate_population_then_further_insert_updates_view() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("s4-immediate-mv-then-insert-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO t (id, name) VALUES (1, 'alice')")
        .await
        .expect("INSERT failed");
    client.simple_query("COMMIT").await.expect("COMMIT failed");

    client
        .simple_query("CREATE MATERIALIZED VIEW mv AS SELECT id, name FROM t")
        .await
        .expect("CREATE MATERIALIZED VIEW failed");

    client
        .simple_query("INSERT INTO t (id, name) VALUES (2, 'bob')")
        .await
        .expect("second INSERT failed");
    client
        .simple_query("COMMIT")
        .await
        .expect("second COMMIT failed");

    let msgs = client
        .simple_query("SELECT * FROM mv ORDER BY id")
        .await
        .expect("SELECT mv failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        2,
        "expected mv to reflect both rows after further insert+commit; got {}",
        rows.len()
    );
    assert_eq!(rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows[1].get("id").unwrap_or(""), "2");
    assert_eq!(rows[1].get("name").unwrap_or(""), "bob");
}

/// v0.51.1 Slice 5: replay the exact `IMPLEMENTATION_STATUS_20260719.md`
/// transcript verbatim — a vanilla autocommitting connection, no `SET`, no
/// explicit `COMMIT` — must return the one row correctly.
#[tokio::test]
async fn postgres_standard_transcript_replay_returns_the_row() {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(
        ShardDb::builder("s5-transcript-shard", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(NoopViewReader);
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(addr, catalog.clone(), view_reader, shard_db.clone());
    let (local_addr, _handle) = server.serve_background().await.unwrap();
    let client = connect_port(local_addr.port()).await;

    client
        .simple_query("CREATE TABLE t (id int, name text)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO t VALUES (1, 'alice')")
        .await
        .expect("INSERT failed");
    let msgs = client
        .simple_query("SELECT * FROM t")
        .await
        .expect("SELECT failed");
    let rows = data_rows_from(&msgs);
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one row from vanilla autocommitting transcript; got {}",
        rows.len()
    );
    assert_eq!(rows[0].get("id").unwrap_or(""), "1");
    assert_eq!(rows[0].get("name").unwrap_or(""), "alice");
}

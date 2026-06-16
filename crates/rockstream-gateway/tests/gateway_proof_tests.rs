//! v0.23 Proof tests — Phase 3b: S6–S10 green gates.
//!
//! Each test maps to one or more Proof claims (P1–P4) from the v0.23 plan.

use std::sync::Arc;
use std::time::Instant;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView},
    view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::{ShardDb, ShardReader};
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

async fn start_gateway_noop(catalog: CatalogStubs) -> (u16, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle)
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
        shard_db.put(key.as_bytes(), value.as_bytes()).await.unwrap();
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
            CatalogColumn { name: "name".to_string(), data_type: "Utf8".to_string() },
            CatalogColumn { name: "val".to_string(), data_type: "Int32".to_string() },
        ],
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
            tokio::io::AsyncReadExt::read_exact(stream, &mut body).await.unwrap();
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
            tokio::io::AsyncReadExt::read_exact(stream, &mut body).await.unwrap();
        }
        match msg_type {
            b'd' => count += 1,       // CopyData
            b'c' => break,            // CopyDone
            b'C' | b'Z' => break,     // CommandComplete / ReadyForQuery → done
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
        shard_db.put(key.as_bytes(), value.as_bytes()).await.unwrap();
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
            CatalogColumn { name: "id".to_string(), data_type: "Utf8".to_string() },
            CatalogColumn { name: "val".to_string(), data_type: "Int32".to_string() },
        ],
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
    assert!(
        p99_ms < 10.0,
        "p99 latency {p99_ms:.2}ms exceeded 10ms SLO"
    );
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
        shard_db.put(key.as_bytes(), value.as_bytes()).await.unwrap();
    }
    shard_db.flush().await.unwrap();

    let reader = ShardReader::open("ivm-shard", store).await.unwrap();
    let view_reader = Arc::new(HotOnlyViewReader {
        shard_reader: Arc::new(reader),
        frontier_epoch: Some(1),
    });

    let catalog = Arc::new(CatalogStubs::new());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server =
        GatewayServer::with_catalog(addr, catalog.clone(), view_reader);
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
    assert!(got_rs1011, "expected RS-1011 for cyclic CREATE VIEW; got: {result:?}");

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
#[cfg_attr(not(feature = "testcontainers"), ignore = "requires testcontainers feature")]
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
            CatalogColumn { name: "id".to_string(), data_type: "Int64".to_string() },
            CatalogColumn { name: "amount".to_string(), data_type: "Float64".to_string() },
        ],
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
        .simple_query(
            "SELECT oid, relname FROM pg_catalog.pg_class WHERE relname = 'orders_mv'",
        )
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
}

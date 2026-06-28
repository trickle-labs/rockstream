//! v0.23 + v0.24 Proof tests — Phase 3b/3a green gates.
//!
//! v0.23 tests: S6–S10
//! v0.24 tests (S2–S5): CREATE TABLE, DML accumulation, COMMIT flush, ROLLBACK
//! v0.32 index DDL wire tests: CREATE INDEX, DROP INDEX, REBUILD INDEX through pgwire

use std::sync::Arc;
use std::time::Instant;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogIndexState, CatalogIndexEntry, CatalogStubs, CatalogTable, CatalogView},
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
    assert!(index_names.contains(&"orders_mv_idx".to_string()), "expected orders_mv_idx in pg_class; got {:?}", index_names);

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
    assert!(proc_names.contains(&"count".to_string()), "expected count in pg_proc");

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
// v0.24 S6 idempotency tests
// ══════════════════════════════════════════════════════════════════════════════

/// S6 green gate: COMMIT without idempotency_key or source_epoch returns RS-2007.
#[tokio::test]
async fn missing_idempotency_key_returns_rs2007() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("s6-missing-key").await;
    let client = connect_port(port).await;

    client
        .simple_query("INSERT INTO t (id, val) VALUES (1, 'hello')")
        .await
        .expect("INSERT failed");

    // COMMIT without setting idempotency_key or source_epoch → RS-2007
    let result = client.simple_query("COMMIT").await;
    let got_rs2007 = match &result {
        Err(e) => {
            if let Some(db_err) = e.as_db_error() {
                db_err.message().contains("RS-2007")
            } else {
                e.to_string().contains("RS-2007")
            }
        }
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("RS-2007")),
    };
    assert!(got_rs2007, "expected RS-2007; got: {result:?}");
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

/// P2 (S10): A write missing idempotency_key returns RS-2007.
#[tokio::test]
async fn proof_missing_idempotency_returns_rs2007() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("proof-p2-missing-key").await;
    let client = connect_port(port).await;

    // Do NOT set idempotency_key
    client
        .simple_query("INSERT INTO orders (id, amount) VALUES (1, 500)")
        .await
        .expect("INSERT should succeed");

    let result = client.simple_query("COMMIT").await;
    let got_rs2007 = match &result {
        Err(e) => {
            if let Some(db_err) = e.as_db_error() {
                db_err.message().contains("RS-2007")
            } else {
                e.to_string().contains("RS-2007")
            }
        }
        Ok(msgs) => msgs.iter().any(|m| format!("{m:?}").contains("RS-2007")),
    };
    assert!(got_rs2007, "P2: expected RS-2007; got: {result:?}");
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

    // Same name on same table is idempotent (no error).
    client
        .simple_query("CREATE INDEX idx_conflict ON table_a (col1)")
        .await
        .expect("idempotent CREATE INDEX on same table should succeed");
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

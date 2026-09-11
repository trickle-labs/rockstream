//! Matrix C & Slice 5 tests: Data Type, Literal Encoding, NULL, and Coercion Semantics.

use object_store::memory::InMemory;
use std::sync::Arc;
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
async fn test_int16_dml_and_coercion() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-int16").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v SMALLINT);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t (id, v) VALUES (1, 100);")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t SET v = 250 WHERE id = 1;")
        .await
        .unwrap();

    let r = rows(&client, "SELECT id, v FROM t WHERE id = 1;").await;
    assert_eq!(r, vec![vec!["1".to_string(), "250".to_string()]]);
}

#[tokio::test]
async fn test_int32_dml_and_coercion() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-int32").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v INT);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t (id, v) VALUES (1, 1000);")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t SET v = v + 500 WHERE id = 1;")
        .await
        .unwrap();

    let r = rows(&client, "SELECT id, v FROM t WHERE id = 1;").await;
    assert_eq!(r, vec![vec!["1".to_string(), "1500".to_string()]]);
}

#[tokio::test]
async fn test_int64_dml_and_coercion() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-int64").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t (id, v) VALUES (1, 9223372036854775800);")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t SET v = v + 1 WHERE id = 1;")
        .await
        .unwrap();

    let r = rows(&client, "SELECT id, v FROM t WHERE id = 1;").await;
    assert_eq!(
        r,
        vec![vec!["1".to_string(), "9223372036854775801".to_string()]]
    );
}

#[tokio::test]
async fn test_float64_dml_and_coercion() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-float64").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v DOUBLE PRECISION);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t (id, v) VALUES (1, 2.5);")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t SET v = v * 2.0 WHERE id = 1;")
        .await
        .unwrap();

    let r = rows(&client, "SELECT id, v FROM t WHERE id = 1;").await;
    assert_eq!(r, vec![vec!["1".to_string(), "5".to_string()]]);
}

#[tokio::test]
async fn test_decimal_dml_and_overflow() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-decimal").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v NUMERIC);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t (id, v) VALUES (1, 99.50);")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t SET v = 150.75 WHERE id = 1;")
        .await
        .unwrap();

    let r = rows(&client, "SELECT id, v FROM t WHERE id = 1;").await;
    assert!(
        r[0][1].starts_with("150.75"),
        "expected 150.75..., got: {:?}",
        r[0][1]
    );

    // Division by zero in arithmetic fails with RS-1016
    let err = client
        .simple_query("UPDATE t SET v = 1 / 0 WHERE id = 1;")
        .await
        .expect_err("division by zero must fail");
    let db_err = err.as_db_error().expect("must be DB error");
    let msg = db_err.message();
    let code = db_err.code().code();
    assert!(
        code == "22012" || msg.contains("RS-1016") || msg.contains("division by zero"),
        "expected RS-1016 or 22012, got code {code}: {msg}"
    );
}

#[tokio::test]
async fn test_utf8_escaping_and_nulls() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-utf8").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, v TEXT);")
        .await
        .unwrap();

    // Text with escaped single quotes
    client
        .simple_query("INSERT INTO t (id, v) VALUES (1, 'it''s a string with ''quotes''');")
        .await
        .unwrap();

    let r1 = rows(&client, "SELECT id, v FROM t WHERE id = 1;").await;
    assert_eq!(
        r1,
        vec![vec![
            "1".to_string(),
            "it's a string with 'quotes'".to_string()
        ]]
    );

    // Update to NULL
    client
        .simple_query("UPDATE t SET v = NULL WHERE id = 1;")
        .await
        .unwrap();

    let r2 = rows(&client, "SELECT id, v FROM t WHERE v IS NULL;").await;
    assert_eq!(r2, vec![vec!["1".to_string(), "".to_string()]]);

    // WHERE v IS NOT NULL returns 0 rows
    let r3 = rows(&client, "SELECT id, v FROM t WHERE v IS NOT NULL;").await;
    assert!(r3.is_empty());
}

#[tokio::test]
async fn test_boolean_dml_and_null_logic() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-bool").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, flag BOOLEAN);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t (id, flag) VALUES (1, true), (2, false), (3, NULL);")
        .await
        .unwrap();

    // UPDATE with boolean predicate
    client
        .simple_query("UPDATE t SET flag = true WHERE flag = false;")
        .await
        .unwrap();

    let mut r = rows(&client, "SELECT id, flag FROM t WHERE flag IS NOT NULL;").await;
    r.sort_by_key(|row| row[0].parse::<i64>().unwrap());
    assert_eq!(
        r,
        vec![
            vec!["1".to_string(), "t".to_string()],
            vec!["2".to_string(), "t".to_string()],
        ]
    );

    let r_null = rows(&client, "SELECT id FROM t WHERE flag IS NULL;").await;
    assert_eq!(r_null, vec![vec!["3".to_string()]]);
}

#[tokio::test]
async fn test_timestamp_dml_and_zones() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-timestamp").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP);")
        .await
        .unwrap();

    client
        .simple_query("INSERT INTO t (id, ts) VALUES (1, '2026-09-11 12:00:00');")
        .await
        .unwrap();

    client
        .simple_query("UPDATE t SET ts = '2026-09-11 15:30:00' WHERE id = 1;")
        .await
        .unwrap();

    let r = rows(&client, "SELECT id, ts FROM t WHERE id = 1;").await;
    assert!(
        r[0][1].starts_with("2026-09-11 15:30:00"),
        "expected timestamp starting with 2026-09-11 15:30:00, got: {:?}",
        r[0][1]
    );
}

#[tokio::test]
async fn test_uuid_dml_roundtrip() {
    let (port, _handle, _shard_db, _store) = start_gateway_with_shard("coercion-uuid").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id INT PRIMARY KEY, u UUID);")
        .await
        .unwrap();

    let uuid_str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    client
        .simple_query(&format!("INSERT INTO t (id, u) VALUES (1, '{uuid_str}');"))
        .await
        .unwrap();

    let uuid_updated = "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22";
    client
        .simple_query(&format!("UPDATE t SET u = '{uuid_updated}' WHERE id = 1;"))
        .await
        .unwrap();

    let r = rows(&client, "SELECT id, u FROM t WHERE id = 1;").await;
    assert_eq!(r, vec![vec!["1".to_string(), uuid_updated.to_string()]]);
}

//! Extended query protocol and prepared statement integration tests.
//!
//! These tests spin up a `GatewayServer` on a random port and connect with
//! `tokio-postgres` to exercise SSL downgrade, prepared statements, parameter
//! inference, describe, and limit/bound checks.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};

// ── No-op ViewReader for catalog-only tests ───────────────────────────────────

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

/// Start a GatewayServer with the given catalog on a random port. Returns the
/// address and a background task handle.
async fn start_gateway(catalog: CatalogStubs) -> (String, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(addr, Arc::new(catalog), Arc::new(NoopViewReader));
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.to_string(), handle)
}

/// Connect with tokio-postgres to `host:port`.
async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.split(':').next_back().unwrap()
        ),
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

// ── S1: SSL Negotiation & Parameter Status ────────────────────────────────────

async fn send_frame(stream: &mut TcpStream, tag: u8, body: &[u8]) {
    stream.write_u8(tag).await.unwrap();
    stream.write_i32(body.len() as i32 + 4).await.unwrap();
    stream.write_all(body).await.unwrap();
}

async fn read_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let len = stream.read_i32().await.unwrap();
            let mut body = vec![0; len as usize - 4];
            stream.read_exact(&mut body).await.unwrap();
            messages.push((tag, body));
            if tag == b'Z' {
                return messages;
            }
        }
    })
    .await
    .expect("protocol did not reach ReadyForQuery after Sync")
}

async fn wire_connect(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let body = b"\0\x03\0\0user\0test\0database\0test\0\0";
    stream.write_i32(body.len() as i32 + 4).await.unwrap();
    stream.write_all(body).await.unwrap();
    read_until_ready(&mut stream).await;
    stream
}

async fn wire_query(
    stream: &mut TcpStream,
    sql: &str,
    params: Option<&[Option<&str>]>,
) -> Vec<(u8, Vec<u8>)> {
    if let Some(params) = params {
        let mut parse = vec![0]; // unnamed statement; infer all parameter types
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0, 0, 0]);
        send_frame(stream, b'P', &parse).await;
        let mut bind = vec![0, 0, 0, 0]; // unnamed portal/statement; text parameters
        bind.extend_from_slice(&(params.len() as i16).to_be_bytes());
        for param in params {
            match param {
                Some(value) => {
                    bind.extend_from_slice(&(value.len() as i32).to_be_bytes());
                    bind.extend_from_slice(value.as_bytes());
                }
                None => bind.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        bind.extend_from_slice(&[0, 0]); // text results
        send_frame(stream, b'B', &bind).await;
        send_frame(stream, b'D', b"P\0").await;
        send_frame(stream, b'E', &[0, 0, 0, 0, 0]).await;
        send_frame(stream, b'S', &[]).await;
    } else {
        let mut query = sql.as_bytes().to_vec();
        query.push(0);
        send_frame(stream, b'Q', &query).await;
    }
    read_until_ready(stream).await
}

fn expected_select(columns: &[(&str, u32)], values: &[Option<&str>]) -> Vec<(u8, Vec<u8>)> {
    let mut description = (columns.len() as i16).to_be_bytes().to_vec();
    for (name, oid) in columns {
        description.extend_from_slice(name.as_bytes());
        description.extend_from_slice(&[0; 7]); // terminator, table OID, attribute number
        description.extend_from_slice(&oid.to_be_bytes());
        description.extend_from_slice(&[0; 6]); // pgwire currently emits zero size/typmod
        description.extend_from_slice(&[0, 0]); // text format
    }
    let mut row = (values.len() as i16).to_be_bytes().to_vec();
    for value in values {
        match value {
            Some(value) => {
                row.extend_from_slice(&(value.len() as i32).to_be_bytes());
                row.extend_from_slice(value.as_bytes());
            }
            None => row.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    vec![
        (b'T', description),
        (b'D', row),
        (b'C', b"SELECT 1\0".to_vec()),
        (b'Z', vec![b'I']),
    ]
}

#[tokio::test]
async fn test_prepared_binding_preserves_lexical_context_and_typed_results() {
    let (addr, handle) = start_gateway(CatalogStubs::new()).await;
    let mut stream = wire_connect(&addr).await;
    // Each oracle is independently written SQL plus the complete expected wire result.
    let cases = [
        (
            "SELECT '$1' AS literal, $1 AS value",
            "SELECT '$1' AS literal, 'x' AS value",
            vec![Some("x")],
            vec![("literal", 25), ("value", 25)],
            vec![Some("$1"), Some("x")],
        ),
        (
            "SELECT $1 AS first, $2 AS second",
            "SELECT 'x' AS first, 'literal $1' AS second",
            vec![Some("x"), Some("literal $1")],
            vec![("first", 25), ("second", 25)],
            vec![Some("x"), Some("literal $1")],
        ),
        (
            "SELECT $1 AS value /* $10 /* $20 */ */ -- $30\n",
            "SELECT 'x' AS value",
            vec![Some("x")],
            vec![("value", 25)],
            vec![Some("x")],
        ),
        (
            "SELECT $$literal $10$$ AS literal, $tag$literal $20$tag$ AS tagged, $1 AS value",
            "SELECT 'literal $10' AS literal, 'literal $20' AS tagged, 'x' AS value",
            vec![Some("x")],
            vec![("literal", 25), ("tagged", 25), ("value", 25)],
            vec![Some("literal $10"), Some("literal $20"), Some("x")],
        ),
        (
            "SELECT $1 AS first, $10 AS tenth",
            "SELECT 'one' AS first, 'ten' AS tenth",
            vec![
                Some("one"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some("ten"),
            ],
            vec![("first", 25), ("tenth", 25)],
            vec![Some("one"), Some("ten")],
        ),
        (
            "SELECT $1 AS first, $1 AS again",
            "SELECT 'it''s $1 = ANY(ARRAY[7])' AS first, 'it''s $1 = ANY(ARRAY[7])' AS again",
            vec![Some("it's $1 = ANY(ARRAY[7])")],
            vec![("first", 25), ("again", 25)],
            vec![
                Some("it's $1 = ANY(ARRAY[7])"),
                Some("it's $1 = ANY(ARRAY[7])"),
            ],
        ),
        (
            "SELECT $1 AS value, $2::INTEGER AS number",
            "SELECT CAST(NULL AS TEXT) AS value, CAST(NULL AS INTEGER) AS number",
            vec![None, None],
            vec![("value", 25), ("number", 23)],
            vec![None, None],
        ),
        (
            "SELECT $1::NUMERIC(6,2) AS amount, CAST($2 AS DECIMAL(5,1)) AS rounded",
            "SELECT '12.34'::NUMERIC(6,2) AS amount, CAST('5.6' AS DECIMAL(5,1)) AS rounded",
            vec![Some("12.34"), Some("5.6")],
            vec![("amount", 1700), ("rounded", 1700)],
            vec![Some("12.34"), Some("5.6")],
        ),
        (
            "SELECT $1::INTEGER AS number",
            "SELECT '1234'::INTEGER AS number",
            vec![Some("1234")],
            vec![("number", 23)],
            vec![Some("1234")],
        ),
        (
            "SELECT E'it\\'s $10' AS \"$20\",\n $1 AS value",
            "SELECT 'it''s $10' AS \"$20\", CAST('x' AS TEXT) AS value",
            vec![Some("x")],
            vec![("$20", 25), ("value", 25)],
            vec![Some("it's $10"), Some("x")],
        ),
        (
            "SELECT $1 AS value",
            "SELECT '{literal $1, x}' AS value",
            vec![Some("{literal $1, x}")],
            vec![("value", 25)],
            vec![Some("{literal $1, x}")],
        ),
        (
            "SELECT '$1' AS literal",
            "SELECT '$1' AS literal",
            vec![],
            vec![("literal", 25)],
            vec![Some("$1")],
        ),
    ];
    for (prepared, literal, params, columns, values) in cases {
        let expected = expected_select(&columns, &values);
        assert_eq!(
            wire_query(&mut stream, literal, None).await,
            expected,
            "simple: {literal}"
        );
        let mut extended_expected = vec![(b'1', vec![]), (b'2', vec![])];
        extended_expected.extend(expected);
        assert_eq!(
            wire_query(&mut stream, prepared, Some(&params)).await,
            extended_expected,
            "extended: {prepared}"
        );
    }
    handle.abort();
}

#[tokio::test]
async fn test_prepared_binding_binary_values_and_statement_metadata() {
    use tokio_postgres::types::Type;
    let (addr, handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;
    let statement = client
        .prepare("SELECT '$10' AS literal, $1 AS value, $2::INTEGER AS number")
        .await
        .unwrap();
    assert_eq!(statement.params(), &[Type::TEXT, Type::INT4]);
    assert_eq!(
        statement
            .columns()
            .iter()
            .map(|c| (c.name(), c.type_().clone()))
            .collect::<Vec<_>>(),
        vec![
            ("literal", Type::TEXT),
            ("value", Type::TEXT),
            ("number", Type::INT4)
        ]
    );
    let rows = client
        .query(&statement, &[&"literal $1", &1234i32])
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| (
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, i32>(2)
            ))
            .collect::<Vec<_>>(),
        vec![("$10".to_owned(), "literal $1".to_owned(), 1234)]
    );
    handle.abort();
}

fn wire_error(messages: &[(u8, Vec<u8>)]) -> (String, String, String) {
    let body = &messages
        .iter()
        .find(|(tag, _)| *tag == b'E')
        .expect("expected ErrorResponse")
        .1;
    let fields = body
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .map(|field| (field[0], std::str::from_utf8(&field[1..]).unwrap()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(messages.last(), Some(&(b'Z', vec![b'I'])));
    (
        fields[&b'S'].to_owned(),
        fields[&b'C'].to_owned(),
        fields[&b'M'].to_owned(),
    )
}

#[tokio::test]
async fn test_prepared_binding_errors_and_sync_recovery() {
    let (addr, handle) = start_gateway(CatalogStubs::new()).await;
    let mut stream = wire_connect(&addr).await;
    for (sql, params, code, message) in [
        (
            "SELECT $0 AS value",
            vec![],
            "42P02",
            "invalid parameter $0 (RS-2056)",
        ),
        (
            "SELECT $65536 AS value",
            vec![],
            "42P02",
            "invalid parameter $65536 (RS-2056)",
        ),
        (
            "SELECT $1 AS value",
            vec![],
            "08P01",
            "invalid Bind parameter count or format codes",
        ),
        (
            "SELECT $1 AS value",
            vec![Some("x"), Some("y")],
            "08P01",
            "invalid Bind parameter count or format codes",
        ),
    ] {
        assert_eq!(
            wire_error(&wire_query(&mut stream, sql, Some(&params)).await),
            ("ERROR".to_owned(), code.to_owned(), message.to_owned())
        );
        assert_eq!(
            wire_query(&mut stream, "SELECT 'recovered' AS value", None).await,
            expected_select(&[("value", 25)], &[Some("recovered")])
        );
    }
    for (prepared, literal, value) in [
        (
            "SELECT $1::INTEGER AS value",
            "SELECT 'nope'::INTEGER AS value",
            "nope",
        ),
        (
            "SELECT $1::NUMERIC(3,2) AS value",
            "SELECT '12345'::NUMERIC(3,2) AS value",
            "12345",
        ),
    ] {
        let simple_error = wire_error(&wire_query(&mut stream, literal, None).await);
        assert_eq!((&*simple_error.0, &*simple_error.1), ("ERROR", "0A000")); // RS-2026 query error contract
        assert_eq!(
            wire_error(&wire_query(&mut stream, prepared, Some(&[Some(value)])).await),
            simple_error
        );
        assert_eq!(
            wire_query(&mut stream, "SELECT 'recovered' AS value", None).await,
            expected_select(&[("value", 25)], &[Some("recovered")])
        );
    }
    handle.abort();
}

#[tokio::test]
async fn test_ssl_negotiation_downgrade() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let port = addr.split(':').next_back().unwrap();

    // 1. Raw TCP: Send SSLRequest, expect 'N'
    let mut socket = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let ssl_request = [0u8, 0, 0, 8, 4, 210, 22, 47];
    socket.write_all(&ssl_request).await.unwrap();

    let mut response = [0u8; 1];
    socket.read_exact(&mut response).await.unwrap();
    assert_eq!(response[0], b'N');

    // 2. Startup Protocol over Raw TCP: Send StartupMessage and verify ParameterStatus messages in response
    let mut socket2 = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .unwrap();

    // Construct StartupMessage:
    // length: 4 (len) + 4 (version) + 25 (params) = 33
    // version: 3.0 (196608)
    // parameters: user\0test\0database\0test\0\0
    let mut startup_packet = vec![0u8, 0, 0, 33, 0, 3, 0, 0];
    startup_packet.extend_from_slice(b"user\0test\0database\0test\0\0");
    socket2.write_all(&startup_packet).await.unwrap();

    let mut buf = vec![0u8; 1024];
    let n = socket2.read(&mut buf).await.unwrap();
    let response_str = String::from_utf8_lossy(&buf[..n]);

    // Check that standard startup parameter status values are present in the response
    assert!(
        response_str.contains("server_version"),
        "server_version missing in startup parameters"
    );
    assert!(
        response_str.contains("14.9 (RockStream)"),
        "server version value missing"
    );
    assert!(
        response_str.contains("server_encoding"),
        "server_encoding missing"
    );
    assert!(response_str.contains("UTF8"), "UTF8 encoding missing");
    assert!(response_str.contains("DateStyle"), "DateStyle missing");
    assert!(response_str.contains("ISO, YMD"), "DateStyle value missing");
}

// ── S2 & S3: Prepared Statement Caching, Deallocate & Describe ───────────────

#[tokio::test]
async fn test_prepared_statement_caching_and_deallocate() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT id, amount FROM orders WHERE amount > 0".to_string(),
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

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // 1. Prepare with parameter type inference & explicit casts
    let stmt = client
        .prepare("SELECT id FROM orders_mv WHERE amount > $1::FLOAT8 AND id = $2")
        .await
        .unwrap();

    // Verify S3 parameter descriptions
    let params = stmt.params();
    assert_eq!(params.len(), 2);
    assert_eq!(params[0], tokio_postgres::types::Type::FLOAT8);
    assert_eq!(params[1], tokio_postgres::types::Type::INT8); // Inferred from id: Int64

    // Verify S3 row descriptions
    let columns = stmt.columns();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].name(), "id");
    assert_eq!(columns[0].type_(), &tokio_postgres::types::Type::INT8);

    // 2. Test DEALLOCATE ALL clears them
    client.simple_query("DEALLOCATE ALL").await.unwrap();
}

// ── S4: Extended Query Pipeline & Limit Bounds ────────────────────────────────

#[tokio::test]
async fn test_extended_query_pipeline() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "orders_mv".to_string(),
        sql: "SELECT id, amount FROM orders WHERE amount > 0".to_string(),
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

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // Standard extended query protocol execution (Parse -> Bind -> Execute)
    let rows = client
        .query("SELECT id FROM orders_mv WHERE id = $1", &[&100i64])
        .await
        .unwrap();
    assert!(rows.is_empty()); // empty since mock reader returns empty
}

#[tokio::test]
async fn test_prepared_statements_lru_eviction() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;

    let evicted_before =
        rockstream_gateway::server::PREPARED_STATEMENTS_EVICTED_COUNT.load(Ordering::Relaxed);

    // Prepare 1005 unique queries against a bound of 1000: v0.51.6 Slice 1
    // replaces the old hard-cap-and-error behavior with LRU eviction, so
    // every prepare must succeed and none should return RS-2600.
    let mut statements = Vec::new();
    for i in 0..1005 {
        let sql = format!("SELECT 1 -- query_uniq_{}", i);
        let res = client.prepare(&sql).await;
        assert!(
            res.is_ok(),
            "prepare {} should succeed under LRU eviction, got: {:?}",
            i,
            res.err()
        );
        statements.push(res.unwrap());
    }

    let evicted_after =
        rockstream_gateway::server::PREPARED_STATEMENTS_EVICTED_COUNT.load(Ordering::Relaxed);
    assert!(
        evicted_after - evicted_before >= 5,
        "expected at least 5 prepared statements evicted, got {}",
        evicted_after - evicted_before
    );
}

#[tokio::test]
async fn test_portals_lru_eviction() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let mut client = connect(&addr).await;

    let transaction = client.transaction().await.unwrap();
    let stmt = transaction.prepare("SELECT 1").await.unwrap();

    let evicted_before = rockstream_gateway::server::PORTALS_EVICTED_COUNT.load(Ordering::Relaxed);

    // Bind 1005 portals against a bound of 1000: every bind must succeed
    // (LRU eviction, not a hard error) since v0.51.6 Slice 1.
    let mut _portals = Vec::new();
    for i in 0..1005 {
        let res = transaction.bind(&stmt, &[]).await;
        assert!(
            res.is_ok(),
            "bind {} should succeed under LRU eviction, got: {:?}",
            i,
            res.err()
        );
        _portals.push(res.unwrap());
    }

    let evicted_after = rockstream_gateway::server::PORTALS_EVICTED_COUNT.load(Ordering::Relaxed);
    assert!(
        evicted_after - evicted_before >= 5,
        "expected at least 5 portals evicted, got {}",
        evicted_after - evicted_before
    );
}

// ── S5: Portal Suspension with max_rows ───────────────────────────────────────

struct SuspensionViewReader {
    rows: Vec<Vec<u8>>,
}

#[async_trait::async_trait]
impl ViewReader for SuspensionViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(self.rows.clone())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

#[tokio::test]
async fn test_portal_suspension_max_rows() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "large_mv".to_string(),
        sql: "SELECT id FROM large_mv".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });

    let mut rows = Vec::new();
    for i in 0..1000 {
        rows.push(format!("{i}").into_bytes());
    }

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_catalog(
        addr,
        Arc::new(catalog),
        Arc::new(SuspensionViewReader { rows }),
    );
    let (local_addr, _handle) = server.serve_background().await.unwrap();

    let mut client = connect(&local_addr.to_string()).await;

    let tx = client.transaction().await.unwrap();
    let stmt = tx.prepare("SELECT id FROM large_mv").await.unwrap();
    let portal = tx.bind(&stmt, &[]).await.unwrap();

    // Fetch first 10 rows
    let batch1 = tx.query_portal(&portal, 10).await.unwrap();
    assert_eq!(batch1.len(), 10);
    for (i, row) in batch1.iter().enumerate() {
        let val: i64 = row.get(0);
        assert_eq!(val, i as i64);
    }

    // Fetch next 15 rows
    let batch2 = tx.query_portal(&portal, 15).await.unwrap();
    assert_eq!(batch2.len(), 15);
    for (i, row) in batch2.iter().enumerate() {
        let val: i64 = row.get(0);
        assert_eq!(val, (10 + i) as i64);
    }

    // Fetch the rest (max_rows = 0 or -1 returns all remaining)
    let batch3 = tx.query_portal(&portal, 0).await.unwrap();
    assert_eq!(batch3.len(), 975);
    for (i, row) in batch3.iter().enumerate() {
        let val: i64 = row.get(0);
        assert_eq!(val, (25 + i) as i64);
    }

    tx.commit().await.unwrap();
}

// ── S6: Multi-Statement Simple Queries & Empty Queries ────────────────────────

#[tokio::test]
async fn test_multi_statement_and_empty_queries() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;

    // 1. Multi-statement simple query
    let results = client.simple_query("SELECT 1; SELECT 2").await.unwrap();
    use tokio_postgres::SimpleQueryMessage;
    let mut command_completes = 0;
    for msg in &results {
        if let SimpleQueryMessage::CommandComplete(_) = msg {
            command_completes += 1;
        }
    }
    assert!(
        command_completes >= 2,
        "Expected at least 2 CommandCompletes, got {}",
        command_completes
    );

    // 2. Empty query / whitespace-only query
    let empty_results = client.simple_query("   ;   ").await.unwrap();
    assert_eq!(empty_results.len(), 1);
    match &empty_results[0] {
        SimpleQueryMessage::CommandComplete(rows) => {
            assert_eq!(*rows, 0);
        }
        _ => panic!("Expected CommandComplete(0) for empty query"),
    }
}

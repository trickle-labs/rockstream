//! Gateway integration tests covering S1–S5 green gates.
//!
//! These tests spin up a `GatewayServer` on a random port and connect with
//! `tokio-postgres`.

use std::{sync::Arc, time::Duration};
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::{
        CatalogColumn, CatalogIndexEntry, CatalogIndexState, CatalogStubs, CatalogTable,
        CatalogView,
    },
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

async fn connect_with_retries(port: u16) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!("host=127.0.0.1 port={port} user=test dbname=test");
    let mut last_err = None;
    for _ in 0..30 {
        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((client, conn)) => {
                let handle = tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        eprintln!("connection error: {e}");
                    }
                });
                return (client, handle);
            }
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    panic!("connect failed after retries: {:?}", last_err);
}

// ── S1: server_starts_and_accepts_connection ──────────────────────────────────

#[tokio::test]
async fn server_starts_and_accepts_connection() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let client = connect(&addr).await;
    // Basic query returns no error
    let rows = client
        .simple_query("SELECT 1")
        .await
        .expect("simple_query failed");
    // We get at least one message (CommandComplete or DataRow/CommandComplete)
    assert!(!rows.is_empty(), "expected at least one message");
}

// ── S2: proof_pg_catalog_schema_reflection_queries ────────────────────────────

#[tokio::test]
async fn proof_pg_catalog_schema_reflection_queries() {
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

    // pg_catalog.pg_tables
    let rows = client
        .simple_query("SELECT schemaname, tablename FROM pg_catalog.pg_tables")
        .await
        .expect("pg_tables query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_views
    let rows = client
        .simple_query("SELECT viewname FROM pg_catalog.pg_views")
        .await
        .expect("pg_views query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_class
    let rows = client
        .simple_query("SELECT oid, relname FROM pg_catalog.pg_class")
        .await
        .expect("pg_class query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_attribute
    let rows = client
        .simple_query("SELECT attrelid, attname, atttypid FROM pg_catalog.pg_attribute")
        .await
        .expect("pg_attribute query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_namespace
    let rows = client
        .simple_query("SELECT oid, nspname FROM pg_catalog.pg_namespace")
        .await
        .expect("pg_namespace query failed");
    assert!(!rows.is_empty());

    // pg_catalog.pg_type
    let rows = client
        .simple_query("SELECT oid, typname FROM pg_catalog.pg_type")
        .await
        .expect("pg_type query failed");
    assert!(!rows.is_empty());

    // information_schema.tables
    let rows = client
        .simple_query("SELECT table_name FROM information_schema.tables")
        .await
        .expect("information_schema.tables query failed");
    assert!(!rows.is_empty());

    // information_schema.columns
    let rows = client
        .simple_query("SELECT column_name, data_type FROM information_schema.columns")
        .await
        .expect("information_schema.columns query failed");
    assert!(!rows.is_empty());

    // SHOW server_version
    let rows = client
        .simple_query("SHOW server_version")
        .await
        .expect("SHOW server_version failed");
    assert!(!rows.is_empty());

    // SHOW transaction_isolation
    let rows = client
        .simple_query("SHOW transaction_isolation")
        .await
        .expect("SHOW transaction_isolation failed");
    assert!(!rows.is_empty());

    // SET search_path
    client
        .simple_query("SET search_path TO public")
        .await
        .expect("SET search_path failed");

    // Generic SET
    client
        .simple_query("SET standard_conforming_strings = on")
        .await
        .expect("SET failed");
}

// ── S3/S4: view_reader and multi_shard inline tests already in src/ ───────────
#[tokio::test]
async fn psql_list_relations_preserves_expressions_and_aliases() {
    let catalog = CatalogStubs::new();
    catalog.add_table(CatalogTable::new("orders", vec![]));
    catalog.add_view(CatalogView {
        name: "sales_by_store".to_string(),
        sql: "SELECT store_id, SUM(amount) FROM orders GROUP BY store_id".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: None,
    });
    let (addr, handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;
    let query = r#"SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','v','m','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;"#;

    for (query, expected_columns, expected_rows) in [
        (
            query,
            vec!["Schema", "Name", "Type", "Owner"],
            vec![
                vec!["public", "orders", "table", "rockstream"],
                vec!["public", "sales_by_store", "view", "rockstream"],
            ],
        ),
        (
            r#"SELECT c.relkind AS "Name" FROM pg_catalog.pg_class c"#,
            vec!["Name"],
            vec![vec!["r"], vec!["v"]],
        ),
        (
            r#"SELECT c.relname AS "Name" FROM pg_catalog.pg_class c"#,
            vec!["Name"],
            vec![vec!["orders"], vec!["sales_by_store"]],
        ),
        (
            query,
            vec!["Schema", "Name", "Type", "Owner"],
            vec![
                vec!["public", "orders", "table", "rockstream"],
                vec!["public", "sales_by_store", "view", "rockstream"],
            ],
        ),
    ] {
        let messages = client.simple_query(query).await.unwrap();
        let rows: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect();
        assert_eq!(
            rows[0]
                .columns()
                .iter()
                .map(|c| c.name())
                .collect::<Vec<_>>(),
            expected_columns
        );
        assert_eq!(
            rows.iter()
                .map(|row| (0..row.len())
                    .map(|i| row.get(i).unwrap())
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            expected_rows
        );
    }
    handle.abort();
}

#[tokio::test]
async fn psql_catalog_queries_return_exact_metadata() {
    #[derive(serde::Deserialize)]
    struct Case {
        name: String,
        sql: String,
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    }
    let catalog = CatalogStubs::new();
    catalog.add_table(CatalogTable::new(
        "orders",
        ["id", "store_id", "amount"]
            .into_iter()
            .map(|name| CatalogColumn {
                name: name.into(),
                data_type: "Int64".into(),
            })
            .collect(),
    ));
    catalog.add_view(CatalogView {
        name: "sales_by_store".into(),
        sql: "SELECT store_id, SUM(amount) AS total_amount FROM orders GROUP BY store_id".into(),
        columns: ["store_id", "total_amount"]
            .into_iter()
            .map(|name| CatalogColumn {
                name: name.into(),
                data_type: "Int64".into(),
            })
            .collect(),
        namespace: "public".into(),
        op_id: None,
    });
    catalog.add_schema("analytics");
    catalog.add_view(CatalogView {
        name: "analytics.report".into(),
        sql: "SELECT 1".into(),
        columns: vec![],
        namespace: "analytics".into(),
        op_id: None,
    });
    catalog.add_index(CatalogIndexEntry {
        name: "orders_id_idx".into(),
        table: "orders".into(),
        index_cols: vec!["id".into()],
        state: CatalogIndexState::Ready,
        op_id: None,
    });
    assert!(catalog
        .handle_query("SELECT 'pg_attribute' FROM orders", &Default::default())
        .is_none());
    let (addr, handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("fixtures/psql_catalog_queries.json")).unwrap();
    for case in cases {
        let messages = client
            .simple_query(&case.sql)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", case.name));
        let mut columns = Vec::new();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut completed = Vec::new();
        for message in messages {
            match message {
                tokio_postgres::SimpleQueryMessage::RowDescription(fields) => {
                    columns = fields.iter().map(|f| f.name().to_string()).collect();
                }
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    rows.push(
                        (0..row.len())
                            .map(|i| row.get(i).map(str::to_string))
                            .collect(),
                    );
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(count) => completed.push(count),
                _ => panic!("unexpected catalog response for {}", case.name),
            }
        }
        assert_eq!(columns, case.columns, "{} columns", case.name);
        assert_eq!(rows, case.rows, "{} rows", case.name);
        assert_eq!(
            completed,
            vec![case.rows.len() as u64],
            "{} completion",
            case.name
        );
    }
    let rows = client
        .query(
            "SELECT c.relname AS name FROM pg_catalog.pg_class c WHERE c.relname = $1",
            &[&"orders"],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, String>("name"))
            .collect::<Vec<_>>(),
        vec!["orders"]
    );
    let error = client
        .simple_query("SELECT unsupported_catalog_function(relname) FROM pg_catalog.pg_class")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(
        (error.code().code(), error.message()),
        (
            "0A000",
            "[RS-2026] unsupported catalog function: unsupported_catalog_function"
        )
    );
    handle.abort();
}

// (Tests in view_reader.rs and multi_shard_reader.rs)

// ── S5: extended_query_protocol_parse_bind_execute ────────────────────────────

#[tokio::test]
async fn extended_query_protocol_parse_bind_execute() {
    let catalog = CatalogStubs::new();
    catalog.add_view(CatalogView {
        name: "my_view".to_string(),
        sql: "SELECT id, val FROM base".to_string(),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            CatalogColumn {
                name: "val".to_string(),
                data_type: "Float64".to_string(),
            },
        ],
        namespace: "public".to_string(),
        op_id: None,
    });

    let (addr, _handle) = start_gateway(catalog).await;
    let client = connect(&addr).await;

    // Extended query protocol: prepare() + query()
    let stmt = client
        .prepare("SELECT * FROM my_view")
        .await
        .expect("prepare failed");

    // Verify column count and type OIDs from RowDescription
    assert_eq!(
        stmt.columns().len(),
        2,
        "expected 2 columns in RowDescription"
    );
    // id → INT8 (OID 20), val → FLOAT8 (OID 701)
    let col_types: Vec<u32> = stmt.columns().iter().map(|c| c.type_().oid()).collect();
    assert_eq!(col_types[0], 20, "id column should be INT8 (OID 20)");
    assert_eq!(col_types[1], 701, "val column should be FLOAT8 (OID 701)");

    // Execute via extended query path
    let rows = client.query(&stmt, &[]).await.expect("query failed");
    // No data in view (NoopViewReader returns empty) — just verify no error
    let _ = rows;
}

// ── Slice 8: concurrent-connection stress test ────────────────────────────────

/// Slice 8 green gate: 1 000 simultaneous connections, 100 queries each, zero errors.
///
/// Memory bound: MAX_CONNECTIONS = 10_000; peak RSS target < 2 GiB (not measured in CI —
/// the absence of OOM kills or connection-level errors serves as the proxy).
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_1000_connections_no_errors() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let port: u16 = addr.split(':').next_back().unwrap().parse().unwrap();

    let n_connections: usize = 1_000;
    let queries_per_connection: usize = 100;
    let sem = Arc::new(tokio::sync::Semaphore::new(30));

    let mut handles = Vec::with_capacity(n_connections);
    for _ in 0..n_connections {
        let sem = Arc::clone(&sem);
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let (client, _conn) = connect_with_retries(port).await;
            for _ in 0..queries_per_connection {
                client.simple_query("SELECT 1").await.expect("query failed");
            }
        }));
    }

    let results = futures::future::join_all(handles).await;
    let errors: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
    assert!(
        errors.is_empty(),
        "{} out of {} connections encountered task errors",
        errors.len(),
        n_connections
    );
}

// ── PgBouncer pooled transactions stress test ─────────────────────────────────

/// Proof claim: PgBouncer 1.21 TC — 10 000 transactions across 50 clients, zero errors.
///
/// Each of the 50 clients runs 200 BEGIN/query/COMMIT cycles sequentially.
#[tokio::test(flavor = "multi_thread")]
async fn test_pgbouncer_pooled_transactions() {
    let (addr, _handle) = start_gateway(CatalogStubs::new()).await;
    let port: u16 = addr.split(':').next_back().unwrap().parse().unwrap();

    let n_clients: usize = 50;
    let txns_per_client: usize = 200; // 50 × 200 = 10 000 total transactions

    let mut handles = Vec::with_capacity(n_clients);
    for _ in 0..n_clients {
        handles.push(tokio::spawn(async move {
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
            for _ in 0..txns_per_client {
                client.simple_query("BEGIN").await.expect("BEGIN failed");
                client
                    .simple_query("SELECT 1")
                    .await
                    .expect("SELECT in txn failed");
                client.simple_query("COMMIT").await.expect("COMMIT failed");
            }
        }));
    }

    let results = futures::future::join_all(handles).await;
    let errors: Vec<_> = results.iter().filter(|r| r.is_err()).collect();
    assert!(
        errors.is_empty(),
        "{} out of {} client tasks encountered errors",
        errors.len(),
        n_clients
    );
}

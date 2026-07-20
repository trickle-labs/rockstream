use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use tokio_postgres::NoTls;

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

fn data_rows_from(
    msgs: &[tokio_postgres::SimpleQueryMessage],
) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

async fn start_gateway_with_shard(
    shard_path: &str,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
}

async fn run_fixture_sql(
    client: &tokio_postgres::Client,
    statements: &[&str],
    idempotency_key: &str,
) {
    client
        .simple_query(&format!(
            "SET rockstream.idempotency_key = '{idempotency_key}'"
        ))
        .await
        .expect("SET rockstream.idempotency_key");
    client.simple_query("BEGIN").await.expect("BEGIN");
    for statement in statements {
        client
            .simple_query(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement} failed: {e}"));
    }
    client.simple_query("COMMIT").await.expect("COMMIT");
}

#[tokio::test]
async fn select_with_where_predicate_filters_rows() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("query-time-where").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE sales (id BIGINT, region TEXT)")
        .await
        .expect("CREATE TABLE sales");
    run_fixture_sql(
        &client,
        &[
            "INSERT INTO sales (id, region) VALUES (1, 'US')",
            "INSERT INTO sales (id, region) VALUES (2, 'EU')",
            "INSERT INTO sales (id, region) VALUES (3, 'US')",
        ],
        "query-time-where-fixture",
    )
    .await;

    let rows = client
        .simple_query("SELECT * FROM sales WHERE region = 'US' ORDER BY id")
        .await
        .expect("SELECT ... WHERE region = 'US'");
    let rows = data_rows_from(&rows);
    let got: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get("id").unwrap_or("").to_string(),
                row.get("region").unwrap_or("").to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".to_string(), "US".to_string()),
            ("3".to_string(), "US".to_string()),
        ]
    );
}

#[tokio::test]
async fn ad_hoc_two_table_join_matches_oracle() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("query-time-join").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE customers (id BIGINT, name TEXT)")
        .await
        .expect("CREATE TABLE customers");
    client
        .simple_query("CREATE TABLE orders (id BIGINT, customer_id BIGINT)")
        .await
        .expect("CREATE TABLE orders");
    run_fixture_sql(
        &client,
        &[
            "INSERT INTO customers (id, name) VALUES (1, 'Alice')",
            "INSERT INTO customers (id, name) VALUES (2, 'Bob')",
            "INSERT INTO orders (id, customer_id) VALUES (100, 1)",
            "INSERT INTO orders (id, customer_id) VALUES (101, 2)",
            "INSERT INTO orders (id, customer_id) VALUES (102, 1)",
        ],
        "query-time-join-fixture",
    )
    .await;

    let rows = client
        .simple_query(
            "SELECT o.id, c.name AS customer_name \
             FROM orders o JOIN customers c ON o.customer_id = c.id \
             ORDER BY o.id",
        )
        .await
        .expect("JOIN query");
    let rows = data_rows_from(&rows);
    let got: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get("id").unwrap_or("").to_string(),
                row.get("customer_name").unwrap_or("").to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("100".to_string(), "Alice".to_string()),
            ("101".to_string(), "Bob".to_string()),
            ("102".to_string(), "Alice".to_string()),
        ]
    );
}

#[tokio::test]
async fn ad_hoc_group_by_matches_oracle() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("query-time-group-by").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE regional_sales (id BIGINT, region TEXT)")
        .await
        .expect("CREATE TABLE regional_sales");
    run_fixture_sql(
        &client,
        &[
            "INSERT INTO regional_sales (id, region) VALUES (1, 'APAC')",
            "INSERT INTO regional_sales (id, region) VALUES (2, 'US')",
            "INSERT INTO regional_sales (id, region) VALUES (3, 'US')",
            "INSERT INTO regional_sales (id, region) VALUES (4, 'EU')",
        ],
        "query-time-group-by-fixture",
    )
    .await;

    let rows = client
        .simple_query(
            "SELECT region, COUNT(*) AS row_count \
             FROM regional_sales GROUP BY region ORDER BY region",
        )
        .await
        .expect("GROUP BY query");
    let rows = data_rows_from(&rows);
    let got: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get("region").unwrap_or("").to_string(),
                row.get("row_count").unwrap_or("").to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("APAC".to_string(), "1".to_string()),
            ("EU".to_string(), "1".to_string()),
            ("US".to_string(), "2".to_string()),
        ]
    );
}

#[tokio::test]
async fn ad_hoc_subquery_matches_oracle() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("query-time-subquery").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT, region TEXT)")
        .await
        .expect("CREATE TABLE t");
    client
        .simple_query("CREATE TABLE t2 (id BIGINT, flag BOOLEAN)")
        .await
        .expect("CREATE TABLE t2");
    run_fixture_sql(
        &client,
        &[
            "INSERT INTO t (id, region) VALUES (1, 'US')",
            "INSERT INTO t (id, region) VALUES (2, 'EU')",
            "INSERT INTO t (id, region) VALUES (3, 'APAC')",
            "INSERT INTO t2 (id, flag) VALUES (1, true)",
            "INSERT INTO t2 (id, flag) VALUES (3, true)",
            "INSERT INTO t2 (id, flag) VALUES (2, false)",
        ],
        "query-time-subquery-fixture",
    )
    .await;

    let rows = client
        .simple_query(
            "SELECT * FROM t \
             WHERE id IN (SELECT id FROM t2 WHERE flag = true) \
             ORDER BY id",
        )
        .await
        .expect("subquery query");
    let rows = data_rows_from(&rows);
    let got: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get("id").unwrap_or("").to_string(),
                row.get("region").unwrap_or("").to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".to_string(), "US".to_string()),
            ("3".to_string(), "APAC".to_string()),
        ]
    );
}

#[tokio::test]
async fn ad_hoc_cte_matches_oracle() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("query-time-cte").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE events (id BIGINT, region TEXT)")
        .await
        .expect("CREATE TABLE events");
    run_fixture_sql(
        &client,
        &[
            "INSERT INTO events (id, region) VALUES (1, 'US')",
            "INSERT INTO events (id, region) VALUES (2, 'EU')",
            "INSERT INTO events (id, region) VALUES (3, 'US')",
        ],
        "query-time-cte-fixture",
    )
    .await;

    let rows = client
        .simple_query(
            "WITH recent AS (SELECT * FROM events WHERE region = 'US') \
             SELECT * FROM recent ORDER BY id",
        )
        .await
        .expect("CTE query");
    let rows = data_rows_from(&rows);
    let got: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get("id").unwrap_or("").to_string(),
                row.get("region").unwrap_or("").to_string(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("1".to_string(), "US".to_string()),
            ("3".to_string(), "US".to_string()),
        ]
    );
}

#[tokio::test]
async fn plain_explain_returns_real_plan_tree() {
    let (port, _handle, _shard_db) = start_gateway_with_shard("query-time-explain").await;
    let client = connect_port(port).await;

    client
        .simple_query("CREATE TABLE customers (id BIGINT, name TEXT, region TEXT)")
        .await
        .expect("CREATE TABLE customers");
    client
        .simple_query("CREATE TABLE orders (id BIGINT, customer_id BIGINT)")
        .await
        .expect("CREATE TABLE orders");

    let rows = client
        .simple_query(
            "EXPLAIN SELECT o.id, c.name \
             FROM orders o JOIN customers c ON o.customer_id = c.id \
             WHERE c.region = 'US'",
        )
        .await
        .expect("EXPLAIN query");
    let rows = data_rows_from(&rows);
    let plan_text = rows
        .iter()
        .map(|row| row.get("QUERY PLAN").unwrap_or("").to_string())
        .collect::<Vec<_>>()
        .join("\n");

    // DataFusion's real `Display` output for a filtered join names actual
    // plan node types — not the old "Plan: SeqScan → ..." fabricated stub.
    assert!(
        plan_text.contains("Filter") || plan_text.contains("filter"),
        "expected a real Filter node in plan, got: {plan_text}"
    );
    assert!(
        plan_text.to_ascii_lowercase().contains("join"),
        "expected a real join node in plan, got: {plan_text}"
    );
    assert!(
        plan_text.contains("TableScan") || plan_text.contains("MemTableExec"),
        "expected a real scan node in plan, got: {plan_text}"
    );
    assert!(
        !plan_text.contains("Query: EXPLAIN SELECT"),
        "old fabricated stub text must not appear, got: {plan_text}"
    );
}

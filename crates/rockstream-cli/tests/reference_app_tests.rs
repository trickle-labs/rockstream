//! Reference Application E2E and multi-source view maintenance tests (`GP-006`).

use rockstream_cli::{start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use tempfile::TempDir;
use tokio_postgres::NoTls;

fn test_gateway_opts(dir: &TempDir) -> StartOptions {
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
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
    }
}

async fn execute_query_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<String>> {
    let msgs = client
        .simple_query(sql)
        .await
        .expect("query execution failed");
    let mut rows = Vec::new();
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            let mut row = Vec::new();
            for i in 0..r.len() {
                row.push(r.get(i).unwrap_or("").to_string());
            }
            rows.push(row);
        }
    }
    rows
}

#[tokio::test]
async fn test_reference_app_e2e_maintenance() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);

    let (addr, handle) = start_gateway(&opts).await.expect("start_gateway failed");
    let connect_str = format!(
        "host=127.0.0.1 port={} user=rockstream dbname=rockstream",
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&connect_str, NoTls)
        .await
        .expect("pg connect failed");

    let conn_task = tokio::spawn(async move {
        let _ = conn.await;
    });

    // 1. Schema setup
    client
        .simple_query("CREATE TABLE customers (customer_id BIGINT, tier BIGINT);")
        .await
        .expect("create customers table");

    client
        .simple_query(
            "CREATE TABLE orders (order_id BIGINT, customer_id BIGINT, store_id BIGINT, amount BIGINT, risk_score BIGINT);",
        )
        .await
        .expect("create orders table");

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW spending_by_tier AS SELECT customers.tier, COUNT(*) AS total_orders, SUM(orders.amount) AS total_amount FROM customers JOIN orders ON customers.customer_id = orders.customer_id GROUP BY customers.tier;",
        )
        .await
        .expect("create spending_by_tier view");

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW store_volume AS SELECT store_id, COUNT(*) AS order_count, SUM(amount) AS total_volume FROM orders GROUP BY store_id;",
        )
        .await
        .expect("create store_volume view");

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW fraud_alerts AS SELECT order_id, customer_id, store_id, amount, risk_score FROM orders WHERE risk_score >= 80;",
        )
        .await
        .expect("create fraud_alerts view");

    // 2. Ingest dimension data (tier: 1=Platinum, 2=Gold, 3=Silver)
    client
        .simple_query("INSERT INTO customers VALUES (1, 1), (2, 2), (3, 3);")
        .await
        .expect("insert customers");

    // 3. Ingest orders stream
    client
        .simple_query(
            "INSERT INTO orders VALUES (1001, 1, 50, 500, 15), (1002, 2, 50, 1200, 85), (1003, 3, 60, 300, 10), (1004, 1, 60, 2500, 92);",
        )
        .await
        .expect("insert orders");

    // 4. Verify spending_by_tier
    let tier_rows = execute_query_rows(
        &client,
        "SELECT tier, total_orders, total_amount FROM spending_by_tier ORDER BY tier;",
    )
    .await;
    assert_eq!(
        tier_rows,
        vec![
            vec!["1".to_string(), "2".to_string(), "3000".to_string()],
            vec!["2".to_string(), "1".to_string(), "1200".to_string()],
            vec!["3".to_string(), "1".to_string(), "300".to_string()],
        ]
    );

    // 5. Verify store_volume
    let store_rows = execute_query_rows(
        &client,
        "SELECT store_id, order_count, total_volume FROM store_volume ORDER BY store_id;",
    )
    .await;
    assert_eq!(
        store_rows,
        vec![
            vec!["50".to_string(), "2".to_string(), "1700".to_string()],
            vec!["60".to_string(), "2".to_string(), "2800".to_string()],
        ]
    );

    // 6. Verify fraud_alerts (risk_score >= 80)
    let fraud_rows = execute_query_rows(
        &client,
        "SELECT order_id, customer_id, store_id, amount, risk_score FROM fraud_alerts ORDER BY order_id;",
    )
    .await;
    assert_eq!(
        fraud_rows,
        vec![
            vec![
                "1002".to_string(),
                "2".to_string(),
                "50".to_string(),
                "1200".to_string(),
                "85".to_string()
            ],
            vec![
                "1004".to_string(),
                "1".to_string(),
                "60".to_string(),
                "2500".to_string(),
                "92".to_string()
            ],
        ]
    );

    // 7. Retraction handling: refund/delete fraudulent order 1002
    client
        .simple_query(
            "DELETE FROM orders WHERE order_id = 1002, customer_id = 2, store_id = 50, amount = 1200, risk_score = 85;",
        )
        .await
        .expect("delete order 1002");

    let fraud_after_delete = execute_query_rows(
        &client,
        "SELECT order_id, customer_id, store_id, amount, risk_score FROM fraud_alerts ORDER BY order_id;",
    )
    .await;
    assert_eq!(
        fraud_after_delete,
        vec![vec![
            "1004".to_string(),
            "1".to_string(),
            "60".to_string(),
            "2500".to_string(),
            "92".to_string()
        ],]
    );

    let store_after_delete = execute_query_rows(
        &client,
        "SELECT store_id, order_count, total_volume FROM store_volume ORDER BY store_id;",
    )
    .await;
    assert_eq!(
        store_after_delete,
        vec![
            vec!["50".to_string(), "1".to_string(), "500".to_string()],
            vec!["60".to_string(), "2".to_string(), "2800".to_string()],
        ]
    );

    handle.abort();
    conn_task.abort();
}

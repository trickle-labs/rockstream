//! Verification tests for Golden Path templates (`GP-002`, `GP-003`, `GP-004`).
//!
//! Asserts that every generated template produces exact incremental materialized
//! view results across sequential inserts, updates, and deletes over pgwire.

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
        shutdown_timeout_secs: None,
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
async fn test_local_template_view_maintenance() {
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

    // 1. Schema setup from local template
    client
        .simple_query("CREATE TABLE orders (id BIGINT, store_id BIGINT, amount BIGINT);")
        .await
        .expect("create orders table");

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW sales_by_store AS SELECT store_id, SUM(amount) AS total_amount FROM orders GROUP BY store_id;",
        )
        .await
        .expect("create sales_by_store view");

    // 2. Initial insert
    client
        .simple_query("INSERT INTO orders (id, store_id, amount) VALUES (1, 100, 50), (2, 100, 70), (3, 200, 40);")
        .await
        .expect("insert initial orders");

    let rows_after_insert = execute_query_rows(
        &client,
        "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
    )
    .await;
    assert_eq!(
        rows_after_insert,
        vec![
            vec!["100".to_string(), "120".to_string()],
            vec!["200".to_string(), "40".to_string()],
        ]
    );

    // 3. Update order
    client
        .simple_query("UPDATE orders SET amount = 100 WHERE id = 1, store_id = 100, amount = 50;")
        .await
        .expect("update order");

    let rows_after_update = execute_query_rows(
        &client,
        "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
    )
    .await;
    assert_eq!(
        rows_after_update,
        vec![
            vec!["100".to_string(), "170".to_string()],
            vec!["200".to_string(), "40".to_string()],
        ]
    );

    // 4. Delete order
    client
        .simple_query("DELETE FROM orders WHERE id = 3, store_id = 200, amount = 40;")
        .await
        .expect("delete order");

    let rows_after_delete = execute_query_rows(
        &client,
        "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
    )
    .await;
    assert_eq!(
        rows_after_delete,
        vec![vec!["100".to_string(), "170".to_string()],]
    );

    handle.abort();
    conn_task.abort();
}

#[tokio::test]
async fn test_kafka_template_view_maintenance() {
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

    // 1. Schema setup from kafka template
    client
        .simple_query("CREATE TABLE events (user_id BIGINT, duration_ms BIGINT);")
        .await
        .expect("create events table");

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW pageviews_by_user AS SELECT user_id, COUNT(*) AS pageviews, SUM(duration_ms) AS total_duration_ms FROM events GROUP BY user_id;",
        )
        .await
        .expect("create pageviews_by_user view");

    // 2. Stream initial event batch
    client
        .simple_query("INSERT INTO events VALUES (1, 150), (1, 250), (2, 100);")
        .await
        .expect("stream events batch 1");

    let rows_batch1 = execute_query_rows(
        &client,
        "SELECT user_id, pageviews, total_duration_ms FROM pageviews_by_user ORDER BY user_id;",
    )
    .await;
    assert_eq!(
        rows_batch1,
        vec![
            vec!["1".to_string(), "2".to_string(), "400".to_string()],
            vec!["2".to_string(), "1".to_string(), "100".to_string()],
        ]
    );

    // 3. Stream second event batch
    client
        .simple_query("INSERT INTO events VALUES (2, 200), (3, 50);")
        .await
        .expect("stream events batch 2");

    let rows_batch2 = execute_query_rows(
        &client,
        "SELECT user_id, pageviews, total_duration_ms FROM pageviews_by_user ORDER BY user_id;",
    )
    .await;
    assert_eq!(
        rows_batch2,
        vec![
            vec!["1".to_string(), "2".to_string(), "400".to_string()],
            vec!["2".to_string(), "2".to_string(), "300".to_string()],
            vec!["3".to_string(), "1".to_string(), "50".to_string()],
        ]
    );

    handle.abort();
    conn_task.abort();
}

#[tokio::test]
async fn test_postgres_cdc_template_view_maintenance() {
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

    // 1. Schema setup from postgres-cdc template
    client
        .simple_query("CREATE TABLE customers (id BIGINT, region_id BIGINT);")
        .await
        .expect("create customers table");

    client
        .simple_query("CREATE TABLE orders (id BIGINT, customer_id BIGINT, total BIGINT);")
        .await
        .expect("create orders table");

    client
        .simple_query(
            "CREATE MATERIALIZED VIEW sales_by_region AS SELECT customers.region_id, SUM(orders.total) AS total_sales FROM customers JOIN orders ON customers.id = orders.customer_id GROUP BY customers.region_id;",
        )
        .await
        .expect("create sales_by_region view");

    // 2. Initial CDC sync
    client
        .simple_query("INSERT INTO customers (id, region_id) VALUES (1, 10), (2, 20), (3, 30);")
        .await
        .expect("insert initial customers");

    client
        .simple_query("INSERT INTO orders (id, customer_id, total) VALUES (101, 1, 150), (102, 2, 200), (103, 1, 50), (104, 3, 300);")
        .await
        .expect("insert initial orders");

    let rows_cdc1 = execute_query_rows(
        &client,
        "SELECT region_id, total_sales FROM sales_by_region ORDER BY region_id;",
    )
    .await;
    assert_eq!(
        rows_cdc1,
        vec![
            vec!["10".to_string(), "200".to_string()],
            vec!["20".to_string(), "200".to_string()],
            vec!["30".to_string(), "300".to_string()],
        ]
    );

    // 3. Replicated mutation
    client
        .simple_query("INSERT INTO orders (id, customer_id, total) VALUES (105, 2, 100);")
        .await
        .expect("insert mutation order");

    let rows_cdc2 = execute_query_rows(
        &client,
        "SELECT region_id, total_sales FROM sales_by_region ORDER BY region_id;",
    )
    .await;
    assert_eq!(
        rows_cdc2,
        vec![
            vec!["10".to_string(), "200".to_string()],
            vec!["20".to_string(), "300".to_string()],
            vec!["30".to_string(), "300".to_string()],
        ]
    );

    handle.abort();
    conn_task.abort();
}

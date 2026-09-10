//! Oracle differential test proving that the Golden Path sales_by_store materialized view
//! maintains exact equivalence between incremental maintenance and batch calculation
//! across sequences of INSERT, UPDATE, and DELETE mutations.

use rockstream_cli::{connect_client, execute_query, start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use std::collections::BTreeMap;
use tempfile::TempDir;

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

#[tokio::test]
async fn sales_view_incremental_equals_batch() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway");
    let endpoint = addr.to_string();

    let (client, _conn_handle) = connect_client(&endpoint, 10).await.expect("connect client");

    // 1. Create base table and materialized view
    execute_query(
        &client,
        "CREATE TABLE orders (id BIGINT, store_id BIGINT, amount BIGINT);",
    )
    .await
    .expect("create table");

    execute_query(
        &client,
        "CREATE MATERIALIZED VIEW sales_by_store AS SELECT store_id, SUM(amount) AS total_amount FROM orders GROUP BY store_id;",
    )
    .await
    .expect("create materialized view");

    // Track ground truth table rows: id -> (store_id, amount)
    let mut table_state: BTreeMap<i64, (i64, i64)> = BTreeMap::new();

    // Deterministic pseudo-random sequence of 40 operations
    let mut op_counter = 0;
    for step in 1..=40 {
        op_counter += 1;
        let store_id = (op_counter % 5) + 1; // stores 1..=5
        let id = op_counter;
        let amount = (op_counter * 17) % 100 + 10;

        if step <= 20 {
            // First 20 steps: purely inserts
            let sql = format!(
                "INSERT INTO orders (id, store_id, amount) VALUES ({id}, {store_id}, {amount});"
            );
            execute_query(&client, &sql).await.expect("insert");
            table_state.insert(id, (store_id, amount));
        } else if step <= 30 {
            // Next 10 steps: updates
            let target_id = (step - 20) as i64;
            if let Some(&(old_store, old_amount)) = table_state.get(&target_id) {
                let new_amount = old_amount + 25;
                let sql = format!(
                    "UPDATE orders SET amount = {new_amount} WHERE id = {target_id}, store_id = {old_store}, amount = {old_amount};"
                );
                execute_query(&client, &sql).await.expect("update");
                table_state.insert(target_id, (old_store, new_amount));
            }
        } else {
            // Last 10 steps: deletes
            let target_id = (step - 30) as i64;
            if let Some((old_store, old_amount)) = table_state.remove(&target_id) {
                let sql = format!(
                    "DELETE FROM orders WHERE id = {target_id}, store_id = {old_store}, amount = {old_amount};"
                );
                execute_query(&client, &sql).await.expect("delete");
            }
        }

        // Verify incremental equals batch at each step
        let mut batch_totals: BTreeMap<i64, i64> = BTreeMap::new();
        for &(s_id, amt) in table_state.values() {
            *batch_totals.entry(s_id).or_insert(0) += amt;
        }

        // Query view via pgwire
        let result = execute_query(
            &client,
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
        )
        .await
        .expect("select view");

        let mut actual_view: BTreeMap<i64, i64> = BTreeMap::new();
        for row in &result.rows {
            let s_id: i64 = row[0]
                .as_ref()
                .expect("store_id")
                .parse()
                .expect("parse store_id");
            let tot: i64 = row[1]
                .as_ref()
                .expect("total_amount")
                .parse()
                .expect("parse total_amount");
            actual_view.insert(s_id, tot);
        }

        assert_eq!(
            actual_view, batch_totals,
            "mismatch at step {step}: incremental view != batch recalculation"
        );
    }
}

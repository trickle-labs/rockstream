//! Embedded Pgwire client tests for `rockstream query` (Slice 2).

use rockstream_cli::{run_embedded_query, start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{RS_0004, RS_0005, RS_2001};
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use std::fs;
use std::path::Path;
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
async fn test_client_unreachable_node_fails_with_rs0004() {
    let err = run_embedded_query("SELECT 1;", None, "table", false, "127.0.0.1:59999")
        .await
        .expect_err("should fail when gateway is unreachable");

    assert_eq!(err.code, RS_0004);
    assert!(err
        .message
        .contains("cannot reach RockStream gateway at 127.0.0.1:59999"));
    assert!(err.next_steps.contains("rockstream start"));
}

#[tokio::test]
async fn test_client_missing_sql_file_fails_with_rs0004() {
    let err = run_embedded_query(
        "",
        Some(Path::new("nonexistent_test_query.sql")),
        "table",
        false,
        "127.0.0.1:5432",
    )
    .await
    .expect_err("should fail for missing file");

    assert_eq!(err.code, RS_0004);
    assert!(err.message.contains("failed to read SQL file"));
}

#[tokio::test]
async fn test_client_empty_query_fails_with_rs2001() {
    let err = run_embedded_query("   ", None, "table", false, "127.0.0.1:5432")
        .await
        .expect_err("should fail for empty query");

    assert_eq!(err.code, RS_2001);
    assert!(err.message.contains("no SQL query provided"));
}

#[tokio::test]
async fn test_client_oversized_sql_file_fails_with_rs0005() {
    let temp_dir = TempDir::new().expect("tempdir");
    let oversized_file = temp_dir.path().join("large.sql");

    // Write file slightly larger than 10 MB limit
    let file = fs::File::create(&oversized_file).expect("create file");
    file.set_len(10 * 1024 * 1024 + 1024).expect("set_len");

    let err = run_embedded_query("", Some(&oversized_file), "table", false, "127.0.0.1:5432")
        .await
        .expect_err("should fail for oversized file");

    assert_eq!(err.code, RS_0005);
    assert!(err
        .message
        .contains("exceeds maximum allowed size of 10 MB"));
}

#[tokio::test]
async fn test_client_simple_queries_across_all_formats() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway");
    let endpoint = addr.to_string();

    // 1. Setup table and data
    run_embedded_query(
        "CREATE TABLE products (id BIGINT, name VARCHAR(32), price BIGINT);",
        None,
        "table",
        false,
        &endpoint,
    )
    .await
    .expect("create table");

    run_embedded_query(
        "INSERT INTO products (id, name, price) VALUES (1, 'apple', 3), (2, 'banana', 5);",
        None,
        "table",
        false,
        &endpoint,
    )
    .await
    .expect("insert data");

    // 2. Table format
    let table_out = run_embedded_query(
        "SELECT id, name, price FROM products ORDER BY id;",
        None,
        "table",
        true,
        &endpoint,
    )
    .await
    .expect("query table");

    assert!(table_out.contains("id | name   | price"));
    assert!(table_out.contains("1  | apple  | 3"));
    assert!(table_out.contains("2  | banana | 5"));
    assert!(table_out.contains("(2 rows)"));
    assert!(table_out.contains("Time:"));

    // 3. JSON format
    let json_out = run_embedded_query(
        "SELECT id, name, price FROM products ORDER BY id;",
        None,
        "json",
        false,
        &endpoint,
    )
    .await
    .expect("query json");

    let json_val: serde_json::Value = serde_json::from_str(&json_out).expect("valid json");
    assert!(json_val.is_array());
    let arr = json_val.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], 1);
    assert_eq!(arr[0]["name"], "apple");
    assert_eq!(arr[0]["price"], 3);
    assert_eq!(arr[1]["id"], 2);
    assert_eq!(arr[1]["name"], "banana");
    assert_eq!(arr[1]["price"], 5);

    // 4. CSV format
    let csv_out = run_embedded_query(
        "SELECT id, name, price FROM products ORDER BY id;",
        None,
        "csv",
        false,
        &endpoint,
    )
    .await
    .expect("query csv");

    let mut csv_lines = csv_out.lines();
    assert_eq!(csv_lines.next(), Some("id,name,price"));
    assert_eq!(csv_lines.next(), Some("1,apple,3"));
    assert_eq!(csv_lines.next(), Some("2,banana,5"));
}

#[tokio::test]
async fn test_client_sql_file_execution_with_timing() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway");
    let endpoint = addr.to_string();

    let temp_sql = dir.path().join("query.sql");
    fs::write(&temp_sql, "SELECT 42 AS answer;").expect("write sql");

    let out = run_embedded_query("", Some(&temp_sql), "table", true, &endpoint)
        .await
        .expect("run sql file");

    assert!(out.contains("answer"));
    assert!(out.contains("42"));
    assert!(out.contains("(1 row)"));
    assert!(out.contains("Time:"));
}

#[tokio::test]
async fn test_client_syntax_error_returns_rs2001() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway");
    let endpoint = addr.to_string();

    let err = run_embedded_query(
        "UPDATE test_t SET x = '1' RETURNING;",
        None,
        "table",
        false,
        &endpoint,
    )
    .await
    .expect_err("should fail with syntax error");

    assert_eq!(err.code, RS_2001);
}

#[tokio::test]
async fn test_client_undefined_relation_returns_rs1004() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);

    let (addr, _handle) = start_gateway(&opts).await.expect("start_gateway");
    let endpoint = addr.to_string();

    let err = run_embedded_query(
        "SELECT * FROM table_does_not_exist_xyz;",
        None,
        "table",
        false,
        &endpoint,
    )
    .await
    .expect_err("should fail with undefined relation error");

    assert_eq!(err.code, rockstream_types::error_code::RS_1004);
}

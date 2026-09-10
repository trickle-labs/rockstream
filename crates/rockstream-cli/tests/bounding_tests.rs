//! Resource bounding and operational limit tests (Slice 7, Matrix F).

use rockstream_cli::client::{
    connect_client, execute_query, run_embedded_query, MAX_QUERY_RESULT_ROWS,
    MAX_SHELL_STATEMENT_BYTES, MAX_SQL_FILE_SIZE_BYTES,
};
use rockstream_cli::init::{scaffold_project, InitOptions};
use rockstream_cli::project::run_project_apply;
use rockstream_cli::shell::run_shell_with_io;
use rockstream_cli::{start_gateway, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{RS_0004, RS_0005};
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use std::fs;
use std::io::Cursor;
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
async fn sql_file_buffer_exceeds_limit_fails() {
    let temp_dir = TempDir::new().expect("tempdir");
    let large_file = temp_dir.path().join("too_large.sql");

    // Write a file > 10 MB (10 MB + 1 byte)
    let size = (MAX_SQL_FILE_SIZE_BYTES + 1) as usize;
    let mut data = vec![b' '; size];
    data[size - 1] = b';';
    fs::write(&large_file, &data).expect("write large file");

    let err = run_embedded_query("", Some(&large_file), "table", false, "127.0.0.1:5432")
        .await
        .expect_err("should fail for file > 10 MB");

    assert_eq!(err.code, RS_0005);
    assert!(err
        .message
        .contains("exceeds maximum allowed size of 10 MB"));
}

#[tokio::test]
async fn shell_input_exceeding_limit_fails() {
    // Generate a statement larger than 64 KB without semicolon
    let oversized_line = "a".repeat(MAX_SHELL_STATEMENT_BYTES + 100) + "\n";
    let input = Cursor::new(oversized_line + "\\q\n");
    let mut output = Vec::new();

    // Start a gateway for client connection
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");

    let (client, _c_handle) = connect_client(&addr.to_string(), 10)
        .await
        .expect("connect");
    run_shell_with_io(&client, input, &mut output)
        .await
        .expect("shell loop");

    let out_str = String::from_utf8_lossy(&output);
    assert!(out_str.contains("RS-0005"));
    assert!(out_str.contains("shell statement buffer exceeded maximum limit of 65536 bytes"));
}

#[tokio::test]
async fn query_result_accumulation_bounded() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let (client, _c_handle) = connect_client(&addr.to_string(), 10)
        .await
        .expect("connect");

    // Create table and insert 10,001 rows
    client
        .simple_query("CREATE TABLE big_items (id BIGINT);")
        .await
        .expect("create table");

    // Insert in batches of 1,000
    for batch_i in 0..11 {
        let mut vals = Vec::new();
        for j in 0..1000 {
            vals.push(format!("({})", batch_i * 1000 + j));
        }
        let insert_sql = format!("INSERT INTO big_items (id) VALUES {};", vals.join(","));
        client
            .simple_query(&insert_sql)
            .await
            .expect("insert batch");
    }

    // Now query big_items — should exceed 10,000 rows!
    let err = execute_query(&client, "SELECT id FROM big_items;")
        .await
        .expect_err("should fail when result > 10,000 rows");

    assert_eq!(err.code, RS_0005);
    assert!(err
        .message
        .contains("exceeded maximum allowed limit of 10000 rows"));
}

#[tokio::test]
async fn client_connection_timeout_fires() {
    // Attempt connection to non-routable IP with 1s timeout
    let err = connect_client("192.0.2.1:5432", 1)
        .await
        .expect_err("must timeout or fail");

    assert_eq!(err.code, RS_0004);
}

#[tokio::test]
async fn seed_stream_stays_bounded_under_large_csv() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");
    let endpoint = addr.to_string();

    let proj_dir = dir.path().join("large_seed_proj");
    let init_opts = InitOptions {
        name: "large_seed_proj".to_string(),
        template: "local".to_string(),
        dir: Some(proj_dir.clone()),
        force: false,
    };
    scaffold_project(&init_opts).expect("scaffold");

    // Generate CSV with 2,500 rows (spans 3 batches of 1,000)
    let mut csv_data = String::from("id,store_id,amount\n");
    for i in 1..=2500 {
        csv_data.push_str(&format!("{i},100,10\n"));
    }
    fs::write(proj_dir.join("data/seed.csv"), csv_data).expect("write large seed");

    // Apply should stream through 1,000-row batches successfully!
    let apply_res = run_project_apply(&proj_dir, &endpoint, 30)
        .await
        .expect("apply should stream batches successfully");

    assert!(apply_res.contains("seed table 'orders' from 'data/seed.csv' (ingested)"));

    let (client, _c_handle) = connect_client(&endpoint, 10).await.expect("connect");
    let view_res = execute_query(
        &client,
        "SELECT store_id, total_amount FROM sales_by_store;",
    )
    .await
    .expect("query view");

    assert_eq!(view_res.rows.len(), 1);
    // 2,500 rows of amount 10 = 25,000 total
    assert_eq!(
        view_res.rows[0],
        vec![Some("100".to_string()), Some("25000".to_string())]
    );
}

#[test]
fn client_and_file_buffers_strictly_enforce_limits() {
    assert_eq!(MAX_SQL_FILE_SIZE_BYTES, 10 * 1024 * 1024);
    assert_eq!(MAX_QUERY_RESULT_ROWS, 10_000);
    assert_eq!(MAX_SHELL_STATEMENT_BYTES, 64 * 1024);
}

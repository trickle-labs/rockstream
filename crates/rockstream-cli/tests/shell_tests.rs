//! Interactive Pgwire shell tests (Slice 3).

use rockstream_cli::{
    connect_client, run_interactive_shell, run_shell_with_io, start_gateway, StartOptions,
};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::RS_0004;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
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

#[test]
fn test_shell_unreachable_node_fails_with_rs0004() {
    let err =
        run_interactive_shell("127.0.0.1:59998").expect_err("should fail for unreachable gateway");
    assert_eq!(err.code, RS_0004);
    assert!(err
        .message
        .contains("cannot reach RockStream gateway at 127.0.0.1:59998"));
}

#[tokio::test]
async fn test_shell_exit_commands() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");

    let (client, _c_handle) = connect_client(&addr.to_string(), 30)
        .await
        .expect("connect");

    for exit_cmd in &["\\q\n", "exit\n", "quit\n"] {
        let input = Cursor::new(exit_cmd.as_bytes());
        let mut output = Vec::new();
        let res = run_shell_with_io(&client, input, &mut output).await;
        assert!(res.is_ok(), "command {exit_cmd} should exit cleanly");
    }
}

#[tokio::test]
async fn test_shell_help_meta_command() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");

    let (client, _c_handle) = connect_client(&addr.to_string(), 30)
        .await
        .expect("connect");

    let input = Cursor::new(b"\\?\n\\q\n");
    let mut output = Vec::new();
    run_shell_with_io(&client, input, &mut output)
        .await
        .expect("run shell");

    let out_str = String::from_utf8_lossy(&output);
    assert!(out_str.contains("General help:"));
    assert!(out_str.contains("\\q              Quit the shell"));
}

#[tokio::test]
async fn test_shell_multiline_statement_buffering_and_execution() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");

    let (client, _c_handle) = connect_client(&addr.to_string(), 30)
        .await
        .expect("connect");

    client
        .simple_query("CREATE TABLE sales (store_id BIGINT, total_amount BIGINT);")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO sales VALUES (100, 250);")
        .await
        .expect("insert");

    let input =
        Cursor::new(b"SELECT\n  store_id,\n  total_amount\nFROM sales\nORDER BY store_id;\n\\q\n");
    let mut output = Vec::new();
    run_shell_with_io(&client, input, &mut output)
        .await
        .expect("run shell");

    let out_str = String::from_utf8_lossy(&output);
    assert!(out_str.contains("store_id | total_amount"));
    assert!(out_str.contains("100      | 250"));
    assert!(out_str.contains("(1 row)"));
}

#[tokio::test]
async fn test_shell_recovers_from_error_and_executes_subsequent_statement() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");

    let (client, _c_handle) = connect_client(&addr.to_string(), 30)
        .await
        .expect("connect");

    let input = Cursor::new(
        b"UPDATE nonexistent SET val = 1 RETURNING;\nSELECT 999 AS valid_number;\n\\q\n",
    );
    let mut output = Vec::new();
    run_shell_with_io(&client, input, &mut output)
        .await
        .expect("run shell");

    let out_str = String::from_utf8_lossy(&output);
    // Should have reported an error
    assert!(out_str.contains("Error"));
    // And then successfully executed the next query
    assert!(out_str.contains("valid_number"));
    assert!(out_str.contains("999"));
}

#[tokio::test]
async fn test_shell_statement_buffer_overflow_resets_safely() {
    let dir = TempDir::new().expect("tempdir");
    let opts = test_gateway_opts(&dir);
    let (addr, _handle) = start_gateway(&opts).await.expect("start gateway");

    let (client, _c_handle) = connect_client(&addr.to_string(), 30)
        .await
        .expect("connect");

    // Over 64 KB without a semicolon
    let large_line = "A".repeat(65 * 1024) + "\n";
    let combined = format!("{large_line}SELECT 777 AS lucky;\n\\q\n");

    let input = Cursor::new(combined.as_bytes());
    let mut output = Vec::new();
    run_shell_with_io(&client, input, &mut output)
        .await
        .expect("run shell");

    let out_str = String::from_utf8_lossy(&output);
    assert!(out_str.contains("RS-0005: shell statement buffer exceeded maximum limit"));
    assert!(out_str.contains("lucky"));
    assert!(out_str.contains("777"));
}

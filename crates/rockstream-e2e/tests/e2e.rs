use std::fs;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers::{core::Mount, GenericImage, ImageExt};

use rockstream_e2e::ensure_image_built;

// Helper to wait for container exit and return exit code
async fn wait_container_exit(container_id: &str) -> i32 {
    let output = Command::new("docker")
        .args(["wait", container_id])
        .output()
        .expect("failed to run docker wait");
    let code_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    code_str.parse::<i32>().unwrap_or(-1)
}

// Helper to get stdout and stderr logs of a container
fn get_container_logs(container_id: &str) -> (String, String) {
    let stdout_output = Command::new("docker")
        .args(["logs", container_id])
        .output()
        .expect("failed to run docker logs");
    let stdout = String::from_utf8_lossy(&stdout_output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&stdout_output.stderr).to_string();
    (stdout, stderr)
}

// Helper to get IP of a container on the bridge network
fn get_container_ip(container_id: &str) -> String {
    let output = Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container_id,
        ])
        .output()
        .expect("failed to inspect container IP");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

// Helper to assert help output of subcommands
async fn assert_help(args: Vec<&str>, expected_substring: &str) {
    ensure_image_built();

    let image = GenericImage::new("rockstream", "test")
        .with_cmd(args.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    let container = image.start().await.unwrap();
    let id = container.id().to_string();

    let exit_code = wait_container_exit(&id).await;
    let (stdout, stderr) = get_container_logs(&id);

    assert_eq!(
        exit_code, 0,
        "CLI command {args:?} failed. Stderr: {stderr}"
    );
    let combined = format!("{stdout}\n{stderr}");
    assert!(
        combined.contains(expected_substring),
        "Output of {args:?} did not contain '{expected_substring}'. Output:\n{combined}"
    );
}

#[tokio::test]
async fn test_cli_help_snapshots() {
    assert_help(vec!["--help"], "Usage: rockstream").await;
    assert_help(vec!["bootstrap", "--help"], "Bootstrap a new cluster").await;
    assert_help(vec!["sql", "--help"], "Run a SQL statement").await;
    assert_help(vec!["describe", "--help"], "Describe a pipeline").await;
    assert_help(vec!["debug-arrangement", "--help"], "Debug an arrangement").await;
    assert_help(
        vec!["support-bundle", "--help"],
        "Generate a support bundle",
    )
    .await;
    assert_help(vec!["tune", "--help"], "Tune the auto-tuner").await;
}

#[tokio::test]
async fn test_start_role_matrix_all() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    let image = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=all".to_string(),
            "--storage=/data".to_string(),
        ]);

    let container = image.start().await.unwrap();
    let id = container.id().to_string();

    let exit_code = wait_container_exit(&id).await;
    let (_stdout, stderr) = get_container_logs(&id);

    assert_eq!(
        exit_code, 0,
        "role=all exited with non-zero. Stderr: {stderr}"
    );

    // Verify audit.jsonl was written and contains expected events
    let audit_path = temp_dir.path().join("audit.jsonl");
    assert!(audit_path.exists(), "audit.jsonl not found in storage");
    let audit_content = fs::read_to_string(&audit_path).unwrap();
    assert!(audit_content.contains("server.started"));
    assert!(audit_content.contains("server.stopped"));

    // Verify support bundle was written and contains required JSON keys
    let mut found_bundle = false;
    for entry in fs::read_dir(temp_dir.path()).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.starts_with("support-bundle-") && name.ends_with(".json") {
            found_bundle = true;
            let content = fs::read_to_string(entry.path()).unwrap();
            let json: serde_json::Value = serde_json::from_str(&content).unwrap();
            assert!(
                json.get("audit_events").is_some(),
                "missing audit_events in bundle"
            );
            assert!(
                json.get("system_info").is_some(),
                "missing system_info in bundle"
            );
        }
    }
    assert!(found_bundle, "support bundle JSON file not found");
}

#[tokio::test]
async fn test_start_role_matrix_worker_missing_control() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    let image = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            "--storage=/data".to_string(),
        ]);

    let container = image.start().await.unwrap();
    let id = container.id().to_string();

    let exit_code = wait_container_exit(&id).await;
    let (stdout, stderr) = get_container_logs(&id);

    assert_ne!(
        exit_code, 0,
        "role=worker should have failed without control"
    );
    let combined = format!("{stdout}\n{stderr}");
    assert!(
        combined.contains("RS-0011")
            || combined.contains("control")
            || combined.contains("required"),
        "error message did not mention control requirement: {combined}"
    );
}

#[tokio::test]
async fn test_start_role_matrix_gateway_and_frontier() {
    ensure_image_built();

    for role in &["gateway", "frontier"] {
        let temp_dir = TempDir::new().unwrap();
        let host_path = temp_dir.path().to_str().unwrap();

        let image = GenericImage::new("rockstream", "test")
            .with_mount(Mount::bind_mount(host_path, "/data"))
            .with_cmd(vec![
                "start".to_string(),
                format!("--role={}", role),
                "--storage=/data".to_string(),
            ]);

        let container = image.start().await.unwrap();
        let id = container.id().to_string();

        let exit_code = wait_container_exit(&id).await;
        let (_stdout, stderr) = get_container_logs(&id);

        assert_eq!(exit_code, 0, "role={role} failed. Stderr: {stderr}");

        // Verify audit log
        let audit_path = temp_dir.path().join("audit.jsonl");
        assert!(audit_path.exists());
        let audit_content = fs::read_to_string(&audit_path).unwrap();
        assert!(audit_content.contains("server.started"));
    }
}

#[tokio::test]
async fn test_bootstrap_flow() {
    ensure_image_built();

    // 1. Bootstrap fails when control is absent
    let image_fail = GenericImage::new("rockstream", "test").with_cmd(vec![
        "bootstrap".to_string(),
        "--control=127.0.0.1:9999".to_string(),
    ]);
    let container_fail = image_fail.start().await.unwrap();
    let id_fail = container_fail.id().to_string();
    let exit_code_fail = wait_container_exit(&id_fail).await;
    let (stdout_fail, stderr_fail) = get_container_logs(&id_fail);
    assert_ne!(exit_code_fail, 0);
    let combined_fail = format!("{stdout_fail}\n{stderr_fail}");
    assert!(
        combined_fail.contains("RS-0013") || combined_fail.contains("connect"),
        "error message did not mention connection failure: {combined_fail}"
    );

    // 2. Bootstrap succeeds when control is present
    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    let image_control = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=control".to_string(),
            "--control-bind=0.0.0.0:7700".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container_control = image_control.start().await.unwrap();
    let control_id = container_control.id().to_string();

    let control_ip = get_container_ip(&control_id);
    assert!(
        !control_ip.is_empty() && control_ip != "invalid IP",
        "failed to get control container IP: {control_ip}"
    );

    // Wait a brief moment to let control start listening
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Run bootstrap pointing to control container IP
    let image_bootstrap = GenericImage::new("rockstream", "test").with_cmd(vec![
        "bootstrap".to_string(),
        format!("--control={}:7700", control_ip),
    ]);
    let container_bootstrap = image_bootstrap.start().await.unwrap();
    let bootstrap_id = container_bootstrap.id().to_string();

    let exit_code_bootstrap = wait_container_exit(&bootstrap_id).await;
    let (stdout_boot, stderr_boot) = get_container_logs(&bootstrap_id);

    assert_eq!(
        exit_code_bootstrap, 0,
        "bootstrap command failed. Stderr: {stderr_boot}"
    );
    let combined_boot = format!("{stdout_boot}\n{stderr_boot}");
    assert!(
        combined_boot.contains("Bootstrap probe registered")
            || combined_boot.contains("Control service is reachable"),
        "bootstrap output was unexpected: {combined_boot}"
    );
}

#[tokio::test]
async fn test_error_path_contract_tls() {
    ensure_image_built();

    // Invalid combination of TLS flags (providing --tls-cert but omitting others)
    let image = GenericImage::new("rockstream", "test").with_cmd(vec![
        "start".to_string(),
        "--role=all".to_string(),
        "--tls-cert=/data/cert.pem".to_string(),
    ]);
    let container = image.start().await.unwrap();
    let id = container.id().to_string();

    let exit_code = wait_container_exit(&id).await;
    let (stdout, stderr) = get_container_logs(&id);

    assert_ne!(exit_code, 0, "invalid TLS flags should have caused failure");
    let combined = format!("{stdout}\n{stderr}");
    assert!(
        combined.contains("RS-0010") || combined.contains("must all be provided together"),
        "error did not contain expected TLS error code RS-0010: {combined}"
    );
}

#[tokio::test]
async fn test_rolling_upgrade_smoke() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // 1. Run role=all first time
    {
        let image = GenericImage::new("rockstream", "test")
            .with_mount(Mount::bind_mount(host_path, "/data"))
            .with_cmd(vec![
                "start".to_string(),
                "--role=all".to_string(),
                "--storage=/data".to_string(),
            ]);

        let container = image.start().await.unwrap();
        let id = container.id().to_string();
        let exit_code = wait_container_exit(&id).await;
        assert_eq!(exit_code, 0);

        let audit_path = temp_dir.path().join("audit.jsonl");
        assert!(audit_path.exists());
        let audit_content = fs::read_to_string(&audit_path).unwrap();
        assert!(audit_content.contains("server.started"));
        assert!(audit_content.contains("server.stopped"));
    }

    // 2. Restart using same storage directory and verify it starts and appends cleanly
    {
        let image = GenericImage::new("rockstream", "test")
            .with_mount(Mount::bind_mount(host_path, "/data"))
            .with_cmd(vec![
                "start".to_string(),
                "--role=all".to_string(),
                "--storage=/data".to_string(),
            ]);

        let container = image.start().await.unwrap();
        let id = container.id().to_string();
        let exit_code = wait_container_exit(&id).await;
        assert_eq!(exit_code, 0);

        let audit_path = temp_dir.path().join("audit.jsonl");
        assert!(audit_path.exists());
        let audit_content = fs::read_to_string(&audit_path).unwrap();
        // Should have two starts and two stops
        let starts = audit_content.matches("server.started").count();
        let stops = audit_content.matches("server.stopped").count();
        assert_eq!(
            starts, 2,
            "audit log should contain 2 server.started events"
        );
        assert_eq!(stops, 2, "audit log should contain 2 server.stopped events");
    }
}

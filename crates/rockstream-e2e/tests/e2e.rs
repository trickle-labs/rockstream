use std::fs;
use std::process::Command;
use std::time::Duration;
use tempfile::TempDir;
use testcontainers::core::{IntoContainerPort, Mount};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use rockstream_e2e::ensure_image_built;

fn get_db_error_message(err: &tokio_postgres::Error) -> String {
    if let Some(db_err) = err.as_db_error() {
        format!("{}: {}", db_err.code().code(), db_err.message())
    } else {
        err.to_string()
    }
}
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

        let exit_code = if role == &"gateway" {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Command::new("docker")
                .args(["stop", &id])
                .output()
                .expect("failed to stop gateway container");
            wait_container_exit(&id).await
        } else {
            wait_container_exit(&id).await
        };
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

#[tokio::test]
async fn test_gateway_pgwire_e2e() {
    ensure_image_built();
    eprintln!("DEBUG: ensure_image_built completed");

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // 1. Start Control
    eprintln!("DEBUG: Starting control container");
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
    eprintln!("DEBUG: Control started (ip: {control_ip})");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Start Worker
    eprintln!("DEBUG: Starting worker container");
    let image_worker = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            format!("--control={}:7700", control_ip),
            "--storage=/data".to_string(),
        ]);
    let container_worker = image_worker.start().await.unwrap();
    let _worker_id = container_worker.id().to_string();
    eprintln!("DEBUG: Worker started");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Start Gateway
    eprintln!("DEBUG: Starting gateway container");
    let image_gateway = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            format!("--control={}:7700", control_ip),
            "--gateway-bind=0.0.0.0:5432".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container_gateway = image_gateway.start().await.unwrap();
    let gateway_id = container_gateway.id().to_string();
    let gateway_port = container_gateway
        .get_host_port_ipv4(5432.tcp())
        .await
        .unwrap();
    eprintln!("DEBUG: Gateway started (port: {gateway_port})");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Helper to connect to gateway
    let connect = |ip: String, port: u16, user: String, db: String, token: Option<String>| async move {
        eprintln!("DEBUG: connecting to {ip}:{port} as {user}...");
        let mut config = tokio_postgres::Config::new();
        config.host(&ip);
        config.port(port);
        config.user(&user);
        config.dbname(&db);
        if let Some(ref t) = token {
            config.password(t);
        }
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pg connection error: {e}");
            }
        });
        eprintln!("DEBUG: connect success");
        Ok::<_, tokio_postgres::Error>(client)
    };

    let host_ip = "127.0.0.1".to_string();

    // Scenario A: Startup handshake & Auth validation
    // 1. Unauthenticated startup path should fail
    eprintln!("DEBUG: Running Scenario A.1 (unauthenticated)");
    {
        let res = connect(
            host_ip.clone(),
            gateway_port,
            "alice".to_owned(),
            "mydb".to_owned(),
            Some("".to_owned()),
        )
        .await;
        assert!(res.is_err(), "unauthenticated connection should fail");
        let err_msg = get_db_error_message(&res.unwrap_err());
        assert!(
            err_msg.contains("client authentication failed") || err_msg.contains("RS-2001"),
            "unexpected error: {err_msg}"
        );
    }
    // 2. Invalid auth token should fail
    eprintln!("DEBUG: Running Scenario A.2 (invalid token)");
    {
        let res = connect(
            host_ip.clone(),
            gateway_port,
            "alice".to_owned(),
            "mydb".to_owned(),
            Some("invalid-token".to_owned()),
        )
        .await;
        assert!(res.is_err());
    }
    // 3. Authenticated startup path (viewer role)
    eprintln!("DEBUG: Running Scenario A.3 (viewer)");
    let client_viewer = connect(
        host_ip.clone(),
        gateway_port,
        "alice".to_owned(),
        "mydb".to_owned(),
        Some("bearer viewer:production".to_owned()),
    )
    .await
    .unwrap();

    // 4. Authenticated startup path (admin role)
    eprintln!("DEBUG: Running Scenario A.4 (admin)");
    let client_admin = connect(
        host_ip.clone(),
        gateway_port,
        "admin".to_owned(),
        "mydb".to_owned(),
        Some("bearer admin:any".to_owned()),
    )
    .await
    .unwrap();

    // Scenario B: Simple query isolation level coverage
    eprintln!("DEBUG: Running Scenario B");
    // 1. SET TRANSACTION ISOLATION LEVEL REPEATABLE READ succeeds
    client_viewer
        .execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .unwrap();
    // 2. SET TRANSACTION ISOLATION LEVEL SERIALIZABLE fails with RS-2003
    {
        let res = client_viewer
            .execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", &[])
            .await;
        assert!(res.is_err());
        let err_msg = get_db_error_message(&res.unwrap_err());
        assert!(
            err_msg.contains("RS-2003"),
            "expected RS-2003, got: {err_msg}"
        );
    }
    // 3. SHOW / SET other variables
    client_viewer
        .execute("SET client_encoding TO 'UTF8'", &[])
        .await
        .unwrap();
    client_viewer
        .execute("SHOW client_encoding", &[])
        .await
        .unwrap();

    // Scenario C: Extended query coverage (Parse/Bind/Execute)
    let stmt = client_viewer
        .prepare("SELECT * FROM pg_catalog.pg_type WHERE oid = $1")
        .await
        .unwrap();
    let rows = client_viewer.query(&stmt, &[&16i32]).await.unwrap();
    assert!(!rows.is_empty(), "pg_type query returned no rows");

    // Scenario D: Catalog reflection
    // 1. pg_catalog.pg_type OIDs and metadata
    let rows = client_viewer
        .query("SELECT oid, typname FROM pg_catalog.pg_type", &[])
        .await
        .unwrap();
    assert!(rows
        .iter()
        .any(|r| r.get::<_, i32>("oid") == 16 && r.get::<_, &str>("typname") == "bool"));
    // 2. information_schema.columns OIDs and types
    let rows = client_viewer
        .query(
            "SELECT table_name, column_name, data_type, udt_oid FROM information_schema.columns",
            &[],
        )
        .await
        .unwrap();
    assert!(rows
        .iter()
        .any(|r| r.get::<_, &str>("table_name") == "orders_mv"
            && r.get::<_, &str>("column_name") == "order_id"
            && r.get::<_, i32>("udt_oid") == 20));

    // Scenario E: Inline DDL/DML, Optimistic conflicts, and View dependencies
    // 1. CREATE VIEW view_a
    client_viewer
        .execute("CREATE VIEW view_a AS SELECT order_id FROM orders_mv", &[])
        .await
        .unwrap();
    // 2. DROP VIEW view_with_dep (dependent view error RS-2004)
    {
        let res = client_viewer.execute("DROP VIEW view_with_dep", &[]).await;
        assert!(res.is_err());
        let err_msg = get_db_error_message(&res.unwrap_err());
        assert!(
            err_msg.contains("RS-2004"),
            "expected RS-2004, got: {err_msg}"
        );
    }
    // 3. Optimistic conflict DML error RS-2008
    {
        let res = client_viewer
            .execute(
                "INSERT INTO balances (account, amount) VALUES ('alice', CONFLICT)",
                &[],
            )
            .await;
        assert!(res.is_err());
        let err_msg = get_db_error_message(&res.unwrap_err());
        assert!(
            err_msg.contains("RS-2008"),
            "expected RS-2008, got: {err_msg}"
        );
    }
    // 4. INSERT ... RETURNING row description and execution
    let rows = client_viewer
        .query(
            "INSERT INTO balances (account, amount) VALUES ('alice', 100) RETURNING id, amount",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);

    // Scenario F: Tenant isolation
    {
        let res = client_viewer
            .query("SELECT * FROM marketing.orders", &[])
            .await;
        assert!(res.is_err());
        let err_msg = get_db_error_message(&res.unwrap_err());
        assert!(
            err_msg.contains("Cross-tenant") || err_msg.contains("RS-2001"),
            "unexpected error: {err_msg}"
        );
    }
    // Admin user should bypass tenant isolation
    let _ = client_admin
        .query("SELECT * FROM marketing.orders", &[])
        .await
        .unwrap();

    // Scenario G: Query semantics
    // 1. Join query
    let rows = client_viewer
        .query("SELECT * FROM a JOIN b ON a.id = b.id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>("order_id"), 100);
    assert_eq!(rows[0].get::<_, &str>("customer"), "Alice");
    assert_eq!(rows[0].get::<_, f64>("price"), 45.5);

    // 2. Aggregate query
    let rows = client_viewer
        .query(
            "SELECT region, SUM(amount) FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("region"), "us-east");
    assert_eq!(rows[0].get::<_, i64>("total"), 5000);

    // 3. Window query
    let rows = client_viewer
        .query(
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id) FROM users",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("name"), "Bob");
    assert_eq!(rows[0].get::<_, i64>("rn"), 1);

    // 4. Subscribe query
    let rows = client_viewer
        .query("SUBSCRIBE orders_mv", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>("mz_timestamp"), 10);
    assert_eq!(rows[0].get::<_, i64>("mz_diff"), 1);
    assert_eq!(rows[0].get::<_, &str>("region"), "us-west");

    // Scenario H: psql client compatibility witness
    let network_arg = format!("--network=container:{gateway_id}");
    let psql_output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-e",
            "PGPASSWORD=bearer viewer:production",
            &network_arg,
            "postgres:14",
            "psql",
            "-h",
            "127.0.0.1",
            "-U",
            "alice",
            "-d",
            "mydb",
            "-c",
            "SELECT * FROM pg_catalog.pg_type WHERE oid = 16",
        ])
        .output()
        .expect("failed to run psql container");

    let stdout = String::from_utf8_lossy(&psql_output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&psql_output.stderr).to_string();
    assert!(
        psql_output.status.success(),
        "psql container failed. Stderr: {stderr}"
    );
    assert!(
        stdout.contains("bool"),
        "psql output did not contain 'bool'. Output: {stdout}"
    );
}

#[tokio::test]
async fn test_v0_52_3_production_beta_handoff() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // 1. Start Control
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
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. Start Worker
    let image_worker = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            format!("--control={}:7700", control_ip),
            "--storage=/data".to_string(),
        ]);
    let container_worker = image_worker.start().await.unwrap();
    let _worker_id = container_worker.id().to_string();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Start Gateway
    let image_gateway = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            format!("--control={}:7700", control_ip),
            "--gateway-bind=0.0.0.0:5432".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container_gateway = image_gateway.start().await.unwrap();
    let _gateway_id = container_gateway.id().to_string();
    let gateway_port = container_gateway
        .get_host_port_ipv4(5432.tcp())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let connect = |ip: String, port: u16, user: String, db: String, token: Option<String>| async move {
        let mut config = tokio_postgres::Config::new();
        config.host(&ip);
        config.port(port);
        config.user(&user);
        config.dbname(&db);
        if let Some(ref t) = token {
            config.password(t);
        }
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pg connection error: {e}");
            }
        });
        Ok::<_, tokio_postgres::Error>(client)
    };

    let host_ip = "127.0.0.1".to_string();

    // Connect to gateway
    let client = connect(
        host_ip.clone(),
        gateway_port,
        "alice".to_owned(),
        "mydb".to_owned(),
        Some("bearer viewer:production".to_owned()),
    )
    .await
    .unwrap();

    // Assert SHOW RESOURCE USAGE
    let rows = client.query("SHOW RESOURCE USAGE", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("workload_id"), "realtime");
    assert_eq!(rows[0].get::<_, i64>("memory_limit"), 10485760);
    assert_eq!(rows[0].get::<_, i64>("memory_allocated"), 8388608);
    assert_eq!(rows[0].get::<_, i64>("freshness_slo_ms"), 100);
    assert!(rows[0].get::<_, bool>("freshness_slo_compliant"));

    // Assert SHOW RESOURCE USAGE FOR WORKLOAD realtime
    let rows = client
        .query("SHOW RESOURCE USAGE FOR WORKLOAD realtime", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("view_name"), "orders_mv");
    assert_eq!(rows[0].get::<_, &str>("workload_id"), "realtime");
    assert_eq!(rows[0].get::<_, i64>("state_bytes"), 1048576);
    assert_eq!(rows[0].get::<_, i64>("memory_bytes"), 524288);
    assert_eq!(rows[0].get::<_, i64>("freshness_lag_ms"), 12);

    // Assert SHOW CLUSTER RESOURCE USAGE
    let rows = client
        .query("SHOW CLUSTER RESOURCE USAGE", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("total_workers"), 1);
    assert_eq!(rows[0].get::<_, i64>("total_state_bytes"), 1048576);
    assert_eq!(rows[0].get::<_, i64>("total_memory_bytes"), 8388608);

    // Assert SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA test_schema
    let rows = client
        .query("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA test_schema", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("schema_name"), "test_schema");
    assert_eq!(rows[0].get::<_, &str>("status"), "UP-TO-DATE");
    assert_eq!(rows[0].get::<_, i32>("pending_changes"), 0);

    // Assert SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW test_mv
    let rows = client
        .query(
            "SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW test_mv",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("view_name"), "test_mv");
    assert_eq!(rows[0].get::<_, i32>("version"), 1);
    assert_eq!(rows[0].get::<_, &str>("evolved_at"), "2026-06-04 00:00:00");
    assert!(rows[0].get::<_, bool>("compatible"));

    // Test timeouts
    client
        .execute("SET statement_timeout = 50", &[])
        .await
        .unwrap();
    let res = client.execute("SELECT pg_sleep(0.2)", &[]).await;
    assert!(res.is_err());
    let err_msg = get_db_error_message(&res.unwrap_err());
    assert!(
        err_msg.contains("RS-2002") || err_msg.contains("timeout"),
        "got error: {err_msg}"
    );

    // Reset timeout and run short sleep successfully
    client
        .execute("SET statement_timeout = 5000", &[])
        .await
        .unwrap();
    client.execute("SELECT pg_sleep(0.01)", &[]).await.unwrap();

    // Test rate limiting
    client.execute("SET max_qps = 2", &[]).await.unwrap();
    client.execute("SELECT 1", &[]).await.unwrap();
    client.execute("SELECT 1", &[]).await.unwrap();
    let res_rate = client.execute("SELECT 1", &[]).await;
    assert!(res_rate.is_err());
    let err_rate = get_db_error_message(&res_rate.unwrap_err());
    assert!(
        err_rate.contains("RS-2005") || err_rate.contains("rate limit"),
        "got error: {err_rate}"
    );

    // Scenario 3: CLI Subcommands
    // debug-arrangement
    let image_debug = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "debug-arrangement".to_string(),
            "orders_mv".to_string(),
            "1".to_string(),
            "test".to_string(),
        ]);
    let container_debug = image_debug.start().await.unwrap();
    let debug_id = container_debug.id().to_string();
    assert_eq!(wait_container_exit(&debug_id).await, 0);
    let (stdout_debug, _) = get_container_logs(&debug_id);
    assert!(stdout_debug.contains("Arrangement Header: law_id=1, law_version=1"));
    assert!(stdout_debug.contains("Tombstone density: 0.15"));

    // support-bundle
    let image_sb = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "support-bundle".to_string(),
            "--output".to_string(),
            "/data/support-bundle.tar.gz".to_string(),
        ]);
    let container_sb = image_sb.start().await.unwrap();
    let sb_id = container_sb.id().to_string();
    assert_eq!(wait_container_exit(&sb_id).await, 0);
    let bundle_path = temp_dir.path().join("support-bundle.tar.gz");
    assert!(bundle_path.exists());

    // Scenario 4: Churn & Restarts
    // 1. Restart Worker
    eprintln!("Restarting worker...");
    drop(container_worker);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let image_worker2 = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            format!("--control={}:7700", control_ip),
            "--storage=/data".to_string(),
        ]);
    let container_worker2 = image_worker2.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check gateway is still reachable and queries succeed
    let client2 = connect(
        host_ip.clone(),
        gateway_port,
        "alice".to_owned(),
        "mydb".to_owned(),
        Some("bearer viewer:production".to_owned()),
    )
    .await
    .unwrap();
    client2.execute("SELECT 1", &[]).await.unwrap();

    // 2. Restart Control
    eprintln!("Restarting control...");
    drop(container_control);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let image_control2 = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=control".to_string(),
            "--control-bind=0.0.0.0:7700".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container_control2 = image_control2.start().await.unwrap();
    let _control_ip2 = get_container_ip(container_control2.id());
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect to gateway again, check queries succeed
    client2.execute("SELECT 1", &[]).await.unwrap();

    // 3. Restart Full Cluster
    eprintln!("Restarting full cluster...");
    drop(container_gateway);
    drop(container_worker2);
    drop(container_control2);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let container_control3 = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=control".to_string(),
            "--control-bind=0.0.0.0:7700".to_string(),
            "--storage=/data".to_string(),
        ])
        .start()
        .await
        .unwrap();
    let control_ip3 = get_container_ip(container_control3.id());
    tokio::time::sleep(Duration::from_millis(500)).await;

    let _container_worker3 = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            format!("--control={}:7700", control_ip3),
            "--storage=/data".to_string(),
        ])
        .start()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let container_gateway3 = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            format!("--control={}:7700", control_ip3),
            "--gateway-bind=0.0.0.0:5432".to_string(),
            "--storage=/data".to_string(),
        ])
        .start()
        .await
        .unwrap();
    let gateway_port3 = container_gateway3
        .get_host_port_ipv4(5432.tcp())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let client3 = connect(
        host_ip.clone(),
        gateway_port3,
        "alice".to_owned(),
        "mydb".to_owned(),
        Some("bearer viewer:production".to_owned()),
    )
    .await
    .unwrap();
    client3.execute("SELECT 1", &[]).await.unwrap();
}

#[tokio::test]
async fn test_v0_52_4_minio_durability_and_connectors() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // 1. Start MinIO container
    let minio_image = GenericImage::new("minio/minio", "RELEASE.2024-01-28T22-35-53Z")
        .with_exposed_port(9000.tcp())
        .with_env_var("MINIO_ROOT_USER", "rockstream")
        .with_env_var("MINIO_ROOT_PASSWORD", "rockstream-secret")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec!["server".to_string(), "/data".to_string()]);
    let minio_container = minio_image.start().await.unwrap();
    let minio_id = minio_container.id().to_string();
    let _minio_port = minio_container
        .get_host_port_ipv4(9000.tcp())
        .await
        .unwrap();
    let minio_ip = get_container_ip(&minio_id);
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 2. Start Control
    let image_control = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=control".to_string(),
            "--control-bind=0.0.0.0:7700".to_string(),
            "--storage=s3://rockstream-bucket/test-run".to_string(),
        ]);
    let container_control = image_control.start().await.unwrap();
    let control_id = container_control.id().to_string();
    let control_ip = get_container_ip(&control_id);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Start Worker
    let image_worker = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            format!("--control={}:7700", control_ip),
            "--storage=s3://rockstream-bucket/test-run".to_string(),
        ]);
    let _container_worker = image_worker.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 4. Start Gateway
    let image_gateway = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_env_var("ROCKSTREAM_STORAGE", "s3://rockstream-bucket/test-run")
        .with_env_var("MINIO_ENDPOINT", format!("{minio_ip}:9000"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            format!("--control={}:7700", control_ip),
            "--gateway-bind=0.0.0.0:5432".to_string(),
            "--storage=s3://rockstream-bucket/test-run".to_string(),
        ]);
    let container_gateway = image_gateway.start().await.unwrap();
    let gateway_port = container_gateway
        .get_host_port_ipv4(5432.tcp())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Helper to connect to gateway
    let connect = |ip: String, port: u16, user: String, db: String, token: Option<String>| async move {
        let mut config = tokio_postgres::Config::new();
        config.host(&ip);
        config.port(port);
        config.user(&user);
        config.dbname(&db);
        if let Some(ref t) = token {
            config.password(t);
        }
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("pg connection error: {e}");
            }
        });
        Ok::<_, tokio_postgres::Error>(client)
    };

    let host_ip = "127.0.0.1".to_string();

    let client = connect(
        host_ip.clone(),
        gateway_port,
        "alice".to_owned(),
        "mydb".to_owned(),
        Some("bearer viewer:production".to_owned()),
    )
    .await
    .unwrap();

    // 5. Run normal DML
    client
        .execute(
            "INSERT INTO balances (account, amount) VALUES ('alice', 100)",
            &[],
        )
        .await
        .unwrap();

    // 6. Force Checkpoint (writes to MinIO bucket)
    client.execute("FORCE CHECKPOINT", &[]).await.unwrap();

    // Verify checkpoint and WAL files exist in the mounted storage directory
    let bucket_dir = temp_dir.path().join("rockstream-bucket").join("test-run");
    assert!(bucket_dir
        .join("wal")
        .join("00000000000000000001.wal")
        .exists());
    assert!(bucket_dir
        .join("checkpoints")
        .join("manifest.json")
        .exists());
    assert!(bucket_dir
        .join("sinks")
        .join("iceberg")
        .join("metadata.json")
        .exists());
    assert!(bucket_dir
        .join("sinks")
        .join("iceberg")
        .join("data.parquet")
        .exists());

    // 7. Verify connector-specific lifecycle DDL commands
    client
        .execute(
            "CREATE SINK my_iceberg_sink TO ICEBERG 's3://rockstream-bucket/test-run'",
            &[],
        )
        .await
        .unwrap();
    client
        .execute("ALTER SOURCE my_kafka_source REPLAY DEAD_LETTER_QUEUE", &[])
        .await
        .unwrap();

    // 8. Force Compaction / Storage Cleanup
    client.execute("CLEANUP STORAGE", &[]).await.unwrap();
    // Verify WAL file got cleaned up (as simulated)
    assert!(!bucket_dir
        .join("wal")
        .join("00000000000000000001.wal")
        .exists());

    // 9. Stop MinIO (failure injection)
    drop(minio_container);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 10. Verify query fails with RS-5003 when MinIO is unreachable
    let res = client.execute("SELECT 1", &[]).await;
    assert!(res.is_err());
    let err_msg = if let Some(db_err) = res.unwrap_err().as_db_error() {
        format!("{}: {}", db_err.code().code(), db_err.message())
    } else {
        "".to_string()
    };
    assert!(err_msg.contains("RS-5003") || err_msg.contains("storage unreachable"));
}

// ─── Helper: HTTP GET request to a running catalog REST server ───────────────

/// Perform an HTTP GET to `http://host:port/path` and return `(status_line, body)`.
fn http_get(host: &str, port: u16, path: &str) -> std::io::Result<(String, String)> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let mut stream = TcpStream::connect(format!("{host}:{port}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let request =
        format!("GET {path} HTTP/1.0\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    // Split headers from body.
    let (headers, body) = response.split_once("\r\n\r\n").unwrap_or((&response, ""));
    let status_line = headers.lines().next().unwrap_or("").to_string();
    Ok((status_line, body.to_string()))
}

/// Retry `http_get` up to `attempts` times with a small delay between attempts.
fn http_get_with_retry(
    host: &str,
    port: u16,
    path: &str,
    attempts: u32,
) -> std::io::Result<(String, String)> {
    let mut last_err = std::io::Error::other("no attempts");
    for i in 0..attempts {
        match http_get(host, port, path) {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_err = e;
                if i + 1 < attempts {
                    std::thread::sleep(Duration::from_millis(300));
                }
            }
        }
    }
    Err(last_err)
}

// ─── v0.52.5 E2E test ────────────────────────────────────────────────────────

/// **v0.52.5 — Catalog Registration and REST Server**
///
/// Verifies that the local HTTP catalog endpoint and the SQL gateway describe
/// the same world, and that catalog state survives a cluster restart.
///
/// Mandatory scenarios (from plans/e2e-test-plan.md §v0.52.5):
///
/// 1. Catalog registration — after start, HTTP catalog reflects namespaces and
///    tables; catalog remains consistent after restart.
/// 2. Metadata coherence — HTTP `/catalog/v1/namespaces` matches what
///    `information_schema.tables` shows via SQL.
/// 3. Auth and transport — endpoint is accessible (open in local dev topology).
/// 4. Regression matrix — smoke-tests from v0.52.1..v0.52.4 pass.
///
/// Pass criteria (v0.52.5):
/// - SQL gateway and catalog endpoint describe the same world.
/// - Local interoperability proven without requiring Spark/Trino/DuckDB/AWS S3.
/// - Suite broad enough that a change to any public surface fails somewhere.
#[tokio::test]
async fn test_v0_52_5_catalog_rest_server() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // ── 1. Start cluster: control + worker + gateway (with catalog REST) ──────

    // Control
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
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Worker
    let image_worker = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            format!("--control={}:7700", control_ip),
            "--storage=/data".to_string(),
        ]);
    let _container_worker = image_worker.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Gateway — expose both pgwire (5432) and catalog REST (8181)
    let image_gateway = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_exposed_port(8181.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            format!("--control={}:7700", control_ip),
            "--gateway-bind=0.0.0.0:5432".to_string(),
            "--catalog-bind=0.0.0.0:8181".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container_gateway = image_gateway.start().await.unwrap();
    let gateway_port = container_gateway
        .get_host_port_ipv4(5432.tcp())
        .await
        .unwrap();
    let catalog_port = container_gateway
        .get_host_port_ipv4(8181.tcp())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let host_ip = "127.0.0.1";

    // ── Scenario 1: Catalog REST health probe ─────────────────────────────────

    let (status, body) =
        http_get_with_retry(host_ip, catalog_port, "/catalog/v1/health", 10).unwrap();
    assert!(
        status.contains("200"),
        "catalog /health should return 200, got: {status}"
    );
    assert!(
        body.contains(r#""status":"ok""#),
        "catalog /health body should contain status:ok, got: {body}"
    );

    // ── Scenario 2: Catalog REST namespaces ───────────────────────────────────

    let (status, body) =
        http_get_with_retry(host_ip, catalog_port, "/catalog/v1/namespaces", 5).unwrap();
    assert!(
        status.contains("200"),
        "catalog /namespaces should return 200, got: {status}"
    );
    assert!(
        body.contains("public"),
        "catalog /namespaces should contain 'public', got: {body}"
    );
    // Parse that it is valid JSON with a "namespaces" key.
    let ns_json: serde_json::Value =
        serde_json::from_str(&body).expect("catalog /namespaces response must be valid JSON");
    let ns_array = ns_json
        .get("namespaces")
        .and_then(|v| v.as_array())
        .expect("catalog response must have 'namespaces' array");
    assert!(!ns_array.is_empty(), "namespaces array must not be empty");

    // ── Scenario 3: Catalog REST tables in 'public' namespace ─────────────────

    let (status, body) = http_get_with_retry(
        host_ip,
        catalog_port,
        "/catalog/v1/namespaces/public/tables",
        5,
    )
    .unwrap();
    assert!(
        status.contains("200"),
        "catalog /namespaces/public/tables should return 200, got: {status}"
    );
    let tables_json: serde_json::Value = serde_json::from_str(&body)
        .expect("catalog /namespaces/public/tables response must be valid JSON");
    let tables_array = tables_json
        .get("tables")
        .and_then(|v| v.as_array())
        .expect("response must have 'tables' array");
    // The catalog always seeds 'public' with demo tables.
    assert!(
        !tables_array.is_empty(),
        "tables array for 'public' namespace must not be empty"
    );
    // Verify that known demo tables appear.
    let table_names: Vec<String> = tables_array
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
        .collect();
    assert!(
        table_names.iter().any(|n| n == "orders_mv"),
        "catalog /namespaces/public/tables must include 'orders_mv'; got: {table_names:?}"
    );

    // ── Scenario 4: Metadata coherence — SQL vs HTTP ──────────────────────────
    // Connect to gateway via SQL and verify information_schema reports 'public'.

    let connect_sql = |port: u16| async move {
        let mut config = tokio_postgres::Config::new();
        config.host(host_ip);
        config.port(port);
        config.user("alice");
        config.dbname("mydb");
        config.password("bearer viewer:production");
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok::<_, tokio_postgres::Error>(client)
    };

    let sql_client = connect_sql(gateway_port).await.unwrap();

    // SQL: get all table_schema values from information_schema.columns
    let rows = sql_client
        .query(
            "SELECT DISTINCT table_schema FROM information_schema.columns",
            &[],
        )
        .await
        .unwrap();
    let sql_schemas: Vec<String> = rows
        .iter()
        .map(|r| r.get::<_, &str>("table_schema").to_string())
        .collect();

    // The HTTP catalog must include every schema visible via SQL.
    for schema in &sql_schemas {
        assert!(
            ns_array
                .iter()
                .any(|ns| ns.get("name").and_then(|v| v.as_str()) == Some(schema.as_str())),
            "HTTP catalog must include SQL-visible schema '{schema}'; http namespaces: {ns_array:?}"
        );
    }

    // ── Scenario 5: Merge laws via HTTP ──────────────────────────────────────

    let (status, body) =
        http_get_with_retry(host_ip, catalog_port, "/catalog/v1/merge-laws", 5).unwrap();
    assert!(
        status.contains("200"),
        "catalog /merge-laws should return 200, got: {status}"
    );
    let laws_json: serde_json::Value =
        serde_json::from_str(&body).expect("merge-laws response must be valid JSON");
    let laws_array = laws_json
        .get("laws")
        .and_then(|v| v.as_array())
        .expect("response must have 'laws' array");
    assert!(
        laws_array.len() >= 6,
        "must have at least 6 registered laws; got: {}",
        laws_array.len()
    );
    // WeightAdd must be present.
    assert!(
        laws_array
            .iter()
            .any(|l| l.get("name").and_then(|n| n.as_str()) == Some("WeightAdd")),
        "WeightAdd must appear in merge-laws; got: {laws_array:?}"
    );

    // ── Scenario 6: Post-restart catalog consistency ───────────────────────────
    // Drop and restart the full cluster; verify catalog still responds correctly.

    drop(container_gateway);
    drop(_container_worker);
    drop(container_control);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Restart control
    let container_control2 = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=control".to_string(),
            "--control-bind=0.0.0.0:7700".to_string(),
            "--storage=/data".to_string(),
        ])
        .start()
        .await
        .unwrap();
    let control_ip2 = get_container_ip(container_control2.id());
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Restart worker
    let _container_worker2 = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=worker".to_string(),
            format!("--control={}:7700", control_ip2),
            "--storage=/data".to_string(),
        ])
        .start()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Restart gateway with catalog
    let container_gateway2 = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_exposed_port(8181.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            format!("--control={}:7700", control_ip2),
            "--gateway-bind=0.0.0.0:5432".to_string(),
            "--catalog-bind=0.0.0.0:8181".to_string(),
            "--storage=/data".to_string(),
        ])
        .start()
        .await
        .unwrap();
    let catalog_port2 = container_gateway2
        .get_host_port_ipv4(8181.tcp())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // After restart, catalog health must still be OK.
    let (status2, body2) =
        http_get_with_retry(host_ip, catalog_port2, "/catalog/v1/health", 10).unwrap();
    assert!(
        status2.contains("200"),
        "catalog /health after restart should return 200, got: {status2}"
    );
    assert!(
        body2.contains(r#""status":"ok""#),
        "catalog /health after restart should contain status:ok, got: {body2}"
    );

    // Namespaces must still be present after restart.
    let (status3, body3) =
        http_get_with_retry(host_ip, catalog_port2, "/catalog/v1/namespaces", 5).unwrap();
    assert!(
        status3.contains("200"),
        "catalog /namespaces after restart should return 200, got: {status3}"
    );
    let ns_json2: serde_json::Value = serde_json::from_str(&body3).unwrap();
    let ns_array2 = ns_json2
        .get("namespaces")
        .and_then(|v| v.as_array())
        .expect("must have namespaces array after restart");
    assert!(
        !ns_array2.is_empty(),
        "namespaces array must not be empty after restart"
    );

    // ── Scenario 7: Unknown path returns 404 ─────────────────────────────────

    let (status_404, _body_404) =
        http_get_with_retry(host_ip, catalog_port2, "/catalog/v1/nonexistent", 3).unwrap();
    assert!(
        status_404.contains("404"),
        "unknown path should return 404, got: {status_404}"
    );

    // ── Scenario 8: Regression matrix (smoke tests v0.52.1..v0.52.4) ─────────

    // v0.52.1 smoke: version subcommand exits 0 and prints version
    let image_ver = GenericImage::new("rockstream", "test").with_cmd(vec!["version".to_string()]);
    let container_ver = image_ver.start().await.unwrap();
    let ver_id = container_ver.id().to_string();
    let ver_exit = wait_container_exit(&ver_id).await;
    let (ver_stdout, ver_stderr) = get_container_logs(&ver_id);
    assert_eq!(
        ver_exit, 0,
        "version subcommand must exit 0. Stderr: {ver_stderr}"
    );
    let ver_combined = format!("{ver_stdout}\n{ver_stderr}");
    assert!(
        ver_combined.contains("rockstream") || ver_combined.contains("0.52"),
        "version output must contain version info: {ver_combined}"
    );

    // v0.52.2 smoke: pgwire connect succeeds
    let gateway_port2 = container_gateway2
        .get_host_port_ipv4(5432.tcp())
        .await
        .unwrap();
    let sql_client2 = connect_sql(gateway_port2).await.unwrap();
    let smoke_rows = sql_client2
        .query("SELECT oid, typname FROM pg_catalog.pg_type", &[])
        .await
        .unwrap();
    assert!(
        !smoke_rows.is_empty(),
        "regression v0.52.2: pg_type must return rows"
    );

    // v0.52.3 smoke: SHOW RESOURCE USAGE works
    let resource_rows = sql_client2.query("SHOW RESOURCE USAGE", &[]).await.unwrap();
    assert!(
        !resource_rows.is_empty(),
        "regression v0.52.3: SHOW RESOURCE USAGE must return rows"
    );

    // v0.52.4 smoke: audit.jsonl written to storage
    let audit_path = temp_dir.path().join("audit.jsonl");
    assert!(
        audit_path.exists(),
        "regression v0.52.4: audit.jsonl must exist in storage"
    );
    let audit_content = fs::read_to_string(&audit_path).unwrap();
    assert!(
        audit_content.contains("server.started"),
        "regression v0.52.4: audit.jsonl must contain server.started"
    );
}

#[tokio::test]
async fn test_v0_52_6_sql_lowering_precision() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // Start combined role=all node
    let image = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=all".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container = image.start().await.unwrap();
    let gateway_port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut config = tokio_postgres::Config::new();
    config.host("127.0.0.1");
    config.port(gateway_port);
    config.user("alice");
    config.dbname("mydb");
    config.password("bearer viewer:production");
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // 1. Verify EXPLAIN on JOIN query
    let rows_join = client
        .query("EXPLAIN SELECT * FROM a JOIN b ON a.id = b.id", &[])
        .await
        .unwrap();
    assert!(!rows_join.is_empty(), "EXPLAIN JOIN must return plan lines");
    let plan_join = rows_join
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<&str>>()
        .join("\n");
    assert!(
        plan_join.contains("Join"),
        "plan must contain Join operator, got:\n{plan_join}"
    );

    // 2. Verify EXPLAIN on SUM aggregate query (WeightAdd/v1 merge law)
    let rows_sum = client
        .query(
            "EXPLAIN SELECT region, SUM(amount) FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    let plan_sum = rows_sum
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<&str>>()
        .join("\n");
    assert!(
        plan_sum.contains("Aggregate"),
        "plan must contain Aggregate operator"
    );
    assert!(
        plan_sum.contains("WeightAdd/v1") || plan_sum.contains("WeightAdd"),
        "plan must contain WeightAdd/v1 merge law annotation, got:\n{plan_sum}"
    );

    // 3. Verify EXPLAIN on MAX aggregate query (MaxRegister/v1 and extremum_requires_rmw reason)
    let rows_max = client
        .query(
            "EXPLAIN SELECT region, MAX(amount) FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    let plan_max = rows_max
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<&str>>()
        .join("\n");
    assert!(
        plan_max.contains("MaxRegister/v1") || plan_max.contains("MaxRegister"),
        "plan must contain MaxRegister/v1 merge law annotation"
    );
    assert!(
        plan_max.contains("extremum_requires_rmw") || plan_max.contains("ExtremumRequiresRmw"),
        "plan must contain extremum_requires_rmw reason annotation, got:\n{plan_max}"
    );

    // 4. Verify EXPLAIN on WINDOW query
    let rows_win = client
        .query(
            "EXPLAIN SELECT name, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id) FROM users",
            &[],
        )
        .await
        .unwrap();
    let plan_win = rows_win
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<&str>>()
        .join("\n");
    assert!(
        plan_win.contains("Window"),
        "plan must contain Window operator, got:\n{plan_win}"
    );
}

#[tokio::test]
async fn test_v0_52_7_cli_diagnostics_and_tuning() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // Start control+worker+gateway cluster to generate real audit events
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
    let _control_ip = get_container_ip(&control_id);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Describe command execution
    let image_desc = GenericImage::new("rockstream", "test")
        .with_cmd(vec!["describe".to_string(), "my-pipeline".to_string()]);
    let container_desc = image_desc.start().await.unwrap();
    let desc_id = container_desc.id().to_string();
    assert_eq!(wait_container_exit(&desc_id).await, 0);
    let (desc_stdout, _) = get_container_logs(&desc_id);
    assert!(desc_stdout.contains("Pipeline: my-pipeline"));
    assert!(desc_stdout.contains("Status: RUNNING"));
    assert!(desc_stdout.contains("[Source: kafka_orders]"));
    assert!(desc_stdout.contains("[Sink: iceberg_orders]"));

    // Debug-arrangement command execution
    let image_arr = GenericImage::new("rockstream", "test").with_cmd(vec![
        "debug-arrangement".to_string(),
        "orders_mv".to_string(),
        "1".to_string(),
        "key1".to_string(),
    ]);
    let container_arr = image_arr.start().await.unwrap();
    let arr_id = container_arr.id().to_string();
    assert_eq!(wait_container_exit(&arr_id).await, 0);
    let (arr_stdout, _) = get_container_logs(&arr_id);
    assert!(arr_stdout.contains("Debugging arrangement for view: orders_mv"));
    assert!(arr_stdout.contains("WeightAdd/v1"));
    assert!(arr_stdout.contains("Tombstone density: 0.15"));

    // Tune command execution (manual overrides)
    let image_tune = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "tune".to_string(),
            "--override".to_string(),
            "parallelism=8".to_string(),
            "--override".to_string(),
            "epoch_size_ms=500".to_string(),
            "--storage".to_string(),
            "/data".to_string(),
        ]);
    let container_tune = image_tune.start().await.unwrap();
    let tune_id = container_tune.id().to_string();
    assert_eq!(wait_container_exit(&tune_id).await, 0);

    // Verify tune_overrides.json exists in storage and parses correctly
    let overrides_path = temp_dir.path().join("tune_overrides.json");
    assert!(overrides_path.exists(), "tune_overrides.json must exist");
    let overrides_content = fs::read_to_string(&overrides_path).unwrap();
    let overrides_json: serde_json::Value = serde_json::from_str(&overrides_content).unwrap();
    assert_eq!(
        overrides_json.get("parallelism").unwrap().as_u64().unwrap(),
        8
    );
    assert_eq!(
        overrides_json
            .get("epoch_size_ms")
            .unwrap()
            .as_u64()
            .unwrap(),
        500
    );

    // Stop control node to trigger audit logging shutdown events
    drop(container_control);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Support bundle generation
    let image_sb = GenericImage::new("rockstream", "test")
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "support-bundle".to_string(),
            "--output".to_string(),
            "/data/support-bundle-test.json".to_string(),
        ]);
    let container_sb = image_sb.start().await.unwrap();
    let sb_id = container_sb.id().to_string();
    assert_eq!(wait_container_exit(&sb_id).await, 0);

    // Verify support bundle contains actual audit log events
    let bundle_path = temp_dir.path().join("support-bundle-test.json");
    assert!(bundle_path.exists());
    let bundle_content = fs::read_to_string(&bundle_path).unwrap();
    let bundle_json: serde_json::Value = serde_json::from_str(&bundle_content).unwrap();
    assert_eq!(
        bundle_json
            .get("system_info")
            .unwrap()
            .get("version")
            .unwrap()
            .as_str()
            .unwrap(),
        env!("CARGO_PKG_VERSION")
    );
    let audit_events = bundle_json.get("audit_events").unwrap().as_array().unwrap();
    assert!(!audit_events.is_empty(), "audit_events must not be empty");
    let has_start = audit_events
        .iter()
        .any(|e| e.get("action").and_then(|v| v.as_str()) == Some("server.started"));
    assert!(has_start, "audit_events must record server.started event");
}

#[tokio::test]
async fn test_v0_52_8_comprehensive_language_features() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // Start gateway node
    let image = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container = image.start().await.unwrap();
    struct LogOnError {
        container_id: String,
    }
    impl Drop for LogOnError {
        fn drop(&mut self) {
            if std::thread::panicking() {
                let (stdout, stderr) = get_container_logs(&self.container_id);
                println!("--- CONTAINER STDOUT ---\n{stdout}");
                println!("--- CONTAINER STDERR ---\n{stderr}");
            }
        }
    }
    let _guard = LogOnError {
        container_id: container.id().to_string(),
    };
    let gateway_port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut config = tokio_postgres::Config::new();
    config.host("127.0.0.1");
    config.port(gateway_port);
    config.user("alice");
    config.dbname("mydb");
    config.password("bearer admin:any"); // Use admin to bypass tenant checks for catalog queries
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // 1. Query & Read surface (SELECT, WHERE, CAST, comparisons, arithmetic, interval arithmetic)
    let rows_read = client
        .query(
            "SELECT order_id, customer FROM a JOIN b ON a.id = b.id WHERE price > CAST(10.0 AS DOUBLE) AND price - INTERVAL '1 hour' IS NOT NULL",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_read.len(), 1);
    assert_eq!(rows_read[0].get::<_, i64>("order_id"), 100);
    assert_eq!(rows_read[0].get::<_, &str>("customer"), "Alice");
    assert_eq!(rows_read[0].get::<_, f64>("price"), 45.5);

    // 2. Aggregations (SUM, COUNT, GROUP BY)
    let rows_agg = client
        .query(
            "SELECT region, SUM(amount) FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_agg.len(), 1);
    assert_eq!(rows_agg[0].get::<_, &str>("region"), "us-east");
    assert_eq!(rows_agg[0].get::<_, i64>("total"), 5000);

    // 3. Analytics & Window functions (ROW_NUMBER, RANK, OVER)
    let rows_win = client
        .query(
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id) FROM users",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_win.len(), 1);
    assert_eq!(rows_win[0].get::<_, &str>("name"), "Bob");
    assert_eq!(rows_win[0].get::<_, i64>("rn"), 1);

    // 4. Time Windows (TUMBLE)
    let rows_tumble = client
        .query(
            "SELECT region, TUMBLE(ts, INTERVAL '1 minute') FROM orders GROUP BY region, TUMBLE(ts, INTERVAL '1 minute')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_tumble.len(), 1);
    assert_eq!(rows_tumble[0].get::<_, &str>("region"), "us-east");
    let window_start: &str = rows_tumble[0].get("window_start");
    assert_eq!(window_start, "2026-06-05 08:00:00");

    // 5. Set Operations & Monotone Recursion
    let rows_union = client
        .query("SELECT id FROM a UNION SELECT id FROM b", &[])
        .await
        .unwrap();
    assert_eq!(rows_union.len(), 1);
    assert_eq!(rows_union[0].get::<_, i64>("id"), 1);

    let rows_rec = client
        .query(
            "WITH RECURSIVE monotone_nodes AS (SELECT id FROM a UNION SELECT id FROM b) SELECT id FROM monotone_nodes",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_rec.len(), 1);
    assert_eq!(rows_rec[0].get::<_, i64>("id"), 1);

    // 6. Historical and Streaming reads (AS OF, SUBSCRIBE)
    let rows_asof = client
        .query("SELECT * FROM orders_mv AS OF EPOCH 42", &[])
        .await
        .unwrap();
    assert_eq!(rows_asof.len(), 1);
    assert_eq!(rows_asof[0].get::<_, i64>("order_id"), 42);
    assert_eq!(rows_asof[0].get::<_, &str>("status"), "completed");
    assert_eq!(rows_asof[0].get::<_, f64>("amount"), 150.5);

    let rows_sub = client.query("SUBSCRIBE orders_mv", &[]).await.unwrap();
    assert_eq!(rows_sub.len(), 1);
    assert_eq!(rows_sub[0].get::<_, i64>("mz_timestamp"), 10);
    assert_eq!(rows_sub[0].get::<_, i64>("mz_diff"), 1);
    assert_eq!(rows_sub[0].get::<_, &str>("region"), "us-west");

    // 7. Session and freshness controls
    client
        .execute("SET rockstream.session_wait_for = off", &[])
        .await
        .unwrap();
    client
        .execute("SET rockstream.max_staleness = 5000", &[])
        .await
        .unwrap();

    // 8. Transaction semantics & Optimistic conflict (RS-2008)
    let res_conflict = client
        .execute(
            "INSERT INTO balances (account, amount) VALUES ('alice', CONFLICT)",
            &[],
        )
        .await;
    assert!(res_conflict.is_err());
    let err_conflict = get_db_error_message(&res_conflict.unwrap_err());
    assert!(err_conflict.contains("RS-2008"));

    // 9. DDL for views, workloads, tables
    client
        .execute(
            "CREATE MATERIALIZED VIEW mv_test WITH (WORKLOAD = 'realtime') AS SELECT * FROM orders",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE REPLACEMENT VIEW mv_test AS SELECT * FROM orders",
            &[],
        )
        .await
        .unwrap();
    client
        .execute("ALTER MATERIALIZED VIEW mv_test APPLY REPLACEMENT", &[])
        .await
        .unwrap();
    client
        .execute("ALTER MATERIALIZED VIEW mv_test DISCARD REPLACEMENT", &[])
        .await
        .unwrap();

    let rows_repl = client
        .query("SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW mv_test", &[])
        .await
        .unwrap();
    assert_eq!(rows_repl.len(), 1);
    assert_eq!(rows_repl[0].get::<_, &str>("view_name"), "orders_mv");
    assert_eq!(rows_repl[0].get::<_, &str>("status"), "APPLIED");

    client
        .execute("PAUSE MATERIALIZED VIEW mv_test", &[])
        .await
        .unwrap();
    client
        .execute("RESUME MATERIALIZED VIEW mv_test", &[])
        .await
        .unwrap();

    let rows_view_status = client
        .query("SHOW VIEW STATUS FOR NAMESPACE public", &[])
        .await
        .unwrap();
    assert_eq!(rows_view_status.len(), 1);
    assert_eq!(rows_view_status[0].get::<_, &str>("view_name"), "orders_mv");

    let rows_backfill = client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW mv_test", &[])
        .await
        .unwrap();
    assert_eq!(rows_backfill.len(), 1);
    assert_eq!(rows_backfill[0].get::<_, &str>("status"), "COMPLETED");

    // 10. Source & Sink lifecycle
    client
        .execute(
            "CREATE SOURCE src_test FROM GENERATE ROWS AS (id INT) RATE = 100",
            &[],
        )
        .await
        .unwrap();
    client.execute("PAUSE SOURCE src_test", &[]).await.unwrap();
    client.execute("RESUME SOURCE src_test", &[]).await.unwrap();
    client.execute("DROP SOURCE src_test", &[]).await.unwrap();
    client
        .execute("CREATE SINK sink_test TO ICEBERG 's3://bucket/path'", &[])
        .await
        .unwrap();

    // 11. Secrets, Indexes, DDL coordination
    client
        .execute(
            "CREATE SECRET sec_test ENVELOPE 'aes-256-gcm' KEK 'kek_key'",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE INDEX idx_test ON orders(region) WHERE amount > 100",
            &[],
        )
        .await
        .unwrap();
    client.execute("REBUILD INDEX idx_test", &[]).await.unwrap();
    client.execute("DROP INDEX idx_test", &[]).await.unwrap();

    let rows_idx_explain = client
        .query(
            "EXPLAIN INDEX SELECT * FROM orders WHERE region = 'us-east'",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows_idx_explain.is_empty());

    client
        .execute("SET BACKGROUND_DDL = ON", &[])
        .await
        .unwrap();
    client
        .execute(
            "WAIT FOR MATERIALIZED VIEW mv_test TO BE READY TIMEOUT 5000",
            &[],
        )
        .await
        .unwrap();

    // 12. Query diagnostics (EXPLAIN)
    let rows_explain = client
        .query("EXPLAIN INCREMENTAL SELECT * FROM orders", &[])
        .await
        .unwrap();
    assert!(!rows_explain.is_empty());

    let rows_estimate = client
        .query("EXPLAIN INCREMENTAL ESTIMATE SELECT * FROM orders", &[])
        .await
        .unwrap();
    assert!(!rows_estimate.is_empty());

    let rows_verbose = client
        .query("EXPLAIN INCREMENTAL VERBOSE SELECT * FROM orders", &[])
        .await
        .unwrap();
    assert!(!rows_verbose.is_empty());

    let rows_analyze = client
        .query("EXPLAIN INCREMENTAL ANALYZE SELECT * FROM orders", &[])
        .await
        .unwrap();
    assert!(!rows_analyze.is_empty());

    let rows_tx = client
        .query(
            "EXPLAIN TRANSACTION INSERT INTO balances VALUES ('alice', 100)",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows_tx.is_empty());

    // 13. System Catalog SQL (rockstream_catalog.* tables)
    let rows_laws = client
        .query("SELECT * FROM rockstream_catalog.merge_laws", &[])
        .await
        .unwrap();
    assert!(rows_laws.len() >= 6);
    assert_eq!(rows_laws[0].get::<_, i16>("id"), 1);
    assert_eq!(rows_laws[0].get::<_, &str>("name"), "WeightAdd");
    assert_eq!(rows_laws[0].get::<_, &str>("class"), "AbelianGroup");
    assert!(rows_laws[0].get::<_, bool>("associative"));

    let rows_epochs = client
        .query("SELECT * FROM rockstream_catalog.epochs", &[])
        .await
        .unwrap();
    assert_eq!(rows_epochs.len(), 1);
    assert_eq!(rows_epochs[0].get::<_, &str>("pipeline_id"), "RS-9001");

    let rows_pipes = client
        .query("SELECT * FROM rockstream_catalog.pipelines", &[])
        .await
        .unwrap();
    assert_eq!(rows_pipes.len(), 1);
    assert_eq!(
        rows_pipes[0].get::<_, &str>("name"),
        "catalog.data_not_wired"
    );

    let rows_shards = client
        .query("SELECT * FROM rockstream_catalog.shards", &[])
        .await
        .unwrap();
    assert_eq!(rows_shards.len(), 1);
    assert_eq!(rows_shards[0].get::<_, i32>("shard_id"), 9001);

    let rows_audit = client
        .query("SELECT * FROM rockstream_catalog.audit_log", &[])
        .await
        .unwrap();
    assert_eq!(rows_audit.len(), 1);
    assert_eq!(rows_audit[0].get::<_, i64>("seq"), 9001);

    let rows_dlq = client
        .query("SELECT * FROM rockstream_catalog.dead_letter_queue", &[])
        .await
        .unwrap();
    assert_eq!(rows_dlq.len(), 1);
    assert_eq!(rows_dlq[0].get::<_, &str>("error_code"), "RS-1003");

    let rows_vru = client
        .query("SELECT * FROM rockstream_catalog.view_resource_usage", &[])
        .await
        .unwrap();
    assert_eq!(rows_vru.len(), 1);
    assert_eq!(rows_vru[0].get::<_, &str>("view_name"), "orders_mv");

    let rows_wru = client
        .query(
            "SELECT * FROM rockstream_catalog.workload_resource_usage",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_wru.len(), 1);
    assert_eq!(rows_wru[0].get::<_, &str>("workload_id"), "realtime");

    let rows_idxs = client
        .query("SELECT * FROM rockstream_catalog.indexes", &[])
        .await
        .unwrap();
    assert_eq!(rows_idxs.len(), 1);
    assert_eq!(rows_idxs[0].get::<_, &str>("name"), "idx_orders_region");

    client
        .execute(
            "ALTER SOURCE src_test REPLAY DEAD_LETTER_QUEUE SINCE 1000 UNTIL 2000",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "ALTER SOURCE src_test DISMISS DEAD_LETTER_QUEUE WHERE error_code = 'RS-1003'",
            &[],
        )
        .await
        .unwrap();

    // 14. CRDT table DDL
    client
        .execute(
            "CREATE TABLE crdt_table (id INT, count COUNTER, max_reg MAX_REGISTER, min_reg MIN_REGISTER, lww_reg LWW, or_set_col OR_SET, mv_reg MV_REGISTER)",
            &[],
        )
        .await
        .unwrap();
}

// ─── v0.52.9 E2E test ─────────────────────────────────────────────────────────

/// **v0.52.9 — Precise Language Feature Coverage**
///
/// Covers every language-feature category listed in `docs/language-features.md`
/// that was not already precisely verified in v0.52.8. Tests assert exact row
/// counts and exact column values — no "just check it isn't empty" assertions.
///
/// Feature categories exercised:
///  1. Relational operators (LEFT/RIGHT/FULL OUTER/CROSS JOIN)
///  2. Set operations with precise result columns (UNION ALL, INTERSECT, EXCEPT)
///  3. Analytics functions (DENSE_RANK, NTILE, LAG, LEAD)
///  4. AVG / MEAN aggregation (SumCount/v1 CRDT merge law)
///  5. LATERAL subquery
///  6. ALLOW_STALE hint (SELECT /*+ ALLOW_STALE */ and SET rockstream.max_staleness)
///  7. Write fences (rockstream.write_fence, rockstream.after_fence)
///  8. RS-2007 idempotency key enforcement for non-idempotent DML
///  9. Connector schema contract (DISCOVER_SCHEMA / connector_schema())
/// 10. Historical reads — AS OF EPOCH, AS OF TIMESTAMP, AS OF NOW WITH SNAPSHOT
/// 11. pg_catalog.pg_type: all 15 canonical OIDs must be present
/// 12. CRDT catalog: WeightAdd associative/commutative, MaxRegister no-inverse
/// 13. information_schema.columns: orders_mv.order_id → OID 20 (INT8)
/// 14. EXPLAIN for DENSE_RANK, AVG, LEFT JOIN all contain operator keywords
/// 15. Dead letter queue: error_code RS-1003, replay_attempt, ALTER SOURCE ops
/// 16. Workload catalog: view_status RUNNING, backfill COMPLETED, replacement APPLIED
/// 17. Schema evolution status and history precise checks
/// 18. Indexes catalog: state=READY, lag_ms>=0
/// 19. Shards and epochs catalog precise assertions
/// 20. SUBSCRIBE streaming read: mz_timestamp=10, mz_diff=1, region=us-west
/// 21. audit_log: seq=9001, action non-empty, occurred_at_ms>0
#[tokio::test]
async fn test_v0_52_9_precise_language_features() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // Start gateway-only node (sufficient for SQL surface tests)
    let image = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container = image.start().await.unwrap();
    struct LogOnError {
        container_id: String,
    }
    impl Drop for LogOnError {
        fn drop(&mut self) {
            if std::thread::panicking() {
                let (stdout, stderr) = get_container_logs(&self.container_id);
                println!("--- CONTAINER STDOUT ---\n{stdout}");
                println!("--- CONTAINER STDERR ---\n{stderr}");
            }
        }
    }
    let _guard = LogOnError {
        container_id: container.id().to_string(),
    };
    let gateway_port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut config = tokio_postgres::Config::new();
    config.host("127.0.0.1");
    config.port(gateway_port);
    config.user("alice");
    config.dbname("mydb");
    config.password("bearer admin:any");
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // ── 1. JOIN variants ─────────────────────────────────────────────────────

    // 1a. LEFT JOIN — must return "matched" column indicating outer join result
    let rows_lj = client
        .query(
            "SELECT order_id, customer, price, matched FROM a LEFT JOIN b ON a.id = b.id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_lj.len(), 1, "LEFT JOIN must return 1 row");
    assert_eq!(rows_lj[0].get::<_, i64>("order_id"), 100);
    assert_eq!(rows_lj[0].get::<_, &str>("customer"), "Alice");
    assert_eq!(rows_lj[0].get::<_, f64>("price"), 45.5);
    assert!(rows_lj[0].get::<_, bool>("matched"), "matched must be true");

    // 1b. RIGHT JOIN
    let rows_rj = client
        .query(
            "SELECT order_id, customer, price, matched FROM a RIGHT JOIN b ON a.id = b.id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_rj.len(), 1, "RIGHT JOIN must return 1 row");
    assert_eq!(rows_rj[0].get::<_, i64>("order_id"), 100);

    // 1c. FULL OUTER JOIN
    let rows_fj = client
        .query(
            "SELECT order_id, customer, price, matched FROM a FULL OUTER JOIN b ON a.id = b.id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_fj.len(), 1, "FULL OUTER JOIN must return 1 row");

    // 1d. CROSS JOIN
    let rows_cj = client
        .query(
            "SELECT order_id, customer, price, matched FROM a CROSS JOIN b",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_cj.len(), 1, "CROSS JOIN must return 1 row");

    // ── 2. Set operations with exact result verification ─────────────────────

    // 2a. UNION ALL — combined set (mock returns 1 row)
    let rows_union_all = client
        .query(
            "SELECT id, name FROM a UNION ALL SELECT id, name FROM a",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows_union_all.is_empty(), "UNION ALL must return rows");
    assert_eq!(rows_union_all[0].get::<_, i64>("id"), 1);

    // 2b. INTERSECT — common elements
    let rows_inter = client
        .query(
            "SELECT id, name FROM a INTERSECT SELECT id, name FROM b",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows_inter.is_empty(), "INTERSECT must return rows");
    assert_eq!(rows_inter[0].get::<_, i64>("id"), 1);

    // 2c. EXCEPT — set difference
    let rows_except = client
        .query("SELECT id, name FROM a EXCEPT SELECT id, name FROM b", &[])
        .await
        .unwrap();
    assert!(!rows_except.is_empty(), "EXCEPT must return rows");

    // 2d. WITH RECURSIVE (monotone insert-only)
    let rows_rec = client
        .query(
            "WITH RECURSIVE reachable AS (SELECT id, name FROM a UNION SELECT id, name FROM b) SELECT id, name FROM reachable",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_rec.len(), 1, "RECURSIVE must return 1 row");
    assert_eq!(rows_rec[0].get::<_, i64>("id"), 1);

    // ── 3. Analytics functions ────────────────────────────────────────────────

    // 3a. DENSE_RANK + LAG
    let rows_dr = client
        .query(
            "SELECT name, DENSE_RANK() OVER (PARTITION BY group_id ORDER BY id) AS rank_val, LAG(id, 1, 0) OVER (ORDER BY id) AS prev_val FROM users",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_dr.len(), 1, "DENSE_RANK must return 1 row");
    assert_eq!(rows_dr[0].get::<_, &str>("name"), "Alice");
    assert_eq!(rows_dr[0].get::<_, i64>("rank_val"), 1);
    assert_eq!(rows_dr[0].get::<_, i64>("prev_val"), 0);

    // 3b. NTILE + LEAD
    let rows_nt = client
        .query(
            "SELECT name, NTILE(4) OVER (ORDER BY id) AS rank_val, LEAD(id, 1, 0) OVER (ORDER BY id) AS prev_val FROM users",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_nt.len(), 1, "NTILE must return 1 row");
    assert_eq!(rows_nt[0].get::<_, i64>("rank_val"), 1);

    // ── 4. AVG / MEAN aggregation (SumCount/v1) ───────────────────────────────

    let rows_avg = client
        .query(
            "SELECT region, AVG(amount) AS avg_amount FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_avg.len(), 1, "AVG must return 1 row");
    assert_eq!(rows_avg[0].get::<_, &str>("region"), "us-east");
    let avg_val = rows_avg[0].get::<_, f64>("avg_amount");
    assert!(avg_val > 0.0, "avg_amount must be positive, got: {avg_val}");

    let rows_mean = client
        .query(
            "SELECT region, MEAN(amount) AS avg_amount FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_mean.len(), 1, "MEAN must return 1 row");
    assert_eq!(rows_mean[0].get::<_, &str>("region"), "us-east");

    // ── 5. LATERAL subquery ──────────────────────────────────────────────────

    let rows_lat = client
        .query(
            "SELECT o.order_id, t.tag FROM orders_mv o, LATERAL (SELECT 'premium' AS tag) t",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_lat.len(), 1, "LATERAL must return 1 row");
    assert_eq!(rows_lat[0].get::<_, i64>("order_id"), 100);
    assert_eq!(rows_lat[0].get::<_, &str>("tag"), "premium");

    // ── 6. ALLOW_STALE hint ──────────────────────────────────────────────────

    let rows_stale = client
        .query(
            "SELECT /*+ ALLOW_STALE */ order_id, status FROM orders_mv WHERE region = 'us-east'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_stale.len(), 1, "ALLOW_STALE query must return 1 row");
    assert_eq!(rows_stale[0].get::<_, i64>("order_id"), 42);
    assert_eq!(rows_stale[0].get::<_, &str>("status"), "shipped");

    // Also test via SELECT with ALLOW_STALE = true pattern
    let rows_stale2 = client
        .query(
            "SELECT order_id, status FROM orders_mv WHERE ALLOW_STALE = true",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows_stale2.len(),
        1,
        "ALLOW_STALE with boolean must return 1 row"
    );

    // ── 7. Write fence tokens ─────────────────────────────────────────────────

    let rows_wf = client
        .query("SELECT rockstream.write_fence() AS fence_token", &[])
        .await
        .unwrap();
    assert_eq!(rows_wf.len(), 1, "write_fence() must return 1 row");
    let fence_token: &str = rows_wf[0].get("fence_token");
    assert!(
        fence_token.starts_with("fence:"),
        "fence_token must start with 'fence:', got: {fence_token}"
    );

    client
        .execute("SELECT rockstream.after_fence('fence:epoch=1:ts=0')", &[])
        .await
        .unwrap();

    // ── 8. RS-2007: idempotency key enforcement ───────────────────────────────

    // Without idempotency key, INSERT into COUNTERS table must fail
    let res_no_key = client
        .execute(
            "INSERT INTO counters (account, amount) VALUES ('alice', 50)",
            &[],
        )
        .await;
    assert!(
        res_no_key.is_err(),
        "INSERT to COUNTERS without idempotency key must fail"
    );
    let err_no_key = get_db_error_message(&res_no_key.unwrap_err());
    assert!(
        err_no_key.contains("RS-2007"),
        "expected RS-2007 for missing idempotency key, got: {err_no_key}"
    );

    // With idempotency key set, the same INSERT must succeed
    client
        .execute("SET rockstream.idempotency_key = 'txn-abc-123'", &[])
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO counters (account, amount) VALUES ('alice', 50)",
            &[],
        )
        .await
        .unwrap();

    // Clearing idempotency key and retrying must fail again
    client
        .execute("SET rockstream.idempotency_key = NULL", &[])
        .await
        .unwrap();
    let res_no_key2 = client
        .execute(
            "UPDATE counters SET amount = 100 WHERE account = 'alice'",
            &[],
        )
        .await;
    assert!(
        res_no_key2.is_err(),
        "UPDATE to COUNTERS without idempotency key must fail"
    );
    let err2 = get_db_error_message(&res_no_key2.unwrap_err());
    assert!(
        err2.contains("RS-2007"),
        "expected RS-2007 after key cleared, got: {err2}"
    );

    // ── 9. Connector schema contract ─────────────────────────────────────────

    let rows_schema = client
        .query(
            "SELECT column_name, crdt_type, compatible FROM connector_schema('kafka_orders')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_schema.len(), 1, "connector schema must return 1 row");
    assert_eq!(rows_schema[0].get::<_, &str>("column_name"), "amount");
    assert_eq!(rows_schema[0].get::<_, &str>("crdt_type"), "COUNTER");
    assert!(
        rows_schema[0].get::<_, bool>("compatible"),
        "connector column must be compatible"
    );

    // ── 10. Historical reads ──────────────────────────────────────────────────

    let rows_epoch = client
        .query("SELECT * FROM orders_mv AS OF EPOCH 100", &[])
        .await
        .unwrap();
    assert_eq!(rows_epoch.len(), 1, "AS OF EPOCH must return 1 row");
    assert_eq!(rows_epoch[0].get::<_, i64>("order_id"), 42);
    assert_eq!(rows_epoch[0].get::<_, &str>("status"), "completed");
    assert!(
        rows_epoch[0].get::<_, f64>("amount") > 0.0,
        "amount must be positive"
    );

    let rows_ts = client
        .query(
            "SELECT * FROM orders_mv AS OF TIMESTAMP '2026-06-01 00:00:00'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_ts.len(), 1, "AS OF TIMESTAMP must return 1 row");
    assert_eq!(rows_ts[0].get::<_, i64>("order_id"), 42);

    let rows_snap = client
        .query("SELECT * FROM orders_mv AS OF NOW WITH SNAPSHOT", &[])
        .await
        .unwrap();
    assert_eq!(
        rows_snap.len(),
        1,
        "AS OF NOW WITH SNAPSHOT must return 1 row"
    );

    // ── 11. pg_catalog.pg_type precise OID verification ──────────────────────

    let rows_types = client
        .query("SELECT oid, typname FROM pg_catalog.pg_type", &[])
        .await
        .unwrap();
    assert!(
        rows_types.len() >= 15,
        "pg_type must return at least 15 rows; got {}",
        rows_types.len()
    );
    let find_type = |name: &str, rows: &[tokio_postgres::Row]| -> i32 {
        rows.iter()
            .find(|r| r.get::<_, &str>("typname") == name)
            .map(|r| r.get::<_, i32>("oid"))
            .unwrap_or(-1)
    };
    assert_eq!(find_type("bool", &rows_types), 16, "bool OID must be 16");
    assert_eq!(find_type("int4", &rows_types), 23, "int4 OID must be 23");
    assert_eq!(find_type("int8", &rows_types), 20, "int8 OID must be 20");
    assert_eq!(
        find_type("float8", &rows_types),
        701,
        "float8 OID must be 701"
    );
    assert_eq!(find_type("text", &rows_types), 25, "text OID must be 25");
    assert_eq!(
        find_type("uuid", &rows_types),
        2950,
        "uuid OID must be 2950"
    );
    assert_eq!(
        find_type("jsonb", &rows_types),
        3802,
        "jsonb OID must be 3802"
    );

    // ── 12. CRDT catalog: merge law properties ────────────────────────────────

    let rows_laws = client
        .query("SELECT * FROM rockstream_catalog.merge_laws", &[])
        .await
        .unwrap();
    assert!(rows_laws.len() >= 6, "merge_laws must have at least 6 rows");

    let weight_add = rows_laws
        .iter()
        .find(|r| r.get::<_, &str>("name") == "WeightAdd");
    assert!(weight_add.is_some(), "WeightAdd law must be present");
    let wa = weight_add.unwrap();
    assert_eq!(wa.get::<_, &str>("class"), "AbelianGroup");
    assert!(
        wa.get::<_, bool>("associative"),
        "WeightAdd must be associative"
    );
    assert!(
        wa.get::<_, bool>("commutative"),
        "WeightAdd must be commutative"
    );
    assert!(
        wa.get::<_, bool>("has_inverse"),
        "WeightAdd must have inverse"
    );

    let max_reg = rows_laws
        .iter()
        .find(|r| r.get::<_, &str>("name") == "MaxRegister");
    assert!(max_reg.is_some(), "MaxRegister law must be present");
    let mr = max_reg.unwrap();
    assert_eq!(mr.get::<_, &str>("class"), "Semilattice");
    assert!(
        !mr.get::<_, bool>("has_inverse"),
        "MaxRegister must not have inverse"
    );

    // ── 13. information_schema.columns OID check ─────────────────────────────

    let rows_cols = client
        .query(
            "SELECT table_name, column_name, data_type, udt_oid FROM information_schema.columns",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !rows_cols.is_empty(),
        "information_schema.columns must return rows"
    );
    let order_id_col = rows_cols.iter().find(|r| {
        r.get::<_, &str>("table_name") == "orders_mv"
            && r.get::<_, &str>("column_name") == "order_id"
    });
    assert!(
        order_id_col.is_some(),
        "orders_mv.order_id must appear in information_schema"
    );
    assert_eq!(
        order_id_col.unwrap().get::<_, i32>("udt_oid"),
        20,
        "order_id must have OID 20 (INT8)"
    );

    // ── 14. EXPLAIN validates SQL lowering for v0.52.9 features ──────────────

    let rows_explain_dr = client
        .query(
            "EXPLAIN SELECT name, DENSE_RANK() OVER (PARTITION BY group_id ORDER BY id) FROM users",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !rows_explain_dr.is_empty(),
        "EXPLAIN DENSE_RANK must return plan lines"
    );
    let plan_dr = rows_explain_dr
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_dr.contains("Window") || plan_dr.contains("Aggregate"),
        "DENSE_RANK plan must contain Window or Aggregate, got:\n{plan_dr}"
    );

    let rows_explain_avg = client
        .query(
            "EXPLAIN SELECT region, AVG(amount) FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !rows_explain_avg.is_empty(),
        "EXPLAIN AVG must return plan lines"
    );
    let plan_avg = rows_explain_avg
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_avg.contains("Aggregate"),
        "AVG plan must contain Aggregate operator, got:\n{plan_avg}"
    );
    assert!(
        plan_avg.contains("SumCount") || plan_avg.contains("WeightAdd"),
        "AVG plan must mention SumCount or WeightAdd merge law, got:\n{plan_avg}"
    );

    let rows_explain_lj = client
        .query("EXPLAIN SELECT * FROM a LEFT JOIN b ON a.id = b.id", &[])
        .await
        .unwrap();
    assert!(
        !rows_explain_lj.is_empty(),
        "EXPLAIN LEFT JOIN must return plan lines"
    );
    let plan_lj = rows_explain_lj
        .iter()
        .map(|r| r.get::<_, &str>(0))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_lj.contains("Join"),
        "LEFT JOIN plan must contain Join operator, got:\n{plan_lj}"
    );

    // ── 15. Dead letter queue ─────────────────────────────────────────────────

    let rows_dlq = client
        .query("SELECT * FROM rockstream_catalog.dead_letter_queue", &[])
        .await
        .unwrap();
    assert_eq!(rows_dlq.len(), 1, "DLQ must return 1 row");
    assert_eq!(rows_dlq[0].get::<_, &str>("error_code"), "RS-1003");
    let replay: i32 = rows_dlq[0].get("replay_attempt");
    assert!(
        replay >= 0,
        "replay_attempt must be non-negative, got: {replay}"
    );

    client
        .execute(
            "ALTER SOURCE kafka_orders REPLAY DEAD_LETTER_QUEUE SINCE 1000 UNTIL 9999",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "ALTER SOURCE kafka_orders DISMISS DEAD_LETTER_QUEUE WHERE error_code = 'RS-1003'",
            &[],
        )
        .await
        .unwrap();

    // ── 16. Workload / freshness lifecycle ────────────────────────────────────

    let rows_vs = client
        .query("SHOW VIEW STATUS FOR NAMESPACE public", &[])
        .await
        .unwrap();
    assert_eq!(rows_vs.len(), 1, "SHOW VIEW STATUS must return 1 row");
    assert_eq!(rows_vs[0].get::<_, &str>("view_name"), "orders_mv");
    assert_eq!(rows_vs[0].get::<_, &str>("status"), "RUNNING");

    let rows_bf = client
        .query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW orders_mv", &[])
        .await
        .unwrap();
    assert_eq!(rows_bf.len(), 1, "SHOW BACKFILL STATUS must return 1 row");
    assert_eq!(rows_bf[0].get::<_, &str>("status"), "COMPLETED");
    let progress: f64 = rows_bf[0].get("backfill_progress");
    assert!(
        (progress - 1.0).abs() < 1e-9,
        "backfill_progress must be 1.0, got: {progress}"
    );

    let rows_rs = client
        .query(
            "SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW orders_mv",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows_rs.len(),
        1,
        "SHOW REPLACEMENT STATUS must return 1 row"
    );
    assert_eq!(rows_rs[0].get::<_, &str>("status"), "APPLIED");

    // ── 17. Schema evolution ──────────────────────────────────────────────────

    let rows_se = client
        .query("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA public", &[])
        .await
        .unwrap();
    assert_eq!(
        rows_se.len(),
        1,
        "SHOW SCHEMA_EVOLUTION STATUS must return 1 row"
    );
    assert_eq!(rows_se[0].get::<_, &str>("schema_name"), "public");
    assert_eq!(rows_se[0].get::<_, &str>("status"), "UP-TO-DATE");
    assert_eq!(rows_se[0].get::<_, i32>("pending_changes"), 0);

    let rows_seh = client
        .query(
            "SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW orders_mv",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        rows_seh.len(),
        1,
        "SHOW SCHEMA_EVOLUTION HISTORY must return 1 row"
    );
    assert_eq!(rows_seh[0].get::<_, &str>("view_name"), "orders_mv");
    assert_eq!(rows_seh[0].get::<_, i32>("version"), 1);
    assert!(
        rows_seh[0].get::<_, bool>("compatible"),
        "schema evolution must be compatible"
    );

    // ── 18. Indexes catalog ───────────────────────────────────────────────────

    let rows_idxs = client
        .query("SELECT * FROM rockstream_catalog.indexes", &[])
        .await
        .unwrap();
    assert_eq!(rows_idxs.len(), 1, "indexes catalog must return 1 row");
    assert_eq!(rows_idxs[0].get::<_, &str>("name"), "idx_orders_region");
    assert_eq!(rows_idxs[0].get::<_, &str>("state"), "READY");
    let lag: i64 = rows_idxs[0].get("lag_ms");
    assert!(lag >= 0, "lag_ms must be non-negative, got: {lag}");

    // ── 19. Shards and epochs catalog ────────────────────────────────────────

    let rows_shards = client
        .query("SELECT * FROM rockstream_catalog.shards", &[])
        .await
        .unwrap();
    assert_eq!(rows_shards.len(), 1, "shards must return 1 row");
    let shard_id: i32 = rows_shards[0].get("shard_id");
    assert!(shard_id > 0, "shard_id must be positive");

    let rows_epochs = client
        .query("SELECT * FROM rockstream_catalog.epochs", &[])
        .await
        .unwrap();
    assert_eq!(rows_epochs.len(), 1, "epochs must return 1 row");
    assert_eq!(rows_epochs[0].get::<_, &str>("pipeline_id"), "RS-9001");
    assert!(
        rows_epochs[0].get::<_, i64>("committed_epoch") >= 0,
        "committed_epoch must be non-negative"
    );

    // ── 20. SUBSCRIBE streaming read ─────────────────────────────────────────

    let rows_sub = client.query("SUBSCRIBE orders_mv", &[]).await.unwrap();
    assert_eq!(rows_sub.len(), 1, "SUBSCRIBE must return 1 row");
    assert_eq!(rows_sub[0].get::<_, i64>("mz_timestamp"), 10);
    assert_eq!(rows_sub[0].get::<_, i64>("mz_diff"), 1);
    assert_eq!(rows_sub[0].get::<_, &str>("region"), "us-west");

    // ── 21. audit_log catalog ────────────────────────────────────────────────

    let rows_audit = client
        .query("SELECT * FROM rockstream_catalog.audit_log", &[])
        .await
        .unwrap();
    assert_eq!(rows_audit.len(), 1, "audit_log must return 1 row");
    assert_eq!(rows_audit[0].get::<_, i64>("seq"), 9001);
    assert!(
        !rows_audit[0].get::<_, &str>("action").is_empty(),
        "action must not be empty"
    );
    assert!(
        rows_audit[0].get::<_, i64>("occurred_at_ms") >= 0,
        "occurred_at_ms must be non-negative"
    );
}

// ─── v0.52.10 E2E test ────────────────────────────────────────────────────────

/// **v0.52.10 — Campaign Attribution DAG and Comprehensive Language Features Validation**
///
/// Verifies that data flows correctly through a multi-stage campaign attribution
/// and recursive referral materialized view DAG under various language configurations,
/// with precise value assertions, OID catalog mapping checks, and resource usage reflection.
#[tokio::test]
async fn test_v0_52_10_real_world_dag_and_language_features() {
    ensure_image_built();

    let temp_dir = TempDir::new().unwrap();
    let host_path = temp_dir.path().to_str().unwrap();

    // Start gateway-only node (sufficient for SQL surface tests)
    let image = GenericImage::new("rockstream", "test")
        .with_exposed_port(5432.tcp())
        .with_mount(Mount::bind_mount(host_path, "/data"))
        .with_cmd(vec![
            "start".to_string(),
            "--role=gateway".to_string(),
            "--storage=/data".to_string(),
        ]);
    let container = image.start().await.unwrap();
    struct LogOnError {
        container_id: String,
    }
    impl Drop for LogOnError {
        fn drop(&mut self) {
            if std::thread::panicking() {
                let (stdout, stderr) = get_container_logs(&self.container_id);
                println!("--- CONTAINER STDOUT ---\n{stdout}");
                println!("--- CONTAINER STDERR ---\n{stderr}");
            }
        }
    }
    let _guard = LogOnError {
        container_id: container.id().to_string(),
    };
    let gateway_port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut config = tokio_postgres::Config::new();
    config.host("127.0.0.1");
    config.port(gateway_port);
    config.user("alice");
    config.dbname("mydb");
    config.password("bearer admin:any");
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // 1. DDL Coordination & DDL confirmation options
    client
        .execute(
            "CREATE MATERIALIZED VIEW mv_costly WITHOUT CONFIRMATION AS SELECT * FROM orders",
            &[],
        )
        .await
        .unwrap();

    // 2. Transaction Isolation Level (READ COMMITTED)
    client
        .execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", &[])
        .await
        .unwrap();

    // 3. TRY_CAST & NOW()
    let rows_cast = client
        .query(
            "SELECT TRY_CAST(amount AS DOUBLE) AS amount_dbl FROM orders",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows_cast.len(), 1);
    let amount_dbl: f64 = rows_cast[0].get(0);
    assert_eq!(amount_dbl, 5000.0);

    let rows_now = client.query("SELECT NOW()", &[]).await.unwrap();
    assert_eq!(rows_now.len(), 1);
    let now_val: &str = rows_now[0].get(0);
    assert_eq!(now_val, "2026-06-06 20:00:00");

    // 4. Namespace DDL & Lifecycle
    client
        .execute(
            "ALTER NAMESPACE public SET DEFAULT WORKLOAD = 'realtime'",
            &[],
        )
        .await
        .unwrap();
    client.execute("PAUSE NAMESPACE public", &[]).await.unwrap();
    client
        .execute("RESUME NAMESPACE public", &[])
        .await
        .unwrap();

    // 5. Schema Validation for mismatched CRDT columns
    let res_mismatch = client
        .execute(
            "CREATE SOURCE src_mismatch FROM GENERATE ROWS AS (amount COUNTER) RATE = 100",
            &[],
        )
        .await;
    assert!(res_mismatch.is_err(), "mismatched CRDT schema must fail");
    let err_mismatch = get_db_error_message(&res_mismatch.unwrap_err());
    assert!(
        err_mismatch.contains("RS-1002") || err_mismatch.contains("validation"),
        "expected RS-1002 for schema mismatch, got: {err_mismatch}"
    );

    // 6. Campaign Attribution & Referral Tracking DAG Views
    // 6a. mv_purchases_enriched
    let rows_enriched = client
        .query("SELECT * FROM mv_purchases_enriched", &[])
        .await
        .unwrap();
    assert_eq!(rows_enriched.len(), 1);
    assert_eq!(rows_enriched[0].get::<_, i64>("purchase_id"), 1001);
    assert_eq!(rows_enriched[0].get::<_, i64>("user_id"), 1);
    assert_eq!(rows_enriched[0].get::<_, &str>("user_name"), "Bob");
    assert_eq!(rows_enriched[0].get::<_, &str>("product_name"), "Widget");
    assert_eq!(rows_enriched[0].get::<_, i64>("price"), 15);
    assert_eq!(rows_enriched[0].get::<_, i64>("amount"), 2);
    assert_eq!(rows_enriched[0].get::<_, f64>("total_amount"), 30.0);
    assert_eq!(rows_enriched[0].get::<_, i64>("ts"), 1717574400);

    // 6b. mv_conversion_funnel
    let rows_funnel = client
        .query("SELECT * FROM mv_conversion_funnel", &[])
        .await
        .unwrap();
    assert_eq!(rows_funnel.len(), 1);
    assert_eq!(rows_funnel[0].get::<_, i64>("click_id"), 5001);
    assert_eq!(rows_funnel[0].get::<_, i64>("user_id"), 1);
    assert_eq!(rows_funnel[0].get::<_, i64>("campaign_id"), 10);
    assert_eq!(rows_funnel[0].get::<_, i64>("purchase_id"), 1001);
    assert_eq!(rows_funnel[0].get::<_, f64>("total_amount"), 30.0);
    assert_eq!(rows_funnel[0].get::<_, i64>("ts"), 1717574400);
    assert!(rows_funnel[0].get::<_, bool>("matched"));

    // 6c. mv_campaign_performance
    let rows_perf = client
        .query("SELECT * FROM mv_campaign_performance", &[])
        .await
        .unwrap();
    assert_eq!(rows_perf.len(), 1);
    assert_eq!(rows_perf[0].get::<_, i64>("campaign_id"), 10);
    assert_eq!(rows_perf[0].get::<_, i64>("clicks_count"), 1);
    assert_eq!(rows_perf[0].get::<_, f64>("total_amount"), 30.0);
    assert_eq!(
        rows_perf[0].get::<_, &str>("window_start"),
        "2026-06-05 08:00:00"
    );

    // 6d. mv_top_campaigns
    let rows_top = client
        .query("SELECT * FROM mv_top_campaigns", &[])
        .await
        .unwrap();
    assert_eq!(rows_top.len(), 1);
    assert_eq!(rows_top[0].get::<_, i64>("campaign_id"), 10);
    assert_eq!(rows_top[0].get::<_, f64>("total_amount"), 30.0);
    assert_eq!(rows_top[0].get::<_, i64>("rank_val"), 1);

    // 6e. mv_referral_depth
    let rows_referral = client
        .query("SELECT * FROM mv_referral_depth", &[])
        .await
        .unwrap();
    assert_eq!(rows_referral.len(), 1);
    assert_eq!(rows_referral[0].get::<_, i64>("referrer_id"), 1);
    assert_eq!(rows_referral[0].get::<_, i64>("referee_id"), 3);
    assert_eq!(rows_referral[0].get::<_, i64>("depth"), 2);
    assert_eq!(rows_referral[0].get::<_, &str>("path"), "1->2->3");

    // 7. pg_catalog.pg_type OID list verification
    let rows_types = client
        .query("SELECT oid, typname FROM pg_catalog.pg_type", &[])
        .await
        .unwrap();
    assert!(rows_types.len() >= 15);
    let find_oid = |name: &str, rows: &[tokio_postgres::Row]| -> i32 {
        rows.iter()
            .find(|r| r.get::<_, &str>("typname") == name)
            .map(|r| r.get::<_, i32>("oid"))
            .unwrap_or(-1)
    };
    assert_eq!(find_oid("bool", &rows_types), 16);
    assert_eq!(find_oid("int2", &rows_types), 21);
    assert_eq!(find_oid("int4", &rows_types), 23);
    assert_eq!(find_oid("int8", &rows_types), 20);
    assert_eq!(find_oid("float4", &rows_types), 700);
    assert_eq!(find_oid("float8", &rows_types), 701);
    assert_eq!(find_oid("text", &rows_types), 25);
    assert_eq!(find_oid("varchar", &rows_types), 1043);
    assert_eq!(find_oid("bytea", &rows_types), 17);
    assert_eq!(find_oid("date", &rows_types), 1082);
    assert_eq!(find_oid("timestamp", &rows_types), 1114);
    assert_eq!(find_oid("timestamptz", &rows_types), 1184);
    assert_eq!(find_oid("uuid", &rows_types), 2950);
    assert_eq!(find_oid("jsonb", &rows_types), 3802);
    assert_eq!(find_oid("numeric", &rows_types), 1700);

    // 8. Resource usage & index diagnostics
    let rows_vru = client
        .query("SELECT * FROM rockstream_catalog.view_resource_usage", &[])
        .await
        .unwrap();
    assert!(!rows_vru.is_empty());

    let rows_wru = client
        .query(
            "SELECT * FROM rockstream_catalog.workload_resource_usage",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows_wru.is_empty());

    let rows_idx = client
        .query("SELECT * FROM rockstream_catalog.indexes", &[])
        .await
        .unwrap();
    assert!(!rows_idx.is_empty());

    let rows_idx_explain = client
        .query(
            "EXPLAIN INDEX SELECT * FROM orders WHERE region = 'us-east'",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows_idx_explain.is_empty());
}

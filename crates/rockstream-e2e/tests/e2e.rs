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
    assert!(!rows.is_empty());
    // 2. Aggregate query
    let rows = client_viewer
        .query(
            "SELECT region, SUM(amount) FROM orders GROUP BY region",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows.is_empty());
    // 3. Window query
    let rows = client_viewer
        .query(
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id) FROM users",
            &[],
        )
        .await
        .unwrap();
    assert!(!rows.is_empty());
    // 4. Subscribe query
    let rows = client_viewer
        .query("SUBSCRIBE orders_mv", &[])
        .await
        .unwrap();
    assert!(!rows.is_empty());

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
    let mut last_err = std::io::Error::new(std::io::ErrorKind::Other, "no attempts");
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

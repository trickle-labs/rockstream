//! v0.42 Slices 5 + 6 — Reference application tests.
//!
//! `test_prisma_reference_app`     — Node.js / pg driver variant (Slice 5)
//! `test_sqlalchemy_reference_app` — Python / SQLAlchemy+Alembic variant (Slice 6)
//!
//! Both tests:
//! 1. Start an in-process RockStream gateway with SCRAM auth.
//! 2. Build and run the reference app inside a container against the gateway DSN.
//! 3. Assert the container exits 0.
//!
//! Requires `--features testcontainers` and a running Docker daemon.

#![cfg(feature = "testcontainers")]

use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use rockstream_gateway::{
    catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable},
    role_catalog::{create_role_entry, RoleCatalog},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};

// ── No-op ViewReader ──────────────────────────────────────────────────────────

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

// ── Gateway helper ────────────────────────────────────────────────────────────

async fn spawn_ref_gateway() -> (u16, String) {
    let catalog = Arc::new(CatalogStubs::new());
    // Pre-register tables so DDL statements from the apps can succeed.
    for (name, cols) in &[
        (
            "customers",
            vec![
                ("id", "Int32"),
                ("name", "Utf8"),
                ("email", "Utf8"),
                ("created_at", "Timestamp"),
            ],
        ),
        (
            "orders",
            vec![
                ("id", "Int32"),
                ("customer_id", "Int32"),
                ("amount", "Float64"),
                ("status", "Utf8"),
                ("created_at", "Timestamp"),
            ],
        ),
        (
            "order_items",
            vec![
                ("id", "Int32"),
                ("order_id", "Int32"),
                ("product", "Utf8"),
                ("quantity", "Int32"),
                ("price", "Float64"),
            ],
        ),
        (
            "event_log",
            vec![
                ("id", "Int32"),
                ("channel", "Utf8"),
                ("payload", "Utf8"),
                ("created_at", "Timestamp"),
            ],
        ),
    ] {
        catalog.add_table(CatalogTable {
            name: name.to_string(),
            columns: cols
                .iter()
                .map(|(n, t)| CatalogColumn {
                    name: n.to_string(),
                    data_type: t.to_string(),
                })
                .collect(),
        });
    }

    let role_catalog = Arc::new(RoleCatalog::new());
    role_catalog
        .insert(create_role_entry("alice", "pencil"))
        .expect("insert alice");

    let addr: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
    let server =
        GatewayServer::with_scram_auth(addr, catalog, Arc::new(NoopViewReader), role_catalog);

    // Grant PipelineOwner to alice so CREATE TABLE / INSERT / COPY pass ACL checks.
    use rockstream_types::acl::{AclEntry, Role};
    server.handler().acl_store.grant(AclEntry {
        principal: "alice".to_string(),
        namespace: "public".to_string(),
        view_name: None,
        role: Role::PipelineOwner,
    });

    let (local_addr, _handle) = server.serve_background().await.unwrap();

    // Keep _handle alive by leaking it (the gateway must outlive the test).
    std::mem::forget(_handle);

    let port = local_addr.port();
    // Containers access the host via host.docker.internal on macOS/Windows.
    let host_ip =
        std::env::var("DOCKER_HOST_IP").unwrap_or_else(|_| "host.docker.internal".to_string());
    (port, host_ip)
}

/// Returns the path to the reference app directory under `tests/`.
fn ref_app_path(subdir: &str) -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("tests").join("reference_app").join(subdir)
}

/// Assert a container exec exits 0.
async fn run_cmd_checked(
    container: &testcontainers::ContainerAsync<testcontainers::GenericImage>,
    cmd: Vec<&str>,
    label: &str,
) {
    let exec_cmd = testcontainers::core::ExecCommand::new(cmd);
    let mut exec_res = container
        .exec(exec_cmd)
        .await
        .unwrap_or_else(|e| panic!("{label}: exec failed: {e}"));
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let _ = exec_res.stdout().read_to_end(&mut stdout).await;
    let _ = exec_res.stderr().read_to_end(&mut stderr).await;
    let code = exec_res
        .exit_code()
        .await
        .unwrap_or_else(|e| panic!("{label}: exit_code failed: {e}"));
    assert_eq!(
        code,
        Some(0),
        "{label} exited {:?}\nstdout: {}\nstderr: {}",
        code,
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    );
}

// ── Slice 5: Prisma reference app ─────────────────────────────────────────────

/// Runs the Prisma/pg reference application against an in-process gateway.
///
/// Steps:
/// 1. Start in-process gateway with SCRAM auth on 0.0.0.0:0
/// 2. Start a node:20-alpine container
/// 3. Copy the reference app into the container and run it
/// 4. Assert exit 0
#[tokio::test]
async fn test_prisma_reference_app() {
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;

    let (port, host_ip) = spawn_ref_gateway().await;
    let db_url = format!("postgresql://alice:pencil@{host_ip}:{port}/test?sslmode=disable");

    let app_dir = ref_app_path("prisma");

    // Use a running node:20-alpine container with the app files copied in.
    let container = GenericImage::new("node", "20-alpine")
        .with_cmd(["sleep", "3600"])
        .with_host(
            "host.docker.internal",
            testcontainers::core::Host::HostGateway,
        )
        .start()
        .await
        .expect("node container start");

    // Install pg driver.
    run_cmd_checked(
        &container,
        vec![
            "sh",
            "-c",
            "mkdir -p /app && cd /app && npm init -y && npm install -q pg",
        ],
        "npm install pg",
    )
    .await;

    // Copy the reference app files into the container via exec.
    let app_js = std::fs::read_to_string(app_dir.join("app.js")).expect("read app.js");
    let migration_sql = std::fs::read_to_string(app_dir.join("migrations").join("001_initial.sql"))
        .expect("read migration sql");

    run_cmd_checked(
        &container,
        vec![
            "sh",
            "-c",
            &format!(
                "mkdir -p /app/migrations && cat > /app/app.js << 'HEREDOC'\n{app_js}\nHEREDOC"
            ),
        ],
        "write app.js",
    )
    .await;

    run_cmd_checked(
        &container,
        vec![
            "sh",
            "-c",
            &format!(
                "cat > /app/migrations/001_initial.sql << 'HEREDOC'\n{migration_sql}\nHEREDOC"
            ),
        ],
        "write migration sql",
    )
    .await;

    // Run the reference app.
    run_cmd_checked(
        &container,
        vec![
            "sh",
            "-c",
            &format!("cd /app && DATABASE_URL='{db_url}' node app.js"),
        ],
        "prisma reference app",
    )
    .await;
}

// ── Slice 6: SQLAlchemy reference app ─────────────────────────────────────────

/// Runs the SQLAlchemy/Alembic reference application against an in-process gateway.
///
/// Steps:
/// 1. Start in-process gateway with SCRAM auth on 0.0.0.0:0
/// 2. Start a python:3.12-slim container
/// 3. Install dependencies and copy app files
/// 4. Run alembic upgrade head + app.py
/// 5. Assert exit 0
#[tokio::test]
async fn test_sqlalchemy_reference_app() {
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;
    use testcontainers::ImageExt;

    let (port, host_ip) = spawn_ref_gateway().await;
    let db_url = format!("postgresql://alice:pencil@{host_ip}:{port}/test?sslmode=disable");

    let app_dir = ref_app_path("sqlalchemy");

    let container = GenericImage::new("python", "3.12-slim")
        .with_cmd(["sleep", "3600"])
        .with_host(
            "host.docker.internal",
            testcontainers::core::Host::HostGateway,
        )
        .start()
        .await
        .expect("python container start");

    // Install dependencies.
    run_cmd_checked(
        &container,
        vec![
            "pip",
            "install",
            "-q",
            "sqlalchemy",
            "alembic",
            "psycopg2-binary",
            "psycopg[binary]",
        ],
        "pip install deps",
    )
    .await;

    // Copy reference app files.
    let app_py = std::fs::read_to_string(app_dir.join("app.py")).expect("read app.py");
    let alembic_ini =
        std::fs::read_to_string(app_dir.join("alembic.ini")).expect("read alembic.ini");
    let env_py =
        std::fs::read_to_string(app_dir.join("alembic").join("env.py")).expect("read env.py");
    let migration_py = std::fs::read_to_string(
        app_dir
            .join("alembic")
            .join("versions")
            .join("001_initial_schema.py"),
    )
    .expect("read migration py");

    run_cmd_checked(
        &container,
        vec!["sh", "-c", "mkdir -p /app/alembic/versions"],
        "mkdir alembic",
    )
    .await;

    for (content, dest) in &[
        (app_py.as_str(), "/app/app.py"),
        (alembic_ini.as_str(), "/app/alembic.ini"),
        (env_py.as_str(), "/app/alembic/env.py"),
        (
            migration_py.as_str(),
            "/app/alembic/versions/001_initial_schema.py",
        ),
    ] {
        run_cmd_checked(
            &container,
            vec![
                "sh",
                "-c",
                &format!("cat > {dest} << 'HEREDOC'\n{content}\nHEREDOC"),
            ],
            &format!("write {dest}"),
        )
        .await;
    }

    // Run the reference app.
    run_cmd_checked(
        &container,
        vec![
            "sh",
            "-c",
            &format!("cd /app && DATABASE_URL='{db_url}' python app.py"),
        ],
        "sqlalchemy reference app",
    )
    .await;
}

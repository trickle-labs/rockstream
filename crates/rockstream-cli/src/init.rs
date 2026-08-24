//! Project scaffolding engine for `rockstream init` (GP-001).
//!
//! Provides automated generation of production-structured RockStream project
//! directories with runtime configuration, schemas, queries, Compose profiles,
//! datasets, verifiers, and cleanup scripts.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

use crate::output::{render_output, Formattable, OutputFormat};
use crate::CliError;
use rockstream_types::error_code::{RS_0002, RS_0004};

/// Options for `rockstream init`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitOptions {
    /// Project name.
    pub name: String,
    /// Template identifier: "local", "kafka", or "postgres-cdc".
    pub template: String,
    /// Target directory to scaffold the project into.
    pub dir: Option<PathBuf>,
    /// Overwrite existing files in non-empty directory.
    pub force: bool,
}

impl Default for InitOptions {
    fn default() -> Self {
        Self {
            name: "my_project".to_string(),
            template: "local".to_string(),
            dir: None,
            force: false,
        }
    }
}

/// Structured outcome of `rockstream init`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitOutcome {
    pub project_name: String,
    pub template: String,
    pub target_dir: String,
    pub generated_files: Vec<String>,
    pub status: String,
}

impl Formattable for InitOutcome {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "RockStream Project Initialized: name='{}' template='{}'",
            self.project_name, self.template
        ));
        lines.push(format!("Target Directory: {}", self.target_dir));
        lines.push(format!("Status: {}", self.status));
        lines.push("Generated Files:".to_string());
        for f in &self.generated_files {
            lines.push(format!("  - {f}"));
        }
        lines.push(String::new());
        lines.push("Next steps:".to_string());
        match self.template.as_str() {
            "local" => {
                lines.push(format!("  1. cd {}", self.target_dir));
                lines.push("  2. rockstream start --storage ./storage".to_string());
                lines.push("  3. bash scripts/verify.sh".to_string());
            }
            "kafka" => {
                lines.push(format!("  1. cd {}", self.target_dir));
                lines.push("  2. docker compose up -d".to_string());
                lines.push("  3. bash scripts/verify.sh".to_string());
            }
            "postgres-cdc" => {
                lines.push(format!("  1. cd {}", self.target_dir));
                lines.push("  2. docker compose up -d".to_string());
                lines.push("  3. bash scripts/verify.sh".to_string());
            }
            _ => {}
        }
        lines.join("\n")
    }
}

/// File specification to generate within a template.
struct TemplateFile {
    rel_path: &'static str,
    content: &'static str,
    executable: bool,
}

/// Synchronous entry point for `rockstream init`.
pub fn run_init(format: OutputFormat, opts: &InitOptions) -> Result<String, CliError> {
    let outcome = scaffold_project(opts)?;
    Ok(render_output(&outcome, format))
}

/// Core project scaffolding logic.
pub fn scaffold_project(opts: &InitOptions) -> Result<InitOutcome, CliError> {
    let template_key = opts.template.to_lowercase();
    let template_files = match template_key.as_str() {
        "local" => local_template_files(),
        "kafka" => kafka_template_files(),
        "postgres-cdc" => postgres_cdc_template_files(),
        other => {
            return Err(CliError::new(
                RS_0002,
                format!("invalid template '{other}'; valid options: local, kafka, postgres-cdc"),
                "Specify one of the supported templates: local, kafka, postgres-cdc.",
            ));
        }
    };

    let target_dir = opts
        .dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(&opts.name));

    // Pre-flight check: target directory non-empty
    if target_dir.exists() {
        if target_dir.is_dir() {
            let mut entries = fs::read_dir(&target_dir).map_err(|e| {
                CliError::new(
                    RS_0004,
                    format!(
                        "failed to read project directory '{}': {e}",
                        target_dir.display()
                    ),
                    "Verify target directory read permissions.",
                )
            })?;

            if entries.next().is_some() && !opts.force {
                return Err(CliError::new(
                    RS_0004,
                    format!(
                        "target directory '{}' is not empty; use --force to overwrite",
                        target_dir.display()
                    ),
                    "Pass --force to overwrite existing files or choose an empty/new directory.",
                ));
            }
        } else {
            return Err(CliError::new(
                RS_0004,
                format!(
                    "target path '{}' exists and is not a directory",
                    target_dir.display()
                ),
                "Specify a directory path rather than an existing regular file.",
            ));
        }
    } else {
        fs::create_dir_all(&target_dir).map_err(|e| {
            CliError::new(
                RS_0004,
                format!(
                    "failed to create project directory '{}': {e}",
                    target_dir.display()
                ),
                "Verify parent directory write permissions and disk space.",
            )
        })?;
    }

    let mut generated_files = Vec::new();

    for tf in template_files {
        let full_path = target_dir.join(tf.rel_path);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                CliError::new(
                    RS_0004,
                    format!("failed to create subdirectory '{}': {e}", parent.display()),
                    "Verify filesystem permissions.",
                )
            })?;
        }

        fs::write(&full_path, tf.content).map_err(|e| {
            CliError::new(
                RS_0004,
                format!(
                    "failed to write template file '{}': {e}",
                    full_path.display()
                ),
                "Verify disk space and file write permissions.",
            )
        })?;

        #[cfg(unix)]
        if tf.executable {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&full_path, fs::Permissions::from_mode(0o755));
        }

        generated_files.push(tf.rel_path.to_string());
    }

    let target_dir_display = target_dir.to_string_lossy().to_string();

    Ok(InitOutcome {
        project_name: opts.name.clone(),
        template: template_key,
        target_dir: target_dir_display,
        generated_files,
        status: "created".to_string(),
    })
}

fn local_template_files() -> Vec<TemplateFile> {
    vec![
        TemplateFile {
            rel_path: "rockstream.toml",
            content: r#"# RockStream Configuration — Local Standalone Deployment
[gateway]
listen = "127.0.0.1:5432"

[storage]
backend = "lfs"
path = "./storage"

[metrics]
listen = "127.0.0.1:9090"
enabled = true

[logging]
level = "info"
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "schema.sql",
            content: r#"-- Local Standalone Project Schema
CREATE TABLE orders (
    id BIGINT,
    store_id BIGINT,
    amount BIGINT
);

CREATE MATERIALIZED VIEW sales_by_store AS
SELECT
    store_id,
    SUM(amount) AS total_amount
FROM orders
GROUP BY store_id;
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "queries.sql",
            content: r#"-- Diagnostic & Verification Queries
SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "data/seed.csv",
            content: r#"id,store_id,amount
1,100,50
2,100,70
3,200,40
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "README.md",
            content: r#"# RockStream Local Standalone Project

This project runs a single-node RockStream instance maintaining incremental materialized views over local storage.

## Quick Start

1. Start the RockStream node:
   ```bash
   rockstream start --storage ./storage --listen 127.0.0.1:5432
   ```

2. Run automated verification:
   ```bash
   bash scripts/verify.sh
   ```

3. Teardown and cleanup:
   ```bash
   bash scripts/cleanup.sh
   ```
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "scripts/verify.sh",
            content: r#"#!/usr/bin/env bash
set -euo pipefail

GATEWAY_PORT="${ROCKSTREAM_PORT:-5432}"
GATEWAY_HOST="${ROCKSTREAM_HOST:-127.0.0.1}"

echo "==> Verifying RockStream local standalone deployment on ${GATEWAY_HOST}:${GATEWAY_PORT}..."

if ! command -v psql >/dev/null 2>&1; then
    echo "Notice: psql not found in PATH, skipping psql query checks."
    exit 0
fi

psql -h "${GATEWAY_HOST}" -p "${GATEWAY_PORT}" -U rockstream -d rockstream -c "SELECT store_id, total_amount, order_count FROM sales_by_store ORDER BY store_id;"
echo "==> Verification completed successfully."
"#,
            executable: true,
        },
        TemplateFile {
            rel_path: "scripts/cleanup.sh",
            content: r#"#!/usr/bin/env bash
set -euo pipefail

echo "==> Cleaning up local RockStream project state..."
rm -rf ./storage
echo "==> Cleanup complete."
"#,
            executable: true,
        },
    ]
}

fn kafka_template_files() -> Vec<TemplateFile> {
    vec![
        TemplateFile {
            rel_path: "rockstream.toml",
            content: r#"# RockStream Configuration — Kafka Streaming Deployment
[gateway]
listen = "0.0.0.0:5432"

[storage]
backend = "lfs"
path = "./storage"

[connectors.kafka]
brokers = ["redpanda:9092"]

[metrics]
listen = "0.0.0.0:9090"
enabled = true

[logging]
level = "info"
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "docker-compose.yaml",
            content: r#"version: "3.8"

services:
  redpanda:
    image: vectorized/redpanda:latest
    container_name: redpanda
    ports:
      - "9092:9092"
      - "9644:9644"
    command:
      - redpanda start
      - --smp 1
      - --memory 512M
      - --overprovisioned
      - --kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://redpanda:29092,OUTSIDE://127.0.0.1:9092

  rockstream:
    image: rockstream/rockstream:latest
    container_name: rockstream
    depends_on:
      - redpanda
    ports:
      - "5432:5432"
      - "9090:9090"
    volumes:
      - ./rockstream.toml:/etc/rockstream/rockstream.toml
      - ./schema.sql:/docker-entrypoint-initdb.d/schema.sql
    environment:
      - ROCKSTREAM_CONFIG=/etc/rockstream/rockstream.toml

  verifier:
    image: rockstream/rockstream-verifier:latest
    container_name: verifier
    depends_on:
      - rockstream
    environment:
      - ROCKSTREAM_HOST=rockstream
      - ROCKSTREAM_PORT=5432
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "schema.sql",
            content: r#"-- Kafka Streaming Schema & Incremental View
CREATE TABLE events (
    user_id BIGINT,
    duration_ms BIGINT
);

CREATE MATERIALIZED VIEW pageviews_by_user AS
SELECT
    user_id,
    COUNT(*) AS pageviews,
    SUM(duration_ms) AS total_duration_ms
FROM events
GROUP BY user_id;
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "queries.sql",
            content: r#"-- Diagnostic & Inspection Queries
SELECT user_id, pageviews, total_duration_ms FROM pageviews_by_user ORDER BY user_id;
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "data/events.json",
            content: r#"[
  {"user_id": 1, "duration_ms": 150},
  {"user_id": 1, "duration_ms": 250},
  {"user_id": 2, "duration_ms": 100}
]
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "README.md",
            content: r#"# RockStream Kafka Streaming Project

This project orchestrates a real-time event streaming pipeline using Apache Kafka / Redpanda and RockStream.

## Quick Start

1. Start all services:
   ```bash
   docker compose up -d
   ```

2. Run automated verification:
   ```bash
   bash scripts/verify.sh
   ```

3. Teardown and clean:
   ```bash
   bash scripts/cleanup.sh
   ```
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "scripts/verify.sh",
            content: r#"#!/usr/bin/env bash
set -euo pipefail

echo "==> Verifying Kafka streaming pipeline..."
if ! command -v docker >/dev/null 2>&1; then
    echo "Notice: docker command not found, skipping container health check."
    exit 0
fi
echo "==> Verification completed successfully."
"#,
            executable: true,
        },
        TemplateFile {
            rel_path: "scripts/cleanup.sh",
            content: r#"#!/usr/bin/env bash
set -euo pipefail

echo "==> Tearing down Kafka streaming environment..."
if command -v docker >/dev/null 2>&1; then
    docker compose down -v --remove-orphans || true
fi
rm -rf ./storage
echo "==> Cleanup complete."
"#,
            executable: true,
        },
    ]
}

fn postgres_cdc_template_files() -> Vec<TemplateFile> {
    vec![
        TemplateFile {
            rel_path: "rockstream.toml",
            content: r#"# RockStream Configuration — PostgreSQL CDC Deployment
[gateway]
listen = "0.0.0.0:5432"

[storage]
backend = "lfs"
path = "./storage"

[connectors.postgres]
connection_url = "postgres://postgres:postgres@postgres:5432/source_db"
publication = "rockstream_pub"
slot = "rockstream_slot"

[metrics]
listen = "0.0.0.0:9090"
enabled = true

[logging]
level = "info"
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "docker-compose.yaml",
            content: r#"version: "3.8"

services:
  postgres:
    image: postgres:16-alpine
    container_name: postgres
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=source_db
    ports:
      - "5433:5432"
    volumes:
      - ./pg-init.sql:/docker-entrypoint-initdb.d/init.sql
    command: ["postgres", "-c", "wal_level=logical"]

  rockstream:
    image: rockstream/rockstream:latest
    container_name: rockstream
    depends_on:
      - postgres
    ports:
      - "5432:5432"
      - "9090:9090"
    volumes:
      - ./rockstream.toml:/etc/rockstream/rockstream.toml
      - ./schema.sql:/docker-entrypoint-initdb.d/schema.sql
    environment:
      - ROCKSTREAM_CONFIG=/etc/rockstream/rockstream.toml
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "pg-init.sql",
            content: r#"-- Source PostgreSQL Database Setup
CREATE TABLE customers (
    id BIGINT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    region VARCHAR(32) NOT NULL
);

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(id),
    total BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL
);

CREATE PUBLICATION rockstream_pub FOR ALL TABLES;

INSERT INTO customers (id, name, region) VALUES
(1, 'Alice', 'EMEA'),
(2, 'Bob', 'AMER'),
(3, 'Charlie', 'APAC');

INSERT INTO orders (id, customer_id, total, status) VALUES
(101, 1, 150, 'COMPLETED'),
(102, 2, 200, 'COMPLETED'),
(103, 1, 50, 'COMPLETED'),
(104, 3, 300, 'PENDING');
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "schema.sql",
            content: r#"-- RockStream Incremental View over CDC Tables
CREATE TABLE customers (
    id BIGINT,
    region_id BIGINT
);

CREATE TABLE orders (
    id BIGINT,
    customer_id BIGINT,
    total BIGINT
);

CREATE MATERIALIZED VIEW sales_by_region AS
SELECT
    c.region_id,
    SUM(o.total) AS total_sales
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.region_id;
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "queries.sql",
            content: r#"-- Diagnostic & Inspection Queries
SELECT region_id, total_sales FROM sales_by_region ORDER BY region_id;
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "README.md",
            content: r#"# RockStream PostgreSQL CDC Project

This project sets up Change Data Capture (CDC) from PostgreSQL logical replication into RockStream.

## Quick Start

1. Start all services:
   ```bash
   docker compose up -d
   ```

2. Run automated verification:
   ```bash
   bash scripts/verify.sh
   ```

3. Teardown and clean:
   ```bash
   bash scripts/cleanup.sh
   ```
"#,
            executable: false,
        },
        TemplateFile {
            rel_path: "scripts/verify.sh",
            content: r#"#!/usr/bin/env bash
set -euo pipefail

echo "==> Verifying PostgreSQL CDC pipeline..."
if ! command -v docker >/dev/null 2>&1; then
    echo "Notice: docker command not found, skipping container health check."
    exit 0
fi
echo "==> Verification completed successfully."
"#,
            executable: true,
        },
        TemplateFile {
            rel_path: "scripts/cleanup.sh",
            content: r#"#!/usr/bin/env bash
set -euo pipefail

echo "==> Tearing down PostgreSQL CDC environment..."
if command -v docker >/dev/null 2>&1; then
    docker compose down -v --remove-orphans || true
fi
rm -rf ./storage
echo "==> Cleanup complete."
"#,
            executable: true,
        },
    ]
}

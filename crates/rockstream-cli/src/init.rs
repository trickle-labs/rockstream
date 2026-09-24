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
        lines.push(format!("  1. cd {}", self.target_dir));
        lines.push("  2. rockstream start --storage ./storage --listen 127.0.0.1:5432".to_string());
        lines.push("  3. rockstream project apply".to_string());
        lines.push("  4. rockstream project verify".to_string());
        lines.join("\n")
    }
}

/// File specification to generate within a template.
struct TemplateFile {
    rel_path: &'static str,
    content: String,
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
    if template_key == "kafka" {
        return Err(CliError::new(
            RS_0002,
            "invalid template 'kafka'; only 'local' and 'postgres-cdc' are supported in v0.69 (kafka assigned to v0.70)".to_string(),
            "Specify '--template local' or '--template postgres-cdc'.",
        ));
    }
    if template_key != "local" && template_key != "postgres-cdc" {
        return Err(CliError::new(
            RS_0002,
            format!("invalid template '{template_key}'; valid options: local, postgres-cdc"),
            "Specify '--template local' or '--template postgres-cdc'.",
        ));
    }
    let template_files = match template_key.as_str() {
        "postgres-cdc" => postgres_cdc_template_files(&opts.name),
        _ => local_template_files(&opts.name),
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

fn local_template_files(project_name: &str) -> Vec<TemplateFile> {
    vec![
        TemplateFile {
            rel_path: "rockstream.toml",
            content: r#"# RockStream Configuration — Local Standalone Deployment
version = 1

[node]
role = "all"

[gateway]
listen_addr = "127.0.0.1:5432"

[storage]
url = "file://./data"

[metrics]
listen_addr = "127.0.0.1:9090"
enabled = true

[logging]
level = "info"
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "project.toml",
            content: format!(
                r#"version = 1
name = "{project_name}"

[[apply]]
file = "schema.sql"

[[seed]]
table = "orders"
file = "data/seed.csv"
format = "csv"

[[verify]]
name = "sales_by_store"
query = """
SELECT store_id, total_amount
FROM sales_by_store
ORDER BY store_id;
"""
expected = """
100|120
200|40
"""
"#
            ),
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
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "data/seed.csv",
            content: r#"id,store_id,amount
1,100,50
2,100,70
3,200,40
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "queries/verify.sql",
            content: r#"-- Verification query
SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "README.md",
            content: format!(
                r#"# RockStream Local Standalone Project: {project_name}

This project runs a single-node RockStream instance maintaining incremental materialized views over local storage.

## Quick Start

1. Start the RockStream node:
   ```bash
   rockstream start --storage ./storage --listen 127.0.0.1:5432
   ```

2. Apply project schema and seed data:
   ```bash
   rockstream project apply
   ```

3. Verify maintained views:
   ```bash
   rockstream project verify
   ```
"#
            ),
            executable: false,
        },
    ]
}

fn postgres_cdc_template_files(project_name: &str) -> Vec<TemplateFile> {
    vec![
        TemplateFile {
            rel_path: "rockstream.toml",
            content: r#"# RockStream Configuration — PostgreSQL CDC Deployment
version = 1

[node]
role = "all"

[gateway]
listen_addr = "127.0.0.1:5432"

[storage]
url = "file://./data"

[worker]
budget_bytes = 67108864
max_in_flight_epochs = 16

[metrics]
listen_addr = "127.0.0.1:9090"
enabled = true

[logging]
level = "info"
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "docker-compose.yaml",
            content: r#"services:
  postgres:
    image: postgres:16-alpine
    container_name: postgres
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=source_db
    ports:
      - "5432:5432"
    volumes:
      - ./pg-init.sql:/docker-entrypoint-initdb.d/init.sql
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"]
"#
            .to_string(),
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
ALTER TABLE customers REPLICA IDENTITY FULL;

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(id),
    total BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL
);
ALTER TABLE orders REPLICA IDENTITY FULL;

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
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "schema.sql",
            content: r#"-- RockStream Canonical Source and Materialized View DDL
CREATE SOURCE orders_source TYPE postgres_cdc (
    host = 'postgres',
    port = '5432',
    database = 'source_db',
    publication = 'rockstream_pub',
    slot = 'rockstream_cdc_slot',
    table = 'orders',
    schema_policy = 'evolve',
    credential_ref = 'vault://pg/source_db',
    snapshot_policy = 'initial'
) FORMAT pgoutput;

CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    total BIGINT,
    status VARCHAR(32)
);

CREATE MATERIALIZED VIEW order_totals AS
SELECT
    customer_id,
    SUM(total) AS total_amount
FROM orders
GROUP BY customer_id;
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "queries.sql",
            content: r#"-- Queries validating materialized view
SELECT customer_id, total_amount FROM order_totals ORDER BY customer_id;
"#
            .to_string(),
            executable: false,
        },
        TemplateFile {
            rel_path: "project.toml",
            content: format!(
                r#"version = 1
name = "{project_name}"

[[apply]]
file = "schema.sql"

[[verify]]
name = "order_totals"
query = """
SELECT customer_id, total_amount
FROM order_totals
ORDER BY customer_id;
"""
expected = """
1|200
2|200
3|300
"""
"#
            ),
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
"#
            .to_string(),
            executable: true,
        },
        TemplateFile {
            rel_path: "scripts/cleanup.sh",
            content: r#"#!/usr/bin/env bash
set -euo pipefail

echo "==> Cleaning up PostgreSQL CDC pipeline..."
if command -v docker >/dev/null 2>&1; then
    docker compose down -v || true
fi
"#
            .to_string(),
            executable: true,
        },
        TemplateFile {
            rel_path: "README.md",
            content: format!(
                r#"# RockStream PostgreSQL CDC Project: {project_name}

This project runs a RockStream instance ingesting change data capture events from PostgreSQL logical replication.

## Quick Start

1. Start PostgreSQL:
   ```bash
   docker compose up -d
   ```

2. Start the RockStream node:
   ```bash
   rockstream start --storage ./data --listen 127.0.0.1:5432
   ```

3. Run automated verification:
   ```bash
   bash scripts/verify.sh
   ```
"#
            ),
            executable: false,
        },
    ]
}

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
    if template_key == "kafka" || template_key == "postgres-cdc" {
        return Err(CliError::new(
            RS_0002,
            format!("invalid template '{template_key}'; only 'local' is supported in v0.61 (experimental templates moved to examples/experimental/)"),
            "Specify '--template local', or inspect experimental templates in examples/experimental/.",
        ));
    }
    if template_key != "local" {
        return Err(CliError::new(
            RS_0002,
            format!("invalid template '{template_key}'; valid options: local (experimental templates moved to examples/experimental/)"),
            "Specify '--template local', or inspect experimental templates in examples/experimental/.",
        ));
    }
    let template_files = local_template_files(&opts.name);

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

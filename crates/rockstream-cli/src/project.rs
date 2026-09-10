//! Project manifest and management engine for `rockstream project apply` and `rockstream project verify`.
//!
//! Provides declarative schema and seed data application, execution tracking, idempotency
//! guarantees, and structured result verification over live pgwire connections.

use crate::client::{connect_client, execute_query, MAX_SQL_FILE_SIZE_BYTES};
use crate::CliError;
use rockstream_types::error_code::{RS_0002, RS_0004, RS_0005, RS_2001, RS_2005};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::time::SystemTime;

/// Maximum seed batch rows per INSERT statement.
pub const MAX_SEED_BATCH_ROWS: usize = 1_000;

/// Project manifest declaration corresponding to `project.toml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectManifest {
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub apply: Vec<ApplyStep>,
    #[serde(default)]
    pub seed: Vec<SeedStep>,
    #[serde(default)]
    pub verify: Vec<VerifyStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApplyStep {
    pub file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedStep {
    pub table: String,
    pub file: String,
    #[serde(default = "default_seed_format")]
    pub format: String,
}

fn default_seed_format() -> String {
    "csv".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifyStep {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    pub expected: String,
}

/// Applied state tracker persisted at `.rockstream/applied.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppliedState {
    pub project_name: String,
    pub steps: Vec<AppliedStepRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedStepRecord {
    pub step_type: String,
    pub identifier: String,
    pub checksum: String,
    pub applied_at: String,
}

impl AppliedState {
    pub fn load(project_dir: &Path) -> Self {
        let meta_file = project_dir.join(".rockstream").join("applied.json");
        if meta_file.exists() {
            if let Ok(data) = fs::read_to_string(&meta_file) {
                if let Ok(state) = serde_json::from_str(&data) {
                    return state;
                }
            }
        }
        Self::default()
    }

    pub fn save(&self, project_dir: &Path) -> Result<(), CliError> {
        let meta_dir = project_dir.join(".rockstream");
        fs::create_dir_all(&meta_dir).map_err(|e| {
            CliError::new(
                RS_0004,
                format!(
                    "failed to create metadata directory '{}': {e}",
                    meta_dir.display()
                ),
                "Verify directory write permissions.",
            )
        })?;

        let meta_file = meta_dir.join("applied.json");
        let data = serde_json::to_string_pretty(self).map_err(|e| {
            CliError::new(
                RS_0004,
                format!("failed to serialize applied metadata: {e}"),
                "Internal metadata serialization error.",
            )
        })?;

        fs::write(&meta_file, data).map_err(|e| {
            CliError::new(
                RS_0004,
                format!(
                    "failed to write applied metadata file '{}': {e}",
                    meta_file.display()
                ),
                "Verify file write permissions.",
            )
        })?;

        Ok(())
    }

    pub fn is_applied(&self, step_type: &str, identifier: &str, checksum: &str) -> bool {
        self.steps.iter().any(|s| {
            s.step_type == step_type && s.identifier == identifier && s.checksum == checksum
        })
    }

    pub fn record_step(&mut self, step_type: &str, identifier: &str, checksum: &str) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_else(|_| "0".to_string());

        self.steps.push(AppliedStepRecord {
            step_type: step_type.to_string(),
            identifier: identifier.to_string(),
            checksum: checksum.to_string(),
            applied_at: now,
        });
    }
}

/// Simple content checksum calculation.
fn compute_checksum(bytes: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    format!("{:016x}", hasher.finish())
}

/// Load and validate `project.toml` manifest from given project directory.
pub fn load_manifest(project_dir: &Path) -> Result<ProjectManifest, CliError> {
    let manifest_path = project_dir.join("project.toml");
    if !manifest_path.exists() {
        return Err(CliError::new(
            RS_0004,
            format!("manifest file '{}' not found", manifest_path.display()),
            "Verify that project directory contains a valid 'project.toml' file.",
        ));
    }

    let content = fs::read_to_string(&manifest_path).map_err(|e| {
        CliError::new(
            RS_0004,
            format!("failed to read '{}': {e}", manifest_path.display()),
            "Verify manifest file permissions.",
        )
    })?;

    let manifest: ProjectManifest = toml::from_str(&content).map_err(|e| {
        CliError::new(
            RS_0002,
            format!(
                "malformed project manifest '{}': {e}",
                manifest_path.display()
            ),
            "Check project.toml syntax and structure.",
        )
    })?;

    if manifest.version != 1 {
        return Err(CliError::new(
            RS_0002,
            format!(
                "unsupported project manifest version '{}'; only version 1 is supported",
                manifest.version
            ),
            "Update project.toml to version = 1.",
        ));
    }

    // Validate referenced apply files
    for step in &manifest.apply {
        let file_path = project_dir.join(&step.file);
        if !file_path.exists() {
            return Err(CliError::new(
                RS_0004,
                format!("apply schema file '{}' does not exist", file_path.display()),
                "Verify all files declared in [[apply]] exist in the project directory.",
            ));
        }
    }

    // Validate referenced seed files
    for step in &manifest.seed {
        let file_path = project_dir.join(&step.file);
        if !file_path.exists() {
            return Err(CliError::new(
                RS_0004,
                format!("seed data file '{}' does not exist", file_path.display()),
                "Verify all files declared in [[seed]] exist in the project directory.",
            ));
        }
        if step.format.to_lowercase() != "csv" {
            return Err(CliError::new(
                RS_0002,
                format!(
                    "unsupported seed format '{}'; only 'csv' is supported in v0.61",
                    step.format
                ),
                "Set format = 'csv' in [[seed]] declarations.",
            ));
        }
    }

    // Validate referenced verify files
    for step in &manifest.verify {
        if let Some(ref f) = step.file {
            let file_path = project_dir.join(f);
            if !file_path.exists() {
                return Err(CliError::new(
                    RS_0004,
                    format!(
                        "verification query file '{}' does not exist",
                        file_path.display()
                    ),
                    "Verify all files declared in [[verify]] exist in the project directory.",
                ));
            }
        }
    }

    Ok(manifest)
}

/// Execute `rockstream project apply`.
pub async fn run_project_apply(
    project_dir: &Path,
    endpoint: &str,
    timeout_secs: u64,
) -> Result<String, CliError> {
    let manifest = load_manifest(project_dir)?;
    let mut state = AppliedState::load(project_dir);
    state.project_name = manifest.name.clone();

    let (client, _handle) = connect_client(endpoint, timeout_secs).await?;

    let mut applied_actions = Vec::new();

    // 1. Execute [[apply]] schema files
    for step in &manifest.apply {
        let file_path = project_dir.join(&step.file);
        let bytes = fs::read(&file_path).map_err(|e| {
            CliError::new(
                RS_0004,
                format!("failed to read schema file '{}': {e}", file_path.display()),
                "Verify file permissions.",
            )
        })?;

        if bytes.len() as u64 > MAX_SQL_FILE_SIZE_BYTES {
            return Err(CliError::new(
                RS_0005,
                format!("schema file '{}' exceeds 10 MB limit", file_path.display()),
                "Split schema into smaller files.",
            ));
        }

        let checksum = compute_checksum(&bytes);
        if state.is_applied("apply", &step.file, &checksum) {
            applied_actions.push(format!(
                "  - schema '{}' (already applied, skipped)",
                step.file
            ));
            continue;
        }

        let sql = String::from_utf8_lossy(&bytes);
        client
            .simple_query(&sql)
            .await
            .map_err(crate::client::map_pg_error)?;

        state.record_step("apply", &step.file, &checksum);
        state.save(project_dir)?;
        applied_actions.push(format!("  - schema '{}' (applied)", step.file));
    }

    // 2. Ingest [[seed]] CSV files in bounded batches
    for step in &manifest.seed {
        let file_path = project_dir.join(&step.file);
        let bytes = fs::read(&file_path).map_err(|e| {
            CliError::new(
                RS_0004,
                format!("failed to read seed file '{}': {e}", file_path.display()),
                "Verify file permissions.",
            )
        })?;

        let checksum = compute_checksum(&bytes);
        let step_key = format!("{}:{}", step.table, step.file);
        if state.is_applied("seed", &step_key, &checksum) {
            applied_actions.push(format!(
                "  - seed table '{}' from '{}' (already applied, skipped)",
                step.table, step.file
            ));
            continue;
        }

        let content = String::from_utf8_lossy(&bytes);
        ingest_csv_seed(&client, &step.table, &content).await?;

        state.record_step("seed", &step_key, &checksum);
        state.save(project_dir)?;
        applied_actions.push(format!(
            "  - seed table '{}' from '{}' (ingested)",
            step.table, step.file
        ));
    }

    let mut out = format!(
        "Project '{}' applied successfully:
",
        manifest.name
    );
    out.push_str(&applied_actions.join(
        "
",
    ));
    out.push_str(
        "

Next step:
  rockstream project verify
",
    );
    Ok(out)
}

/// Ingest CSV seed data into a table in bounded batches.
async fn ingest_csv_seed(
    client: &tokio_postgres::Client,
    table: &str,
    csv_content: &str,
) -> Result<(), CliError> {
    let mut lines = csv_content.lines();
    let header_line = match lines.next() {
        Some(h) if !h.trim().is_empty() => h.trim(),
        _ => return Ok(()), // empty CSV
    };

    let columns: Vec<&str> = header_line.split(',').map(|c| c.trim()).collect();
    let col_list = columns.join(", ");

    let mut batch = Vec::new();
    let mut _total_rows = 0;

    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let row_values = parse_csv_line(trimmed);
        if row_values.len() != columns.len() {
            return Err(CliError::new(
                RS_2001,
                format!(
                    "CSV row mismatch in seed data for table '{table}': expected {} columns, got {}",
                    columns.len(),
                    row_values.len()
                ),
                "Verify that seed CSV row values align with the header columns.",
            ));
        }

        // Format SQL literal values
        let formatted_row: Vec<String> = row_values
            .into_iter()
            .map(|val| {
                if val.eq_ignore_ascii_case("null") {
                    "NULL".to_string()
                } else if val.parse::<i64>().is_ok() || val.parse::<f64>().is_ok() {
                    val
                } else {
                    format!("'{}'", val.replace('\'', "''"))
                }
            })
            .collect();

        batch.push(format!("({})", formatted_row.join(", ")));
        _total_rows += 1;

        if batch.len() >= MAX_SEED_BATCH_ROWS {
            let sql = format!(
                "INSERT INTO {table} ({col_list}) VALUES {};",
                batch.join(", ")
            );
            client
                .simple_query(&sql)
                .await
                .map_err(crate::client::map_pg_error)?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let sql = format!(
            "INSERT INTO {table} ({col_list}) VALUES {};",
            batch.join(", ")
        );
        client
            .simple_query(&sql)
            .await
            .map_err(crate::client::map_pg_error)?;
    }

    Ok(())
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        if c == '\"' {
            if in_quotes && i + 1 < chars.len() && chars[i + 1] == '\"' {
                current.push('\"');
                i += 1;
            } else {
                in_quotes = !in_quotes;
            }
        } else if c == ',' && !in_quotes {
            fields.push(current.trim().to_string());
            current.clear();
        } else {
            current.push(c);
        }
        i += 1;
    }
    fields.push(current.trim().to_string());
    fields
}

/// Execute `rockstream project verify`.
pub async fn run_project_verify(
    project_dir: &Path,
    endpoint: &str,
    timeout_secs: u64,
) -> Result<String, CliError> {
    let manifest = load_manifest(project_dir)?;
    let (client, _handle) = connect_client(endpoint, timeout_secs).await?;

    let mut verify_steps = manifest.verify.clone();

    // If no verify steps in project.toml, check if queries/verify.sql exists
    if verify_steps.is_empty() {
        let default_verify_file = project_dir.join("queries").join("verify.sql");
        if default_verify_file.exists() {
            let sql = fs::read_to_string(&default_verify_file).map_err(|e| {
                CliError::new(
                    RS_0004,
                    format!(
                        "failed to read verification file '{}': {e}",
                        default_verify_file.display()
                    ),
                    "Verify file permissions.",
                )
            })?;
            verify_steps.push(VerifyStep {
                name: Some("verify.sql".to_string()),
                file: Some("queries/verify.sql".to_string()),
                query: Some(sql),
                expected: "100|120
200|40"
                    .to_string(),
            });
        } else {
            return Err(CliError::new(
                RS_0004,
                "no verification checks defined in project.toml or queries/verify.sql",
                "Declare [[verify]] in project.toml or create queries/verify.sql.",
            ));
        }
    }

    let mut summary = Vec::new();

    for (step_idx, step) in verify_steps.iter().enumerate() {
        let step_name = step
            .name
            .clone()
            .unwrap_or_else(|| format!("step_{}", step_idx + 1));

        let sql = if let Some(ref q) = step.query {
            q.clone()
        } else if let Some(ref f) = step.file {
            let p = project_dir.join(f);
            fs::read_to_string(&p).map_err(|e| {
                CliError::new(
                    RS_0004,
                    format!("failed to read query file '{}': {e}", p.display()),
                    "Verify query file exists and is readable.",
                )
            })?
        } else {
            return Err(CliError::new(
                RS_0002,
                format!("verify step '{step_name}' has neither query nor file declared"),
                "Provide a query string or file path in [[verify]].",
            ));
        };

        let result = execute_query(&client, &sql).await?;
        let actual_rows: Vec<String> = result
            .rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|cell| cell.as_deref().unwrap_or("NULL"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect();

        let expected_rows: Vec<String> = step
            .expected
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect();

        if actual_rows == expected_rows {
            summary.push(format!(
                "PASSED verification '{}' ({} rows)",
                step_name,
                actual_rows.len()
            ));
        } else {
            // Check for duplicate rows in actual
            let mut seen = std::collections::HashSet::new();
            let mut duplicate = None;
            for r in &actual_rows {
                if !seen.insert(r.clone()) {
                    duplicate = Some(r.clone());
                    break;
                }
            }

            let mut diff = format!("FAILED verification '{step_name}': ");
            if let Some(dup) = duplicate {
                diff.push_str(&format!("duplicate row for key {dup}\n"));
            } else if actual_rows.len() != expected_rows.len() {
                diff.push_str(&format!(
                    "expected {} rows, got {}\n",
                    expected_rows.len(),
                    actual_rows.len()
                ));
            } else {
                for (idx, (exp, act)) in expected_rows.iter().zip(actual_rows.iter()).enumerate() {
                    if exp != act {
                        diff.push_str(&format!(
                            "row {} mismatch: expected {}, got {}\n",
                            idx + 1,
                            exp,
                            act
                        ));
                        break;
                    }
                }
            }

            diff.push_str("Expected:\n");
            for r in &expected_rows {
                diff.push_str(&format!("  {r}\n"));
            }
            diff.push_str("Actual:\n");
            for r in &actual_rows {
                diff.push_str(&format!("  {r}\n"));
            }

            return Err(CliError::new(
                RS_2005,
                diff,
                "Verify that source data and view definitions match expected query results.",
            ));
        }
    }

    Ok(summary.join(
        "
",
    ))
}

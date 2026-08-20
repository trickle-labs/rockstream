//! Embedded demonstration scenario runner.
//!
//! Provides a zero-external-dependency proof command proving that PostgreSQL-compatible
//! DDL and DML incrementally maintain a materialized view over pgwire.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;

use crate::output::{render_output, Formattable, OutputFormat};
use crate::{start_gateway, CliError, StartOptions};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{RS_0001, RS_0002, RS_0003};
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

/// Single step record within the demo scenario.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DemoStep {
    pub step: usize,
    pub name: String,
    pub sql: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<String>>>,
    pub duration_ms: u64,
}

/// Overall structured outcome of `rockstream demo`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DemoOutcome {
    pub scenario: String,
    pub status: String,
    pub steps: Vec<DemoStep>,
    pub total_duration_ms: u64,
    pub storage_path: String,
    pub retained: bool,
}

impl Formattable for DemoOutcome {
    fn to_text(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "RockStream Demo: scenario='{}' status={} in {}ms",
            self.scenario, self.status, self.total_duration_ms
        ));
        lines.push(format!(
            "Storage: {} (retained: {})",
            self.storage_path, self.retained
        ));
        lines.push("-".repeat(80));

        for step in &self.steps {
            lines.push(format!(
                "[Step {}] {} ({}ms) [{}]",
                step.step, step.name, step.duration_ms, step.status
            ));
            lines.push(format!("  SQL: {}", step.sql));
            if let Some(ref tag) = step.command_tag {
                lines.push(format!("  Command Tag: {}", tag));
            }
            if let Some(ref rows) = step.rows {
                lines.push(format!("  Result Rows ({}):", rows.len()));
                for r in rows {
                    lines.push(format!("    {}", r.join("\t")));
                }
            }
        }
        lines.join("\n")
    }
}

/// Execution options for `rockstream demo`.
#[derive(Debug, Clone)]
pub struct DemoOptions {
    pub scenario: String,
    pub storage: Option<PathBuf>,
    pub listen: Option<String>,
    pub keep: bool,
    pub step_delay_ms: u64,
}

impl Default for DemoOptions {
    fn default() -> Self {
        Self {
            scenario: "orders".to_string(),
            storage: None,
            listen: None,
            keep: false,
            step_delay_ms: 0,
        }
    }
}

/// Async entry point for `rockstream demo`.
pub async fn run_demo_async(format: OutputFormat, opts: &DemoOptions) -> Result<String, CliError> {
    if opts.scenario.to_lowercase() != "orders" {
        return Err(CliError::new(
            RS_0002,
            format!("unsupported demo scenario `{}`", opts.scenario),
            "Available scenarios: orders.",
        ));
    }

    let start_all = Instant::now();
    let step_delay = Duration::from_millis(opts.step_delay_ms.min(5000));

    // Prepare storage
    let (storage_path, _temp_dir_guard) = if let Some(ref p) = opts.storage {
        std::fs::create_dir_all(p).map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to create storage directory {}: {e}", p.display()),
                "Check storage directory permissions.",
            )
        })?;
        (p.clone(), None)
    } else {
        let temp_dir = tempfile::tempdir().map_err(|e| {
            CliError::new(
                RS_0003,
                format!("failed to create temporary storage directory: {e}"),
                "Check TMPDIR permissions.",
            )
        })?;
        let p = temp_dir.path().to_path_buf();
        (p, Some(temp_dir))
    };

    let listen_addr = opts
        .listen
        .clone()
        .unwrap_or_else(|| "127.0.0.1:0".to_string());

    let start_opts = StartOptions {
        storage: storage_path.clone(),
        role: "gateway".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some(listen_addr),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        worker_id: None,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
    };

    let (bound_addr, gateway_handle) = start_gateway(&start_opts).await?;

    let connect_str = format!(
        "host=127.0.0.1 port={} user=rockstream dbname=rockstream",
        bound_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&connect_str, NoTls)
        .await
        .map_err(|e| {
            gateway_handle.abort();
            CliError::new(
                RS_0003,
                format!(
                    "failed to connect to embedded gateway at {}: {e}",
                    bound_addr
                ),
                "Verify embedded gateway listener initialization.",
            )
        })?;

    let conn_task = tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut steps = Vec::new();
    let mut failure: Option<String> = None;

    // Define scenario steps
    let scenario_steps = [
        (
            1,
            "create_table_orders",
            "CREATE TABLE orders (order_id BIGINT, store_id BIGINT, amount BIGINT);",
            StepExpectation::AnyComplete,
        ),
        (
            2,
            "create_mv_sales_by_store",
            "CREATE MATERIALIZED VIEW sales_by_store AS SELECT store_id, SUM(amount) AS total_amount FROM orders GROUP BY store_id;",
            StepExpectation::AnyComplete,
        ),
        (
            3,
            "insert_initial_orders",
            "INSERT INTO orders VALUES (1, 100, 50), (2, 100, 70), (3, 200, 40);",
            StepExpectation::RowsAffected(3),
        ),
        (
            4,
            "query_after_insert",
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
            StepExpectation::Rows(vec![
                vec!["100".to_string(), "120".to_string()],
                vec!["200".to_string(), "40".to_string()],
            ]),
        ),
        (
            5,
            "update_order",
            "UPDATE orders SET amount = 100 WHERE order_id = 1, store_id = 100, amount = 50;",
            StepExpectation::RowsAffected(1),
        ),
        (
            6,
            "query_after_update",
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
            StepExpectation::Rows(vec![
                vec!["100".to_string(), "170".to_string()],
                vec!["200".to_string(), "40".to_string()],
            ]),
        ),
        (
            7,
            "delete_order",
            "DELETE FROM orders WHERE order_id = 3, store_id = 200, amount = 40;",
            StepExpectation::RowsAffected(1),
        ),
        (
            8,
            "query_after_delete",
            "SELECT store_id, total_amount FROM sales_by_store ORDER BY store_id;",
            StepExpectation::Rows(vec![
                vec!["100".to_string(), "170".to_string()],
            ]),
        ),
    ];

    for (step_num, name, sql, expected) in scenario_steps {
        if step_delay > Duration::ZERO && step_num > 1 {
            tokio::time::sleep(step_delay).await;
        }

        let step_start = Instant::now();
        match client.simple_query(sql).await {
            Ok(messages) => {
                let mut rows_affected: Option<u64> = None;
                let mut rows: Vec<Vec<String>> = Vec::new();

                for msg in messages {
                    match msg {
                        tokio_postgres::SimpleQueryMessage::CommandComplete(t) => {
                            rows_affected = Some(t);
                        }
                        tokio_postgres::SimpleQueryMessage::Row(r) => {
                            let row_vals = (0..r.len())
                                .map(|i| r.get(i).unwrap_or("").to_string())
                                .collect::<Vec<_>>();
                            rows.push(row_vals);
                        }
                        _ => {}
                    }
                }

                let step_duration = step_start.elapsed().as_millis() as u64;

                let matches_expectation = match &expected {
                    StepExpectation::AnyComplete => rows_affected.is_some(),
                    StepExpectation::RowsAffected(expected_count) => {
                        rows_affected == Some(*expected_count)
                    }
                    StepExpectation::Rows(expected_rows) => &rows == expected_rows,
                };

                let tag_str = rows_affected.map(|c| format!("rows={c}"));

                if matches_expectation {
                    steps.push(DemoStep {
                        step: step_num,
                        name: name.to_string(),
                        sql: sql.to_string(),
                        status: "ok".to_string(),
                        command_tag: tag_str,
                        rows: if rows.is_empty() { None } else { Some(rows) },
                        duration_ms: step_duration,
                    });
                } else {
                    let err_msg = format!(
                        "step {} ('{}') expectation mismatch: expected {:?}, got tag={:?}, rows={:?}",
                        step_num, name, expected, tag_str, rows
                    );
                    steps.push(DemoStep {
                        step: step_num,
                        name: name.to_string(),
                        sql: sql.to_string(),
                        status: "failed".to_string(),
                        command_tag: tag_str,
                        rows: if rows.is_empty() { None } else { Some(rows) },
                        duration_ms: step_duration,
                    });
                    failure = Some(err_msg);
                    break;
                }
            }
            Err(e) => {
                let step_duration = step_start.elapsed().as_millis() as u64;
                let err_msg = format!("step {} ('{}') execution error: {e}", step_num, name);
                steps.push(DemoStep {
                    step: step_num,
                    name: name.to_string(),
                    sql: sql.to_string(),
                    status: "failed".to_string(),
                    command_tag: None,
                    rows: None,
                    duration_ms: step_duration,
                });
                failure = Some(err_msg);
                break;
            }
        }
    }

    // Cleanup resources
    gateway_handle.abort();
    conn_task.abort();

    let retained = if opts.keep {
        if let Some(guard) = _temp_dir_guard {
            // Keep the temp dir on disk
            let _ = guard.keep();
        }
        true
    } else {
        false
    };

    let total_duration_ms = start_all.elapsed().as_millis() as u64;
    let outcome = DemoOutcome {
        scenario: opts.scenario.clone(),
        status: if failure.is_none() {
            "passed".to_string()
        } else {
            "failed".to_string()
        },
        steps,
        total_duration_ms,
        storage_path: storage_path.to_string_lossy().to_string(),
        retained,
    };

    let rendered = render_output(&outcome, format);

    if let Some(err) = failure {
        return Err(CliError::new(
            RS_0001,
            format!("demo scenario `{}` failed: {err}", opts.scenario),
            rendered,
        ));
    }

    Ok(rendered)
}

#[derive(Debug)]
enum StepExpectation {
    AnyComplete,
    RowsAffected(u64),
    Rows(Vec<Vec<String>>),
}

/// Synchronous entry point for `rockstream demo`.
pub fn run_demo(format: OutputFormat, opts: &DemoOptions) -> Result<String, CliError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        let opts = opts.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), "")
                })?;
            rt.block_on(run_demo_async(format, &opts))
        })
        .join()
        .map_err(|_| CliError::new(RS_0003, "demo runner thread panicked", ""))?
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                CliError::new(RS_0003, format!("failed to start tokio runtime: {e}"), "")
            })?;
        rt.block_on(run_demo_async(format, opts))
    }
}

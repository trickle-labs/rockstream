//! Scenario drivers (`TST-002`): the pluggable execution backends a
//! [`crate::scenario::dsl::Scenario`] can be run through.
//!
//! - [`InProcessDriver`] executes each step's SQL directly through DataFusion,
//!   in the calling process, without a wire protocol or an external process.
//! - [`PgwireProcessDriver`] spawns the compiled `rockstream` binary as a
//!   separate OS process and runs each step's SQL against it over real
//!   pgwire (generalizing the compiled-binary + real-client pattern in
//!   `crates/rockstream-cli/tests/r1_strategy_selection_process_tests.rs`
//!   and `crates/rockstream-sim/tests/shard_migration_tc_tests.rs`).
//! - [`DockerDriver`] runs the same steps against a real Postgres container
//!   (generalizing the container pattern in
//!   `crates/rockstream-connectors/tests/common/mod.rs`), auto-skipping with
//!   a logged message when Docker is unavailable.
//!
//! `rockstream-gateway` cannot be a normal dependency of this crate (it is
//! already a normal dependent *of* `rockstream-oracle` — see
//! `.claude/v0.59.17-plan.md` §"Scope and Boundary" — so the reverse edge
//! would be a dependency cycle). `PgwireProcessDriver` and `DockerDriver`
//! therefore reach a real pgwire/Postgres endpoint only as an external OS
//! process, never by linking gateway code directly.

use std::fmt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use tokio_postgres::NoTls;

use crate::scenario::dsl::{Scenario, ScenarioStep};
use crate::scenario::transcript::{ScenarioEvent, ScenarioTranscript};

/// Error returned by a [`ScenarioDriver`] run.
#[derive(Debug)]
pub enum DriverError {
    Execution(String),
    Unavailable(String),
    Capacity(crate::scenario::transcript::TranscriptCapacityExceeded),
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execution(msg) => write!(f, "scenario driver execution error: {msg}"),
            Self::Unavailable(msg) => write!(f, "scenario driver unavailable: {msg}"),
            Self::Capacity(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for DriverError {}

impl From<crate::scenario::transcript::TranscriptCapacityExceeded> for DriverError {
    fn from(err: crate::scenario::transcript::TranscriptCapacityExceeded) -> Self {
        Self::Capacity(err)
    }
}

/// A pluggable backend that runs a [`Scenario`] and reports what it observed.
#[async_trait::async_trait]
pub trait ScenarioDriver {
    async fn run(&self, scenario: &Scenario) -> Result<ScenarioTranscript, DriverError>;
}

fn rows_from_batches(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells = (0..batch.num_columns())
                .map(|col| {
                    let column = batch.column(col);
                    if column.is_null(row) {
                        r"\N".to_string()
                    } else {
                        array_value_to_string(column.as_ref(), row)
                            .unwrap_or_else(|e| format!("<display error: {e}>"))
                    }
                })
                .collect();
            rows.push(cells);
        }
    }
    rows
}

// ── InProcessDriver ───────────────────────────────────────────────────────────

/// Runs each step's SQL directly through a fresh DataFusion `SessionContext`.
#[derive(Debug, Default)]
pub struct InProcessDriver;

#[async_trait::async_trait]
impl ScenarioDriver for InProcessDriver {
    async fn run(&self, scenario: &Scenario) -> Result<ScenarioTranscript, DriverError> {
        let ctx = SessionContext::new();
        let mut transcript = ScenarioTranscript::new();
        for (step_index, step) in scenario.steps.iter().enumerate() {
            let ScenarioStep::ExecuteSql(sql) = step;
            let df = ctx
                .sql(sql)
                .await
                .map_err(|e| DriverError::Execution(format!("step {step_index}: {e}")))?;
            let batches = df
                .collect()
                .await
                .map_err(|e| DriverError::Execution(format!("step {step_index}: {e}")))?;
            transcript.push_event(ScenarioEvent {
                step_index,
                rows: rows_from_batches(&batches),
            })?;
        }
        Ok(transcript)
    }
}

// ── PgwireProcessDriver ───────────────────────────────────────────────────────

/// Spawns the compiled `rockstream` binary and runs each step's SQL against
/// it over real pgwire.
pub struct PgwireProcessDriver {
    binary: PathBuf,
}

impl PgwireProcessDriver {
    /// Locate the workspace-built `rockstream` binary relative to this
    /// crate's manifest directory, mirroring
    /// `crates/rockstream-sim/tests/shard_migration_tc_tests.rs`.
    pub fn new() -> Self {
        let profile = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        let binary = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("target")
            .join(profile)
            .join("rockstream");
        Self { binary }
    }
}

impl Default for PgwireProcessDriver {
    fn default() -> Self {
        Self::new()
    }
}

struct SpawnedGateway(Child);

impl Drop for SpawnedGateway {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_addr() -> String {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

#[async_trait::async_trait]
impl ScenarioDriver for PgwireProcessDriver {
    async fn run(&self, scenario: &Scenario) -> Result<ScenarioTranscript, DriverError> {
        if !self.binary.exists() {
            return Err(DriverError::Unavailable(format!(
                "rockstream binary not found at {} — run `rtk cargo build --bin rockstream` first",
                self.binary.display()
            )));
        }

        let storage =
            tempfile::tempdir().map_err(|e| DriverError::Execution(format!("tempdir: {e}")))?;
        let listen = free_addr();

        let child = Command::new(&self.binary)
            .args(["start", "--storage"])
            .arg(storage.path())
            .args(["--role", "gateway", "--listen", &listen])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| DriverError::Execution(format!("spawn rockstream binary: {e}")))?;
        let _gateway = SpawnedGateway(child);

        let addr: std::net::SocketAddr = listen
            .parse()
            .map_err(|e| DriverError::Execution(format!("parse listen addr: {e}")))?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let client = loop {
            if let Ok((client, connection)) = tokio_postgres::connect(
                &format!("host={} port={} user=rockstream", addr.ip(), addr.port()),
                NoTls,
            )
            .await
            {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                break client;
            }
            if Instant::now() >= deadline {
                return Err(DriverError::Execution(format!(
                    "rockstream binary did not accept connections at {listen} within 10s"
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        let mut transcript = ScenarioTranscript::new();
        for (step_index, step) in scenario.steps.iter().enumerate() {
            let ScenarioStep::ExecuteSql(sql) = step;
            let messages = client
                .simple_query(sql)
                .await
                .map_err(|e| DriverError::Execution(format!("step {step_index}: {e}")))?;
            let rows = messages
                .iter()
                .filter_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                        (0..row.len())
                            .map(|i| row.get(i).unwrap_or(r"\N").to_string())
                            .collect(),
                    ),
                    _ => None,
                })
                .collect();
            transcript.push_event(ScenarioEvent { step_index, rows })?;
        }
        Ok(transcript)
    }
}

// ── DockerDriver ──────────────────────────────────────────────────────────────

/// Runs each step's SQL against a real Postgres container.
///
/// [`ScenarioDriver::run`] returns [`DriverError::Unavailable`] when Docker is
/// not present locally, so callers can skip-and-log rather than fail; see
/// `docker_driver_matches_in_process_driver` in
/// `crates/rockstream-oracle/tests/scenario_driver_tests.rs`.
#[derive(Debug, Default)]
pub struct DockerDriver;

#[async_trait::async_trait]
impl ScenarioDriver for DockerDriver {
    async fn run(&self, scenario: &Scenario) -> Result<ScenarioTranscript, DriverError> {
        if !rockstream_test_support::docker_available() {
            return Err(DriverError::Unavailable(
                "Docker is not available locally".to_string(),
            ));
        }

        use testcontainers::core::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        let container = GenericImage::new("postgres", "16-alpine")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_DB", "postgres")
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
            .start()
            .await
            .map_err(|e| DriverError::Execution(format!("postgres container start: {e}")))?;
        let host = container
            .get_host()
            .await
            .map_err(|e| DriverError::Execution(format!("container host: {e}")))?;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .map_err(|e| DriverError::Execution(format!("container port: {e}")))?;
        let dsn = format!("host={host} port={port} user=postgres dbname=postgres");
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
            .await
            .map_err(|e| DriverError::Execution(format!("connect: {e}")))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let mut transcript = ScenarioTranscript::new();
        for (step_index, step) in scenario.steps.iter().enumerate() {
            let ScenarioStep::ExecuteSql(sql) = step;
            let messages = client
                .simple_query(sql)
                .await
                .map_err(|e| DriverError::Execution(format!("step {step_index}: {e}")))?;
            let rows = messages
                .iter()
                .filter_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                        (0..row.len())
                            .map(|i| row.get(i).unwrap_or(r"\N").to_string())
                            .collect(),
                    ),
                    _ => None,
                })
                .collect();
            transcript.push_event(ScenarioEvent { step_index, rows })?;
        }
        Ok(transcript)
    }
}

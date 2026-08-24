//! Command-line argument definitions and AST for the single `rockstream` binary.

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::output::OutputFormat;

/// Target shell for completion generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShellType {
    Bash,
    Zsh,
    Fish,
}

/// RockStream — a cloud-native incremental view maintenance engine with a
/// PostgreSQL wire access layer.
#[derive(Debug, Parser)]
#[command(name = "rockstream", version, about, long_about = None)]
pub struct Cli {
    /// Format output as text or JSON.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,

    /// Format output as JSON (backwards-compatible alias for --output json).
    #[arg(long, global = true)]
    pub json: bool,

    /// Control service URL.
    #[arg(long, global = true)]
    pub control: Option<String>,

    /// Storage directory for local state and artifacts.
    #[arg(long, global = true)]
    pub storage_dir: Option<PathBuf>,

    /// Principal presented to control-plane and catalog mutations.
    #[arg(long, global = true, default_value = "rockstream")]
    pub identity_user: String,

    /// RBAC role presented to control-plane and catalog mutations.
    #[arg(long, global = true, value_parser = ["viewer", "pipeline-owner", "admin"], default_value = "viewer")]
    pub identity_role: String,

    /// Path to the PEM-encoded CA certificate used to validate peer certificates for mTLS.
    #[arg(long = "tls-ca-cert-path", global = true)]
    pub tls_ca_cert_path: Option<PathBuf>,

    /// Path to the PEM-encoded client/server certificate presented during TLS handshake.
    #[arg(long = "tls-cert-path", global = true)]
    pub tls_cert_path: Option<PathBuf>,

    /// Path to the PEM-encoded private key matching `--tls-cert-path`.
    #[arg(long = "tls-key-path", global = true)]
    pub tls_key_path: Option<PathBuf>,

    /// Path to the PEM-encoded certificate chain for internal cluster mTLS.
    #[arg(long = "internal-tls-cert-path", global = true)]
    pub internal_tls_cert_path: Option<PathBuf>,

    /// Path to the PEM-encoded private key for internal cluster mTLS.
    #[arg(long = "internal-tls-key-path", global = true)]
    pub internal_tls_key_path: Option<PathBuf>,

    /// Path to the PEM-encoded CA certificate for internal cluster mTLS.
    #[arg(long = "internal-tls-ca-cert-path", global = true)]
    pub internal_tls_ca_cert_path: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    /// Return the effective output format taking `--json` and `--output` into account.
    pub fn effective_output_format(&self) -> OutputFormat {
        if self.json {
            OutputFormat::Json
        } else {
            self.output
        }
    }
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Migrate shard storage formats offline.
    Migrate {
        /// Existing storage format version.
        #[arg(long)]
        from: u8,
        /// Target storage format version.
        #[arg(long)]
        to: u8,
        /// Local path or s3://bucket/prefix containing shard databases.
        #[arg(long)]
        storage: String,
    },
    /// Start a RockStream node.
    Start {
        /// Local storage directory for node state and artifacts.
        #[arg(long)]
        storage: PathBuf,

        /// Node role.
        #[arg(long, default_value = "all")]
        role: String,

        /// Control service URL (required for the worker and frontier roles).
        #[arg(long)]
        control: Option<String>,

        /// Authentication mode.
        #[arg(long, default_value = "off", value_parser = clap::builder::PossibleValuesParser::new(["off", "oidc", "mtls"]))]
        auth: String,

        /// Stable same-host identity advertised during worker registration.
        #[arg(long)]
        host_id: Option<String>,

        /// Explicit worker ID advertised during worker registration.
        #[arg(long)]
        worker_id: Option<u64>,

        /// Availability zone advertised during worker registration.
        #[arg(long)]
        availability_zone: Option<String>,

        /// Metrics HTTP server listen address.
        #[arg(long)]
        metrics_addr: Option<String>,

        /// PostgreSQL wire gateway listen address.
        #[arg(long, default_value = "127.0.0.1:5432")]
        listen: String,

        /// Independent HTTP listener for `POST /webhook/<source>` ingestion.
        #[arg(long)]
        webhook_listen: Option<String>,

        /// Comma-separated list of other control nodes in Raft group.
        #[arg(long)]
        raft_peers: Option<String>,

        /// This node's ID within its Raft group.
        #[arg(long)]
        raft_node_id: Option<u64>,

        /// Address this node's Raft peer-RPC listener binds to.
        #[arg(long)]
        raft_bind: Option<String>,

        /// Start an election immediately on boot.
        #[arg(long, default_value_t = false)]
        raft_bootstrap: bool,

        /// Run the control role as a daemon.
        #[arg(long, default_value_t = false)]
        daemon: bool,

        /// Override address worker-facing ControlService binds to.
        #[arg(long)]
        control_bind: Option<String>,

        /// Directory for state shared across control nodes in Raft group.
        #[arg(long)]
        control_shared_storage: Option<PathBuf>,

        /// Root directory of a non-local shard included in query-time scatter read.
        #[arg(long = "query-time-shard-dir")]
        query_time_shard_dirs: Vec<PathBuf>,

        #[arg(long)]
        min_epoch_ms: Option<u64>,
        #[arg(long)]
        checkpoint_retention_count: Option<u32>,
        #[arg(long)]
        state_budget_gb: Option<u64>,
        #[arg(long)]
        exchange_direct_threshold_bytes: Option<usize>,
        #[arg(long)]
        exchange_spill_threshold_mb: Option<u64>,
        #[arg(long)]
        exchange_domain_size: Option<usize>,
        #[arg(long, default_value_t = false)]
        exchange_force_durable: bool,
        #[arg(long)]
        same_host_shm_segment_bytes: Option<usize>,
        #[arg(long)]
        same_host_shm_segments_per_peer: Option<usize>,
        #[arg(long)]
        max_exchange_compression_states: Option<usize>,
    },
    /// View inspection commands.
    View {
        #[command(subcommand)]
        command: ViewCommand,
    },
    /// Source inspection commands.
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    /// Schema inspection commands.
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Workload inspection commands.
    Workload {
        #[command(subcommand)]
        command: WorkloadCommand,
    },
    /// Cluster administration and inspection commands.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    /// Shard inspection commands.
    Shard {
        #[command(subcommand)]
        command: ShardCommand,
    },
    /// Checkpoint inspection commands.
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
    /// Resource usage inspection commands.
    Resource {
        #[command(subcommand)]
        command: ResourceCommand,
    },
    /// Schema evolution inspection commands.
    #[command(name = "schema-evolution")]
    SchemaEvolution {
        #[command(subcommand)]
        command: SchemaEvolutionCommand,
    },
    /// Audit log inspection commands.
    Audit {
        #[command(subcommand)]
        command: AuditCommand,
    },
    /// Diagnostic support commands.
    Support {
        #[command(subcommand)]
        command: SupportCommand,
    },
    /// Explain the incremental execution plan for a view.
    Explain {
        /// View name to explain.
        view: String,
        /// Show static cost and state memory estimates without deploying.
        #[arg(long)]
        estimate: bool,
        /// Show operator IDs and addressability details for intermediate state.
        #[arg(long)]
        op_ids: bool,
    },
    /// Parse, lower, and explain a SQL query without deploying.
    Sql {
        /// SQL query to parse and lower.
        query: String,
    },
    /// Low-level debugging and arrangement state inspection.
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
    /// Print candidate identity and version information.
    Version {
        /// Format version information as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Evidence manifest verification and inspection.
    Manifest {
        #[command(subcommand)]
        command: ManifestCommand,
    },
    /// Run release qualification suite or check prerequisites.
    Qualify {
        /// Check execution environment prerequisites fail-closed.
        #[arg(long)]
        check_prerequisites: bool,
        /// Qualification test suite to execute.
        #[arg(long)]
        suite: Option<String>,
        /// Output file path for raw metrics and summary.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Configuration validation and effective printing.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Generate shell completion scripts for Bash, Zsh, or Fish.
    Completions {
        /// Target shell to generate completions for.
        #[arg(value_enum)]
        shell: ShellType,
    },
    /// Initialize a new RockStream project from a template.
    Init {
        /// Project name (defaults to "my_project").
        #[arg(default_value = "my_project")]
        name: String,

        /// Project template: "local", "kafka", or "postgres-cdc".
        #[arg(long, default_value = "local")]
        template: String,

        /// Target directory to scaffold the project into (defaults to ./<name>).
        #[arg(long)]
        dir: Option<PathBuf>,

        /// Overwrite existing files in non-empty directory.
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    /// Run an embedded demonstration scenario proving incremental view maintenance.
    Demo {
        /// Demo scenario to execute (default: orders).
        #[arg(long, default_value = "orders")]
        scenario: String,
        /// Storage directory for local state and artifacts.
        #[arg(long)]
        storage: Option<PathBuf>,
        /// PostgreSQL wire gateway listen address.
        #[arg(long, default_value = "127.0.0.1:0")]
        listen: String,
        /// Retain demo storage directory after execution.
        #[arg(long)]
        keep: bool,
        /// Optional presentation delay in milliseconds between scenario steps (max 5000).
        #[arg(long, default_value_t = 0)]
        step_delay_ms: u64,
    },
    /// Run non-destructive diagnostic checks on binary, config, system, storage, and network reachability.
    Doctor {
        /// Path to configuration file to validate.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Storage path or s3:// URL to validate.
        #[arg(long)]
        storage: Option<String>,
        /// Control service URL to probe.
        #[arg(long)]
        control: Option<String>,
        /// PostgreSQL wire gateway address (host:port) to probe.
        #[arg(long)]
        gateway: Option<String>,
        /// Perform active storage write/read/delete probe.
        #[arg(long)]
        deep: bool,
        /// Include Docker daemon socket accessibility check.
        #[arg(long)]
        include_docker: bool,
        /// Check execution timeout in seconds (default 5, max 30).
        #[arg(long, default_value_t = 5)]
        timeout: u64,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Validate RockStream configuration files for syntax, unknown keys, and semantic bounds.
    Validate {
        /// Path to configuration file to validate (defaults to standard search paths).
        #[arg(long)]
        file: Option<PathBuf>,
        /// Enforce strict validation (fail on unknown or deprecated keys).
        #[arg(long, default_value_t = true)]
        strict: bool,
        /// Validate accessibility of referenced TLS certificate and key files.
        #[arg(long)]
        check_files: bool,
    },
    /// Print the effective configuration resolved from defaults, config file, environment, and CLI flags.
    #[command(name = "print-effective")]
    PrintEffective {
        /// Path to configuration file (defaults to standard search paths).
        #[arg(long)]
        file: Option<PathBuf>,
        /// Include source origin annotations in the printed configuration.
        #[arg(long)]
        show_origins: bool,
        #[arg(long)]
        min_epoch_ms: Option<u64>,
        #[arg(long)]
        checkpoint_retention_count: Option<u32>,
        #[arg(long)]
        state_budget_gb: Option<u64>,
        #[arg(long)]
        exchange_direct_threshold_bytes: Option<usize>,
        #[arg(long)]
        exchange_spill_threshold_mb: Option<u64>,
        #[arg(long)]
        exchange_domain_size: Option<usize>,
        #[arg(long, default_value_t = false)]
        exchange_force_durable: bool,
        #[arg(long)]
        same_host_shm_segment_bytes: Option<usize>,
        #[arg(long)]
        same_host_shm_segments_per_peer: Option<usize>,
        #[arg(long)]
        max_exchange_compression_states: Option<usize>,
        #[arg(long)]
        webhook_listen: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum ManifestCommand {
    /// Validate an evidence-manifest.json file.
    Validate {
        /// Path to the evidence-manifest.json file.
        path: PathBuf,
        /// Optional base directory containing referenced artifact files.
        #[arg(long)]
        base_dir: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
pub enum DebugCommand {
    /// Inspect intermediate arrangement Z-set state for an operator.
    Arrangement {
        /// View name to inspect.
        view: String,
        /// Operator ID to inspect.
        op_id: String,
        /// Key expression to inspect.
        key: String,
        /// Historical epoch to inspect.
        #[arg(long)]
        epoch: Option<u64>,
    },
}

#[derive(Debug, Subcommand)]
pub enum ViewCommand {
    /// List all views.
    List,
    /// Show detailed view metadata.
    Show {
        /// View name.
        name: String,
    },
    /// Show view lifecycle and freshness status.
    Status {
        /// Optional view name filter.
        name: Option<String>,
    },
    /// Pause an active view.
    Pause {
        /// View name.
        name: String,
        /// Confirm destructive action without interactive prompt.
        #[arg(long)]
        yes: bool,
    },
    /// Resume a paused view.
    Resume {
        /// View name.
        name: String,
    },
    /// Query view results.
    Query {
        /// View name.
        name: String,
        /// Maximum rows to return.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Stream live view updates.
    Subscribe {
        /// View name.
        name: String,
        /// Start streaming from a specific epoch.
        #[arg(long)]
        from_epoch: Option<u64>,
        /// Begin subscription with a baseline snapshot.
        #[arg(long)]
        snapshot: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum SourceCommand {
    /// List all sources.
    List,
    /// Show source connector detail.
    Show {
        /// Source name.
        name: String,
    },
    /// Pause source ingestion.
    Pause {
        /// Source name.
        name: String,
    },
    /// Resume paused source ingestion.
    Resume {
        /// Source name.
        name: String,
    },
    /// Drop a source connector.
    Drop {
        /// Source name.
        name: String,
        /// Confirm destructive action without interactive prompt.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum SchemaCommand {
    /// List all tables and views in the schema.
    List,
    /// Show schema columns for a table or view.
    Show {
        /// Table or view name.
        name: String,
    },
    /// Create a new schema table.
    Create {
        /// Table name.
        name: String,
        /// Column specification.
        #[arg(long)]
        columns: Option<String>,
    },
    /// Drop a schema table or view.
    Drop {
        /// Table name.
        name: String,
        /// Confirm destructive action without interactive prompt.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum WorkloadCommand {
    /// List all workloads.
    List,
    /// Show workload definition detail.
    Show {
        /// Workload name.
        name: String,
    },
    /// Create a new workload.
    Create {
        /// Workload name.
        name: String,
        /// Scheduling priority.
        #[arg(long)]
        priority: Option<u32>,
        /// Freshness SLO in milliseconds.
        #[arg(long)]
        freshness_slo_ms: Option<u64>,
        /// Memory limit in bytes.
        #[arg(long)]
        memory_limit: Option<u64>,
        /// Maximum worker parallelism.
        #[arg(long)]
        max_parallelism: Option<usize>,
    },
    /// Alter an existing workload.
    Alter {
        /// Workload name.
        name: String,
        /// Scheduling priority.
        #[arg(long)]
        priority: Option<u32>,
        /// Freshness SLO in milliseconds.
        #[arg(long)]
        freshness_slo_ms: Option<u64>,
        /// Memory limit in bytes.
        #[arg(long)]
        memory_limit: Option<u64>,
        /// Maximum worker parallelism.
        #[arg(long)]
        max_parallelism: Option<usize>,
    },
    /// Drop a workload.
    Drop {
        /// Workload name.
        name: String,
        /// Confirm destructive action without interactive prompt.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ShardCommand {
    /// List all shards and their lease assignments.
    List,
    /// Migrate a shard to another worker.
    Migrate {
        /// Shard ID.
        shard_id: u64,
        /// Target worker ID.
        #[arg(long)]
        to: u64,
        /// Confirm destructive action without interactive prompt.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum CheckpointCommand {
    /// List cluster checkpoints.
    List,
    /// Show per-shard checkpoint alignment state.
    Show {
        /// Checkpoint ID.
        checkpoint_id: u64,
    },
    /// Export latest committed checkpoint.
    Export {
        /// Destination object-store URL.
        #[arg(long)]
        destination: String,
    },
    /// Restore committed export.
    Restore {
        /// Export object-store URL.
        #[arg(long)]
        source: String,
        /// Fresh target object-store URL.
        #[arg(long)]
        storage: String,
        /// Confirm destructive action without interactive prompt.
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum SupportCommand {
    /// Generate on-demand diagnostic support bundle.
    Bundle {
        /// Optional view name filter.
        #[arg(long)]
        view: Option<String>,
        /// Optional duration filter.
        #[arg(long)]
        since: Option<String>,
        /// Output file path for the support bundle.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
pub enum ResourceCommand {
    /// Show per-view and per-workload resource usage.
    Usage {
        /// Optional workload name filter.
        #[arg(long)]
        workload: Option<String>,
    },
    /// Show aggregate cluster resource usage.
    Cluster,
}

#[derive(Debug, Subcommand)]
pub enum SchemaEvolutionCommand {
    /// Show schema evolution status.
    Status,
    /// Show schema evolution version history.
    History,
}

#[derive(Debug, Subcommand)]
pub enum AuditCommand {
    /// Tail recent audit log events.
    Tail {
        /// Maximum events to return (max 1000).
        #[arg(long, default_value_t = 100)]
        max: usize,
    },
    /// Query audit log events matching a filter.
    Query {
        /// Substring filter for actor, action, or resource.
        #[arg(long)]
        filter: Option<String>,
        /// Maximum events to return (max 1000).
        #[arg(long, default_value_t = 100)]
        max: usize,
    },
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    /// Show cluster status and leadership.
    Status,
    /// Show cluster quotas and capacity limits.
    Quotas,
    /// Worker administration commands.
    Workers {
        #[command(subcommand)]
        command: WorkerCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum WorkerCommand {
    /// List all registered workers.
    List,
    /// Show detailed worker status.
    Status {
        /// Optional worker ID.
        worker_id: Option<u64>,
    },
    /// Begin draining a worker.
    Drain {
        /// Control-plane worker-facing TCP address.
        #[arg(long)]
        control: Option<String>,
        /// Worker ID to drain.
        worker_id: u64,
        /// Confirm destructive action without interactive prompt.
        #[arg(long)]
        yes: bool,
    },
}

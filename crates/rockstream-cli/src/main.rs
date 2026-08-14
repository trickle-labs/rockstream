//! The single `rockstream` binary.
//!
//! Every node role is a flag on this one binary. At v0.1 it runs an embedded
//! no-op node; see [`rockstream_cli`] for the command implementations.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rockstream_cli::output::OutputFormat;
use rockstream_cli::transport::{CatalogClient, ClientIdentity, ControlClient, StorageClient};
use rockstream_cli::{
    request_worker_drain, run_audit_query, run_audit_tail, run_checkpoint_list, run_cluster_quotas,
    run_cluster_status, run_cluster_workers_list, run_cluster_workers_status, run_explain_view,
    run_resource_cluster, run_resource_usage, run_schema_evolution_history,
    run_schema_evolution_status, run_schema_list, run_schema_show, run_shard_list, run_source_list,
    run_source_show, run_sql_compile, run_start, run_view_list, run_view_show, run_view_status,
    run_workload_list, run_workload_show, StartOptions,
};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

/// RockStream — a cloud-native incremental view maintenance engine with a
/// PostgreSQL wire access layer.
#[derive(Debug, Parser)]
#[command(name = "rockstream", version, about, long_about = None)]
struct Cli {
    /// Format output as JSON.
    #[arg(long, global = true)]
    json: bool,

    /// Control service URL.
    #[arg(long, global = true)]
    control: Option<String>,

    /// Storage directory for local state and artifacts.
    #[arg(long, global = true)]
    storage_dir: Option<std::path::PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
// `Start` carries many long-lived config fields as clap-derived struct-variant
// fields; boxing it would complicate the clap derive for no runtime benefit
// (this enum is constructed once per process, not on a hot path).
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Start a RockStream node.
    ///
    /// For the `gateway` or `all` role the node starts a long-running PostgreSQL
    /// wire server on `--listen` and blocks until SIGTERM / Ctrl-C. Other roles
    /// run the embedded no-op node (audit log + support bundle), then exit.
    Start {
        /// Local storage directory for node state and artifacts.
        #[arg(long)]
        storage: std::path::PathBuf,

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

        /// Availability zone advertised during worker registration.
        #[arg(long)]
        availability_zone: Option<String>,

        /// Metrics HTTP server listen address.
        #[arg(long)]
        metrics_addr: Option<String>,

        /// PostgreSQL wire gateway listen address.
        /// Activates the live gateway server for the `gateway` and `all` roles.
        #[arg(long, default_value = "127.0.0.1:5432")]
        listen: String,

        /// Independent HTTP listener for `POST /webhook/<source>` ingestion.
        #[arg(long)]
        webhook_listen: Option<String>,

        /// v0.45.2 M7: comma-separated list of the *other* control nodes in
        /// this node's Raft group, `id@host:port,id@host:port`. Only
        /// meaningful for `--role=control`. When omitted, the control role
        /// runs exactly as before v0.45.2 (single embedded node, no Raft
        /// leader-only write gating).
        #[arg(long)]
        raft_peers: Option<String>,

        /// This node's id within its Raft group (required with
        /// `--raft-peers`).
        #[arg(long)]
        raft_node_id: Option<u64>,

        /// Address this node's Raft peer-RPC listener binds to (required
        /// with `--raft-peers`).
        #[arg(long)]
        raft_bind: Option<String>,

        /// Start an election immediately on boot rather than waiting out a
        /// randomized timeout. Exactly one node in a freshly-bootstrapped
        /// Raft group should set this.
        #[arg(long, default_value_t = false)]
        raft_bootstrap: bool,

        /// v0.45.2 M7 S4: run the `control` role as a real long-lived
        /// daemon that blocks on SIGTERM / Ctrl-C, exactly like the
        /// `gateway`/`all` roles' live PostgreSQL wire server, instead of
        /// the short embedded no-op run. Defaults to `false`, preserving
        /// exact pre-v0.45.2 behavior (and every existing test) for every
        /// caller that does not pass this flag. Only meaningful for
        /// `--role=control`; required for a real multi-node control-plane
        /// cluster (each node's process must keep running so peers can
        /// reach it and so it can keep serving worker/status requests).
        #[arg(long, default_value_t = false)]
        daemon: bool,

        /// v0.45.2 M7 S4: override the address the control-plane's
        /// worker-facing `ControlService` TCP listener binds to. Defaults
        /// to `127.0.0.1:8000` (the pre-v0.45.2 convention) when omitted.
        /// Only meaningful for `--role=control`/`--role=all`. Needed to
        /// bind `0.0.0.0:<port>` inside a container so peer control nodes
        /// and workers on other hosts can reach it.
        #[arg(long)]
        control_bind: Option<String>,

        /// v0.45.2 M7 S4/S5: directory for state that must be *shared*
        /// across every control node in this node's Raft group — the Raft
        /// term/vote/log and the shard-lease-manager snapshot (DESIGN.md
        /// §3's "control SlateDB": the elected leader is its sole writer,
        /// and a newly-elected leader on a different real process loads
        /// the last-persisted state from here before serving its first
        /// write). Only meaningful for `--role=control` with
        /// `--raft-peers`. When omitted (the default), each control node's
        /// Raft state lives under its own private `--storage` directory
        /// exactly as before v0.45.2, and the `ShardManager` stays
        /// purely in-memory (no cross-process lease continuity) —
        /// preserving exact pre-v0.45.2 single-node behavior.
        #[arg(long)]
        control_shared_storage: Option<std::path::PathBuf>,

        /// Root directory of a non-local shard included in every query-time
        /// scatter read. Repeat once for each additional owning shard.
        #[arg(long = "query-time-shard-dir")]
        query_time_shard_dirs: Vec<std::path::PathBuf>,

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

        /// v0.51.5: path to the PEM-encoded server certificate (chain) for
        /// gateway-facing TLS termination. Requires `--tls-key-path`.
        #[arg(long)]
        tls_cert_path: Option<std::path::PathBuf>,
        /// v0.51.5: path to the PEM-encoded private key matching
        /// `--tls-cert-path`.
        #[arg(long)]
        tls_key_path: Option<std::path::PathBuf>,
        /// v0.51.5: path to the PEM-encoded CA certificate used to validate
        /// client certificates for `--auth=mtls`. Required whenever
        /// `--auth=mtls` is set.
        #[arg(long)]
        tls_ca_cert_path: Option<std::path::PathBuf>,
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
    /// Explain the incremental execution plan for a view.
    Explain {
        /// View name to explain.
        view: String,
        /// Show static cost and state memory estimates without deploying.
        #[arg(long)]
        estimate: bool,
    },
    /// Parse, lower, and explain a SQL query without deploying.
    Sql {
        /// SQL query to parse and lower.
        query: String,
    },
}

#[derive(Debug, Subcommand)]
enum ViewCommand {
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
}

#[derive(Debug, Subcommand)]
enum SourceCommand {
    /// List all sources.
    List,
    /// Show source connector detail.
    Show {
        /// Source name.
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum SchemaCommand {
    /// List all tables and views in the schema.
    List,
    /// Show schema columns for a table or view.
    Show {
        /// Table or view name.
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum WorkloadCommand {
    /// List all workloads.
    List,
    /// Show workload definition detail.
    Show {
        /// Workload name.
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum ShardCommand {
    /// List all shards and their lease assignments.
    List,
}

#[derive(Debug, Subcommand)]
enum CheckpointCommand {
    /// List cluster checkpoints.
    List,
}

#[derive(Debug, Subcommand)]
enum ResourceCommand {
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
enum SchemaEvolutionCommand {
    /// Show schema evolution status.
    Status,
    /// Show schema evolution version history.
    History,
}

#[derive(Debug, Subcommand)]
enum AuditCommand {
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
enum ClusterCommand {
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
enum WorkerCommand {
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
        control: String,
        /// Worker id to drain.
        worker_id: u64,
    },
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let format = OutputFormat::from_json_flag(cli.json);

    match cli.command {
        Command::Start {
            storage,
            role,
            control,
            auth,
            host_id,
            availability_zone,
            metrics_addr,
            listen,
            webhook_listen,
            raft_peers,
            raft_node_id,
            raft_bind,
            raft_bootstrap,
            daemon,
            control_bind,
            control_shared_storage,
            query_time_shard_dirs,
            exchange_direct_threshold_bytes,
            exchange_spill_threshold_mb,
            exchange_domain_size,
            exchange_force_durable,
            same_host_shm_segment_bytes,
            same_host_shm_segments_per_peer,
            max_exchange_compression_states,
            tls_cert_path,
            tls_key_path,
            tls_ca_cert_path,
        } => {
            let mut config = RockstreamConfig::default();
            if let Some(value) = exchange_direct_threshold_bytes {
                config.exchange.exchange_direct_threshold_bytes = value;
            }
            if let Some(value) = exchange_spill_threshold_mb {
                config.exchange.exchange_spill_threshold_mb = value;
            }
            if let Some(value) = exchange_domain_size {
                config.exchange.exchange_domain_size = value;
            }
            if exchange_force_durable {
                config.exchange.exchange_force_durable = true;
            }
            if let Some(value) = same_host_shm_segment_bytes {
                config.exchange.same_host_shm_segment_bytes = value;
            }
            if let Some(value) = same_host_shm_segments_per_peer {
                config.exchange.same_host_shm_segments_per_peer = value;
            }
            if let Some(value) = max_exchange_compression_states {
                config.exchange.max_exchange_compression_states = value;
            }
            if let Some(value) = tls_cert_path {
                config.gateway.tls_cert_path = Some(value);
            }
            if let Some(value) = tls_key_path {
                config.gateway.tls_key_path = Some(value);
            }
            if let Some(value) = tls_ca_cert_path {
                config.gateway.tls_ca_cert_path = Some(value);
            }
            config.gateway.webhook_listen_addr = webhook_listen;
            let opts = StartOptions {
                storage,
                role,
                control,
                auth_mode: auth,
                worker_location: WorkerLocation::new(
                    host_id
                        .or_else(|| std::env::var("HOSTNAME").ok())
                        .unwrap_or_default(),
                    availability_zone
                        .or_else(|| std::env::var("ROCKSTREAM_AVAILABILITY_ZONE").ok())
                        .unwrap_or_default(),
                ),
                worker_capabilities: WorkerCapabilities {
                    same_host_arrow_shm_v1: true,
                    shuffle_codec_v1: true,
                    checkpoint_manifest_codec_v1: true,
                },
                config,
                metrics_addr,
                listen_addr: Some(listen),
                raft_peers,
                raft_node_id,
                raft_bind,
                raft_bootstrap,
                daemon,
                control_bind,
                control_shared_storage,
                query_time_shard_dirs,
            };
            match run_start(&opts) {
                Ok(outcome) => {
                    tracing::info!(
                        audit = %outcome.audit_path.display(),
                        bundle = %outcome.bundle_path.display(),
                        events = outcome.events_written,
                        "rockstream: node stopped cleanly"
                    );
                    ExitCode::SUCCESS
                }
                Err(err) => {
                    // Operator-visible failure: print with its RS-XXXX code and
                    // actionable next steps.
                    eprintln!("{err}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::View { command } => {
            let identity = ClientIdentity::default();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                ViewCommand::List => run_view_list(format, &catalog),
                ViewCommand::Show { name } => run_view_show(format, &catalog, &name),
                ViewCommand::Status { name } => run_view_status(format, &catalog, name.as_deref()),
            };
            handle_result(res)
        }
        Command::Source { command } => {
            let identity = ClientIdentity::default();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                SourceCommand::List => run_source_list(format, &catalog),
                SourceCommand::Show { name } => run_source_show(format, &catalog, &name),
            };
            handle_result(res)
        }
        Command::Schema { command } => {
            let identity = ClientIdentity::default();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                SchemaCommand::List => run_schema_list(format, &catalog),
                SchemaCommand::Show { name } => run_schema_show(format, &catalog, &name),
            };
            handle_result(res)
        }
        Command::Workload { command } => {
            let identity = ClientIdentity::default();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                WorkloadCommand::List => run_workload_list(format, &catalog),
                WorkloadCommand::Show { name } => run_workload_show(format, &catalog, &name),
            };
            handle_result(res)
        }
        Command::Cluster { command } => {
            let identity = ClientIdentity::default();
            let control = ControlClient::new(cli.control, identity);
            match command {
                ClusterCommand::Status => handle_result(run_cluster_status(format, &control)),
                ClusterCommand::Quotas => handle_result(run_cluster_quotas(format, &control)),
                ClusterCommand::Workers {
                    command: WorkerCommand::List,
                } => handle_result(run_cluster_workers_list(format, &control)),
                ClusterCommand::Workers {
                    command: WorkerCommand::Status { worker_id },
                } => handle_result(run_cluster_workers_status(format, &control, worker_id)),
                ClusterCommand::Workers {
                    command: WorkerCommand::Drain { control, worker_id },
                } => match request_worker_drain(&control, worker_id) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(err) => {
                        eprintln!("{err}");
                        ExitCode::FAILURE
                    }
                },
            }
        }
        Command::Shard { command } => {
            let identity = ClientIdentity::default();
            let control = ControlClient::new(cli.control, identity);
            let res = match command {
                ShardCommand::List => run_shard_list(format, &control),
            };
            handle_result(res)
        }
        Command::Checkpoint { command } => {
            let storage = StorageClient::new();
            let storage_path = cli
                .storage_dir
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            let res = match command {
                CheckpointCommand::List => run_checkpoint_list(format, &storage, &storage_path),
            };
            handle_result(res)
        }
        Command::Resource { command } => {
            let identity = ClientIdentity::default();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                ResourceCommand::Usage { workload } => {
                    run_resource_usage(format, &catalog, workload.as_deref())
                }
                ResourceCommand::Cluster => run_resource_cluster(format, &catalog),
            };
            handle_result(res)
        }
        Command::SchemaEvolution { command } => {
            let identity = ClientIdentity::default();
            let catalog = CatalogClient::new(identity);
            let res = match command {
                SchemaEvolutionCommand::Status => run_schema_evolution_status(format, &catalog),
                SchemaEvolutionCommand::History => run_schema_evolution_history(format, &catalog),
            };
            handle_result(res)
        }
        Command::Audit { command } => {
            let storage = StorageClient::new();
            let storage_path = cli
                .storage_dir
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            let res = match command {
                AuditCommand::Tail { max } => run_audit_tail(format, &storage, &storage_path, max),
                AuditCommand::Query { filter, max } => {
                    run_audit_query(format, &storage, &storage_path, filter.as_deref(), max)
                }
            };
            handle_result(res)
        }
        Command::Explain { view, estimate } => {
            let _identity = ClientIdentity::default();
            let catalog = CatalogClient::with_defaults();
            handle_result(run_explain_view(format, &catalog, &view, estimate))
        }
        Command::Sql { query } => handle_result(run_sql_compile(format, &query)),
    }
}

fn handle_result(res: Result<String, rockstream_cli::CliError>) -> ExitCode {
    match res {
        Ok(out) => {
            println!("{out}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

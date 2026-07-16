//! The single `rockstream` binary.
//!
//! Every node role is a flag on this one binary. At v0.1 it runs an embedded
//! no-op node; see [`rockstream_cli`] for the command implementations.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rockstream_cli::{request_worker_drain, run_start, StartOptions};

/// RockStream — a cloud-native incremental view maintenance engine with a
/// PostgreSQL wire access layer.
#[derive(Debug, Parser)]
#[command(name = "rockstream", version, about, long_about = None)]
struct Cli {
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

        /// Metrics HTTP server listen address.
        #[arg(long)]
        metrics_addr: Option<String>,

        /// PostgreSQL wire gateway listen address.
        /// Activates the live gateway server for the `gateway` and `all` roles.
        #[arg(long, default_value = "127.0.0.1:5432")]
        listen: String,

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
    },
    /// Cluster administration commands.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ClusterCommand {
    /// Worker administration commands.
    Workers {
        #[command(subcommand)]
        command: WorkerCommand,
    },
}

#[derive(Debug, Subcommand)]
enum WorkerCommand {
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

    match cli.command {
        Command::Start {
            storage,
            role,
            control,
            auth,
            metrics_addr,
            listen,
            raft_peers,
            raft_node_id,
            raft_bind,
            raft_bootstrap,
            daemon,
            control_bind,
            control_shared_storage,
        } => {
            let opts = StartOptions {
                storage,
                role,
                control,
                auth_mode: auth,
                metrics_addr,
                listen_addr: Some(listen),
                raft_peers,
                raft_node_id,
                raft_bind,
                raft_bootstrap,
                daemon,
                control_bind,
                control_shared_storage,
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
        Command::Cluster {
            command:
                ClusterCommand::Workers {
                    command: WorkerCommand::Drain { control, worker_id },
                },
        } => match request_worker_drain(&control, worker_id) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{err}");
                ExitCode::FAILURE
            }
        },
    }
}

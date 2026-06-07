//! The single `rockstream` binary.
//!
//! Every node role is a flag on this one binary. At v0.1 it runs an embedded
//! no-op node; see [`rockstream_cli`] for the command implementations.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rockstream_cli::{run_start, StartOptions};

/// RockStream — a cloud-native incremental view maintenance engine with a
/// PostgreSQL wire access layer.
#[derive(Debug, Parser)]
#[command(name = "rockstream", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a RockStream node. At v0.1 this runs an embedded no-op node:
    /// it brings the node up, runs a no-op pipeline to completion, writes an
    /// audit log and a support bundle under the storage directory, and exits.
    Start {
        /// Local storage directory for node state and artifacts.
        #[arg(long)]
        storage: std::path::PathBuf,

        /// Node role. v0.1 runs the embedded `all` profile regardless of role.
        #[arg(long, default_value = "all")]
        role: String,
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
        Command::Start { storage, role } => {
            let opts = StartOptions { storage, role };
            match run_start(&opts) {
                Ok(outcome) => {
                    tracing::info!(
                        audit = %outcome.audit_path.display(),
                        bundle = %outcome.bundle_path.display(),
                        events = outcome.events_written,
                        "rockstream: embedded no-op node ran to completion"
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
    }
}

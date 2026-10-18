//! Structural v0.72 overload and large-state preflight: not release sign-off.
use anyhow::{Context, Result};
use clap::Parser;
use r1_local_harness::overload::{verify_fault_preflight, FaultEvidenceBundle};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Check frozen #140 fault evidence structure; not capacity qualification")]
struct Cli {
    #[arg(long)]
    profile: PathBuf,
    #[arg(long)]
    bundle: PathBuf,
}

fn run(cli: Cli) -> Result<()> {
    let profile = std::fs::read(&cli.profile).context("read frozen v0.72 profile")?;
    let bytes = std::fs::read(&cli.bundle).context("read overload evidence")?;
    let bundle: FaultEvidenceBundle =
        serde_json::from_slice(&bytes).context("parse overload evidence")?;
    let preflight = verify_fault_preflight(&profile, &bundle)?;
    println!("{}", serde_json::to_string_pretty(&preflight)?);
    Ok(())
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

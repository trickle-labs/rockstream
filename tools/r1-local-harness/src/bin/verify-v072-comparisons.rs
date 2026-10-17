//! Structural v0.72 scale-comparison preflight, never capacity sign-off.
use anyhow::{Context, Result};
use clap::Parser;
use r1_local_harness::comparisons::{verify_comparison_preflight, ComparisonMeasurement};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    about = "Check frozen #139 comparison structure and arithmetic; not capacity qualification"
)]
struct Cli {
    #[arg(long)]
    profile: PathBuf,
    #[arg(long)]
    measurements: PathBuf,
}

fn run(cli: Cli) -> Result<()> {
    let profile = std::fs::read(&cli.profile).context("read frozen v0.72 profile")?;
    let bytes = std::fs::read(&cli.measurements).context("read comparison measurements")?;
    let measurements: Vec<ComparisonMeasurement> =
        serde_json::from_slice(&bytes).context("parse comparison measurements")?;
    let preflight = verify_comparison_preflight(&profile, &measurements)?;
    println!("{}", serde_json::to_string_pretty(&preflight)?);
    Ok(())
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

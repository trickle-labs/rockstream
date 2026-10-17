//! Structural preflight only; not a release-process capacity verdict.
use anyhow::{Context, Result};
use clap::Parser;
use r1_local_harness::matrix::{verify_v072_matrix, MatrixReport};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Validate complete v0.72 matrix structure, not raw process evidence")]
struct Cli {
    #[arg(long)]
    matrix: PathBuf,
}

fn run(cli: Cli) -> Result<()> {
    let bytes = std::fs::read(&cli.matrix).context("read matrix report")?;
    let report: MatrixReport = serde_json::from_slice(&bytes).context("parse matrix report")?;
    verify_v072_matrix(&report)?;
    println!(
        "v0.72 matrix structure is complete; raw process evidence still requires qualification"
    );
    Ok(())
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

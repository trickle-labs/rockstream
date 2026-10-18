//! Cost arithmetic gate only; never converts local data into cloud evidence.
use anyhow::{Context, Result};
use clap::Parser;
use r1_local_harness::report::{validate_itemized_cost, CostBreakdown};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Validate v0.72 itemized local cost arithmetic; not capacity qualification")]
struct Cli {
    #[arg(long)]
    input: PathBuf,
}

fn run(cli: Cli) -> Result<()> {
    let bytes = std::fs::read(&cli.input).context("read cost input")?;
    let cost: CostBreakdown = serde_json::from_slice(&bytes).context("parse itemized cost")?;
    validate_itemized_cost(&cost)?;
    println!(
        "v0.72 local cost arithmetic is consistent; measured-source and capacity qualification remain separate"
    );
    Ok(())
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

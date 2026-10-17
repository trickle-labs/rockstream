//! Check exact independent SQLite result multisets for eight canonical
//! v0.72 workloads. This is only a structural oracle, not capacity evidence.
use anyhow::{Context, Result};
use clap::Parser;
use r1_local_harness::artifact::atomic_json;
use r1_local_harness::v072_oracles::{verify_eight_oracles, OracleEvidenceManifest};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    profile: PathBuf,
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    output: PathBuf,
}

fn run(args: Args) -> Result<()> {
    let manifest: OracleEvidenceManifest = serde_json::from_slice(
        &fs::read(&args.manifest).context("read v0.72 oracle input manifest")?,
    )?;
    let profile = fs::read(&args.profile).context("read frozen v0.72 profile")?;
    let root = args
        .manifest
        .canonicalize()
        .context("resolve oracle manifest")?
        .parent()
        .context("oracle manifest has no parent")?
        .to_path_buf();
    let summary = verify_eight_oracles(&root, &profile, &manifest)?;
    atomic_json(&args.output, &summary)?;
    println!("{}", summary.status);
    Ok(())
}

fn main() {
    if let Err(error) = run(Args::parse()) {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

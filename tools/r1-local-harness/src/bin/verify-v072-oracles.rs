//! Check exact independent SQLite result multisets for eight canonical
//! v0.72 workloads. This is only a structural oracle, not capacity evidence.
use anyhow::{Context, Result};
use clap::Parser;
use r1_local_harness::artifact::atomic_json;
use r1_local_harness::v072_oracles::{verify_eight_oracles, OracleEvidenceManifest};
use std::fs::{self, OpenOptions};
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
    // Reserve a *new* report name exclusively. A replay of this command,
    // another qualification process, or a path alias to an input/result
    // artifact must never overwrite prior evidence, including on failure.
    let parent = args.output.parent().context("report path has no parent")?;
    fs::create_dir_all(parent).context("create report parent directory")?;
    drop(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&args.output)
            .context("report output already exists or cannot be reserved")?,
    );
    let result = atomic_json(&args.output, &summary);
    if result.is_err() {
        let _ = fs::remove_file(&args.output);
    }
    result?;
    println!("{}", summary.status);
    Ok(())
}

fn main() {
    if let Err(error) = run(Args::parse()) {
        eprintln!("VIOLATION: {error:#}");
        std::process::exit(1);
    }
}

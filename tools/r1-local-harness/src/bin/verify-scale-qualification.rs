use anyhow::{bail, Context, Result};
use clap::Parser;
use r1_local_harness::scale_program::{IndependentQualificationChecker, ScaleQualificationBundle};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "verify-scale-qualification")]
struct Cli {
    /// Path to a ScaleQualificationBundle JSON artifact to verify.
    #[arg(long)]
    bundle: PathBuf,
    /// Also execute the mandatory 6-case negative-control suite against the bundle.
    #[arg(long, default_value_t = true)]
    run_negative_controls: bool,
}

fn run(cli: Cli) -> Result<()> {
    let raw = fs::read_to_string(&cli.bundle)
        .with_context(|| format!("read {}", cli.bundle.display()))?;
    let bundle: ScaleQualificationBundle =
        serde_json::from_str(&raw).with_context(|| format!("parse {}", cli.bundle.display()))?;

    if let Err(violation) = IndependentQualificationChecker::verify(&bundle) {
        bail!("qualification bundle rejected: {violation:?}");
    }

    let neg = if cli.run_negative_controls {
        Some(IndependentQualificationChecker::run_negative_control_suite(
            &bundle,
        )?)
    } else {
        None
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "status": "VERIFIED",
            "run_id": bundle.run_id,
            "worker_count": bundle.worker_count,
            "cost_per_million_usd": bundle.cost_report.cost_per_million_correct_durable_changes_usd,
            "negative_controls": neg,
        }))?
    );
    Ok(())
}

fn main() {
    if let Err(err) = run(Cli::parse()) {
        eprintln!("FAIL: {err:#}");
        std::process::exit(1);
    }
}

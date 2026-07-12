use std::fs;
use std::path::PathBuf;

use rockstream_ops::nexmark_regression::{
    compare_against_baseline, parse_summary_line, NexmarkBenchmarkSummary,
};

fn parse_args() -> Result<(PathBuf, PathBuf), String> {
    let mut baseline = None;
    let mut output = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--baseline" => baseline = args.next().map(PathBuf::from),
            "--output" => output = args.next().map(PathBuf::from),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    match (baseline, output) {
        (Some(baseline), Some(output)) => Ok((baseline, output)),
        _ => Err(
            "usage: nexmark_regression_gate --baseline <json> --output <bench-output>".to_string(),
        ),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (baseline_path, output_path) = parse_args().map_err(std::io::Error::other)?;
    let baseline: NexmarkBenchmarkSummary =
        serde_json::from_str(&fs::read_to_string(baseline_path)?)?;
    let output = fs::read_to_string(output_path)?;
    let observed = parse_summary_line(&output)
        .ok_or_else(|| std::io::Error::other("missing [nexmark_summary] JSON line"))?;
    let check = compare_against_baseline(&baseline, &observed);
    if check.passed {
        println!("nexmark regression gate passed");
        return Ok(());
    }
    for failure in check.failures {
        eprintln!("{failure}");
    }
    std::process::exit(1);
}

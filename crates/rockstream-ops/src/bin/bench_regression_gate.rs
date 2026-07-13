use std::fs;
use std::path::PathBuf;

use rockstream_ops::bench_regression::{compare_against_baseline, parse_summary_line};

struct Args {
    baseline: PathBuf,
    output: PathBuf,
    tag: String,
    threshold_pct: f64,
}

fn parse_args() -> Result<Args, String> {
    let mut baseline = None;
    let mut output = None;
    let mut tag = None;
    let mut threshold_pct = 10.0;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--baseline" => baseline = args.next().map(PathBuf::from),
            "--output" => output = args.next().map(PathBuf::from),
            "--tag" => tag = args.next(),
            "--threshold-pct" => {
                threshold_pct = args
                    .next()
                    .ok_or_else(|| "--threshold-pct requires a value".to_string())?
                    .parse::<f64>()
                    .map_err(|e| format!("invalid --threshold-pct: {e}"))?;
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    match (baseline, output, tag) {
        (Some(baseline), Some(output), Some(tag)) => Ok(Args {
            baseline,
            output,
            tag,
            threshold_pct,
        }),
        _ => Err(
            "usage: bench_regression_gate --baseline <json> --output <bench-output> --tag <name> [--threshold-pct 10]"
                .to_string(),
        ),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args().map_err(std::io::Error::other)?;
    let baseline = serde_json::from_str(&fs::read_to_string(&args.baseline)?)?;
    let output = fs::read_to_string(&args.output)?;
    let observed = parse_summary_line(&output, &args.tag).ok_or_else(|| {
        std::io::Error::other(format!(
            "missing [bench_summary:{}] JSON line in {:?}",
            args.tag, args.output
        ))
    })?;
    let check = compare_against_baseline(&baseline, &observed, args.threshold_pct);
    if check.passed {
        println!("bench regression gate ({}) passed", args.tag);
        return Ok(());
    }
    for failure in check.failures {
        eprintln!("{failure}");
    }
    std::process::exit(1);
}

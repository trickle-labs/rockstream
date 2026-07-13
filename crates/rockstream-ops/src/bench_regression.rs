//! Generic benchmark-regression comparator (v0.45.4).
//!
//! Generalizes the Nexmark-specific gate in [`crate::nexmark_regression`] to
//! any subsystem's criterion benchmark suite. Each new bench main prints a
//! single tagged JSON summary line, `[bench_summary:<tag>] {...}`, containing
//! a flat map of `benchmark_id -> mean_nanos`. A shared comparison binary
//! (`bin/bench_regression_gate.rs`) parses that line, compares it against a
//! checked-in baseline JSON file, and fails CI when any benchmark regresses
//! beyond a threshold — or when a previously baselined benchmark id is
//! missing from the observed run (a removed/renamed benchmark must not
//! silently stop being gated).
//!
//! This module does not change `nexmark_regression.rs`'s behavior; it is a
//! pure sibling addition.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// A flat map of benchmark id -> mean nanoseconds, matching the shape of
/// `critcmp`'s own JSON export for comparison purposes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct BenchmarkSummary(pub BTreeMap<String, f64>);

/// Result of comparing an observed [`BenchmarkSummary`] against a baseline.
#[derive(Debug, Clone, PartialEq)]
pub struct RegressionCheck {
    pub passed: bool,
    pub failures: Vec<String>,
}

/// Scan `text` (bench stdout+stderr) for a `[bench_summary:<tag>] {...}` JSON
/// line and parse it into a [`BenchmarkSummary`].
pub fn parse_summary_line(text: &str, tag: &str) -> Option<BenchmarkSummary> {
    let prefix = format!("[bench_summary:{tag}] ");
    text.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .and_then(|json| serde_json::from_str(json).ok())
}

/// Compare `observed` against `baseline`, flagging:
/// - any id present in both where `observed > baseline * (1 + threshold_pct/100)`
/// - any id present in `baseline` but missing from `observed` (a
///   removed/renamed benchmark must not silently stop being gated)
pub fn compare_against_baseline(
    baseline: &BenchmarkSummary,
    observed: &BenchmarkSummary,
    threshold_pct: f64,
) -> RegressionCheck {
    let mut failures = Vec::new();
    let factor = 1.0 + threshold_pct / 100.0;

    for (id, baseline_mean) in &baseline.0 {
        match observed.0.get(id) {
            None => {
                failures.push(format!(
                    "benchmark '{id}' missing from observed results (baseline mean {baseline_mean:.3} ns)"
                ));
            }
            Some(observed_mean) => {
                let limit = baseline_mean * factor;
                if *observed_mean > limit {
                    failures.push(format!(
                        "benchmark '{id}' regressed: baseline {baseline_mean:.3} ns, observed {observed_mean:.3} ns (limit {limit:.3} ns at {threshold_pct}% threshold)"
                    ));
                }
            }
        }
    }

    RegressionCheck {
        passed: failures.is_empty(),
        failures,
    }
}

/// Resolve the workspace's `target/criterion` directory from a bench
/// binary's `CARGO_MANIFEST_DIR` (the crate directory).
///
/// Cargo runs test/bench binaries with the crate directory as the process
/// working directory, so a bare relative `"target/criterion"` resolves
/// incorrectly. This honors an explicit `CARGO_TARGET_DIR` override (used by
/// CI/custom setups) and otherwise assumes the standard `crates/<name>`
/// layout, walking up two directories from the crate root to the workspace
/// root.
pub fn default_criterion_dir(manifest_dir: &str) -> std::path::PathBuf {
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| Path::new(manifest_dir).join("../../target"));
    target_dir.join("criterion")
}

/// Walk `criterion_dir` (normally `target/criterion`) under each named
/// `groups` entry, find every leaf `new/estimates.json` produced by a
/// completed criterion run, and build a [`BenchmarkSummary`] keyed by the
/// benchmark's path relative to `criterion_dir` (e.g.
/// `filter_performance/filter_in_memory/10000`), reading each entry's
/// `mean.point_estimate` (nanoseconds).
///
/// Kept dependency-light with `serde_json` — no new crate is introduced.
pub fn collect_criterion_summary(criterion_dir: &Path, groups: &[&str]) -> BenchmarkSummary {
    let mut summary = BTreeMap::new();
    for group in groups {
        let group_dir = criterion_dir.join(group);
        collect_estimates_recursive(&group_dir, group, &mut summary);
    }
    BenchmarkSummary(summary)
}

fn collect_estimates_recursive(dir: &Path, id_prefix: &str, out: &mut BTreeMap<String, f64>) {
    let estimates_path = dir.join("new").join("estimates.json");
    if estimates_path.is_file() {
        if let Ok(text) = std::fs::read_to_string(&estimates_path) {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(mean) = value
                    .get("mean")
                    .and_then(|m| m.get("point_estimate"))
                    .and_then(|p| p.as_f64())
                {
                    out.insert(id_prefix.to_string(), mean);
                }
            }
        }
    }

    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Skip criterion's own bookkeeping/report directories.
        if name == "new" || name == "base" || name == "report" {
            continue;
        }
        let child_id = format!("{id_prefix}/{name}");
        collect_estimates_recursive(&path, &child_id, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(pairs: &[(&str, f64)]) -> BenchmarkSummary {
        BenchmarkSummary(pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect())
    }

    #[test]
    fn test_regression_gate_passes_on_identical_summary() {
        let baseline = summary(&[("a", 100.0), ("b", 200.0)]);
        let observed = baseline.clone();
        let check = compare_against_baseline(&baseline, &observed, 10.0);
        assert!(check.passed);
        assert!(check.failures.is_empty());
    }

    #[test]
    fn test_regression_gate_fails_on_15pct_slowdown() {
        let baseline = summary(&[("a", 100.0), ("b", 200.0)]);
        let observed = summary(&[("a", 115.0), ("b", 200.0)]);
        let check = compare_against_baseline(&baseline, &observed, 10.0);
        assert!(!check.passed);
        assert_eq!(check.failures.len(), 1);
        assert!(check.failures[0].contains('a'));
    }

    #[test]
    fn test_regression_gate_passes_on_9pct_jitter() {
        let baseline = summary(&[("a", 100.0)]);
        let observed = summary(&[("a", 109.0)]);
        let check = compare_against_baseline(&baseline, &observed, 10.0);
        assert!(check.passed);
    }

    #[test]
    fn test_regression_gate_fails_on_missing_benchmark_id() {
        let baseline = summary(&[("a", 100.0), ("b", 200.0)]);
        let observed = summary(&[("a", 100.0)]);
        let check = compare_against_baseline(&baseline, &observed, 10.0);
        assert!(!check.passed);
        assert_eq!(check.failures.len(), 1);
        assert!(check.failures[0].contains('b'));
        assert!(check.failures[0].contains("missing"));
    }

    #[test]
    fn test_bench_regression_gate_reads_baseline_update_makes_it_pass() {
        // Simulate a reviewed improvement: observed is faster than baseline.
        let baseline = summary(&[("a", 100.0), ("b", 200.0)]);
        let improved = summary(&[("a", 80.0), ("b", 150.0)]);
        let check = compare_against_baseline(&baseline, &improved, 10.0);
        assert!(check.passed, "improvement must not fail the gate");

        // Simulate `make bench-baseline-update`: the improved summary becomes
        // the new baseline. Re-running the gate with the same observed
        // summary must be idempotent and still pass.
        let new_baseline = improved.clone();
        let check_after_update = compare_against_baseline(&new_baseline, &improved, 10.0);
        assert!(
            check_after_update.passed,
            "re-gating right after a baseline update must never self-fail"
        );
    }

    #[test]
    fn test_parse_summary_line_extracts_tagged_json() {
        let text = "some criterion noise\n[bench_summary:storage] {\"get\":123.0}\nmore noise\n";
        let parsed = parse_summary_line(text, "storage").expect("should parse");
        assert_eq!(parsed.0.get("get"), Some(&123.0));
    }

    #[test]
    fn test_parse_summary_line_ignores_other_tags() {
        let text = "[bench_summary:ops] {\"filter\":1.0}\n";
        assert!(parse_summary_line(text, "storage").is_none());
    }

    #[test]
    fn test_collect_criterion_summary_reads_estimates_json() {
        let dir = tempfile::tempdir().unwrap();
        let leaf = dir
            .path()
            .join("filter_performance/filter_in_memory/10000/new");
        std::fs::create_dir_all(&leaf).unwrap();
        std::fs::write(
            leaf.join("estimates.json"),
            r#"{"mean":{"point_estimate":41047.05,"standard_error":1.0}}"#,
        )
        .unwrap();

        let summary = collect_criterion_summary(dir.path(), &["filter_performance"]);
        assert_eq!(
            summary.0.get("filter_performance/filter_in_memory/10000"),
            Some(&41047.05)
        );
    }

    #[test]
    fn test_collect_criterion_summary_ignores_missing_group() {
        let dir = tempfile::tempdir().unwrap();
        let summary = collect_criterion_summary(dir.path(), &["does_not_exist"]);
        assert!(summary.0.is_empty());
    }
}

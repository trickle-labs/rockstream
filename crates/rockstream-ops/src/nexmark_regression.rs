use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NexmarkBenchmarkSummary {
    pub max_delta_amplification: f64,
    pub propagation_latency_p50_ms: f64,
    pub propagation_latency_p99_ms: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegressionCheck {
    pub passed: bool,
    pub failures: Vec<String>,
}

pub fn parse_summary_line(output: &str) -> Option<NexmarkBenchmarkSummary> {
    output
        .lines()
        .find_map(|line| line.strip_prefix("[nexmark_summary] "))
        .and_then(|json| serde_json::from_str(json).ok())
}

pub fn compare_against_baseline(
    baseline: &NexmarkBenchmarkSummary,
    observed: &NexmarkBenchmarkSummary,
) -> RegressionCheck {
    let mut failures = Vec::new();
    if observed.max_delta_amplification > baseline.max_delta_amplification * 1.10 {
        failures.push(format!(
            "delta amplification regressed: baseline {:.3}, observed {:.3}",
            baseline.max_delta_amplification, observed.max_delta_amplification
        ));
    }
    if observed.propagation_latency_p99_ms > baseline.propagation_latency_p99_ms * 1.10 {
        failures.push(format!(
            "propagation latency p99 regressed: baseline {:.3} ms, observed {:.3} ms",
            baseline.propagation_latency_p99_ms, observed.propagation_latency_p99_ms
        ));
    }
    if observed.propagation_latency_p50_ms > baseline.propagation_latency_p50_ms * 1.10 {
        failures.push(format!(
            "propagation latency p50 regressed: baseline {:.3} ms, observed {:.3} ms",
            baseline.propagation_latency_p50_ms, observed.propagation_latency_p50_ms
        ));
    }
    RegressionCheck {
        passed: failures.is_empty(),
        failures,
    }
}

pub fn percentile(samples: &mut [f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)]
}

//! Qualification metrics collector and resource profiler.
//!
//! Captures raw time-series measurements during qualification runs:
//! - Ingestion throughput (rows/sec)
//! - Query latencies (microseconds/milliseconds)
//! - Worker RSS memory consumption (bytes)
//! - File descriptors and socket allocations
//! - Queue depths and state sizes
//! - Object store request operations (GET, PUT, LIST)
//!
//! Provides mathematical statistical summary generation with R-7 linear interpolation percentiles.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

/// Statistical summary of a metric sample series.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricSummary {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub sample_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_per_sec: Option<f64>,
}

/// Raw time-series samples recorded during qualification.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RawMetricsData {
    pub failure_detection_ms: Vec<f64>,
    pub shard_reassignment_ms: Vec<f64>,
    pub freshness_recovery_ms: Vec<f64>,
    pub steady_state_throughput_rows_per_sec: Vec<f64>,
    pub query_latencies_ms: Vec<f64>,
    pub rss_memory_bytes: Vec<f64>,
    pub file_descriptors: Vec<f64>,
    pub open_sockets: Vec<f64>,
    pub queue_depths: Vec<f64>,
    pub state_size_bytes: Vec<f64>,
    pub object_store_requests: BTreeMap<String, u64>,
}

/// Metrics collector for release qualification.
#[derive(Debug, Clone, Default)]
pub struct QualificationMetricsCollector {
    raw: RawMetricsData,
}

impl QualificationMetricsCollector {
    /// Create a new metrics collector.
    pub fn new() -> Self {
        Self {
            raw: RawMetricsData::default(),
        }
    }

    /// Record a batch ingestion throughput sample.
    pub fn record_throughput(&mut self, row_count: u64, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        if secs > 0.0 {
            let tps = row_count as f64 / secs;
            self.raw.steady_state_throughput_rows_per_sec.push(tps);
        }
    }

    /// Record a query latency sample in milliseconds.
    pub fn record_query_latency_ms(&mut self, latency_ms: f64) {
        self.raw.query_latencies_ms.push(latency_ms);
    }

    /// Record RSS memory consumption in bytes.
    pub fn record_rss_bytes(&mut self, rss_bytes: u64) {
        self.raw.rss_memory_bytes.push(rss_bytes as f64);
    }

    /// Record open file descriptor count.
    pub fn record_file_descriptors(&mut self, count: u64) {
        self.raw.file_descriptors.push(count as f64);
    }

    /// Record open socket count.
    pub fn record_open_sockets(&mut self, count: u64) {
        self.raw.open_sockets.push(count as f64);
    }

    /// Record in-flight queue depth.
    pub fn record_queue_depth(&mut self, depth: u64) {
        self.raw.queue_depths.push(depth as f64);
    }

    /// Record state arrangement size in bytes.
    pub fn record_state_size_bytes(&mut self, size_bytes: u64) {
        self.raw.state_size_bytes.push(size_bytes as f64);
    }

    /// Record an object store request operation.
    pub fn record_object_store_request(&mut self, op: &str) {
        *self
            .raw
            .object_store_requests
            .entry(op.to_string())
            .or_insert(0) += 1;
    }

    /// Access raw metrics data.
    pub fn raw_data(&self) -> &RawMetricsData {
        &self.raw
    }

    /// Calculate percentile using standard R-7 linear interpolation.
    pub fn compute_percentile(sorted_samples: &[f64], p: f64) -> f64 {
        if sorted_samples.is_empty() {
            return 0.0;
        }
        if sorted_samples.len() == 1 {
            return sorted_samples[0];
        }
        let rank = p * (sorted_samples.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = (lower + 1).min(sorted_samples.len() - 1);
        let weight = rank - lower as f64;
        sorted_samples[lower] * (1.0 - weight) + sorted_samples[upper] * weight
    }

    /// Calculate mathematical summary for a sample series.
    pub fn calculate_summary(samples: &[f64], throughput: Option<f64>) -> Option<MetricSummary> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let count = sorted.len();
        let sum: f64 = sorted.iter().sum();
        let mean = sum / count as f64;

        Some(MetricSummary {
            p50: Self::compute_percentile(&sorted, 0.50),
            p95: Self::compute_percentile(&sorted, 0.95),
            p99: Self::compute_percentile(&sorted, 0.99),
            mean,
            min: sorted[0],
            max: sorted[count - 1],
            sample_count: count,
            throughput_per_sec: throughput,
        })
    }

    /// Export raw metrics to JSON file.
    pub fn export_raw_json(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json_str = serde_json::to_string_pretty(&self.raw)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut file = File::create(path)?;
        file.write_all(json_str.as_bytes())?;
        Ok(())
    }
}

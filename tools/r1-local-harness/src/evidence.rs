use crate::metrics::WorkerActivity;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreshnessHistogram {
    pub upper_bounds_ms: Vec<u64>,
    pub counts: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessUsage {
    pub role: String,
    pub pid: u32,
    pub user_cpu_ns: u64,
    pub system_cpu_ns: u64,
    pub rss_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSample {
    pub schema_version: u32,
    pub run_id: String,
    pub pair_id: String,
    pub order: String,
    pub candidate_id: String,
    pub binary_sha256: String,
    pub profile_sha256: String,
    pub corpus_sha256: String,
    pub thresholds_sha256: String,
    pub workload: String,
    pub strategy: String,
    pub worker_count: u32,
    pub seed: u64,
    pub change_stream_sha256: String,
    pub monotonic_duration_ns: u64,
    pub accepted_changes: u64,
    pub visible_changes: u64,
    pub freshness_histogram: FreshnessHistogram,
    pub processes: Vec<ProcessUsage>,
    pub logical_bytes: u64,
    pub lfs_bytes: u64,
    pub exchange_bytes: u64,
    pub max_queue_depth: u64,
    pub operator_counters: BTreeMap<String, u64>,
    pub workers: Vec<WorkerActivity>,
    pub canonical_input_sha256: String,
    pub rockstream_output_sha256: String,
    pub sqlite_oracle_output_sha256: String,
    pub outputs_equal: bool,
}

impl RawSample {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 {
            bail!("raw sample {} schema_version must be 1", self.run_id);
        }
        for (name, digest) in [
            ("binary", &self.binary_sha256),
            ("profile", &self.profile_sha256),
            ("corpus", &self.corpus_sha256),
            ("thresholds", &self.thresholds_sha256),
            ("change stream", &self.change_stream_sha256),
            ("canonical input", &self.canonical_input_sha256),
            ("RockStream output", &self.rockstream_output_sha256),
            ("SQLite output", &self.sqlite_oracle_output_sha256),
        ] {
            if !is_sha256(digest) {
                bail!("raw sample {} has invalid {name} digest", self.run_id);
            }
        }
        if self.accepted_changes == 0 || self.accepted_changes != self.visible_changes {
            bail!("raw sample {} has incomplete visible changes", self.run_id);
        }
        if !self.outputs_equal || self.rockstream_output_sha256 != self.sqlite_oracle_output_sha256
        {
            bail!("raw sample {} output differs from SQLite", self.run_id);
        }
        if self.monotonic_duration_ns == 0 || self.processes.is_empty() {
            bail!("raw sample {} is missing runtime counters", self.run_id);
        }
        if self.freshness_histogram.upper_bounds_ms.is_empty()
            || self.freshness_histogram.upper_bounds_ms.len()
                != self.freshness_histogram.counts.len()
        {
            bail!(
                "raw sample {} has an invalid freshness histogram",
                self.run_id
            );
        }
        let baseline = self.candidate_id.starts_with("b0-");
        if !baseline && self.operator_counters.is_empty() {
            bail!("raw sample {} is missing runtime counters", self.run_id);
        }
        if self.workers.len() != self.worker_count as usize {
            bail!("raw sample {} is missing declared workers", self.run_id);
        }
        for worker in &self.workers {
            if worker.shards_owned == 0 || worker.input_rows == 0 || worker.output_rows == 0 {
                bail!(
                    "raw sample {} has an idle worker {}",
                    self.run_id,
                    worker.worker_id
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralResult {
    pub name: String,
    pub passed: bool,
    pub counters: BTreeMap<String, u64>,
    pub log_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralEvidence {
    pub schema_version: u32,
    pub results: Vec<StructuralResult>,
}

pub fn read_samples(path: &Path) -> Result<Vec<RawSample>> {
    let contents = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let mut ids = BTreeSet::new();
    contents
        .lines()
        .enumerate()
        .map(|(index, line)| {
            let sample: RawSample = serde_json::from_str(line)
                .with_context(|| format!("parse {} line {}", path.display(), index + 1))?;
            sample.validate()?;
            if !ids.insert(sample.run_id.clone()) {
                bail!("duplicate raw sample run_id {}", sample.run_id);
            }
            Ok(sample)
        })
        .collect()
}

pub fn read_structural(path: &Path) -> Result<StructuralEvidence> {
    let evidence: StructuralEvidence = serde_json::from_slice(
        &fs::read(path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse {}", path.display()))?;
    if evidence.schema_version != 1 || evidence.results.is_empty() {
        bail!("structural evidence is incomplete");
    }
    for result in &evidence.results {
        if !result.passed || !is_sha256(&result.log_sha256) || result.counters.is_empty() {
            bail!("structural result {} failed or is incomplete", result.name);
        }
    }
    Ok(evidence)
}

fn is_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

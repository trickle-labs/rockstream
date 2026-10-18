use crate::evidence::{read_samples, RawSample};
use crate::matrix::{verify_matrix, MatrixReport};
use anyhow::{bail, Context, Result};
use std::fs;
use std::path::Path;

pub fn qualify_sample(sample: &RawSample) -> Result<()> {
    if sample.candidate_id == "sample_reference_run"
        || sample.run_id.contains("sample_reference_run")
    {
        bail!(
            "raw sample {} uses synthetic sample_reference_run: synthetic sample disallowed in release baseline",
            sample.run_id
        );
    }
    if !sample.outputs_equal
        || sample.rockstream_output_sha256 != sample.sqlite_oracle_output_sha256
    {
        bail!(
            "raw sample {} output differs from SQLite: oracle mismatch detected",
            sample.run_id
        );
    }
    if !is_sha256(&sample.binary_sha256) {
        bail!("raw sample {} has invalid binary digest", sample.run_id);
    }
    sample.validate()?;
    for (name, value) in [
        ("state_writes_per_change", sample.state_writes_per_change),
        (
            "intermediate_rows_per_change",
            sample.intermediate_rows_per_change,
        ),
        ("network_bytes_per_change", sample.network_bytes_per_change),
        (
            "object_store_requests_per_change",
            sample.object_store_requests_per_change,
        ),
        ("read_p99_ms", sample.read_p99_ms),
        ("commit_p99_ms", sample.commit_p99_ms),
        ("freshness_p99_ms", sample.freshness_p99_ms),
        ("generator_delay_p99_ms", sample.generator_delay_p99_ms),
        ("queue_age_ms", sample.queue_age_ms),
        ("control_node_load", sample.control_node_load),
        (
            "physical_flushes_per_epoch",
            sample.physical_flushes_per_epoch,
        ),
    ] {
        let value =
            value.with_context(|| format!("raw sample {} missing {name}", sample.run_id))?;
        if !value.is_finite() || value < 0.0 {
            bail!("raw sample {} invalid {name}", sample.run_id);
        }
    }
    for (name, value) in [
        ("generator_queue_drops", sample.generator_queue_drops),
        ("timeouts_and_errors", sample.timeouts_and_errors),
    ] {
        let value =
            value.with_context(|| format!("raw sample {} missing {name}", sample.run_id))?;
        if value != 0 {
            bail!("raw sample {} has nonzero {name}: {value}", sample.run_id);
        }
    }
    Ok(())
}

pub fn qualify_matrix(report: &MatrixReport) -> Result<()> {
    verify_matrix(report)
}

/// Validate a complete legacy evidence bundle, never an optional subset.
/// This integrity check is not the v0.72 release-process capacity verdict.
pub fn qualify_evidence(evidence_dir: &Path) -> Result<()> {
    let matrix_path = evidence_dir.join("matrix-report.json");
    let samples_path = evidence_dir.join("raw-samples.jsonl");
    for (path, name) in [
        (&matrix_path, "matrix-report.json"),
        (&samples_path, "raw-samples.jsonl"),
    ] {
        if !path.is_file() {
            bail!("missing required evidence file: {name}");
        }
    }

    let content =
        fs::read(&matrix_path).with_context(|| format!("read {}", matrix_path.display()))?;
    let report: MatrixReport = serde_json::from_slice(&content)
        .with_context(|| format!("parse {}", matrix_path.display()))?;
    qualify_matrix(&report)?;

    let samples = read_samples(&samples_path)?;
    if samples.is_empty() {
        bail!("raw-samples.jsonl contains no runs");
    }
    for sample in &samples {
        qualify_sample(sample)?;
    }

    Ok(())
}

fn is_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

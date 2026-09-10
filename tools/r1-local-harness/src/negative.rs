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
    Ok(())
}

pub fn qualify_matrix(report: &MatrixReport) -> Result<()> {
    verify_matrix(report)
}

pub fn qualify_evidence(evidence_dir: &Path) -> Result<()> {
    let matrix_path = evidence_dir.join("matrix-report.json");
    if matrix_path.exists() {
        let content =
            fs::read(&matrix_path).with_context(|| format!("read {}", matrix_path.display()))?;
        let report: MatrixReport = serde_json::from_slice(&content)
            .with_context(|| format!("parse {}", matrix_path.display()))?;
        qualify_matrix(&report)?;
    }

    let samples_path = evidence_dir.join("raw-samples.jsonl");
    if samples_path.exists() {
        let samples = read_samples(&samples_path)?;
        for sample in &samples {
            qualify_sample(sample)?;
        }
    }

    Ok(())
}

fn is_sha256(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

use crate::artifact::atomic_json;
use crate::evidence::{read_samples, read_structural, RawSample};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryCell {
    pub workload: String,
    pub candidate_id: String,
    pub strategy: String,
    pub worker_count: u32,
    pub raw_throughput_rows_per_second: Vec<f64>,
    pub mean_throughput_rows_per_second: f64,
    pub coefficient_of_variation: f64,
    pub max_coefficient_of_variation: f64,
    pub comparator: String,
    pub verdict: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Decision {
    pub schema_version: u32,
    pub verdict: String,
    pub raw_sample_count: usize,
    pub structural_result_count: usize,
    pub cells: Vec<SummaryCell>,
}

pub fn evaluate(evidence_dir: &Path) -> Result<Decision> {
    let samples = read_samples(&evidence_dir.join("raw-samples.jsonl"))?;
    let structural = read_structural(&evidence_dir.join("structural-results.json"))?;
    let mut groups: BTreeMap<(String, String, String, u32), Vec<&RawSample>> = BTreeMap::new();
    for sample in &samples {
        groups
            .entry((
                sample.workload.clone(),
                sample.candidate_id.clone(),
                sample.strategy.clone(),
                sample.worker_count,
            ))
            .or_default()
            .push(sample);
    }
    let mut cells = Vec::with_capacity(groups.len());
    for ((workload, candidate_id, strategy, worker_count), group) in groups {
        if group.len() != 5 {
            bail!("cell {workload}/{candidate_id}/{strategy}/{worker_count} has {} samples, expected 5", group.len());
        }
        let values = group
            .iter()
            .map(|sample| {
                sample.visible_changes as f64 * 1_000_000_000.0
                    / sample.monotonic_duration_ns as f64
            })
            .collect::<Vec<_>>();
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let cv = coefficient_of_variation(&values)?;
        cells.push(SummaryCell {
            workload,
            candidate_id,
            strategy,
            worker_count,
            raw_throughput_rows_per_second: values,
            mean_throughput_rows_per_second: mean,
            coefficient_of_variation: cv,
            max_coefficient_of_variation: 0.15,
            comparator: "<=".to_string(),
            verdict: if cv <= 0.15 { "GREEN" } else { "RED" }.to_string(),
        });
    }
    if cells.is_empty() {
        bail!("raw evidence has no timing cells");
    }
    let verdict = if structural.results.len() == 3
        && cells.iter().all(|cell| cell.verdict == "GREEN")
    {
        "GREEN"
    } else {
        "INCOMPLETE"
    };
    Ok(Decision {
        schema_version: 1,
        verdict: verdict.to_string(),
        raw_sample_count: samples.len(),
        structural_result_count: structural.results.len(),
        cells,
    })
}

pub fn write_decision(evidence_dir: &Path) -> Result<Decision> {
    let decision = evaluate(evidence_dir)?;
    atomic_json(&evidence_dir.join("decision.json"), &decision)?;
    Ok(decision)
}

pub fn verify(evidence_dir: &Path) -> Result<()> {
    let expected = evaluate(evidence_dir)?;
    let actual: Decision = serde_json::from_slice(
        &fs::read(evidence_dir.join("decision.json")).context("read decision.json")?,
    )
    .context("parse decision.json")?;
    if actual != expected {
        bail!("decision.json does not regenerate from raw evidence");
    }
    Ok(())
}

fn coefficient_of_variation(values: &[f64]) -> Result<f64> {
    if values.len() < 2 || values.iter().any(|value| !value.is_finite()) {
        bail!("CV requires at least two finite samples");
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    if mean == 0.0 {
        return if values.iter().all(|value| *value == 0.0) {
            Ok(0.0)
        } else {
            bail!("CV is undefined for a zero mean")
        };
    }
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    Ok(variance.sqrt() / mean.abs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_cv_uses_n_minus_one() {
        assert_eq!(
            coefficient_of_variation(&[8.0, 9.0, 10.0, 11.0, 12.0]).unwrap(),
            0.15811388300841897
        );
    }
}

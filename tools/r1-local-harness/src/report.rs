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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CostBreakdown {
    pub hourly_gateways_cost: f64,
    pub hourly_control_nodes_cost: f64,
    pub hourly_workers_cost: f64,
    pub hourly_storage_requests_cost: f64,
    pub hourly_retained_storage_cost: f64,
    pub hourly_network_transfer_cost: f64,
    pub hourly_compaction_cost: f64,
    pub total_hourly_cost: f64,
    pub sustainable_changes_per_sec: f64,
    pub cost_per_million_changes: f64,
    pub pricing_scope: String,
    pub pricing_date: Option<String>,
}

pub fn calculate_cost_per_million(
    hourly_cost: f64,
    sustainable_changes_per_sec: f64,
) -> Result<f64> {
    if !hourly_cost.is_finite() || hourly_cost < 0.0 {
        bail!("hourly_cost must be finite and nonnegative");
    }
    if !sustainable_changes_per_sec.is_finite() || sustainable_changes_per_sec <= 0.0 {
        bail!("sustainable_changes_per_sec must be finite and positive");
    }
    let numerator = hourly_cost * 1_000_000.0;
    let denominator = sustainable_changes_per_sec * 3_600.0;
    if !numerator.is_finite() || !denominator.is_finite() || denominator <= 0.0 {
        bail!("cost_per_million arithmetic overflow");
    }
    let cost = numerator / denominator;
    if !cost.is_finite() {
        bail!("cost_per_million is non-finite");
    }
    Ok(cost)
}

/// Recompute itemized cost from declared components and reject fabricated or
/// inconsistent totals. This is arithmetic validation only: external cloud
/// cost provenance still needs a measured deployment and dated source prices.
pub fn validate_itemized_cost(breakdown: &CostBreakdown) -> Result<()> {
    let items = [
        ("gateways", breakdown.hourly_gateways_cost),
        ("control nodes", breakdown.hourly_control_nodes_cost),
        ("workers", breakdown.hourly_workers_cost),
        ("storage requests", breakdown.hourly_storage_requests_cost),
        ("retained storage", breakdown.hourly_retained_storage_cost),
        ("network transfer", breakdown.hourly_network_transfer_cost),
        ("compaction", breakdown.hourly_compaction_cost),
    ];
    let mut total = 0.0_f64;
    for (name, hourly) in items {
        if !hourly.is_finite() || hourly < 0.0 {
            bail!("invalid hourly cost for {name}");
        }
        total += hourly;
        if !total.is_finite() {
            bail!("hourly cost item sum overflow");
        }
    }
    if !breakdown.total_hourly_cost.is_finite() || breakdown.total_hourly_cost < 0.0 {
        bail!("total_hourly_cost must be finite and nonnegative");
    }
    let tolerance = 1e-9_f64.max(total.abs() * 1e-12);
    if (total - breakdown.total_hourly_cost).abs() > tolerance {
        bail!("itemized hourly cost sum differs from total_hourly_cost");
    }
    let expected = calculate_cost_per_million(total, breakdown.sustainable_changes_per_sec)?;
    if !breakdown.cost_per_million_changes.is_finite()
        || breakdown.cost_per_million_changes < 0.0
        || (expected - breakdown.cost_per_million_changes).abs()
            > 1e-9_f64.max(expected.abs() * 1e-12)
    {
        bail!("cost_per_million_changes does not match itemized cost and measured throughput");
    }
    match breakdown.pricing_scope.as_str() {
        "local-observed" => {}
        "cloud-production-dated" => {
            // No cloud-deployment capture is currently bound to CostBreakdown:
            // a date and arbitrary items do not prove these are cloud prices.
            bail!("cloud cost requires measured deployment provenance; itemized data alone cannot qualify")
        }
        other => bail!("unsupported pricing_scope {other}"),
    }
    if let Some(date) = &breakdown.pricing_date {
        if !is_valid_iso_date(date) {
            bail!("pricing_date must be a valid ISO 8601 calendar date");
        }
    }
    Ok(())
}

fn is_valid_iso_date(date: &str) -> bool {
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(i, byte)| i == 4 || i == 7 || byte.is_ascii_digit())
    {
        return false;
    }
    let (Ok(year), Ok(month), Ok(day)) = (
        date[0..4].parse::<u32>(),
        date[5..7].parse::<usize>(),
        date[8..10].parse::<u32>(),
    ) else {
        return false;
    };
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    day > 0 && day <= days_in_month[month - 1]
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
    let verdict =
        if structural.results.len() == 3 && cells.iter().all(|cell| cell.verdict == "GREEN") {
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

    fn local_cost() -> CostBreakdown {
        CostBreakdown {
            hourly_gateways_cost: 0.8,
            hourly_control_nodes_cost: 0.8,
            hourly_workers_cost: 1.6,
            hourly_storage_requests_cost: 0.1,
            hourly_retained_storage_cost: 0.1,
            hourly_network_transfer_cost: 0.1,
            hourly_compaction_cost: 0.1,
            total_hourly_cost: 3.6,
            sustainable_changes_per_sec: 1_000.0,
            cost_per_million_changes: 1.0,
            pricing_scope: "local-observed".to_owned(),
            pricing_date: Some("2026-10-17".to_owned()),
        }
    }

    #[test]
    fn v072_itemized_local_cost_regenerates_exact_total_and_rate() {
        let cost = local_cost();
        validate_itemized_cost(&cost).unwrap();
        assert_eq!(calculate_cost_per_million(3.6, 1_000.0).unwrap(), 1.0);
    }

    #[test]
    fn v072_rejects_incomplete_or_non_finite_cost_items() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            let mut cost = local_cost();
            cost.hourly_storage_requests_cost = bad;
            assert!(validate_itemized_cost(&cost).is_err());
            assert!(calculate_cost_per_million(bad, 1_000.0).is_err());
        }
        for rate in [0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            assert!(calculate_cost_per_million(3.6, rate).is_err());
        }
        assert!(calculate_cost_per_million(f64::MAX, 1_000.0).is_err());
        assert!(calculate_cost_per_million(3.6, f64::MAX).is_err());
    }

    #[test]
    fn v072_rejects_wrong_total_wrong_cost_per_million_and_cloud_without_proof() {
        let mut cost = local_cost();
        cost.total_hourly_cost = 4.0;
        assert!(validate_itemized_cost(&cost)
            .unwrap_err()
            .to_string()
            .contains("sum differs"));
        cost = local_cost();
        cost.cost_per_million_changes = 2.0;
        assert!(validate_itemized_cost(&cost)
            .unwrap_err()
            .to_string()
            .contains("does not match"));
        cost = local_cost();
        cost.pricing_scope = "cloud-production-dated".to_owned();
        assert_eq!(
            validate_itemized_cost(&cost).unwrap_err().to_string(),
            "cloud cost requires measured deployment provenance; itemized data alone cannot qualify"
        );
        cost = local_cost();
        cost.pricing_scope = "mystery".to_owned();
        assert_eq!(
            validate_itemized_cost(&cost).unwrap_err().to_string(),
            "unsupported pricing_scope mystery"
        );
    }

    #[test]
    fn v072_rejects_invalid_calendar_dates_in_cost_sources() {
        for bad in [
            "2026-02-29",
            "2000-02-30",
            "2026-13-01",
            "0000-01-01",
            "2026-01-00",
        ] {
            let mut cost = local_cost();
            cost.pricing_date = Some(bad.to_owned());
            assert_eq!(
                validate_itemized_cost(&cost).unwrap_err().to_string(),
                "pricing_date must be a valid ISO 8601 calendar date"
            );
        }
        for good in ["2000-02-29", "2024-02-29", "2026-12-31"] {
            let mut cost = local_cost();
            cost.pricing_date = Some(good.to_owned());
            validate_itemized_cost(&cost).unwrap();
        }
    }

    #[test]
    fn sample_cv_uses_n_minus_one() {
        assert_eq!(
            coefficient_of_variation(&[8.0, 9.0, 10.0, 11.0, 12.0]).unwrap(),
            0.15811388300841897
        );
    }
}

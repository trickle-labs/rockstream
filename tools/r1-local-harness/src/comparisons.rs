//! Structural comparison gate for the frozen v0.72 scale matrix (#139).
//!
//! This gate does not execute a release binary, independently derive oracle
//! rows, or attest process capture provenance. A passing preflight is NOT a
//! capacity claim. Separate release-process evidence remains mandatory.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const FROZEN_PROFILE_SHA256: &str =
    "d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    OneKey1k,
    OneKey100k,
    OneKey10m,
    ViewsOne,
    Views20Private,
    Views20Shared,
    Worker1,
    Worker2,
    Worker4,
    Worker8,
    MixedRead,
}

impl Scenario {
    pub const ALL: [Self; 11] = [
        Self::OneKey1k,
        Self::OneKey100k,
        Self::OneKey10m,
        Self::ViewsOne,
        Self::Views20Private,
        Self::Views20Shared,
        Self::Worker1,
        Self::Worker2,
        Self::Worker4,
        Self::Worker8,
        Self::MixedRead,
    ];

    fn shape(self) -> (u32, u32, Option<u64>, Option<bool>) {
        match self {
            Self::OneKey1k => (1, 1, Some(1_000), Some(false)),
            Self::OneKey100k => (1, 1, Some(100_000), Some(false)),
            Self::OneKey10m => (1, 1, Some(10_000_000), Some(false)),
            Self::ViewsOne => (1, 1, None, Some(false)),
            Self::Views20Private => (1, 20, None, Some(false)),
            Self::Views20Shared => (1, 20, None, Some(true)),
            Self::Worker1 => (1, 1, None, Some(false)),
            Self::Worker2 => (2, 1, None, Some(false)),
            Self::Worker4 => (4, 1, None, Some(false)),
            Self::Worker8 => (8, 1, None, Some(false)),
            Self::MixedRead => (1, 1, None, Some(false)),
        }
    }

    fn id(self) -> &'static str {
        match self {
            Self::OneKey1k => "one_key_1k",
            Self::OneKey100k => "one_key_100k",
            Self::OneKey10m => "one_key_10m",
            Self::ViewsOne => "views_one",
            Self::Views20Private => "views_20_private",
            Self::Views20Shared => "views_20_shared",
            Self::Worker1 => "worker_1",
            Self::Worker2 => "worker_2",
            Self::Worker4 => "worker_4",
            Self::Worker8 => "worker_8",
            Self::MixedRead => "mixed_read",
        }
    }
}

/// Per-run measurements must be sourced from public interfaces and independent
/// full-result oracles. Digests are structural links, not self-validating proof.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonMeasurement {
    pub run_id: String,
    pub scenario: Scenario,
    pub profile_sha256: String,
    pub binary_sha256: String,
    pub machine_chip: String,
    pub backend: String,
    pub offered_rows_per_sec: u64,
    pub payload_bytes: u64,
    pub durability_mode: String,
    pub freshness_slo_ms: u64,
    pub workers: Vec<u32>,
    pub view_count: u32,
    pub sharing: bool,
    pub live_groups: Option<u64>,
    pub expected_complete_result_sha256: String,
    pub observed_complete_result_sha256: String,
    pub process_capture_sha256: String,
    pub committed_changes: u64,
    pub measurement_duration_ns: u64,
    pub state_bytes: u64,
    pub state_writes: u64,
    pub process_cpu_ns: u64,
    pub lookup_p99_ms: Option<f64>,
    pub read_p99_ms: Option<f64>,
    pub durable_commit_p99_ms: Option<f64>,
    pub freshness_p99_ms: Option<f64>,
    pub generator_delay_p99_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonPreflight {
    pub schema_version: u32,
    pub status: String,
    pub sample_count: usize,
    pub repetition_count: usize,
    pub observed_ratios: BTreeMap<String, f64>,
}

#[derive(Debug, Deserialize)]
struct FrozenProfile {
    profile_id: String,
    revision: u32,
    state: String,
    machine: Machine,
    load: Load,
    workloads: Workloads,
    comparisons: Limits,
    latency: Latency,
}

#[derive(Debug, Deserialize)]
struct Machine {
    chip: String,
    backend: String,
}

#[derive(Debug, Deserialize)]
struct Load {
    payload_bytes: u64,
    offered_rows_per_sec: Vec<u64>,
    repetitions: usize,
    max_sample_cv: f64,
    measurement_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct Workloads {
    freshness_slo_ms: u64,
}

#[derive(Debug, Deserialize)]
struct Latency {
    read_p99_ms: f64,
    durable_commit_p99_ms: f64,
    correct_committed_result_freshness_p99_ms: f64,
    max_generator_delay_ms: f64,
}

#[derive(Debug, Deserialize)]
struct Limits {
    max_one_key_write_ratio: f64,
    max_shared_20_to_private_20_state_ratio: f64,
    max_shared_20_to_private_20_write_ratio: f64,
    max_shared_20_to_private_20_cpu_ratio: f64,
    min_two_to_one_worker_throughput_ratio: f64,
    min_four_to_one_worker_throughput_ratio: f64,
    min_eight_to_one_worker_throughput_ratio: f64,
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn nonnegative(value: f64, label: &str, run: &str) -> Result<f64> {
    if !value.is_finite() || value < 0.0 {
        bail!("invalid {label} in {run}");
    }
    Ok(value)
}

fn ratio(numerator: f64, denominator: f64, label: &str) -> Result<f64> {
    if !numerator.is_finite() || !denominator.is_finite() || denominator <= 0.0 {
        bail!("unavailable {label} denominator or invalid numerator");
    }
    let value = numerator / denominator;
    if !value.is_finite() {
        bail!("invalid {label} ratio");
    }
    Ok(value)
}

fn mean(values: &[f64]) -> Result<f64> {
    if values.is_empty() || values.iter().any(|x| !x.is_finite() || *x < 0.0) {
        bail!("invalid comparison samples");
    }
    let result = values.iter().sum::<f64>() / values.len() as f64;
    if !result.is_finite() {
        bail!("comparison sample mean overflow");
    }
    Ok(result)
}

fn cv(values: &[f64]) -> Result<f64> {
    if values.len() < 2 {
        bail!("comparison variation requires repeated runs");
    }
    let m = mean(values)?;
    if m <= 0.0 {
        bail!("comparison throughput mean must be positive");
    }
    let variance = values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
    Ok(variance.sqrt() / m)
}

/// Check only the *structure and arithmetic* of all #139 comparison groups.
/// This does not verify that the digests belong to actual independent oracles
/// or real process evidence. No successful result means V072 is qualified.
pub fn verify_comparison_preflight(
    frozen_profile_bytes: &[u8],
    measurements: &[ComparisonMeasurement],
) -> Result<ComparisonPreflight> {
    if crate::artifact::sha256(frozen_profile_bytes) != FROZEN_PROFILE_SHA256 {
        bail!("frozen v0.72 profile digest mismatch");
    }
    let profile: FrozenProfile = toml::from_str(std::str::from_utf8(frozen_profile_bytes)?)
        .context("parse frozen profile")?;
    if profile.profile_id != "v072-m5pro-48gb-v1"
        || profile.revision != 1
        || profile.state != "frozen"
        || profile.load.repetitions != 5
    {
        bail!("unexpected frozen profile revision or repetition count");
    }
    let expected = Scenario::ALL.len() * profile.load.repetitions;
    if measurements.len() != expected {
        bail!(
            "comparison matrix has {} samples; expected {expected}",
            measurements.len()
        );
    }
    let mut groups: BTreeMap<Scenario, Vec<&ComparisonMeasurement>> = BTreeMap::new();
    let mut run_ids = BTreeSet::new();
    let mut common: Option<(String, u64)> = None;
    for m in measurements {
        if m.run_id.trim().is_empty() || !run_ids.insert(m.run_id.clone()) {
            bail!("empty or duplicate comparison run_id {}", m.run_id);
        }
        if m.profile_sha256 != FROZEN_PROFILE_SHA256
            || !digest(&m.binary_sha256)
            || !digest(&m.process_capture_sha256)
            || !digest(&m.expected_complete_result_sha256)
            || !digest(&m.observed_complete_result_sha256)
            || m.expected_complete_result_sha256 != m.observed_complete_result_sha256
        {
            bail!(
                "invalid or unequal provenance/oracle digests for {}",
                m.run_id
            );
        }
        if m.machine_chip != profile.machine.chip
            || m.backend != profile.machine.backend
            || m.payload_bytes != profile.load.payload_bytes
            || m.freshness_slo_ms != profile.workloads.freshness_slo_ms
            || m.durability_mode != "Durable"
            || !profile
                .load
                .offered_rows_per_sec
                .contains(&m.offered_rows_per_sec)
        {
            bail!("frozen comparison context mismatch for {}", m.run_id);
        }
        match &common {
            Some((binary, offered))
                if binary != &m.binary_sha256 || *offered != m.offered_rows_per_sec =>
            {
                bail!(
                    "comparison changed candidate or offered load at {}",
                    m.run_id
                );
            }
            None => common = Some((m.binary_sha256.clone(), m.offered_rows_per_sec)),
            _ => {}
        }
        let (workers, views, groups_expected, sharing) = m.scenario.shape();
        if m.workers.len() != workers as usize
            || m.workers.contains(&0)
            || m.workers.iter().copied().collect::<BTreeSet<_>>().len() != m.workers.len()
            || m.view_count != views
            || Some(m.sharing) != sharing
            || m.live_groups != groups_expected
        {
            bail!("invalid observed comparison topology for {}", m.run_id);
        }
        if m.committed_changes == 0
            || m.measurement_duration_ns < profile.load.measurement_seconds * 1_000_000_000
            || m.process_cpu_ns == 0
        {
            bail!(
                "invalid measured duration, changes, or CPU for {}",
                m.run_id
            );
        }
        for (label, value) in [
            ("lookup_p99_ms", m.lookup_p99_ms),
            ("read_p99_ms", m.read_p99_ms),
            ("durable_commit_p99_ms", m.durable_commit_p99_ms),
            ("freshness_p99_ms", m.freshness_p99_ms),
            ("generator_delay_p99_ms", m.generator_delay_p99_ms),
        ] {
            if let Some(value) = value {
                nonnegative(value, label, &m.run_id)?;
            }
        }
        if matches!(
            m.scenario,
            Scenario::OneKey1k | Scenario::OneKey100k | Scenario::OneKey10m
        ) && m.lookup_p99_ms.is_none()
        {
            bail!(
                "one-key lookup cost must be measured separately for {}",
                m.run_id
            );
        }
        if m.scenario == Scenario::MixedRead {
            for (name, observed, max) in [
                ("read_p99_ms", m.read_p99_ms, profile.latency.read_p99_ms),
                (
                    "durable_commit_p99_ms",
                    m.durable_commit_p99_ms,
                    profile.latency.durable_commit_p99_ms,
                ),
                (
                    "freshness_p99_ms",
                    m.freshness_p99_ms,
                    profile.latency.correct_committed_result_freshness_p99_ms,
                ),
                (
                    "generator_delay_p99_ms",
                    m.generator_delay_p99_ms,
                    profile.latency.max_generator_delay_ms,
                ),
            ] {
                let actual =
                    observed.ok_or_else(|| anyhow::anyhow!("missing {name} for {}", m.run_id))?;
                if actual > max {
                    bail!("{name} exceeds frozen threshold in {}", m.run_id);
                }
            }
        }
        groups.entry(m.scenario).or_default().push(m);
    }
    let mut throughput = BTreeMap::new();
    let mut writes = BTreeMap::new();
    let mut state = BTreeMap::new();
    let mut cpu = BTreeMap::new();
    for scenario in Scenario::ALL {
        let group = groups
            .get(&scenario)
            .ok_or_else(|| anyhow::anyhow!("missing comparison scenario {}", scenario.id()))?;
        if group.len() != profile.load.repetitions {
            bail!(
                "scenario {} has {} samples, expected {}",
                scenario.id(),
                group.len(),
                profile.load.repetitions
            );
        }
        let rates: Vec<f64> = group
            .iter()
            .map(|m| {
                m.committed_changes as f64 * 1_000_000_000.0 / m.measurement_duration_ns as f64
            })
            .collect();
        let variation = cv(&rates)?;
        if !variation.is_finite() || variation > profile.load.max_sample_cv {
            bail!(
                "scenario {} throughput CV exceeds frozen noise budget",
                scenario.id()
            );
        }
        throughput.insert(scenario, mean(&rates)?);
        writes.insert(
            scenario,
            mean(
                &group
                    .iter()
                    .map(|m| m.state_writes as f64 / m.committed_changes as f64)
                    .collect::<Vec<_>>(),
            )?,
        );
        state.insert(
            scenario,
            mean(
                &group
                    .iter()
                    .map(|m| m.state_bytes as f64)
                    .collect::<Vec<_>>(),
            )?,
        );
        cpu.insert(
            scenario,
            mean(
                &group
                    .iter()
                    .map(|m| m.process_cpu_ns as f64)
                    .collect::<Vec<_>>(),
            )?,
        );
    }
    let mut observed_ratios = BTreeMap::new();
    let one_key_min = [
        writes[&Scenario::OneKey1k],
        writes[&Scenario::OneKey100k],
        writes[&Scenario::OneKey10m],
    ]
    .into_iter()
    .reduce(f64::min)
    .unwrap_or(0.0);
    let one_key_max = [
        writes[&Scenario::OneKey1k],
        writes[&Scenario::OneKey100k],
        writes[&Scenario::OneKey10m],
    ]
    .into_iter()
    .reduce(f64::max)
    .unwrap_or(0.0);
    observed_ratios.insert(
        "one_key_max_to_min_writes".to_string(),
        ratio(one_key_max, one_key_min, "one-key writes")?,
    );
    for (name, numerator, denominator, max) in [
        (
            "shared_20_state",
            state[&Scenario::Views20Shared],
            state[&Scenario::Views20Private],
            profile.comparisons.max_shared_20_to_private_20_state_ratio,
        ),
        (
            "shared_20_writes",
            writes[&Scenario::Views20Shared],
            writes[&Scenario::Views20Private],
            profile.comparisons.max_shared_20_to_private_20_write_ratio,
        ),
        (
            "shared_20_cpu",
            cpu[&Scenario::Views20Shared],
            cpu[&Scenario::Views20Private],
            profile.comparisons.max_shared_20_to_private_20_cpu_ratio,
        ),
    ] {
        let value = ratio(numerator, denominator, name)?;
        observed_ratios.insert(name.to_owned(), value);
        if value > max {
            bail!("{name} ratio {value} exceeds frozen threshold {max}");
        }
    }
    if observed_ratios["one_key_max_to_min_writes"] > profile.comparisons.max_one_key_write_ratio {
        bail!("one-key write ratio exceeds frozen threshold");
    }
    for (name, scenario, threshold) in [
        (
            "worker_2_to_1",
            Scenario::Worker2,
            profile.comparisons.min_two_to_one_worker_throughput_ratio,
        ),
        (
            "worker_4_to_1",
            Scenario::Worker4,
            profile.comparisons.min_four_to_one_worker_throughput_ratio,
        ),
        (
            "worker_8_to_1",
            Scenario::Worker8,
            profile.comparisons.min_eight_to_one_worker_throughput_ratio,
        ),
    ] {
        let value = ratio(throughput[&scenario], throughput[&Scenario::Worker1], name)?;
        observed_ratios.insert(name.to_owned(), value);
        if value < threshold {
            bail!("{name} ratio {value} below frozen threshold {threshold}");
        }
    }
    Ok(ComparisonPreflight {
        schema_version: 1,
        status: "structural_preflight_only_not_capacity_qualified".to_owned(),
        sample_count: measurements.len(),
        repetition_count: profile.load.repetitions,
        observed_ratios,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE: &[u8] = include_bytes!("../../../benchmarks/r1-local/profile-v072.toml");
    fn digest_byte(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn fixture() -> Vec<ComparisonMeasurement> {
        Scenario::ALL
            .into_iter()
            .flat_map(|scenario| {
                (0..5).map(move |index| {
                    let (workers, view_count, live_groups, sharing) = scenario.shape();
                    let committed_changes = match scenario {
                        Scenario::Worker1 => 1_000,
                        Scenario::Worker2 => 1_500,
                        Scenario::Worker4 => 2_100,
                        Scenario::Worker8 => 2_200,
                        _ => 1_000,
                    };
                    let (state_bytes, state_writes, process_cpu_ns) = match scenario {
                        Scenario::Views20Private => (10_000, 1000, 1000),
                        Scenario::Views20Shared => (1_000, 100, 500),
                        _ => (1000, 100, 500),
                    };
                    ComparisonMeasurement {
                        run_id: format!("{}-r{}", scenario.id(), index),
                        scenario,
                        profile_sha256: FROZEN_PROFILE_SHA256.to_owned(),
                        binary_sha256: digest_byte('a'),
                        machine_chip: "Apple M5 Pro".to_string(),
                        backend: "native_lfs".to_string(),
                        offered_rows_per_sec: 2000,
                        payload_bytes: 128,
                        durability_mode: "Durable".to_string(),
                        freshness_slo_ms: 100,
                        workers: (1..=workers).collect(),
                        view_count,
                        sharing: sharing.unwrap(),
                        live_groups,
                        expected_complete_result_sha256: digest_byte('b'),
                        observed_complete_result_sha256: digest_byte('b'),
                        process_capture_sha256: digest_byte('c'),
                        committed_changes,
                        measurement_duration_ns: 60_000_000_000,
                        state_bytes,
                        state_writes,
                        process_cpu_ns,
                        lookup_p99_ms: matches!(
                            scenario,
                            Scenario::OneKey1k | Scenario::OneKey100k | Scenario::OneKey10m
                        )
                        .then_some(1.0),
                        read_p99_ms: (scenario == Scenario::MixedRead).then_some(2.0),
                        durable_commit_p99_ms: (scenario == Scenario::MixedRead).then_some(4.0),
                        freshness_p99_ms: (scenario == Scenario::MixedRead).then_some(20.0),
                        generator_delay_p99_ms: (scenario == Scenario::MixedRead).then_some(1.0),
                    }
                })
            })
            .collect()
    }

    #[test]
    fn preflight_accepts_complete_consistent_comparisons_without_qualifying() {
        let result = verify_comparison_preflight(PROFILE, &fixture()).unwrap();
        assert_eq!(result.schema_version, 1);
        assert_eq!(result.sample_count, 55);
        assert_eq!(result.repetition_count, 5);
        assert_eq!(
            result.status,
            "structural_preflight_only_not_capacity_qualified"
        );
        assert_eq!(result.observed_ratios.len(), 7);
        assert_eq!(result.observed_ratios["worker_2_to_1"], 1.5);
        assert!((result.observed_ratios["worker_4_to_1"] - 2.1).abs() < 1e-12);
        assert!((result.observed_ratios["worker_8_to_1"] - 2.2).abs() < 1e-12);
    }

    #[test]
    fn rejects_modified_profile_and_missing_cells() {
        assert!(verify_comparison_preflight(b"state = 'frozen'", &fixture())
            .unwrap_err()
            .to_string()
            .contains("digest mismatch"));
        let mut runs = fixture();
        runs.pop();
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("expected 55"));
    }

    #[test]
    fn rejects_wrong_worker_topology_or_changed_load() {
        let mut runs = fixture();
        let w = runs
            .iter_mut()
            .find(|m| m.scenario == Scenario::Worker8)
            .unwrap();
        w.workers = vec![1, 2, 3, 4, 5, 6, 7, 7];
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("topology"));
        let mut runs = fixture();
        runs[10].offered_rows_per_sec = 4000;
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("changed candidate or offered load"));
    }

    #[test]
    fn rejects_false_oracle_and_duplicate_run_ids() {
        let mut runs = fixture();
        runs[0].observed_complete_result_sha256 = digest_byte('e');
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("oracle digests"));
        let mut runs = fixture();
        runs[10].run_id = runs[0].run_id.clone();
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
    }

    #[test]
    fn rejects_worse_write_or_sharing_ratios() {
        let mut runs = fixture();
        for m in &mut runs {
            if m.scenario == Scenario::OneKey10m {
                m.state_writes = 200;
            }
        }
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("one-key write ratio"));
        let mut runs = fixture();
        for m in &mut runs {
            if m.scenario == Scenario::Views20Shared {
                m.state_bytes = 9000;
            }
        }
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("shared_20_state"));
    }

    #[test]
    fn rejects_weak_worker_scaling_and_noisy_repetitions() {
        let mut runs = fixture();
        for m in &mut runs {
            if m.scenario == Scenario::Worker2 {
                m.committed_changes = 1100;
            }
        }
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("worker_2_to_1"));
        let mut runs = fixture();
        let run = runs
            .iter_mut()
            .find(|m| m.scenario == Scenario::Worker1)
            .unwrap();
        run.committed_changes = 100_000;
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("noise budget"));
    }

    #[test]
    fn rejects_missing_mixed_read_timings_or_slo_breaches() {
        let mut runs = fixture();
        let run = runs
            .iter_mut()
            .find(|m| m.scenario == Scenario::MixedRead)
            .unwrap();
        run.read_p99_ms = None;
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("missing read_p99_ms"));
        let mut runs = fixture();
        let run = runs
            .iter_mut()
            .find(|m| m.scenario == Scenario::MixedRead)
            .unwrap();
        run.freshness_p99_ms = Some(101.0);
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("freshness_p99_ms exceeds"));
    }

    #[test]
    fn rejects_nonfinite_optional_metrics_and_unmeasured_lookup() {
        let mut runs = fixture();
        runs[0].lookup_p99_ms = Some(f64::NAN);
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("invalid lookup_p99_ms"));
        let mut runs = fixture();
        runs[0].lookup_p99_ms = None;
        assert!(verify_comparison_preflight(PROFILE, &runs)
            .unwrap_err()
            .to_string()
            .contains("one-key lookup"));
    }
}

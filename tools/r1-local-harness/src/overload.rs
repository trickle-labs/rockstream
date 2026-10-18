//! Fail-closed *structural* overload and beyond-RAM evidence preflight (#140).
//!
//! The independent oracle and process evidence are externally supplied.
//! Passing this check never qualifies release-process capacity or recovery.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const FROZEN_PROFILE_SHA256: &str =
    "d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a";

#[derive(Debug, Deserialize)]
struct Profile {
    profile_id: String,
    revision: u32,
    state: String,
    machine: Machine,
    workloads: Workloads,
}

#[derive(Debug, Deserialize)]
struct Machine {
    chip: String,
    backend: String,
    memory_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct Workloads {
    worker_hard_bytes: u64,
    max_rss_over_worker_budget_percent: u64,
    max_storage_backlog_bytes: u64,
    max_disk_bytes_per_worker: u64,
    max_spill_and_compaction_io_bytes_per_change: u64,
    max_waiters: u64,
    max_queue_age_ms: u64,
    overload_seconds: u64,
    recovery_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    pub profile_sha256: String,
    pub release_binary_sha256: String,
    pub process_capture_sha256: String,
    pub independent_input_oracle_sha256: String,
    pub machine_chip: String,
    pub backend: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fault {
    ShardChurn,
    SlowObjectStorage,
    WorkerLoss,
    Migration,
    Compaction,
    Backfill,
    ConnectorPressure,
    QueryLoad,
}

impl Fault {
    pub const ALL: [Self; 8] = [
        Self::ShardChurn,
        Self::SlowObjectStorage,
        Self::WorkerLoss,
        Self::Migration,
        Self::Compaction,
        Self::Backfill,
        Self::ConnectorPressure,
        Self::QueryLoad,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultEvent {
    pub kind: Fault,
    pub injected_elapsed_ms: u64,
    pub observed_elapsed_ms: u64,
    pub acknowledged: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResource {
    /// Process PID actually observed by the capture tool (not invented run numbers).
    pub pid: u32,
    pub process_started_unix_ms: u64,
    pub rss_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSample {
    pub elapsed_ms: u64,
    pub workers: Vec<WorkerResource>,
    pub storage_backlog_bytes: u64,
    pub outstanding_waiters: u64,
    pub max_queue_age_ms: u64,
    pub cumulative_committed_changes: u64,
    pub cumulative_spill_and_compaction_io_bytes: u64,
    pub cumulative_control_heartbeats: u64,
    pub cumulative_oom_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryObservation {
    pub restart_requested_elapsed_ms: u64,
    pub recovery_finished_elapsed_ms: u64,
    pub paged_recovery_completed: bool,
    pub state_bytes_before_restart: u64,
    pub recovered_epoch: u64,
    pub last_committed_epoch_before_restart: u64,
    pub recovered_complete_result_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LargeStateRecord {
    pub run_id: String,
    pub identity: SourceIdentity,
    /// Must exceed the machine RAM; this is actual storage state, not a fabricated logical key count.
    pub state_bytes: u64,
    pub exercised_eviction: bool,
    pub exercised_compaction: bool,
    pub exercised_migration: bool,
    pub recovery: RecoveryObservation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OverloadRecord {
    pub run_id: String,
    pub identity: SourceIdentity,
    pub declared_sample_interval_ms: u64,
    pub offered_rows_per_sec: u64,
    pub fault_events: Vec<FaultEvent>,
    pub samples: Vec<ResourceSample>,
    pub recovery: RecoveryObservation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultEvidenceBundle {
    pub schema_version: u32,
    pub large_state: LargeStateRecord,
    pub overload: OverloadRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultPreflight {
    pub schema_version: u32,
    pub qualification_status: String,
    pub overload_sample_count: usize,
    pub overload_duration_ms: u64,
    pub largest_state_bytes: u64,
    pub observed_fault_count: usize,
}

fn sha256_shape(v: &str) -> bool {
    v.len() == 64
        && v.bytes()
            .all(|x| x.is_ascii_digit() || (b'a'..=b'f').contains(&x))
}

fn validate_identity(identity: &SourceIdentity, profile: &Profile) -> Result<()> {
    if identity.profile_sha256 != FROZEN_PROFILE_SHA256
        || identity.machine_chip != profile.machine.chip
        || identity.backend != profile.machine.backend
        || ![
            &identity.release_binary_sha256,
            &identity.process_capture_sha256,
            &identity.independent_input_oracle_sha256,
        ]
        .into_iter()
        .all(|value| sha256_shape(value))
    {
        bail!("invalid or incomparable release-profile/process/oracle provenance");
    }
    Ok(())
}

fn validate_recovery(
    recovery: &RecoveryObservation,
    identity: &SourceIdentity,
    profile: &Profile,
) -> Result<()> {
    let elapsed = recovery
        .recovery_finished_elapsed_ms
        .checked_sub(recovery.restart_requested_elapsed_ms)
        .context("recovery finished before restart")?;
    if elapsed == 0
        || elapsed > profile.workloads.recovery_seconds.saturating_mul(1_000)
        || !recovery.paged_recovery_completed
        || recovery.state_bytes_before_restart == 0
        || recovery.recovered_epoch < recovery.last_committed_epoch_before_restart
        || recovery.recovered_complete_result_sha256 != identity.independent_input_oracle_sha256
    {
        bail!("unqualified recovery duration, incomplete pagination, epoch loss, or wrong full oracle");
    }
    Ok(())
}

fn check_sample(sample: &ResourceSample, profile: &Profile) -> Result<()> {
    if sample.workers.is_empty() {
        bail!("no active worker process identities");
    }
    let mut pids = BTreeSet::new();
    let max_rss = (u128::from(profile.workloads.worker_hard_bytes)
        * (100 + u128::from(profile.workloads.max_rss_over_worker_budget_percent)))
        / 100;
    for worker in &sample.workers {
        if worker.pid == 0
            || worker.process_started_unix_ms == 0
            || !pids.insert(worker.pid)
            || u128::from(worker.rss_bytes) > max_rss
            || worker.disk_bytes > profile.workloads.max_disk_bytes_per_worker
        {
            bail!("invalid worker identity or over-RSS/disk resource bound");
        }
    }
    if sample.storage_backlog_bytes > profile.workloads.max_storage_backlog_bytes
        || sample.outstanding_waiters > profile.workloads.max_waiters
        || sample.max_queue_age_ms > profile.workloads.max_queue_age_ms
        || sample.cumulative_oom_events > 0
    {
        bail!("resource queue/backlog/waiter/OOM envelope breached");
    }
    Ok(())
}

/// Validate only frozen thresholds, field completeness, internal consistency,
/// and full-result digest declarations. This cannot independently attest that
/// the supplied process captures or oracle digests represent actual runs.
pub fn verify_fault_preflight(
    frozen_profile: &[u8],
    bundle: &FaultEvidenceBundle,
) -> Result<FaultPreflight> {
    if crate::artifact::sha256(frozen_profile) != FROZEN_PROFILE_SHA256 {
        bail!("frozen v0.72 profile digest mismatch");
    }
    let profile: Profile =
        toml::from_str(std::str::from_utf8(frozen_profile)?).context("parse frozen profile")?;
    if profile.profile_id != "v072-m5pro-48gb-v1"
        || profile.revision != 1
        || profile.state != "frozen"
        || bundle.schema_version != 1
        || bundle.large_state.run_id.trim().is_empty()
        || bundle.overload.run_id.trim().is_empty()
        || bundle.large_state.run_id == bundle.overload.run_id
    {
        bail!("invalid profile revision, evidence schema, or distinct run IDs");
    }
    validate_identity(&bundle.large_state.identity, &profile)?;
    validate_identity(&bundle.overload.identity, &profile)?;
    if bundle.large_state.identity.release_binary_sha256
        != bundle.overload.identity.release_binary_sha256
    {
        bail!("candidate release binary differs between runs");
    }
    if bundle.large_state.state_bytes <= profile.machine.memory_bytes
        || !bundle.large_state.exercised_eviction
        || !bundle.large_state.exercised_compaction
        || !bundle.large_state.exercised_migration
    {
        bail!("missing beyond-RAM state/eviction/compaction/migration coverage");
    }
    validate_recovery(
        &bundle.large_state.recovery,
        &bundle.large_state.identity,
        &profile,
    )?;
    if bundle.large_state.recovery.state_bytes_before_restart < bundle.large_state.state_bytes {
        bail!("large-state recovery bytes are below the measured beyond-RAM state");
    }
    let record = &bundle.overload;
    validate_recovery(&record.recovery, &record.identity, &profile)?;
    // A tooling density check, not an additional frozen product SLO.
    if !(1_000..=60_000).contains(&record.declared_sample_interval_ms)
        || record.offered_rows_per_sec == 0
        || record.samples.len() < 3
    {
        bail!("missing dense observed overload series or scheduled offered load");
    }
    let first = &record.samples[0];
    let last = record.samples.last().context("empty overload samples")?;
    let soak_ms = profile.workloads.overload_seconds.saturating_mul(1_000);
    if first.elapsed_ms > record.declared_sample_interval_ms
        || last.elapsed_ms < soak_ms
        || record.recovery.restart_requested_elapsed_ms < soak_ms
    {
        bail!("observed overload series does not cover frozen soak duration");
    }
    let mut prev: Option<&ResourceSample> = None;
    for sample in &record.samples {
        check_sample(sample, &profile)?;
        if let Some(before) = prev {
            let delta = sample
                .elapsed_ms
                .checked_sub(before.elapsed_ms)
                .context("non-monotonic time series")?;
            if delta == 0
                || delta >= record.declared_sample_interval_ms.saturating_mul(2)
                || sample.cumulative_committed_changes < before.cumulative_committed_changes
                || sample.cumulative_spill_and_compaction_io_bytes
                    < before.cumulative_spill_and_compaction_io_bytes
                || sample.cumulative_control_heartbeats < before.cumulative_control_heartbeats
                || sample.cumulative_oom_events < before.cumulative_oom_events
            {
                bail!("missing samples, decreasing raw counters, or invalid timeline");
            }
        }
        prev = Some(sample);
    }
    if last.cumulative_committed_changes <= first.cumulative_committed_changes
        || last.cumulative_control_heartbeats <= first.cumulative_control_heartbeats
    {
        bail!("no committed progress or control-plane liveness under overload");
    }
    let io_delta = last
        .cumulative_spill_and_compaction_io_bytes
        .checked_sub(first.cumulative_spill_and_compaction_io_bytes)
        .context("spill+compaction IO counter decreased")?;
    let committed_delta = last.cumulative_committed_changes - first.cumulative_committed_changes;
    if u128::from(io_delta)
        > u128::from(committed_delta)
            * u128::from(
                profile
                    .workloads
                    .max_spill_and_compaction_io_bytes_per_change,
            )
    {
        bail!("spill/compaction IO per committed change exceeds frozen bound");
    }
    if record.fault_events.len() < Fault::ALL.len() {
        bail!("missing required overload faults");
    }
    let kinds: BTreeSet<_> = record.fault_events.iter().map(|event| event.kind).collect();
    if kinds.len() != Fault::ALL.len() {
        bail!("missing required distinct overload fault kinds");
    }
    for event in &record.fault_events {
        if !event.acknowledged
            || event.injected_elapsed_ms >= soak_ms
            || event.observed_elapsed_ms < event.injected_elapsed_ms
            || event.observed_elapsed_ms > soak_ms
        {
            bail!("unobserved or outside-soak fault injection");
        }
    }
    if record.recovery.state_bytes_before_restart == 0 {
        bail!("restart missing persisted state");
    }
    Ok(FaultPreflight {
        schema_version: 1,
        qualification_status: "structural_preflight_only_not_capacity_qualified".to_string(),
        overload_sample_count: record.samples.len(),
        overload_duration_ms: last.elapsed_ms.saturating_sub(first.elapsed_ms),
        largest_state_bytes: bundle.large_state.state_bytes,
        observed_fault_count: kinds.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE: &[u8] = include_bytes!("../../../benchmarks/r1-local/profile-v072.toml");
    fn digest(x: char) -> String {
        x.to_string().repeat(64)
    }
    fn identity() -> SourceIdentity {
        SourceIdentity {
            profile_sha256: FROZEN_PROFILE_SHA256.to_owned(),
            release_binary_sha256: digest('a'),
            process_capture_sha256: digest('b'),
            independent_input_oracle_sha256: digest('c'),
            machine_chip: "Apple M5 Pro".to_owned(),
            backend: "native_lfs".to_owned(),
        }
    }
    fn recovery(start: u64, state: u64) -> RecoveryObservation {
        RecoveryObservation {
            restart_requested_elapsed_ms: start,
            recovery_finished_elapsed_ms: start + 30_000,
            paged_recovery_completed: true,
            state_bytes_before_restart: state,
            recovered_epoch: 20,
            last_committed_epoch_before_restart: 20,
            recovered_complete_result_sha256: digest('c'),
        }
    }
    fn fixture() -> FaultEvidenceBundle {
        let interval_ms = 30_000;
        let samples = (0..=60)
            .map(|n| ResourceSample {
                elapsed_ms: n * interval_ms,
                workers: vec![WorkerResource {
                    pid: 111,
                    process_started_unix_ms: 1_234_567,
                    rss_bytes: 1_000_000,
                    disk_bytes: 1_000_000,
                }],
                storage_backlog_bytes: 10_000,
                outstanding_waiters: 2,
                max_queue_age_ms: 5,
                cumulative_committed_changes: n * 1000,
                cumulative_spill_and_compaction_io_bytes: n * 1000,
                cumulative_control_heartbeats: n,
                cumulative_oom_events: 0,
            })
            .collect();
        FaultEvidenceBundle {
            schema_version: 1,
            large_state: LargeStateRecord {
                run_id: "beyond-ram".to_owned(),
                identity: identity(),
                state_bytes: 52_000_000_000,
                exercised_eviction: true,
                exercised_compaction: true,
                exercised_migration: true,
                recovery: recovery(1_000, 52_000_000_000),
            },
            overload: OverloadRecord {
                run_id: "overload".to_owned(),
                identity: identity(),
                declared_sample_interval_ms: interval_ms,
                offered_rows_per_sec: 1000,
                fault_events: Fault::ALL
                    .into_iter()
                    .enumerate()
                    .map(|(i, kind)| FaultEvent {
                        kind,
                        injected_elapsed_ms: (i as u64) * 100_000,
                        observed_elapsed_ms: (i as u64) * 100_000 + 1,
                        acknowledged: true,
                    })
                    .collect(),
                samples,
                recovery: recovery(1_800_010, 1_000_000),
            },
        }
    }

    #[test]
    fn structural_bundle_is_explicitly_not_release_qualified() {
        let result = verify_fault_preflight(PROFILE, &fixture()).unwrap();
        assert_eq!(result.schema_version, 1);
        assert_eq!(result.overload_sample_count, 61);
        assert_eq!(result.observed_fault_count, 8);
        assert_eq!(result.overload_duration_ms, 1_800_000);
        assert_eq!(result.largest_state_bytes, 52_000_000_000);
        assert_eq!(
            result.qualification_status,
            "structural_preflight_only_not_capacity_qualified"
        );
    }

    #[test]
    fn mutated_profile_and_short_or_sparse_soak_are_rejected() {
        assert!(verify_fault_preflight(b"invalid", &fixture())
            .unwrap_err()
            .to_string()
            .contains("digest mismatch"));
        let mut b = fixture();
        b.overload.samples.pop();
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("soak duration"));
        let mut b = fixture();
        b.overload.samples.remove(3);
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("missing samples"));
    }

    #[test]
    fn over_rss_disk_backlog_waiters_age_and_oom_are_rejected() {
        type Mutation = (&'static str, Box<dyn Fn(&mut ResourceSample)>);
        let mutate: Vec<Mutation> = vec![
            ("RSS", Box::new(|x| x.workers[0].rss_bytes = 3_000_000_000)),
            (
                "disk",
                Box::new(|x| x.workers[0].disk_bytes = 35_000_000_000),
            ),
            (
                "backlog",
                Box::new(|x| x.storage_backlog_bytes = 5_000_000_000),
            ),
            ("waiters", Box::new(|x| x.outstanding_waiters = 1025)),
            ("queue", Box::new(|x| x.max_queue_age_ms = 1001)),
            ("OOM", Box::new(|x| x.cumulative_oom_events = 1)),
        ];
        for (label, change) in mutate {
            let mut b = fixture();
            change(&mut b.overload.samples[10]);
            assert!(
                verify_fault_preflight(PROFILE, &b).is_err(),
                "{label} unexpectedly accepted"
            );
        }
    }

    #[test]
    fn missing_faults_wrong_oracle_and_recovery_are_rejected() {
        let mut b = fixture();
        b.overload.fault_events.pop();
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("required overload faults"));
        let mut b = fixture();
        b.overload.recovery.recovered_complete_result_sha256 = digest('d');
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("full oracle"));
        let mut b = fixture();
        b.large_state.recovery.recovery_finished_elapsed_ms += 901_000;
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("recovery duration"));
    }

    #[test]
    fn beyond_ram_and_growth_counters_are_checked() {
        let mut b = fixture();
        b.large_state.state_bytes = 1000;
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("beyond-RAM"));
        let mut b = fixture();
        b.overload.samples[20].cumulative_committed_changes = 1;
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("decreasing raw counters"));
        let mut b = fixture();
        b.overload.samples[20].cumulative_spill_and_compaction_io_bytes = u64::MAX;
        assert!(verify_fault_preflight(PROFILE, &b).is_err());
    }

    #[test]
    fn independent_scenario_oracles_may_have_distinct_correct_results() {
        let mut bundle = fixture();
        bundle.overload.identity.independent_input_oracle_sha256 = digest('d');
        bundle.overload.recovery.recovered_complete_result_sha256 = digest('d');
        assert!(verify_fault_preflight(PROFILE, &bundle).is_ok());
        bundle.overload.identity.release_binary_sha256 = digest('e');
        assert!(verify_fault_preflight(PROFILE, &bundle)
            .unwrap_err()
            .to_string()
            .contains("candidate release binary differs"));
    }

    #[test]
    fn worker_identity_and_io_per_change_are_bounded() {
        let mut b = fixture();
        b.overload.samples[0].workers[0].pid = 0;
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("worker identity"));
        let mut b = fixture();
        for (i, item) in b.overload.samples.iter_mut().enumerate() {
            item.cumulative_spill_and_compaction_io_bytes = (i as u64) * 20_000_000;
        }
        assert!(verify_fault_preflight(PROFILE, &b)
            .unwrap_err()
            .to_string()
            .contains("IO per committed change"));
    }
}

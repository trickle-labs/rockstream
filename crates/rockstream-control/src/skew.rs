use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rockstream_plan::OpKind;
use rockstream_storage::{ShardDb, ShardPrefix, WriteBatch};
use rockstream_types::config::{SkewSplitConfig, TunerOverrides};
use rockstream_types::error_code::{ErrorCode, RS_5036};
use rockstream_types::ids::OperatorId;
use rockstream_types::ids::ShardId;
use rockstream_types::merge_law::LawDescriptor;
use rockstream_types::topology::{ClusterWorkerPressure, KeyLoadSample, ShardLoadSample};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::audit::FileAuditLog;
use crate::migration::{
    BucketMapVersionTracker, MigrationCoordinator, MigrationPersistentStore, MigrationShard,
    PhaseClocks,
};
use crate::CheckpointCoordinator;
use rockstream_plan::virtual_bucket::normalize_power_of_two_bucket_count;

pub const MAX_TRACKED_KEY_LOADS: usize = 1024;
pub const MAX_PROACTIVE_SPLIT_SAMPLE_KEYS: usize = 1024;
pub const MAX_PROACTIVE_SPLIT_SAMPLE_BYTES: usize = 4 * 1024 * 1024;
pub const PROACTIVE_SPLIT_THROTTLE: Duration = Duration::from_secs(60);
pub const SKEW_SPLIT_TRIGGER_WINDOW: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HotKeyMitigationPlan {
    Split {
        bucket_count: u16,
        source: OperatorId,
        split: OpKind,
        combine: OpKind,
    },
    Spill {
        shard_id: ShardId,
        code: ErrorCode,
        next_steps: &'static str,
    },
}

/// Configuration for proactive shard splitting (v0.47).
///
/// The stale v0.38 scaffold named these thresholds "fractions". In v0.47 we
/// keep the field names for compatibility, but interpret them as multipliers so
/// the default split point can match the plan's `1.5 × target_shard_state_bytes`
/// requirement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProactiveSplitConfig {
    pub target_shard_state_bytes: u64,
    pub min_shard_state_bytes: u64,
    pub split_trigger_fraction: f64,
    pub alert_threshold_fraction: f64,
}

impl Default for ProactiveSplitConfig {
    fn default() -> Self {
        Self {
            target_shard_state_bytes: 32 * 1024 * 1024 * 1024,
            min_shard_state_bytes: 4 * 1024 * 1024 * 1024,
            split_trigger_fraction: 1.5,
            alert_threshold_fraction: 1.75,
        }
    }
}

impl ProactiveSplitConfig {
    pub fn split_trigger_bytes(&self) -> u64 {
        (self.target_shard_state_bytes as f64 * self.split_trigger_fraction) as u64
    }

    pub fn alert_threshold_bytes(&self) -> u64 {
        (self.target_shard_state_bytes as f64 * self.alert_threshold_fraction) as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardFootprintReport {
    pub shard_id: ShardId,
    pub op_state_bytes: u64,
    pub view_output_bytes: u64,
    pub state_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProactiveSplitOutcome {
    pub migration_id: String,
    pub recipient_shard_id: ShardId,
    pub moved_keys: usize,
    pub midpoint_key: Vec<u8>,
    pub fill_level: SkewFillLevel,
    pub footprint: ShardFootprintReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProactiveMergeOutcome {
    pub migration_id: String,
    pub recipient_shard_id: ShardId,
    pub moved_keys: usize,
    pub fill_level: SkewFillLevel,
    pub donor_footprint: ShardFootprintReport,
    pub recipient_footprint: ShardFootprintReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkewFillLevel {
    pub used: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HotKeyReport {
    pub shard_id: ShardId,
    pub key_prefix: Vec<u8>,
    pub cpu_nanos: u64,
    pub bytes_per_epoch: u64,
    pub state_writes_per_epoch: u64,
    pub median_score: u64,
    pub key_score: u64,
    pub hotness_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineShardPressureSample {
    pub pipeline_id: String,
    pub demanded_shard_count: u32,
    pub placed_shard_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SkewSplitDecision {
    pub operator_id: OperatorId,
    pub shard_id: ShardId,
    pub bucket_count: u16,
    pub load_factor: f64,
    pub hot_key: HotKeyReport,
    pub plan: HotKeyMitigationPlan,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HotKeyDetectorError {
    #[error(
        "tracked hot-key sample capacity exceeded ({used}/{max}); next_steps: reduce per-epoch key fan-out or increase MAX_TRACKED_KEY_LOADS once memory headroom is confirmed"
    )]
    CapacityExceeded { used: usize, max: usize },
}

#[derive(Debug, Error)]
pub enum ProactiveSplitError {
    #[error(
        "RS-5031: proactive split sample window full ({used}/{max}); next_steps: reduce shard key cardinality per split or increase MAX_PROACTIVE_SPLIT_SAMPLE_KEYS once memory headroom is confirmed"
    )]
    SampleWindowFull { used: usize, max: usize },
    #[error("storage error: {0}")]
    Storage(String),
    #[error("migration error: {0}")]
    Migration(String),
}

#[derive(Debug, Clone, Copy)]
pub struct HotKeyDetector {
    hot_key_factor: f64,
    last_fill: SkewFillLevel,
}

#[derive(Debug, Clone)]
pub struct AdaptiveSkewSplitter {
    config: SkewSplitConfig,
    overload_started_at_ms: BTreeMap<OperatorId, u64>,
}

#[derive(Debug, Clone)]
pub struct ProactiveSplitter {
    config: ProactiveSplitConfig,
    last_action_at_ms: BTreeMap<ShardId, u64>,
    last_fill: SkewFillLevel,
    next_shard_id: u64,
}

impl ProactiveSplitter {
    pub fn new(config: ProactiveSplitConfig) -> Self {
        Self {
            config,
            last_action_at_ms: BTreeMap::new(),
            last_fill: SkewFillLevel {
                used: 0,
                capacity: MAX_PROACTIVE_SPLIT_SAMPLE_KEYS,
            },
            next_shard_id: 10_000,
        }
    }

    pub fn fill_level(&self) -> SkewFillLevel {
        self.last_fill
    }

    pub async fn shard_footprint(
        &mut self,
        shard_id: ShardId,
        db: &ShardDb,
    ) -> Result<ShardFootprintReport, ProactiveSplitError> {
        let (op_entries, _) = db
            .scan_prefix_bounded(
                &[ShardPrefix::OpState.as_byte()],
                MAX_PROACTIVE_SPLIT_SAMPLE_BYTES,
            )
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
        let (view_entries, _) = db
            .scan_prefix_bounded(
                &[ShardPrefix::ViewOutput.as_byte()],
                MAX_PROACTIVE_SPLIT_SAMPLE_BYTES,
            )
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
        let op_state_bytes = op_entries
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();
        let view_output_bytes = view_entries
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();
        Ok(ShardFootprintReport {
            shard_id,
            op_state_bytes,
            view_output_bytes,
            state_bytes: op_state_bytes + view_output_bytes,
        })
    }

    pub async fn maybe_split(
        &mut self,
        donor: &MigrationShard,
        recipient: &MigrationShard,
        checkpoint_coordinator: &CheckpointCoordinator,
        migration_store: Option<&MigrationPersistentStore>,
        audit: Option<&FileAuditLog>,
        now_ms: u64,
    ) -> Result<Option<ProactiveSplitOutcome>, ProactiveSplitError> {
        let footprint = self.shard_footprint(donor.shard_id, &donor.db).await?;
        if footprint.state_bytes < self.config.split_trigger_bytes() {
            return Ok(None);
        }
        if self.shard_is_throttled(now_ms, &[donor.shard_id, recipient.shard_id]) {
            return Ok(None);
        }

        let sampled = sample_split_keys(&donor.db).await?;
        self.last_fill = SkewFillLevel {
            used: sampled.len(),
            capacity: MAX_PROACTIVE_SPLIT_SAMPLE_KEYS,
        };
        if sampled.len() < 2 {
            return Ok(None);
        }
        let midpoint_index = sampled.len() / 2;
        let midpoint_key = sampled[midpoint_index].clone();
        let selected_keys = sampled[midpoint_index..].to_vec();
        let migration_id = format!("proactive-split-{}-{now_ms}", donor.shard_id.0);

        if let Some(audit) = audit {
            let event = rockstream_types::audit::AuditEvent::now(
                "control",
                "shard.proactive_split",
                donor.shard_id.to_string(),
            )
            .with_detail(format!(
                "recipient={}, trigger_bytes={}, state_bytes={}",
                recipient.shard_id,
                self.config.split_trigger_bytes(),
                footprint.state_bytes
            ));
            let _ = audit.append(&event);
        }

        let mut record = rockstream_types::migration::MigrationRecord::new(
            migration_id.clone(),
            vec![donor.shard_id],
            recipient.shard_id,
            rockstream_types::migration::BucketSet::new([donor.shard_id.0]),
            donor.frontier,
            1,
        );
        if let Some(store) = migration_store {
            store
                .save(&record)
                .await
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        }

        let coordinator = MigrationCoordinator::new();
        coordinator
            .drive_planned_to_copying(
                &mut record,
                std::slice::from_ref(donor),
                recipient,
                checkpoint_coordinator,
                PhaseClocks {
                    snapshotting_started_at: Instant::now(),
                    copying_started_at: Instant::now(),
                },
                audit,
            )
            .await
            .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        coordinator
            .begin_dual_writing(&mut record, audit)
            .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        coordinator
            .advance_to_catching_up(&mut record, audit)
            .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        coordinator
            .advance_to_fencing_old_if_caught_up(
                &mut record,
                donor.frontier,
                recipient.frontier,
                audit,
            )
            .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        let tracker = BucketMapVersionTracker::new();
        for component in ["reader", "exchange", "gateway"] {
            tracker
                .observe(component, record.target_bucket_map_version)
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        }
        coordinator
            .await_cutover_readiness_at_frontier(
                &mut record,
                &tracker,
                &["reader", "exchange", "gateway"],
                donor.frontier.min(recipient.frontier),
                Instant::now(),
                audit,
            )
            .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;

        if let Some(store) = migration_store {
            store
                .transition(
                    &mut record,
                    rockstream_types::migration::MigrationState::Verifying,
                    audit,
                )
                .await
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        } else {
            record
                .apply_transition(rockstream_types::migration::MigrationState::Verifying)
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        }
        verify_selected_keys_match(donor, recipient, &selected_keys).await?;
        prune_keys_not_in_selection(&recipient.db, &selected_keys).await?;

        if let Some(store) = migration_store {
            store
                .transition(
                    &mut record,
                    rockstream_types::migration::MigrationState::GcEligible,
                    audit,
                )
                .await
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
            store
                .transition(
                    &mut record,
                    rockstream_types::migration::MigrationState::Done,
                    audit,
                )
                .await
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        } else {
            record
                .apply_transition(rockstream_types::migration::MigrationState::GcEligible)
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
            record
                .apply_transition(rockstream_types::migration::MigrationState::Done)
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        }
        prune_selected_keys(&donor.db, &selected_keys).await?;
        if let Some(store) = migration_store {
            store
                .archive(&record, audit)
                .await
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        }

        self.mark_action(now_ms, &[donor.shard_id, recipient.shard_id]);
        self.next_shard_id = self.next_shard_id.max(recipient.shard_id.0 + 1);

        Ok(Some(ProactiveSplitOutcome {
            migration_id,
            recipient_shard_id: recipient.shard_id,
            moved_keys: selected_keys.len(),
            midpoint_key,
            fill_level: self.last_fill,
            footprint,
        }))
    }

    pub async fn maybe_merge(
        &mut self,
        donor: &MigrationShard,
        recipient: &MigrationShard,
        _checkpoint_coordinator: &CheckpointCoordinator,
        migration_store: Option<&MigrationPersistentStore>,
        audit: Option<&FileAuditLog>,
        now_ms: u64,
    ) -> Result<Option<ProactiveMergeOutcome>, ProactiveSplitError> {
        let donor_footprint = self.shard_footprint(donor.shard_id, &donor.db).await?;
        let recipient_footprint = self
            .shard_footprint(recipient.shard_id, &recipient.db)
            .await?;
        if donor_footprint.state_bytes >= self.config.min_shard_state_bytes
            || recipient_footprint.state_bytes >= self.config.min_shard_state_bytes
        {
            return Ok(None);
        }
        if self.shard_is_throttled(now_ms, &[donor.shard_id, recipient.shard_id]) {
            return Ok(None);
        }

        let selected_keys = sample_split_keys(&donor.db).await?;
        self.last_fill = SkewFillLevel {
            used: selected_keys.len(),
            capacity: MAX_PROACTIVE_SPLIT_SAMPLE_KEYS,
        };
        if selected_keys.is_empty() {
            return Ok(None);
        }

        let migration_id = format!("cold-merge-{}-{now_ms}", donor.shard_id.0);
        if let Some(audit) = audit {
            let event = rockstream_types::audit::AuditEvent::now(
                "control",
                "shard.cold_merge",
                donor.shard_id.to_string(),
            )
            .with_detail(format!(
                "recipient={}, donor_state_bytes={}, recipient_state_bytes={}, merge_floor_bytes={}",
                recipient.shard_id,
                donor_footprint.state_bytes,
                recipient_footprint.state_bytes,
                self.config.min_shard_state_bytes
            ));
            let _ = audit.append(&event);
        }

        let mut record = rockstream_types::migration::MigrationRecord::new(
            migration_id.clone(),
            vec![donor.shard_id],
            recipient.shard_id,
            rockstream_types::migration::BucketSet::new([donor.shard_id.0]),
            donor.frontier.max(recipient.frontier),
            1,
        );
        if let Some(store) = migration_store {
            store
                .save(&record)
                .await
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        }

        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::Snapshotting,
            audit,
        )
        .await?;
        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::Copying,
            audit,
        )
        .await?;
        copy_selected_keys(donor, recipient, &selected_keys).await?;
        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::DualWriting,
            audit,
        )
        .await?;
        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::CatchingUp,
            audit,
        )
        .await?;
        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::FencingOld,
            audit,
        )
        .await?;
        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::Cutover,
            audit,
        )
        .await?;
        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::Verifying,
            audit,
        )
        .await?;
        verify_selected_keys_match(donor, recipient, &selected_keys).await?;

        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::GcEligible,
            audit,
        )
        .await?;
        transition_merge_record(
            migration_store,
            &mut record,
            rockstream_types::migration::MigrationState::Done,
            audit,
        )
        .await?;
        prune_selected_keys(&donor.db, &selected_keys).await?;
        if let Some(store) = migration_store {
            store
                .archive(&record, audit)
                .await
                .map_err(|err| ProactiveSplitError::Migration(err.to_string()))?;
        }

        self.mark_action(now_ms, &[donor.shard_id, recipient.shard_id]);

        Ok(Some(ProactiveMergeOutcome {
            migration_id,
            recipient_shard_id: recipient.shard_id,
            moved_keys: selected_keys.len(),
            fill_level: self.last_fill,
            donor_footprint,
            recipient_footprint,
        }))
    }

    fn shard_is_throttled(&self, now_ms: u64, shard_ids: &[ShardId]) -> bool {
        shard_ids.iter().any(|shard_id| {
            self.last_action_at_ms
                .get(shard_id)
                .copied()
                .map(|last| {
                    now_ms.saturating_sub(last) < PROACTIVE_SPLIT_THROTTLE.as_millis() as u64
                })
                .unwrap_or(false)
        })
    }

    fn mark_action(&mut self, now_ms: u64, shard_ids: &[ShardId]) {
        for shard_id in shard_ids {
            self.last_action_at_ms.insert(*shard_id, now_ms);
        }
    }
}

impl HotKeyDetector {
    pub fn new(hot_key_factor: f64) -> Self {
        Self {
            hot_key_factor,
            last_fill: SkewFillLevel {
                used: 0,
                capacity: MAX_TRACKED_KEY_LOADS,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &mut self,
        sample: &ShardLoadSample,
    ) -> Result<Option<HotKeyReport>, HotKeyDetectorError> {
        if sample.key_loads.len() > MAX_TRACKED_KEY_LOADS {
            return Err(HotKeyDetectorError::CapacityExceeded {
                used: sample.key_loads.len(),
                max: MAX_TRACKED_KEY_LOADS,
            });
        }
        self.last_fill.used = sample.key_loads.len();
        detect_hot_key(sample, self.hot_key_factor)
    }

    pub fn fill_level(&self) -> SkewFillLevel {
        self.last_fill
    }
}

impl AdaptiveSkewSplitter {
    pub fn new(config: SkewSplitConfig) -> Self {
        Self {
            config,
            overload_started_at_ms: BTreeMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn observe(
        &mut self,
        operator_id: OperatorId,
        law: &LawDescriptor,
        shard_samples: &[ShardLoadSample],
        overrides: Option<&TunerOverrides>,
        spill_shard: ShardId,
        now_ms: u64,
        audit: Option<&FileAuditLog>,
    ) -> Result<Option<SkewSplitDecision>, HotKeyDetectorError> {
        if !self.config.enabled || shard_samples.is_empty() {
            self.overload_started_at_ms.remove(&operator_id);
            return Ok(None);
        }

        let mut scores: Vec<u64> = shard_samples.iter().map(shard_load_score).collect();
        scores.sort_unstable();
        let median = scores[(scores.len().saturating_sub(1)) / 2];
        if median == 0 {
            self.overload_started_at_ms.remove(&operator_id);
            return Ok(None);
        }

        let worst = shard_samples
            .iter()
            .max_by_key(|sample| shard_load_score(sample))
            .expect("shard_samples checked non-empty");
        let worst_score = shard_load_score(worst);
        let load_factor = worst_score as f64 / median as f64;
        if load_factor <= self.config.hot_key_factor {
            self.overload_started_at_ms.remove(&operator_id);
            return Ok(None);
        }

        let overloaded_since = self
            .overload_started_at_ms
            .entry(operator_id)
            .or_insert(now_ms);
        if now_ms.saturating_sub(*overloaded_since) < SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64 {
            return Ok(None);
        }

        let Some(hot_key) = detect_hot_key(worst, self.config.hot_key_factor)? else {
            return Ok(None);
        };
        let bucket_count = overrides
            .and_then(|override_cfg| override_cfg.skew_buckets)
            .unwrap_or(self.config.max_skew_buckets)
            .clamp(1, self.config.max_skew_buckets);
        let plan = plan_hot_key_mitigation(law, operator_id, bucket_count, spill_shard);
        if let Some(audit) = audit {
            let event = rockstream_types::audit::AuditEvent::now(
                "auto_tuner",
                "skew_splitting.adjusted",
                operator_id.to_string(),
            )
            .with_detail(format!(
                "shard_id={}, hot_key={}, load_factor={load_factor:.3}, worst_score={}, median_score={}, hot_key_factor={}, bucket_count={}, sustained_ms={}",
                worst.shard_id,
                hex_key(&hot_key.key_prefix),
                worst_score,
                median,
                self.config.hot_key_factor,
                bucket_count,
                now_ms.saturating_sub(*overloaded_since)
            ));
            let _ = audit.append(&event);
        }
        self.overload_started_at_ms.insert(operator_id, now_ms);
        Ok(Some(SkewSplitDecision {
            operator_id,
            shard_id: worst.shard_id,
            bucket_count,
            load_factor,
            hot_key,
            plan,
        }))
    }
}

pub fn detect_hot_key(
    sample: &ShardLoadSample,
    hot_key_factor: f64,
) -> Result<Option<HotKeyReport>, HotKeyDetectorError> {
    if sample.key_loads.len() > MAX_TRACKED_KEY_LOADS {
        return Err(HotKeyDetectorError::CapacityExceeded {
            used: sample.key_loads.len(),
            max: MAX_TRACKED_KEY_LOADS,
        });
    }
    if sample.key_loads.is_empty() {
        return Ok(None);
    }

    let mut sorted_scores: Vec<u64> = sample.key_loads.iter().map(load_score).collect();
    sorted_scores.sort_unstable();
    let median_score = sorted_scores[(sorted_scores.len().saturating_sub(1)) / 2];
    if median_score == 0 {
        return Ok(None);
    }

    let hottest = sample
        .key_loads
        .iter()
        .max_by_key(|load| load_score(load))
        .expect("key_loads checked non-empty");
    let key_score = load_score(hottest);
    let hotness_factor = key_score as f64 / median_score as f64;
    if hotness_factor <= hot_key_factor {
        return Ok(None);
    }

    Ok(Some(HotKeyReport {
        shard_id: sample.shard_id,
        key_prefix: hottest.key_prefix.clone(),
        cpu_nanos: hottest.cpu_nanos,
        bytes_per_epoch: hottest.bytes_per_epoch,
        state_writes_per_epoch: hottest.state_writes_per_epoch,
        median_score,
        key_score,
        hotness_factor,
    }))
}

fn load_score(load: &KeyLoadSample) -> u64 {
    load.cpu_nanos
        .saturating_add(load.bytes_per_epoch)
        .saturating_add(load.state_writes_per_epoch)
}

fn shard_load_score(load: &ShardLoadSample) -> u64 {
    load.cpu_nanos
        .saturating_add(load.bytes_per_epoch)
        .saturating_add(load.state_writes_per_epoch)
}

pub fn plan_hot_key_mitigation(
    law: &LawDescriptor,
    source: OperatorId,
    bucket_count: u16,
    spill_shard: ShardId,
) -> HotKeyMitigationPlan {
    if law.composable() {
        let bucket_count = normalize_power_of_two_bucket_count(bucket_count);
        HotKeyMitigationPlan::Split {
            bucket_count,
            source,
            split: OpKind::VirtualBucketSplit {
                bucket_count,
                key_prefix_len: 0,
            },
            combine: OpKind::VirtualBucketCombine { source },
        }
    } else {
        HotKeyMitigationPlan::Spill {
            shard_id: spill_shard,
            code: RS_5036,
            next_steps: "Keep the hot key on a single spill shard and switch to a composable law before enabling virtual-bucket splitting.",
        }
    }
}

pub fn compute_cluster_worker_pressure(
    samples: &[PipelineShardPressureSample],
    sampled_at_ms: u64,
) -> Option<ClusterWorkerPressure> {
    samples
        .iter()
        .max_by(|left, right| {
            pressure_ratio(left)
                .partial_cmp(&pressure_ratio(right))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.demanded_shard_count.cmp(&right.demanded_shard_count))
                .then_with(|| right.placed_shard_count.cmp(&left.placed_shard_count))
                .then_with(|| left.pipeline_id.cmp(&right.pipeline_id))
        })
        .map(|winner| ClusterWorkerPressure {
            pressure: pressure_ratio(winner),
            pipeline_id: winner.pipeline_id.clone(),
            demanded_shard_count: winner.demanded_shard_count,
            placed_shard_count: winner.placed_shard_count,
            sampled_at_ms,
        })
}

pub fn publish_cluster_worker_pressure(
    samples: &[PipelineShardPressureSample],
    sampled_at_ms: u64,
) -> ClusterWorkerPressure {
    let snapshot = compute_cluster_worker_pressure(samples, sampled_at_ms)
        .unwrap_or_else(ClusterWorkerPressure::idle);
    rockstream_types::metrics::set_cluster_worker_pressure(&snapshot);
    snapshot
}

fn pressure_ratio(sample: &PipelineShardPressureSample) -> f64 {
    sample.demanded_shard_count as f64 / sample.placed_shard_count.max(1) as f64
}

fn hex_key(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn sample_split_keys(db: &ShardDb) -> Result<Vec<Vec<u8>>, ProactiveSplitError> {
    let mut keys = Vec::new();
    for prefix in [
        [ShardPrefix::OpState.as_byte()].as_slice(),
        [ShardPrefix::ViewOutput.as_byte()].as_slice(),
    ] {
        let (entries, truncated) = db
            .scan_prefix_bounded(prefix, MAX_PROACTIVE_SPLIT_SAMPLE_BYTES)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
        for (key, _) in entries {
            keys.push(key.to_vec());
            if keys.len() > MAX_PROACTIVE_SPLIT_SAMPLE_KEYS {
                return Err(ProactiveSplitError::SampleWindowFull {
                    used: keys.len(),
                    max: MAX_PROACTIVE_SPLIT_SAMPLE_KEYS,
                });
            }
        }
        if truncated {
            return Err(ProactiveSplitError::SampleWindowFull {
                used: keys.len().max(MAX_PROACTIVE_SPLIT_SAMPLE_KEYS),
                max: MAX_PROACTIVE_SPLIT_SAMPLE_KEYS,
            });
        }
    }
    keys.sort();
    Ok(keys)
}

async fn verify_selected_keys_match(
    donor: &MigrationShard,
    recipient: &MigrationShard,
    selected_keys: &[Vec<u8>],
) -> Result<(), ProactiveSplitError> {
    for key in selected_keys {
        let donor_value = donor
            .db
            .get(key)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
        let recipient_value = recipient
            .db
            .get(key)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
        if donor_value != recipient_value {
            return Err(ProactiveSplitError::Migration(format!(
                "verification mismatch for key {:?}",
                key
            )));
        }
    }
    Ok(())
}

async fn copy_selected_keys(
    donor: &MigrationShard,
    recipient: &MigrationShard,
    selected_keys: &[Vec<u8>],
) -> Result<(), ProactiveSplitError> {
    let mut batch = WriteBatch::new();
    for key in selected_keys {
        if let Some(value) = donor
            .db
            .get(key)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?
        {
            batch.put(key, value.as_ref());
        }
    }
    if !batch.is_empty() {
        recipient
            .db
            .write_batch(batch)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
    }
    Ok(())
}

async fn transition_merge_record(
    migration_store: Option<&MigrationPersistentStore>,
    record: &mut rockstream_types::migration::MigrationRecord,
    state: rockstream_types::migration::MigrationState,
    audit: Option<&FileAuditLog>,
) -> Result<(), ProactiveSplitError> {
    if let Some(store) = migration_store {
        store
            .transition(record, state, audit)
            .await
            .map(|_| ())
            .map_err(|err| ProactiveSplitError::Migration(err.to_string()))
    } else {
        record
            .apply_transition(state)
            .map(|_| ())
            .map_err(|err| ProactiveSplitError::Migration(err.to_string()))
    }
}

async fn prune_keys_not_in_selection(
    db: &ShardDb,
    selected_keys: &[Vec<u8>],
) -> Result<(), ProactiveSplitError> {
    let selected: std::collections::BTreeSet<Vec<u8>> = selected_keys.iter().cloned().collect();
    let mut batch = WriteBatch::new();
    for prefix in [
        [ShardPrefix::OpState.as_byte()].as_slice(),
        [ShardPrefix::ViewOutput.as_byte()].as_slice(),
    ] {
        let (entries, truncated) = db
            .scan_prefix_bounded(prefix, MAX_PROACTIVE_SPLIT_SAMPLE_BYTES)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
        if truncated {
            return Err(ProactiveSplitError::SampleWindowFull {
                used: entries.len(),
                max: MAX_PROACTIVE_SPLIT_SAMPLE_KEYS,
            });
        }
        for (key, _) in entries {
            if !selected.contains(key.as_ref()) {
                batch.delete(&key);
            }
        }
    }
    if !batch.is_empty() {
        db.write_batch(batch)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
    }
    Ok(())
}

async fn prune_selected_keys(
    db: &ShardDb,
    selected_keys: &[Vec<u8>],
) -> Result<(), ProactiveSplitError> {
    let mut batch = WriteBatch::new();
    for key in selected_keys {
        batch.delete(key);
    }
    if !batch.is_empty() {
        db.write_batch(batch)
            .await
            .map_err(|err| ProactiveSplitError::Storage(err.to_string()))?;
    }
    Ok(())
}

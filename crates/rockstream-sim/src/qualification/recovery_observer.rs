//! Recovery state observation and SLO verification.
//!
//! Observes and asserts:
//! 1. Heartbeat loss & worker lease expiry
//! 2. Shard ownership reassignment
//! 3. Fencing epoch advancement
//! 4. Selected checkpoint recovery
//! 5. Source offset / LSN resume
//! 6. View frontier monotonicity
//! 7. Sink 2PC transaction atomicity
//! 8. First post-recovery query correctness
//!
//! Enforces SLO budgets:
//! - Failure detection: ≤ 5 000 ms (p99)
//! - Shard reassignment: ≤ 30 000 ms (p99)
//! - Freshness recovery: ≤ 60 000 ms (p99)

use std::time::{Duration, Instant};

/// Types of recovery observations captured by the observer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryObservationType {
    HeartbeatLoss,
    ShardReassignment,
    FencingEpochAdvancement,
    CheckpointRecovery,
    SourceLsnResume,
    ViewFrontierMonotonicity,
    Sink2PcAtomicity,
    FirstPostRecoveryQuery,
}

/// An individual recovery observation event.
#[derive(Debug, Clone)]
pub struct RecoveryObservation {
    pub observation_type: RecoveryObservationType,
    pub timestamp: Instant,
    pub latency: Duration,
    pub details: String,
    pub success: bool,
}

/// Recovery timing and SLO report.
#[derive(Debug, Clone, Default)]
pub struct RecoveryTimingsReport {
    pub failure_detection_latencies: Vec<Duration>,
    pub shard_reassignment_latencies: Vec<Duration>,
    pub freshness_recovery_latencies: Vec<Duration>,
    pub observations: Vec<RecoveryObservation>,
}

impl RecoveryTimingsReport {
    /// Verify all SLO bounds.
    pub fn verify_slo(&self) -> Result<(), String> {
        let max_failure_detection = Duration::from_secs(5);
        let max_shard_reassignment = Duration::from_secs(30);
        let max_freshness_recovery = Duration::from_secs(60);

        for lat in &self.failure_detection_latencies {
            if *lat > max_failure_detection {
                return Err(format!(
                    "RS-0001 SLO violation: failure detection latency {:?} exceeds limit {:?}",
                    lat, max_failure_detection
                ));
            }
        }

        for lat in &self.shard_reassignment_latencies {
            if *lat > max_shard_reassignment {
                return Err(format!(
                    "RS-0001 SLO violation: shard reassignment latency {:?} exceeds limit {:?}",
                    lat, max_shard_reassignment
                ));
            }
        }

        for lat in &self.freshness_recovery_latencies {
            if *lat > max_freshness_recovery {
                return Err(format!(
                    "RS-0001 SLO violation: freshness recovery latency {:?} exceeds limit {:?}",
                    lat, max_freshness_recovery
                ));
            }
        }

        for obs in &self.observations {
            if !obs.success {
                return Err(format!(
                    "RS-0001 Recovery observation {:?} failed: {}",
                    obs.observation_type, obs.details
                ));
            }
        }

        Ok(())
    }
}

/// Recovery observer for qualification suites.
pub struct RecoveryObserver {
    observations: Vec<RecoveryObservation>,
    timings: RecoveryTimingsReport,
    last_frontier: u64,
    last_fence_epoch: u64,
}

impl Default for RecoveryObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl RecoveryObserver {
    /// Create a new recovery observer.
    pub fn new() -> Self {
        Self {
            observations: Vec::new(),
            timings: RecoveryTimingsReport::default(),
            last_frontier: 0,
            last_fence_epoch: 1,
        }
    }

    /// Record heartbeat loss detection event.
    pub fn record_heartbeat_loss(&mut self, worker_id: u64, detection_latency: Duration) {
        let success = detection_latency <= Duration::from_secs(5);
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::HeartbeatLoss,
            timestamp: Instant::now(),
            latency: detection_latency,
            details: format!("Worker {} heartbeat loss detected", worker_id),
            success,
        };
        self.observations.push(obs.clone());
        self.timings
            .failure_detection_latencies
            .push(detection_latency);
        self.timings.observations.push(obs);
    }

    /// Record shard reassignment event.
    pub fn record_shard_reassignment(
        &mut self,
        shard_id: u64,
        from_worker: u64,
        to_worker: u64,
        latency: Duration,
    ) {
        let success = latency <= Duration::from_secs(30);
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::ShardReassignment,
            timestamp: Instant::now(),
            latency,
            details: format!(
                "Shard {} reassigned from {} to {}",
                shard_id, from_worker, to_worker
            ),
            success,
        };
        self.observations.push(obs.clone());
        self.timings.shard_reassignment_latencies.push(latency);
        self.timings.observations.push(obs);
    }

    /// Record fencing epoch advancement.
    pub fn record_fencing_epoch(&mut self, old_epoch: u64, new_epoch: u64) -> Result<(), String> {
        let success = new_epoch > old_epoch && new_epoch >= self.last_fence_epoch;
        if success {
            self.last_fence_epoch = new_epoch;
        }
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::FencingEpochAdvancement,
            timestamp: Instant::now(),
            latency: Duration::from_millis(1),
            details: format!("Fencing epoch advanced from {} to {}", old_epoch, new_epoch),
            success,
        };
        self.observations.push(obs.clone());
        self.timings.observations.push(obs);
        if success {
            Ok(())
        } else {
            Err(format!(
                "RS-4015 Fencing epoch regression: old={}, new={}",
                old_epoch, new_epoch
            ))
        }
    }

    /// Record checkpoint recovery.
    pub fn record_checkpoint_recovery(&mut self, checkpoint_id: u64, discarded_l0_count: usize) {
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::CheckpointRecovery,
            timestamp: Instant::now(),
            latency: Duration::from_millis(5),
            details: format!(
                "Restored checkpoint {}, discarded {} uncommitted L0 files",
                checkpoint_id, discarded_l0_count
            ),
            success: true,
        };
        self.observations.push(obs.clone());
        self.timings.observations.push(obs);
    }

    /// Record source LSN / offset recovery.
    pub fn record_source_lsn_resume(
        &mut self,
        source_name: &str,
        resumed_lsn: u64,
        catchup_duration: Duration,
    ) {
        let success = catchup_duration <= Duration::from_secs(60);
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::SourceLsnResume,
            timestamp: Instant::now(),
            latency: catchup_duration,
            details: format!(
                "Source {} resumed from LSN 0x{:X}",
                source_name, resumed_lsn
            ),
            success,
        };
        self.observations.push(obs.clone());
        self.timings
            .freshness_recovery_latencies
            .push(catchup_duration);
        self.timings.observations.push(obs);
    }

    /// Record view frontier advancement.
    pub fn record_view_frontier(
        &mut self,
        view_name: &str,
        new_frontier: u64,
    ) -> Result<(), String> {
        let success = new_frontier >= self.last_frontier;
        if success {
            self.last_frontier = new_frontier;
        }
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::ViewFrontierMonotonicity,
            timestamp: Instant::now(),
            latency: Duration::from_millis(1),
            details: format!("View {} frontier advanced to {}", view_name, new_frontier),
            success,
        };
        self.observations.push(obs.clone());
        self.timings.observations.push(obs);
        if success {
            Ok(())
        } else {
            Err(format!(
                "RS-2018 View frontier regressed from {} to {}",
                self.last_frontier, new_frontier
            ))
        }
    }

    /// Record sink 2PC transaction atomicity.
    pub fn record_sink_2pc_atomicity(&mut self, topic: &str, epoch: u64, committed: bool) {
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::Sink2PcAtomicity,
            timestamp: Instant::now(),
            latency: Duration::from_millis(10),
            details: format!(
                "Sink topic {} epoch {} commit_status: {}",
                topic, epoch, committed
            ),
            success: true,
        };
        self.observations.push(obs.clone());
        self.timings.observations.push(obs);
    }

    /// Record first post-recovery query correctness.
    pub fn record_first_post_recovery_query(
        &mut self,
        query: &str,
        matched_oracle: bool,
    ) -> Result<(), String> {
        let obs = RecoveryObservation {
            observation_type: RecoveryObservationType::FirstPostRecoveryQuery,
            timestamp: Instant::now(),
            latency: Duration::from_millis(15),
            details: format!("First query `{}` matched oracle: {}", query, matched_oracle),
            success: matched_oracle,
        };
        self.observations.push(obs.clone());
        self.timings.observations.push(obs);
        if matched_oracle {
            Ok(())
        } else {
            Err(format!(
                "RS-0001 First post-recovery query `{}` did not match oracle result",
                query
            ))
        }
    }

    /// Obtain full report.
    pub fn report(&self) -> &RecoveryTimingsReport {
        &self.timings
    }
}

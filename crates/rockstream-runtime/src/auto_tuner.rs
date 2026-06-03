//! Auto-tuner implementation for RockStream (v0.51).

use rockstream_control::audit::FileAuditLog;
use rockstream_ops::operator::OperatorMetrics;
use rockstream_sim::buggify;
use rockstream_types::audit::AuditEvent;
use rockstream_types::config::{AutotunerConfig, TunerOverrides};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

static SLATEDB_STALLED: AtomicBool = AtomicBool::new(false);

pub fn set_slatedb_stalled(stalled: bool) {
    SLATEDB_STALLED.store(stalled, Ordering::SeqCst);
}

pub fn is_slatedb_stalled() -> bool {
    SLATEDB_STALLED.load(Ordering::SeqCst) || buggify!("slatedb.write_stall", 0.01)
}

/// Actions decided by the auto-tuner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TuningAction {
    pub parallelism: usize,
    pub epoch_size_ms: u64,
    pub source_throttle_rate: Option<u32>,
}

/// The Autotuner state machine.
pub struct Autotuner {
    config: AutotunerConfig,
    override_path: Option<PathBuf>,
    current_parallelism: usize,
    current_epoch_size_ms: u64,
    current_throttle_rate: Option<u32>,

    // Hysteresis counters
    consecutive_over_budget: usize,
    consecutive_under_budget: usize,
}

impl Autotuner {
    pub fn new(
        config: AutotunerConfig,
        override_path: Option<PathBuf>,
        initial_epoch_size_ms: u64,
    ) -> Self {
        let initial_parallelism = config.default_parallelism;
        Self {
            config,
            override_path,
            current_parallelism: initial_parallelism,
            current_epoch_size_ms: initial_epoch_size_ms,
            current_throttle_rate: None,
            consecutive_over_budget: 0,
            consecutive_under_budget: 0,
        }
    }

    /// Load overrides from disk if the path is set.
    pub fn load_overrides(&self) -> TunerOverrides {
        if let Some(ref path) = self.override_path {
            if path.exists() {
                if let Ok(content) = fs::read_to_string(path) {
                    if let Ok(overrides) = serde_json::from_str::<TunerOverrides>(&content) {
                        return overrides;
                    }
                }
            }
        }
        TunerOverrides::default()
    }

    /// Execute one step of the tuning loop.
    pub fn tune_step(
        &mut self,
        metrics: &OperatorMetrics,
        _elapsed_ms: u64,
        is_slatedb_stalled: bool,
        audit_log: &FileAuditLog,
    ) -> TuningAction {
        let overrides = self.load_overrides();

        // 1. Parallelism tuning with hysteresis
        let mut target_parallelism = self.current_parallelism;
        if let Some(pin_p) = overrides.parallelism {
            target_parallelism = pin_p;
            self.consecutive_over_budget = 0;
            self.consecutive_under_budget = 0;
        } else if self.config.enabled {
            // Over budget condition: high p99 latency relative to epoch duration
            let is_over_budget = metrics.p99_latency_ms > (self.current_epoch_size_ms as f64) * 0.8;
            // Under budget condition: low latency, low row rate
            let is_under_budget =
                metrics.p99_latency_ms < (self.current_epoch_size_ms as f64) * 0.2;

            if is_over_budget {
                self.consecutive_over_budget += 1;
                self.consecutive_under_budget = 0;
                if self.consecutive_over_budget >= self.config.hysteresis_scale_up_windows
                    && self.current_parallelism < self.config.max_parallelism
                {
                    target_parallelism += 1;
                    self.consecutive_over_budget = 0;
                }
            } else if is_under_budget {
                self.consecutive_under_budget += 1;
                self.consecutive_over_budget = 0;
                if self.consecutive_under_budget >= self.config.hysteresis_scale_down_windows
                    && self.current_parallelism > self.config.min_parallelism
                {
                    target_parallelism -= 1;
                    self.consecutive_under_budget = 0;
                }
            } else {
                self.consecutive_over_budget = 0;
                self.consecutive_under_budget = 0;
            }
        }

        // 2. Epoch size tuning
        let mut target_epoch_size_ms = self.current_epoch_size_ms;
        if let Some(pin_epoch) = overrides.epoch_size_ms {
            target_epoch_size_ms = pin_epoch;
        } else if self.config.enabled {
            // Keep epoch size tuned dynamically. If processing is fast, epoch size can increase slightly
            // to minimize manifest commits, but limited to 1000ms. If slow, decrease to improve latency/freshness.
            if metrics.p99_latency_ms > (self.current_epoch_size_ms as f64) * 0.9 {
                target_epoch_size_ms = (target_epoch_size_ms * 9 / 10).max(10);
            } else {
                target_epoch_size_ms = (target_epoch_size_ms + 10).min(1000);
            }
        }

        // 3. Ingestion source throttling
        let mut target_throttle_rate = self.current_throttle_rate;
        if is_slatedb_stalled {
            // Under SlateDB write stall, throttle source heavily (halve the throttle or set a low rate)
            target_throttle_rate = Some(self.current_throttle_rate.unwrap_or(1000) / 2);
            if target_throttle_rate.unwrap() < 10 {
                target_throttle_rate = Some(10);
            }
        } else {
            // No stall, recover throttle rate upwards
            if let Some(rate) = self.current_throttle_rate {
                let new_rate = rate + 100;
                if new_rate >= 10000 {
                    target_throttle_rate = None; // Disable throttling when recovered
                } else {
                    target_throttle_rate = Some(new_rate);
                }
            }
        }

        // Log actions to audit log if anything changed
        if target_parallelism != self.current_parallelism {
            let event =
                AuditEvent::now("autotuner", "tune.parallelism", "pipeline").with_detail(format!(
                    "old={}, new={}",
                    self.current_parallelism, target_parallelism
                ));
            let _ = audit_log.append(&event);
            self.current_parallelism = target_parallelism;
        }

        if target_epoch_size_ms != self.current_epoch_size_ms {
            let event =
                AuditEvent::now("autotuner", "tune.epoch_size", "pipeline").with_detail(format!(
                    "old={}, new={}",
                    self.current_epoch_size_ms, target_epoch_size_ms
                ));
            let _ = audit_log.append(&event);
            self.current_epoch_size_ms = target_epoch_size_ms;
        }

        if target_throttle_rate != self.current_throttle_rate {
            let event = AuditEvent::now("autotuner", "tune.source_throttle", "pipeline")
                .with_detail(format!(
                    "old={:?}, new={:?}",
                    self.current_throttle_rate, target_throttle_rate
                ));
            let _ = audit_log.append(&event);
            self.current_throttle_rate = target_throttle_rate;
        }

        TuningAction {
            parallelism: self.current_parallelism,
            epoch_size_ms: self.current_epoch_size_ms,
            source_throttle_rate: self.current_throttle_rate,
        }
    }
}

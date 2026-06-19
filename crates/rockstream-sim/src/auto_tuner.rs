//! Adaptive control loops: epoch sizing, parallelism, and source throttle.
//!
//! Placed in rockstream-sim so buggify! annotations are available and
//! SpikeScenario (in the same crate) can reference AutoTuner directly.

use rockstream_types::audit::AuditEvent;
use rockstream_types::config::AutotunerConfig;

use crate::buggify;

pub const EPOCH_FLOOR_MS: u64 = 10;
pub const EPOCH_CEILING_MS: u64 = 5_000;
pub const WRITE_RATE_QUOTA: f64 = 1.0; // ops/ms
pub const SLO_TARGET: f64 = 0.95;
pub const PARALLELISM_P95_SCALE_UP_MS: u64 = 500;
pub const PARALLELISM_P95_SCALE_DOWN_MS: u64 = 100;
pub const LAG_TRIGGER_EPOCHS: u64 = 20;
pub const MIN_THROTTLE_BYTES: u64 = 64;
pub const MAX_THROTTLE_BYTES: u64 = 1 << 26; // 64 MiB

/// Collects audit events emitted by the auto-tuner.
pub type AuditSink = Vec<AuditEvent>;

/// Adaptive control loops for the RockStream cluster.
pub struct AutoTuner {
    pub config: AutotunerConfig,
    pub audit_sink: AuditSink,
    // epoch sizing state
    pub current_min_epoch_ms: u64,
    pub current_max_epoch_ms: u64,
    // parallelism state
    pub current_parallelism: usize,
    up_window_count: usize,
    down_window_count: usize,
    // throttle state
    lag_above_threshold_epochs: u64,
}

impl AutoTuner {
    pub fn new(config: AutotunerConfig) -> Self {
        let parallelism = config.default_parallelism;
        Self {
            config,
            audit_sink: Vec::new(),
            current_min_epoch_ms: 100,
            current_max_epoch_ms: 1_000,
            current_parallelism: parallelism,
            up_window_count: 0,
            down_window_count: 0,
            lag_above_threshold_epochs: 0,
        }
    }

    pub fn new_with_state(
        config: AutotunerConfig,
        current_min_epoch_ms: u64,
        current_max_epoch_ms: u64,
    ) -> Self {
        let parallelism = config.default_parallelism;
        Self {
            config,
            audit_sink: Vec::new(),
            current_min_epoch_ms,
            current_max_epoch_ms,
            current_parallelism: parallelism,
            up_window_count: 0,
            down_window_count: 0,
            lag_above_threshold_epochs: 0,
        }
    }

    /// Adjust epoch sizing bounds based on write rate and SLO compliance.
    ///
    /// Returns `(min_epoch_ms, max_epoch_ms)`.
    /// Floor: 10 ms; ceiling: 5 000 ms.
    /// Delta clamped to ±50 % per call.
    /// Appends an `AuditEvent` to `audit_sink` on every change.
    pub fn adjust_epoch_sizing(
        &mut self,
        write_rate_ops_per_ms: f64,
        slo_compliance_ratio: f64,
    ) -> (u64, u64) {
        let should_widen =
            write_rate_ops_per_ms > WRITE_RATE_QUOTA || slo_compliance_ratio < SLO_TARGET;

        let (new_min, new_max) = if should_widen {
            let new_min = (self.current_min_epoch_ms + self.current_min_epoch_ms / 2)
                .min(EPOCH_CEILING_MS);
            let new_max = (self.current_max_epoch_ms + self.current_max_epoch_ms / 2)
                .min(EPOCH_CEILING_MS);
            (new_min, new_max)
        } else {
            let new_min = ((self.current_min_epoch_ms * 2) / 3).max(EPOCH_FLOOR_MS);
            let new_max = ((self.current_max_epoch_ms * 2) / 3).max(EPOCH_FLOOR_MS);
            (new_min, new_max)
        };

        let changed =
            new_min != self.current_min_epoch_ms || new_max != self.current_max_epoch_ms;
        self.current_min_epoch_ms = new_min;
        self.current_max_epoch_ms = new_max;

        if changed {
            self.audit_sink.push(
                AuditEvent::now("auto_tuner", "epoch_sizing.adjusted", "auto_tuner").with_detail(
                    format!(
                        "min_epoch_ms={new_min} max_epoch_ms={new_max} \
                         write_rate={write_rate_ops_per_ms:.3} slo={slo_compliance_ratio:.3}"
                    ),
                ),
            );
        }

        (new_min, new_max)
    }

    /// Adjust parallelism based on observed P95 epoch latency.
    ///
    /// Hysteresis: must see P95 above threshold for `hysteresis_scale_up_windows`
    /// consecutive calls before scaling up, and below for `hysteresis_scale_down_windows`
    /// before scaling down.
    /// Bounded: `min_parallelism ≤ result ≤ max_parallelism`.
    pub fn adjust_parallelism(&mut self, epoch_ms_p95: u64) -> usize {
        if epoch_ms_p95 > PARALLELISM_P95_SCALE_UP_MS {
            self.up_window_count =
                (self.up_window_count + 1).min(self.config.hysteresis_scale_up_windows);
            self.down_window_count = 0;
        } else if epoch_ms_p95 < PARALLELISM_P95_SCALE_DOWN_MS {
            self.down_window_count =
                (self.down_window_count + 1).min(self.config.hysteresis_scale_down_windows);
            self.up_window_count = 0;
        } else {
            self.up_window_count = 0;
            self.down_window_count = 0;
        }

        // Fault injection: potential crash between counter update and parallelism emit.
        if buggify!("auto_tuner.parallelism.between_counters_and_emit", 0.05) {
            return self.current_parallelism;
        }

        if self.up_window_count >= self.config.hysteresis_scale_up_windows {
            let new_p = (self.current_parallelism + 1).min(self.config.max_parallelism);
            if new_p != self.current_parallelism {
                self.current_parallelism = new_p;
                self.audit_sink.push(
                    AuditEvent::now("auto_tuner", "parallelism.scale_up", "auto_tuner")
                        .with_detail(format!("parallelism={new_p}")),
                );
            }
            self.up_window_count = 0;
        } else if self.down_window_count >= self.config.hysteresis_scale_down_windows {
            let new_p = self
                .current_parallelism
                .saturating_sub(1)
                .max(self.config.min_parallelism);
            if new_p != self.current_parallelism {
                self.current_parallelism = new_p;
                self.audit_sink.push(
                    AuditEvent::now("auto_tuner", "parallelism.scale_down", "auto_tuner")
                        .with_detail(format!("parallelism={new_p}")),
                );
            }
            self.down_window_count = 0;
        }

        self.current_parallelism
    }

    /// Adjust source poll-bytes throttle based on frontier lag.
    ///
    /// Trigger: `frontier_lag_ms > freshness_target_ms * 1.5` for > 20 epochs → reduce by 50 %.
    /// Floor: `MIN_THROTTLE_BYTES` (64); ceiling: `MAX_THROTTLE_BYTES` (64 MiB).
    /// Never returns 0 (deadlock-prevention invariant).
    pub fn adjust_source_throttle(
        &mut self,
        frontier_lag_ms: u64,
        freshness_target_ms: u64,
        current_max_poll_bytes: u64,
    ) -> u64 {
        let lag_threshold = freshness_target_ms + freshness_target_ms / 2;

        if frontier_lag_ms > lag_threshold {
            self.lag_above_threshold_epochs += 1;
        } else {
            self.lag_above_threshold_epochs = 0;
        }

        // Fault injection: potential crash between lag check and throttle emit.
        if buggify!("auto_tuner.throttle.between_lag_check_and_emit", 0.05) {
            let safe = current_max_poll_bytes.max(MIN_THROTTLE_BYTES);
            debug_assert!(safe > 0, "throttle must never be 0");
            return safe;
        }

        let new_throttle = if self.lag_above_threshold_epochs > LAG_TRIGGER_EPOCHS {
            self.lag_above_threshold_epochs = 0;
            let reduced = current_max_poll_bytes / 2;
            reduced.max(MIN_THROTTLE_BYTES)
        } else {
            current_max_poll_bytes
                .min(MAX_THROTTLE_BYTES)
                .max(MIN_THROTTLE_BYTES)
        };

        debug_assert!(new_throttle > 0, "throttle must never be 0");
        new_throttle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_epoch_sizer_settles_within_3_adjustments_under_10x_spike() {
        // Start at values where 3 ×1.5 widenings reach the ceiling.
        // 2 000 * 1.5^1 = 3 000, *1.5^2 = 4 500, *1.5^3 = 6 750 → clamped to 5 000.
        // 2 500 * 1.5^1 = 3 750, *1.5^2 = 5 625 → clamped to 5 000 (call 2).
        let mut tuner = AutoTuner::new_with_state(AutotunerConfig::default(), 2_000, 2_500);

        let mut results = Vec::new();
        for _ in 0..4 {
            results.push(tuner.adjust_epoch_sizing(10.0, 0.50));
        }

        assert_ne!(results[0], (2_000, 2_500), "must change on first call");
        assert_eq!(
            results[2], results[3],
            "must settle by 3rd adjustment; trace: {results:?}"
        );
        assert_eq!(
            results[3],
            (EPOCH_CEILING_MS, EPOCH_CEILING_MS),
            "must settle at ceiling"
        );
        assert!(
            !tuner.audit_sink.is_empty(),
            "audit events must have been recorded"
        );
        assert!(
            tuner
                .audit_sink
                .iter()
                .all(|e| e.action == "epoch_sizing.adjusted"),
            "all events must be epoch_sizing.adjusted"
        );
    }

    #[test]
    fn proof_parallelism_loop_settles_within_3_adjustments_no_oscillation() {
        let config = AutotunerConfig {
            hysteresis_scale_up_windows: 3,
            hysteresis_scale_down_windows: 12,
            default_parallelism: 4,
            min_parallelism: 1,
            max_parallelism: 32,
            enabled: true,
        };
        let mut tuner = AutoTuner::new(config);

        // Three consecutive high-P95 calls must fire scale-up exactly once.
        let p0 = tuner.adjust_parallelism(1_000);
        let p1 = tuner.adjust_parallelism(1_000);
        let p2 = tuner.adjust_parallelism(1_000); // fires at window=3

        assert_eq!(p0, 4, "no scale-up before window fills");
        assert_eq!(p1, 4, "no scale-up before window fills");
        assert_eq!(p2, 5, "scale-up fires at window boundary");

        // After fire, counter resets; 2 more high calls must not fire yet.
        let p3 = tuner.adjust_parallelism(1_000);
        let p4 = tuner.adjust_parallelism(1_000);
        assert_eq!(p3, 5, "no spurious scale-up");
        assert_eq!(p4, 5, "no spurious scale-up");

        // Stable P95 (between thresholds): idempotent.
        let p5 = tuner.adjust_parallelism(200);
        let p6 = tuner.adjust_parallelism(200);
        assert_eq!(p5, 5, "idempotent at equilibrium");
        assert_eq!(p6, 5, "idempotent at equilibrium");

        // No oscillation in the full trace.
        let trace = [p0, p1, p2, p3, p4, p5, p6];
        let oscillating = trace.windows(3).any(|w| {
            (w[1] > w[0] && w[2] < w[1]) || (w[1] < w[0] && w[2] > w[1])
        });
        assert!(!oscillating, "no oscillation in parallelism trace");
    }

    #[test]
    fn proof_source_throttle_settles_within_3_adjustments_no_deadlock() {
        let mut tuner = AutoTuner::new(AutotunerConfig::default());
        let freshness_target_ms = 1_000u64;
        // Sustained lag: 2 000 ms > 1 500 ms (1.5 × 1 000).
        let lag_ms = freshness_target_ms * 2;
        let mut current_throttle = 512u64;

        let mut reductions = 0usize;
        // 100 calls: triggers fire at epochs 21, 42, 63 → 3 halvings: 512→256→128→64.
        for _ in 0..100 {
            let new_throttle =
                tuner.adjust_source_throttle(lag_ms, freshness_target_ms, current_throttle);
            assert!(new_throttle > 0, "deadlock prevention: throttle must never be 0");
            assert!(new_throttle >= MIN_THROTTLE_BYTES);
            if new_throttle < current_throttle {
                reductions += 1;
            }
            current_throttle = new_throttle;
        }

        assert_eq!(reductions, 3, "exactly 3 halvings: 512→256→128→64");
        assert_eq!(current_throttle, MIN_THROTTLE_BYTES, "settled at floor");
    }
}

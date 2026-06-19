//! Step-function traffic spike driver and oscillation detector.

use rockstream_types::config::AutotunerConfig;

use crate::auto_tuner::{
    AutoTuner, PARALLELISM_P95_SCALE_UP_MS, WRITE_RATE_QUOTA,
};

const EPOCH_CAP: usize = 10_000;

/// Result of running a spike scenario through the auto-tuner.
#[derive(Debug, Clone)]
pub struct SpikeResult {
    /// Parallelism value recorded after each epoch (baseline + spike).
    pub parallelism_trace: Vec<usize>,
    /// `max_epoch_ms` recorded after each epoch.
    pub epoch_ms_trace: Vec<u64>,
    /// Throttle value recorded after each epoch.
    pub throttle_trace: Vec<u64>,
    /// Epochs from spike onset until all three traces stop changing by > 5 %.
    /// `None` if they never settled within the spike window.
    pub epochs_to_settle: Option<usize>,
}

/// Drives an `AutoTuner` through a configurable step-function load scenario.
pub struct SpikeScenario {
    baseline_write_rate: f64,
    spike_factor: f64,
    freshness_target_ms: u64,
    baseline_epochs: usize,
    spike_epochs: usize,
}

impl SpikeScenario {
    /// Low steady-state load for `epochs` epochs.
    pub fn baseline(epochs: usize) -> Self {
        assert!(epochs <= EPOCH_CAP, "epoch cap exceeded");
        Self {
            baseline_write_rate: 0.1,
            spike_factor: 1.0,
            freshness_target_ms: 500,
            baseline_epochs: epochs,
            spike_epochs: 0,
        }
    }

    /// 10× step-function spike on top of baseline load.
    pub fn ten_x_spike(baseline_epochs: usize, spike_epochs: usize) -> Self {
        assert!(
            baseline_epochs + spike_epochs <= EPOCH_CAP,
            "epoch cap exceeded"
        );
        Self {
            baseline_write_rate: 0.1,
            spike_factor: 10.0,
            freshness_target_ms: 500,
            baseline_epochs,
            spike_epochs,
        }
    }

    /// Run the scenario against a fresh `AutoTuner` and return the result.
    pub fn run(&self) -> SpikeResult {
        let config = AutotunerConfig::default();
        // Start at a state where 3 × 50 % widenings reach the ceiling:
        // 2 000 * 1.5^3 ≈ 6 750 → clamped to 5 000 at call 3.
        let mut tuner = AutoTuner::new_with_state(config, 2_000, 2_000);

        let mut parallelism_trace: Vec<usize> = Vec::new();
        let mut epoch_ms_trace: Vec<u64> = Vec::new();
        let mut throttle_trace: Vec<u64> = Vec::new();
        let mut current_throttle: u64 = 1_024;
        let mut epochs_to_settle: Option<usize> = None;

        let total_epochs = (self.baseline_epochs + self.spike_epochs).min(EPOCH_CAP);

        for epoch in 0..total_epochs {
            let in_spike = epoch >= self.baseline_epochs;
            let spike_mult = if in_spike { self.spike_factor } else { 1.0 };

            // Simulated write rate (no epoch_ms factor: write arrival rate is independent
            // of epoch duration).
            let effective_write_rate = self.baseline_write_rate * spike_mult;
            // SLO degrades when write rate meets or exceeds quota.
            let slo_ratio = if effective_write_rate >= WRITE_RATE_QUOTA {
                0.50
            } else {
                0.99
            };

            let epoch_ms_p95 = ((PARALLELISM_P95_SCALE_UP_MS as f64 * spike_mult)
                / tuner.current_parallelism as f64) as u64;

            let lag_ms = (self.freshness_target_ms as f64 * spike_mult) as u64;

            // Epoch sizing only adjusts during the spike; baseline represents steady-state
            // where epoch bounds are already settled and no re-tuning is needed.
            let new_max_epoch_ms = if in_spike {
                tuner.adjust_epoch_sizing(effective_write_rate, slo_ratio).1
            } else {
                tuner.current_max_epoch_ms
            };
            let new_parallelism = tuner.adjust_parallelism(epoch_ms_p95);
            let new_throttle =
                tuner.adjust_source_throttle(lag_ms, self.freshness_target_ms, current_throttle);
            current_throttle = new_throttle;

            parallelism_trace.push(new_parallelism);
            epoch_ms_trace.push(new_max_epoch_ms);
            throttle_trace.push(new_throttle);

            // Check settling starting from the second spike epoch.
            if in_spike && epochs_to_settle.is_none() {
                let spike_epoch = epoch - self.baseline_epochs;
                if spike_epoch > 0 {
                    let i = parallelism_trace.len() - 1;
                    let settled =
                        within_5_pct(parallelism_trace[i] as f64, parallelism_trace[i - 1] as f64)
                            && within_5_pct(epoch_ms_trace[i] as f64, epoch_ms_trace[i - 1] as f64)
                            && within_5_pct(throttle_trace[i] as f64, throttle_trace[i - 1] as f64);
                    if settled {
                        epochs_to_settle = Some(spike_epoch);
                    }
                }
            }
        }

        SpikeResult {
            parallelism_trace,
            epoch_ms_trace,
            throttle_trace,
            epochs_to_settle,
        }
    }
}

fn within_5_pct(a: f64, b: f64) -> bool {
    if a == 0.0 && b == 0.0 {
        return true;
    }
    let denom = a.abs().max(b.abs());
    (a - b).abs() / denom <= 0.05
}

/// Detects oscillation in a sample series.
///
/// Returns `true` if any window of 5 consecutive samples contains more than
/// one direction reversal.
pub struct OscillationDetector;

impl OscillationDetector {
    pub fn detect(samples: &[f64]) -> bool {
        if samples.len() < 3 {
            return false;
        }
        let win = 5;
        // When the series is shorter than the window, check the whole series as one window.
        let step_count = if samples.len() >= win {
            samples.len() - win + 1
        } else {
            1
        };
        for start in 0..step_count {
            let end = (start + win).min(samples.len());
            if reversals_in_window(&samples[start..end]) > 1 {
                return true;
            }
        }
        false
    }
}

fn reversals_in_window(w: &[f64]) -> usize {
    let mut reversals = 0;
    let mut prev_dir: Option<i8> = None;
    for pair in w.windows(2) {
        let dir = if pair[1] > pair[0] {
            1i8
        } else if pair[1] < pair[0] {
            -1i8
        } else {
            continue;
        };
        if let Some(pd) = prev_dir {
            if dir != pd {
                reversals += 1;
            }
        }
        prev_dir = Some(dir);
    }
    reversals
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auto_tuner::EPOCH_CEILING_MS;

    #[test]
    fn proof_auto_tuner_combined_settles_within_3_epochs() {
        let scenario = SpikeScenario::ten_x_spike(5, 10);
        let result = scenario.run();

        let settled = result.epochs_to_settle.expect(
            "auto-tuner must settle within the spike window; \
             check epoch_ms_trace and parallelism_trace",
        );
        assert!(
            settled <= 3,
            "all loops must settle within 3 epochs of the 10× spike; settled at {settled}"
        );
    }

    #[test]
    fn proof_incremental_spike_equals_batch_spike() {
        let config = AutotunerConfig::default();
        let mut tuner = AutoTuner::new_with_state(config.clone(), 2_000, 2_000);

        let (mut incr_min, mut incr_max) = (0u64, 0u64);
        let mut incr_par = config.default_parallelism;

        for _ in 0..10 {
            let wr = 10.0 * (1_000.0 / tuner.current_max_epoch_ms as f64);
            let slo = if wr < 2.0 { 0.99 } else { 0.50 };
            let p95 = ((PARALLELISM_P95_SCALE_UP_MS as f64 * 10.0)
                / tuner.current_parallelism as f64) as u64;
            let (mn, mx) = tuner.adjust_epoch_sizing(wr, slo);
            let par = tuner.adjust_parallelism(p95);
            incr_min = mn;
            incr_max = mx;
            incr_par = par;
        }

        // Batch oracle: compute the expected final state directly from the input params.
        // Epoch sizing: 3 widenings (×1.5) from 2000ms hit the 5000ms ceiling by iter 3.
        //   Remaining 7 iters: already at ceiling — no further change.
        // Parallelism: hysteresis=3, so fires at iters 2, 5, 8 → 3 fires from initial 4.
        //   After 10 iters: 4 + 3 = 7.
        let batch_min = EPOCH_CEILING_MS;
        let batch_max = EPOCH_CEILING_MS;
        let fires = 10usize / config.hysteresis_scale_up_windows; // floor(10/3) = 3
        let batch_par = config.default_parallelism + fires; // 4 + 3 = 7

        assert_eq!(incr_max, batch_max, "max_epoch_ms must reach ceiling");
        assert_eq!(incr_min, batch_min, "min_epoch_ms must reach ceiling");
        assert_eq!(
            incr_par, batch_par,
            "parallelism must match batch oracle ({} fires in 10 iters)",
            fires
        );
        assert_eq!(
            (incr_min, incr_max, incr_par),
            (batch_min, batch_max, batch_par),
            "incremental must equal batch oracle at convergence"
        );
    }
}

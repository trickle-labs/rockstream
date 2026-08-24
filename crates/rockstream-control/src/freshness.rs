//! Bounded freshness control for the v0.59.9 runtime.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FreshnessObservation {
    pub source_lag_ms: u64,
    pub compute_ms: u64,
    pub queue_age_ms: u64,
    pub memory_bytes: u64,
    pub checkpoint_cost_ms: u64,
    pub compaction_debt_bytes: u64,
    pub object_store_latency_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessBounds {
    pub min_morsel_bytes: usize,
    pub max_morsel_bytes: usize,
    pub min_morsel_compute_ms: u64,
    pub max_morsel_compute_ms: u64,
    pub min_exchange_credit_bytes: usize,
    pub max_exchange_credit_bytes: usize,
    pub min_physical_group_epochs: usize,
    pub max_physical_group_epochs: usize,
    pub max_memory_bytes: u64,
    pub max_compaction_debt_bytes: u64,
}

impl Default for FreshnessBounds {
    fn default() -> Self {
        Self {
            min_morsel_bytes: 4 * 1024,
            max_morsel_bytes: 1024 * 1024,
            min_morsel_compute_ms: 1,
            max_morsel_compute_ms: 100,
            min_exchange_credit_bytes: 16 * 1024,
            max_exchange_credit_bytes: 4 * 1024 * 1024,
            min_physical_group_epochs: 1,
            max_physical_group_epochs: 64,
            max_memory_bytes: 1024 * 1024 * 1024,
            max_compaction_debt_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    Aligned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionMode {
    Normal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessAction {
    pub morsel_bytes: usize,
    pub morsel_compute_ms: u64,
    pub exchange_credit_bytes: usize,
    pub physical_group_epochs: usize,
    pub checkpoint_mode: CheckpointMode,
    pub admission: AdmissionMode,
}

pub struct FreshnessController {
    target_ms: u64,
    bounds: FreshnessBounds,
    action: FreshnessAction,
    calm_samples: u8,
}

impl FreshnessController {
    pub fn new(target_ms: u64, bounds: FreshnessBounds) -> Self {
        let bounds = normalize_bounds(bounds);
        Self {
            target_ms: target_ms.max(1),
            action: FreshnessAction {
                morsel_bytes: bounds.max_morsel_bytes,
                morsel_compute_ms: bounds.max_morsel_compute_ms,
                exchange_credit_bytes: bounds.max_exchange_credit_bytes,
                physical_group_epochs: bounds.max_physical_group_epochs,
                checkpoint_mode: CheckpointMode::Aligned,
                admission: AdmissionMode::Normal,
            },
            bounds,
            calm_samples: 0,
        }
    }

    pub fn bounds(&self) -> FreshnessBounds {
        self.bounds
    }

    pub fn action(&self) -> FreshnessAction {
        self.action
    }

    pub fn observe(&mut self, observation: FreshnessObservation) -> FreshnessAction {
        let pressure = observation.source_lag_ms > self.target_ms
            || observation
                .compute_ms
                .saturating_add(observation.queue_age_ms)
                > self.target_ms
            || observation.memory_bytes > self.bounds.max_memory_bytes
            || observation.checkpoint_cost_ms > self.target_ms / 2
            || observation.compaction_debt_bytes > self.bounds.max_compaction_debt_bytes
            || observation.object_store_latency_ms > self.target_ms / 2;
        let calm = observation.source_lag_ms <= self.target_ms / 2
            && observation
                .compute_ms
                .saturating_add(observation.queue_age_ms)
                <= self.target_ms / 2
            && observation.memory_bytes <= self.bounds.max_memory_bytes / 2
            && observation.checkpoint_cost_ms <= self.target_ms / 4
            && observation.compaction_debt_bytes <= self.bounds.max_compaction_debt_bytes / 2
            && observation.object_store_latency_ms <= self.target_ms / 4;

        if pressure {
            self.calm_samples = 0;
            self.action.morsel_bytes = (self.action.morsel_bytes / 2)
                .max(self.bounds.min_morsel_bytes)
                .min(self.bounds.max_morsel_bytes);
            self.action.morsel_compute_ms = (self.action.morsel_compute_ms / 2)
                .max(self.bounds.min_morsel_compute_ms)
                .min(self.bounds.max_morsel_compute_ms);
            self.action.exchange_credit_bytes = (self.action.exchange_credit_bytes / 2)
                .max(self.bounds.min_exchange_credit_bytes)
                .min(self.bounds.max_exchange_credit_bytes);
            self.action.physical_group_epochs = (self.action.physical_group_epochs / 2)
                .max(self.bounds.min_physical_group_epochs)
                .min(self.bounds.max_physical_group_epochs);
        } else if calm {
            self.calm_samples = self.calm_samples.saturating_add(1);
            if self.calm_samples >= 2 {
                self.action.morsel_bytes = (self.action.morsel_bytes.saturating_mul(5) / 4)
                    .max(self.bounds.min_morsel_bytes)
                    .min(self.bounds.max_morsel_bytes);
                self.action.morsel_compute_ms = (self.action.morsel_compute_ms * 5 / 4)
                    .max(self.bounds.min_morsel_compute_ms)
                    .min(self.bounds.max_morsel_compute_ms);
                self.action.exchange_credit_bytes =
                    (self.action.exchange_credit_bytes.saturating_mul(5) / 4)
                        .max(self.bounds.min_exchange_credit_bytes)
                        .min(self.bounds.max_exchange_credit_bytes);
                self.action.physical_group_epochs = (self.action.physical_group_epochs * 5 / 4)
                    .max(self.bounds.min_physical_group_epochs)
                    .min(self.bounds.max_physical_group_epochs);
                self.calm_samples = 0;
            }
        } else {
            self.calm_samples = 0;
        }
        self.action
    }
}

fn normalize_bounds(mut bounds: FreshnessBounds) -> FreshnessBounds {
    bounds.max_morsel_bytes = bounds.max_morsel_bytes.max(bounds.min_morsel_bytes);
    bounds.max_morsel_compute_ms = bounds
        .max_morsel_compute_ms
        .max(bounds.min_morsel_compute_ms);
    bounds.max_exchange_credit_bytes = bounds
        .max_exchange_credit_bytes
        .max(bounds.min_exchange_credit_bytes);
    bounds.max_physical_group_epochs = bounds
        .max_physical_group_epochs
        .max(bounds.min_physical_group_epochs);
    bounds
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_actions_stay_inside_declared_bounds() {
        let bounds = FreshnessBounds::default();
        let mut controller = FreshnessController::new(1_000, bounds);
        let action = controller.observe(FreshnessObservation {
            source_lag_ms: 2_000,
            compute_ms: 2_000,
            queue_age_ms: 2_000,
            memory_bytes: bounds.max_memory_bytes + 1,
            checkpoint_cost_ms: 600,
            compaction_debt_bytes: bounds.max_compaction_debt_bytes + 1,
            object_store_latency_ms: 600,
        });
        assert_eq!(action.morsel_bytes, bounds.max_morsel_bytes / 2);
        assert_eq!(action.morsel_compute_ms, bounds.max_morsel_compute_ms / 2);
        assert_eq!(
            action.exchange_credit_bytes,
            bounds.max_exchange_credit_bytes / 2
        );
        assert_eq!(
            action.physical_group_epochs,
            bounds.max_physical_group_epochs / 2
        );
    }

    #[test]
    fn calm_recovery_requires_two_samples_and_oscillation_does_not_grow_queue() {
        let bounds = FreshnessBounds::default();
        let mut controller = FreshnessController::new(1_000, bounds);
        let high = FreshnessObservation {
            source_lag_ms: 2_000,
            ..Default::default()
        };
        let calm = FreshnessObservation::default();
        let pressured = controller.observe(high);
        assert_eq!(controller.observe(calm), pressured);
        let recovered = controller.observe(calm);
        assert!(recovered.morsel_bytes > pressured.morsel_bytes);
        assert_eq!(
            controller.observe(high).morsel_bytes,
            recovered.morsel_bytes / 2
        );
    }
}

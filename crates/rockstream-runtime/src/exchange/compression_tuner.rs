use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rockstream_types::config::{AutotunerConfig, ExchangeConfig};
use rockstream_types::exchange::ShuffleCompression;
use rockstream_types::ids::ExchangeId;

#[derive(Debug, Clone, Copy, Default)]
struct CompressionState {
    disabled: bool,
    over_budget_windows: usize,
    under_budget_windows: usize,
}

#[derive(Clone)]
pub struct CompressionTuner {
    exchange_config: ExchangeConfig,
    autotuner: AutotunerConfig,
    states: Arc<Mutex<HashMap<ExchangeId, CompressionState>>>,
}

impl CompressionTuner {
    pub fn new(exchange_config: ExchangeConfig, autotuner: AutotunerConfig) -> Self {
        rockstream_types::metrics::set_shuffle_compression_state_entries(0);
        Self {
            exchange_config,
            autotuner,
            states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn decide(
        &self,
        exchange_id: ExchangeId,
        preferred: ShuffleCompression,
        compression_cpu_ms: u64,
    ) -> ShuffleCompression {
        if !matches!(preferred, ShuffleCompression::Lz4) {
            return preferred;
        }
        let mut states = self.states.lock().unwrap();
        if !states.contains_key(&exchange_id)
            && states.len() >= self.exchange_config.max_exchange_compression_states
        {
            rockstream_types::metrics::inc_shuffle_compression_disabled_total();
            rockstream_types::metrics::set_shuffle_compression_state_entries(states.len() as u64);
            return ShuffleCompression::None;
        }
        let state = states.entry(exchange_id).or_default();
        let over_budget = compression_cpu_ms > self.autotuner.direct_compression_cpu_budget_ms;
        if over_budget {
            state.over_budget_windows = state.over_budget_windows.saturating_add(1);
            state.under_budget_windows = 0;
            if state.over_budget_windows >= self.autotuner.compression_disable_hysteresis_windows {
                state.disabled = true;
            }
        } else {
            state.under_budget_windows = state.under_budget_windows.saturating_add(1);
            state.over_budget_windows = 0;
            if state.disabled
                && state.under_budget_windows
                    >= self.autotuner.compression_reenable_hysteresis_windows
            {
                state.disabled = false;
            }
        }
        let decision = if state.disabled {
            rockstream_types::metrics::inc_shuffle_compression_disabled_total();
            ShuffleCompression::None
        } else {
            preferred
        };
        rockstream_types::metrics::set_shuffle_compression_state_entries(states.len() as u64);
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configs() -> (ExchangeConfig, AutotunerConfig) {
        let exchange = ExchangeConfig {
            max_exchange_compression_states: 1,
            ..ExchangeConfig::default()
        };
        let autotuner = AutotunerConfig {
            direct_compression_cpu_budget_ms: 5,
            compression_disable_hysteresis_windows: 2,
            compression_reenable_hysteresis_windows: 3,
            ..AutotunerConfig::default()
        };
        (exchange, autotuner)
    }

    #[test]
    fn compression_tuner_disables_lz4_when_cpu_budget_exceeded() {
        let (exchange, autotuner) = configs();
        let tuner = CompressionTuner::new(exchange, autotuner);
        assert_eq!(
            tuner.decide(ExchangeId(1), ShuffleCompression::Lz4, 9),
            ShuffleCompression::Lz4
        );
        assert_eq!(
            tuner.decide(ExchangeId(1), ShuffleCompression::Lz4, 9),
            ShuffleCompression::None
        );
        assert!(
            tuner
                .states
                .lock()
                .unwrap()
                .get(&ExchangeId(1))
                .unwrap()
                .disabled
        );
    }

    #[test]
    fn compression_tuner_holds_disable_until_hysteresis_clears() {
        let (exchange, autotuner) = configs();
        let tuner = CompressionTuner::new(exchange, autotuner);
        tuner.decide(ExchangeId(7), ShuffleCompression::Lz4, 9);
        tuner.decide(ExchangeId(7), ShuffleCompression::Lz4, 9);
        assert_eq!(
            tuner.decide(ExchangeId(7), ShuffleCompression::Lz4, 1),
            ShuffleCompression::None
        );
        assert_eq!(
            tuner.decide(ExchangeId(7), ShuffleCompression::Lz4, 1),
            ShuffleCompression::None
        );
        assert_eq!(
            tuner.decide(ExchangeId(7), ShuffleCompression::Lz4, 1),
            ShuffleCompression::Lz4
        );
    }

    #[test]
    fn compression_state_map_never_exceeds_bound() {
        let (exchange, autotuner) = configs();
        let tuner = CompressionTuner::new(exchange, autotuner);
        assert_eq!(
            tuner.decide(ExchangeId(1), ShuffleCompression::Lz4, 1),
            ShuffleCompression::Lz4
        );
        assert_eq!(
            tuner.decide(ExchangeId(2), ShuffleCompression::Lz4, 1),
            ShuffleCompression::None
        );
        assert_eq!(tuner.states.lock().unwrap().len(), 1);
    }
}

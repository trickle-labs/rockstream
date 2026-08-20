//! Bounded delta-amplification accounting for factorized operators.

use std::sync::{Arc, Mutex};

pub const FACTORIZED_SELECTION_RULE_VERSION: u32 = 1;

pub const DEFAULT_FACTORIZED_DELTA_BUDGET: DeltaAmplificationBudget = DeltaAmplificationBudget {
    max_input_deltas: 1_000_000,
    max_probes: 1_000_000,
    max_shuffled_bytes: 8 * 1024 * 1024,
    max_intermediate_tuples: 1_000_000,
    max_output_deltas: 1_000_000,
    max_state_writes: 1_000_000,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeltaAmplificationCounters {
    pub input_deltas: u64,
    pub probes: u64,
    pub shuffled_bytes: u64,
    pub intermediate_tuples: u64,
    pub output_deltas: u64,
    pub state_writes: u64,
}

impl DeltaAmplificationCounters {
    pub const fn get(self, dimension: AmplificationDimension) -> u64 {
        match dimension {
            AmplificationDimension::InputDeltas => self.input_deltas,
            AmplificationDimension::Probes => self.probes,
            AmplificationDimension::ShuffledBytes => self.shuffled_bytes,
            AmplificationDimension::IntermediateTuples => self.intermediate_tuples,
            AmplificationDimension::OutputDeltas => self.output_deltas,
            AmplificationDimension::StateWrites => self.state_writes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaAmplificationBudget {
    pub max_input_deltas: u64,
    pub max_probes: u64,
    pub max_shuffled_bytes: u64,
    pub max_intermediate_tuples: u64,
    pub max_output_deltas: u64,
    pub max_state_writes: u64,
}

impl Default for DeltaAmplificationBudget {
    fn default() -> Self {
        Self {
            max_input_deltas: u64::MAX,
            max_probes: u64::MAX,
            max_shuffled_bytes: u64::MAX,
            max_intermediate_tuples: u64::MAX,
            max_output_deltas: u64::MAX,
            max_state_writes: u64::MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmplificationDimension {
    InputDeltas,
    Probes,
    ShuffledBytes,
    IntermediateTuples,
    OutputDeltas,
    StateWrites,
}

impl AmplificationDimension {
    pub const fn name(self) -> &'static str {
        match self {
            Self::InputDeltas => "input_deltas",
            Self::Probes => "probes",
            Self::ShuffledBytes => "shuffled_bytes",
            Self::IntermediateTuples => "intermediate_tuples",
            Self::OutputDeltas => "output_deltas",
            Self::StateWrites => "state_writes",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanStrategy {
    Classic,
    Factorized,
}

#[derive(Debug, Clone)]
pub struct DeltaAmplificationGovernor {
    budget: DeltaAmplificationBudget,
    counters: Arc<Mutex<DeltaAmplificationCounters>>,
}

impl DeltaAmplificationGovernor {
    pub fn new(budget: DeltaAmplificationBudget) -> Self {
        Self {
            budget,
            counters: std::sync::Arc::new(Mutex::new(DeltaAmplificationCounters::default())),
        }
    }

    pub fn budget(&self) -> DeltaAmplificationBudget {
        self.budget
    }

    pub fn counters(&self) -> DeltaAmplificationCounters {
        *self.counters.lock().unwrap()
    }

    pub fn projected(&self, next: DeltaAmplificationCounters) -> DeltaAmplificationCounters {
        let current = self.counters();
        DeltaAmplificationCounters {
            input_deltas: current.input_deltas.saturating_add(next.input_deltas),
            probes: current.probes.saturating_add(next.probes),
            shuffled_bytes: current.shuffled_bytes.saturating_add(next.shuffled_bytes),
            intermediate_tuples: current
                .intermediate_tuples
                .saturating_add(next.intermediate_tuples),
            output_deltas: current.output_deltas.saturating_add(next.output_deltas),
            state_writes: current.state_writes.saturating_add(next.state_writes),
        }
    }

    pub const fn limit(&self, dimension: AmplificationDimension) -> u64 {
        match dimension {
            AmplificationDimension::InputDeltas => self.budget.max_input_deltas,
            AmplificationDimension::Probes => self.budget.max_probes,
            AmplificationDimension::ShuffledBytes => self.budget.max_shuffled_bytes,
            AmplificationDimension::IntermediateTuples => self.budget.max_intermediate_tuples,
            AmplificationDimension::OutputDeltas => self.budget.max_output_deltas,
            AmplificationDimension::StateWrites => self.budget.max_state_writes,
        }
    }

    pub fn exceeded(&self, next: DeltaAmplificationCounters) -> Option<AmplificationDimension> {
        let total = self.projected(next);
        [
            (
                AmplificationDimension::InputDeltas,
                total.input_deltas,
                self.budget.max_input_deltas,
            ),
            (
                AmplificationDimension::Probes,
                total.probes,
                self.budget.max_probes,
            ),
            (
                AmplificationDimension::ShuffledBytes,
                total.shuffled_bytes,
                self.budget.max_shuffled_bytes,
            ),
            (
                AmplificationDimension::IntermediateTuples,
                total.intermediate_tuples,
                self.budget.max_intermediate_tuples,
            ),
            (
                AmplificationDimension::OutputDeltas,
                total.output_deltas,
                self.budget.max_output_deltas,
            ),
            (
                AmplificationDimension::StateWrites,
                total.state_writes,
                self.budget.max_state_writes,
            ),
        ]
        .into_iter()
        .find_map(|(dimension, value, limit)| (value > limit).then_some(dimension))
    }

    pub fn record(&self, delta: DeltaAmplificationCounters) {
        let mut counters = self.counters.lock().unwrap();
        counters.input_deltas = counters.input_deltas.saturating_add(delta.input_deltas);
        counters.probes = counters.probes.saturating_add(delta.probes);
        counters.shuffled_bytes = counters.shuffled_bytes.saturating_add(delta.shuffled_bytes);
        counters.intermediate_tuples = counters
            .intermediate_tuples
            .saturating_add(delta.intermediate_tuples);
        counters.output_deltas = counters.output_deltas.saturating_add(delta.output_deltas);
        counters.state_writes = counters.state_writes.saturating_add(delta.state_writes);
    }

    pub fn restore(&self, counters: DeltaAmplificationCounters) {
        *self.counters.lock().unwrap() = counters;
    }

    pub fn select(
        estimate: DeltaAmplificationCounters,
        budget: DeltaAmplificationBudget,
    ) -> PlanStrategy {
        let governor = Self::new(budget);
        if governor.exceeded(estimate).is_none() {
            PlanStrategy::Factorized
        } else {
            PlanStrategy::Classic
        }
    }
}

//! Typed scenario DSL (`TST-001`).
//!
//! A [`Scenario`] is a named sequence of [`ScenarioStep`]s together with the
//! [`ExpectedTranscript`] a correct [`crate::scenario::driver::ScenarioDriver`]
//! run must produce. v0.59.17 scope only needs a single step kind — executing
//! one SQL statement and observing its result rows — since that is all
//! slices 1-4 (DSL, drivers, transcript) require to prove driver agreement.
//! Differential/metamorphic corpora (slices 7-8, out of scope this phase)
//! reuse this same DSL rather than a parallel one.

use serde::{Deserialize, Serialize};

use crate::scenario::transcript::ScenarioTranscript;

/// One step of a [`Scenario`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScenarioStep {
    /// Execute one SQL statement via the simple query protocol.
    ExecuteSql(String),
}

/// The transcript a correct driver run of a [`Scenario`] is expected to
/// produce. A distinct newtype (not a bare `ScenarioTranscript`) so a
/// scenario's expectation and a driver's observation are never accidentally
/// interchanged at a call site.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedTranscript(pub ScenarioTranscript);

/// A named, typed scenario: a sequence of steps plus the transcript a
/// correct run must produce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub steps: Vec<ScenarioStep>,
    pub expected: ExpectedTranscript,
}

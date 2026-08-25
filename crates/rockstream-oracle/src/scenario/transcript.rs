//! Typed scenario transcripts (`TST-003`).
//!
//! A [`ScenarioTranscript`] is the ordered sequence of typed events a
//! [`crate::scenario::driver::ScenarioDriver`] observed while running a
//! [`crate::scenario::dsl::Scenario`]. It is a distinct, additional typed
//! artifact and must **not** be conflated with the pre-existing meaning of
//! "transcript" in this repository: `docs/adr/0003-transcript-ownership.md`
//! fixes "transcript" to mean a checked-in documentation command-transcript
//! owned by its executable doc test. A `ScenarioTranscript` is never checked
//! in as documentation and is never read by the doc-transcript machinery.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Upper bound on the number of events a single [`ScenarioTranscript`] may
/// hold. Unbounded in-memory accumulation is never acceptable; a scenario
/// that would exceed this must fail explicitly via [`push_event`], not
/// truncate silently.
pub const MAX_TRANSCRIPT_EVENTS: usize = 10_000;

/// One observed event: the rows produced by executing one [`ScenarioStep`](crate::scenario::dsl::ScenarioStep).
///
/// Cell values are captured as their wire-text representation (as returned
/// by the simple query protocol), which is what makes transcripts from
/// different drivers (in-process, pgwire-process, Docker) directly comparable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioEvent {
    /// Index of the [`ScenarioStep`](crate::scenario::dsl::ScenarioStep) that produced this event.
    pub step_index: usize,
    /// Rows returned by that step, each row a list of text cell values.
    pub rows: Vec<Vec<String>>,
}

/// A bounded, ordered sequence of [`ScenarioEvent`]s with deterministic
/// serialization and an exact equality/diff API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ScenarioTranscript {
    events: Vec<ScenarioEvent>,
}

/// Returned by [`ScenarioTranscript::push_event`] when appending would exceed
/// [`MAX_TRANSCRIPT_EVENTS`]. The transcript is left unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TranscriptCapacityExceeded {
    pub attempted: usize,
    pub max: usize,
}

impl fmt::Display for TranscriptCapacityExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "scenario transcript would grow to {} events, exceeding the bound of {}",
            self.attempted, self.max
        )
    }
}

impl std::error::Error for TranscriptCapacityExceeded {}

/// One mismatched position between two transcripts. `expected`/`actual` are
/// `None` when the corresponding transcript is shorter than the other at
/// this index, so a length mismatch is reported the same way as a value
/// mismatch rather than as a separate, less-informative case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptMismatch {
    pub index: usize,
    pub expected: Option<ScenarioEvent>,
    pub actual: Option<ScenarioEvent>,
}

impl ScenarioTranscript {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an event, failing closed if it would exceed [`MAX_TRANSCRIPT_EVENTS`].
    pub fn push_event(&mut self, event: ScenarioEvent) -> Result<(), TranscriptCapacityExceeded> {
        if self.events.len() >= MAX_TRANSCRIPT_EVENTS {
            return Err(TranscriptCapacityExceeded {
                attempted: self.events.len() + 1,
                max: MAX_TRANSCRIPT_EVENTS,
            });
        }
        self.events.push(event);
        Ok(())
    }

    pub fn events(&self) -> &[ScenarioEvent] {
        &self.events
    }

    /// Compare two transcripts event-by-event, returning every index at
    /// which they disagree (value or presence), never a boolean.
    pub fn diff(&self, other: &Self) -> Vec<TranscriptMismatch> {
        let len = self.events.len().max(other.events.len());
        (0..len)
            .filter_map(|i| {
                let expected = self.events.get(i).cloned();
                let actual = other.events.get(i).cloned();
                if expected == actual {
                    None
                } else {
                    Some(TranscriptMismatch {
                        index: i,
                        expected,
                        actual,
                    })
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_transcript_is_empty() {
        assert!(ScenarioTranscript::new().events().is_empty());
    }

    #[test]
    fn push_event_appends_in_order() {
        let mut t = ScenarioTranscript::new();
        t.push_event(ScenarioEvent {
            step_index: 0,
            rows: vec![],
        })
        .unwrap();
        t.push_event(ScenarioEvent {
            step_index: 1,
            rows: vec![],
        })
        .unwrap();
        assert_eq!(t.events().len(), 2);
        assert_eq!(t.events()[1].step_index, 1);
    }
}

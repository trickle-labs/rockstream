//! v0.59.17 Slice 4 — `ScenarioTranscript` diff proof.

use rockstream_oracle::scenario::transcript::{ScenarioEvent, ScenarioTranscript};

fn event(step_index: usize, rows: &[&[&str]]) -> ScenarioEvent {
    ScenarioEvent {
        step_index,
        rows: rows
            .iter()
            .map(|row| row.iter().map(|c| c.to_string()).collect())
            .collect(),
    }
}

#[test]
fn transcript_diff_reports_exact_mismatched_events() {
    let mut expected = ScenarioTranscript::new();
    expected.push_event(event(0, &[&["1"]])).unwrap();
    expected.push_event(event(1, &[&["42"]])).unwrap();
    expected.push_event(event(2, &[&["ok"]])).unwrap();

    let mut actual = ScenarioTranscript::new();
    actual.push_event(event(0, &[&["1"]])).unwrap();
    actual.push_event(event(1, &[&["99"]])).unwrap(); // mismatched event
    actual.push_event(event(2, &[&["ok"]])).unwrap();

    let mismatches = expected.diff(&actual);
    assert_eq!(mismatches.len(), 1, "exactly one event differs");
    let m = &mismatches[0];
    assert_eq!(m.index, 1);
    assert_eq!(m.expected.as_ref().unwrap(), &event(1, &[&["42"]]));
    assert_eq!(m.actual.as_ref().unwrap(), &event(1, &[&["99"]]));
}

#[test]
fn transcript_diff_of_identical_transcripts_is_empty() {
    let mut a = ScenarioTranscript::new();
    a.push_event(event(0, &[&["1"]])).unwrap();
    let mut b = ScenarioTranscript::new();
    b.push_event(event(0, &[&["1"]])).unwrap();
    assert!(a.diff(&b).is_empty());
}

#[test]
fn transcript_diff_reports_length_mismatch_as_missing_event() {
    let mut expected = ScenarioTranscript::new();
    expected.push_event(event(0, &[&["1"]])).unwrap();
    let actual = ScenarioTranscript::new();

    let mismatches = expected.diff(&actual);
    assert_eq!(mismatches.len(), 1);
    assert_eq!(mismatches[0].index, 0);
    assert!(mismatches[0].expected.is_some());
    assert!(mismatches[0].actual.is_none());
}

#[test]
fn transcript_push_event_rejects_past_capacity() {
    let mut t = ScenarioTranscript::new();
    for i in 0..rockstream_oracle::scenario::transcript::MAX_TRANSCRIPT_EVENTS {
        t.push_event(event(i, &[&["x"]])).unwrap();
    }
    let err = t.push_event(event(999_999, &[&["x"]])).unwrap_err();
    assert_eq!(
        err.max,
        rockstream_oracle::scenario::transcript::MAX_TRANSCRIPT_EVENTS
    );
}

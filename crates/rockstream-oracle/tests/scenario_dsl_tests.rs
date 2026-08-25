//! v0.59.17 Slice 1 — typed scenario DSL round-trip proof.

use rockstream_oracle::scenario::dsl::{ExpectedTranscript, Scenario, ScenarioStep};
use rockstream_oracle::scenario::transcript::{ScenarioEvent, ScenarioTranscript};

#[test]
fn scenario_round_trips_through_serialization() {
    let mut expected = ScenarioTranscript::new();
    expected
        .push_event(ScenarioEvent {
            step_index: 0,
            rows: vec![vec!["1".to_string()]],
        })
        .unwrap();

    let scenario = Scenario {
        name: "select_one".to_string(),
        steps: vec![ScenarioStep::ExecuteSql("SELECT 1".to_string())],
        expected: ExpectedTranscript(expected),
    };

    let json = serde_json::to_string(&scenario).expect("serialize");
    let round_tripped: Scenario = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(scenario, round_tripped);
}

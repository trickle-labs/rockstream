//! v0.59.17 Slice 9 — reproducibility artifacts.
//!
//! Proves that a scenario/transcript mismatch produced by the differential
//! machinery in `crates/rockstream-oracle/src/scenario/` can be captured as
//! a minimized, replayable `rockstream_sim::ScenarioMismatchArtifact` (the
//! same artifact machinery `rockstream-sim/src/soak.rs` extends alongside
//! its existing `SimRuntime` fault-seed regressions), and that the artifact
//! (a) replays to the exact same mismatch and (b) is minimal: dropping the
//! last event makes the mismatch disappear.

use rockstream_oracle::scenario::driver::{InProcessDriver, ScenarioDriver};
use rockstream_oracle::scenario::dsl::{ExpectedTranscript, Scenario, ScenarioStep};
use rockstream_oracle::scenario::transcript::{
    ScenarioEvent, ScenarioTranscript, TranscriptMismatch,
};
use rockstream_sim::{ArtifactEvent, ArtifactMismatch, ScenarioMismatchArtifact};

fn to_artifact_event(e: &ScenarioEvent) -> ArtifactEvent {
    ArtifactEvent {
        step_index: e.step_index,
        rows: e.rows.clone(),
    }
}

fn to_artifact_mismatch(m: &TranscriptMismatch) -> ArtifactMismatch {
    ArtifactMismatch {
        index: m.index,
        expected: m.expected.as_ref().map(to_artifact_event),
        actual: m.actual.as_ref().map(to_artifact_event),
    }
}

fn transcript_prefix(t: &ScenarioTranscript, len: usize) -> ScenarioTranscript {
    let mut out = ScenarioTranscript::new();
    for e in t.events().iter().take(len) {
        out.push_event(e.clone()).unwrap();
    }
    out
}

fn scenario_of(name: &str, sqls: &[&str]) -> Scenario {
    Scenario {
        name: name.to_string(),
        steps: sqls
            .iter()
            .map(|s| ScenarioStep::ExecuteSql(s.to_string()))
            .collect(),
        expected: ExpectedTranscript(ScenarioTranscript::new()),
    }
}

/// Shared shape for both tests: run a scenario, deliberately corrupt one
/// event to create a single mismatch, build a minimized
/// `ScenarioMismatchArtifact` for it, then prove replay + minimality.
async fn assert_mismatch_artifact_is_minimized_and_reproducible(scenario_name: &str) {
    let sqls = ["SELECT 1", "SELECT 2", "SELECT 3"];
    let scenario = scenario_of(scenario_name, &sqls);
    let driver = InProcessDriver;

    let actual = driver.run(&scenario).await.expect("scenario run");
    assert_eq!(actual.events().len(), sqls.len());

    // A deliberately wrong "expected" transcript: identical to `actual`
    // except the row at event index 1 is corrupted.
    let mut wrong_events: Vec<ScenarioEvent> = actual.events().to_vec();
    wrong_events[1].rows = vec![vec!["999999".to_string()]];
    let mut wrong_expected = ScenarioTranscript::new();
    for e in &wrong_events {
        wrong_expected.push_event(e.clone()).unwrap();
    }

    let mismatches = wrong_expected.diff(&actual);
    assert_eq!(mismatches.len(), 1, "expected exactly one mismatch");
    let mismatch = &mismatches[0];
    assert_eq!(mismatch.index, 1);

    // Minimized reproducer: only the SQL steps up to and including the
    // mismatched event.
    let repro_len = mismatch.index + 1;
    let repro_steps: Vec<String> = sqls.iter().take(repro_len).map(|s| s.to_string()).collect();
    let artifact = ScenarioMismatchArtifact::new(
        scenario_name.to_string(),
        repro_steps.clone(),
        to_artifact_mismatch(mismatch),
    )
    .expect("artifact within bound");

    // (a) Replaying the artifact's steps against the same wrong-expected
    // prefix reproduces the exact same mismatch (same index, same
    // expected/actual values).
    let repro_scenario = scenario_of(
        &artifact.scenario_name,
        &artifact
            .steps
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );
    let repro_actual = driver.run(&repro_scenario).await.expect("replay run");
    let repro_expected = transcript_prefix(&wrong_expected, artifact.steps.len());
    let repro_mismatches = repro_expected.diff(&repro_actual);
    assert_eq!(
        repro_mismatches.len(),
        1,
        "replay must reproduce exactly one mismatch"
    );
    assert_eq!(
        to_artifact_mismatch(&repro_mismatches[0]),
        artifact.mismatch,
        "replay must reproduce the exact same mismatch as the artifact"
    );

    // (b) Minimality: dropping the last event (the mismatched one) leaves
    // no mismatch at all.
    let shorter_steps: Vec<&str> = sqls.iter().take(repro_len - 1).copied().collect();
    let shorter_scenario = scenario_of(&artifact.scenario_name, &shorter_steps);
    let shorter_actual = driver.run(&shorter_scenario).await.expect("shorter run");
    let shorter_expected = transcript_prefix(&wrong_expected, repro_len - 1);
    let shorter_mismatches = shorter_expected.diff(&shorter_actual);
    assert!(
        shorter_mismatches.is_empty(),
        "removing the mismatched event must remove the mismatch"
    );
}

#[tokio::test]
async fn differential_mismatch_produces_minimized_artifact() {
    assert_mismatch_artifact_is_minimized_and_reproducible("differential_smoke").await;
}

#[tokio::test]
async fn connector_mismatch_produces_minimized_artifact() {
    // Scoped/labeled as a connector scenario (Kafka sink), proving the same
    // reproducibility-artifact machinery works when the scenario/mismatch
    // data is tagged as connector-sourced. No real Kafka/Postgres connector
    // is exercised here — that is covered separately.
    assert_mismatch_artifact_is_minimized_and_reproducible("kafka_sink_smoke").await;
}

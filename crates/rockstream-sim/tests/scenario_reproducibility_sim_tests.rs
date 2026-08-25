//! v0.59.17 Slice 9 — reproducibility artifacts, `SimRuntime` side.
//!
//! Proves that the `v05917.scenario.mismatch_injection` buggify fault point
//! (registered in `crates/rockstream-sim/src/fault_model.rs`) produces the
//! exact same `ScenarioMismatchArtifact` when the same seed is replayed
//! twice, matching the idiom used by
//! `crates/rockstream-sim/tests/v05911_sql_ergonomics_sim_tests.rs`
//! (`SimRuntime::new(seed)` + `buggify_init(seed)` + `buggify!()`).

use rockstream_sim::{
    buggify, register_scenario_faults, ArtifactEvent, ArtifactMismatch, ScenarioMismatchArtifact,
    SimRuntime,
};

/// Runs one deterministic seed through the `v05917.scenario.mismatch_injection`
/// fault point and returns the minimized artifact it produces.
fn run_seed(seed: u64) -> ScenarioMismatchArtifact {
    register_scenario_faults();
    rockstream_sim::buggify::buggify_init(seed);

    let rt = SimRuntime::new(seed);
    // Under the `simulation` feature this deterministically returns `true`
    // (probability 1.0); in a plain build `buggify!` is a compiled-out
    // no-op that always returns `false`. Either way the same seed must
    // replay to the same boolean, so it is safe to fold into the artifact.
    let injected = buggify!("v05917.scenario.mismatch_injection", 1.0);

    // Seed-derived "actual" value standing in for a corrupted transcript
    // row — deterministic because it is drawn from `SimRuntime`'s seeded
    // RNG, not from wall-clock or ambient state.
    let corrupted = rt.random_u64() % 1_000;

    rockstream_sim::buggify::buggify_disable();

    let expected_event = ArtifactEvent {
        step_index: 0,
        rows: vec![vec!["1".to_string()]],
    };
    let actual_event = ArtifactEvent {
        step_index: 0,
        rows: vec![vec![corrupted.to_string(), injected.to_string()]],
    };
    let mismatch = ArtifactMismatch {
        index: 0,
        expected: Some(expected_event),
        actual: Some(actual_event),
    };

    ScenarioMismatchArtifact::new(
        "v05917_scenario_mismatch".to_string(),
        vec!["SELECT 1".to_string()],
        mismatch,
    )
    .expect("artifact within MAX_ARTIFACT_STEPS bound")
}

#[test]
fn buggify_scenario_mismatch_replays_deterministically() {
    let seed = 0x5917_5917_5917_5917;

    let first = run_seed(seed);
    let second = run_seed(seed);

    assert_eq!(
        first, second,
        "the same seed must produce the byte-for-byte identical artifact both times"
    );
}

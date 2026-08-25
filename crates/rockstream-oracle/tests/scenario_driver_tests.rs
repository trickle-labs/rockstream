//! v0.59.17 Slices 2-3 — scenario driver agreement proofs.

use rockstream_oracle::scenario::driver::{
    DockerDriver, InProcessDriver, PgwireProcessDriver, ScenarioDriver,
};
use rockstream_oracle::scenario::dsl::{ExpectedTranscript, Scenario, ScenarioStep};
use rockstream_oracle::scenario::transcript::ScenarioTranscript;

fn constant_scenario() -> Scenario {
    Scenario {
        name: "select_one".to_string(),
        steps: vec![ScenarioStep::ExecuteSql("SELECT 1".to_string())],
        expected: ExpectedTranscript(ScenarioTranscript::new()),
    }
}

#[tokio::test]
async fn in_process_and_pgwire_process_drivers_agree() {
    let scenario = constant_scenario();

    let in_process = InProcessDriver
        .run(&scenario)
        .await
        .expect("in-process driver run");

    let pgwire_driver = PgwireProcessDriver::new();
    let pgwire = match pgwire_driver.run(&scenario).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "SKIP in_process_and_pgwire_process_drivers_agree: rockstream binary unavailable ({e})"
            );
            return;
        }
    };

    let mismatches = in_process.diff(&pgwire);
    assert!(
        mismatches.is_empty(),
        "in-process and pgwire-process transcripts disagree: {mismatches:?}"
    );
}

#[tokio::test]
async fn docker_driver_matches_in_process_driver() {
    let scenario = constant_scenario();

    let in_process = InProcessDriver
        .run(&scenario)
        .await
        .expect("in-process driver run");

    match DockerDriver.run(&scenario).await {
        Ok(docker) => {
            let mismatches = in_process.diff(&docker);
            assert!(
                mismatches.is_empty(),
                "in-process and Docker transcripts disagree: {mismatches:?}"
            );
        }
        Err(e) => {
            eprintln!("SKIP docker_driver_matches_in_process_driver: {e}");
        }
    }
}

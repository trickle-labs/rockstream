//! v0.72 epoch-window bounded adaptation regression tests (issue #134).
use rockstream_runtime::epoch_compaction::{
    EpochAdaptationSignals, EpochCompactionConfig, EpochCompactionError, EpochCompactor,
    EpochWindowDecision,
};
use rockstream_types::data_plane::RuntimeRow;
use std::time::{Duration, Instant};

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn config() -> EpochCompactionConfig {
    EpochCompactionConfig::default()
        .with_bounds(ms(10), ms(50))
        .with_window_duration_clamped(ms(25))
}

fn signals(ingest: u64, commit_ms: u64, pressure: u8, freshness_ms: u64) -> EpochAdaptationSignals {
    EpochAdaptationSignals {
        ingest_rows_per_second: ingest,
        durable_commit_latency: ms(commit_ms),
        state_pressure_percent: pressure,
        freshness_target: ms(freshness_ms),
    }
}

#[test]
fn v072_rejects_zero_reversed_and_infeasible_initial_window_bounds() {
    assert_eq!(
        EpochCompactor::try_new(EpochCompactionConfig::default().with_bounds(ms(50), ms(10)))
            .unwrap_err(),
        EpochCompactionError::InvalidBounds {
            minimum: ms(50),
            maximum: ms(10),
        }
    );
    assert_eq!(
        EpochCompactor::try_new(EpochCompactionConfig::default().with_bounds(ms(0), ms(50)))
            .unwrap_err(),
        EpochCompactionError::ZeroMinimum
    );
    assert_eq!(
        EpochCompactor::try_new(EpochCompactionConfig::default().with_bounds(ms(10), ms(20)))
            .unwrap_err(),
        EpochCompactionError::WindowDurationAboveMaximum {
            actual: ms(100),
            maximum: ms(20),
        }
    );
    assert_eq!(config().validate(), Ok(()));
}

#[test]
fn v072_four_signals_choose_explicit_bounded_windows() {
    let config = config();
    let expected = [
        (signals(0, 5, 10, 100), ms(10), true),
        (signals(1500, 5, 10, 100), ms(30), true),
        (signals(20_000, 5, 10, 100), ms(50), true),
        (signals(20_000, 80, 10, 100), ms(10), true),
        (signals(20_000, 5, 95, 100), ms(10), true),
        (signals(20_000, 5, 10, 25), ms(20), true),
        (signals(20_000, 5, 10, 5), ms(10), false),
    ];
    for (input, window, feasible) in expected {
        assert_eq!(
            config.choose_adaptive_window(input),
            Ok(EpochWindowDecision {
                selected_window: window,
                reserved_commit_time: input.durable_commit_latency,
                freshness_feasible: feasible,
            })
        );
    }
    assert_eq!(
        config.choose_adaptive_window(signals(20000, 5, 101, 100)),
        Err(EpochCompactionError::InvalidPressure(101))
    );
}

#[test]
fn v072_live_adaptation_changes_expiration_but_not_epoch_membership_or_result() {
    let compactor = EpochCompactor::try_new(config()).unwrap();
    let opened = Instant::now();
    compactor.push_row_at(
        7,
        RuntimeRow {
            values_tsv: "key\tvalue".to_string(),
            weight: 1,
        },
        opened,
    );
    let copy = compactor.clone();
    assert_eq!(compactor.current_window_duration(), ms(25));
    assert!(compactor.is_epoch_expired_at(7, opened + ms(30)));
    assert_eq!(
        compactor.adapt_window(signals(20000, 5, 10, 100)),
        Ok(EpochWindowDecision {
            selected_window: ms(50),
            reserved_commit_time: ms(5),
            freshness_feasible: true,
        })
    );
    assert_eq!(copy.current_window_duration(), ms(50));
    assert!(!compactor.is_epoch_expired_at(7, opened + ms(30)));
    assert!(compactor.is_epoch_expired_at(7, opened + ms(50)));
    assert_eq!(compactor.active_epochs(), vec![7]);
    assert_eq!(
        compactor.drain_epoch(7),
        Some(vec![RuntimeRow {
            values_tsv: "key\tvalue".to_string(),
            weight: 1
        }])
    );
    assert_eq!(compactor.active_epochs(), Vec::<u64>::new());
}

#[test]
fn v072_infeasible_freshness_is_reported_and_keeps_safe_minimum() {
    let compactor = EpochCompactor::try_new(config()).unwrap();
    assert_eq!(
        compactor.adapt_window(signals(100_000, 200, 20, 100)),
        Ok(EpochWindowDecision {
            selected_window: ms(10),
            reserved_commit_time: ms(200),
            freshness_feasible: false,
        })
    );
    assert_eq!(compactor.current_window_duration(), ms(10));
}

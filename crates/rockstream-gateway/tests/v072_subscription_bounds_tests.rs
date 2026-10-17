//! v0.72 producer-side subscription boundedness and complete-epoch regressions.
use bytes::Bytes;
use rockstream_gateway::change_log::{
    ChangeEntry, ChangeLogError, ViewChangeLog, CHANGE_LOG_MAX_EPOCH_BYTES,
};
use rockstream_gateway::subscribe_handler::{
    deliver_snapshot_checked, run_subscribe, start_from_epoch, SubscribeError, SubscribeRegistry,
    SubscriberHandle, SUBSCRIBE_BACKLOG_MAX, SUBSCRIBE_MAX_STREAMS, SUBSCRIBE_SNAPSHOT_MAX_ROWS,
};
use rockstream_gateway::subscribe_parser::parse_subscribe;
use std::sync::Arc;

fn row(epoch: u64, key: &str, diff: i8, value: &str) -> ChangeEntry {
    ChangeEntry {
        epoch,
        row_key: Bytes::copy_from_slice(key.as_bytes()),
        mz_diff: diff,
        encoded_row: Bytes::copy_from_slice(value.as_bytes()),
    }
}

#[test]
fn complete_epoch_is_visible_with_all_retractions_duplicates_and_inserts() {
    let registry = SubscribeRegistry::new();
    registry
        .push_epoch(
            "v",
            vec![
                row(4, "same", -1, "before"),
                row(4, "same", 1, "after"),
                row(4, "another", 1, "after"),
            ],
        )
        .unwrap();
    let request = parse_subscribe("SUBSCRIBE v").unwrap();
    let mut subscriber = SubscriberHandle::new("v".to_string(), 3, request, vec![]);
    let actual = subscriber
        .poll(&registry)
        .unwrap()
        .into_iter()
        .map(|row| (row.mz_timestamp, row.mz_diff, row.encoded_row))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (4, -1, Bytes::from_static(b"before")),
            (4, 1, Bytes::from_static(b"after")),
            (4, 1, Bytes::from_static(b"after")),
        ]
    );
    assert_eq!(subscriber.last_sent_epoch, 4);
    assert!(subscriber.poll(&registry).unwrap().is_empty());
    assert_eq!(registry.buffered_bytes("v"), 31);
}

#[test]
fn eviction_removes_old_epochs_as_complete_units() {
    let mut log = ViewChangeLog::new(3);
    log.push_epoch(vec![row(1, "a", 1, "one"), row(1, "b", 1, "two")])
        .unwrap();
    log.push_epoch(vec![row(2, "c", 1, "three"), row(2, "d", 1, "four")])
        .unwrap();
    assert_eq!(log.entry_count(), 2);
    assert_eq!(log.replay_floor(), Some(2));
    assert_eq!(log.earliest_epoch(), Some(2));
    assert_eq!(
        log.since_epoch(1)
            .iter()
            .map(|entry| (entry.epoch, entry.encoded_row.clone()))
            .collect::<Vec<_>>(),
        vec![
            (2, Bytes::from_static(b"three")),
            (2, Bytes::from_static(b"four")),
        ]
    );
}

#[test]
fn oversized_durable_epoch_forces_resnapshot_without_partial_output() {
    let registry = SubscribeRegistry::new();
    registry
        .push_epoch("v", vec![row(2, "a", 1, "ok")])
        .unwrap();
    let oversized = "x".repeat(CHANGE_LOG_MAX_EPOCH_BYTES + 1);
    assert_eq!(
        registry.push_epoch("v", vec![row(3, "b", 1, &oversized)]),
        Err(ChangeLogError::EpochTooLarge {
            epoch: 3,
            rows: 1,
            bytes: oversized.len() + 1
        })
    );
    assert_eq!(registry.entry_count("v"), 0);
    assert_eq!(registry.buffered_bytes("v"), 0);
    assert_eq!(registry.replay_floor("v"), Some(4));
    assert_eq!(registry.latest_epoch("v"), Some(3));

    let req = parse_subscribe("SUBSCRIBE v").unwrap();
    let mut subscriber = SubscriberHandle::new("v".to_string(), 2, req, vec![]);
    assert!(matches!(
        subscriber.poll(&registry),
        Err(SubscribeError::ConsumerTooSlow)
    ));
    registry
        .push_epoch("v", vec![row(4, "c", 1, "next")])
        .unwrap();
    assert!(matches!(
        subscriber.poll(&registry),
        Err(SubscribeError::ConsumerTooSlow)
    ));

    let request = parse_subscribe("SUBSCRIBE v AS OF EPOCH 3").unwrap();
    assert!(matches!(
        start_from_epoch(&registry, &request, 3, vec![]),
        Err(SubscribeError::EpochBeforeRetention {
            requested: 3,
            earliest: 4
        })
    ));
}

#[test]
fn empty_history_before_first_committed_epoch_is_not_an_eviction() {
    let registry = SubscribeRegistry::new();
    registry
        .push_epoch("v", vec![row(5, "k", 1, "first")])
        .unwrap();
    assert_eq!(registry.replay_floor("v"), None);
    let mut handle = SubscriberHandle::new(
        "v".to_owned(),
        0,
        parse_subscribe("SUBSCRIBE v").unwrap(),
        vec![],
    );
    let rows = handle.poll(&registry).unwrap();
    assert_eq!(
        rows.iter()
            .map(|entry| (entry.mz_timestamp, entry.mz_diff, entry.encoded_row.clone()))
            .collect::<Vec<_>>(),
        vec![(5, 1, Bytes::from_static(b"first"))]
    );
}

#[test]
fn out_of_order_and_mixed_epoch_batches_do_not_modify_committed_log() {
    let mut log = ViewChangeLog::new(4);
    log.push_epoch(vec![row(4, "a", 1, "a")]).unwrap();
    assert_eq!(
        log.push_epoch(vec![row(4, "b", 1, "b")]),
        Err(ChangeLogError::NonMonotoneEpoch {
            latest: 4,
            requested: 4
        })
    );
    assert_eq!(
        log.push_epoch(vec![row(5, "c", 1, "c"), row(6, "d", 1, "d")]),
        Err(ChangeLogError::MixedEpochs)
    );
    assert_eq!(
        log.since_epoch(0)
            .iter()
            .map(|entry| (entry.epoch, entry.encoded_row.clone()))
            .collect::<Vec<_>>(),
        vec![(4, Bytes::from_static(b"a"))]
    );
    assert_eq!(log.latest_epoch(), Some(4));
}

#[test]
fn stream_limit_is_drop_owned_and_refuses_excess() {
    let registry = Arc::new(SubscribeRegistry::new());
    let slots = (0..SUBSCRIBE_MAX_STREAMS)
        .map(|_| registry.acquire_stream().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(registry.active_streams(), 16);
    assert!(matches!(
        registry.acquire_stream(),
        Err(SubscribeError::LimitExceeded {
            resource: "streams",
            current: 16,
            limit: 16
        })
    ));
    drop(slots);
    assert_eq!(registry.active_streams(), 0);
    let slot = registry.acquire_stream().unwrap();
    assert_eq!(registry.active_streams(), 1);
    drop(slot);
    assert_eq!(registry.active_streams(), 0);
}

#[test]
fn v072_empty_replay_after_restart_requires_snapshot_instead_of_empty_tail() {
    let old = SubscribeRegistry::new();
    old.push_epoch("v", vec![row(5, "a", 1, "old")]).unwrap();
    assert_eq!(old.latest_epoch("v"), Some(5));
    // The new process has no durable replay. A matching numeric epoch alone
    // cannot attest any prior committed result or retention.
    let restarted = SubscribeRegistry::new();
    let request = parse_subscribe("SUBSCRIBE v AS OF EPOCH 5").unwrap();
    assert_eq!(
        start_from_epoch(&restarted, &request, 5, vec![])
            .unwrap_err(),
        SubscribeError::ReplayUnavailable { requested: 5 }
    );
    assert_eq!(
        SubscribeError::ReplayUnavailable { requested: 5 }.to_string(),
        "[RS-2006] history.replay_unavailable: requested epoch 5 has no complete retained replay. next_steps: Reconnect with AS OF NOW WITH SNAPSHOT; durable replay has not been qualified."
    );
    assert_eq!(restarted.entry_count("v"), 0);
}

#[test]
fn v072_valid_retained_replay_is_exact_and_future_epochs_are_rejected() {
    let registry = SubscribeRegistry::new();
    registry.push_epoch("v", vec![row(7, "a", -1, "before"), row(7, "a", 1, "after")]).unwrap();
    registry.push_epoch("v", vec![row(8, "b", 1, "next")]).unwrap();
    let request = parse_subscribe("SUBSCRIBE v AS OF EPOCH 7").unwrap();
    let mut handle = start_from_epoch(&registry, &request, 7, vec![]).unwrap();
    assert_eq!(
        handle.poll(&registry).unwrap().into_iter()
            .map(|item| (item.mz_timestamp, item.mz_diff, item.encoded_row))
            .collect::<Vec<_>>(),
        vec![
            (7, -1, Bytes::from_static(b"before")),
            (7, 1, Bytes::from_static(b"after")),
            (8, 1, Bytes::from_static(b"next"))
        ]
    );
    assert_eq!(handle.last_sent_epoch, 8);
    assert_eq!(registry.entry_count("v"), 3);
    let future = parse_subscribe("SUBSCRIBE v AS OF EPOCH 9").unwrap();
    assert_eq!(
        start_from_epoch(&registry, &future, 9, vec![])
            .unwrap_err(),
        SubscribeError::ReplayUnavailable { requested: 9 }
    );
}

#[test]
fn v072_invalidated_complete_epoch_cannot_be_resumed_without_new_history() {
    let registry = SubscribeRegistry::new();
    registry.push_epoch("v", vec![row(2, "k", 1, "ok")]).unwrap();
    let oversized = "x".repeat(CHANGE_LOG_MAX_EPOCH_BYTES + 1);
    assert!(matches!(
        registry.push_epoch("v", vec![row(3, "k", 1, &oversized)]),
        Err(ChangeLogError::EpochTooLarge { epoch: 3, .. })
    ));
    let from_before = parse_subscribe("SUBSCRIBE v AS OF EPOCH 3").unwrap();
    assert_eq!(
        start_from_epoch(&registry, &from_before, 3, vec![]).unwrap_err(),
        SubscribeError::EpochBeforeRetention { requested: 3, earliest: 4 }
    );
    let from_missing = parse_subscribe("SUBSCRIBE v AS OF EPOCH 4").unwrap();
    assert_eq!(
        start_from_epoch(&registry, &from_missing, 4, vec![]).unwrap_err(),
        SubscribeError::ReplayUnavailable { requested: 4 }
    );
    registry.push_epoch("v", vec![row(4, "k", 1, "next")]).unwrap();
    let mut handle = start_from_epoch(&registry, &from_missing, 4, vec![]).unwrap();
    assert_eq!(
        handle.poll(&registry).unwrap().into_iter()
            .map(|item| (item.mz_timestamp, item.mz_diff, item.encoded_row))
            .collect::<Vec<_>>(),
        vec![(4, 1, Bytes::from_static(b"next"))]
    );
}

#[tokio::test]
async fn v072_async_replay_rejects_before_emitting_any_row() {
    let registry = SubscribeRegistry::new();
    let request = parse_subscribe("SUBSCRIBE v AS OF EPOCH 22").unwrap();
    let mut output = Vec::new();
    let outcome = run_subscribe(
        &request,
        vec![],
        22,
        &registry,
        vec![],
        |item| output.push((item.mz_timestamp, item.mz_diff, item.encoded_row)),
        || true,
    ).await;
    assert_eq!(outcome, Err(SubscribeError::ReplayUnavailable { requested: 22 }));
    assert_eq!(output, Vec::<(u64, i8, Bytes)>::new());
}

#[tokio::test]
async fn v072_async_replay_checks_backlog_before_emitting_any_row() {
    let registry = SubscribeRegistry::new();
    registry
        .push_epoch("v", vec![row(1, "early", 1, "early")])
        .unwrap();
    registry
        .push_epoch(
            "v",
            vec![row(SUBSCRIBE_BACKLOG_MAX as u64 + 2, "late", 1, "late")],
        )
        .unwrap();
    let request = parse_subscribe("SUBSCRIBE v AS OF EPOCH 1").unwrap();
    let mut observed = Vec::new();
    let result = run_subscribe(
        &request,
        vec![],
        1,
        &registry,
        vec![],
        |change| observed.push((change.mz_timestamp, change.mz_diff, change.encoded_row)),
        || true,
    )
    .await;
    assert_eq!(result, Err(SubscribeError::ConsumerTooSlow));
    assert!(observed.is_empty(), "reject before emitting partial replay");
    assert_eq!(registry.entry_count("v"), 2);
}

#[tokio::test]
async fn v072_async_replay_preserves_exact_signed_rows_once() {
    let registry = SubscribeRegistry::new();
    registry
        .push_epoch(
            "v",
            vec![
                row(7, "same", -1, "old"),
                row(7, "same", 1, "new"),
                row(7, "other", 1, "new"),
            ],
        )
        .unwrap();
    registry
        .push_epoch("v", vec![row(8, "final", 1, "next")])
        .unwrap();
    let request = parse_subscribe("SUBSCRIBE v AS OF EPOCH 7").unwrap();
    let mut observed = Vec::new();
    let result = run_subscribe(
        &request,
        vec![],
        8,
        &registry,
        vec![],
        |change| observed.push((change.mz_timestamp, change.mz_diff, change.encoded_row)),
        || true,
    )
    .await;
    assert_eq!(result, Ok(()));
    assert_eq!(
        observed,
        vec![
            (7, -1, Bytes::from_static(b"old")),
            (7, 1, Bytes::from_static(b"new")),
            (7, 1, Bytes::from_static(b"new")),
            (8, 1, Bytes::from_static(b"next")),
        ]
    );
}

#[tokio::test]
async fn v072_async_snapshot_bound_rejects_before_any_row_callback() {
    let registry = SubscribeRegistry::new();
    let request = parse_subscribe("SUBSCRIBE v AS OF NOW WITH SNAPSHOT").unwrap();
    let rows = vec![
        (Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        SUBSCRIBE_SNAPSHOT_MAX_ROWS + 1
    ];
    let mut observed = Vec::new();
    let actual = run_subscribe(
        &request,
        rows,
        5,
        &registry,
        vec![],
        |row| observed.push((row.mz_timestamp, row.mz_diff, row.encoded_row)),
        || true,
    )
    .await;
    assert_eq!(
        actual,
        Err(SubscribeError::LimitExceeded {
            resource: "snapshot_rows",
            current: 10001,
            limit: 10000,
        })
    );
    assert_eq!(observed, Vec::<(u64, i8, Bytes)>::new());
}

#[test]
fn oversized_snapshot_returns_no_partial_output() {
    let request = parse_subscribe("SUBSCRIBE v").unwrap();
    let oversized_rows =
        vec![(Bytes::from_static(b"k"), Bytes::from_static(b"x")); SUBSCRIBE_SNAPSHOT_MAX_ROWS + 1];
    assert!(matches!(
        deliver_snapshot_checked(oversized_rows, 5, &request, &[]),
        Err(SubscribeError::LimitExceeded {
            resource: "snapshot_rows",
            current: 10001,
            limit: 10000
        })
    ));
    let oversized_bytes = vec![(
        Bytes::from_static(b"k"),
        Bytes::from(vec![b'x'; 16 * 1024 * 1024 + 1]),
    )];
    assert!(matches!(
        deliver_snapshot_checked(oversized_bytes, 5, &request, &[]),
        Err(SubscribeError::LimitExceeded {
            resource: "snapshot_bytes",
            current: 16777217,
            limit: 16777216
        })
    ));
}

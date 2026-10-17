//! v0.72 producer-side subscription boundedness and complete-epoch regressions.
use bytes::Bytes;
use rockstream_gateway::change_log::{
    ChangeEntry, ChangeLogError, ViewChangeLog, CHANGE_LOG_MAX_EPOCH_BYTES,
};
use rockstream_gateway::subscribe_handler::{
    deliver_snapshot_checked, start_from_epoch, SubscribeError, SubscribeRegistry,
    SubscriberHandle, SUBSCRIBE_MAX_STREAMS, SUBSCRIBE_SNAPSHOT_MAX_ROWS,
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

use rockstream_ops::shared_window::{
    SharedWindowError, SharedWindowFabric, MAX_SHARED_WINDOW_QUERY_SLICES, MAX_SHARED_WINDOW_SLICES,
};
use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};
use rockstream_types::SharedWindowSpec;

fn spec() -> SharedWindowSpec {
    SharedWindowSpec::new(
        "orders",
        "tenant-hash",
        "event_time",
        10,
        Some([7; 32]),
        MergeLawId(2),
        MergeLawVersion(1),
    )
    .unwrap()
}

#[test]
fn different_window_widths_share_physical_slices() {
    let mut fabric = SharedWindowFabric::new();
    let key = spec();
    fabric.register(key.clone()).unwrap();
    fabric.attach(&key, "five-minute", 20).unwrap();
    fabric.attach(&key, "ten-minute", 40).unwrap();

    fabric.apply(&key, 0, 4, 1).unwrap();
    fabric.apply(&key, 10, 6, 1).unwrap();
    fabric.apply(&key, 20, 3, 1).unwrap();

    assert_eq!(fabric.consumer_count(&key), Some(2));
    assert_eq!(fabric.slice_count(&key), Some(3));
    assert_eq!(fabric.window_sum(&key, 0, 20).unwrap(), (10, 2));
    assert_eq!(fabric.window_sum(&key, 0, 40).unwrap(), (13, 3));
}

#[test]
fn shared_slices_apply_retractions_exactly() {
    let mut fabric = SharedWindowFabric::new();
    let key = spec();
    fabric.register(key.clone()).unwrap();
    fabric.attach(&key, "consumer", 20).unwrap();

    fabric.apply(&key, 12, 9, 1).unwrap();
    fabric.apply(&key, 12, 9, -1).unwrap();

    assert_eq!(fabric.window_sum(&key, 10, 20).unwrap(), (0, 0));
    assert_eq!(fabric.slice_count(&key), Some(0));
}

#[test]
fn shared_window_fabric_enforces_slice_and_query_bounds() {
    let key = spec();
    let mut fabric = SharedWindowFabric::with_limits(2, 2);
    fabric.register(key.clone()).unwrap();
    fabric.attach(&key, "consumer", 20).unwrap();
    fabric.apply(&key, 0, 1, 1).unwrap();
    fabric.apply(&key, 10, 1, 1).unwrap();
    assert!(matches!(
        fabric.apply(&key, 20, 1, 1),
        Err(SharedWindowError::SliceCapacityExceeded { max: 2 })
    ));
    assert!(matches!(
        fabric.window_sum(&key, 0, 30),
        Err(SharedWindowError::QueryTooWide { max: 2, .. })
    ));
    assert_eq!(MAX_SHARED_WINDOW_SLICES, 100_000);
    assert_eq!(MAX_SHARED_WINDOW_QUERY_SLICES, 1_024);
}

#[test]
fn shared_window_identity_keeps_isolated_predicates_separate() {
    let mut fabric = SharedWindowFabric::new();
    let first = spec();
    let second = SharedWindowSpec::new(
        "orders",
        "tenant-hash",
        "event_time",
        10,
        Some([8; 32]),
        MergeLawId(2),
        MergeLawVersion(1),
    )
    .unwrap();
    fabric.register(first.clone()).unwrap();
    fabric.register(second.clone()).unwrap();
    fabric.attach(&first, "consumer", 20).unwrap();
    fabric.attach(&second, "consumer", 20).unwrap();
    fabric.apply(&first, 0, 5, 1).unwrap();

    assert_eq!(fabric.window_sum(&first, 0, 10).unwrap(), (5, 1));
    assert_eq!(fabric.window_sum(&second, 0, 10).unwrap(), (0, 0));
}

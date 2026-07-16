use rockstream_control::skew::{detect_hot_key, HotKeyDetector, MAX_TRACKED_KEY_LOADS};
use rockstream_types::ids::ShardId;
use rockstream_types::topology::{KeyLoadSample, ShardLoadSample};

fn make_sample(hot_cpu: u64, median_cpu: u64) -> ShardLoadSample {
    let mut key_loads = Vec::new();
    key_loads.push(KeyLoadSample {
        key_prefix: b"hot".to_vec(),
        cpu_nanos: hot_cpu,
        bytes_per_epoch: hot_cpu / 2,
        state_writes_per_epoch: hot_cpu / 4,
    });
    for idx in 0..5 {
        key_loads.push(KeyLoadSample {
            key_prefix: format!("cold-{idx}").into_bytes(),
            cpu_nanos: median_cpu,
            bytes_per_epoch: median_cpu / 2,
            state_writes_per_epoch: median_cpu / 4,
        });
    }

    ShardLoadSample {
        shard_id: ShardId(9),
        state_bytes: 1024,
        rows_per_epoch: 2048,
        cpu_nanos: key_loads.iter().map(|sample| sample.cpu_nanos).sum(),
        bytes_per_epoch: key_loads.iter().map(|sample| sample.bytes_per_epoch).sum(),
        state_writes_per_epoch: key_loads
            .iter()
            .map(|sample| sample.state_writes_per_epoch)
            .sum(),
        key_loads,
    }
}

#[test]
fn hot_key_detection_trips_at_fifty_times_median() {
    let report = detect_hot_key(&make_sample(50_000, 1_000), 20.0).unwrap();
    let report = report.expect("expected hot key report");
    assert_eq!(report.shard_id, ShardId(9));
    assert_eq!(report.key_prefix, b"hot".to_vec());
    assert!(report.hotness_factor >= 50.0);
}

#[test]
fn hot_key_detection_does_not_trip_below_threshold() {
    let report = detect_hot_key(&make_sample(9_000, 1_000), 10.0).unwrap();
    assert!(report.is_none());
}

#[test]
fn hot_key_tracker_is_bounded_with_fill_level_metric() {
    let mut detector = HotKeyDetector::new(10.0);
    let sample = make_sample(50_000, 1_000);
    detector.observe(&sample).unwrap();
    let fill = detector.fill_level();
    assert_eq!(fill.used, sample.key_loads.len());
    assert_eq!(fill.capacity, MAX_TRACKED_KEY_LOADS);
}

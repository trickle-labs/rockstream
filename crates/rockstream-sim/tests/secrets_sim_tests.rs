#![cfg(feature = "simulation")]

use rockstream_runtime::WorkerSecretManager;
use rockstream_sim::{buggify, buggify::buggify_init, SimRuntime};

#[test]
fn secret_resolution_remains_bounded_under_seeded_rotation_jitter() {
    buggify_init(55_001);
    let runtime = SimRuntime::new(55_001);
    let manager = WorkerSecretManager::new("worker-1");
    for _ in 0..8 {
        let _network_jitter = buggify!("network.partition", 0.0);
        runtime.advance_time(std::time::Duration::from_millis(runtime.random_u64() % 3));
        assert_eq!(manager.fill_level(), 0);
    }
}

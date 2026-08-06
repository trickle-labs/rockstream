#![cfg(feature = "simulation")]

use std::panic::{catch_unwind, AssertUnwindSafe};

use rockstream_sim::{buggify, Runtime, SimRuntime};
use rockstream_types::dlq::{get_global_dlq, DlqEntry};

const LOCK_POISONING_SEED: u64 = 0x5101_9000_0000_0001;
const LOCK_POISONING_FAULT: &str = "lock_poisoning.holder_panic";

fn entry(source_name: &str, arrived_at: u64) -> DlqEntry {
    DlqEntry {
        arrived_at,
        source_name: source_name.to_string(),
        source_offset: "offset".to_string(),
        error_code: "RS-4008".to_string(),
        error_message: "invalid payload".to_string(),
        raw_bytes_hex: "7b7d".to_string(),
        replay_attempt: 0,
    }
}

fn run_seeded_lock_poisoning_scenario(seed: u64) -> Vec<DlqEntry> {
    let runtime = SimRuntime::new(seed);
    rockstream_sim::buggify::buggify_init(runtime.seed());
    rockstream_sim::buggify::buggify_focus(LOCK_POISONING_FAULT);

    assert!(
        buggify!(LOCK_POISONING_FAULT, 1.0),
        "seed {seed:#x} must inject the holder panic"
    );
    let dlq = get_global_dlq();
    dlq.lock().clear();

    let holder_panic = catch_unwind(AssertUnwindSafe(|| {
        let mut entries = dlq.lock();
        entries.push(entry("abandoned", 1));
        panic!("seeded lock holder panic");
    }));
    assert!(holder_panic.is_err());

    dlq.lock().push(entry("peer", 2));
    let result = dlq.lock().clone();
    rockstream_sim::buggify::buggify_disable();
    result
}

#[test]
fn lock_poisoning_simruntime_seed_replays_and_preserves_peer_access() {
    let corpus = rockstream_sim::build_initial_corpus();
    assert!(corpus
        .regression_seeds()
        .iter()
        .any(|seed| seed.seed == LOCK_POISONING_SEED));

    let first = run_seeded_lock_poisoning_scenario(LOCK_POISONING_SEED);
    let replay = run_seeded_lock_poisoning_scenario(LOCK_POISONING_SEED);
    assert_eq!(first, replay);
    assert_eq!(
        first,
        vec![entry("abandoned", 1), entry("peer", 2)],
        "the peer must observe the complete ordered DLQ output after holder panic"
    );

    get_global_dlq().lock().clear();
}

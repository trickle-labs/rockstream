//! DST simulation fuzz harness under SimRuntime (v0.14).

use rockstream_oracle::sql_fuzzer;
use rockstream_sim::{Runtime, SeedOutcome, SoakRunner};

#[test]
fn dst_fuzz_harness_seeds() {
    const SEEDS: u64 = 100_000;
    let mut runner = SoakRunner::new();

    // Create a single thread tokio runtime to execute the async fuzzer cases.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for seed in 0..SEEDS {
        runner.run_seed(seed, |sim_rt| {
            // Write a deterministic key/value to SimRuntime's storage
            let key = format!("fuzz_harness/{seed:08x}");
            let random_val = sim_rt.random_u64();
            sim_rt
                .object_store()
                .put(&key, bytes::Bytes::from(random_val.to_le_bytes().to_vec()))
                .unwrap();

            // Simulate a network message and advance time
            sim_rt
                .network()
                .send(seed % 4, (seed + 1) % 4, bytes::Bytes::new());
            sim_rt.advance_time(std::time::Duration::from_millis(5));

            // Execute the fuzzer case deterministically based on seed
            rt.block_on(async {
                sql_fuzzer::run_fuzz_case(seed).await;
            });

            SeedOutcome::Pass
        });
    }

    assert_eq!(runner.seeds_run(), SEEDS);
    assert!(
        runner.all_passed(),
        "Expected all {SEEDS} fuzz seeds to pass; got: {:?}",
        runner.failures()
    );
}

//! DST simulation fuzz harness under SimRuntime (v0.14).

use rockstream_oracle::sql_fuzzer;
use rockstream_sim::{Runtime, SeedOutcome, SoakRunner};

fn run_seed_range(start: u64, end: u64) -> SoakRunner {
    let mut runner = SoakRunner::new();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for seed in start..end {
        runner.run_seed(seed, |sim_rt| {
            let key = format!("fuzz_harness/{seed:08x}");
            let random_val = sim_rt.random_u64();
            sim_rt
                .object_store()
                .put(&key, bytes::Bytes::from(random_val.to_le_bytes().to_vec()))
                .unwrap();

            sim_rt
                .network()
                .send(seed % 4, (seed + 1) % 4, bytes::Bytes::new());
            sim_rt.advance_time(std::time::Duration::from_millis(5));

            rt.block_on(async {
                sql_fuzzer::run_fuzz_case(seed).await;
            });

            SeedOutcome::Pass
        });
    }

    runner
}

#[test]
fn dst_fuzz_harness_seeds() {
    const SEEDS: u64 = 100_000;
    const WORKERS: u64 = 4;
    let chunk = SEEDS.div_ceil(WORKERS);

    let runners = std::thread::scope(|scope| {
        (0..WORKERS)
            .map(|worker| {
                let start = worker * chunk;
                let end = (start + chunk).min(SEEDS);
                scope.spawn(move || run_seed_range(start, end))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });

    let seeds_run: u64 = runners.iter().map(SoakRunner::seeds_run).sum();
    let failures: Vec<_> = runners
        .iter()
        .flat_map(|runner| runner.failures().iter().cloned())
        .collect();

    assert_eq!(seeds_run, SEEDS);
    assert!(
        failures.is_empty(),
        "Expected all {SEEDS} fuzz seeds to pass; got: {failures:?}"
    );
}

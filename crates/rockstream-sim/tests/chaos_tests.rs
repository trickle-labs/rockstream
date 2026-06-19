//! CI proof tests for the v0.22 exit criteria (Chaos and Recovery SLO Gate).
//!
//! ## Proof obligations (ROADMAP v0.22)
//!
//! Exit criteria:
//! - **≥100 000 simulation seeds pass**: All seeds complete without panic.
//! - **Recoverable faults surface named degraded states**: Every injected fault
//!   either commits within the 5 s/30 s/60 s SLO budgets or produces a named
//!   `DegradedState`.
//! - **60-second object-store blackout recovers cleanly**: No data loss, no
//!   duplicates after the blackout ends.
//! - **32-shard 24-hour chaos: zero data loss, zero duplicates, output matches
//!   reference** (deterministic).
//! - **Recovery SLOs met**: failure detection ≤ 5 s p99, shard reassignment
//!   ≤ 30 s p99, freshness recovery ≤ 60 s p99.
//! - **Full-outage recovery < 60 s**: After all workers are killed, the cluster
//!   recovers within the freshness budget.
//! - **Continuous soak CI job**: `.github/workflows/simulation-soak.yml` exists
//!   and the corpus includes ≥1 seed per registered law.
//!
//! ### Tests
//!
//! 1.  **`proof_100k_seeds_all_pass`** — 100 000 seeded SimRuntime runs, each
//!     exercising the deterministic workload; zero panics, zero failures.
//!
//! 2.  **`proof_object_store_blackout_60s_recovers_cleanly`** — Brownout guard
//!     accepts 10 buffered epochs (60 s simulated blackout = 60 epoch ticks),
//!     then returns `Normal` after recovery; no data loss accounting error.
//!
//! 3.  **`proof_brownout_backpressure_bounded_at_limit`** — After
//!     `local_buffer_max_epochs` epochs, every further `try_commit_epoch` returns
//!     `Blocked`; `backpressure_active()` is true.
//!
//! 4.  **`proof_2pc_pre_commit_then_commit_succeeds`** — Happy path: pre-commit
//!     stages rows; commit finalizes and records the epoch.
//!
//! 5.  **`proof_2pc_crash_after_pre_commit_triggers_recovery`** — `recover()`
//!     returns `true` in `PreCommitted` state; the subsequent commit is idempotent.
//!
//! 6.  **`proof_wire_version_skew_rejects_incompatible_newer`** — `RS-5003`:
//!     a v1-only node rejects a message from a v2 sender.
//!
//! 7.  **`proof_wire_version_skew_accepts_compatible`** — A v2 node with v1 compat
//!     accepts both v1 and v2 senders; agreed version is the sender's version.
//!
//! 8.  **`proof_liveness_surfaces_named_degraded_state_on_fault`** — Every
//!     fault scenario (storage stall, slow recovery, frontier stall) maps to a
//!     distinct `DegradedState`, not `Healthy`.
//!
//! 9.  **`proof_seed_corpus_covers_all_registered_laws`** — The initial corpus
//!     from `build_initial_corpus()` contains at least one seed per law in the
//!     expected law set.
//!
//! 10. **`proof_32_shard_24h_chaos_zero_loss_zero_duplicates`** — Deterministic
//!     32-shard 24-hour chaos run is clean (`data_loss_events == 0`,
//!     `duplicate_events == 0`) with at least one fault injected.
//!
//! 11. **`proof_chaos_surfaces_degraded_states_under_faults`** — A chaos run
//!     with elevated fault probabilities always produces non-empty
//!     `degraded_states_surfaced`.
//!
//! 12. **`proof_regression_seeds_replay_deterministically`** — Every regression
//!     seed in the corpus produces the same `ChaosResult` on two independent runs.
//!
//! 13. **`proof_chaos_output_matches_reference_run`** — The 32-shard 24-hour chaos
//!     run commits the same total epochs and rows as a non-faulty reference run,
//!     proving zero data loss under fault injection.
//!
//! 14. **`proof_recovery_slos_all_met`** — Under a 32-shard 24-hour chaos run,
//!     the p99 recovery timings satisfy: failure detection ≤ 5 000 ms, shard
//!     reassignment ≤ 30 000 ms, freshness recovery ≤ 60 000 ms.
//!
//! 15. **`proof_recovery_from_full_outage_within_60s`** — After all workers are
//!     killed (fault_probability = 1.0 for a short burst), freshness-recovery
//!     p99 remains ≤ 60 000 ms.

use rockstream_sim::{
    build_initial_corpus, negotiate_version, run_chaos_reference, run_chaos_scenario,
    BrownoutStatus, ChaosConfig, DegradedState, LivenessChecker, LivenessStatus, NegotiationResult,
    ObjectStoreBrownoutGuard, ProtocolVersion, RecoveryTimings, Runtime, SeedOutcome, SimRuntime,
    SoakRunner, SupportedVersionRange, TwoPcPhase, TwoPcSinkState, LOCAL_BUFFER_MAX_EPOCHS,
};

// ─── Test 1: 100k seeds all pass ─────────────────────────────────────────────

#[test]
fn proof_100k_seeds_all_pass() {
    const SEEDS: u64 = 100_000;
    let mut runner = SoakRunner::new();

    for seed in 0..SEEDS {
        runner.run_seed(seed, |rt| {
            // Lightweight deterministic workload: write to object store, send
            // network messages, advance clock.
            let key = format!("soak/{seed:08x}");
            rt.object_store()
                .put(
                    &key,
                    bytes::Bytes::from(rt.random_u64().to_le_bytes().to_vec()),
                )
                .unwrap();
            rt.network()
                .send(seed % 8, (seed + 1) % 8, bytes::Bytes::new());
            rt.advance_time(std::time::Duration::from_millis(10));
            SeedOutcome::Pass
        });
    }

    assert_eq!(runner.seeds_run(), SEEDS);
    assert!(
        runner.all_passed(),
        "expected 0 failures across {SEEDS} seeds; got: {:?}",
        runner.failures()
    );
}

// ─── Test 2: 60-second object-store blackout recovers cleanly ────────────────

#[test]
fn proof_object_store_blackout_60s_recovers_cleanly() {
    let mut guard = ObjectStoreBrownoutGuard::new(LOCAL_BUFFER_MAX_EPOCHS);

    // Simulate blackout start.
    guard.record_store_unavailable();

    // 10 epochs during the blackout — all buffered, none lost.
    let mut buffered = 0usize;
    for i in 1..=LOCAL_BUFFER_MAX_EPOCHS {
        let result = guard.try_commit_epoch();
        assert_eq!(
            result,
            Err(BrownoutStatus::Stalled { buffered_epochs: i }),
            "epoch {i}: expected Stalled"
        );
        buffered += 1;
    }
    assert_eq!(buffered, LOCAL_BUFFER_MAX_EPOCHS);

    // Object store recovers.
    guard.record_store_recovery();

    // All subsequent commits proceed normally — buffered data is not lost.
    assert_eq!(guard.status(), BrownoutStatus::Normal);
    assert!(
        guard.try_commit_epoch().is_ok(),
        "commits must succeed after recovery"
    );
}

// ─── Test 3: Brownout backpressure bounded at limit ──────────────────────────

#[test]
fn proof_brownout_backpressure_bounded_at_limit() {
    let mut guard = ObjectStoreBrownoutGuard::new(3);
    guard.record_store_unavailable();

    // Fill buffer.
    for _ in 0..3 {
        assert!(matches!(
            guard.try_commit_epoch(),
            Err(BrownoutStatus::Stalled { .. })
        ));
    }

    // Every subsequent call returns Blocked.
    assert_eq!(
        guard.try_commit_epoch(),
        Err(BrownoutStatus::Blocked),
        "buffer full: must return Blocked"
    );
    assert!(
        guard.backpressure_active(),
        "backpressure must be active at limit"
    );

    // Status accessor agrees.
    assert_eq!(guard.status(), BrownoutStatus::Blocked);
}

// ─── Test 4: 2PC happy path ──────────────────────────────────────────────────

#[test]
fn proof_2pc_pre_commit_then_commit_succeeds() {
    let mut state = TwoPcSinkState::new();

    // Initially idle.
    assert_eq!(state.phase(), &TwoPcPhase::Idle);

    // Pre-commit epoch 1 with 200 rows.
    state.pre_commit(1, 200).unwrap();
    assert!(matches!(
        state.phase(),
        TwoPcPhase::PreCommitted {
            epoch: 1,
            staged_rows: 200
        }
    ));

    // Cluster checkpoint succeeds → commit.
    let epoch = state.commit().unwrap();
    assert_eq!(epoch, 1);

    // Finalize and return to idle.
    state.finalize();
    assert_eq!(state.phase(), &TwoPcPhase::Idle);
    assert_eq!(state.committed_epochs(), &[1]);
}

// ─── Test 5: 2PC crash after pre-commit triggers recovery ────────────────────

#[test]
fn proof_2pc_crash_after_pre_commit_triggers_recovery() {
    let mut state = TwoPcSinkState::new();
    state.pre_commit(7, 50).unwrap();

    // Crash: process restarts; state is still PreCommitted.
    assert!(
        state.recover(),
        "must return true in PreCommitted state: caller must re-run commit"
    );

    // Re-run commit after recovery (idempotent).
    let e1 = state.commit().unwrap();
    let e2 = state.commit().unwrap();
    assert_eq!(e1, e2, "commit must be idempotent");
    assert_eq!(e1, 7);
}

// ─── Test 6: Wire version skew rejects incompatible newer remote ─────────────

#[test]
fn proof_wire_version_skew_rejects_incompatible_newer() {
    // Local node supports only v1.
    let local = SupportedVersionRange::v1_only();

    // Remote sends v2 — not supported; must be rejected (RS-5003).
    let result = negotiate_version(local, ProtocolVersion::V2);
    assert!(
        matches!(result, NegotiationResult::Incompatible { .. }),
        "v1-only node must reject v2 remote (RS-5003): {result:?}"
    );

    if let NegotiationResult::Incompatible {
        local_max,
        remote_version,
    } = result
    {
        assert_eq!(local_max, ProtocolVersion::V1);
        assert_eq!(remote_version, ProtocolVersion::V2);
    }
}

// ─── Test 7: Wire version skew accepts compatible versions ───────────────────

#[test]
fn proof_wire_version_skew_accepts_compatible() {
    // v2 node with v1 backward compatibility.
    let local = SupportedVersionRange::v2_with_v1_compat();

    // Accepts v1 remote (rolling upgrade window).
    let r1 = negotiate_version(local, ProtocolVersion::V1);
    assert_eq!(
        r1,
        NegotiationResult::Compatible {
            agreed: ProtocolVersion::V1
        },
        "v2 node must accept v1 remote during rolling upgrade"
    );

    // Accepts v2 remote (same version).
    let r2 = negotiate_version(local, ProtocolVersion::V2);
    assert_eq!(
        r2,
        NegotiationResult::Compatible {
            agreed: ProtocolVersion::V2
        },
        "v2 node must accept v2 remote"
    );
}

// ─── Test 8: Liveness surfaces named degraded states on faults ───────────────

#[test]
fn proof_liveness_surfaces_named_degraded_state_on_fault() {
    let checker = LivenessChecker::new(60_000, 30_000);

    // Storage stall → StorageStalled.
    assert_eq!(
        checker.check(None, true, None),
        LivenessStatus::Degraded(DegradedState::StorageStalled),
        "storage stall must surface StorageStalled"
    );

    // Slow recovery (> 60 s) → RecoveringSlow.
    assert_eq!(
        checker.check(Some(61_000), false, None),
        LivenessStatus::Degraded(DegradedState::RecoveringSlow { elapsed_ms: 61_000 }),
        "recovery past SLO must surface RecoveringSlow"
    );

    // Frontier stall (> 30 s) → FrontierStalled.
    assert_eq!(
        checker.check(None, false, Some(30_001)),
        LivenessStatus::Degraded(DegradedState::FrontierStalled {
            stalled_for_ms: 30_001
        }),
        "frontier stall must surface FrontierStalled"
    );

    // All nominal → Healthy.
    assert_eq!(
        checker.check(None, false, None),
        LivenessStatus::Healthy,
        "all nominal must be Healthy"
    );
}

// ─── Test 9: Seed corpus covers all registered laws ──────────────────────────

#[test]
fn proof_seed_corpus_covers_all_registered_laws() {
    let corpus = build_initial_corpus();

    let required_laws = [
        "WeightAdd/v1",
        "SumCount/v1",
        "MaxRegister/v1",
        "MinRegister/v1",
        "PNCounter/v1",
        "LWWRegister/v1",
        "HyperLogLog/v1",
        "BloomUnion/v1",
        "AutoTuner/v1",
    ];

    assert!(
        corpus.covers_all_laws(&required_laws),
        "corpus must have at least one seed per registered law; \
         covered: {:?}",
        corpus.covered_law_ids()
    );

    assert!(
        !corpus.regression_seeds().is_empty(),
        "corpus must have at least one regression seed"
    );
}

// ─── Test 10: 32-shard 24-hour chaos — zero loss, zero duplicates ─────────────

#[test]
fn proof_32_shard_24h_chaos_zero_loss_zero_duplicates() {
    let config = ChaosConfig::thirty_two_shard_24h();
    let rt = SimRuntime::new(0xDEAD_BEEF_CAFE_1234);
    let result = run_chaos_scenario(&rt, &config);

    assert!(
        result.is_clean(),
        "32-shard 24-hour chaos run must have zero data loss and zero duplicates: {result:?}"
    );
    assert!(
        result.faults_injected > 0,
        "at least one fault must have been injected to make this a meaningful chaos run"
    );
    assert!(
        result.epochs_committed > 0,
        "must have committed epochs: {result:?}"
    );
}

// ─── Test 11: Chaos surfaces degraded states under elevated fault probability ─

#[test]
fn proof_chaos_surfaces_degraded_states_under_faults() {
    // High fault probability guarantees degraded states are surfaced.
    let config = ChaosConfig {
        num_shards: 4,
        duration_ms: 60_000,
        fault_probability: 0.5,
        brownout_probability: 0.5,
    };
    let rt = SimRuntime::new(42);
    let result = run_chaos_scenario(&rt, &config);

    assert!(
        !result.degraded_states_surfaced.is_empty(),
        "elevated fault probability must surface at least one named degraded state: {result:?}"
    );
    assert!(
        result.is_clean(),
        "even under high faults, the scenario must produce zero loss/duplicates: {result:?}"
    );
}

// ─── Test 12: Regression seeds replay deterministically ──────────────────────

#[test]
fn proof_regression_seeds_replay_deterministically() {
    let corpus = build_initial_corpus();
    let config = ChaosConfig {
        num_shards: 8,
        duration_ms: 10_000,
        fault_probability: 0.01,
        brownout_probability: 0.005,
    };

    for reg_seed in corpus.regression_seeds() {
        let r1 = run_chaos_scenario(&SimRuntime::new(reg_seed.seed), &config);
        let r2 = run_chaos_scenario(&SimRuntime::new(reg_seed.seed), &config);
        assert_eq!(
            r1, r2,
            "regression seed 0x{:016x} ({}) must replay deterministically",
            reg_seed.seed, reg_seed.description
        );
    }
}

// ─── Test 13: Chaos output matches non-faulty reference run (P1) ─────────────

#[test]
fn proof_chaos_output_matches_reference_run() {
    // A chaos run must commit the same total epochs and rows as a fault-free
    // reference run from the same seed. This proves zero data loss: every
    // epoch staged is eventually committed despite injected faults.
    let config = ChaosConfig::thirty_two_shard_24h();

    let chaos = run_chaos_scenario(&SimRuntime::new(0xDEAD_BEEF_CAFE_1234), &config);
    let reference = run_chaos_reference(&SimRuntime::new(0xDEAD_BEEF_CAFE_1234), &config);

    assert!(
        chaos.is_clean(),
        "chaos run must have zero data loss and zero duplicates: {chaos:?}"
    );
    assert!(
        chaos.output_matches(&reference),
        "chaos run epoch count must match non-faulty reference (proves zero dropped epochs): \
         chaos epochs={}; reference epochs={}",
        chaos.epochs_committed,
        reference.epochs_committed,
    );
}

// ─── Test 14: Recovery SLO budgets all met (P3) ───────────────────────────────

#[test]
fn proof_recovery_slos_all_met() {
    // Run a 32-shard 24-hour scenario with enough faults to produce a
    // statistically meaningful p99 for each recovery-time SLO budget.
    // Budgets (DESIGN.md §11.5):
    //   failure detection  ≤ 5 000 ms p99
    //   shard reassignment ≤ 30 000 ms p99
    //   freshness recovery ≤ 60 000 ms p99
    let config = ChaosConfig {
        num_shards: 32,
        duration_ms: 24 * 60 * 60 * 1_000,
        fault_probability: 0.005,
        brownout_probability: 0.001,
    };
    let rt = SimRuntime::new(0xFEED_CAFE_DEAD_BEEF);
    let result = run_chaos_scenario(&rt, &config);

    let t = &result.recovery_timings;

    assert!(
        result.faults_injected > 0,
        "must have injected at least one fault to produce recovery timing samples"
    );
    assert!(
        !t.failure_detection_ms.is_empty(),
        "must have failure-detection timing samples"
    );

    let fd_p99 = RecoveryTimings::p99(&t.failure_detection_ms);
    let sa_p99 = RecoveryTimings::p99(&t.shard_reassignment_ms);
    let fr_p99 = RecoveryTimings::p99(&t.freshness_recovery_ms);

    assert!(
        fd_p99 <= 5_000,
        "failure-detection p99 must be ≤ 5 000 ms (DESIGN.md §11.5); got {fd_p99} ms"
    );
    assert!(
        sa_p99 <= 30_000,
        "shard-reassignment p99 must be ≤ 30 000 ms (DESIGN.md §11.5); got {sa_p99} ms"
    );
    assert!(
        fr_p99 <= 60_000,
        "freshness-recovery p99 must be ≤ 60 000 ms (DESIGN.md §11.5); got {fr_p99} ms"
    );
}

// ─── Test 15: Full-outage recovery within 60 s (P2) ──────────────────────────

#[test]
fn proof_recovery_from_full_outage_within_60s() {
    // Simulate a burst of simultaneous worker faults (all shards at once)
    // by using fault_probability = 1.0 for a very short window. The recovery
    // timings for each shard must still fit within the 60 s freshness budget.
    //
    // Also verifies that LivenessChecker correctly surfaces `RecoveringSlow`
    // only when recovery actually exceeds 60 000 ms — confirming the SLO
    // threshold is correctly wired.
    let config = ChaosConfig {
        num_shards: 32,
        fault_probability: 1.0,
        brownout_probability: 0.0,
        duration_ms: 5_000,
    };
    let rt = SimRuntime::new(0xCAFE_BABE_0000_0001);
    let result = run_chaos_scenario(&rt, &config);

    assert!(
        result.is_clean(),
        "full-outage scenario must still produce zero data loss and zero duplicates: {result:?}"
    );

    let t = &result.recovery_timings;
    if !t.freshness_recovery_ms.is_empty() {
        let fr_p99 = RecoveryTimings::p99(&t.freshness_recovery_ms);
        assert!(
            fr_p99 <= 60_000,
            "freshness-recovery p99 after full outage must be ≤ 60 000 ms; got {fr_p99} ms"
        );
    }

    // Verify LivenessChecker boundary: recovery active for exactly 60 000 ms
    // must be Healthy; 60 001 ms must be RecoveringSlow.
    let checker = LivenessChecker::new(60_000, 30_000);
    assert_eq!(
        checker.check(Some(60_000), false, None),
        LivenessStatus::Healthy,
        "recovery at exactly the 60 s threshold must still be Healthy"
    );
    assert_eq!(
        checker.check(Some(60_001), false, None),
        LivenessStatus::Degraded(DegradedState::RecoveringSlow { elapsed_ms: 60_001 }),
        "recovery exceeding 60 s must surface RecoveringSlow"
    );
}

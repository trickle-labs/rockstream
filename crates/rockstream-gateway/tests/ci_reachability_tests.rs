#[test]
fn ci_workspace_test_runs_explicit_feature_matrix() {
    let workflow = include_str!("../../../.github/workflows/ci.yml");
    // The accounting runner supersedes the old hard-coded Cargo command.
    // Require both live Cargo discovery and an explicit workspace execution.
    assert!(workflow.contains("python3 scripts/ci-test-inventory.py check"));
    assert!(workflow.contains("python3 scripts/ci-test-inventory.py run --job pr-workspace"));
    let matrix = include_str!("../../../scripts/ci-test-matrix.json");
    assert!(matrix.contains("\"pr-workspace\""));
    assert!(workflow.contains("python3 scripts/ci-test-inventory.py run --job pr-gateway"));

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let gated_files = [
        (
            "crates/rockstream-gateway/tests/auth_scram_tests.rs",
            "testcontainers",
        ),
        (
            "crates/rockstream-gateway/tests/driver_matrix_tests.rs",
            "testcontainers",
        ),
        (
            "crates/rockstream-gateway/tests/query_time_multi_shard_scatter_minio_tests.rs",
            "testcontainers",
        ),
        (
            "crates/rockstream-gateway/tests/reference_app_tests.rs",
            "testcontainers",
        ),
        (
            "crates/rockstream-sim/tests/az_aware_exchange_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/control_plane_ha_tests.rs",
            "simulation",
        ),
        ("crates/rockstream-sim/tests/control_sim.rs", "simulation"),
        (
            "crates/rockstream-sim/tests/frontier_publisher_election.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/hot_key_detection_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/lock_poisoning_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/query_time_scatter_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/recursive_cte_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/shard_merge_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/shard_migration_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/shard_split_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/shard_stats_checkpoint_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/sim_aggregate_coordination_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/skew_control_loop_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/worker_drain_sim_tests.rs",
            "simulation",
        ),
        (
            "crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs",
            "docker_tests",
        ),
        (
            "crates/rockstream-sim/tests/resource_leak_soak_real_binary_tests.rs",
            "docker_tests",
        ),
    ];
    for (path, feature) in gated_files {
        assert!(
            std::fs::read_to_string(root.join(path))
                .unwrap()
                .contains(&format!("#![cfg(feature = \"{feature}\")]")),
            "{path} must stay whole-file gated by {feature}",
        );
        let target = std::path::Path::new(path)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            matrix.contains(&format!("\"{target}\""))
                || (path.starts_with("crates/rockstream-gateway/")
                    && matrix.contains("\"rockstream-gateway\"")),
            "{path} must have an explicit CI assignment",
        );
        assert!(
            matrix.contains(&format!("\"{feature}\"")),
            "{path} requires its feature to be declared in the CI matrix",
        );
    }
}

#[test]
fn simulation_soak_enables_simulation() {
    let workflow = include_str!("../../../.github/workflows/simulation-soak.yml");
    assert_eq!(
        workflow
            .replace("\\\n", "")
            .lines()
            .filter(|line| line.contains("cargo test"))
            .map(|line| line.trim_start().trim_start_matches("run: ").split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>(),
        vec![
            "cargo test -p rockstream-sim --features simulation --test chaos_tests -- --nocapture",
            "cargo test -p rockstream-sim --features simulation --test chaos_tests incremental_chaos::bounded_seed_run -- --exact",
            "cargo test -p rockstream-sim --features simulation --test chaos_tests proof_seed_corpus_covers_all_registered_laws -- --nocapture",
        ]
    );
}

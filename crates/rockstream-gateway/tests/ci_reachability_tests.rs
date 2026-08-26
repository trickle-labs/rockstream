#[test]
fn ci_workspace_test_runs_explicit_feature_matrix() {
    let workflow = include_str!("../../../.github/workflows/ci.yml");
    assert!(workflow
        .lines()
        .any(|line| line == "          cargo test --workspace --exclude rockstream-connectors --exclude rockstream-control --exclude rockstream-gateway --exclude rockstream-ops --exclude rockstream-runtime --exclude rockstream-storage --exclude rockstream-sim"));
    assert!(workflow.lines().any(|line| line.contains(
        "cargo test --quiet --all-features -p rockstream-gateway --test auth_scram_tests --test gateway_dml_tests --test gateway_extended_query_tests --test gateway_integration_tests --test gateway_proof_tests --test golden_wire_tests --test listen_notify_tests --test transaction_savepoint_tests -- -Z unstable-options --format json"
    )));

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
            workflow.contains(&format!("--features {feature}"))
                && workflow.contains(&format!("--test {target}")),
            "{path} must be compiled and run by the {feature} CI matrix",
        );
    }
}

#[test]
fn simulation_soak_enables_simulation() {
    let workflow = include_str!("../../../.github/workflows/simulation-soak.yml");
    assert_eq!(
        workflow
            .lines()
            .filter(|line| line
                .contains("cargo test -p rockstream-sim --features simulation --test chaos_tests"))
            .count(),
        2
    );
    assert_eq!(workflow.matches("--features simulation").count(), 2);
}

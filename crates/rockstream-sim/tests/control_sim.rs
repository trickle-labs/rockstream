//! Deterministic control plane simulation test.
//!
//! Asserts that worker heartbeats, shard lease assignment, and network partition
//! fencing are correct under fault injection via `buggify!`.

#![cfg(feature = "simulation")]

use std::time::Duration;

use rockstream_control::{ControlService, FrontierAggregator, ShardManager, TopologyCatalog};
use rockstream_runtime::start_worker_client;
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::frontier::ShardFrontierReport;
use rockstream_types::ids::{ShardId, WorkerId};

#[tokio::test]
async fn sim_control_plane_leases() {
    // 1. Initialize buggify with a fixed seed.
    buggify_init(98765);

    // 2. Setup the control service catalog and manager.
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let service = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());

    let handle = service.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir1 = tempfile::tempdir().unwrap();
    let storage_dir2 = tempfile::tempdir().unwrap();

    // 3. Start worker 1
    let (client1, worker_handle1) = start_worker_client(1, &control_url, storage_dir1.path())
        .await
        .unwrap();

    // 4. Start worker 2
    let (client2, worker_handle2) = start_worker_client(2, &control_url, storage_dir2.path())
        .await
        .unwrap();

    // Wait for registration
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(catalog.len(), 2);

    // 5. Worker 1 requests lease for shard 10
    client1.request_shard(ShardId(10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify lease is granted to worker 1
    let leases = client1.leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].shard_id, ShardId(10));
    assert_eq!(leases[0].worker_id, WorkerId(1));

    // 6. Simulate network partition / failure by killing/aborting worker 1
    let inject_partition = buggify!("network.partition", 1.0);
    assert!(inject_partition);

    if inject_partition {
        worker_handle1.abort();
    }

    // Give time for the control plane to detect worker disconnect, clean up, and allow worker 2 to request it.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Lease should be released
    assert!(
        manager.is_empty(),
        "lease should be released by control plane"
    );

    // 7. Worker 2 requests lease for shard 10
    client2.request_shard(ShardId(10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify lease is now held by worker 2
    let leases2 = client2.leases();
    assert_eq!(leases2.len(), 1);
    assert_eq!(leases2[0].shard_id, ShardId(10));
    assert_eq!(leases2[0].worker_id, WorkerId(2));

    worker_handle2.abort();
    handle.shutdown();
    buggify_disable();
}

/// Slice 7: Frontier reordering parity — arbitrary reordering of shard final-epoch
/// reports converges to the same cluster frontier.
///
/// Proof claim (SimRuntime): after all shards have been pre-registered at a baseline
/// epoch, any permutation of their final-epoch updates produces the same cluster
/// frontier (`meet` of final epochs).  This proves commutativity of
/// `FrontierAggregator::ingest` for epoch updates on a fully-registered cluster.
///
/// All 24 permutations of 4 shards → 4 unique final epochs must produce
/// `meet(10, 5, 8, 3) = 3`.
#[test]
fn test_frontier_reordering_convergence() {
    // Final-epoch reports: one per shard.
    let final_reports: Vec<ShardFrontierReport> = vec![
        ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 10,
        },
        ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 5,
        },
        ShardFrontierReport {
            shard_id: ShardId(2),
            epoch: 8,
        },
        ShardFrontierReport {
            shard_id: ShardId(3),
            epoch: 3,
        },
    ];
    let expected = Some(3u64); // meet(10, 5, 8, 3) = 3

    let permutations = permutations_of(&(0..final_reports.len()).collect::<Vec<_>>());
    for perm in &permutations {
        let agg = FrontierAggregator::new();

        // Pre-register all shards at epoch 1 (establishes the full cluster membership).
        for report in &final_reports {
            agg.ingest(ShardFrontierReport {
                shard_id: report.shard_id,
                epoch: 1,
            })
            .unwrap();
        }

        // Deliver final-epoch updates in the permuted order.
        for &idx in perm {
            agg.ingest(final_reports[idx].clone()).unwrap();
        }

        assert_eq!(
            agg.cluster_frontier().epoch,
            expected,
            "permutation {:?} did not converge to meet=3 (got {:?})",
            perm,
            agg.cluster_frontier().epoch
        );
    }
}

/// Generate all permutations of a slice of indices.
fn permutations_of(v: &[usize]) -> Vec<Vec<usize>> {
    if v.is_empty() {
        return vec![vec![]];
    }
    let mut result = vec![];
    for i in 0..v.len() {
        let mut rest = v.to_vec();
        let elem = rest.remove(i);
        for mut perm in permutations_of(&rest) {
            perm.insert(0, elem);
            result.push(perm);
        }
    }
    result
}

/// Slice 7: Frontier aggregation stress — thousands of shards converge correctly.
///
/// Proof claim (SimRuntime): a frontier-aggregation over `N_SHARDS` shards
/// across `N_ROUNDS` simulated operator epochs converges to the correct
/// global minimum without the control plane subscribing to each shard
/// individually.
#[test]
fn test_frontier_aggregation_stress() {
    const N_SHARDS: u64 = 1_000;
    const N_ROUNDS: u64 = 10;

    let agg = FrontierAggregator::new();

    // Round 1: all shards report epoch 1.
    for shard in 0..N_SHARDS {
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(shard),
            epoch: 1,
        })
        .unwrap();
    }
    assert_eq!(
        agg.cluster_frontier().epoch,
        Some(1),
        "all shards at epoch 1 → cluster frontier must be 1"
    );

    // Rounds 2..N_ROUNDS: advance all shards.
    for round in 2..=N_ROUNDS {
        for shard in 0..N_SHARDS {
            agg.ingest(ShardFrontierReport {
                shard_id: ShardId(shard),
                epoch: round,
            })
            .unwrap();
        }
        assert_eq!(
            agg.cluster_frontier().epoch,
            Some(round),
            "at round {round} all shards are at epoch {round}"
        );
    }

    // One shard advances ahead; others remain at N_ROUNDS — cluster stays at N_ROUNDS.
    agg.ingest(ShardFrontierReport {
        shard_id: ShardId(0),
        epoch: N_ROUNDS + 1,
    })
    .unwrap();
    assert_eq!(
        agg.cluster_frontier().epoch,
        Some(N_ROUNDS),
        "lagging shards must hold the cluster frontier back"
    );

    // Now advance all remaining shards to N_ROUNDS + 1.
    for shard in 1..N_SHARDS {
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(shard),
            epoch: N_ROUNDS + 1,
        })
        .unwrap();
    }
    assert_eq!(
        agg.cluster_frontier().epoch,
        Some(N_ROUNDS + 1),
        "cluster frontier must advance once all shards reach epoch {}",
        N_ROUNDS + 1
    );

    // Fill level must be correctly bounded.
    let fill = agg.fill_level();
    assert_eq!(fill.registered, N_SHARDS as usize);
    assert!(
        fill.fill_fraction() < 1.0,
        "stress test must not exceed shard capacity"
    );
}

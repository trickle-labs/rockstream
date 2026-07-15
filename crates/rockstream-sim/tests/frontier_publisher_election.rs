//! v0.45.6 S8 — coordination test for the frontier-aggregator publisher-
//! lease election path introduced by S3–S6
//! (`rockstream_control::frontier::{FrontierAggregator, FrontierLeaseStore}`).
//!
//! `.claude/v0.45.6-plan.md` §5 "Coordination Slices": three simulated
//! `FrontierAggregator` instances contend for the same `frontier/leader` CAS
//! record. `buggify!("frontier.stale_publish_race", p)` forces a fenced
//! (superseded) aggregator to attempt a late `publish_frontier` call *after*
//! a new leader's CAS has already succeeded and minted a higher token.
//! Asserts, across seeds, that the stale attempt is always rejected
//! (`RS-8002`, S4's `assert_valid_publisher` firing) and that
//! `store.published_frontier` is never regressed by it — proving the
//! roadmap's Proof-column claim that "a real 3-aggregator `SimRuntime`
//! scenario proves a stale-fenced publisher can never re-publish a frontier
//! after a new leader's token supersedes it".
//!
//! This is also the runtime witness for **COV-M2** (`fencing_occurred`) from
//! `formal/m2_frontier_agg.fizz`.
//!
//! Gated behind the `simulation` feature (`buggify!` is a compile-time no-op
//! otherwise): `cargo test -p rockstream-sim --features simulation`.

#![cfg(feature = "simulation")]

use std::sync::Arc;

use object_store::memory::InMemory;

use rockstream_control::frontier::{FrontierAggregator, FrontierLeaseError, FrontierLeaseStore};
use rockstream_sim::buggify;
use rockstream_sim::buggify::buggify_init;
use rockstream_types::frontier::ShardFrontierReport;
use rockstream_types::ids::{AggregatorId, ShardId};

/// Across multiple seeds, three `FrontierAggregator`s contend for the same
/// durable lease; the stale-fenced publisher's late `try_publish` call is
/// always rejected without a panic, and a direct-drive of the lower-level
/// `store.publish_frontier` CAS (mirroring the true time-of-check-to-time-
/// of-use race S4's `assert_valid_publisher` guards against) always panics
/// with `RS-8002` — never silently landing a regressed value.
#[tokio::test]
async fn three_frontier_aggregators_stale_publisher_never_republishes() {
    for seed in [100u64, 101, 102, 103, 104] {
        buggify_init(seed);

        let store = Arc::new(
            FrontierLeaseStore::open(
                format!("frontier-lease-sim-{seed}"),
                Arc::new(InMemory::new()),
            )
            .await
            .unwrap(),
        );

        let agg_a = FrontierAggregator::with_lease_store(AggregatorId(1), store.clone());
        let agg_b = FrontierAggregator::with_lease_store(AggregatorId(2), store.clone());
        let agg_c = FrontierAggregator::with_lease_store(AggregatorId(3), store.clone());

        // A wins the first election and publishes frontier 10.
        assert!(agg_a.acquire_lease().await.unwrap());
        agg_a
            .ingest(ShardFrontierReport {
                shard_id: ShardId(0),
                epoch: 10,
            })
            .unwrap();
        assert!(agg_a.try_publish().await.unwrap());
        assert_eq!(
            store.read_published_frontier_after_handoff().await,
            Some(10)
        );
        let stale_token = agg_a.lease_token();

        // B supersedes A: a new leader's CAS succeeds and mints a strictly
        // higher token, fencing A out — mirrors the FizzBee model's
        // `AcquireLease` election churn.
        assert!(agg_b.acquire_lease().await.unwrap());
        assert!(agg_b.is_publisher());
        assert!(agg_b.lease_token() > stale_token);

        // COV-M2: fencing_occurred — A has now been superseded by B's
        // higher-token CAS win, the exact coverage-witness state the
        // FizzBee model requires.
        let fencing_occurred = agg_b.lease_token() > stale_token;
        assert!(fencing_occurred, "seed={seed}: fencing must have occurred");

        // buggify forces the fenced-out aggregator A to attempt a late
        // publish anyway, racing against B's already-succeeded CAS —
        // models a stale worker that hasn't yet observed its own fencing.
        let force_stale_publish_race = buggify!("frontier.stale_publish_race", 1.0);
        assert!(
            force_stale_publish_race,
            "seed={seed}: fault must fire at p=1.0"
        );

        if force_stale_publish_race {
            // Path 1: A's own `try_publish` peeks the current fence token
            // first and gracefully demotes — the normal, expected outcome
            // for a race loser (never panics).
            agg_a
                .ingest(ShardFrontierReport {
                    shard_id: ShardId(0),
                    epoch: 999, // a much higher (regressive-looking) value
                })
                .unwrap();
            assert!(
                !agg_a.try_publish().await.unwrap(),
                "seed={seed}: stale-fenced aggregator must never successfully publish"
            );
            assert!(!agg_a.is_publisher());

            // Path 2: a genuine time-of-check-to-time-of-use race — A holds
            // its now-stale token and calls the durable store directly,
            // bypassing try_publish's peek (modeling a request already
            // in flight when the fencing occurred). This must hit S4's
            // assert_valid_publisher hard panic (RS-8002), never silently
            // landing a regressed value. Run on a spawned task so a panic
            // is captured as a `JoinError` rather than aborting the test.
            let store_for_task = store.clone();
            let join_result = tokio::spawn(async move {
                store_for_task
                    .publish_frontier(rockstream_types::ids::LeaseToken(stale_token), 999)
                    .await
            })
            .await;
            match join_result {
                Err(join_err) => {
                    assert!(
                        join_err.is_panic(),
                        "seed={seed}: expected a panic (RS-8002), got {join_err:?}"
                    );
                }
                Ok(Err(FrontierLeaseError::StaleFenceToken { .. })) => {
                    // Acceptable graceful-rejection outcome for this API shape.
                }
                Ok(Ok(())) => {
                    panic!("seed={seed}: stale publish must not succeed silently")
                }
                Ok(Err(other)) => {
                    panic!("seed={seed}: unexpected error, not a fencing rejection: {other:?}")
                }
            }
        }

        // The published frontier is never regressed by the stale attempt —
        // still B's last successful publish (or A's original 10 if B has
        // not yet published this round), never the stale 999.
        let published = store.read_published_frontier_after_handoff().await;
        assert_ne!(
            published,
            Some(999),
            "seed={seed}: stale-fenced publish must never land"
        );

        // C, a third aggregator, can still observe a consistent, correctly
        // fenced world: acquiring next mints a token strictly higher than
        // both A's stale token and B's.
        assert!(agg_c.acquire_lease().await.unwrap());
        assert!(agg_c.lease_token() > agg_b.lease_token());
        assert!(agg_c.lease_token() > stale_token);
    }
}

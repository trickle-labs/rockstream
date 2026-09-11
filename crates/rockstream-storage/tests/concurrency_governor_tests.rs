//! Concurrency Governor Tests for Background Compaction, Backfill, and Migration (v0.62.1 Slice 5 / Phase 3b).

use rockstream_storage::concurrency_governor::ConcurrencyGovernor;
use rockstream_types::config::WorkerSection;
#[test]
fn test_compaction_and_backfill_concurrency_limits_enforced() {
    let cfg = WorkerSection {
        max_compaction_concurrency: 2,
        max_backfill_concurrency: 1,
        max_migration_concurrency: 1,
        ..Default::default()
    };

    let governor = ConcurrencyGovernor::from_worker_config(&cfg);
    assert_eq!(governor.max_compaction(), 2);
    assert_eq!(governor.max_backfill(), 1);
    assert_eq!(governor.max_migration(), 1);
    assert_eq!(governor.available_compaction(), 2);
    assert_eq!(governor.available_backfill(), 1);
    assert_eq!(governor.available_migration(), 1);

    // Acquire compaction permits
    let p1 = governor.try_acquire_compaction().expect("permit 1");
    let p2 = governor.try_acquire_compaction().expect("permit 2");
    assert_eq!(governor.available_compaction(), 0);

    // Third compaction permit must be rejected with RS-9001
    let p3_res = governor.try_acquire_compaction();
    assert!(p3_res.is_err());
    let err = p3_res.unwrap_err();
    assert!(err.to_string().contains("RS-9001"));
    assert!(err.to_string().contains("compaction concurrency limit"));

    // Release permit 1, now compaction should succeed
    drop(p1);
    assert_eq!(governor.available_compaction(), 1);
    let p3 = governor.try_acquire_compaction().expect("permit 3");
    drop(p2);
    drop(p3);
    assert_eq!(governor.available_compaction(), 2);
}

#[test]
fn test_compaction_concurrency_bounded_and_throttled() {
    let governor = ConcurrencyGovernor::new(2, 1, 1);

    let p1 = governor.try_acquire_compaction().unwrap();
    let p2 = governor.try_acquire_compaction().unwrap();
    assert!(governor.try_acquire_compaction().is_err());

    drop(p2);
    let p2_reacquired = governor.try_acquire_compaction().unwrap();
    drop(p1);
    drop(p2_reacquired);
    assert_eq!(governor.available_compaction(), 2);
}

#[test]
fn test_backfill_concurrency_shed_first_under_pressure() {
    let governor = ConcurrencyGovernor::new(2, 1, 1);

    let bf1 = governor.try_acquire_backfill().expect("backfill 1");
    assert_eq!(governor.available_backfill(), 0);

    // Further backfill rejected immediately with RS-9001
    let bf2 = governor.try_acquire_backfill();
    assert!(bf2.is_err());
    let err = bf2.unwrap_err();
    assert!(err.to_string().contains("RS-9001"));
    assert!(err.to_string().contains("backfill concurrency limit"));

    drop(bf1);
    assert_eq!(governor.available_backfill(), 1);
}

#[test]
fn test_migration_concurrency_bounded_by_budget() {
    let governor = ConcurrencyGovernor::new(2, 1, 1);

    let mig1 = governor.try_acquire_migration().expect("migration 1");
    assert_eq!(governor.available_migration(), 0);

    let mig2 = governor.try_acquire_migration();
    assert!(mig2.is_err());
    let err = mig2.unwrap_err();
    assert!(err.to_string().contains("RS-9001"));
    assert!(err.to_string().contains("migration concurrency limit"));

    drop(mig1);
    assert_eq!(governor.available_migration(), 1);
}

use r1_local_harness::scale_program::{
    canonical_r3_ladder, scaling_efficiency, CheckerViolation, CompleteCostReport, DurationProfile,
    EvidenceProvenance, FaultInjectionEvent, IndependentQualificationChecker, KeyDistribution,
    LadderCellStatus, OwnershipWriteRecord, RetainedStateMultiplier, ScaleQualificationBundle,
    VisibilitySnapshotRecord, WorkloadShape,
};
use std::collections::{BTreeMap, BTreeSet};

fn sample_measured_bundle() -> ScaleQualificationBundle {
    let achieved_rate = 250_000.0;
    let total_hourly = 45.0;
    let cpm = (total_hourly * 1_000_000.0) / (achieved_rate * 3_600.0);

    let mut snap = BTreeMap::new();
    snap.insert("sales_by_region".to_string(), 120);
    snap.insert("top_customers".to_string(), 120);

    ScaleQualificationBundle {
        run_id: "flagship-cdc-4w-20261122".to_string(),
        provenance: EvidenceProvenance::MeasuredExecution,
        commit_sha: "2aaa8c92deadbeef0123456789abcdef01234567".to_string(),
        binary_sha256: "f".repeat(64),
        hardware_spec: "4x c7i.4xlarge (16 vCPU, 32 GiB RAM) + S3 us-east-1".to_string(),
        worker_count: 4,
        worker_pids: vec![10101, 10102, 10103, 10104],
        offered_rate_rows_per_sec: 250_000.0,
        achieved_rate_rows_per_sec: achieved_rate,
        freshness_p50_ms: 42.0,
        freshness_p95_ms: 185.0,
        freshness_p99_ms: 410.0,
        read_p99_ms: 6.5,
        errors_and_timeouts: 0,
        max_rss_bytes: 18 * 1024 * 1024 * 1024,
        network_bytes: 4 * 1024 * 1024 * 1024,
        storage_requests: 125_000,
        compaction_debt_bytes: 64 * 1024 * 1024,
        expected_oracle_rows: vec![
            vec!["east".to_string(), "1500".to_string()],
            vec!["west".to_string(), "2300".to_string()],
            vec!["north".to_string(), "900".to_string()],
        ],
        observed_rockstream_rows: vec![
            vec!["north".to_string(), "900".to_string()],
            vec!["east".to_string(), "1500".to_string()],
            vec!["west".to_string(), "2300".to_string()],
        ],
        ownership_writes: vec![
            OwnershipWriteRecord {
                shard_id: 0,
                worker_id: "worker-0".to_string(),
                writer_lease_epoch: 5,
                authoritative_lease_epoch: 5,
                committed_epoch: 120,
            },
            OwnershipWriteRecord {
                shard_id: 1,
                worker_id: "worker-1".to_string(),
                writer_lease_epoch: 7,
                authoritative_lease_epoch: 7,
                committed_epoch: 120,
            },
        ],
        visibility_snapshots: vec![VisibilitySnapshotRecord {
            read_id: 1,
            view_or_shard_epochs: snap,
        }],
        planned_faults: vec![
            FaultInjectionEvent {
                fault_name: "scale_out_2_to_4".to_string(),
                target_process_or_pod: "rockstream-worker-2,rockstream-worker-3".to_string(),
                fired_at_rfc3339: Some("2026-11-22T12:05:00Z".to_string()),
                recovered_at_rfc3339: Some("2026-11-22T12:05:18Z".to_string()),
            },
            FaultInjectionEvent {
                fault_name: "worker_kill".to_string(),
                target_process_or_pod: "rockstream-worker-1".to_string(),
                fired_at_rfc3339: Some("2026-11-22T12:10:00Z".to_string()),
                recovered_at_rfc3339: Some("2026-11-22T12:10:09Z".to_string()),
            },
            FaultInjectionEvent {
                fault_name: "online_backup_and_clean_restore".to_string(),
                target_process_or_pod: "rockstream-control-0".to_string(),
                fired_at_rfc3339: Some("2026-11-22T12:15:00Z".to_string()),
                recovered_at_rfc3339: Some("2026-11-22T12:15:24Z".to_string()),
            },
        ],
        cost_report: CompleteCostReport {
            serving_gateways_hourly_usd: 4.0,
            control_plane_hourly_usd: 3.0,
            maintenance_workers_hourly_usd: 20.0,
            compaction_hourly_usd: 4.0,
            retained_history_storage_hourly_usd: 3.5,
            object_store_requests_hourly_usd: 2.5,
            backups_hourly_usd: 2.0,
            failover_headroom_hourly_usd: 4.0,
            cross_az_traffic_hourly_usd: 2.0,
            total_hourly_usd: total_hourly,
            freshness_slo_ms: 1000.0,
            qualified_changes_per_sec: achieved_rate,
            cost_per_million_correct_durable_changes_usd: cpm,
            cloud_instance_class: "c7i.4xlarge".to_string(),
            region: "us-east-1".to_string(),
            pricing_date_iso: "2026-11-22".to_string(),
            pricing_scope: "cloud-measured-dated".to_string(),
        },
    }
}

#[test]
fn verification_1_checker_rejects_every_negative_control() {
    let baseline = sample_measured_bundle();
    IndependentQualificationChecker::verify(&baseline).expect("valid baseline must pass");

    let report = IndependentQualificationChecker::run_negative_control_suite(&baseline)
        .expect("negative-control suite must verify all 6 rejections");
    assert!(report.all_negative_controls_rejected);
    assert_eq!(
        report.verified_controls,
        vec![
            "dropped_row_rejected",
            "duplicate_row_rejected",
            "stale_owner_write_rejected",
            "torn_visibility_rejected",
            "fault_never_triggered_rejected",
            "synthetic_fixture_rejected",
        ]
    );
}

#[test]
fn verification_2_synthetic_fixture_fails_closed() {
    let mut bundle = sample_measured_bundle();
    bundle.provenance = EvidenceProvenance::SyntheticFixture;
    let err = IndependentQualificationChecker::verify(&bundle).unwrap_err();
    assert!(matches!(
        err,
        CheckerViolation::SyntheticFixtureDisallowed { .. }
    ));

    let mut named_synthetic = sample_measured_bundle();
    named_synthetic.run_id = "sample_reference_run_8w".to_string();
    let err2 = IndependentQualificationChecker::verify(&named_synthetic).unwrap_err();
    assert!(matches!(
        err2,
        CheckerViolation::SyntheticFixtureDisallowed { .. }
    ));
}

#[test]
fn r3_scaling_efficiency_and_expanding_ladder_cover_all_required_dimensions() {
    // Scaling efficiency(n) = Q_n / (n * Q_1)
    let eff_4w = scaling_efficiency(360_000.0, 100_000.0, 4).unwrap();
    assert!((eff_4w - 0.90).abs() < 1e-12);
    let eff_16w = scaling_efficiency(1_280_000.0, 100_000.0, 16).unwrap();
    assert!((eff_16w - 0.80).abs() < 1e-12);
    assert!(scaling_efficiency(100_000.0, 0.0, 4).is_err());
    assert!(scaling_efficiency(100_000.0, 100_000.0, 0).is_err());

    let ladder = canonical_r3_ladder();

    // Verify all worker tiers 1, 2, 4, 8, 16, 32, 64 are represented
    let workers: BTreeSet<usize> = ladder.iter().map(|c| c.worker_count).collect();
    assert_eq!(workers, BTreeSet::from([1, 2, 4, 8, 16, 32, 64]));

    // Verify 1x, 4x, 10x RAM multipliers
    let ram_tiers: BTreeSet<RetainedStateMultiplier> =
        ladder.iter().map(|c| c.retained_state).collect();
    assert_eq!(
        ram_tiers,
        BTreeSet::from([
            RetainedStateMultiplier::Ram1x,
            RetainedStateMultiplier::Ram4x,
            RetainedStateMultiplier::Ram10x,
        ])
    );

    // Verify uniform and 50x skewed hot keys
    let dists: BTreeSet<KeyDistribution> = ladder.iter().map(|c| c.distribution).collect();
    assert_eq!(
        dists,
        BTreeSet::from([KeyDistribution::Uniform, KeyDistribution::Skewed50xHotKey])
    );

    // Verify all required workload shapes
    let shapes: BTreeSet<WorkloadShape> = ladder.iter().map(|c| c.workload_shape).collect();
    assert_eq!(
        shapes,
        BTreeSet::from([
            WorkloadShape::Aggregation,
            WorkloadShape::CoPartitionedJoin,
            WorkloadShape::RepartitionedJoin,
            WorkloadShape::DependentViewsSingle,
            WorkloadShape::DependentViews20SharedArrangements,
            WorkloadShape::BoundedWindow,
            WorkloadShape::ConcurrentHighQpsReadsAndSubscribe,
        ])
    );

    // Verify short diagnostic and 72-hour soak are both present
    let durations: BTreeSet<DurationProfile> = ladder.iter().map(|c| c.duration_profile).collect();
    assert_eq!(
        durations,
        BTreeSet::from([
            DurationProfile::ShortDiagnostic,
            DurationProfile::Soak72HourMixedLoad,
        ])
    );

    // Verify every blocked cell names an owning issue and blocker
    for cell in &ladder {
        if let LadderCellStatus::Blocked {
            owning_issue,
            blocker,
        } = &cell.status
        {
            assert!(!owning_issue.is_empty());
            assert!(!blocker.is_empty());
        }
    }
}

#[test]
fn r4_complete_cost_accounting_rejects_omitted_or_tampered_components() {
    let mut bundle = sample_measured_bundle();
    bundle.cost_report.cross_az_traffic_hourly_usd += 5.0; // total_hourly_usd no longer matches
    let err = IndependentQualificationChecker::verify(&bundle).unwrap_err();
    assert!(matches!(
        err,
        CheckerViolation::InvalidCostAccounting { .. }
    ));

    let mut bad_cpm = sample_measured_bundle();
    bad_cpm
        .cost_report
        .cost_per_million_correct_durable_changes_usd = 0.0001;
    let err2 = IndependentQualificationChecker::verify(&bad_cpm).unwrap_err();
    assert!(matches!(
        err2,
        CheckerViolation::InvalidCostAccounting { .. }
    ));
}

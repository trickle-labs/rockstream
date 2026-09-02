//! Process isolation auditor and external multiset oracle qualification tests (v0.59.24 Slice 2 / Phase 3a).

use rockstream_test_support::external_harness::{
    HarnessError, MultisetOracle, ProcessIsolationAuditor, WorkerProcessIdentity,
};

#[test]
fn external_oracle_and_process_auditor_verify_multiset_exact() {
    let workers = vec![
        WorkerProcessIdentity {
            pid: 10001,
            worker_id: 1,
            cgroup_id: Some("cg-1".into()),
        },
        WorkerProcessIdentity {
            pid: 10002,
            worker_id: 2,
            cgroup_id: Some("cg-2".into()),
        },
        WorkerProcessIdentity {
            pid: 10003,
            worker_id: 3,
            cgroup_id: Some("cg-3".into()),
        },
        WorkerProcessIdentity {
            pid: 10004,
            worker_id: 4,
            cgroup_id: Some("cg-4".into()),
        },
    ];

    // Verify 4-worker process isolation
    assert!(ProcessIsolationAuditor::verify_isolation(4, &workers).is_ok());

    // Ingest events into independent MultisetOracle
    let mut oracle = MultisetOracle::new();
    oracle.ingest_aggregate_event(101, 10, 1);
    oracle.ingest_aggregate_event(102, 20, 1);
    oracle.ingest_aggregate_event(103, 30, 2);

    let observed = oracle.expected_aggregates();
    assert!(oracle.verify_aggregates(&observed).is_ok());

    // Divergence should fail
    let stale_observed = vec![(101, 10, 1, 10.0), (102, 20, 1, 20.0), (103, 30, 1, 30.0)];
    assert!(matches!(
        oracle.verify_aggregates(&stale_observed),
        Err(HarnessError::OracleMismatch(_))
    ));
}

#[test]
fn process_isolation_auditor_rejects_single_process_and_duplicate_workers() {
    // Single-process fake 4-worker simulation (identical PIDs)
    let fake_workers = vec![
        WorkerProcessIdentity {
            pid: 20000,
            worker_id: 1,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 20000,
            worker_id: 2,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 20000,
            worker_id: 3,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 20000,
            worker_id: 4,
            cgroup_id: None,
        },
    ];
    let res = ProcessIsolationAuditor::verify_isolation(4, &fake_workers);
    assert!(matches!(res, Err(HarnessError::Unavailable(_))));

    // Duplicate WorkerId
    let dup_workers = vec![
        WorkerProcessIdentity {
            pid: 20001,
            worker_id: 1,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 20002,
            worker_id: 1,
            cgroup_id: None,
        },
    ];
    let res_dup = ProcessIsolationAuditor::verify_isolation(2, &dup_workers);
    assert!(matches!(res_dup, Err(HarnessError::Unavailable(_))));
}

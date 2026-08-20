//! v0.59.5 Slice 2: Benchmark Process Isolation Tests.
//!
//! Asserts that missing real multi-worker process isolation (duplicate PIDs or WorkerIds)
//! fails closed with `UNAVAILABLE`, never simulated by single-process handles.

use rockstream_test_support::external_harness::{
    HarnessError, ProcessIsolationAuditor, WorkerProcessIdentity,
};

#[test]
fn test_multi_worker_single_process_simulation_rejected_with_unavailable() {
    // 4 workers declared, but all share the same PID (in-process handles)
    let simulated_workers = vec![
        WorkerProcessIdentity {
            pid: 42000,
            worker_id: 1,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 42000,
            worker_id: 2,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 42000,
            worker_id: 3,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 42000,
            worker_id: 4,
            cgroup_id: None,
        },
    ];

    let res = ProcessIsolationAuditor::verify_isolation(4, &simulated_workers);
    assert!(matches!(res, Err(HarnessError::Unavailable(_))));
    if let Err(HarnessError::Unavailable(msg)) = res {
        assert!(
            msg.contains("duplicate PID"),
            "Error must clearly identify fake process simulation: {msg}"
        );
    }
}

#[test]
fn test_duplicate_worker_id_rejected_with_unavailable() {
    // Distinct PIDs, but duplicate WorkerId
    let duplicate_workers = vec![
        WorkerProcessIdentity {
            pid: 42001,
            worker_id: 1,
            cgroup_id: None,
        },
        WorkerProcessIdentity {
            pid: 42002,
            worker_id: 1,
            cgroup_id: None,
        },
    ];

    let res = ProcessIsolationAuditor::verify_isolation(2, &duplicate_workers);
    assert!(matches!(res, Err(HarnessError::Unavailable(_))));
    if let Err(HarnessError::Unavailable(msg)) = res {
        assert!(
            msg.contains("duplicate WorkerId"),
            "Error must identify duplicate WorkerId: {msg}"
        );
    }
}

#[test]
fn test_declared_count_mismatch_rejected_with_unavailable() {
    let workers = vec![WorkerProcessIdentity {
        pid: 42001,
        worker_id: 1,
        cgroup_id: None,
    }];

    let res = ProcessIsolationAuditor::verify_isolation(4, &workers);
    assert!(matches!(res, Err(HarnessError::Unavailable(_))));
}

#[test]
fn test_real_multi_worker_processes_accepted() {
    let real_workers = vec![
        WorkerProcessIdentity {
            pid: 50001,
            worker_id: 1,
            cgroup_id: Some("worker-1".into()),
        },
        WorkerProcessIdentity {
            pid: 50002,
            worker_id: 2,
            cgroup_id: Some("worker-2".into()),
        },
        WorkerProcessIdentity {
            pid: 50003,
            worker_id: 3,
            cgroup_id: Some("worker-3".into()),
        },
        WorkerProcessIdentity {
            pid: 50004,
            worker_id: 4,
            cgroup_id: Some("worker-4".into()),
        },
        WorkerProcessIdentity {
            pid: 50005,
            worker_id: 5,
            cgroup_id: Some("worker-5".into()),
        },
        WorkerProcessIdentity {
            pid: 50006,
            worker_id: 6,
            cgroup_id: Some("worker-6".into()),
        },
        WorkerProcessIdentity {
            pid: 50007,
            worker_id: 7,
            cgroup_id: Some("worker-7".into()),
        },
        WorkerProcessIdentity {
            pid: 50008,
            worker_id: 8,
            cgroup_id: Some("worker-8".into()),
        },
    ];

    assert!(ProcessIsolationAuditor::verify_isolation(8, &real_workers).is_ok());
}

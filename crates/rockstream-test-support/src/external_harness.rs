//! v0.59.5 Slice 2: Independent External Benchmark and Oracle Test Harness.
//!
//! Imports NO RockStream engine internal crates. Operates strictly over external
//! process boundaries (PIDs/cgroups, WorkerIds, CLI/PGWire, and datasets).
//! Enforces true process isolation and provides an independent multiset oracle.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;

/// Error returned by the external benchmark harness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HarnessError {
    /// Multi-worker process isolation requirement violated.
    Unavailable(String),
    /// Oracle multiset mismatch between expected and actual query results.
    OracleMismatch(String),
    /// External process failure.
    ProcessError(String),
}

impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(msg) => write!(f, "UNAVAILABLE: {msg}"),
            Self::OracleMismatch(msg) => write!(f, "ORACLE_MISMATCH: {msg}"),
            Self::ProcessError(msg) => write!(f, "PROCESS_ERROR: {msg}"),
        }
    }
}

impl std::error::Error for HarnessError {}

/// Identity descriptor for a worker process.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerProcessIdentity {
    pub pid: u32,
    pub worker_id: u64,
    pub cgroup_id: Option<String>,
}

/// Process isolation auditor.
///
/// Enforces that a declared worker count N is valid only when every worker
/// has a distinct OS PID / cgroup and distinct `WorkerId`.
/// Single-process simulations or duplicate identities fail closed with `UNAVAILABLE`.
#[derive(Debug, Default)]
pub struct ProcessIsolationAuditor;

impl ProcessIsolationAuditor {
    pub fn verify_isolation(
        declared_workers: usize,
        workers: &[WorkerProcessIdentity],
    ) -> Result<(), HarnessError> {
        if declared_workers == 0 {
            return Err(HarnessError::Unavailable(
                "declared workers must be > 0".into(),
            ));
        }
        if workers.len() != declared_workers {
            return Err(HarnessError::Unavailable(format!(
                "worker count mismatch: declared {} but found {}",
                declared_workers,
                workers.len()
            )));
        }

        let mut seen_pids = HashSet::new();
        let mut seen_worker_ids = HashSet::new();

        for w in workers {
            if !seen_pids.insert(w.pid) {
                return Err(HarnessError::Unavailable(format!(
                    "duplicate PID {} detected: multi-worker execution must use distinct OS processes",
                    w.pid
                )));
            }
            if !seen_worker_ids.insert(w.worker_id) {
                return Err(HarnessError::Unavailable(format!(
                    "duplicate WorkerId {} detected: every worker must have an authoritative distinct WorkerId",
                    w.worker_id
                )));
            }
        }

        Ok(())
    }
}

/// Independent multiset oracle.
///
/// Computes exact expected aggregations and group states directly from raw input
/// event sequences without depending on any RockStream engine internals.
#[derive(Debug, Default, Clone)]
pub struct MultisetOracle {
    /// group_key -> (sum, count)
    aggregates: BTreeMap<i64, (i64, i64)>,
    /// distinct elements -> multiplicity weight
    distinct_elements: BTreeMap<Vec<u8>, i64>,
}

impl MultisetOracle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Process a raw aggregate input event: `(group_key, value, weight)`.
    pub fn ingest_aggregate_event(&mut self, key: i64, value: i64, weight: i64) {
        let entry = self.aggregates.entry(key).or_insert((0, 0));
        entry.0 += value * weight;
        entry.1 += weight;
        if entry.1 == 0 {
            self.aggregates.remove(&key);
        }
    }

    /// Process a raw distinct event: `(bytes, weight)`.
    pub fn ingest_distinct_event(&mut self, item: Vec<u8>, weight: i64) {
        let entry = self.distinct_elements.entry(item.clone()).or_insert(0);
        *entry += weight;
        if *entry <= 0 {
            self.distinct_elements.remove(&item);
        }
    }

    /// Return expected aggregate results as a sorted vector: `[(key, sum, count, avg)]`.
    pub fn expected_aggregates(&self) -> Vec<(i64, i64, i64, f64)> {
        self.aggregates
            .iter()
            .map(|(&k, &(sum, count))| {
                let avg = sum as f64 / count as f64;
                (k, sum, count, avg)
            })
            .collect()
    }

    /// Verify actual query results against oracle expected aggregates.
    pub fn verify_aggregates(&self, actual: &[(i64, i64, i64, f64)]) -> Result<(), HarnessError> {
        let expected = self.expected_aggregates();
        if expected.len() != actual.len() {
            return Err(HarnessError::OracleMismatch(format!(
                "aggregate row count mismatch: expected {} rows, actual {} rows",
                expected.len(),
                actual.len()
            )));
        }

        for (exp, act) in expected.iter().zip(actual.iter()) {
            if exp.0 != act.0 || exp.1 != act.1 || exp.2 != act.2 || (exp.3 - act.3).abs() > 1e-6 {
                return Err(HarnessError::OracleMismatch(format!(
                    "mismatch on key {}: expected ({}, {}, {}), got ({}, {}, {})",
                    exp.0, exp.1, exp.2, exp.3, act.1, act.2, act.3
                )));
            }
        }
        Ok(())
    }
}

/// S1 baseline measurement snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S1BaselineMetrics {
    pub worker_count: usize,
    pub group_cardinality: usize,
    pub throughput_events_per_sec: f64,
    pub p50_freshness_ms: f64,
    pub p95_freshness_ms: f64,
    pub p99_freshness_ms: f64,
    pub logical_write_bytes: usize,
    pub slatedb_storage_bytes: usize,
    pub write_amplification: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_isolation_verification_passes_distinct() {
        let workers = vec![
            WorkerProcessIdentity {
                pid: 1001,
                worker_id: 1,
                cgroup_id: None,
            },
            WorkerProcessIdentity {
                pid: 1002,
                worker_id: 2,
                cgroup_id: None,
            },
            WorkerProcessIdentity {
                pid: 1003,
                worker_id: 3,
                cgroup_id: None,
            },
            WorkerProcessIdentity {
                pid: 1004,
                worker_id: 4,
                cgroup_id: None,
            },
        ];
        assert!(ProcessIsolationAuditor::verify_isolation(4, &workers).is_ok());
    }

    #[test]
    fn test_process_isolation_rejects_duplicate_pid_with_unavailable() {
        let workers = vec![
            WorkerProcessIdentity {
                pid: 1001,
                worker_id: 1,
                cgroup_id: None,
            },
            WorkerProcessIdentity {
                pid: 1001,
                worker_id: 2,
                cgroup_id: None,
            },
        ];
        let err = ProcessIsolationAuditor::verify_isolation(2, &workers).unwrap_err();
        match err {
            HarnessError::Unavailable(msg) => assert!(msg.contains("duplicate PID")),
            _ => panic!("expected UNAVAILABLE"),
        }
    }

    #[test]
    fn test_multiset_oracle_aggregates_and_retractions() {
        let mut oracle = MultisetOracle::new();
        oracle.ingest_aggregate_event(1, 10, 1);
        oracle.ingest_aggregate_event(1, 20, 1);
        oracle.ingest_aggregate_event(2, 50, 1);
        oracle.ingest_aggregate_event(1, 10, -1); // retract first item

        let expected = oracle.expected_aggregates();
        assert_eq!(expected.len(), 2);
        assert_eq!(expected[0], (1, 20, 1, 20.0));
        assert_eq!(expected[1], (2, 50, 1, 50.0));

        let actual = vec![(1, 20, 1, 20.0), (2, 50, 1, 50.0)];
        assert!(oracle.verify_aggregates(&actual).is_ok());
    }
}

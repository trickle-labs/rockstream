//! v0.59.17 Slice 5 — unified `Oracle` trait contract tests.
//!
//! `rockstream_sim::qualification::oracle_auditor::OracleAuditor` is a
//! dev-dependency-only oracle (`rockstream-sim` is not a normal dependency
//! of `rockstream-oracle`), so its `Oracle` adapter is implemented here
//! rather than in `src/scenario/oracle.rs`.

use rockstream_oracle::scenario::oracle::{Oracle, OracleVerifyError, SourceProvenance};
use rockstream_sim::qualification::oracle_auditor::{MultisetDiff, OracleAuditor};
use rockstream_sim::qualification::workload::{MutationOp, WorkloadRecord};
use std::collections::BTreeMap;

/// Wraps [`OracleAuditor`], built by ingesting raw workload records
/// independently of the live view state it will check.
struct OracleAuditorAdapter {
    auditor: OracleAuditor,
}

impl OracleAuditorAdapter {
    fn from_independent_records(records: &[WorkloadRecord]) -> Self {
        let mut auditor = OracleAuditor::new();
        auditor.ingest(records);
        Self { auditor }
    }
}

impl Oracle for OracleAuditorAdapter {
    type Actual = BTreeMap<String, i64>;
    type Mismatch = MultisetDiff;

    fn source_provenance(&self) -> SourceProvenance {
        SourceProvenance::Independent
    }

    fn check(&self, actual: &Self::Actual) -> Result<(), Self::Mismatch> {
        self.auditor.verify_multiset(actual)
    }
}

fn record(key: &str, val: i64) -> WorkloadRecord {
    WorkloadRecord {
        key: key.to_string(),
        val,
        op: MutationOp::Insert,
        event_time_ms: 0,
        ingest_epoch: 0,
        sequence_num: 0,
    }
}

#[test]
fn oracle_auditor_adapter_matches_live_state() {
    let oracle = OracleAuditorAdapter::from_independent_records(&[record("a", 1)]);
    let live_state = BTreeMap::from([("a".to_string(), 1)]);

    assert_eq!(oracle.verify(&live_state), Ok(()));
}

#[test]
fn oracle_auditor_adapter_reports_mismatch() {
    let oracle = OracleAuditorAdapter::from_independent_records(&[record("a", 1)]);
    let wrong_live_state = BTreeMap::from([("a".to_string(), 2)]);

    let result = oracle.verify(&wrong_live_state);

    assert!(matches!(result, Err(OracleVerifyError::Mismatch(_))));
}

//! Unified independent-oracle contract (`TST-004`).
//!
//! Three oracle implementations already exist, fragmented across three
//! crates: [`crate::harness::assert_oracle_noop`],
//! `rockstream_test_support::external_harness::{MultisetOracle,
//! ProcessIsolationAuditor}`, and (dev-dependency only, so it implements
//! this trait from its own test file) `rockstream_sim`'s
//! `qualification::oracle_auditor::OracleAuditor`.
//!
//! `sign-offs/scalability-value-review-v0.59.7.md` (RED) found `MultisetOracle`'s
//! original test self-referential: it validated a fixture against its own
//! echoed-back expected value, not against output from a running engine
//! process. That was fixed once, ad hoc. [`Oracle::verify`] makes the fix
//! generic and enforced: any oracle tagged [`SourceProvenance::SelfReferential`]
//! fails verification here, in the shared trait, before its own `check` logic
//! ever runs — so no future adapter can reintroduce the same bug silently.

use std::fmt;

/// Where an oracle's reference data came from.
///
/// An oracle proves nothing if its "expected" value was derived from the
/// same value it is about to check — that is exactly the anti-pattern this
/// type exists to name and reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceProvenance {
    /// Reference data was computed from an independently sourced input
    /// (a real Postgres/Kafka instance, raw workload records, a second
    /// derivation from the same raw deltas) — never from the actual value
    /// under test.
    Independent,
    /// Reference data was derived from (or is identical to) the actual
    /// value under test. Always rejected by [`Oracle::verify`].
    SelfReferential,
}

/// Error returned by [`Oracle::verify`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleVerifyError<M> {
    /// The oracle's reference data was not independently sourced.
    SelfReferentialFixture,
    /// The independently sourced reference disagreed with the actual value.
    Mismatch(M),
}

impl<M: fmt::Display> fmt::Display for OracleVerifyError<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SelfReferentialFixture => write!(
                f,
                "oracle rejected: reference data was not independently sourced"
            ),
            Self::Mismatch(m) => write!(f, "oracle mismatch: {m}"),
        }
    }
}

/// An independent oracle: computes or holds ground truth sourced separately
/// from the value it will check, and verifies that value against it.
pub trait Oracle {
    /// The value under test.
    type Actual;
    /// Describes a disagreement between reference and actual.
    type Mismatch;

    /// Where this oracle's reference data came from. Implementations built
    /// from a genuinely independent source must return
    /// [`SourceProvenance::Independent`]; anything else must return
    /// [`SourceProvenance::SelfReferential`].
    fn source_provenance(&self) -> SourceProvenance;

    /// Compare `actual` against the reference. Only called by [`Oracle::verify`]
    /// after provenance has already been checked.
    fn check(&self, actual: &Self::Actual) -> Result<(), Self::Mismatch>;

    /// Enforced entry point. Fails closed on a self-referential fixture
    /// instead of running `check` at all, so a self-referential oracle can
    /// never report a pass.
    fn verify(&self, actual: &Self::Actual) -> Result<(), OracleVerifyError<Self::Mismatch>> {
        if self.source_provenance() == SourceProvenance::SelfReferential {
            return Err(OracleVerifyError::SelfReferentialFixture);
        }
        self.check(actual).map_err(OracleVerifyError::Mismatch)
    }
}

/// Adapter over `rockstream_test_support::external_harness::MultisetOracle`.
pub mod multiset {
    use super::{Oracle, SourceProvenance};
    use rockstream_test_support::external_harness::{HarnessError, MultisetOracle};

    /// Wraps [`MultisetOracle`], built from independently ingested raw
    /// events (never from the actual query result it will check).
    pub struct MultisetOracleAdapter {
        oracle: MultisetOracle,
        provenance: SourceProvenance,
    }

    impl MultisetOracleAdapter {
        /// The correct, independent construction path: an oracle built by
        /// ingesting raw `(key, value, weight)` events, separately from
        /// whatever query result it will later check.
        pub fn from_independent_events(events: &[(i64, i64, i64)]) -> Self {
            let mut oracle = MultisetOracle::new();
            for &(key, value, weight) in events {
                oracle.ingest_aggregate_event(key, value, weight);
            }
            Self {
                oracle,
                provenance: SourceProvenance::Independent,
            }
        }

        /// The rejected anti-pattern named by `sign-offs/scalability-value-review-v0.59.7.md`:
        /// build the "expected" oracle directly from the actual result under
        /// test, echoing it back at itself. Exists only so the contract test
        /// can prove [`Oracle::verify`] refuses to pass such a fixture.
        pub fn from_echoed_actual(actual: &[(i64, i64, i64, f64)]) -> Self {
            let mut oracle = MultisetOracle::new();
            for &(key, value, _count, _avg) in actual {
                oracle.ingest_aggregate_event(key, value, 1);
            }
            Self {
                oracle,
                provenance: SourceProvenance::SelfReferential,
            }
        }
    }

    impl Oracle for MultisetOracleAdapter {
        type Actual = Vec<(i64, i64, i64, f64)>;
        type Mismatch = HarnessError;

        fn source_provenance(&self) -> SourceProvenance {
            self.provenance
        }

        fn check(&self, actual: &Self::Actual) -> Result<(), Self::Mismatch> {
            self.oracle.verify_aggregates(actual)
        }
    }
}

/// Adapter over `rockstream_test_support::external_harness::ProcessIsolationAuditor`.
pub mod isolation {
    use super::{Oracle, SourceProvenance};
    use rockstream_test_support::external_harness::{
        HarnessError, ProcessIsolationAuditor, WorkerProcessIdentity,
    };

    /// Wraps [`ProcessIsolationAuditor`]; reference data is the declared
    /// worker count, independent of the worker identities it checks.
    pub struct ProcessIsolationOracleAdapter {
        declared_workers: usize,
    }

    impl ProcessIsolationOracleAdapter {
        pub fn new(declared_workers: usize) -> Self {
            Self { declared_workers }
        }
    }

    impl Oracle for ProcessIsolationOracleAdapter {
        type Actual = Vec<WorkerProcessIdentity>;
        type Mismatch = HarnessError;

        fn source_provenance(&self) -> SourceProvenance {
            SourceProvenance::Independent
        }

        fn check(&self, actual: &Self::Actual) -> Result<(), Self::Mismatch> {
            ProcessIsolationAuditor::verify_isolation(self.declared_workers, actual)
        }
    }
}

/// Adapter over [`crate::harness::assert_oracle_noop`]'s incremental/batch
/// comparison, reshaped to return a `Result` instead of panicking.
pub mod noop {
    use super::{Oracle, SourceProvenance};
    use crate::batch::run_noop_batch_query;
    use crate::zset::{accumulate, present_rows, TestRow, ZSetDelta};

    /// Reference side is the DataFusion batch result over the accumulated
    /// deltas — an independent re-derivation, not an echo of the
    /// incremental side it checks.
    pub struct NoopOracleAdapter {
        deltas: Vec<ZSetDelta>,
    }

    impl NoopOracleAdapter {
        pub fn from_deltas(deltas: Vec<ZSetDelta>) -> Self {
            Self { deltas }
        }
    }

    impl Oracle for NoopOracleAdapter {
        type Actual = Vec<TestRow>;
        type Mismatch = String;

        fn source_provenance(&self) -> SourceProvenance {
            SourceProvenance::Independent
        }

        fn check(&self, actual: &Self::Actual) -> Result<(), Self::Mismatch> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let acc = accumulate(&self.deltas);
            let mut expected = present_rows(&acc);
            expected.sort();
            let mut batch = rt
                .block_on(run_noop_batch_query(&expected))
                .map_err(|e| e.to_string())?;
            batch.sort();
            let mut actual = actual.clone();
            actual.sort();
            if batch == actual {
                Ok(())
            } else {
                Err(format!(
                    "incremental != batch\nincremental: {actual:?}\nbatch: {batch:?}"
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::multiset::MultisetOracleAdapter;
    use super::*;

    #[test]
    fn oracle_rejects_self_referential_fixture() {
        let actual = vec![(1_i64, 10_i64, 1_i64, 10.0_f64)];
        let oracle = MultisetOracleAdapter::from_echoed_actual(&actual);

        let result = oracle.verify(&actual);

        assert_eq!(result, Err(OracleVerifyError::SelfReferentialFixture));
    }

    #[test]
    fn independent_oracle_verifies_matching_actual() {
        let oracle = MultisetOracleAdapter::from_independent_events(&[(1, 10, 1)]);
        let actual = vec![(1_i64, 10_i64, 1_i64, 10.0_f64)];

        assert_eq!(oracle.verify(&actual), Ok(()));
    }

    #[test]
    fn independent_oracle_reports_mismatch_not_a_silent_pass() {
        let oracle = MultisetOracleAdapter::from_independent_events(&[(1, 10, 1)]);
        let wrong_actual = vec![(1_i64, 99_i64, 1_i64, 99.0_f64)];

        let result = oracle.verify(&wrong_actual);

        assert!(matches!(result, Err(OracleVerifyError::Mismatch(_))));
    }
}

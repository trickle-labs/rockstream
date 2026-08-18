//! Batch oracle auditor for qualification multiset validation.
//!
//! Independently tracks all ingested stream records and computes the expected
//! ground-truth view state and Kafka sink egress using `rockstream-oracle`.
//! Asserts exact multiset equivalence and monotonic watermark progression.

use super::workload::{MutationOp, WorkloadRecord};
use std::collections::BTreeMap;

/// Discrepancy report when live results differ from batch oracle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultisetDiff {
    pub missing_keys: Vec<String>,
    pub unexpected_keys: Vec<String>,
    pub mismatched_values: Vec<(String, i64, i64)>, // key, live_val, oracle_val
}

impl MultisetDiff {
    pub fn is_empty(&self) -> bool {
        self.missing_keys.is_empty()
            && self.unexpected_keys.is_empty()
            && self.mismatched_values.is_empty()
    }
}

/// External batch oracle auditor.
pub struct OracleAuditor {
    accumulated_state: BTreeMap<String, i64>,
    sink_history: Vec<WorkloadRecord>,
    last_watermark: u64,
}

impl Default for OracleAuditor {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleAuditor {
    /// Create a new oracle auditor.
    pub fn new() -> Self {
        Self {
            accumulated_state: BTreeMap::new(),
            sink_history: Vec::new(),
            last_watermark: 0,
        }
    }

    /// Ingest a slice of workload records into the reference oracle.
    pub fn ingest(&mut self, records: &[WorkloadRecord]) {
        for record in records {
            match record.op {
                MutationOp::Insert | MutationOp::Update => {
                    // Update or insert value for key
                    let entry = self
                        .accumulated_state
                        .entry(record.key.clone())
                        .or_insert(0);
                    *entry = record.val;
                    self.sink_history.push(record.clone());
                }
                MutationOp::Delete => {
                    // Remove key if present
                    self.accumulated_state.remove(&record.key);
                    self.sink_history.push(record.clone());
                }
            }
            if record.ingest_epoch > self.last_watermark {
                self.last_watermark = record.ingest_epoch;
            }
        }
    }

    /// Ingest an aggregated sum view computation (GROUP BY key, SUM(val)).
    pub fn ingest_sum_aggregate(&mut self, records: &[WorkloadRecord]) {
        for record in records {
            match record.op {
                MutationOp::Insert => {
                    let entry = self
                        .accumulated_state
                        .entry(record.key.clone())
                        .or_insert(0);
                    *entry += record.val;
                    self.sink_history.push(record.clone());
                }
                MutationOp::Update => {
                    let entry = self
                        .accumulated_state
                        .entry(record.key.clone())
                        .or_insert(0);
                    *entry = record.val;
                    self.sink_history.push(record.clone());
                }
                MutationOp::Delete => {
                    let entry = self
                        .accumulated_state
                        .entry(record.key.clone())
                        .or_insert(0);
                    *entry -= record.val;
                    if *entry <= 0 {
                        self.accumulated_state.remove(&record.key);
                    }
                    self.sink_history.push(record.clone());
                }
            }
            if record.ingest_epoch > self.last_watermark {
                self.last_watermark = record.ingest_epoch;
            }
        }
    }

    /// Retrieve the current expected oracle multiset.
    pub fn expected_view_state(&self) -> &BTreeMap<String, i64> {
        &self.accumulated_state
    }

    /// Retrieve the expected sink history.
    pub fn expected_sink_history(&self) -> &[WorkloadRecord] {
        &self.sink_history
    }

    /// Verify multiset equivalence between live view state and reference oracle.
    pub fn verify_multiset(&self, live_state: &BTreeMap<String, i64>) -> Result<(), MultisetDiff> {
        let mut diff = MultisetDiff {
            missing_keys: Vec::new(),
            unexpected_keys: Vec::new(),
            mismatched_values: Vec::new(),
        };

        // Check for missing keys or value mismatches
        for (k, expected_v) in &self.accumulated_state {
            match live_state.get(k) {
                Some(live_v) => {
                    if live_v != expected_v {
                        diff.mismatched_values
                            .push((k.clone(), *live_v, *expected_v));
                    }
                }
                None => {
                    diff.missing_keys.push(k.clone());
                }
            }
        }

        // Check for unexpected extra keys in live state
        for k in live_state.keys() {
            if !self.accumulated_state.contains_key(k) {
                diff.unexpected_keys.push(k.clone());
            }
        }

        if diff.is_empty() {
            Ok(())
        } else {
            Err(diff)
        }
    }

    /// Verify that consumed sink records match expected records exactly.
    pub fn verify_sink_records(&self, sink_records: &[WorkloadRecord]) -> Result<(), MultisetDiff> {
        if sink_records == self.sink_history.as_slice() {
            Ok(())
        } else {
            let mut diff = MultisetDiff {
                missing_keys: Vec::new(),
                unexpected_keys: Vec::new(),
                mismatched_values: Vec::new(),
            };
            if sink_records.len() != self.sink_history.len() {
                diff.missing_keys.push(format!(
                    "Length mismatch: live {} vs oracle {}",
                    sink_records.len(),
                    self.sink_history.len()
                ));
            }
            Err(diff)
        }
    }

    /// Verify monotonic advancement of watermarks.
    pub fn verify_watermark_monotone(&self, current: u64) -> Result<(), String> {
        if current >= self.last_watermark {
            Ok(())
        } else {
            Err(format!(
                "RS-0001 Watermark regressed: current {} < last {}",
                current, self.last_watermark
            ))
        }
    }
}

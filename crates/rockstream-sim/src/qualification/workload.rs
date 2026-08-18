//! Workload engine for distributed release qualification.
//!
//! Generates synthetic transactional CDC streams and Kafka record streams
//! covering:
//! - Insert / Update / Delete mutations
//! - Out-of-order event timestamps
//! - Key skew and zipfian distributions
//! - High-cardinality state

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

/// Mutation type of a workload event record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationOp {
    Insert,
    Update,
    Delete,
}

/// A synthetic data record generated for qualification workloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadRecord {
    pub key: String,
    pub val: i64,
    pub op: MutationOp,
    pub event_time_ms: u64,
    pub ingest_epoch: u64,
    pub sequence_num: u64,
}

/// A transaction container for PostgreSQL CDC logical replication stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcTransaction {
    pub xid: u64,
    pub lsn: u64,
    pub commit_time_ms: u64,
    pub records: Vec<WorkloadRecord>,
    pub is_committed: bool,
}

/// Synthetic workload generator.
pub struct QualificationWorkloadGenerator {
    rng: SmallRng,
    seq: u64,
    current_xid: u64,
    current_lsn: u64,
}

impl QualificationWorkloadGenerator {
    /// Create a new generator with a deterministic seed.
    pub fn new(seed: u64) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            seq: 0,
            current_xid: 1000,
            current_lsn: 0x1000_0000,
        }
    }

    /// Generate a batch of Kafka streaming records.
    pub fn generate_kafka_batch(
        &mut self,
        count: usize,
        epoch: u64,
        out_of_order_fraction: f64,
        num_keys: usize,
    ) -> Vec<WorkloadRecord> {
        let mut records = Vec::with_capacity(count);
        let base_time = epoch * 1000;

        for _ in 0..count {
            self.seq += 1;
            // Generate skewed key
            let key_idx = if self.rng.gen_bool(0.7) {
                // 70% of traffic goes to top 10% of keys (hot keys)
                self.rng.gen_range(0..num_keys.clamp(1, 10))
            } else {
                self.rng.gen_range(0..num_keys.max(1))
            };
            let key = format!("k_{}", key_idx);

            // Operation distribution: 60% insert, 30% update, 10% delete
            let op_rnd = self.rng.gen_range(0..100);
            let op = if op_rnd < 60 {
                MutationOp::Insert
            } else if op_rnd < 90 {
                MutationOp::Update
            } else {
                MutationOp::Delete
            };

            let val = self.rng.gen_range(1..100_000);

            // Event timestamp with optional out-of-order jitter
            let event_time = if self.rng.gen_bool(out_of_order_fraction.clamp(0.0, 1.0)) {
                let lag = self.rng.gen_range(100..5_000);
                base_time.saturating_sub(lag)
            } else {
                base_time + self.rng.gen_range(0..1_000)
            };

            records.push(WorkloadRecord {
                key,
                val,
                op,
                event_time_ms: event_time,
                ingest_epoch: epoch,
                sequence_num: self.seq,
            });
        }
        records
    }

    /// Generate a sequence of PostgreSQL CDC transactions.
    pub fn generate_cdc_transactions(
        &mut self,
        tx_count: usize,
        records_per_tx: usize,
        include_rollbacks: bool,
    ) -> Vec<CdcTransaction> {
        let mut transactions = Vec::with_capacity(tx_count);

        for i in 0..tx_count {
            self.current_xid += 1;
            self.current_lsn += 0x1000;
            let is_committed = if include_rollbacks && (i % 5 == 4) {
                false // Rollback 1 in 5 transactions
            } else {
                true
            };

            let mut records = Vec::with_capacity(records_per_tx);
            for _ in 0..records_per_tx {
                self.seq += 1;
                let key_idx = self.rng.gen_range(0..50);
                records.push(WorkloadRecord {
                    key: format!("cdc_k_{}", key_idx),
                    val: self.rng.gen_range(10..1_000),
                    op: MutationOp::Insert,
                    event_time_ms: self.current_xid * 100,
                    ingest_epoch: self.current_xid,
                    sequence_num: self.seq,
                });
            }

            transactions.push(CdcTransaction {
                xid: self.current_xid,
                lsn: self.current_lsn,
                commit_time_ms: self.current_xid * 100 + 50,
                records,
                is_committed,
            });
        }
        transactions
    }
}

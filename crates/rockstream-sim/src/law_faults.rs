//! Fault-model entries for built-in merge laws (IVM-0, DESIGN.md §17.4).
//!
//! Every merge law registered in v0.5+ must have a named entry here that
//! describes which failure mode the law must survive during simulation.
//! This registry is the audit trail for law-level simulation discipline.

use crate::fault_model::{FaultCategory, FaultEntry, FaultModel};

/// Register all built-in law fault entries into a `FaultModel`.
///
/// Call this once at simulation startup via `rockstream_sim::with_builtins()`.
pub fn register_law_faults(model: &mut FaultModel) {
    // WeightAdd/v1 — Z-set weight addition (abelian group)
    model.register(FaultEntry {
        id: "law.weight_add.reorder",
        description: "WeightAdd/v1: operand pairs arrive out of order across epoch boundaries. \
                       The abelian group property guarantees commutativity, so the final merged \
                       value must be identical regardless of delivery order.",
        category: FaultCategory::Network,
    });
    model.register(FaultEntry {
        id: "law.weight_add.crash_replay",
        description: "WeightAdd/v1: the process crashes mid-WriteBatch after some merge \
                       operations are persisted. On restart the shard replays from its \
                       persisted frontier; the merged weight must be bit-identical to an \
                       uninterrupted run.",
        category: FaultCategory::Io,
    });
    model.register(FaultEntry {
        id: "law.weight_add.fence",
        description: "WeightAdd/v1: a storage fence is injected between the merge write \
                       and the frontier update. The shard must not expose uncommitted weight \
                       state via DbReader snapshot reads.",
        category: FaultCategory::Io,
    });

    // SumCount/v1 — sum and count pair for AVG (abelian group)
    model.register(FaultEntry {
        id: "law.sum_count.reorder",
        description: "SumCount/v1: partial sums and counts arrive out of order across epoch \
                       boundaries. Commutativity of addition guarantees the final AVG is \
                       identical regardless of delivery order.",
        category: FaultCategory::Network,
    });
    model.register(FaultEntry {
        id: "law.sum_count.crash_replay",
        description: "SumCount/v1: crash after writing sum but before writing count. \
                       Replay must reproduce the full (sum, count) pair atomically.",
        category: FaultCategory::Io,
    });

    // MaxRegister/v1 — last-write-wins maximum (bounded semilattice)
    model.register(FaultEntry {
        id: "law.max_register.reorder",
        description: "MaxRegister/v1: updates arrive out of order. Idempotency and \
                       commutativity of max() ensure the highest observed value wins \
                       regardless of delivery order.",
        category: FaultCategory::Network,
    });
    model.register(FaultEntry {
        id: "law.max_register.duplicate",
        description: "MaxRegister/v1: the same update is delivered twice (at-least-once \
                       delivery). Idempotency of max() ensures no inflation.",
        category: FaultCategory::Network,
    });

    // HyperLogLog/v1 — approximate distinct count (merge-safe sketch)
    model.register(FaultEntry {
        id: "law.hyper_log_log.reorder",
        description: "HyperLogLog/v1: sketch merge operands arrive out of order. \
                       Union of HLL sketches is commutative; the cardinality estimate \
                       must be within error bounds regardless of merge order.",
        category: FaultCategory::Network,
    });
    model.register(FaultEntry {
        id: "law.hyper_log_log.crash_replay",
        description: "HyperLogLog/v1: crash after partial sketch write. On replay, \
                       the full sketch is re-merged; the estimate must not diverge \
                       beyond the documented error bound.",
        category: FaultCategory::Io,
    });

    // BloomUnion/v1 — set membership sketch (monotone, merge-safe)
    model.register(FaultEntry {
        id: "law.bloom_union.reorder",
        description: "BloomUnion/v1: bit-OR merge operands arrive out of order. \
                       Bit-OR is commutative and associative; false-positive rate \
                       must stay within documented bounds.",
        category: FaultCategory::Network,
    });
    model.register(FaultEntry {
        id: "law.bloom_union.duplicate",
        description: "BloomUnion/v1: the same filter is merged twice. \
                       Idempotency of bit-OR ensures no additional false positives.",
        category: FaultCategory::Network,
    });

    // KafkaTxTimeoutRecoveryTest (v0.43, DESIGN.md §17.8 gap 2) — Kafka
    // transactional producer's open transaction times out and is
    // broker-side force-aborted before the sink can commit it.
    model.register(FaultEntry {
        id: "object_store.partial_write",
        description:
            "FaultInjectingObjectStore truncates a write for retained connector fault tests.",
        category: FaultCategory::Io,
    });

    model.register(FaultEntry {
        id: "kafka.tx_timeout",
        description: "KafkaTxTimeoutRecoveryTest: the Kafka broker force-aborts an open \
                       producer transaction when it exceeds `transaction.timeout.ms`, \
                       mirroring a real broker-side transactional timeout. The Kafka sink's \
                       2PC commit protocol (`kafka_sink.rs`) must not treat the aborted \
                       transaction as committed, and recovery's `CheckBeforeCommit` path \
                       (query topic → absent → re-commit in a fresh transaction) must \
                       deliver the epoch exactly once.",
        category: FaultCategory::Network,
    });

    // ListStalenessRegressionTest (v0.43, DESIGN.md §17.8 gap 3, informational) —
    // simulated S3-style eventual-consistency delay on LIST results.
    model.register(FaultEntry {
        id: "object_store.list_staleness",
        description: "ListStalenessRegressionTest: SimObjectStore's `list()` omits keys \
                       written within the last `list_staleness_epochs` epochs, mirroring \
                       real S3 LIST eventual consistency. Direct-key `get`/`exists` reads \
                       remain immediately consistent. This is informational only: no \
                       RockStream correctness property may depend on LIST — CALM epoch \
                       manifest reads are always direct-key reads, never LIST-based — so \
                       this fault must never trigger a regression in any `assert_*` in \
                       `sink_connector.rs`.",
        category: FaultCategory::Io,
    });
}

/// Fault-model entries registered by `register_law_faults`.
pub const LAW_FAULT_IDS: &[&str] = &[
    "law.weight_add.reorder",
    "law.weight_add.crash_replay",
    "law.weight_add.fence",
    "law.sum_count.reorder",
    "law.sum_count.crash_replay",
    "law.max_register.reorder",
    "law.max_register.duplicate",
    "law.hyper_log_log.reorder",
    "law.hyper_log_log.crash_replay",
    "law.bloom_union.reorder",
    "law.bloom_union.duplicate",
    "object_store.partial_write",
    "kafka.tx_timeout",
    "object_store.list_staleness",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault_model::FaultModel;

    #[test]
    fn law_faults_register_without_collision() {
        let mut model = FaultModel::new();
        register_law_faults(&mut model);
        assert_eq!(model.len(), LAW_FAULT_IDS.len());
        for id in LAW_FAULT_IDS {
            assert!(model.get(id).is_some(), "missing fault entry: {id}");
        }
    }

    #[test]
    fn law_faults_are_categorized() {
        let mut model = FaultModel::new();
        register_law_faults(&mut model);
        assert_eq!(
            model.get("law.weight_add.reorder").unwrap().category,
            FaultCategory::Network
        );
        assert_eq!(
            model.get("law.weight_add.crash_replay").unwrap().category,
            FaultCategory::Io
        );
        assert_eq!(
            model.get("law.weight_add.fence").unwrap().category,
            FaultCategory::Io
        );
        assert_eq!(
            model.get("law.sum_count.reorder").unwrap().category,
            FaultCategory::Network
        );
        assert_eq!(
            model.get("law.max_register.duplicate").unwrap().category,
            FaultCategory::Network
        );
        assert_eq!(
            model
                .get("law.hyper_log_log.crash_replay")
                .unwrap()
                .category,
            FaultCategory::Io
        );
        assert_eq!(
            model.get("law.bloom_union.duplicate").unwrap().category,
            FaultCategory::Network
        );
        assert_eq!(
            model.get("object_store.partial_write").unwrap().category,
            FaultCategory::Io
        );
        assert_eq!(
            model.get("kafka.tx_timeout").unwrap().category,
            FaultCategory::Network
        );
        assert_eq!(
            model.get("object_store.list_staleness").unwrap().category,
            FaultCategory::Io
        );
    }
}

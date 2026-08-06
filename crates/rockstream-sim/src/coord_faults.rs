//! Fault-model entries for the epoch coordinator and operator task spawner.
//!
//! These cover the race-prone paths in `EpochCoordinator::commit_epoch` and
//! `spawn_operator_task_with_config` that are annotated with `buggify!()`.

use crate::fault_model::{FaultCategory, FaultEntry, FaultModel};

/// Register all epoch coordinator fault entries into a `FaultModel`.
pub fn register_coord_faults(model: &mut FaultModel) {
    // EpochCoordinator::commit_epoch — partial WriteBatch failure.
    model.register(FaultEntry {
        id: "epoch.write_batch_partial_failure",
        description: "commit_epoch: the WriteBatch completes only partially — some \
                       view-output rows are written but the frontier key is not updated. \
                       On restart the worker must re-process the epoch from the old \
                       frontier, producing bit-identical output (idempotent keys).",
        category: FaultCategory::Io,
    });
    // EpochCoordinator::commit_epoch — frontier write delay / ordering inversion.
    model.register(FaultEntry {
        id: "epoch.frontier_write_delay",
        description: "commit_epoch: a delay is injected between the view-output writes \
                       and the frontier `put`. A concurrent reader must not observe an \
                       advanced frontier for rows that are not yet durable.",
        category: FaultCategory::Timing,
    });
    // spawn_operator_task — channel send failure after partial processing.
    model.register(FaultEntry {
        id: "task.output_channel_closed",
        description: "spawn_operator_task: the output_tx channel closes mid-epoch \
                       (receiver dropped). The operator task must exit cleanly without \
                       panicking and without writing partial output to the coordinator.",
        category: FaultCategory::Logic,
    });
    // SimObjectStore - simulated HTTP 429 Too Many Requests
    model.register(FaultEntry {
        id: "object_store.rate_limit",
        description: "SimObjectStore: injects an HTTP 429 Too Many Requests error to test \
                       client-side retry and backoff logic.",
        category: FaultCategory::Io,
    });
    model.register(FaultEntry {
        id: "skew.control_loop_delay",
        description: "Adaptive skew-splitting control loop pauses between observing the skew \
                       breach and emitting the split action; the loop must still converge once \
                       the 30s sustain window is met.",
        category: FaultCategory::Timing,
    });
    model.register(FaultEntry {
        id: "split.kill_donor_mid_copy",
        description: "Proactive split loses the donor mid-copy; restart must recover to a \
                       consistent post-split or pre-split state without losing rows.",
        category: FaultCategory::Io,
    });
    model.register(FaultEntry {
        id: "merge.concurrent_split_race",
        description: "Cold-shard merge races a concurrent proactive split decision on the same \
                       shard pair; the one-per-minute throttle must prevent both actions from \
                       firing together.",
        category: FaultCategory::Logic,
    });
    model.register(FaultEntry {
        id: "hotkey.concurrent_bucket_map_bump",
        description: "Hot-key detection runs while the bucket-map version advances; the control \
                       loop must still emit a safe split plan for the observed hot shard.",
        category: FaultCategory::Logic,
    });
    model.register(FaultEntry {
        id: "exchange.az_metadata_missing",
        description: "Exchange classifier temporarily loses locality metadata while resolving a \
                       route; delivery must fall back to the safe legacy-compatible direct path \
                       without dropping rows.",
        category: FaultCategory::Logic,
    });
    model.register(FaultEntry {
        id: "exchange.shm_segment_unavailable",
        description: "Same-host shared-memory segment publish/open fails before ACK; the sender \
                       must fall back to direct delivery and inbox dedupe must prevent duplicate \
                       processing.",
        category: FaultCategory::Io,
    });
    model.register(FaultEntry {
        id: "exchange.domain_rebuild_during_drain",
        description: "AZ-domain membership rebuild races with a worker drain; same-AZ recipients \
                       must still be preferred and no frame may be stranded while domains refresh.",
        category: FaultCategory::Timing,
    });
    model.register(FaultEntry {
        id: "lock_poisoning.holder_panic",
        description:
            "A task panics while holding shared lock state; peer operations must continue \
                       without PoisonError, lock acquisition stalls, or degraded service.",
        category: FaultCategory::Logic,
    });
}

/// Fault-model entries registered by `register_coord_faults`.
pub const COORD_FAULT_IDS: &[&str] = &[
    "epoch.write_batch_partial_failure",
    "epoch.frontier_write_delay",
    "task.output_channel_closed",
    "object_store.rate_limit",
    "skew.control_loop_delay",
    "split.kill_donor_mid_copy",
    "merge.concurrent_split_race",
    "hotkey.concurrent_bucket_map_bump",
    "exchange.az_metadata_missing",
    "exchange.shm_segment_unavailable",
    "exchange.domain_rebuild_during_drain",
    "lock_poisoning.holder_panic",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault_model::FaultModel;

    #[test]
    fn coord_faults_register_without_collision() {
        let mut model = FaultModel::new();
        register_coord_faults(&mut model);
        assert_eq!(model.len(), COORD_FAULT_IDS.len());
        for id in COORD_FAULT_IDS {
            assert!(model.get(id).is_some(), "missing fault entry: {id}");
        }
    }
}

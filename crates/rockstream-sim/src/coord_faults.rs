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
}

/// Fault-model entries registered by `register_coord_faults`.
pub const COORD_FAULT_IDS: &[&str] = &[
    "epoch.write_batch_partial_failure",
    "epoch.frontier_write_delay",
    "task.output_channel_closed",
    "object_store.rate_limit",
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

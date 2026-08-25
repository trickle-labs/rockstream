//! Fault model registry.
//!
//! Every `buggify!()` call site must name an entry in the fault model. This
//! registry enumerates all known fault injection points, making the simulation
//! discipline explicit and auditable.

use std::collections::HashMap;
use std::sync::LazyLock;

use parking_lot::RwLock;

/// A single fault model entry describing an injection point.
#[derive(Debug, Clone)]
pub struct FaultEntry {
    /// Unique identifier for this fault (e.g. "write_batch_partial_failure").
    pub id: &'static str,
    /// Human-readable description of the fault scenario.
    pub description: &'static str,
    /// The category of fault (io, network, timing, logic).
    pub category: FaultCategory,
}

/// Categories of faults in the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultCategory {
    /// I/O faults: partial writes, corrupt reads, permission errors.
    Io,
    /// Network faults: drops, delays, reordering, partitions.
    Network,
    /// Timing faults: clock skew, delayed wakeups, spurious timeouts.
    Timing,
    /// Logic faults: unexpected state transitions, race conditions.
    Logic,
}

/// The global fault model registry.
#[derive(Debug)]
pub struct FaultModel {
    entries: HashMap<&'static str, FaultEntry>,
}

impl FaultModel {
    /// Create a new empty fault model.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Register a fault entry. Panics if the ID is already registered.
    pub fn register(&mut self, entry: FaultEntry) {
        let id = entry.id;
        if self.entries.contains_key(id) {
            panic!("Duplicate fault model entry: {id}");
        }
        self.entries.insert(id, entry);
    }

    /// Look up a fault entry by ID.
    pub fn get(&self, id: &str) -> Option<&FaultEntry> {
        self.entries.get(id)
    }

    /// Return all registered fault IDs.
    pub fn all_ids(&self) -> Vec<&'static str> {
        self.entries.keys().copied().collect()
    }

    /// Number of registered faults.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for FaultModel {
    fn default() -> Self {
        Self::new()
    }
}

/// Global fault model instance. All buggify sites register here.
static GLOBAL_FAULT_MODEL: LazyLock<RwLock<FaultModel>> =
    LazyLock::new(|| RwLock::new(FaultModel::new()));

/// Register a fault entry in the global fault model.
pub fn register_fault(entry: FaultEntry) {
    GLOBAL_FAULT_MODEL.write().register(entry);
}

/// Look up a fault by ID in the global model.
pub fn get_fault(id: &str) -> Option<FaultEntry> {
    GLOBAL_FAULT_MODEL.read().get(id).cloned()
}

/// Get all registered fault IDs.
pub fn all_fault_ids() -> Vec<&'static str> {
    GLOBAL_FAULT_MODEL.read().all_ids()
}

/// Number of registered faults in the global model.
pub fn fault_count() -> usize {
    GLOBAL_FAULT_MODEL.read().len()
}

/// Registers the v0.59.17 scenario-reproducibility fault-injection points
/// into the global fault model. Idempotent: safe to call more than once
/// (e.g. from multiple test binaries) since [`register_fault`] is only
/// invoked the first time each ID is missing.
pub fn register_scenario_faults() {
    for (id, description) in [(
        "v05917.scenario.mismatch_injection",
        "Injects a deliberate scenario-transcript mismatch during a SimRuntime \
             run, so the reproducibility-artifact machinery can be exercised under \
             deterministic replay (same seed must reproduce the same mismatch).",
    )] {
        if get_fault(id).is_none() {
            register_fault(FaultEntry {
                id,
                description,
                category: FaultCategory::Logic,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_model_register_and_lookup() {
        let mut model = FaultModel::new();
        model.register(FaultEntry {
            id: "test_fault_1",
            description: "A test fault for unit testing",
            category: FaultCategory::Io,
        });
        assert_eq!(model.len(), 1);
        let entry = model.get("test_fault_1").unwrap();
        assert_eq!(entry.category, FaultCategory::Io);
    }

    #[test]
    #[should_panic(expected = "Duplicate fault model entry")]
    fn fault_model_duplicate_panics() {
        let mut model = FaultModel::new();
        let entry = FaultEntry {
            id: "dup_fault",
            description: "duplicate",
            category: FaultCategory::Network,
        };
        model.register(entry.clone());
        model.register(entry);
    }

    #[test]
    fn fault_model_empty() {
        let model = FaultModel::new();
        assert!(model.is_empty());
        assert_eq!(model.len(), 0);
    }
}

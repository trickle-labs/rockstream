//! Global law registry.
//!
//! The registry holds all registered `LawBundle` implementations and provides
//! lookup by `MergeLawId`. It is the single source of truth for which laws
//! are available at runtime.

use crate::merge_law::{LawBundle, LawDescriptor, MergeLawId};
use std::collections::HashMap;
use std::sync::Arc;

/// A thread-safe registry of all known merge laws.
#[derive(Debug, Clone, Default)]
pub struct LawRegistry {
    laws: HashMap<MergeLawId, Arc<dyn LawBundle>>,
}

// Safety: LawBundle requires Send + Sync + 'static
impl LawRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        reg.register(Arc::new(super::WeightAddV1));
        reg.register(Arc::new(super::SumCountV1));
        reg.register(Arc::new(super::MaxRegisterV1));
        reg.register(Arc::new(super::MinRegisterV1));
        reg.register(Arc::new(super::HyperLogLogV1));
        reg.register(Arc::new(super::BloomUnionV1));
        reg.register(Arc::new(super::PNCounterV1));
        reg.register(Arc::new(super::LWWRegisterV1));
        reg.register(Arc::new(super::OrSetV1));
        reg.register(Arc::new(super::MVRegisterV1));
        reg
    }

    /// Register a law. Panics if a law with the same ID is already registered.
    pub fn register(&mut self, law: Arc<dyn LawBundle>) {
        let id = law.id();
        if self.laws.contains_key(&id) {
            panic!("Duplicate law registration: {} ({})", id, law.name());
        }
        self.laws.insert(id, law);
    }

    /// Look up a law by ID.
    pub fn get(&self, id: MergeLawId) -> Option<&Arc<dyn LawBundle>> {
        self.laws.get(&id)
    }

    /// Returns true if a law with the given ID is registered.
    pub fn contains(&self, id: MergeLawId) -> bool {
        self.laws.contains_key(&id)
    }

    /// Number of registered laws.
    pub fn len(&self) -> usize {
        self.laws.len()
    }

    /// Returns true if no laws are registered.
    pub fn is_empty(&self) -> bool {
        self.laws.is_empty()
    }

    /// List all registered law descriptors (for `EXPLAIN` / catalog queries).
    pub fn descriptors(&self) -> Vec<LawDescriptor> {
        self.laws
            .values()
            .map(|law| LawDescriptor::from_bundle(law.as_ref()))
            .collect()
    }
}

// We need Debug for Arc<dyn LawBundle>
impl std::fmt::Debug for dyn LawBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LawBundle({}/{})", self.name(), self.version())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::laws::weight_add::WEIGHT_ADD_ID;

    #[test]
    fn registry_with_builtins() {
        let reg = LawRegistry::with_builtins();
        assert_eq!(reg.len(), 10);
        assert!(reg.contains(WEIGHT_ADD_ID));
        assert!(reg.contains(crate::laws::sum_count::SUM_COUNT_ID));
        assert!(reg.contains(crate::laws::max_register::MAX_REGISTER_ID));
        assert!(reg.contains(crate::laws::min_register::MIN_REGISTER_ID));
        assert!(reg.contains(crate::laws::hyper_log_log::HLL_ID));
        assert!(reg.contains(crate::laws::bloom_union::BLOOM_UNION_ID));
        assert!(reg.contains(crate::laws::pn_counter::PN_COUNTER_ID));
        assert!(reg.contains(crate::laws::lww_register::LWW_REGISTER_ID));
        assert!(reg.contains(crate::laws::or_set::OR_SET_ID));
        assert!(reg.contains(crate::laws::mv_register::MV_REGISTER_ID));
    }

    #[test]
    fn lookup_returns_correct_law() {
        let reg = LawRegistry::with_builtins();
        let law = reg.get(WEIGHT_ADD_ID).unwrap();
        assert_eq!(law.name(), "WeightAdd");
        let sum_law = reg.get(crate::laws::sum_count::SUM_COUNT_ID).unwrap();
        assert_eq!(sum_law.name(), "SumCount");
        let max_law = reg.get(crate::laws::max_register::MAX_REGISTER_ID).unwrap();
        assert_eq!(max_law.name(), "MaxRegister");
        let min_law = reg.get(crate::laws::min_register::MIN_REGISTER_ID).unwrap();
        assert_eq!(min_law.name(), "MinRegister");
        let hll_law = reg.get(crate::laws::hyper_log_log::HLL_ID).unwrap();
        assert_eq!(hll_law.name(), "HyperLogLog");
        let bloom_law = reg.get(crate::laws::bloom_union::BLOOM_UNION_ID).unwrap();
        assert_eq!(bloom_law.name(), "BloomUnion");
        let or_law = reg.get(crate::laws::or_set::OR_SET_ID).unwrap();
        assert_eq!(or_law.name(), "OrSet");
        let mv_law = reg.get(crate::laws::mv_register::MV_REGISTER_ID).unwrap();
        assert_eq!(mv_law.name(), "MVRegister");
    }

    #[test]
    fn descriptors_lists_all() {
        let reg = LawRegistry::with_builtins();
        let descs = reg.descriptors();
        assert_eq!(descs.len(), 10);
        let names: std::collections::HashSet<_> = descs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains("WeightAdd"));
        assert!(names.contains("SumCount"));
        assert!(names.contains("MaxRegister"));
        assert!(names.contains("MinRegister"));
        assert!(names.contains("HyperLogLog"));
        assert!(names.contains("BloomUnion"));
        assert!(names.contains("PNCounter"));
        assert!(names.contains("LWWRegister"));
        assert!(names.contains("OrSet"));
        assert!(names.contains("MVRegister"));
    }

    #[test]
    #[should_panic(expected = "Duplicate law registration")]
    fn duplicate_registration_panics() {
        let mut reg = LawRegistry::with_builtins();
        reg.register(Arc::new(crate::laws::WeightAddV1));
    }
}

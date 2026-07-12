//! Workload definition types for RockStream.
//!
//! A *workload* is a named resource-and-SLO group that materialized views can
//! be assigned to. Every workload carries:
//!
//! - `freshness_slo_ms` — target maximum staleness in milliseconds.
//! - `memory_limit_bytes` — maximum in-memory state budget.
//! - `max_parallelism` — upper bound for workload-scoped auto-tuning.
//! - `priority` — scheduling priority (lower value = higher priority).
//!
//! Workloads are created with `CREATE WORKLOAD` and referenced by name when
//! a view is created with `WITH WORKLOAD = <name>`.

use serde::{Deserialize, Serialize};

/// Scheduling priority for a workload.
///
/// Lower numeric value means higher scheduling priority.
/// Priority 0 is the highest; priority 255 is the lowest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkloadPriority(pub u8);

impl WorkloadPriority {
    /// Highest priority (0).
    pub const HIGH: Self = Self(0);
    /// Default priority (128).
    pub const DEFAULT: Self = Self(128);
    /// Lowest priority (255).
    pub const LOW: Self = Self(255);
}

impl Default for WorkloadPriority {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// A freshness SLO expressed as a target maximum staleness duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshnessSlo {
    /// Target maximum staleness in milliseconds.
    pub target_ms: u64,
}

impl FreshnessSlo {
    /// Create a freshness SLO with the given target milliseconds.
    pub const fn new(target_ms: u64) -> Self {
        Self { target_ms }
    }
}

/// A memory limit for a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryLimit {
    /// Maximum memory budget in bytes.
    pub bytes: u64,
}

impl MemoryLimit {
    /// Create a memory limit with the given byte budget.
    pub const fn new(bytes: u64) -> Self {
        Self { bytes }
    }
}

/// Definition of a named workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadDef {
    /// Human-readable workload name (unique within a namespace).
    pub name: String,
    /// Target maximum staleness for all views in this workload.
    pub freshness_slo: Option<FreshnessSlo>,
    /// Maximum aggregate memory budget for all views in this workload.
    pub memory_limit: Option<MemoryLimit>,
    /// Maximum parallelism ceiling for views in this workload.
    pub max_parallelism: Option<u32>,
    /// Scheduling priority relative to other workloads.
    pub priority: WorkloadPriority,
}

impl WorkloadDef {
    /// Create a new workload definition with default priority and no limits.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            freshness_slo: None,
            memory_limit: None,
            max_parallelism: None,
            priority: WorkloadPriority::DEFAULT,
        }
    }

    /// Set the freshness SLO.
    pub fn with_freshness_slo(mut self, slo: FreshnessSlo) -> Self {
        self.freshness_slo = Some(slo);
        self
    }

    /// Set the memory limit.
    pub fn with_memory_limit(mut self, limit: MemoryLimit) -> Self {
        self.memory_limit = Some(limit);
        self
    }

    /// Set the maximum parallelism ceiling.
    pub fn with_max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    /// Set the scheduling priority.
    pub fn with_priority(mut self, priority: WorkloadPriority) -> Self {
        self.priority = priority;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_def_new_has_defaults() {
        let w = WorkloadDef::new("batch");
        assert_eq!(w.name, "batch");
        assert!(w.freshness_slo.is_none());
        assert!(w.memory_limit.is_none());
        assert!(w.max_parallelism.is_none());
        assert_eq!(w.priority, WorkloadPriority::DEFAULT);
    }

    #[test]
    fn workload_def_builder_chain() {
        let w = WorkloadDef::new("fast")
            .with_freshness_slo(FreshnessSlo::new(500))
            .with_memory_limit(MemoryLimit::new(1 << 30))
            .with_max_parallelism(8)
            .with_priority(WorkloadPriority::HIGH);
        assert_eq!(w.freshness_slo.unwrap().target_ms, 500);
        assert_eq!(w.memory_limit.unwrap().bytes, 1 << 30);
        assert_eq!(w.max_parallelism, Some(8));
        assert_eq!(w.priority, WorkloadPriority::HIGH);
    }

    #[test]
    fn workload_priority_ordering() {
        assert!(WorkloadPriority::HIGH < WorkloadPriority::DEFAULT);
        assert!(WorkloadPriority::DEFAULT < WorkloadPriority::LOW);
    }

    #[test]
    fn workload_def_serializes_round_trip() {
        let w = WorkloadDef::new("test")
            .with_freshness_slo(FreshnessSlo::new(1000))
            .with_memory_limit(MemoryLimit::new(512 * 1024 * 1024))
            .with_max_parallelism(4)
            .with_priority(WorkloadPriority(64));
        let json = serde_json::to_string(&w).unwrap();
        let back: WorkloadDef = serde_json::from_str(&json).unwrap();
        assert_eq!(w, back);
    }
}

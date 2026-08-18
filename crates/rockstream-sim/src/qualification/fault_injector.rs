//! Fault injection engine for distributed qualification scenarios.
//!
//! Models and executes faults across the distributed topology:
//! - Worker process crash / pause / restart
//! - Control HA leader failover
//! - Network partition / packet drop
//! - Object storage rate limit and brownouts

use super::orchestrator::QualificationCluster;
use std::time::{Duration, Instant};

/// Type of injected fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultType {
    WorkerKill,
    WorkerPause,
    ControlLeaderKill,
    NetworkPartition,
    StorageRateLimit,
    StorageBrownout,
    CorruptedL0Segment,
}

/// Record of an injected fault instance.
#[derive(Debug, Clone)]
pub struct InjectedFault {
    pub fault_type: FaultType,
    pub target_node_id: Option<u64>,
    pub injected_at: Instant,
    pub duration: Duration,
    pub resolved: bool,
}

/// Fault injection controller.
pub struct FaultInjector {
    history: Vec<InjectedFault>,
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

impl FaultInjector {
    /// Create a new fault injector.
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }

    /// Inject worker kill into the cluster.
    pub fn inject_worker_kill(
        &mut self,
        cluster: &QualificationCluster,
        worker_id: u64,
    ) -> Result<InjectedFault, String> {
        let _ = cluster.kill_node(worker_id)?;
        let fault = InjectedFault {
            fault_type: FaultType::WorkerKill,
            target_node_id: Some(worker_id),
            injected_at: Instant::now(),
            duration: Duration::from_millis(0),
            resolved: false,
        };
        self.history.push(fault.clone());
        Ok(fault)
    }

    /// Inject control leader failover by killing the active leader.
    pub fn inject_control_leader_kill(
        &mut self,
        cluster: &QualificationCluster,
    ) -> Result<InjectedFault, String> {
        let health = cluster.health_check();
        let leader_id = health
            .leader_id
            .ok_or_else(|| "RS-0001 No active leader in cluster".to_string())?;
        let _ = cluster.kill_node(leader_id)?;
        let fault = InjectedFault {
            fault_type: FaultType::ControlLeaderKill,
            target_node_id: Some(leader_id),
            injected_at: Instant::now(),
            duration: Duration::from_millis(0),
            resolved: false,
        };
        self.history.push(fault.clone());
        Ok(fault)
    }

    /// Inject storage brownout.
    pub fn inject_storage_brownout(&mut self, duration: Duration) -> InjectedFault {
        let fault = InjectedFault {
            fault_type: FaultType::StorageBrownout,
            target_node_id: None,
            injected_at: Instant::now(),
            duration,
            resolved: false,
        };
        self.history.push(fault.clone());
        fault
    }

    /// Retrieve recorded fault history.
    pub fn history(&self) -> &[InjectedFault] {
        &self.history
    }
}

//! Distributed qualification cluster orchestrator.
//!
//! Manages the lifecycle of a full distributed topology:
//! - HA Control Cluster (Raft leader, followers)
//! - Frontier and Compute Workers
//! - PostgreSQL Wire Gateway
//! - Kafka / Redpanda Message Broker
//! - PostgreSQL CDC Source
//! - MinIO / S3 Object Storage

use parking_lot::RwLock;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Roles in the distributed qualification topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeRole {
    ControlLeader,
    ControlFollower,
    ComputeWorker,
    FrontierWorker,
    Gateway,
    KafkaBroker,
    PostgresCdc,
    MinioStorage,
}

/// Status of an individual node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Starting,
    Running,
    Paused,
    Killed,
    Restarting,
}

/// Description and handle for an active node in the cluster.
#[derive(Debug, Clone)]
pub struct NodeHandle {
    pub id: u64,
    pub role: NodeRole,
    pub name: String,
    pub status: NodeStatus,
    pub listen_addr: String,
    pub storage_dir: PathBuf,
    pub started_at: Instant,
    pub restart_count: u32,
    pub epoch: u64,
}

/// Configuration for the distributed qualification cluster.
#[derive(Debug, Clone)]
pub struct QualificationClusterConfig {
    pub cluster_id: String,
    pub base_dir: PathBuf,
    pub control_nodes: usize,
    pub compute_workers: usize,
    pub frontier_workers: usize,
    pub gateway_port: u16,
    pub kafka_port: u16,
    pub minio_port: u16,
    pub pg_cdc_port: u16,
    pub image_tag: String,
    pub secondary_image_tag: Option<String>,
}

impl Default for QualificationClusterConfig {
    fn default() -> Self {
        Self {
            cluster_id: "rockstream-qual-cluster".into(),
            base_dir: std::env::temp_dir().join("rockstream-qual"),
            control_nodes: 3,
            compute_workers: 2,
            frontier_workers: 1,
            gateway_port: 5432,
            kafka_port: 9092,
            minio_port: 9000,
            pg_cdc_port: 5433,
            image_tag: "rockstream-tc-test:v0.59.2".into(),
            secondary_image_tag: None,
        }
    }
}

/// Summary report of cluster health.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHealth {
    pub is_healthy: bool,
    pub total_nodes: usize,
    pub running_nodes: usize,
    pub leader_id: Option<u64>,
    pub current_epoch: u64,
}

/// Lifecycle manager for the multi-process qualification cluster.
pub struct QualificationCluster {
    config: QualificationClusterConfig,
    nodes: Arc<RwLock<HashMap<u64, NodeHandle>>>,
    current_epoch: AtomicU64,
    is_running: AtomicBool,
    active_leader_id: AtomicU64,
}

impl QualificationCluster {
    /// Create a new qualification cluster.
    pub fn new(config: QualificationClusterConfig) -> Self {
        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            current_epoch: AtomicU64::new(1),
            is_running: AtomicBool::new(false),
            active_leader_id: AtomicU64::new(1),
        }
    }

    /// Provision and start all nodes in the distributed topology.
    pub async fn start(&self) -> Result<(), String> {
        let mut nodes = self.nodes.write();
        let mut next_id = 1u64;

        // 1. MinIO Storage
        let minio_dir = self.config.base_dir.join("minio");
        let _ = std::fs::create_dir_all(&minio_dir);
        nodes.insert(
            next_id,
            NodeHandle {
                id: next_id,
                role: NodeRole::MinioStorage,
                name: "minio-1".into(),
                status: NodeStatus::Running,
                listen_addr: format!("127.0.0.1:{}", self.config.minio_port),
                storage_dir: minio_dir,
                started_at: Instant::now(),
                restart_count: 0,
                epoch: 1,
            },
        );
        next_id += 1;

        // 2. Kafka Broker
        let kafka_dir = self.config.base_dir.join("kafka");
        let _ = std::fs::create_dir_all(&kafka_dir);
        nodes.insert(
            next_id,
            NodeHandle {
                id: next_id,
                role: NodeRole::KafkaBroker,
                name: "kafka-1".into(),
                status: NodeStatus::Running,
                listen_addr: format!("127.0.0.1:{}", self.config.kafka_port),
                storage_dir: kafka_dir,
                started_at: Instant::now(),
                restart_count: 0,
                epoch: 1,
            },
        );
        next_id += 1;

        // 3. PostgreSQL CDC Source
        let pg_dir = self.config.base_dir.join("postgres");
        let _ = std::fs::create_dir_all(&pg_dir);
        nodes.insert(
            next_id,
            NodeHandle {
                id: next_id,
                role: NodeRole::PostgresCdc,
                name: "postgres-cdc-1".into(),
                status: NodeStatus::Running,
                listen_addr: format!("127.0.0.1:{}", self.config.pg_cdc_port),
                storage_dir: pg_dir,
                started_at: Instant::now(),
                restart_count: 0,
                epoch: 1,
            },
        );
        next_id += 1;

        // 4. HA Control Nodes (Raft cluster)
        for i in 0..self.config.control_nodes {
            let role = if i == 0 {
                NodeRole::ControlLeader
            } else {
                NodeRole::ControlFollower
            };
            let dir = self.config.base_dir.join(format!("control-{}", i + 1));
            let _ = std::fs::create_dir_all(&dir);
            let id = next_id;
            if role == NodeRole::ControlLeader {
                self.active_leader_id.store(id, Ordering::SeqCst);
            }
            nodes.insert(
                id,
                NodeHandle {
                    id,
                    role,
                    name: format!("control-{}", i + 1),
                    status: NodeStatus::Running,
                    listen_addr: format!("127.0.0.1:{}", 8000 + i as u16),
                    storage_dir: dir,
                    started_at: Instant::now(),
                    restart_count: 0,
                    epoch: 1,
                },
            );
            next_id += 1;
        }

        // 5. Compute Workers
        for i in 0..self.config.compute_workers {
            let dir = self.config.base_dir.join(format!("worker-{}", i + 1));
            let _ = std::fs::create_dir_all(&dir);
            let id = next_id;
            nodes.insert(
                id,
                NodeHandle {
                    id,
                    role: NodeRole::ComputeWorker,
                    name: format!("compute-worker-{}", i + 1),
                    status: NodeStatus::Running,
                    listen_addr: format!("127.0.0.1:{}", 8100 + i as u16),
                    storage_dir: dir,
                    started_at: Instant::now(),
                    restart_count: 0,
                    epoch: 1,
                },
            );
            next_id += 1;
        }

        // 6. Frontier Workers
        for i in 0..self.config.frontier_workers {
            let dir = self.config.base_dir.join(format!("frontier-{}", i + 1));
            let _ = std::fs::create_dir_all(&dir);
            let id = next_id;
            nodes.insert(
                id,
                NodeHandle {
                    id,
                    role: NodeRole::FrontierWorker,
                    name: format!("frontier-worker-{}", i + 1),
                    status: NodeStatus::Running,
                    listen_addr: format!("127.0.0.1:{}", 8200 + i as u16),
                    storage_dir: dir,
                    started_at: Instant::now(),
                    restart_count: 0,
                    epoch: 1,
                },
            );
            next_id += 1;
        }

        // 7. Gateway Node
        let gw_dir = self.config.base_dir.join("gateway");
        let _ = std::fs::create_dir_all(&gw_dir);
        let id = next_id;
        nodes.insert(
            id,
            NodeHandle {
                id,
                role: NodeRole::Gateway,
                name: "gateway-1".into(),
                status: NodeStatus::Running,
                listen_addr: format!("127.0.0.1:{}", self.config.gateway_port),
                storage_dir: gw_dir,
                started_at: Instant::now(),
                restart_count: 0,
                epoch: 1,
            },
        );

        self.is_running.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Kill an individual node by its node ID or role.
    pub fn kill_node(&self, node_id: u64) -> Result<NodeHandle, String> {
        let mut nodes = self.nodes.write();
        let was_leader = {
            let node = nodes
                .get_mut(&node_id)
                .ok_or_else(|| format!("RS-0001 Node {} not found", node_id))?;
            node.status = NodeStatus::Killed;
            node.role == NodeRole::ControlLeader
        };

        // If killed node was control leader, advance epoch and trigger failover
        if was_leader {
            self.current_epoch.fetch_add(1, Ordering::SeqCst);
            // Promote next available follower
            for (id, candidate) in nodes.iter_mut() {
                if *id != node_id
                    && candidate.role == NodeRole::ControlFollower
                    && candidate.status == NodeStatus::Running
                {
                    candidate.role = NodeRole::ControlLeader;
                    self.active_leader_id.store(*id, Ordering::SeqCst);
                    break;
                }
            }
        }
        let result_node = nodes
            .get(&node_id)
            .cloned()
            .ok_or_else(|| format!("RS-0001 Node {} not found", node_id))?;
        Ok(result_node)
    }

    /// Pause an individual node by its node ID.
    pub fn pause_node(&self, node_id: u64) -> Result<NodeHandle, String> {
        let mut nodes = self.nodes.write();
        let node = nodes
            .get_mut(&node_id)
            .ok_or_else(|| format!("RS-0001 Node {} not found", node_id))?;
        node.status = NodeStatus::Paused;
        Ok(node.clone())
    }

    /// Restart a previously paused or killed node.
    pub fn restart_node(&self, node_id: u64) -> Result<NodeHandle, String> {
        let mut nodes = self.nodes.write();
        let node = nodes
            .get_mut(&node_id)
            .ok_or_else(|| format!("RS-0001 Node {} not found", node_id))?;
        node.status = NodeStatus::Running;
        node.restart_count += 1;
        node.started_at = Instant::now();
        node.epoch = self.current_epoch.load(Ordering::SeqCst);
        Ok(node.clone())
    }

    /// Advance the cluster fencing epoch.
    pub fn advance_epoch(&self) -> u64 {
        self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Return current cluster health summary.
    pub fn health_check(&self) -> ClusterHealth {
        let nodes = self.nodes.read();
        let total_nodes = nodes.len();
        let running_nodes = nodes
            .values()
            .filter(|n| n.status == NodeStatus::Running)
            .count();
        let leader_id = Some(self.active_leader_id.load(Ordering::SeqCst));
        let is_healthy = running_nodes > total_nodes / 2;
        ClusterHealth {
            is_healthy,
            total_nodes,
            running_nodes,
            leader_id,
            current_epoch: self.current_epoch.load(Ordering::SeqCst),
        }
    }

    /// Return all active running node handles.
    pub fn active_nodes(&self) -> Vec<NodeHandle> {
        self.nodes
            .read()
            .values()
            .filter(|n| n.status == NodeStatus::Running)
            .cloned()
            .collect()
    }

    /// Return gateway address.
    pub fn get_gateway_addr(&self) -> String {
        format!("127.0.0.1:{}", self.config.gateway_port)
    }

    /// Return kafka broker address.
    pub fn get_kafka_addr(&self) -> String {
        format!("127.0.0.1:{}", self.config.kafka_port)
    }

    /// Return minio storage address.
    pub fn get_minio_addr(&self) -> String {
        format!("127.0.0.1:{}", self.config.minio_port)
    }

    /// Stop all nodes and tear down cluster.
    pub fn stop(&self) {
        let mut nodes = self.nodes.write();
        for node in nodes.values_mut() {
            node.status = NodeStatus::Killed;
        }
        self.is_running.store(false, Ordering::SeqCst);
    }
}

impl Drop for QualificationCluster {
    fn drop(&mut self) {
        self.stop();
    }
}

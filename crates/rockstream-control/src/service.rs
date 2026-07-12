//! Control-plane network service for RockStream.
//!
//! The `ControlService` listens on a TCP address and accepts connections from
//! worker nodes. Workers send [`WorkerMessage`] frames (newline-delimited JSON)
//! and receive [`ControlMessage`] responses.
//!
//! ## Wire protocol
//!
//! Each message is a single-line JSON object terminated by `\n`.
//! Messages are framed without any length prefix: each line is one message.
//!
//! ```text
//! Worker → Control:  {"type":"register", ...}\n
//! Control → Worker:  {"type":"registered","worker_id":1}\n
//! Worker → Control:  {"type":"heartbeat","worker_id":1,"capacity_headroom":0.8}\n
//! Worker → Control:  {"type":"request_shard","worker_id":1,"shard_id":5}\n
//! Control → Worker:  {"type":"shard_assigned","lease":{...}}\n
//! Worker → Control:  {"type":"fence_write","shard_id":5,"lease_token":3}\n
//! Control → Worker:  {"type":"fence_ack","shard_id":5,"valid":true}\n
//! Worker → Control:  {"type":"deregister","worker_id":1}\n
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use rockstream_types::lease::ShardRevokeReason;
use rockstream_types::topology::{ControlMessage, WorkerMessage};

use crate::audit::{AuditEvent, FileAuditLog};
use crate::shard::ShardManager;
use crate::topology::TopologyCatalog;

/// Handle to the running control service.
pub struct ControlServiceHandle {
    /// Bound address.
    pub addr: SocketAddr,
    /// Shutdown sender; drop or send to stop the service.
    shutdown_tx: broadcast::Sender<()>,
}

impl ControlServiceHandle {
    /// Signal the service to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Control-plane service: listens for worker registrations and shard lease
/// requests on TCP.
pub struct ControlService {
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    audit: Option<Arc<FileAuditLog>>,
}

impl ControlService {
    /// Create a new `ControlService` backed by the given catalog.
    pub fn new(catalog: TopologyCatalog) -> Self {
        Self {
            catalog,
            shard_manager: ShardManager::new(),
            audit: None,
        }
    }

    /// Attach a pre-existing [`ShardManager`].  Useful when tests or the
    /// binary want to share a manager instance across multiple services.
    pub fn with_shard_manager(mut self, manager: ShardManager) -> Self {
        self.shard_manager = manager;
        self
    }

    /// Attach an audit log; topology events will be written to it.
    pub fn with_audit(mut self, audit: Arc<FileAuditLog>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Start the service on `bind_addr`.
    ///
    /// Returns a [`ControlServiceHandle`] which can be used to query the
    /// bound address and send a shutdown signal.
    pub async fn start(self, bind_addr: &str) -> io::Result<ControlServiceHandle> {
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;
        tracing::info!(addr = %addr, "control service listening");

        let (shutdown_tx, _) = broadcast::channel(1);
        let shutdown_tx2 = shutdown_tx.clone();

        let catalog = self.catalog.clone();
        let shard_manager = self.shard_manager.clone();
        let audit = self.audit.clone();

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_tx2.subscribe();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, peer)) => {
                                tracing::debug!(%peer, "control: new connection");
                                let cat = catalog.clone();
                                let mgr = shard_manager.clone();
                                let aud = audit.clone();
                                let mut sd = shutdown_tx2.subscribe();
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = handle_connection(stream, peer, cat, mgr, aud) => {}
                                        _ = sd.recv() => {}
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "control: accept error");
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("control service shutting down");
                        break;
                    }
                }
            }
        });

        Ok(ControlServiceHandle { addr, shutdown_tx })
    }

    /// Collect live operator statistics for a pipeline.
    pub fn collect_operator_stats(
        &self,
        _pipeline_id: u64,
    ) -> Vec<rockstream_types::explain::OperatorStats> {
        rockstream_types::metrics::operator_runtime_report()
            .into_iter()
            .map(|snapshot| {
                let (rmw_avoided, rmw_required) =
                    rockstream_types::metrics::operator_rmw_totals(snapshot.operator_id);
                let rmw_total = rmw_avoided + rmw_required;
                let rmw_ratio = if rmw_total == 0 {
                    0.0
                } else {
                    rmw_required as f64 / rmw_total as f64
                };
                rockstream_types::explain::OperatorStats {
                    rows_per_s: snapshot.rows_per_s,
                    state_reads: snapshot.state_reads,
                    rmw_ratio,
                    p99_latency_ms: snapshot.p99_latency_ms,
                    dlq_entries: snapshot.dlq_entries,
                }
            })
            .collect()
    }
}

/// Handle a single worker connection.
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    catalog: TopologyCatalog,
    shard_manager: ShardManager,
    audit: Option<Arc<FileAuditLog>>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut connected_worker_id: Option<rockstream_types::ids::WorkerId> = None;

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        let msg: WorkerMessage = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(%peer, error = %e, "control: invalid message");
                continue;
            }
        };

        match msg {
            WorkerMessage::Register(reg) => {
                let worker_id = catalog.register(&reg);
                connected_worker_id = Some(worker_id);
                tracing::info!(
                    worker_id = %worker_id,
                    address = %reg.address,
                    headroom = %reg.capacity_headroom,
                    "control: worker registered"
                );
                if let Some(aud) = &audit {
                    let event =
                        AuditEvent::now("control", "worker.registered", worker_id.to_string())
                            .with_detail(format!(
                                "address={}, headroom={}",
                                reg.address, reg.capacity_headroom
                            ));
                    let _ = aud.append(&event);
                }
                let reply = ControlMessage::Registered { worker_id };
                send_message(&mut writer, &reply).await;
            }
            WorkerMessage::Heartbeat {
                worker_id,
                capacity_headroom,
            } => {
                if catalog.heartbeat(worker_id, capacity_headroom) {
                    tracing::debug!(
                        %worker_id,
                        headroom = %capacity_headroom,
                        "control: heartbeat"
                    );
                } else {
                    tracing::warn!(
                        %worker_id,
                        "control: heartbeat from unknown worker"
                    );
                }
            }
            WorkerMessage::Deregister { worker_id } => {
                let removed = catalog.deregister(worker_id);
                tracing::info!(
                    %worker_id,
                    found = removed.is_some(),
                    "control: worker deregistered"
                );
                if let Some(aud) = &audit {
                    let event =
                        AuditEvent::now("control", "worker.deregistered", worker_id.to_string());
                    let _ = aud.append(&event);
                }
                // Release all shard leases held by this worker.
                let freed = shard_manager.release_worker(worker_id);
                if !freed.is_empty() {
                    tracing::info!(
                        %worker_id,
                        freed_shards = freed.len(),
                        "control: released shard leases on deregister"
                    );
                    if let Some(aud) = &audit {
                        let event = AuditEvent::now(
                            "control",
                            "worker.shards_released",
                            worker_id.to_string(),
                        )
                        .with_detail(format!("freed_shards={}", freed.len()));
                        let _ = aud.append(&event);
                    }
                    // Notify about shard revocations.
                    for shard_id in freed {
                        let revoke = ControlMessage::ShardRevoked {
                            shard_id,
                            reason: ShardRevokeReason::WorkerDead,
                        };
                        send_message(&mut writer, &revoke).await;
                    }
                }
                // Notify remaining workers about topology change.
                let workers = catalog.healthy_workers();
                let notify = ControlMessage::TopologyChanged { workers };
                send_message(&mut writer, &notify).await;
            }
            WorkerMessage::RequestShard {
                worker_id,
                shard_id,
            } => {
                match shard_manager.acquire(shard_id, worker_id) {
                    Ok(lease) => {
                        tracing::info!(
                            %worker_id,
                            %shard_id,
                            token = lease.lease_token.0,
                            "control: shard lease granted"
                        );
                        if let Some(aud) = &audit {
                            let event = AuditEvent::now(
                                "control",
                                "shard.lease_granted",
                                shard_id.to_string(),
                            )
                            .with_detail(format!(
                                "worker={}, token={}",
                                worker_id, lease.lease_token
                            ));
                            let _ = aud.append(&event);
                        }
                        let reply = ControlMessage::ShardAssigned { lease };
                        send_message(&mut writer, &reply).await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            %worker_id,
                            %shard_id,
                            error = %e,
                            "control: shard lease denied"
                        );
                        // Connection close signals the denial to the worker.
                    }
                }
            }
            WorkerMessage::FenceWrite {
                shard_id,
                lease_token,
            } => {
                let valid = shard_manager.is_valid_writer(shard_id, lease_token);
                tracing::debug!(
                    %shard_id,
                    token = lease_token.0,
                    valid,
                    "control: fence write check"
                );
                let reply = ControlMessage::FenceAck { shard_id, valid };
                send_message(&mut writer, &reply).await;
            }
            // v0.38 drain / lifecycle messages — acknowledged but not yet
            // fully handled by the control-plane service stub.
            WorkerMessage::DrainAck {
                worker_id,
                shards_remaining,
            } => {
                tracing::info!(
                    %worker_id,
                    shards_remaining,
                    "control: drain ack received"
                );
            }
            WorkerMessage::LifecycleState { worker_id, state } => {
                tracing::info!(
                    %worker_id,
                    state = ?state,
                    "control: worker lifecycle state update"
                );
            }
            WorkerMessage::ShardLoadReport { worker_id, samples } => {
                tracing::debug!(
                    %worker_id,
                    sample_count = samples.len(),
                    "control: shard load report received"
                );
            }
        }
    }

    // Connection dropped without explicit deregister: release all shard leases
    // but keep the topology catalog entry (it stays until explicit Deregister).
    if let Some(worker_id) = connected_worker_id {
        let freed = shard_manager.release_worker(worker_id);
        if !freed.is_empty() {
            tracing::info!(
                %worker_id,
                freed_shards = freed.len(),
                "control: released shard leases on disconnect"
            );
            if let Some(aud) = &audit {
                let event = AuditEvent::now(
                    "control",
                    "worker.shards_released_on_disconnect",
                    worker_id.to_string(),
                )
                .with_detail(format!("freed_shards={}", freed.len()));
                let _ = aud.append(&event);
            }
        }
    }

    tracing::debug!(%peer, "control: connection closed");
}

async fn send_message(writer: &mut tokio::net::tcp::OwnedWriteHalf, msg: &ControlMessage) {
    match serde_json::to_string(msg) {
        Ok(mut line) => {
            line.push('\n');
            if let Err(e) = writer.write_all(line.as_bytes()).await {
                tracing::warn!(error = %e, "control: write error");
            }
        }
        Err(e) => {
            tracing::error!(
                code = %rockstream_types::error_code::RS_0001,
                error = %e,
                "control: failed to serialize message"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::ShardManager;
    use crate::topology::TopologyCatalog;
    use rockstream_types::ids::WorkerId;
    use rockstream_types::topology::{
        CapacityHeadroom, NodeRole, WorkerMessage, WorkerRegistration,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    async fn start_test_service() -> (ControlServiceHandle, TopologyCatalog) {
        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog.clone());
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        (handle, catalog)
    }

    async fn send_and_recv(stream: &mut TcpStream, msg: &WorkerMessage) -> String {
        let line = serde_json::to_string(msg).unwrap() + "\n";
        stream.write_all(line.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(&mut *stream);
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        resp
    }

    #[tokio::test]
    async fn worker_registers_and_receives_ack() {
        let (handle, catalog) = start_test_service().await;
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();

        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:7001",
            CapacityHeadroom::new(0.9),
        );
        let resp = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::Registered { worker_id } => {
                assert_eq!(worker_id, WorkerId(1));
            }
            _ => panic!("expected Registered reply"),
        }
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.get(WorkerId(1)).unwrap().address, "127.0.0.1:7001");

        handle.shutdown();
    }

    #[tokio::test]
    async fn topology_catalog_updated_after_registration() {
        let (handle, catalog) = start_test_service().await;

        for i in 1..=3u64 {
            let mut stream = TcpStream::connect(handle.addr).await.unwrap();
            let reg = WorkerRegistration::new(
                WorkerId(i),
                NodeRole::Worker,
                format!("127.0.0.1:{}", 7000 + i),
                CapacityHeadroom::new(0.5 + i as f64 * 0.1),
            );
            let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
            stream.write_all(line.as_bytes()).await.unwrap();
            // wait for ack
            let mut reader = BufReader::new(&mut stream);
            let mut resp = String::new();
            reader.read_line(&mut resp).await.unwrap();
        }

        // Allow async tasks to process
        tokio::task::yield_now().await;
        assert_eq!(catalog.len(), 3);
        handle.shutdown();
    }

    #[tokio::test]
    async fn tier2_start_flow() {
        // Tier 2: --role=all means control + worker start in the same process.
        // Verify the control service starts and a worker can self-register.
        let catalog = TopologyCatalog::new();
        let svc = ControlService::new(catalog.clone());
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        let addr = handle.addr.to_string();

        // Simulate a worker connecting to its own in-process control service.
        let mut stream = TcpStream::connect(&addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(100),
            NodeRole::All,
            addr.clone(),
            CapacityHeadroom::FULL,
        );
        let line = serde_json::to_string(&WorkerMessage::Register(reg)).unwrap() + "\n";
        stream.write_all(line.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(&mut stream);
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        assert!(matches!(reply, ControlMessage::Registered { .. }));

        assert!(catalog.get(WorkerId(100)).is_some());
        handle.shutdown();
    }

    // -----------------------------------------------------------------------
    // v0.29: Shard lease and fence tests
    // -----------------------------------------------------------------------

    async fn start_test_service_with_manager(
    ) -> (ControlServiceHandle, TopologyCatalog, ShardManager) {
        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        let svc = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());
        let handle = svc.start("127.0.0.1:0").await.unwrap();
        (handle, catalog, manager)
    }

    #[tokio::test]
    async fn worker_can_request_and_receive_shard_lease() {
        use rockstream_types::ids::ShardId;

        let (handle, _catalog, manager) = start_test_service_with_manager().await;
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();

        // Register first.
        let reg = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:9001",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        // Request a shard lease.
        let req = WorkerMessage::RequestShard {
            worker_id: WorkerId(1),
            shard_id: ShardId(42),
        };
        let resp = send_and_recv(&mut stream, &req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::ShardAssigned { lease } => {
                assert_eq!(lease.shard_id, ShardId(42));
                assert_eq!(lease.worker_id, WorkerId(1));
                // Verify the manager also has the lease.
                assert!(manager.is_valid_writer(ShardId(42), lease.lease_token));
            }
            _ => panic!("expected ShardAssigned, got: {reply:?}"),
        }

        handle.shutdown();
    }

    #[test]
    fn collect_operator_stats_uses_live_windowed_metrics() {
        use rockstream_types::ids::OperatorId;
        use rockstream_types::merge_law::MergeLawId;
        use rockstream_types::metrics::{self, LawMetricKey};
        use std::time::{Duration, SystemTime};

        metrics::reset_all();
        let op_id = OperatorId(7);
        let at = SystemTime::now();
        metrics::record_operator_runtime_sample_at(op_id, 300, 12, Duration::from_millis(4), 1, at);
        metrics::record_operator_runtime_sample_at(op_id, 200, 8, Duration::from_millis(9), 0, at);
        metrics::record_operator_runtime_sample_at(
            op_id,
            100,
            10,
            Duration::from_millis(15),
            1,
            at,
        );

        let metric_key = LawMetricKey {
            law_id: MergeLawId(1),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: Some(op_id),
        };
        metrics::inc_rmw_avoided(&metric_key);
        metrics::inc_rmw_avoided(&metric_key);
        metrics::inc_rmw_avoided(&metric_key);
        metrics::inc_rmw_required(&metric_key);

        let svc = ControlService::new(TopologyCatalog::new());
        let stats = svc.collect_operator_stats(0);
        let stat = stats
            .iter()
            .find(|s| (s.rows_per_s - 10.0).abs() < 1e-9)
            .expect("expected driven operator stats");

        assert_ne!(stat.rows_per_s, 12500.0);
        assert_ne!(stat.state_reads, 120);
        assert_ne!(stat.p99_latency_ms, 12.0);
        assert_eq!(stat.rows_per_s, 10.0);
        assert_eq!(stat.state_reads, 30);
        assert!(
            (stat.rmw_ratio - 0.25).abs() < 1e-9,
            "rmw_ratio={}",
            stat.rmw_ratio
        );
        assert_eq!(stat.p99_latency_ms, 15.0);
        assert_eq!(stat.dlq_entries, 2);

        metrics::reset_all();
    }

    #[tokio::test]
    async fn fence_write_confirms_valid_token() {
        use rockstream_types::ids::{LeaseToken, ShardId};

        let (handle, _catalog, manager) = start_test_service_with_manager().await;

        // Pre-create a lease directly in the manager (bypassing the network for
        // setup speed).
        let lease = manager.acquire(ShardId(5), WorkerId(7)).unwrap();

        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        // Register so the connection is associated.
        let reg = WorkerRegistration::new(
            WorkerId(7),
            NodeRole::Worker,
            "127.0.0.1:9002",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        // Fence with valid token.
        let fence_req = WorkerMessage::FenceWrite {
            shard_id: ShardId(5),
            lease_token: lease.lease_token,
        };
        let resp = send_and_recv(&mut stream, &fence_req).await;
        let reply: ControlMessage = serde_json::from_str(resp.trim()).unwrap();
        match reply {
            ControlMessage::FenceAck { shard_id, valid } => {
                assert_eq!(shard_id, ShardId(5));
                assert!(valid, "current token must be valid");
            }
            _ => panic!("expected FenceAck, got: {reply:?}"),
        }

        // Fence with stale token (simulate worker A being fenced by worker B).
        let stale_token = LeaseToken(0); // guaranteed to be lower
        let fence_stale = WorkerMessage::FenceWrite {
            shard_id: ShardId(5),
            lease_token: stale_token,
        };
        let resp2 = send_and_recv(&mut stream, &fence_stale).await;
        let reply2: ControlMessage = serde_json::from_str(resp2.trim()).unwrap();
        match reply2 {
            ControlMessage::FenceAck { valid, .. } => {
                assert!(!valid, "stale token must be rejected");
            }
            _ => panic!("expected FenceAck, got: {reply2:?}"),
        }

        handle.shutdown();
    }

    #[tokio::test]
    async fn shard_leases_released_on_worker_disconnect() {
        use rockstream_types::ids::ShardId;

        let (handle, _catalog, manager) = start_test_service_with_manager().await;

        // Register worker and acquire shards over the wire.
        let mut stream = TcpStream::connect(handle.addr).await.unwrap();
        let reg = WorkerRegistration::new(
            WorkerId(3),
            NodeRole::Worker,
            "127.0.0.1:9003",
            CapacityHeadroom::FULL,
        );
        let _ = send_and_recv(&mut stream, &WorkerMessage::Register(reg)).await;

        let r1 = WorkerMessage::RequestShard {
            worker_id: WorkerId(3),
            shard_id: ShardId(10),
        };
        let r2 = WorkerMessage::RequestShard {
            worker_id: WorkerId(3),
            shard_id: ShardId(11),
        };
        let _ = send_and_recv(&mut stream, &r1).await;
        let _ = send_and_recv(&mut stream, &r2).await;

        assert_eq!(manager.len(), 2);

        // Drop the TCP stream — simulates worker death.
        drop(stream);
        // Give the async handler time to notice the disconnect.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // All leases should be released.
        assert!(
            manager.is_empty(),
            "shard leases must be released when the worker disconnects"
        );

        handle.shutdown();
    }
}

//! Worker client daemon for RockStream.
//!
//! Manages registration, periodic heartbeats, shard lease assignment,
//! and fencing validation with the control plane over TCP.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde_json;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLocation,
    WorkerMessage, WorkerRegistration,
};

use crate::secrets::WorkerSecretManager;
use rockstream_storage::ShardDb;

/// Tracks a shard lease and its local active database instance.
pub struct ShardState {
    pub lease: ShardLease,
    pub db: Option<ShardDb>,
}

/// A client handle to interact with the running worker daemon.
#[derive(Clone)]
pub struct WorkerClientHandle {
    worker_id: Arc<RwLock<Option<WorkerId>>>,
    active_shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    topology_workers: Arc<RwLock<HashMap<WorkerId, WorkerInfo>>>,
    msg_tx: mpsc::Sender<WorkerMessage>,
    fence_waiters:
        Arc<parking_lot::Mutex<HashMap<ShardId, Vec<tokio::sync::oneshot::Sender<bool>>>>>,
    secret_manager: Arc<WorkerSecretManager>,
}

impl WorkerClientHandle {
    /// Returns the worker ID assigned by the control plane.
    pub fn worker_id(&self) -> Option<WorkerId> {
        *self.worker_id.read()
    }

    /// Check if we own a shard lease and get its database instance.
    pub fn get_shard_db(&self, shard_id: ShardId) -> Option<ShardDb> {
        self.active_shards
            .read()
            .get(&shard_id)
            .and_then(|state| state.db.clone())
    }

    /// Get active leases held by this worker.
    pub fn leases(&self) -> Vec<ShardLease> {
        self.active_shards
            .read()
            .values()
            .map(|s| s.lease.clone())
            .collect()
    }

    /// Latest topology snapshot advertised by the control plane.
    pub fn topology_snapshot(&self) -> Vec<WorkerInfo> {
        self.topology_workers.read().values().cloned().collect()
    }

    /// Request a fresh short-lived token over the authenticated control channel.
    pub async fn request_secret_token(
        &self,
        secret_name: impl Into<String>,
    ) -> Result<(), io::Error> {
        self.msg_tx
            .send(WorkerMessage::ResolveSecretToken {
                secret_name: secret_name.into(),
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "Client channel closed"))
    }

    /// Read a decrypted credential from memory. No storage path is consulted.
    pub fn secret(
        &self,
        secret_name: &str,
        now_secs: u64,
    ) -> Option<crate::secrets::ResolvedSecret> {
        self.secret_manager.get(secret_name, now_secs)
    }

    pub fn secret_manager(&self) -> Arc<WorkerSecretManager> {
        self.secret_manager.clone()
    }

    /// Send a request to acquire a shard lease.
    pub async fn request_shard(&self, shard_id: ShardId) -> Result<(), io::Error> {
        let wid = self.worker_id().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "Worker is not registered yet")
        })?;
        let msg = WorkerMessage::RequestShard {
            worker_id: wid,
            shard_id,
        };
        self.msg_tx
            .send(msg)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::ConnectionAborted, "Client channel closed"))
    }

    /// Send a fence write check to the control plane and return whether it is valid.
    pub async fn check_fence_write(
        &self,
        shard_id: ShardId,
        lease_token: LeaseToken,
    ) -> Result<bool, io::Error> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            self.fence_waiters
                .lock()
                .entry(shard_id)
                .or_default()
                .push(tx);
        }
        let msg = WorkerMessage::FenceWrite {
            shard_id,
            lease_token,
        };
        if self.msg_tx.send(msg).await.is_err() {
            self.fence_waiters
                .lock()
                .get_mut(&shard_id)
                .map(|w| w.pop());
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "Client channel closed",
            ));
        }
        rx.await.map_err(|_| {
            io::Error::new(io::ErrorKind::ConnectionAborted, "Response channel closed")
        })
    }
}

/// Connect to the control plane and start the worker client daemon loop.
pub async fn start_worker_client(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_metadata(
        proposed_worker_id,
        control_url,
        storage_dir,
        WorkerLocation::default(),
        WorkerCapabilities::default(),
    )
    .await
}

/// Connect to the control plane and start the worker client daemon loop with
/// explicit locality/capability metadata.
pub async fn start_worker_client_with_metadata(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_tls_and_metadata(
        proposed_worker_id,
        control_url,
        storage_dir,
        location,
        capabilities,
        InternalTlsConfig::default(),
    )
    .await
}

/// Connect to the control plane over mTLS and start the worker client daemon loop.
pub async fn start_worker_client_with_tls(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    tls_config: InternalTlsConfig,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    start_worker_client_with_tls_and_metadata(
        proposed_worker_id,
        control_url,
        storage_dir,
        WorkerLocation::default(),
        WorkerCapabilities::default(),
        tls_config,
    )
    .await
}

/// Connect to the control plane over mTLS with explicit locality/capability metadata.
pub async fn start_worker_client_with_tls_and_metadata(
    proposed_worker_id: u64,
    control_url: &str,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    tls_config: InternalTlsConfig,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)> {
    let clean_url = control_url
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let stream = TcpStream::connect(clean_url).await?;

    if tls_config.is_enabled() {
        let connector = crate::tls::build_client_tls_connector(&tls_config).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("RS-2405: TLS client error: {e}"),
            )
        })?;
        let host = clean_url.split(':').next().unwrap_or("localhost");
        let server_name =
            rustls::pki_types::ServerName::try_from(host.to_string()).unwrap_or_else(|_| {
                rustls::pki_types::ServerName::try_from("localhost".to_string()).unwrap()
            });
        let tls_stream = connector.connect(server_name, stream).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("RS-2411: TLS handshake error: {e}"),
            )
        })?;
        let (reader, writer) = tokio::io::split(tls_stream);
        run_worker_client(
            proposed_worker_id,
            storage_dir,
            location,
            capabilities,
            reader,
            writer,
        )
        .await
    } else {
        let (reader, writer) = stream.into_split();
        run_worker_client(
            proposed_worker_id,
            storage_dir,
            location,
            capabilities,
            reader,
            writer,
        )
        .await
    }
}

async fn run_worker_client<R, W>(
    proposed_worker_id: u64,
    storage_dir: &Path,
    location: WorkerLocation,
    capabilities: WorkerCapabilities,
    reader: R,
    mut writer: W,
) -> io::Result<(WorkerClientHandle, tokio::task::JoinHandle<()>)>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let worker_id = Arc::new(RwLock::new(None));
    let active_shards = Arc::new(RwLock::new(HashMap::new()));
    let topology_workers = Arc::new(RwLock::new(HashMap::new()));
    let (msg_tx, mut msg_rx) = mpsc::channel::<WorkerMessage>(32);
    let fence_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let secret_manager = Arc::new(WorkerSecretManager::new(format!(
        "worker-{proposed_worker_id}"
    )));

    let handle = WorkerClientHandle {
        worker_id: worker_id.clone(),
        active_shards: active_shards.clone(),
        topology_workers: topology_workers.clone(),
        msg_tx: msg_tx.clone(),
        fence_waiters: fence_waiters.clone(),
        secret_manager: secret_manager.clone(),
    };

    let worker_id_clone = worker_id.clone();
    let active_shards_clone = active_shards.clone();
    let fence_waiters_clone = fence_waiters.clone();
    let storage_dir = storage_dir.to_path_buf();
    let secret_manager_clone = secret_manager.clone();

    let join_handle = tokio::spawn(async move {
        // 1. Send Registration message.
        let reg = WorkerRegistration::new(
            WorkerId(proposed_worker_id),
            NodeRole::Worker,
            "127.0.0.1:0", // Default loopback
            CapacityHeadroom::FULL,
        )
        .with_location(location.clone())
        .with_capabilities(capabilities);
        let reg_msg = WorkerMessage::Register(reg);
        let reg_line = serde_json::to_string(&reg_msg).unwrap() + "\n";
        if let Err(e) = writer.write_all(reg_line.as_bytes()).await {
            tracing::error!(
                code = %rockstream_types::error_code::RS_0001,
                "Failed to write registration to control plane: {:?}",
                e
            );
            return;
        }

        // 2. Spawn writer task to forward WorkerMessage channel over TCP.
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let mut writer_tx = writer;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = msg_rx.recv() => {
                        if let Some(msg) = msg {
                            if let Ok(line) = serde_json::to_string(&msg) {
                                let line = line + "\n";
                                if let Err(e) = writer_tx.write_all(line.as_bytes()).await {
                                    tracing::error!(
                                        code = %rockstream_types::error_code::RS_0001,
                                        "Worker client write error: {:?}",
                                        e
                                    );
                                    break;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        break;
                    }
                }
            }
        });

        // 3. Spawn heartbeat task once registered.
        let msg_tx_hb = msg_tx.clone();
        let worker_id_hb = worker_id_clone.clone();
        tokio::spawn(async move {
            loop {
                let wid_opt = *worker_id_hb.read();
                if let Some(wid) = wid_opt {
                    let hb = WorkerMessage::Heartbeat {
                        worker_id: wid,
                        capacity_headroom: CapacityHeadroom::FULL,
                    };
                    if msg_tx_hb.send(hb).await.is_err() {
                        break;
                    }
                }
                sleep(Duration::from_millis(500)).await;
            }
        });

        // 4. Read Loop: process ControlMessage commands from control plane.
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let msg: ControlMessage = match serde_json::from_str(&line) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        code = %rockstream_types::error_code::RS_0001,
                        "Invalid message from control plane: {:?}, raw: {}",
                        e,
                        line
                    );
                    continue;
                }
            };

            match msg {
                ControlMessage::Registered { worker_id: wid } => {
                    tracing::info!("Worker client registered successfully as {:?}", wid);
                    *worker_id_clone.write() = Some(wid);
                }
                ControlMessage::TopologyChanged { workers } => {
                    let mut topology = topology_workers.write();
                    topology.clear();
                    topology.extend(workers.into_iter().map(|worker| (worker.worker_id, worker)));
                }
                ControlMessage::ShardAssigned { lease } => {
                    tracing::info!("Received ShardAssigned lease for {:?}", lease.shard_id);
                    // Open database for this shard
                    let shard_path = storage_dir
                        .join("shards")
                        .join(lease.shard_id.0.to_string());
                    let remote_prefix = format!("shards/{}", lease.shard_id.0);
                    let store = match rockstream_storage::build_runtime_object_store(
                        &shard_path,
                        &remote_prefix,
                    ) {
                        Ok(store) => store,
                        Err(error) => {
                            tracing::error!(
                                code = %rockstream_types::error_code::RS_0003,
                                shard = ?lease.shard_id,
                                "Failed to configure shard object store: {error}"
                            );
                            continue;
                        }
                    };

                    // Attempt to open the ShardDb
                    let mut builder = ShardDb::builder("db", store);
                    if let Ok(metric_shard_id) = u16::try_from(lease.shard_id.0) {
                        builder = builder
                            .with_metrics_identity(metric_shard_id, lease.worker_id.to_string());
                    }
                    match builder.build().await {
                        Ok(db) => {
                            active_shards_clone.write().insert(
                                lease.shard_id,
                                ShardState {
                                    lease,
                                    db: Some(db),
                                },
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                code = %rockstream_types::error_code::RS_0003,
                                "Failed to open ShardDb for {:?}: {:?}",
                                lease.shard_id,
                                e
                            );
                        }
                    }
                }
                ControlMessage::ShardRevoked { shard_id, reason } => {
                    tracing::info!(
                        "Received ShardRevoked for {:?} due to {:?}",
                        shard_id,
                        reason
                    );
                    // Close and drop ShardDb without holding lock across await
                    let db_to_close = {
                        active_shards_clone
                            .write()
                            .remove(&shard_id)
                            .and_then(|state| state.db)
                    };
                    if let Some(db) = db_to_close {
                        let _ = db.close().await;
                    }
                }
                ControlMessage::FenceAck { shard_id, valid } => {
                    tracing::info!("Received FenceAck for {:?}: valid={}", shard_id, valid);
                    if !valid {
                        // Immediately detach and close ShardDb
                        let db_to_close = {
                            active_shards_clone
                                .write()
                                .remove(&shard_id)
                                .and_then(|state| state.db)
                        };
                        if let Some(db) = db_to_close {
                            let _ = db.close().await;
                        }
                    }
                    // Notify waiters
                    let waiters = { fence_waiters_clone.lock().remove(&shard_id) };
                    if let Some(waiters) = waiters {
                        for tx in waiters {
                            let _ = tx.send(valid);
                        }
                    }
                }
                ControlMessage::ClusterFrontierAdvanced { epoch } => {
                    tracing::info!("Cluster frontier advanced to {}", epoch);
                    let dbs: Vec<rockstream_storage::ShardDb> = active_shards_clone
                        .read()
                        .values()
                        .filter_map(|state| state.db.clone())
                        .collect();
                    for db in dbs {
                        if let Err(e) =
                            crate::exchange::persistence::gc_exchange_storage(&db, epoch).await
                        {
                            tracing::error!(
                                code = %rockstream_types::error_code::RS_0003,
                                "Failed to gc exchange storage: {:?}",
                                e
                            );
                        }
                    }
                }
                ControlMessage::SecretTokenIssued { token } => {
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    if let Err(error) = secret_manager_clone.resolve_token(&token, now_secs) {
                        tracing::error!(
                            code = %rockstream_types::error_code::RS_2423,
                            error = %error,
                            "worker secret token rejected"
                        );
                    }
                }
                ControlMessage::SecretRotated { rotation } => {
                    if let Err(error) = msg_tx
                        .send(WorkerMessage::ResolveSecretToken {
                            secret_name: rotation.secret_name,
                        })
                        .await
                    {
                        tracing::warn!(code = %rockstream_types::error_code::RS_0001, error = %error, "secret rotation refresh request could not be queued");
                    }
                }
                ControlMessage::Shutdown => {
                    tracing::info!("Control plane requested shutdown");
                    break;
                }
                _ => {}
            }
        }

        // Clean up remaining active shards on shutdown/disconnect
        let dbs_to_close: Vec<ShardDb> = {
            let mut shards = active_shards_clone.write();
            shards.drain().filter_map(|(_, state)| state.db).collect()
        };
        for db in dbs_to_close {
            let _ = db.close().await;
        }

        let _ = shutdown_tx.send(true);
    });

    Ok((handle, join_handle))
}

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::sleep;

use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};

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
    msg_tx: mpsc::Sender<WorkerMessage>,
    fence_waiters:
        Arc<parking_lot::Mutex<HashMap<ShardId, Vec<tokio::sync::oneshot::Sender<bool>>>>>,
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
    let stream = TcpStream::connect(control_url).await?;
    let (reader, mut writer) = stream.into_split();

    let worker_id = Arc::new(RwLock::new(None));
    let active_shards = Arc::new(RwLock::new(HashMap::new()));
    let (msg_tx, mut msg_rx) = mpsc::channel::<WorkerMessage>(32);
    let fence_waiters = Arc::new(parking_lot::Mutex::new(HashMap::new()));

    let handle = WorkerClientHandle {
        worker_id: worker_id.clone(),
        active_shards: active_shards.clone(),
        msg_tx: msg_tx.clone(),
        fence_waiters: fence_waiters.clone(),
    };

    let worker_id_clone = worker_id.clone();
    let active_shards_clone = active_shards.clone();
    let fence_waiters_clone = fence_waiters.clone();
    let storage_dir = storage_dir.to_path_buf();

    let join_handle = tokio::spawn(async move {
        // 1. Send Registration message.
        let reg = WorkerRegistration::new(
            WorkerId(proposed_worker_id),
            NodeRole::Worker,
            "127.0.0.1:0", // Default loopback
            CapacityHeadroom::FULL,
        );
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
                ControlMessage::ShardAssigned { lease } => {
                    tracing::info!("Received ShardAssigned lease for {:?}", lease.shard_id);
                    // Open database for this shard
                    let shard_path = storage_dir
                        .join("shards")
                        .join(lease.shard_id.0.to_string());
                    std::fs::create_dir_all(&shard_path).unwrap();
                    let store = Arc::new(
                        object_store::local::LocalFileSystem::new_with_prefix(&shard_path).unwrap(),
                    );

                    // Attempt to open the ShardDb
                    match ShardDb::builder("db", store).build().await {
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

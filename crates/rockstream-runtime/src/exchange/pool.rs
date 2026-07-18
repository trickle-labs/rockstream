use crate::exchange::proto::shuffle_service_client::ShuffleServiceClient;
use crate::exchange::shared_memory::SharedMemoryClient;
use parking_lot::RwLock;
use rockstream_types::ids::WorkerId;
use rockstream_types::topology::WorkerInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

/// A client pool that caches connection clients and peer metadata.
#[derive(Clone, Default)]
pub struct ShuffleClientPool {
    peers: Arc<RwLock<HashMap<WorkerId, String>>>,
    peer_infos: Arc<RwLock<HashMap<WorkerId, WorkerInfo>>>,
    local_worker: Arc<RwLock<Option<WorkerInfo>>>,
    clients: Arc<RwLock<HashMap<WorkerId, ShuffleServiceClient<Channel>>>>,
    shm_clients: Arc<RwLock<HashMap<WorkerId, SharedMemoryClient>>>,
}

impl ShuffleClientPool {
    /// Create a new ShuffleClientPool sharing the same peer registry.
    pub fn new(peers: Arc<RwLock<HashMap<WorkerId, String>>>) -> Self {
        ShuffleClientPool {
            peers,
            peer_infos: Arc::new(RwLock::new(HashMap::new())),
            local_worker: Arc::new(RwLock::new(None)),
            clients: Arc::new(RwLock::new(HashMap::new())),
            shm_clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_local_worker_info(&self, worker: WorkerInfo) {
        *self.local_worker.write() = Some(worker);
    }

    pub fn local_worker_info(&self) -> Option<WorkerInfo> {
        self.local_worker.read().clone()
    }

    pub fn upsert_peer_info(&self, worker: WorkerInfo) {
        self.peers
            .write()
            .insert(worker.worker_id, worker.address.clone());
        self.peer_infos.write().insert(worker.worker_id, worker);
    }

    pub fn replace_peer_infos(&self, workers: impl IntoIterator<Item = WorkerInfo>) {
        let workers: Vec<WorkerInfo> = workers.into_iter().collect();
        let mut peers = self.peers.write();
        let mut infos = self.peer_infos.write();
        peers.clear();
        infos.clear();
        for worker in workers {
            peers.insert(worker.worker_id, worker.address.clone());
            infos.insert(worker.worker_id, worker);
        }
    }

    pub fn peer_info(&self, worker_id: WorkerId) -> Option<WorkerInfo> {
        self.peer_infos.read().get(&worker_id).cloned()
    }

    /// Retrieve or establish a gRPC client connection to the specified worker.
    pub async fn get_client(
        &self,
        worker_id: WorkerId,
    ) -> Result<ShuffleServiceClient<Channel>, String> {
        {
            let clients = self.clients.read();
            if let Some(client) = clients.get(&worker_id) {
                return Ok(client.clone());
            }
        }

        let addr = {
            let peers = self.peers.read();
            peers
                .get(&worker_id)
                .cloned()
                .ok_or_else(|| format!("No registered address for worker {:?}", worker_id))?
        };

        let url = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else {
            format!("http://{}", addr)
        };

        let endpoint = Channel::from_shared(url)
            .map_err(|e| format!("Invalid gRPC URL: {:?}", e))?
            .connect_timeout(std::time::Duration::from_millis(250));

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to worker {:?}: {:?}", worker_id, e))?;

        let client = ShuffleServiceClient::new(channel);
        self.clients.write().insert(worker_id, client.clone());
        Ok(client)
    }

    pub async fn get_shared_memory_client(
        &self,
        worker_id: WorkerId,
    ) -> Result<SharedMemoryClient, String> {
        {
            let clients = self.shm_clients.read();
            if let Some(client) = clients.get(&worker_id) {
                return Ok(*client);
            }
        }
        let client = SharedMemoryClient::new(worker_id);
        self.shm_clients.write().insert(worker_id, client);
        Ok(client)
    }
}

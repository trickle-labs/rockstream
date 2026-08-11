use crate::exchange::proto::shuffle_service_client::ShuffleServiceClient;
use crate::exchange::shared_memory::SharedMemoryClient;
use parking_lot::RwLock;
use rockstream_types::config::ExchangeConfig;
use rockstream_types::ids::WorkerId;
use rockstream_types::topology::WorkerInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

/// A client pool that caches connection clients and peer metadata,
/// governed by network policy timeouts, retries, and peer circuit breaking.
#[derive(Clone)]
pub struct ShuffleClientPool {
    peers: Arc<RwLock<HashMap<WorkerId, String>>>,
    peer_infos: Arc<RwLock<HashMap<WorkerId, WorkerInfo>>>,
    local_worker: Arc<RwLock<Option<WorkerInfo>>>,
    clients: Arc<RwLock<HashMap<WorkerId, ShuffleServiceClient<Channel>>>>,
    shm_clients: Arc<RwLock<HashMap<WorkerId, SharedMemoryClient>>>,
    config: ExchangeConfig,
    consecutive_failures: Arc<RwLock<HashMap<WorkerId, u32>>>,
}

impl Default for ShuffleClientPool {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(HashMap::new())))
    }
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
            config: ExchangeConfig::default(),
            consecutive_failures: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_config(mut self, config: ExchangeConfig) -> Self {
        self.config = config;
        self
    }

    pub fn config(&self) -> &ExchangeConfig {
        &self.config
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

    pub fn is_circuit_broken(&self, worker_id: WorkerId) -> bool {
        let failures = self.consecutive_failures.read();
        failures.get(&worker_id).copied().unwrap_or(0) >= self.config.max_retries
    }

    fn update_metric(&self) {
        let count = self.clients.read().len() + self.shm_clients.read().len();
        rockstream_types::metrics::set_exchange_pool_clients_size(count as u64);
    }

    /// Evict cached gRPC and SHM clients, peer address, peer info, and failure count for a dead or drained worker.
    pub fn evict_worker(&self, worker_id: WorkerId) {
        self.peers.write().remove(&worker_id);
        self.peer_infos.write().remove(&worker_id);
        self.clients.write().remove(&worker_id);
        self.shm_clients.write().remove(&worker_id);
        self.consecutive_failures.write().remove(&worker_id);
        self.update_metric();
    }

    pub fn reset_circuit_breaker(&self, worker_id: WorkerId) {
        self.consecutive_failures.write().remove(&worker_id);
        self.clients.write().remove(&worker_id);
        self.update_metric();
    }

    pub fn record_failure(&self, worker_id: WorkerId) {
        let mut failures = self.consecutive_failures.write();
        *failures.entry(worker_id).or_insert(0) += 1;
        self.clients.write().remove(&worker_id);
        self.update_metric();
    }

    pub fn record_success(&self, worker_id: WorkerId) {
        self.consecutive_failures.write().remove(&worker_id);
    }

    /// Retrieve or establish a gRPC client connection to the specified worker.
    pub async fn get_client(
        &self,
        worker_id: WorkerId,
    ) -> Result<ShuffleServiceClient<Channel>, String> {
        if self.is_circuit_broken(worker_id) {
            let failures = self
                .consecutive_failures
                .read()
                .get(&worker_id)
                .copied()
                .unwrap_or(0);
            return Err(format!(
                "RS-5003: peer worker connection circuit breaker tripped for worker {:?} (failures={}, max_retries={}). Next steps: verify peer network health or reset circuit breaker.",
                worker_id, failures, self.config.max_retries
            ));
        }

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
                .ok_or_else(|| format!("RS-5002: no registered address for worker {:?}. Next steps: verify cluster topology registration.", worker_id))?
        };

        let url = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else {
            format!("http://{}", addr)
        };

        let mut last_err = String::new();
        let max_attempts = self.config.max_retries.max(1);

        for attempt in 1..=max_attempts {
            let endpoint = match Channel::from_shared(url.clone()) {
                Ok(ep) => ep.connect_timeout(std::time::Duration::from_millis(
                    self.config.connect_timeout_ms,
                )),
                Err(e) => {
                    self.record_failure(worker_id);
                    return Err(format!("RS-5002: invalid gRPC URL for worker {:?}: {:?}. Next steps: check address configuration.", worker_id, e));
                }
            };

            match endpoint.connect().await {
                Ok(channel) => {
                    let client = ShuffleServiceClient::new(channel);
                    self.record_success(worker_id);
                    self.clients.write().insert(worker_id, client.clone());
                    self.update_metric();
                    return Ok(client);
                }
                Err(e) => {
                    self.record_failure(worker_id);
                    last_err = format!("{:?}", e);
                    if attempt < max_attempts && self.config.backoff_jitter_ms > 0 {
                        let jitter = (attempt as u64) * (self.config.backoff_jitter_ms / 2)
                            + (rand::random::<u64>() % self.config.backoff_jitter_ms.max(1));
                        tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;
                    }
                }
            }
        }

        Err(format!(
            "RS-5003: failed to connect to worker {:?} after {} attempts (timeout {}ms): {}. Next steps: check peer reachability.",
            worker_id, max_attempts, self.config.connect_timeout_ms, last_err
        ))
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
        self.update_metric();
        Ok(client)
    }
}

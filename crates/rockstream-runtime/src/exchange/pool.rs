use crate::exchange::proto::shuffle_service_client::ShuffleServiceClient;
use parking_lot::RwLock;
use rockstream_types::ids::WorkerId;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::Channel;

/// A client pool that caches connection clients to peer workers.
#[derive(Clone, Default)]
pub struct ShuffleClientPool {
    peers: Arc<RwLock<HashMap<WorkerId, String>>>,
    clients: Arc<RwLock<HashMap<WorkerId, ShuffleServiceClient<Channel>>>>,
}

impl ShuffleClientPool {
    /// Create a new ShuffleClientPool sharing the same peer registry.
    pub fn new(peers: Arc<RwLock<HashMap<WorkerId, String>>>) -> Self {
        ShuffleClientPool {
            peers,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Retrieve or establish a gRPC client connection to the specified worker.
    pub async fn get_client(
        &self,
        worker_id: WorkerId,
    ) -> Result<ShuffleServiceClient<Channel>, String> {
        // Fast path: cached client
        {
            let clients = self.clients.read();
            if let Some(client) = clients.get(&worker_id) {
                return Ok(client.clone());
            }
        }

        // Slow path: resolve peer address and connect
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

        let endpoint =
            Channel::from_shared(url).map_err(|e| format!("Invalid gRPC URL: {:?}", e))?;

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to worker {:?}: {:?}", worker_id, e))?;

        let client = ShuffleServiceClient::new(channel);

        self.clients.write().insert(worker_id, client.clone());
        Ok(client)
    }
}

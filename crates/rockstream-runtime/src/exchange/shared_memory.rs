use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use arrow_flight::Ticket;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::exchange::proto::ShuffleAck;
use crate::exchange::proto::ShuffleFrame;
use crate::exchange::service::ExchangeRegistry;
use crate::exchange::{persistence, serialization};
use rockstream_types::config::ExchangeConfig;
use rockstream_types::error_code::{RS_3019, RS_3020, RS_3023};
use rockstream_types::ids::WorkerId;

static NEXT_SEGMENT_ID: AtomicU64 = AtomicU64::new(1);
static SEGMENTS: LazyLock<Mutex<HashMap<String, PublishedSegment>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static RECEIVERS: LazyLock<Mutex<HashMap<WorkerId, ExchangeRegistry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
pub struct SharedMemorySegmentPool {
    config: ExchangeConfig,
    usage: Arc<Mutex<HashMap<WorkerId, PeerUsage>>>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerUsage {
    bytes_used: usize,
    segments_in_use: usize,
}

#[derive(Debug, Clone)]
struct PublishedSegment {
    payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedMemoryTicketEnvelope {
    pub segment_id: String,
    pub exchange_id: u64,
    pub src_shard: u32,
    pub target_shard: u32,
    pub epoch: u64,
    pub seq: u64,
    pub row_count: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct SharedMemoryClient {
    worker_id: WorkerId,
}

impl SharedMemoryClient {
    pub fn new(worker_id: WorkerId) -> Self {
        Self { worker_id }
    }

    pub async fn deliver(&self, ticket: Ticket) -> Result<ShuffleAck, String> {
        let ticket: SharedMemoryTicketEnvelope = serde_json::from_slice(&ticket.ticket)
            .map_err(|e| format!("invalid shared-memory ticket: {e}"))?;
        deliver_ticket(self.worker_id, ticket).await
    }
}

impl SharedMemorySegmentPool {
    pub fn new(config: ExchangeConfig) -> Self {
        Self {
            config,
            usage: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn publish(
        &self,
        target_worker: WorkerId,
        frame: &ShuffleFrame,
    ) -> Result<SharedMemoryTicketEnvelope, String> {
        if frame.payload.len() > self.config.same_host_shm_segment_bytes {
            return Err(format!(
                "[{RS_3019}] payload exceeds same_host_shm_segment_bytes bound. Next steps: reduce batch size or increase same_host_shm_segment_bytes within host memory limits."
            ));
        }

        let (bytes_used, segments_in_use) = {
            let mut usage = self.usage.lock().unwrap();
            let peer_usage = usage.entry(target_worker).or_default();
            if peer_usage.segments_in_use >= self.config.same_host_shm_segments_per_peer {
                return Err(format!(
                    "[{RS_3019}] shared-memory segment pool exhausted for peer {target_worker}. Next steps: let in-flight segments drain or increase same_host_shm_segments_per_peer within the memory budget."
                ));
            }
            peer_usage.segments_in_use += 1;
            peer_usage.bytes_used += frame.payload.len();
            summarize_usage(&usage)
        };
        rockstream_types::metrics::set_shuffle_shm_bytes_used(bytes_used);
        rockstream_types::metrics::set_shuffle_shm_segments_in_use(segments_in_use);

        let segment_id = format!("shm-{}", NEXT_SEGMENT_ID.fetch_add(1, Ordering::Relaxed));
        SEGMENTS.lock().unwrap().insert(
            segment_id.clone(),
            PublishedSegment {
                payload: frame.payload.to_vec(),
            },
        );
        Ok(SharedMemoryTicketEnvelope {
            segment_id,
            exchange_id: frame.exchange_id,
            src_shard: frame.src_shard,
            target_shard: frame.target_shard,
            epoch: frame.epoch,
            seq: frame.seq,
            row_count: frame.row_count,
        })
    }

    pub fn revoke(&self, target_worker: WorkerId, ticket: &SharedMemoryTicketEnvelope) {
        let payload_len = SEGMENTS
            .lock()
            .unwrap()
            .remove(&ticket.segment_id)
            .map(|segment| segment.payload.len())
            .unwrap_or(0);
        self.release_usage(target_worker, payload_len);
    }

    pub fn release_usage(&self, target_worker: WorkerId, payload_len: usize) {
        let (bytes_used, segments_in_use) = {
            let mut usage = self.usage.lock().unwrap();
            if let Some(peer_usage) = usage.get_mut(&target_worker) {
                peer_usage.segments_in_use = peer_usage.segments_in_use.saturating_sub(1);
                peer_usage.bytes_used = peer_usage.bytes_used.saturating_sub(payload_len);
                if peer_usage.segments_in_use == 0 && peer_usage.bytes_used == 0 {
                    usage.remove(&target_worker);
                }
            }
            summarize_usage(&usage)
        };
        rockstream_types::metrics::set_shuffle_shm_bytes_used(bytes_used);
        rockstream_types::metrics::set_shuffle_shm_segments_in_use(segments_in_use);
    }
}

fn summarize_usage(usage: &HashMap<WorkerId, PeerUsage>) -> (u64, u64) {
    let totals = usage
        .values()
        .fold((0usize, 0usize), |(bytes, segments), value| {
            (bytes + value.bytes_used, segments + value.segments_in_use)
        });
    (totals.0 as u64, totals.1 as u64)
}

pub fn register_shared_memory_receiver(worker_id: WorkerId, registry: ExchangeRegistry) {
    RECEIVERS.lock().unwrap().insert(worker_id, registry);
}

pub fn unregister_shared_memory_receiver(worker_id: WorkerId) {
    RECEIVERS.lock().unwrap().remove(&worker_id);
}

pub fn ticket(ticket: &SharedMemoryTicketEnvelope) -> Result<Ticket, String> {
    serde_json::to_vec(ticket)
        .map(|bytes| Ticket {
            ticket: bytes.into(),
        })
        .map_err(|e| format!("serialize shared-memory ticket: {e}"))
}

async fn deliver_ticket(
    worker_id: WorkerId,
    ticket: SharedMemoryTicketEnvelope,
) -> Result<ShuffleAck, String> {
    let registry = RECEIVERS
        .lock()
        .unwrap()
        .get(&worker_id)
        .cloned()
        .ok_or_else(|| {
            format!("[{RS_3019}] no shared-memory receiver registered for {worker_id}")
        })?;

    #[cfg(feature = "simulation")]
    if rockstream_sim::buggify!("exchange.shm_segment_unavailable", 1.0) {
        return Err(format!(
            "[{RS_3019}] shared-memory segment {} is unavailable. Next steps: inspect host shared-memory capacity and worker reachability.",
            ticket.segment_id
        ));
    }

    let payload = SEGMENTS
        .lock()
        .unwrap()
        .remove(&ticket.segment_id)
        .ok_or_else(|| {
            format!(
                "[{RS_3019}] shared-memory segment {} is unavailable. Next steps: inspect host shared-memory capacity and worker reachability.",
                ticket.segment_id
            )
        })?
        .payload;

    let inlet = registry
        .get(ticket.exchange_id, ticket.target_shard)
        .ok_or_else(|| format!("target inlet not registered for worker {worker_id}"))?;
    let target_db = registry
        .get_shard_db(ticket.target_shard)
        .ok_or_else(|| format!("target shard db not active for worker {worker_id}"))?;

    // Fast-path shuffle WAL elision (v0.51, Slice 2): same-host shared-memory
    // delivery no longer persists a `shuffle_inbox/` key. Replay-dedup relies on
    // the target shard's committed frontier.
    let already_reflected = match persistence::committed_frontier(&target_db).await {
        Ok(frontier) => ticket.epoch <= frontier,
        Err(e) => {
            tracing::warn!(
                code = %RS_3023,
                exchange_id = ticket.exchange_id,
                target_shard = ticket.target_shard,
                epoch = ticket.epoch,
                "failed to read committed frontier for shared-memory replay dedup; delivering conservatively: {e}"
            );
            false
        }
    };
    if !already_reflected {
        let zset = serialization::deserialize_zset(&payload, inlet.schema.clone()).map_err(|e| {
            format!(
                "[{RS_3020}] shared-memory payload decode failed: {e}. Next steps: verify both workers run the same codec-capable build and inspect the payload bytes for corruption."
            )
        })?;
        inlet
            .sender
            .send(zset)
            .await
            .map_err(|_| "failed to deliver shared-memory batch".to_string())?;
    }

    Ok(ShuffleAck {
        exchange_id: ticket.exchange_id,
        src_shard: ticket.src_shard,
        target_shard: ticket.target_shard,
        epoch: ticket.epoch,
        seq: ticket.seq,
        credit_grant: ticket.row_count.max(1),
    })
}

pub fn encode_ack(ack: &ShuffleAck) -> Vec<u8> {
    ack.encode_to_vec()
}

pub fn decode_ack(bytes: &[u8]) -> Result<ShuffleAck, String> {
    ShuffleAck::decode(bytes)
        .map_err(|e| format!("[{RS_3020}] failed to decode shared-memory ACK: {e}"))
}

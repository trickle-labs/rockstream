//! Bounded, lease-owned execution actors.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use rockstream_types::data_plane::RuntimeExchangeMessage;
use rockstream_types::ids::{LeaseToken, ShardId};

pub const SHARD_ACTOR_MAILBOX_MESSAGES: usize = 32;
pub const SHARD_ACTOR_MAILBOX_BYTES: usize = 4 * 1024 * 1024;
const SHARD_ACTOR_MAILBOX_COMPUTE_MS: u64 = 32 * 50;

pub type FrameExecutor =
    Arc<dyn Fn(RuntimeExchangeMessage) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxFillLevel {
    pub messages: usize,
    pub bytes: usize,
    pub max_messages: usize,
    pub max_bytes: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ShardActorError {
    #[error("shard {0} has no current actor; next_steps: wait for lease assignment")]
    UnknownShard(ShardId),
    #[error(
        "shard {shard} actor mailbox is full ({messages}/{max_messages} messages, {bytes}/{max_bytes} bytes); next_steps: apply backpressure or add a shard"
    )]
    Full {
        shard: ShardId,
        messages: usize,
        max_messages: usize,
        bytes: usize,
        max_bytes: usize,
    },
    #[error(
        "shard {0} actor lease is stale; next_steps: route the frame to the current lease owner"
    )]
    StaleLease(ShardId),
    #[error("shard {0} actor stopped; next_steps: wait for lease assignment")]
    Closed(ShardId),
    #[error("execution frame could not be encoded: {0}; next_steps: inspect the frame schema")]
    Encode(String),
}

struct QueuedFrame {
    frame: RuntimeExchangeMessage,
    bytes: usize,
    _credit: ExchangeCredit,
}

struct ActorHandle {
    lease_token: LeaseToken,
    sender: mpsc::Sender<QueuedFrame>,
    credits: ExchangeCredits,
    queued_messages: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicUsize>,
    abort: tokio::task::AbortHandle,
}

#[derive(Clone, Default)]
pub struct ShardActorRegistry {
    actors: Arc<RwLock<HashMap<ShardId, ActorHandle>>>,
}

impl ShardActorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, shard_id: ShardId, lease_token: LeaseToken, execute: FrameExecutor) {
        if let Some(previous) = self.actors.write().remove(&shard_id) {
            previous.abort.abort();
        }

        let (sender, mut receiver) = mpsc::channel::<QueuedFrame>(SHARD_ACTOR_MAILBOX_MESSAGES);
        let credits =
            ExchangeCredits::new(SHARD_ACTOR_MAILBOX_BYTES, SHARD_ACTOR_MAILBOX_COMPUTE_MS);
        let queued_messages = Arc::new(AtomicUsize::new(0));
        let queued_bytes = Arc::new(AtomicUsize::new(0));
        let messages = queued_messages.clone();
        let bytes = queued_bytes.clone();
        let task = tokio::spawn(async move {
            while let Some(queued) = receiver.recv().await {
                messages.fetch_sub(1, Ordering::AcqRel);
                bytes.fetch_sub(queued.bytes, Ordering::AcqRel);
                execute(queued.frame).await;
            }
        });

        self.actors.write().insert(
            shard_id,
            ActorHandle {
                lease_token,
                sender,
                credits,
                queued_messages,
                queued_bytes,
                abort: task.abort_handle(),
            },
        );
    }

    pub fn enqueue(&self, frame: RuntimeExchangeMessage) -> Result<(), ShardActorError> {
        let shard_id = frame.shard_id;
        let actors = self.actors.read();
        let actor = actors
            .get(&shard_id)
            .ok_or(ShardActorError::UnknownShard(shard_id))?;
        if actor.lease_token != frame.lease_token {
            return Err(ShardActorError::StaleLease(shard_id));
        }

        let bytes = frame
            .encoded_len()
            .map_err(|error| ShardActorError::Encode(error.to_string()))?;
        let estimated_compute_ms = (frame.rows.len() as u64)
            .max(1)
            .min(MorselLimits::default().max_compute_ms);
        if !reserve(&actor.queued_messages, SHARD_ACTOR_MAILBOX_MESSAGES, 1) {
            let messages = actor.queued_messages.load(Ordering::Acquire);
            let queued_bytes = actor.queued_bytes.load(Ordering::Acquire);
            return Err(ShardActorError::Full {
                shard: shard_id,
                messages,
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                bytes: queued_bytes,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
            });
        }
        if !reserve(&actor.queued_bytes, SHARD_ACTOR_MAILBOX_BYTES, bytes) {
            actor.queued_messages.fetch_sub(1, Ordering::AcqRel);
            let queued_bytes = actor.queued_bytes.load(Ordering::Acquire);
            return Err(ShardActorError::Full {
                shard: shard_id,
                messages: actor.queued_messages.load(Ordering::Acquire),
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                bytes: queued_bytes,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
            });
        }
        let credit = match actor.credits.try_acquire(bytes, estimated_compute_ms) {
            Ok(credit) => credit,
            Err(_) => {
                actor.queued_messages.fetch_sub(1, Ordering::AcqRel);
                actor.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                let queued_bytes = actor.queued_bytes.load(Ordering::Acquire);
                return Err(ShardActorError::Full {
                    shard: shard_id,
                    messages: actor.queued_messages.load(Ordering::Acquire),
                    max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                    bytes: queued_bytes,
                    max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                });
            }
        };
        if actor
            .sender
            .try_send(QueuedFrame {
                frame,
                bytes,
                _credit: credit,
            })
            .is_err()
        {
            actor.queued_messages.fetch_sub(1, Ordering::AcqRel);
            actor.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
            return Err(ShardActorError::Closed(shard_id));
        }
        Ok(())
    }

    pub fn fill_level(&self, shard_id: ShardId) -> Option<MailboxFillLevel> {
        self.actors
            .read()
            .get(&shard_id)
            .map(|actor| MailboxFillLevel {
                messages: actor.queued_messages.load(Ordering::Acquire),
                bytes: actor.queued_bytes.load(Ordering::Acquire),
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
            })
    }

    pub fn revoke(&self, shard_id: ShardId) {
        if let Some(actor) = self.actors.write().remove(&shard_id) {
            actor.abort.abort();
        }
    }

    pub fn shutdown(&self) {
        let actors = std::mem::take(&mut *self.actors.write());
        for actor in actors.into_values() {
            actor.abort.abort();
        }
    }
}

fn reserve(counter: &AtomicUsize, limit: usize, amount: usize) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if current > limit.saturating_sub(amount) {
            return false;
        }
        match counter.compare_exchange_weak(
            current,
            current + amount,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselLimits {
    pub max_bytes: usize,
    pub max_compute_ms: u64,
}

impl Default for MorselLimits {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024,
            max_compute_ms: 50,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MorselFullReason {
    Bytes,
    ComputeTime,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MorselError {
    #[error("frame is {bytes} bytes but morsel limit is {limit} bytes")]
    FrameTooLarge { bytes: usize, limit: usize },
    #[error("morsel is full: {0:?}")]
    Full(MorselFullReason),
    #[error("frame encoding failed: {0}")]
    Encode(String),
}

pub struct ExecutionMorsel {
    limits: MorselLimits,
    frames: Vec<RuntimeExchangeMessage>,
    bytes: usize,
    compute_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreditError {
    Bytes,
    ComputeTime,
}

#[derive(Clone)]
pub struct ExchangeCredits {
    max_bytes: usize,
    max_compute_ms: u64,
    available_bytes: Arc<AtomicUsize>,
    available_compute_ms: Arc<AtomicU64>,
}

pub struct ExchangeCredit {
    available_bytes: Arc<AtomicUsize>,
    available_compute_ms: Arc<AtomicU64>,
    bytes: usize,
    compute_ms: u64,
}

impl ExchangeCredits {
    pub fn new(max_bytes: usize, max_compute_ms: u64) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            max_compute_ms: max_compute_ms.max(1),
            available_bytes: Arc::new(AtomicUsize::new(max_bytes.max(1))),
            available_compute_ms: Arc::new(AtomicU64::new(max_compute_ms.max(1))),
        }
    }

    pub fn available_bytes(&self) -> usize {
        self.available_bytes.load(Ordering::Acquire)
    }

    pub fn available_compute_ms(&self) -> u64 {
        self.available_compute_ms.load(Ordering::Acquire)
    }

    pub fn max_compute_ms(&self) -> u64 {
        self.max_compute_ms
    }

    pub fn try_acquire(
        &self,
        bytes: usize,
        estimated_compute_ms: u64,
    ) -> Result<ExchangeCredit, CreditError> {
        if bytes > self.max_bytes {
            return Err(CreditError::Bytes);
        }
        if estimated_compute_ms > self.max_compute_ms {
            return Err(CreditError::ComputeTime);
        }
        let mut available = self.available_bytes.load(Ordering::Acquire);
        loop {
            if available < bytes {
                return Err(CreditError::Bytes);
            }
            match self.available_bytes.compare_exchange_weak(
                available,
                available - bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => available = actual,
            }
        }

        let mut available_compute_ms = self.available_compute_ms.load(Ordering::Acquire);
        loop {
            if available_compute_ms < estimated_compute_ms {
                self.available_bytes.fetch_add(bytes, Ordering::AcqRel);
                return Err(CreditError::ComputeTime);
            }
            match self.available_compute_ms.compare_exchange_weak(
                available_compute_ms,
                available_compute_ms - estimated_compute_ms,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ExchangeCredit {
                        available_bytes: self.available_bytes.clone(),
                        available_compute_ms: self.available_compute_ms.clone(),
                        bytes,
                        compute_ms: estimated_compute_ms,
                    });
                }
                Err(actual) => available_compute_ms = actual,
            }
        }
    }
}

impl Drop for ExchangeCredit {
    fn drop(&mut self) {
        self.available_bytes.fetch_add(self.bytes, Ordering::AcqRel);
        self.available_compute_ms
            .fetch_add(self.compute_ms, Ordering::AcqRel);
    }
}

impl ExecutionMorsel {
    pub fn new(limits: MorselLimits) -> Self {
        Self {
            limits,
            frames: Vec::new(),
            bytes: 0,
            compute_ms: 0,
        }
    }

    pub fn push(&mut self, frame: RuntimeExchangeMessage) -> Result<(), MorselError> {
        let bytes = frame
            .encoded_len()
            .map_err(|error| MorselError::Encode(error.to_string()))?;
        if bytes > self.limits.max_bytes {
            return Err(MorselError::FrameTooLarge {
                bytes,
                limit: self.limits.max_bytes,
            });
        }
        if !self.frames.is_empty() {
            if self.bytes > self.limits.max_bytes.saturating_sub(bytes) {
                return Err(MorselError::Full(MorselFullReason::Bytes));
            }
            if self.compute_ms >= self.limits.max_compute_ms {
                return Err(MorselError::Full(MorselFullReason::ComputeTime));
            }
        }
        self.bytes += bytes;
        self.frames.push(frame);
        Ok(())
    }

    pub fn record_compute_ms(&mut self, elapsed_ms: u64) {
        self.compute_ms = self.compute_ms.saturating_add(elapsed_ms);
    }

    pub fn fill_level(&self) -> (usize, usize, u64) {
        (self.frames.len(), self.bytes, self.compute_ms)
    }

    pub fn into_frames(self) -> Vec<RuntimeExchangeMessage> {
        self.frames
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::data_plane::RuntimeRow;
    use rockstream_types::ids::{OperatorId, WorkloadId};

    fn frame(shard_id: u64, request_id: &str) -> RuntimeExchangeMessage {
        RuntimeExchangeMessage {
            version: 1,
            request_id: request_id.to_string(),
            workload_id: WorkloadId(1),
            shard_id: ShardId(shard_id),
            epoch: 1,
            operator_id: OperatorId(1),
            lease_token: LeaseToken(7),
            source: "source".to_string(),
            rows: vec![RuntimeRow {
                values_tsv: "1".to_string(),
                weight: 1,
            }],
        }
    }

    #[tokio::test]
    async fn actor_preserves_exact_frame_order() {
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let target = seen.clone();
        let execute: FrameExecutor = Arc::new(move |frame| {
            let target = target.clone();
            Box::pin(async move { target.lock().push(frame.request_id) })
        });
        let actors = ShardActorRegistry::new();
        actors.register(ShardId(1), LeaseToken(7), execute);
        actors.enqueue(frame(1, "first")).unwrap();
        actors.enqueue(frame(1, "second")).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert_eq!(&*seen.lock(), &["first", "second"]);
    }

    #[tokio::test]
    async fn revocation_removes_queued_frames() {
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let target = seen.clone();
        let execute: FrameExecutor = Arc::new(move |frame| {
            let target = target.clone();
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                target.lock().push(frame.request_id)
            })
        });
        let actors = ShardActorRegistry::new();
        actors.register(ShardId(1), LeaseToken(7), execute);
        actors.enqueue(frame(1, "stale")).unwrap();
        actors.revoke(ShardId(1));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert!(seen.lock().is_empty());
        assert_eq!(
            actors.enqueue(frame(1, "new")),
            Err(ShardActorError::UnknownShard(ShardId(1)))
        );
    }

    #[test]
    fn morsel_enforces_bytes_and_time_and_keeps_exact_frames() {
        let mut morsel = ExecutionMorsel::new(MorselLimits {
            max_bytes: 1_000,
            max_compute_ms: 10,
        });
        morsel.push(frame(1, "first")).unwrap();
        morsel.record_compute_ms(10);
        assert_eq!(
            morsel.push(frame(1, "second")),
            Err(MorselError::Full(MorselFullReason::ComputeTime))
        );
        assert_eq!(morsel.into_frames().len(), 1);
    }

    #[test]
    fn exchange_credits_bound_bytes_and_compute_time() {
        let credits = ExchangeCredits::new(10, 5);
        let permit = credits.try_acquire(6, 5).unwrap();
        assert_eq!(credits.available_bytes(), 4);
        assert_eq!(credits.available_compute_ms(), 0);
        assert!(matches!(credits.try_acquire(5, 1), Err(CreditError::Bytes)));
        assert!(matches!(
            credits.try_acquire(1, 6),
            Err(CreditError::ComputeTime)
        ));
        drop(permit);
        assert_eq!(credits.available_bytes(), 10);
        assert_eq!(credits.available_compute_ms(), 5);
    }
}

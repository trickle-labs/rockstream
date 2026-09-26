//! Bounded, lease-owned execution actors.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::{mpsc, watch};

use rockstream_types::data_plane::RuntimeExchangeMessage;
use rockstream_types::ids::{LeaseToken, ShardId};
use rockstream_types::state_budget::MemoryPermit;

use crate::source_pressure::SourcePressureController;

pub const SHARD_ACTOR_MAILBOX_MESSAGES: usize = 32;
pub const SHARD_ACTOR_MAILBOX_BYTES: usize = 4 * 1024 * 1024;
pub const SHARD_ACTOR_MAILBOX_OVERFLOW_POLICY: &str =
    "backpressure_until_credit_for_up_to_30s; oversized_or_timeout_returns_correlated_failure";
const SHARD_ACTOR_MAILBOX_COMPUTE_MS: u64 = 32 * 50;
const SHARD_ACTOR_BACKPRESSURE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub type FrameExecutor =
    Arc<dyn Fn(RuntimeExchangeMessage) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxFillLevel {
    pub messages: usize,
    pub bytes: usize,
    pub max_messages: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardMailboxStatus {
    pub shard_id: ShardId,
    pub messages: usize,
    pub bytes: usize,
    pub max_messages: usize,
    pub max_bytes: usize,
    pub overflow_policy: String,
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
    #[error("workload memory budget rejected exchange frame: {0}")]
    Budget(#[from] rockstream_types::state_budget::StateBudgetError),
    #[error("shard {0} actor remained full for 30 seconds; next_steps: retry after the worker drains queued frames")]
    BackpressureTimeout(ShardId),
}

struct QueuedFrame {
    frame: RuntimeExchangeMessage,
    bytes: usize,
    _credit: ExchangeCredit,
    _memory_permit: Option<MemoryPermit>,
}

struct ActorHandle {
    lease_token: LeaseToken,
    sender: mpsc::Sender<QueuedFrame>,
    credits: ExchangeCredits,
    queued_messages: Arc<AtomicUsize>,
    queued_bytes: Arc<AtomicUsize>,
    capacity_updates: watch::Sender<u64>,
    abort: tokio::task::AbortHandle,
}

#[derive(Clone, Default)]
pub struct ShardActorRegistry {
    actors: Arc<RwLock<HashMap<ShardId, ActorHandle>>>,
    source_pressure: Option<Arc<SourcePressureController>>,
}

impl ShardActorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_source_pressure(source_pressure: Arc<SourcePressureController>) -> Self {
        Self {
            actors: Arc::new(RwLock::new(HashMap::new())),
            source_pressure: Some(source_pressure),
        }
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
        let (capacity_updates, _) = watch::channel(0u64);
        let messages = queued_messages.clone();
        let bytes = queued_bytes.clone();
        let task_capacity_updates = capacity_updates.clone();
        let task = tokio::spawn(async move {
            while let Some(queued) = receiver.recv().await {
                messages.fetch_sub(1, Ordering::AcqRel);
                bytes.fetch_sub(queued.bytes, Ordering::AcqRel);
                let QueuedFrame {
                    frame,
                    _credit,
                    _memory_permit,
                    ..
                } = queued;
                execute(frame).await;
                drop((_credit, _memory_permit));
                let _ = task_capacity_updates.send_modify(|generation| {
                    *generation = generation.wrapping_add(1);
                });
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
                capacity_updates,
                abort: task.abort_handle(),
            },
        );
    }

    pub fn enqueue(&self, frame: RuntimeExchangeMessage) -> Result<(), ShardActorError> {
        let memory_permit = self.reserve_exchange_buffer(&frame)?;
        let bytes = frame
            .encoded_len()
            .map_err(|error| ShardActorError::Encode(error.to_string()))?;
        self.try_enqueue(frame, bytes, memory_permit)
            .map_err(|(_, _, error)| error)
    }

    /// Pace soft-pressured workloads and wait for mailbox credits instead of dropping frames.
    pub async fn enqueue_with_backpressure(
        &self,
        mut frame: RuntimeExchangeMessage,
    ) -> Result<(), ShardActorError> {
        let mut memory_permit = self.reserve_exchange_buffer(&frame)?;
        let deadline = tokio::time::Instant::now() + SHARD_ACTOR_BACKPRESSURE_TIMEOUT;
        if let Some(pressure) = &self.source_pressure {
            let delay = pressure.workload_frame_delay(frame.workload_id);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }

        let bytes = frame
            .encoded_len()
            .map_err(|error| ShardActorError::Encode(error.to_string()))?;
        loop {
            let shard_id = frame.shard_id;
            let (mut capacity_updates, sender) = {
                let actors = self.actors.read();
                let actor = actors
                    .get(&shard_id)
                    .ok_or(ShardActorError::UnknownShard(shard_id))?;
                (actor.capacity_updates.subscribe(), actor.sender.clone())
            };
            match self.try_enqueue(frame, bytes, memory_permit) {
                Ok(()) => return Ok(()),
                Err((next_frame, next_permit, error @ ShardActorError::Full { .. })) => {
                    if bytes > SHARD_ACTOR_MAILBOX_BYTES {
                        return Err(error);
                    }
                    frame = next_frame;
                    memory_permit = next_permit;
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(ShardActorError::BackpressureTimeout(shard_id));
                    }
                    let wake = tokio::time::timeout(remaining, async {
                        tokio::select! {
                            changed = capacity_updates.changed() => {
                                let _ = changed;
                            }
                            _ = sender.closed() => {}
                        }
                    })
                    .await;
                    if wake.is_err() {
                        return Err(ShardActorError::BackpressureTimeout(shard_id));
                    }
                }
                Err((_, _, error)) => return Err(error),
            }
        }
    }

    fn reserve_exchange_buffer(
        &self,
        frame: &RuntimeExchangeMessage,
    ) -> Result<Option<MemoryPermit>, ShardActorError> {
        self.source_pressure
            .as_ref()
            .map(|pressure| {
                pressure.can_ingest()?;
                pressure.reserve_exchange_frame(
                    frame.workload_id,
                    estimate_exchange_buffer_bytes(frame),
                )
            })
            .transpose()
            .map_err(ShardActorError::from)
    }

    fn try_enqueue(
        &self,
        frame: RuntimeExchangeMessage,
        bytes: usize,
        memory_permit: Option<MemoryPermit>,
    ) -> Result<
        (),
        (
            RuntimeExchangeMessage,
            Option<MemoryPermit>,
            ShardActorError,
        ),
    > {
        let shard_id = frame.shard_id;
        let actors = self.actors.read();
        let Some(actor) = actors.get(&shard_id) else {
            return Err((
                frame,
                memory_permit,
                ShardActorError::UnknownShard(shard_id),
            ));
        };
        if actor.lease_token != frame.lease_token {
            return Err((frame, memory_permit, ShardActorError::StaleLease(shard_id)));
        }

        let estimated_compute_ms = (frame.rows.len() as u64)
            .max(1)
            .min(MorselLimits::default().max_compute_ms);
        if !reserve(&actor.queued_messages, SHARD_ACTOR_MAILBOX_MESSAGES, 1) {
            let messages = actor.queued_messages.load(Ordering::Acquire);
            let queued_bytes = actor.queued_bytes.load(Ordering::Acquire);
            return Err((
                frame,
                memory_permit,
                ShardActorError::Full {
                    shard: shard_id,
                    messages,
                    max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                    bytes: queued_bytes,
                    max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                },
            ));
        }
        if !reserve(&actor.queued_bytes, SHARD_ACTOR_MAILBOX_BYTES, bytes) {
            actor.queued_messages.fetch_sub(1, Ordering::AcqRel);
            let queued_bytes = actor.queued_bytes.load(Ordering::Acquire);
            return Err((
                frame,
                memory_permit,
                ShardActorError::Full {
                    shard: shard_id,
                    messages: actor.queued_messages.load(Ordering::Acquire),
                    max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                    bytes: queued_bytes,
                    max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                },
            ));
        }
        let credit = match actor.credits.try_acquire(bytes, estimated_compute_ms) {
            Ok(credit) => credit,
            Err(_) => {
                actor.queued_messages.fetch_sub(1, Ordering::AcqRel);
                actor.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                let queued_bytes = actor.queued_bytes.load(Ordering::Acquire);
                return Err((
                    frame,
                    memory_permit,
                    ShardActorError::Full {
                        shard: shard_id,
                        messages: actor.queued_messages.load(Ordering::Acquire),
                        max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                        bytes: queued_bytes,
                        max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                    },
                ));
            }
        };
        match actor.sender.try_send(QueuedFrame {
            frame,
            bytes,
            _credit: credit,
            _memory_permit: memory_permit,
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(queued)) => {
                actor.queued_messages.fetch_sub(1, Ordering::AcqRel);
                actor.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                let queued_bytes = actor.queued_bytes.load(Ordering::Acquire);
                Err((
                    queued.frame,
                    queued._memory_permit,
                    ShardActorError::Full {
                        shard: shard_id,
                        messages: actor.queued_messages.load(Ordering::Acquire),
                        max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                        bytes: queued_bytes,
                        max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                    },
                ))
            }
            Err(mpsc::error::TrySendError::Closed(queued)) => {
                actor.queued_messages.fetch_sub(1, Ordering::AcqRel);
                actor.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                Err((
                    queued.frame,
                    queued._memory_permit,
                    ShardActorError::Closed(shard_id),
                ))
            }
        }
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

    pub fn mailbox_status(&self) -> Vec<ShardMailboxStatus> {
        let mut status = self
            .actors
            .read()
            .iter()
            .map(|(shard_id, actor)| ShardMailboxStatus {
                shard_id: *shard_id,
                messages: actor.queued_messages.load(Ordering::Acquire),
                bytes: actor.queued_bytes.load(Ordering::Acquire),
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                overflow_policy: SHARD_ACTOR_MAILBOX_OVERFLOW_POLICY.to_owned(),
            })
            .collect::<Vec<_>>();
        status.sort_by_key(|mailbox| mailbox.shard_id);
        status
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

fn estimate_exchange_buffer_bytes(frame: &RuntimeExchangeMessage) -> u64 {
    (std::mem::size_of::<RuntimeExchangeMessage>() as u64)
        .saturating_add(
            (frame.rows.len() as u64).saturating_mul(std::mem::size_of::<
                rockstream_types::data_plane::RuntimeRow,
            >() as u64),
        )
        .saturating_add(frame.rows.iter().fold(0u64, |total, row| {
            total.saturating_add(row.values_tsv.len() as u64)
        }))
        .saturating_add(frame.request_id.len() as u64)
        .saturating_add(frame.source.len() as u64)
        .saturating_add(32)
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
    use rockstream_types::state_budget::{MemoryCategory, MemoryOwner, WorkerBudgetLedger};
    use tokio::sync::Notify;

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
    async fn soft_pressure_paces_and_retains_the_queued_frame() {
        let ledger = Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729));
        let pressure = Arc::new(SourcePressureController::new(ledger.clone(), 100));
        let existing = ledger
            .try_acquire_for_owner(
                MemoryCategory::SourceBuffers,
                MemoryOwner::workload(WorkloadId(1)),
                super::super::source_pressure::WORKLOAD_SOURCE_SOFT_LIMIT_BYTES - 1,
                true,
            )
            .unwrap();
        let start = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (done_tx, mut done_rx) = mpsc::channel(1);
        let execute: FrameExecutor = Arc::new({
            let start = start.clone();
            let release = release.clone();
            move |frame| {
                let start = start.clone();
                let release = release.clone();
                let done_tx = done_tx.clone();
                Box::pin(async move {
                    start.notify_one();
                    release.notified().await;
                    let _ = done_tx.send(frame.request_id).await;
                })
            }
        });
        let actors = ShardActorRegistry::with_source_pressure(pressure.clone());
        actors.register(ShardId(1), LeaseToken(7), execute);

        let frame = frame(1, "soft-pressured");
        let exchange_bytes = estimate_exchange_buffer_bytes(&frame);
        let started = tokio::time::Instant::now();
        actors.enqueue_with_backpressure(frame).await.unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(20));
        start.notified().await;
        assert_eq!(
            pressure.workload_status(WorkloadId(1)),
            crate::source_pressure::WorkloadSourcePressureStatus {
                workload_id: WorkloadId(1),
                allocated_bytes: super::super::source_pressure::WORKLOAD_SOURCE_SOFT_LIMIT_BYTES
                    - 1
                    + exchange_bytes,
                soft_limit_bytes: super::super::source_pressure::WORKLOAD_SOURCE_SOFT_LIMIT_BYTES,
                state: crate::source_pressure::SourcePressureState::Throttled,
                available_credits: 50,
            }
        );

        release.notify_one();
        assert_eq!(done_rx.recv().await.as_deref(), Some("soft-pressured"));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pressure.workload_status(WorkloadId(1)).allocated_bytes
                != super::super::source_pressure::WORKLOAD_SOURCE_SOFT_LIMIT_BYTES - 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            pressure.workload_status(WorkloadId(1)),
            crate::source_pressure::WorkloadSourcePressureStatus {
                workload_id: WorkloadId(1),
                allocated_bytes: super::super::source_pressure::WORKLOAD_SOURCE_SOFT_LIMIT_BYTES
                    - 1,
                soft_limit_bytes: super::super::source_pressure::WORKLOAD_SOURCE_SOFT_LIMIT_BYTES,
                state: crate::source_pressure::SourcePressureState::Normal,
                available_credits: 100,
            }
        );
        drop(existing);
    }

    #[tokio::test]
    async fn mailbox_status_reports_exact_fill_overflow_and_recovery() {
        let start = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (done_tx, mut done_rx) = mpsc::channel(SHARD_ACTOR_MAILBOX_MESSAGES + 1);
        let execute: FrameExecutor = Arc::new({
            let start = start.clone();
            let release = release.clone();
            let first = first.clone();
            move |frame| {
                let start = start.clone();
                let release = release.clone();
                let first = first.clone();
                let done_tx = done_tx.clone();
                Box::pin(async move {
                    if first.swap(false, Ordering::AcqRel) {
                        start.notify_one();
                        release.notified().await;
                    }
                    let _ = done_tx.send(frame.request_id).await;
                })
            }
        });
        let actors = ShardActorRegistry::new();
        actors.register(ShardId(1), LeaseToken(7), execute);
        actors.enqueue(frame(1, "running")).unwrap();
        start.notified().await;

        let mut expected_ids = vec!["running".to_string()];
        let mut queued_bytes = 0;
        for index in 0..SHARD_ACTOR_MAILBOX_MESSAGES {
            let request_id = format!("queued-{index}");
            let queued = frame(1, &request_id);
            queued_bytes += queued.encoded_len().unwrap();
            actors.enqueue(queued).unwrap();
            expected_ids.push(request_id);
        }
        assert_eq!(
            actors.mailbox_status(),
            vec![ShardMailboxStatus {
                shard_id: ShardId(1),
                messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                bytes: queued_bytes,
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                overflow_policy: SHARD_ACTOR_MAILBOX_OVERFLOW_POLICY.to_string(),
            }]
        );

        let overflowing = frame(1, "overflow");
        assert_eq!(
            actors.enqueue(overflowing),
            Err(ShardActorError::Full {
                shard: ShardId(1),
                messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                bytes: queued_bytes,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
            })
        );
        assert_eq!(
            actors.mailbox_status(),
            vec![ShardMailboxStatus {
                shard_id: ShardId(1),
                messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                bytes: queued_bytes,
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                overflow_policy: SHARD_ACTOR_MAILBOX_OVERFLOW_POLICY.to_string(),
            }]
        );

        release.notify_one();
        let mut processed = Vec::with_capacity(expected_ids.len());
        for _ in 0..expected_ids.len() {
            processed.push(
                tokio::time::timeout(std::time::Duration::from_secs(1), done_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(processed, expected_ids);
        assert_eq!(
            actors.mailbox_status(),
            vec![ShardMailboxStatus {
                shard_id: ShardId(1),
                messages: 0,
                bytes: 0,
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
                overflow_policy: SHARD_ACTOR_MAILBOX_OVERFLOW_POLICY.to_string(),
            }]
        );
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

    #[tokio::test]
    async fn paused_worker_rejects_source_before_mailbox_admission() {
        let ledger = Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729));
        let pressure = Arc::new(SourcePressureController::new(ledger.clone(), 100));
        let _existing = ledger
            .try_acquire_for_owner(
                MemoryCategory::OperatorState,
                MemoryOwner::worker("test-operator-state"),
                1_900_000_000,
                true,
            )
            .unwrap();
        let execute: FrameExecutor = Arc::new(|_| Box::pin(async {}));
        let actors = ShardActorRegistry::with_source_pressure(pressure);
        actors.register(ShardId(1), LeaseToken(7), execute);

        let error = actors.enqueue(frame(1, "paused")).unwrap_err();

        assert_eq!(
            error.to_string(),
            "workload memory budget rejected exchange frame: RS-5003: state budget exceeded for 'source_pressure_paused': current=1900000000 bytes, requested=0 bytes, limit=2147483648 bytes"
        );
        assert_eq!(ledger.category_bytes(MemoryCategory::ExchangeBuffers), 0);
        assert_eq!(
            actors.fill_level(ShardId(1)),
            Some(MailboxFillLevel {
                messages: 0,
                bytes: 0,
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
            })
        );
    }

    #[tokio::test]
    async fn workload_hard_limit_admits_exact_boundary_and_recovers_after_release() {
        let ledger = Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729));
        let pressure = Arc::new(SourcePressureController::new(ledger.clone(), 100));
        let (started_tx, mut started_rx) = mpsc::channel(2);
        let (completed_tx, mut completed_rx) = mpsc::channel(2);
        let release = Arc::new(Notify::new());
        let execute: FrameExecutor = Arc::new({
            let release = release.clone();
            move |frame| {
                let started_tx = started_tx.clone();
                let completed_tx = completed_tx.clone();
                let release = release.clone();
                Box::pin(async move {
                    let request_id = frame.request_id;
                    let _ = started_tx.send(request_id.clone()).await;
                    release.notified().await;
                    let _ = completed_tx.send(request_id).await;
                })
            }
        });
        let actors = ShardActorRegistry::with_source_pressure(pressure);
        actors.register(ShardId(1), LeaseToken(7), execute);
        let owner = MemoryOwner::workload(WorkloadId(1));

        let exact_frame = frame(1, "at-limit");
        let exact_bytes = estimate_exchange_buffer_bytes(&exact_frame);
        let exact_state_bytes = WorkerBudgetLedger::WORKLOAD_MEMORY_HARD_LIMIT_BYTES - exact_bytes;
        let exact_state = ledger
            .try_acquire_for_owner(
                MemoryCategory::OperatorState,
                owner.clone(),
                exact_state_bytes,
                false,
            )
            .unwrap();
        actors.enqueue_with_backpressure(exact_frame).await.unwrap();
        assert_eq!(started_rx.recv().await.unwrap(), "at-limit");
        assert_eq!(
            ledger.allocated_bytes_for_owner(&owner),
            WorkerBudgetLedger::WORKLOAD_MEMORY_HARD_LIMIT_BYTES
        );
        assert_eq!(
            ledger.category_bytes(MemoryCategory::ExchangeBuffers),
            exact_bytes
        );
        release.notify_one();
        assert_eq!(completed_rx.recv().await.unwrap(), "at-limit");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while ledger.allocated_bytes_for_owner(&owner) != exact_state_bytes {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(exact_state);
        assert_eq!(ledger.allocated_bytes_for_owner(&owner), 0);

        let over_frame = frame(1, "over-limit");
        let over_bytes = estimate_exchange_buffer_bytes(&over_frame);
        let over_state_bytes =
            WorkerBudgetLedger::WORKLOAD_MEMORY_HARD_LIMIT_BYTES - over_bytes + 1;
        let over_state = ledger
            .try_acquire_for_owner(
                MemoryCategory::OperatorState,
                owner.clone(),
                over_state_bytes,
                false,
            )
            .unwrap();
        let error = actors
            .enqueue_with_backpressure(over_frame)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "workload memory budget rejected exchange frame: RS-5003: state budget exceeded for 'workload-hard-budget-1': current={over_state_bytes} bytes, requested={over_bytes} bytes, limit={} bytes",
                WorkerBudgetLedger::WORKLOAD_MEMORY_HARD_LIMIT_BYTES
            )
        );
        assert_eq!(ledger.allocated_bytes_for_owner(&owner), over_state_bytes);
        assert_eq!(ledger.category_bytes(MemoryCategory::ExchangeBuffers), 0);
        assert_eq!(
            actors.fill_level(ShardId(1)),
            Some(MailboxFillLevel {
                messages: 0,
                bytes: 0,
                max_messages: SHARD_ACTOR_MAILBOX_MESSAGES,
                max_bytes: SHARD_ACTOR_MAILBOX_BYTES,
            })
        );
        drop(over_state);
        assert_eq!(ledger.allocated_bytes_for_owner(&owner), 0);

        actors
            .enqueue_with_backpressure(frame(1, "recovered"))
            .await
            .unwrap();
        assert_eq!(started_rx.recv().await.unwrap(), "recovered");
        release.notify_one();
        assert_eq!(completed_rx.recv().await.unwrap(), "recovered");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while ledger.allocated_bytes_for_owner(&owner) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

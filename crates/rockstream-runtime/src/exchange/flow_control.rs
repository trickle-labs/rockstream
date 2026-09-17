use crate::exchange::proto::ShuffleAck;
use parking_lot::Mutex;
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{RS_3006, RS_3024};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;

pub const DEFAULT_MAX_INFLIGHT_BATCHES: usize = 64;
pub const DEFAULT_MAX_INFLIGHT_BYTES: usize = 64 * 1024 * 1024; // 64 MiB
pub const DEFAULT_MAX_BATCH_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
pub const DEFAULT_MAX_PENDING_REQUESTS: usize = 256;

type CreditKey = (u64, u32, u32);

#[derive(Clone, Copy)]
struct ChannelCreditState {
    available_rows: u32,
    max_rows: u32,
    rows_in_flight: u32,
    batches_in_flight: usize,
    bytes_in_flight: usize,
}

impl ChannelCreditState {
    fn new(max_rows: u32) -> Self {
        Self {
            available_rows: max_rows,
            max_rows,
            rows_in_flight: 0,
            batches_in_flight: 0,
            bytes_in_flight: 0,
        }
    }
}

/// An RAII guard permit for exchange batch flow control.
/// Automatically releases backpressure permits on Drop or explicit `release()`.
pub struct FlowPermit {
    controller: FlowController,
    key: CreditKey,
    batch_bytes: usize,
    row_count: u32,
    acquired_at: Instant,
    active: bool,
}

impl std::fmt::Debug for FlowPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowPermit")
            .field("key", &self.key)
            .field("batch_bytes", &self.batch_bytes)
            .field("row_count", &self.row_count)
            .field("active", &self.active)
            .finish()
    }
}

impl FlowPermit {
    pub fn acquired_at(&self) -> Instant {
        self.acquired_at
    }

    pub fn queue_age(&self) -> std::time::Duration {
        self.acquired_at.elapsed()
    }

    pub fn release(mut self) {
        if self.active {
            self.active = false;
            self.controller
                .release_batch_permit(self.key, self.batch_bytes, self.row_count);
        }
    }
}

impl Drop for FlowPermit {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.controller
                .release_batch_permit(self.key, self.batch_bytes, self.row_count);
        }
    }
}

/// Manages multi-limit transport flow control for outbox exchange channels.
///
/// Enforces four explicit bounds:
/// 1. `max_batch_bytes`: 16 MiB maximum single frame size (rejected before allocation).
/// 2. `max_inflight_batches`: 64 per channel.
/// 3. `max_inflight_bytes`: 64 MiB aggregate across worker.
/// 4. `max_pending_requests`: 256 concurrent pending RPC calls.
#[derive(Clone)]
pub struct FlowController {
    row_budget: Arc<AtomicU32>,
    max_inflight_batches: usize,
    max_inflight_bytes: usize,
    max_batch_bytes: usize,
    max_pending_requests: usize,
    channels: Arc<Mutex<HashMap<CreditKey, ChannelCreditState>>>,
    notifiers: Arc<Mutex<HashMap<CreditKey, Arc<Notify>>>>,
    aggregate_bytes: Arc<AtomicU64>,
    pending_requests: Arc<AtomicU64>,
    global_notify: Arc<Notify>,
}

impl Default for FlowController {
    fn default() -> Self {
        Self::with_row_budget(RockstreamConfig::default().worker.max_rows_per_quantum as u32)
    }
}

impl FlowController {
    /// Create a new flow controller using default worker limits.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new flow controller with an explicit per-channel row budget.
    pub fn with_row_budget(row_budget: u32) -> Self {
        Self {
            row_budget: Arc::new(AtomicU32::new(row_budget.max(1))),
            max_inflight_batches: DEFAULT_MAX_INFLIGHT_BATCHES,
            max_inflight_bytes: DEFAULT_MAX_INFLIGHT_BYTES,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_pending_requests: DEFAULT_MAX_PENDING_REQUESTS,
            channels: Arc::new(Mutex::new(HashMap::new())),
            notifiers: Arc::new(Mutex::new(HashMap::new())),
            aggregate_bytes: Arc::new(AtomicU64::new(0)),
            pending_requests: Arc::new(AtomicU64::new(0)),
            global_notify: Arc::new(Notify::new()),
        }
    }

    pub fn with_max_inflight_batches(mut self, limit: usize) -> Self {
        self.max_inflight_batches = limit.max(1);
        self
    }

    pub fn with_max_inflight_bytes(mut self, limit: usize) -> Self {
        self.max_inflight_bytes = limit.max(1);
        self
    }

    pub fn with_max_batch_bytes(mut self, limit: usize) -> Self {
        self.max_batch_bytes = limit.max(1);
        self
    }

    pub fn with_max_pending_requests(mut self, limit: usize) -> Self {
        self.max_pending_requests = limit.max(1);
        self
    }

    pub fn set_row_budget(&self, row_budget: u32) {
        self.row_budget.store(row_budget.max(1), Ordering::Relaxed);
    }

    pub fn row_budget(&self) -> u32 {
        self.row_budget.load(Ordering::Relaxed).max(1)
    }

    pub fn max_inflight_batches(&self) -> usize {
        self.max_inflight_batches
    }

    pub fn max_inflight_bytes(&self) -> usize {
        self.max_inflight_bytes
    }

    pub fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    pub fn max_pending_requests(&self) -> usize {
        self.max_pending_requests
    }

    pub fn bytes_in_flight(&self) -> usize {
        self.aggregate_bytes.load(Ordering::Relaxed) as usize
    }

    pub fn pending_requests(&self) -> usize {
        self.pending_requests.load(Ordering::Relaxed) as usize
    }

    pub fn batches_in_flight(&self, exchange_id: u64, src_shard: u32, target_shard: u32) -> usize {
        let key = (exchange_id, src_shard, target_shard);
        let channels = self.channels.lock();
        channels.get(&key).map(|s| s.batches_in_flight).unwrap_or(0)
    }

    pub fn fill_ratio_bytes(&self) -> f64 {
        self.bytes_in_flight() as f64 / self.max_inflight_bytes as f64
    }

    pub fn fill_ratio_batches(&self, exchange_id: u64, src_shard: u32, target_shard: u32) -> f64 {
        self.batches_in_flight(exchange_id, src_shard, target_shard) as f64
            / self.max_inflight_batches as f64
    }

    pub fn fill_ratio_pending(&self) -> f64 {
        self.pending_requests() as f64 / self.max_pending_requests as f64
    }

    fn update_credit_metrics(channels: &HashMap<CreditKey, ChannelCreditState>) {
        let rows_in_flight = channels
            .values()
            .map(|state| state.rows_in_flight as u64)
            .sum::<u64>();
        rockstream_types::metrics::update_flow_control_metrics(
            rows_in_flight,
            channels.len() as u64,
        );
    }

    fn update_batch_metrics(
        channels: &HashMap<CreditKey, ChannelCreditState>,
        aggregate_bytes: u64,
        pending: u64,
    ) {
        let rows_in_flight = channels
            .values()
            .map(|state| state.rows_in_flight as u64)
            .sum::<u64>();
        let batches_in_flight = channels
            .values()
            .map(|state| state.batches_in_flight as u64)
            .sum::<u64>();

        rockstream_types::metrics::update_flow_batch_metrics(
            rows_in_flight,
            channels.len() as u64,
            aggregate_bytes,
            batches_in_flight,
            pending,
        );
    }

    /// Check if frame size exceeds max_batch_bytes without mutating state.
    pub fn check_frame_size(&self, batch_bytes: usize) -> Result<(), String> {
        if batch_bytes > self.max_batch_bytes {
            return Err(format!(
                "[{RS_3006}] exchange frame size ({batch_bytes} bytes) exceeds max_batch_bytes ({}) limit. Next steps: split large record batches into smaller chunks within configured transport budget.",
                self.max_batch_bytes
            ));
        }
        Ok(())
    }

    /// Try to acquire an exchange batch permit non-blockingly, failing immediately if saturated.
    pub fn try_acquire_batch_permit(
        &self,
        exchange_id: u64,
        src_shard: u32,
        target_shard: u32,
        batch_bytes: usize,
        row_count: u32,
    ) -> Result<FlowPermit, String> {
        self.check_frame_size(batch_bytes)?;

        let row_count = row_count.max(1);
        let max_rows = self.row_budget();
        if row_count > max_rows {
            return Err(format!(
                "[{RS_3024}] shuffle frame carries {row_count} rows but worker.max_rows_per_quantum only permits {max_rows}. Next steps: reduce exchange batch size/rechunking or raise worker.max_rows_per_quantum."
            ));
        }

        let key = (exchange_id, src_shard, target_shard);
        let mut channels = self.channels.lock();

        let cur_pending = self.pending_requests.load(Ordering::Relaxed) as usize;
        if cur_pending >= self.max_pending_requests {
            return Err(format!(
                "[{RS_3006}] RESOURCE_EXHAUSTED: max_pending_requests ({}) exceeded. Next steps: wait for existing exchange requests to complete or raise max_pending_requests limit.",
                self.max_pending_requests
            ));
        }

        let cur_bytes = self.aggregate_bytes.load(Ordering::Relaxed) as usize;
        if cur_bytes.saturating_add(batch_bytes) > self.max_inflight_bytes {
            return Err(format!(
                "[{RS_3006}] RESOURCE_EXHAUSTED: max_inflight_bytes ({}) exceeded (currently {cur_bytes} + {batch_bytes}). Next steps: wait for in-flight batches to be acknowledged or reduce batch size.",
                self.max_inflight_bytes
            ));
        }

        let state = channels
            .entry(key)
            .or_insert_with(|| ChannelCreditState::new(max_rows));

        if state.batches_in_flight >= self.max_inflight_batches {
            return Err(format!(
                "[{RS_3006}] RESOURCE_EXHAUSTED: max_inflight_batches ({}) exceeded for channel. Next steps: wait for in-flight channel batches to clear.",
                self.max_inflight_batches
            ));
        }

        if state.available_rows < row_count {
            return Err(format!(
                "[{RS_3006}] RESOURCE_EXHAUSTED: channel row budget exhausted (available: {}, requested: {row_count}). Next steps: wait for ShuffleAck credit release.",
                state.available_rows
            ));
        }

        // All 4 limits satisfied: acquire
        state.available_rows -= row_count;
        state.rows_in_flight = state.rows_in_flight.saturating_add(row_count);
        state.batches_in_flight += 1;
        state.bytes_in_flight += batch_bytes;

        let new_bytes = self
            .aggregate_bytes
            .fetch_add(batch_bytes as u64, Ordering::Relaxed)
            + batch_bytes as u64;
        let new_pending = self.pending_requests.fetch_add(1, Ordering::Relaxed) + 1;

        Self::update_batch_metrics(&channels, new_bytes, new_pending);

        Ok(FlowPermit {
            controller: self.clone(),
            key,
            batch_bytes,
            row_count,
            acquired_at: Instant::now(),
            active: true,
        })
    }

    /// Acquire an exchange batch permit asynchronously, suspending if saturated until permits are freed.
    pub async fn acquire_batch_permit(
        &self,
        exchange_id: u64,
        src_shard: u32,
        target_shard: u32,
        batch_bytes: usize,
        row_count: u32,
    ) -> Result<FlowPermit, String> {
        self.check_frame_size(batch_bytes)?;

        let row_count = row_count.max(1);
        let max_rows = self.row_budget();
        if row_count > max_rows {
            return Err(format!(
                "[{RS_3024}] shuffle frame carries {row_count} rows but worker.max_rows_per_quantum only permits {max_rows}. Next steps: reduce exchange batch size/rechunking or raise worker.max_rows_per_quantum."
            ));
        }

        loop {
            match self.try_acquire_batch_permit(
                exchange_id,
                src_shard,
                target_shard,
                batch_bytes,
                row_count,
            ) {
                Ok(permit) => return Ok(permit),
                Err(err) => {
                    if !err.contains("RESOURCE_EXHAUSTED") {
                        return Err(err);
                    }
                    self.global_notify.notified().await;
                }
            }
        }
    }

    /// Internal release called by `FlowPermit` on drop/release.
    fn release_batch_permit(&self, key: CreditKey, batch_bytes: usize, row_count: u32) {
        {
            let mut channels = self.channels.lock();
            if let Some(state) = channels.get_mut(&key) {
                let released_rows = row_count.min(state.rows_in_flight);
                state.rows_in_flight -= released_rows;
                state.available_rows =
                    (state.available_rows.saturating_add(row_count)).min(state.max_rows);
                state.batches_in_flight = state.batches_in_flight.saturating_sub(1);
                state.bytes_in_flight = state.bytes_in_flight.saturating_sub(batch_bytes);
            }

            let new_bytes = self
                .aggregate_bytes
                .fetch_sub(batch_bytes as u64, Ordering::Relaxed)
                .saturating_sub(batch_bytes as u64);
            let new_pending = self
                .pending_requests
                .fetch_sub(1, Ordering::Relaxed)
                .saturating_sub(1);

            Self::update_batch_metrics(&channels, new_bytes, new_pending);
        }

        self.global_notify.notify_waiters();

        let notifiers = self.notifiers.lock();
        if let Some(notify) = notifiers.get(&key) {
            notify.notify_waiters();
        }
    }

    /// Purge all channel credit state and notifier entries for the given exchange.
    pub fn teardown_exchange(&self, exchange_id: u64) {
        {
            let mut channels = self.channels.lock();
            channels.retain(|k, _| k.0 != exchange_id);
            let cur_bytes = self.aggregate_bytes.load(Ordering::Relaxed);
            let cur_pending = self.pending_requests.load(Ordering::Relaxed);
            Self::update_batch_metrics(&channels, cur_bytes, cur_pending);
        }
        let notified: Vec<Arc<Notify>> = {
            let mut notifiers = self.notifiers.lock();
            let keys: Vec<CreditKey> = notifiers
                .keys()
                .filter(|k| k.0 == exchange_id)
                .cloned()
                .collect();
            let mut list = Vec::new();
            for k in keys {
                if let Some(n) = notifiers.remove(&k) {
                    list.push(n);
                }
            }
            list
        };
        for n in notified {
            n.notify_waiters();
        }
        self.global_notify.notify_waiters();
    }

    fn channel_state<'a>(
        &'a self,
        channels: &'a mut HashMap<CreditKey, ChannelCreditState>,
        key: CreditKey,
    ) -> &'a mut ChannelCreditState {
        channels
            .entry(key)
            .or_insert_with(|| ChannelCreditState::new(self.row_budget()))
    }

    /// Process a received ShuffleAck and restore row credits for the corresponding channel.
    pub fn handle_ack(&self, ack: &ShuffleAck) {
        self.release_credit(
            ack.exchange_id,
            ack.src_shard,
            ack.target_shard,
            ack.credit_grant,
        );
    }

    pub fn release_credit(
        &self,
        exchange_id: u64,
        src_shard: u32,
        target_shard: u32,
        row_count: u32,
    ) {
        let key = (exchange_id, src_shard, target_shard);
        {
            let mut channels = self.channels.lock();
            let state = self.channel_state(&mut channels, key);
            let released = row_count.min(state.rows_in_flight);
            state.rows_in_flight -= released;
            state.available_rows =
                (state.available_rows.saturating_add(row_count)).min(state.max_rows);
            Self::update_credit_metrics(&channels);
        }

        if self.aggregate_bytes.load(Ordering::Relaxed) > 0
            || self.pending_requests.load(Ordering::Relaxed) > 0
        {
            self.global_notify.notify_waiters();
        }
        let notifiers = self.notifiers.lock();
        if let Some(notify) = notifiers.get(&key) {
            notify.notify_waiters();
        }
    }

    /// Check current available row credits without blocking or consuming.
    pub fn get_credits(&self, exchange_id: u64, src_shard: u32, target_shard: u32) -> u32 {
        let key = (exchange_id, src_shard, target_shard);
        let channels = self.channels.lock();
        channels
            .get(&key)
            .copied()
            .unwrap_or_else(|| ChannelCreditState::new(self.row_budget()))
            .available_rows
    }

    pub fn rows_in_flight(&self, exchange_id: u64, src_shard: u32, target_shard: u32) -> u32 {
        let key = (exchange_id, src_shard, target_shard);
        let channels = self.channels.lock();
        channels
            .get(&key)
            .map(|state| state.rows_in_flight)
            .unwrap_or(0)
    }

    /// Set initial/explicit row credits for a pathway.
    pub fn set_credits(&self, exchange_id: u64, src_shard: u32, target_shard: u32, amount: u32) {
        let key = (exchange_id, src_shard, target_shard);
        let mut channels = self.channels.lock();
        channels.insert(
            key,
            ChannelCreditState {
                available_rows: amount,
                max_rows: amount.max(1),
                rows_in_flight: 0,
                batches_in_flight: 0,
                bytes_in_flight: 0,
            },
        );
        Self::update_credit_metrics(&channels);
    }

    /// Acquire row credit for the specified channel, suspending the caller if none are available.
    pub async fn acquire_credit(
        &self,
        exchange_id: u64,
        src_shard: u32,
        target_shard: u32,
        row_count: u32,
    ) -> Result<(), String> {
        let row_count = row_count.max(1);
        let max_rows = self.row_budget();
        if row_count > max_rows {
            return Err(format!(
                "[{RS_3024}] shuffle frame carries {row_count} rows but worker.max_rows_per_quantum only permits {max_rows}. Next steps: reduce exchange batch size/rechunking or raise worker.max_rows_per_quantum."
            ));
        }

        let key = (exchange_id, src_shard, target_shard);
        let notify = {
            let mut notifiers = self.notifiers.lock();
            notifiers
                .entry(key)
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        loop {
            {
                let mut channels = self.channels.lock();
                let state = self.channel_state(&mut channels, key);
                if state.available_rows >= row_count {
                    state.available_rows -= row_count;
                    state.rows_in_flight = state.rows_in_flight.saturating_add(row_count);
                    Self::update_credit_metrics(&channels);
                    return Ok(());
                }
            }
            notify.notified().await;
        }
    }
}

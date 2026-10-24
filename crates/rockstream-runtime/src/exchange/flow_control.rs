use crate::exchange::proto::ShuffleAck;
use parking_lot::Mutex;
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::{RS_3006, RS_3024};
use rockstream_verified::ownership;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Notify;

pub const DEFAULT_MAX_INFLIGHT_BATCHES: usize = 64;
pub const DEFAULT_MAX_INFLIGHT_BYTES: usize = 64 * 1024 * 1024; // 64 MiB
pub const DEFAULT_MAX_BATCH_BYTES: usize = 16 * 1024 * 1024; // 16 MiB
pub const DEFAULT_MAX_PENDING_REQUESTS: usize = 256;

type CreditKey = (u64, u32, u32);

struct ChannelCreditState {
    owner: Arc<()>,
    had_delivery: bool,
    available_rows: u32,
    max_rows: u32,
    rows_in_flight: u32,
    batches_in_flight: usize,
    bytes_in_flight: usize,
    awaiting_ack: Vec<((u64, u64), (usize, u32))>,
}

impl ChannelCreditState {
    fn new(max_rows: u32) -> Self {
        Self {
            owner: Arc::new(()),
            had_delivery: false,
            available_rows: max_rows,
            max_rows,
            rows_in_flight: 0,
            batches_in_flight: 0,
            bytes_in_flight: 0,
            awaiting_ack: Vec::new(),
        }
    }
}

/// One lock protects both live reservations and retired channel keys.
/// A separate retired-key mutex added measurable overhead to the common
/// credit-acquire/ACK path while providing no extra synchronization guarantee.
#[derive(Default)]
struct ChannelRegistry {
    live: HashMap<CreditKey, ChannelCreditState>,
    retired: HashSet<CreditKey>,
}

impl std::ops::Deref for ChannelRegistry {
    type Target = HashMap<CreditKey, ChannelCreditState>;

    fn deref(&self) -> &Self::Target {
        &self.live
    }
}

impl std::ops::DerefMut for ChannelRegistry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.live
    }
}

/// A per-channel notifier exists only while an actual legacy row-credit
/// waiter owns it. This bounds the notifier registry across many ephemeral
/// channels instead of retaining a key forever after cancellation.
struct ChannelNotifier {
    notify: Arc<Notify>,
    waiters: usize,
}

/// Drop-owned subscription to a specific channel. A teardown may remove the
/// entry while this guard is alive; pointer identity protects a replacement
/// entry with the same channel key from a stale release.
struct ChannelWaiterSlot {
    _quota: CreditWaiterSlot,
    key: CreditKey,
    notifiers: Arc<Mutex<HashMap<CreditKey, ChannelNotifier>>>,
    notify: Arc<Notify>,
}

impl Drop for ChannelWaiterSlot {
    fn drop(&mut self) {
        let mut notifiers = self.notifiers.lock();
        if let Some(entry) = notifiers.get_mut(&self.key) {
            if Arc::ptr_eq(&entry.notify, &self.notify) {
                if entry.waiters <= 1 {
                    notifiers.remove(&self.key);
                } else {
                    entry.waiters -= 1;
                }
            }
        }
    }
}

/// A bounded queued exchange waiter, released on every future exit path,
/// including task abort and dropping a pending future.
struct CreditWaiterSlot {
    count: Arc<AtomicU64>,
}

impl Drop for CreditWaiterSlot {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// An RAII guard permit for exchange batch flow control.
/// Automatically releases backpressure permits on Drop or explicit `release()`.
pub struct FlowPermit {
    controller: FlowController,
    key: CreditKey,
    owner: Arc<()>,
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
    /// Transfer ownership to acknowledgement accounting before publishing the frame.
    pub(crate) fn handoff(mut self, epoch: u64, seq: u64) {
        let mut channels = self.controller.channels.lock();
        if let Some(state) = channels.get_mut(&self.key).filter(|state| {
            ownership::same_owner(
                Arc::as_ptr(&state.owner) as usize,
                Arc::as_ptr(&self.owner) as usize,
            )
        }) {
            state.had_delivery = true;
            state
                .awaiting_ack
                .push(((epoch, seq), (self.batch_bytes, self.row_count)));
            self.active = false;
        }
    }

    pub fn acquired_at(&self) -> Instant {
        self.acquired_at
    }

    pub fn queue_age(&self) -> std::time::Duration {
        self.acquired_at.elapsed()
    }

    pub fn release(mut self) {
        if self.active {
            self.active = false;
            self.controller.release_batch_permit(
                self.key,
                &self.owner,
                self.batch_bytes,
                self.row_count,
            );
        }
    }
}

impl Drop for FlowPermit {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.controller.release_batch_permit(
                self.key,
                &self.owner,
                self.batch_bytes,
                self.row_count,
            );
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
    // Delivered IDs remain retired until a wire-level generation can distinguish them.
    channels: Arc<Mutex<ChannelRegistry>>,
    notifiers: Arc<Mutex<HashMap<CreditKey, ChannelNotifier>>>,
    aggregate_bytes: Arc<AtomicU64>,
    pending_requests: Arc<AtomicU64>,
    /// Queued futures are distinct from in-flight delivered batch permits.
    queued_credit_waiters: Arc<AtomicU64>,
    global_notify: Arc<Notify>,
    #[cfg(test)]
    pub(super) wait_barriers: Option<Arc<(tokio::sync::Barrier, tokio::sync::Barrier)>>,
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
            channels: Arc::new(Mutex::new(ChannelRegistry::default())),
            notifiers: Arc::new(Mutex::new(HashMap::new())),
            aggregate_bytes: Arc::new(AtomicU64::new(0)),
            pending_requests: Arc::new(AtomicU64::new(0)),
            queued_credit_waiters: Arc::new(AtomicU64::new(0)),
            global_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            wait_barriers: None,
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
        let row_budget = row_budget.max(1);
        let mut channels = self.channels.lock();
        self.row_budget.store(row_budget, Ordering::Relaxed);
        for state in channels.values_mut() {
            state.max_rows = row_budget;
            state.available_rows = row_budget.saturating_sub(state.rows_in_flight);
        }
        Self::update_credit_metrics(&channels);
        drop(channels);
        self.global_notify.notify_waiters();
        for notifier in self.notifiers.lock().values() {
            notifier.notify.notify_waiters();
        }
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

    /// Queued exchange-credit futures (not in-flight delivered batch permits).
    /// This is a separate bounded class sharing the same configured ceiling.
    pub fn queued_credit_waiters(&self) -> usize {
        self.queued_credit_waiters.load(Ordering::Acquire) as usize
    }

    fn reserve_credit_waiter(&self) -> Result<CreditWaiterSlot, String> {
        loop {
            let current = self.queued_credit_waiters.load(Ordering::Acquire);
            if current >= self.max_pending_requests as u64 {
                return Err(format!(
                    "[{RS_3006}] RESOURCE_EXHAUSTED: queued exchange credit waiters exceed max_pending_requests ({}). Next steps: reduce concurrent shuffle demand or raise the configured limit.",
                    self.max_pending_requests
                ));
            }
            if self
                .queued_credit_waiters
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(CreditWaiterSlot {
                    count: Arc::clone(&self.queued_credit_waiters),
                });
            }
        }
    }

    /// Number of per-channel notifier keys currently owned by live waiters.
    pub fn queued_notifier_channels(&self) -> usize {
        self.notifiers.lock().len()
    }

    fn reserve_channel_waiter(&self, key: CreditKey) -> Result<ChannelWaiterSlot, String> {
        let quota = self.reserve_credit_waiter()?;
        let notify = {
            let mut notifiers = self.notifiers.lock();
            let channel = notifiers.entry(key).or_insert_with(|| ChannelNotifier {
                notify: Arc::new(Notify::new()),
                waiters: 0,
            });
            channel.waiters += 1;
            Arc::clone(&channel.notify)
        };
        Ok(ChannelWaiterSlot {
            _quota: quota,
            key,
            notifiers: Arc::clone(&self.notifiers),
            notify,
        })
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
        if batch_bytes > self.max_inflight_bytes {
            return Err(format!(
                "[{RS_3006}] exchange frame size ({batch_bytes} bytes) exceeds max_inflight_bytes ({}) limit. Next steps: split large record batches into smaller chunks within configured transport budget.",
                self.max_inflight_bytes
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
        if channels.retired.contains(&key) {
            return Err(format!(
                "[{RS_3006}] exchange channel was retired; use a new exchange ID"
            ));
        }

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
        let owner = state.owner.clone();

        let new_bytes = self
            .aggregate_bytes
            .fetch_add(batch_bytes as u64, Ordering::Relaxed)
            + batch_bytes as u64;
        let new_pending = self.pending_requests.fetch_add(1, Ordering::Relaxed) + 1;

        Self::update_batch_metrics(&channels, new_bytes, new_pending);

        Ok(FlowPermit {
            controller: self.clone(),
            key,
            owner,
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

        let mut waiter = None;
        loop {
            let notified = self.global_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
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
                    if waiter.is_none() {
                        waiter = Some(self.reserve_credit_waiter()?);
                    }
                    #[cfg(test)]
                    self.pause_before_wait().await;
                    notified.await;
                }
            }
        }
    }

    /// Internal release called by `FlowPermit` on drop/release.
    fn release_batch_permit(
        &self,
        key: CreditKey,
        owner: &Arc<()>,
        batch_bytes: usize,
        row_count: u32,
    ) {
        {
            let mut channels = self.channels.lock();
            let (matches, rows, batches, bytes) =
                channels.get(&key).map_or((false, 0, 0, 0), |state| {
                    (
                        ownership::same_owner(
                            Arc::as_ptr(&state.owner) as usize,
                            Arc::as_ptr(owner) as usize,
                        ),
                        state.rows_in_flight,
                        state.batches_in_flight,
                        state.bytes_in_flight,
                    )
                });
            let (next_rows, next_batches, next_channel_bytes, new_bytes, new_pending) =
                ownership::release_accounting(
                    matches,
                    rows,
                    batches,
                    bytes,
                    self.aggregate_bytes.load(Ordering::Relaxed),
                    self.pending_requests.load(Ordering::Relaxed),
                    row_count,
                    batch_bytes,
                );
            if matches {
                let state = channels.get_mut(&key).expect("matching channel exists");
                state.rows_in_flight = next_rows;
                state.batches_in_flight = next_batches;
                state.bytes_in_flight = next_channel_bytes;
                state.available_rows = state.max_rows.saturating_sub(state.rows_in_flight);
            }
            self.aggregate_bytes.store(new_bytes, Ordering::Relaxed);
            self.pending_requests.store(new_pending, Ordering::Relaxed);

            Self::update_batch_metrics(&channels, new_bytes, new_pending);
        }

        self.notify_waiters(key);
    }

    fn notify_waiters(&self, key: CreditKey) {
        self.global_notify.notify_waiters();
        let notifiers = self.notifiers.lock();
        if let Some(channel) = notifiers.get(&key) {
            channel.notify.notify_waiters();
        }
    }

    /// Purge all channel credit state and notifier entries for the given exchange.
    pub fn teardown_exchange(&self, exchange_id: u64) {
        {
            let mut channels = self.channels.lock();
            let mut retired = Vec::new();
            channels.retain(|key, state| {
                if key.0 != exchange_id {
                    return true;
                }
                if state.had_delivery {
                    retired.push(*key);
                }
                let bytes = state
                    .awaiting_ack
                    .iter()
                    .map(|(_, (bytes, _))| *bytes as u64)
                    .sum::<u64>();
                self.aggregate_bytes.fetch_sub(bytes, Ordering::Relaxed);
                self.pending_requests
                    .fetch_sub(state.awaiting_ack.len() as u64, Ordering::Relaxed);
                false
            });
            channels.retired.extend(retired);
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
                if let Some(channel) = notifiers.remove(&k) {
                    list.push(channel.notify);
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

    /// Release the matching delivered reservation, or legacy row-only credits.
    pub fn handle_ack(&self, ack: &ShuffleAck) {
        let key = (ack.exchange_id, ack.src_shard, ack.target_shard);
        let reservation = {
            let mut channels = self.channels.lock();
            if channels.retired.contains(&key) {
                return;
            }
            let state = self.channel_state(&mut channels, key);
            if let Some(index) = state
                .awaiting_ack
                .iter()
                .position(|(identity, _)| *identity == (ack.epoch, ack.seq))
            {
                Some((state.owner.clone(), state.awaiting_ack.remove(index).1))
            } else {
                // Keep legacy credit release atomic with admission; stale ACKs cannot free a new batch.
                if state.batches_in_flight == 0 {
                    state.rows_in_flight -= ack.credit_grant.min(state.rows_in_flight);
                    state.available_rows = state.max_rows.saturating_sub(state.rows_in_flight);
                }
                Self::update_credit_metrics(&channels);
                None
            }
        };
        if let Some((owner, (batch_bytes, row_count))) = reservation {
            self.release_batch_permit(key, &owner, batch_bytes, row_count);
        } else {
            self.notify_waiters(key);
        }
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
            if channels.retired.contains(&key) {
                return;
            }
            let state = self.channel_state(&mut channels, key);
            let released = row_count.min(state.rows_in_flight);
            state.rows_in_flight -= released;
            state.available_rows = state.max_rows.saturating_sub(state.rows_in_flight);
            Self::update_credit_metrics(&channels);
        }

        self.notify_waiters(key);
    }

    /// Check current available row credits without blocking or consuming.
    pub fn get_credits(&self, exchange_id: u64, src_shard: u32, target_shard: u32) -> u32 {
        let key = (exchange_id, src_shard, target_shard);
        let channels = self.channels.lock();
        channels
            .get(&key)
            .map(|state| state.available_rows)
            .unwrap_or_else(|| self.row_budget())
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
        if channels.retired.contains(&key) {
            return;
        }
        if let Some(state) = channels.get_mut(&key) {
            // Replacing a live channel would detach its permit and ACK charges.
            if state.rows_in_flight != 0
                || state.batches_in_flight != 0
                || !state.awaiting_ack.is_empty()
            {
                return;
            }
            state.available_rows = amount;
            state.max_rows = amount.max(1);
        } else {
            channels.insert(
                key,
                ChannelCreditState {
                    owner: Arc::new(()),
                    had_delivery: false,
                    available_rows: amount,
                    max_rows: amount.max(1),
                    rows_in_flight: 0,
                    batches_in_flight: 0,
                    bytes_in_flight: 0,
                    awaiting_ack: Vec::new(),
                },
            );
        }
        Self::update_credit_metrics(&channels);
    }

    /// Admit row credits under the channel lock without touching notifier
    /// state. The common uncontended path needs no waiter allocation or
    /// notifier-map lock; rejected attempts never consume partial credits.
    fn try_acquire_rows(&self, key: CreditKey, row_count: u32) -> Result<bool, String> {
        let mut channels = self.channels.lock();
        if channels.retired.contains(&key) {
            return Err(format!(
                "[{RS_3006}] exchange channel was retired; use a new exchange ID"
            ));
        }
        let state = self.channel_state(&mut channels, key);
        if state.available_rows < row_count {
            return Ok(false);
        }
        state.available_rows -= row_count;
        state.rows_in_flight = state.rows_in_flight.saturating_add(row_count);
        state.had_delivery = true;
        Self::update_credit_metrics(&channels);
        Ok(true)
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
        if self.try_acquire_rows(key, row_count)? {
            return Ok(());
        }
        // Reservation and keyed notifier have the same drop-owned lifetime.
        // An abort frees both the waiter slot and the channel key.
        let waiter = self.reserve_channel_waiter(key)?;
        let notify = Arc::clone(&waiter.notify);

        // Register the future *before* rechecking channels to avoid a lost
        // wake when ACK races against waiter registration or cancellation.
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.try_acquire_rows(key, row_count)? {
                return Ok(());
            }
            #[cfg(test)]
            self.pause_before_wait().await;
            notified.await;
        }
    }

    #[cfg(test)]
    async fn pause_before_wait(&self) {
        if let Some((checked, resume)) = self.wait_barriers.as_deref() {
            checked.wait().await;
            resume.wait().await;
        }
    }
}

#[cfg(test)]
mod v072_credit_fastpath_tests {
    use super::*;
    use std::time::Duration;

    fn ack() -> ShuffleAck {
        ShuffleAck {
            exchange_id: 7,
            src_shard: 1,
            target_shard: 2,
            epoch: 0,
            seq: 0,
            credit_grant: 1,
        }
    }

    #[tokio::test]
    async fn v072_immediate_credits_never_allocate_waiter_state() {
        let controller = FlowController::with_row_budget(2);
        assert_eq!(controller.notifiers.lock().len(), 0);
        controller.acquire_credit(7, 1, 2, 1).await.unwrap();
        assert_eq!(controller.notifiers.lock().len(), 0);
        assert_eq!(controller.get_credits(7, 1, 2), 1);
        assert_eq!(controller.rows_in_flight(7, 1, 2), 1);
        controller.acquire_credit(7, 1, 2, 1).await.unwrap();
        assert_eq!(controller.notifiers.lock().len(), 0);
        assert_eq!(controller.get_credits(7, 1, 2), 0);
        assert_eq!(controller.rows_in_flight(7, 1, 2), 2);
        controller.handle_ack(&ack());
        assert_eq!(controller.get_credits(7, 1, 2), 1);
        assert_eq!(controller.rows_in_flight(7, 1, 2), 1);
        controller.release_credit(7, 1, 2, 1);
        assert_eq!(controller.get_credits(7, 1, 2), 2);
        assert_eq!(controller.rows_in_flight(7, 1, 2), 0);
        assert_eq!(controller.notifiers.lock().len(), 0);
    }

    #[tokio::test]
    async fn v072_waiter_notifier_creation_does_not_lose_racing_ack() {
        let controller = FlowController::with_row_budget(1);
        controller.acquire_credit(7, 1, 2, 1).await.unwrap();
        let waiting = controller.clone();
        let task = tokio::spawn(async move { waiting.acquire_credit(7, 1, 2, 1).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while controller.notifiers.lock().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        controller.handle_ack(&ack());
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(controller.get_credits(7, 1, 2), 0);
        assert_eq!(controller.rows_in_flight(7, 1, 2), 1);
        controller.handle_ack(&ack());
        assert_eq!(controller.get_credits(7, 1, 2), 1);
        assert_eq!(controller.rows_in_flight(7, 1, 2), 0);
    }

    #[tokio::test]
    async fn v072_oversized_request_rejected_without_allocating_any_state() {
        let controller = FlowController::with_row_budget(1);
        let error = controller.acquire_credit(7, 1, 2, 2).await.unwrap_err();
        assert_eq!(
            error,
            format!(
                "[{RS_3024}] shuffle frame carries 2 rows but worker.max_rows_per_quantum only permits 1. Next steps: reduce exchange batch size/rechunking or raise worker.max_rows_per_quantum."
            )
        );
        assert_eq!(controller.notifiers.lock().len(), 0);
        assert_eq!(controller.channels.lock().len(), 0);
        assert_eq!(controller.get_credits(7, 1, 2), 1);
        assert_eq!(controller.rows_in_flight(7, 1, 2), 0);
    }

    #[test]
    fn old_permit_cannot_release_recreated_channel() {
        let controller = FlowController::with_row_budget(1)
            .with_max_inflight_batches(1)
            .with_max_inflight_bytes(8);
        let snapshot = || {
            let channels = controller.channels.lock();
            (
                channels.get(&(7, 1, 2)).map(|state| {
                    (
                        state.available_rows,
                        state.rows_in_flight,
                        state.batches_in_flight,
                        state.bytes_in_flight,
                        state.awaiting_ack.clone(),
                    )
                }),
                controller.bytes_in_flight(),
                controller.pending_requests(),
            )
        };
        let mut transcript = vec![("initial", snapshot())];
        let old = controller.try_acquire_batch_permit(7, 1, 2, 2, 1).unwrap();
        transcript.push(("old acquired", snapshot()));
        controller.teardown_exchange(7);
        transcript.push(("torn down", snapshot()));
        let new = controller.try_acquire_batch_permit(7, 1, 2, 3, 1).unwrap();
        transcript.push(("new acquired", snapshot()));
        controller.set_credits(7, 1, 2, 5);
        transcript.push(("live reset refused", snapshot()));
        drop(old);
        transcript.push(("old dropped", snapshot()));
        assert!(controller.try_acquire_batch_permit(7, 1, 2, 1, 1).is_err());
        transcript.push(("new bound enforced", snapshot()));
        drop(new);
        transcript.push(("new dropped", snapshot()));
        assert_eq!(
            transcript,
            vec![
                ("initial", (None, 0, 0)),
                ("old acquired", (Some((0, 1, 1, 2, vec![])), 2, 1)),
                ("torn down", (None, 2, 1)),
                ("new acquired", (Some((0, 1, 1, 3, vec![])), 5, 2)),
                ("live reset refused", (Some((0, 1, 1, 3, vec![])), 5, 2)),
                ("old dropped", (Some((0, 1, 1, 3, vec![])), 3, 1)),
                ("new bound enforced", (Some((0, 1, 1, 3, vec![])), 3, 1)),
                ("new dropped", (Some((1, 0, 0, 0, vec![])), 0, 0)),
            ]
        );
    }

    #[test]
    fn lowering_live_row_budget_preserves_charges() {
        let controller = FlowController::with_row_budget(3);
        let first = controller.try_acquire_batch_permit(7, 1, 2, 2, 1).unwrap();
        let second = controller.try_acquire_batch_permit(7, 1, 2, 3, 1).unwrap();
        let snapshot = || {
            (
                controller.get_credits(7, 1, 2),
                controller.rows_in_flight(7, 1, 2),
                controller.batches_in_flight(7, 1, 2),
                controller.bytes_in_flight(),
                controller.pending_requests(),
            )
        };
        let mut transcript = vec![("two live", snapshot())];
        controller.set_row_budget(1);
        transcript.push(("lowered", snapshot()));
        assert!(controller.try_acquire_batch_permit(7, 1, 2, 1, 1).is_err());
        drop(first);
        transcript.push(("one live", snapshot()));
        assert!(controller.try_acquire_batch_permit(7, 1, 2, 1, 1).is_err());
        drop(second);
        transcript.push(("released", snapshot()));
        let third = controller.try_acquire_batch_permit(7, 1, 2, 1, 1).unwrap();
        transcript.push(("new live", snapshot()));
        drop(third);
        transcript.push(("idle", snapshot()));
        assert_eq!(
            transcript,
            vec![
                ("two live", (1, 2, 2, 5, 2)),
                ("lowered", (0, 2, 2, 5, 2)),
                ("one live", (0, 1, 1, 3, 1)),
                ("released", (1, 0, 0, 0, 0)),
                ("new live", (0, 1, 1, 1, 1)),
                ("idle", (1, 0, 0, 0, 0)),
            ]
        );
    }

    #[test]
    fn delivered_cancelled_and_torn_down_reservations_keep_exact_charges() {
        let controller = FlowController::with_row_budget(2);
        let snapshot = || {
            (
                controller.get_credits(7, 1, 2),
                controller.rows_in_flight(7, 1, 2),
                controller.batches_in_flight(7, 1, 2),
                controller.bytes_in_flight(),
                controller.pending_requests(),
            )
        };
        let ack = |seq| ShuffleAck {
            exchange_id: 7,
            src_shard: 1,
            target_shard: 2,
            epoch: 1,
            seq,
            credit_grant: 1,
        };
        let delivered = controller.try_acquire_batch_permit(7, 1, 2, 2, 1).unwrap();
        let cancelled = controller.try_acquire_batch_permit(7, 1, 2, 3, 1).unwrap();
        let mut transcript = vec![("acquired", snapshot())];
        delivered.handoff(1, 1);
        transcript.push(("handed off", snapshot()));
        drop(cancelled);
        transcript.push(("cancelled", snapshot()));
        controller.handle_ack(&ack(1));
        transcript.push(("acknowledged", snapshot()));
        let later = controller.try_acquire_batch_permit(7, 1, 2, 4, 1).unwrap();
        later.handoff(1, 2);
        transcript.push(("later delivered", snapshot()));
        controller.handle_ack(&ack(1));
        transcript.push(("duplicate ack", snapshot()));
        controller.teardown_exchange(7);
        transcript.push(("torn down", snapshot()));
        controller.handle_ack(&ack(2));
        transcript.push(("late ack", snapshot()));
        assert_eq!(
            controller
                .try_acquire_batch_permit(7, 1, 2, 1, 1)
                .unwrap_err(),
            format!("[{RS_3006}] exchange channel was retired; use a new exchange ID")
        );
        controller.set_credits(7, 1, 2, 2);
        assert!(!controller.channels.lock().contains_key(&(7, 1, 2)));
        assert_eq!(
            transcript,
            vec![
                ("acquired", (0, 2, 2, 5, 2)),
                ("handed off", (0, 2, 2, 5, 2)),
                ("cancelled", (1, 1, 1, 2, 1)),
                ("acknowledged", (2, 0, 0, 0, 0)),
                ("later delivered", (1, 1, 1, 4, 1)),
                ("duplicate ack", (1, 1, 1, 4, 1)),
                ("torn down", (2, 0, 0, 0, 0)),
                ("late ack", (2, 0, 0, 0, 0)),
            ]
        );
    }
}

use crate::exchange::proto::ShuffleAck;
use parking_lot::Mutex;
use rockstream_types::config::RockstreamConfig;
use rockstream_types::error_code::RS_3024;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

type CreditKey = (u64, u32, u32);

#[derive(Clone, Copy)]
struct ChannelCreditState {
    available_rows: u32,
    max_rows: u32,
    rows_in_flight: u32,
}

impl ChannelCreditState {
    fn new(max_rows: u32) -> Self {
        Self {
            available_rows: max_rows,
            max_rows,
            rows_in_flight: 0,
        }
    }
}

/// Manages row-budget-based flow control for outbox exchange channels.
#[derive(Clone)]
pub struct FlowController {
    row_budget: Arc<AtomicU32>,
    channels: Arc<Mutex<HashMap<CreditKey, ChannelCreditState>>>,
    notifiers: Arc<Mutex<HashMap<CreditKey, Arc<Notify>>>>,
}

impl Default for FlowController {
    fn default() -> Self {
        Self::with_row_budget(RockstreamConfig::default().worker.max_rows_per_quantum as u32)
    }
}

impl FlowController {
    /// Create a new flow controller using the default worker row budget.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new flow controller with an explicit per-channel row budget.
    pub fn with_row_budget(row_budget: u32) -> Self {
        Self {
            row_budget: Arc::new(AtomicU32::new(row_budget.max(1))),
            channels: Arc::new(Mutex::new(HashMap::new())),
            notifiers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set_row_budget(&self, row_budget: u32) {
        self.row_budget.store(row_budget.max(1), Ordering::Relaxed);
    }

    fn row_budget(&self) -> u32 {
        self.row_budget.load(Ordering::Relaxed).max(1)
    }

    fn update_metric(channels: &HashMap<CreditKey, ChannelCreditState>) {
        let rows_in_flight = channels
            .values()
            .map(|state| state.rows_in_flight as u64)
            .sum::<u64>();
        rockstream_types::metrics::set_shuffle_rows_in_flight(rows_in_flight);
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
            Self::update_metric(&channels);
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
            },
        );
        Self::update_metric(&channels);
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
                    Self::update_metric(&channels);
                    return Ok(());
                }
            }
            notify.notified().await;
        }
    }
}

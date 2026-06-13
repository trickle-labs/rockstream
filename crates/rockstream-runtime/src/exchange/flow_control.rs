use crate::exchange::proto::ShuffleAck;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;

type CreditKey = (u64, u32, u32);

/// Manages credit-based flow control for outbox exchange channels.
#[derive(Clone, Default)]
pub struct FlowController {
    // Map: (exchange_id, src_shard, target_shard) -> available credits
    credits: Arc<Mutex<HashMap<CreditKey, u32>>>,
    // Map: (exchange_id, src_shard, target_shard) -> notifier for suspended senders
    notifiers: Arc<Mutex<HashMap<CreditKey, Arc<Notify>>>>,
}

impl FlowController {
    /// Create a new flow controller.
    pub fn new() -> Self {
        FlowController::default()
    }

    /// Process a received ShuffleAck and restore credits for the corresponding channel.
    pub fn handle_ack(&self, ack: &ShuffleAck) {
        let key = (ack.exchange_id, ack.src_shard, ack.target_shard);
        let mut credits = self.credits.lock();
        let val = credits.entry(key).or_insert(16); // Initial capacity = 16 (OPERATOR_CHANNEL_CAPACITY)
        *val = val.saturating_add(ack.credit_grant);

        let notifiers = self.notifiers.lock();
        if let Some(notify) = notifiers.get(&key) {
            notify.notify_one();
        }
    }

    /// Check current available credits without blocking or consuming.
    pub fn get_credits(&self, exchange_id: u64, src_shard: u32, target_shard: u32) -> u32 {
        let key = (exchange_id, src_shard, target_shard);
        let credits = self.credits.lock();
        *credits.get(&key).unwrap_or(&16)
    }

    /// Set initial/explicit credits for a pathway.
    pub fn set_credits(&self, exchange_id: u64, src_shard: u32, target_shard: u32, amount: u32) {
        let key = (exchange_id, src_shard, target_shard);
        let mut credits = self.credits.lock();
        credits.insert(key, amount);
    }

    /// Acquire 1 credit for the specified channel, suspending the caller if none are available.
    pub async fn acquire_credit(&self, exchange_id: u64, src_shard: u32, target_shard: u32) {
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
                let mut credits = self.credits.lock();
                let val = credits.entry(key).or_insert(16);
                if *val > 0 {
                    *val -= 1;
                    return;
                }
            }
            notify.notified().await;
        }
    }
}

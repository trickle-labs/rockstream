//! In-process LISTEN/NOTIFY channel registry.
//!
//! Named upper bounds:
//!   `MAX_NOTIFY_CHANNELS = 1_000` — distinct channel names across the gateway.
//!   `MAX_OUTBOX_PER_CONNECTION = 10_000` — pending notifications per connection.
//!
//! Fill-level metrics:
//!   `subscriptions.len()` — current channel count.
//!   `outboxes[conn_id].len()` — pending notifications for a connection.

use std::collections::HashSet;

use dashmap::DashMap;

use crate::error::GatewayError;

/// Max distinct notify channel names registered per gateway.
pub const MAX_NOTIFY_CHANNELS: usize = 1_000;
/// Max pending notifications per connection outbox.
pub const MAX_OUTBOX_PER_CONNECTION: usize = 10_000;

/// In-memory LISTEN/NOTIFY channel registry.
///
/// Thread-safe via `DashMap`. All methods are lock-free at the connection level.
#[derive(Default)]
pub struct NotifyRegistry {
    /// channel_name → set of conn_ids currently LISTENing.
    subscriptions: DashMap<String, HashSet<String>>,
    /// conn_id → pending notifications not yet drained to the client.
    outboxes: DashMap<String, Vec<(String, String, i32)>>,
}

impl NotifyRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe `conn_id` to `channel`.
    ///
    /// Returns `NotifyChannelLimitExceeded` when a new channel would exceed
    /// `MAX_NOTIFY_CHANNELS`.
    pub fn subscribe(&self, channel: &str, conn_id: &str) -> Result<(), GatewayError> {
        if !self.subscriptions.contains_key(channel)
            && self.subscriptions.len() >= MAX_NOTIFY_CHANNELS
        {
            return Err(GatewayError::NotifyChannelLimitExceeded {
                limit: MAX_NOTIFY_CHANNELS,
            });
        }
        self.subscriptions
            .entry(channel.to_string())
            .or_default()
            .insert(conn_id.to_string());
        // Ensure outbox entry exists.
        self.outboxes.entry(conn_id.to_string()).or_default();
        Ok(())
    }

    /// Unsubscribe `conn_id` from `channel`.
    ///
    /// No-op if `conn_id` was not subscribed (Postgres semantics).
    pub fn unsubscribe(&self, channel: &str, conn_id: &str) {
        if let Some(mut set) = self.subscriptions.get_mut(channel) {
            set.remove(conn_id);
            if set.is_empty() {
                drop(set);
                self.subscriptions.remove(channel);
            }
        }
    }

    /// Remove `conn_id` from every channel and clear its outbox.
    pub fn unsubscribe_all(&self, conn_id: &str) {
        let mut empty_channels: Vec<String> = Vec::new();
        for mut entry in self.subscriptions.iter_mut() {
            entry.value_mut().remove(conn_id);
            if entry.value().is_empty() {
                empty_channels.push(entry.key().clone());
            }
        }
        for ch in empty_channels {
            self.subscriptions.remove_if(&ch, |_, v| v.is_empty());
        }
        self.outboxes.remove(conn_id);
    }

    /// Deliver `(channel, payload, sender_pid)` to all subscribers.
    ///
    /// Excess notifications beyond `MAX_OUTBOX_PER_CONNECTION` are silently dropped.
    /// Returns the number of connections that received the notification.
    pub fn deliver(&self, channel: &str, payload: &str, sender_pid: i32) -> usize {
        let conn_ids: Vec<String> = self
            .subscriptions
            .get(channel)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        let mut delivered = 0;
        for conn_id in &conn_ids {
            let mut outbox = self.outboxes.entry(conn_id.clone()).or_default();
            if outbox.len() < MAX_OUTBOX_PER_CONNECTION {
                outbox.push((channel.to_string(), payload.to_string(), sender_pid));
                delivered += 1;
            }
        }
        delivered
    }

    /// Drain all pending notifications for `conn_id`.
    pub fn drain_outbox(&self, conn_id: &str) -> Vec<(String, String, i32)> {
        match self.outboxes.get_mut(conn_id) {
            Some(mut v) => std::mem::take(v.value_mut()),
            None => Vec::new(),
        }
    }

    /// Clear all channel subscriptions and outboxes.
    pub fn clear(&self) {
        self.subscriptions.clear();
        self.outboxes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_deliver_drain() {
        let reg = NotifyRegistry::new();
        reg.subscribe("chan", "conn_a").unwrap();
        reg.deliver("chan", "hi", 42);
        let drained = reg.drain_outbox("conn_a");
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, "chan");
        assert_eq!(drained[0].1, "hi");
        assert_eq!(drained[0].2, 42);
    }

    #[test]
    fn test_unsubscribe_stops_delivery() {
        let reg = NotifyRegistry::new();
        reg.subscribe("chan", "conn_a").unwrap();
        reg.unsubscribe("chan", "conn_a");
        reg.deliver("chan", "msg", 1);
        let drained = reg.drain_outbox("conn_a");
        assert!(
            drained.is_empty(),
            "expected no notifications after unsubscribe"
        );
    }

    #[test]
    fn test_unsubscribe_all_clears_all_channels() {
        let reg = NotifyRegistry::new();
        reg.subscribe("c1", "conn_a").unwrap();
        reg.subscribe("c2", "conn_a").unwrap();
        reg.unsubscribe_all("conn_a");
        reg.deliver("c1", "x", 1);
        reg.deliver("c2", "y", 1);
        let drained = reg.drain_outbox("conn_a");
        assert!(
            drained.is_empty(),
            "expected no notifications after unsubscribe_all"
        );
    }

    #[test]
    fn test_outbox_limit_no_panic() {
        let reg = NotifyRegistry::new();
        reg.subscribe("chan", "conn_a").unwrap();
        for i in 0..MAX_OUTBOX_PER_CONNECTION + 10 {
            reg.deliver("chan", &format!("msg_{i}"), 1);
        }
        let drained = reg.drain_outbox("conn_a");
        assert_eq!(
            drained.len(),
            MAX_OUTBOX_PER_CONNECTION,
            "outbox must not exceed MAX_OUTBOX_PER_CONNECTION"
        );
    }
}

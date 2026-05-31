//! Connection pooling for the RockStream Postgres gateway (v0.40).
//!
//! Implements a fixed-capacity connection pool that limits the number of
//! concurrent client connections.  Excess connection attempts receive
//! `GatewayError::PoolExhausted` immediately (fail-fast semantics).
//!
//! # Design
//!
//! The pool maintains a set of available `ConnectionId` slots and a set of
//! in-use slots.  `acquire` moves a slot from available → in-use; `release`
//! moves it back.  The pool size is bounded by `ConnectionPoolConfig::max_connections`.
//!
//! In production the pool would manage real TCP sockets or async task handles.
//! The types here model the logical protocol to prove the invariants.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::error::GatewayError;

// ─── Configuration ────────────────────────────────────────────────────────────

/// Configuration for the gateway connection pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionPoolConfig {
    /// Maximum number of concurrent client connections.
    pub max_connections: usize,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 64,
        }
    }
}

// ─── Connection handle ────────────────────────────────────────────────────────

/// An opaque handle for a pooled connection slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(pub u64);

/// A connection acquired from the pool.
///
/// Must be returned via [`ConnectionPool::release`] when the query completes.
#[derive(Debug)]
pub struct PooledConnection {
    /// Unique ID for this connection slot.
    pub id: ConnectionId,
    /// Monotonically increasing sequence number of the acquisition (for audit).
    pub acquisition_seq: u64,
}

// ─── ConnectionPool ───────────────────────────────────────────────────────────

/// Fixed-capacity connection pool for the gateway.
///
/// # Invariants
///
/// - `available.len() + in_use.len() == max_connections` at all times.
/// - `acquire` fails immediately when `available` is empty.
/// - `release` panics in debug mode if `id` was not in-use.
#[derive(Debug)]
pub struct ConnectionPool {
    /// Pool configuration.
    pub config: ConnectionPoolConfig,
    /// Available (idle) connection slots.
    available: Vec<ConnectionId>,
    /// In-use connection slots.
    in_use: HashSet<ConnectionId>,
    /// Monotonically increasing acquisition sequence counter.
    next_seq: u64,
}

impl ConnectionPool {
    /// Create a new connection pool with the given configuration.
    ///
    /// Pre-allocates all `max_connections` slots in the available list.
    pub fn new(config: ConnectionPoolConfig) -> Self {
        let max = config.max_connections;
        let available = (0..max as u64).map(ConnectionId).collect();
        Self {
            config,
            available,
            in_use: HashSet::new(),
            next_seq: 0,
        }
    }

    /// Acquire a connection slot from the pool.
    ///
    /// Returns `Err(GatewayError::PoolExhausted)` immediately if no slots are
    /// available (non-blocking fail-fast semantics).
    pub fn acquire(&mut self) -> Result<PooledConnection, GatewayError> {
        let id = self
            .available
            .pop()
            .ok_or(GatewayError::PoolExhausted(self.config.max_connections))?;
        self.in_use.insert(id);
        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(PooledConnection {
            id,
            acquisition_seq: seq,
        })
    }

    /// Release a previously acquired connection slot back to the pool.
    ///
    /// # Panics
    ///
    /// Panics in debug mode if `id` was not in the in-use set (double-release
    /// is a programming error).
    pub fn release(&mut self, id: ConnectionId) {
        // Non-debug path: gracefully ignore unknown IDs (double-release is
        // a programming error but we don't panic in release builds).
        debug_assert!(
            self.in_use.contains(&id),
            "ConnectionPool::release called for a connection not in use: {id:?}"
        );
        self.in_use.remove(&id);
        self.available.push(id);
    }

    /// Number of slots currently in use.
    pub fn in_use_count(&self) -> usize {
        self.in_use.len()
    }

    /// Number of slots currently available.
    pub fn available_count(&self) -> usize {
        self.available.len()
    }

    /// `true` if all slots are currently in use.
    pub fn is_exhausted(&self) -> bool {
        self.available.is_empty()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn small_pool(max: usize) -> ConnectionPool {
        ConnectionPool::new(ConnectionPoolConfig {
            max_connections: max,
        })
    }

    #[test]
    fn pool_starts_fully_available() {
        let pool = small_pool(4);
        assert_eq!(pool.available_count(), 4);
        assert_eq!(pool.in_use_count(), 0);
        assert!(!pool.is_exhausted());
    }

    #[test]
    fn acquire_moves_slot_to_in_use() {
        let mut pool = small_pool(4);
        let conn = pool.acquire().unwrap();
        assert_eq!(pool.in_use_count(), 1);
        assert_eq!(pool.available_count(), 3);
        pool.release(conn.id);
    }

    #[test]
    fn release_returns_slot_to_available() {
        let mut pool = small_pool(2);
        let c1 = pool.acquire().unwrap();
        let c2 = pool.acquire().unwrap();
        pool.release(c1.id);
        assert_eq!(pool.available_count(), 1);
        pool.release(c2.id);
        assert_eq!(pool.available_count(), 2);
        assert_eq!(pool.in_use_count(), 0);
    }

    #[test]
    fn pool_exhausted_when_all_slots_in_use() {
        let mut pool = small_pool(2);
        let _c1 = pool.acquire().unwrap();
        let _c2 = pool.acquire().unwrap();
        assert!(pool.is_exhausted());
        let err = pool.acquire().unwrap_err();
        assert!(matches!(err, GatewayError::PoolExhausted(2)));
    }

    #[test]
    fn acquisition_seq_increments_monotonically() {
        let mut pool = small_pool(4);
        let c1 = pool.acquire().unwrap();
        let c2 = pool.acquire().unwrap();
        assert!(c2.acquisition_seq > c1.acquisition_seq);
        pool.release(c1.id);
        pool.release(c2.id);
    }

    #[test]
    fn default_config_has_64_max_connections() {
        let pool = ConnectionPool::new(ConnectionPoolConfig::default());
        assert_eq!(pool.available_count(), 64);
    }
}

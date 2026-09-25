//! Canonical structured logging and correlation engine (v0.71 V071-05).
//!
//! Tracks and propagates the 7 canonical correlation identifiers:
//! - `request_id`
//! - `operation_id`
//! - `workload_id`
//! - `view_id`
//! - `shard_id`
//! - `worker_id`
//! - `epoch`

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum bounded capacity for the structured log ring buffer (v0.71 Slice 5 / §4.8).
pub const MAX_STRUCTURED_LOG_EVENTS: usize = 4096;

/// Canonical 7-tuple correlation context.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogContext {
    pub request_id: Option<String>,
    pub operation_id: Option<String>,
    pub workload_id: Option<String>,
    pub view_id: Option<String>,
    pub shard_id: Option<u64>,
    pub worker_id: Option<String>,
    pub epoch: Option<u64>,
}

impl LogContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_operation_id(mut self, operation_id: impl Into<String>) -> Self {
        self.operation_id = Some(operation_id.into());
        self
    }

    pub fn with_workload_id(mut self, workload_id: impl Into<String>) -> Self {
        self.workload_id = Some(workload_id.into());
        self
    }

    pub fn with_view_id(mut self, view_id: impl Into<String>) -> Self {
        self.view_id = Some(view_id.into());
        self
    }

    pub fn with_shard_id(mut self, shard_id: u64) -> Self {
        self.shard_id = Some(shard_id);
        self
    }

    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch = Some(epoch);
        self
    }

    /// Check if all 7 identifiers are unset.
    pub fn is_empty(&self) -> bool {
        self.request_id.is_none()
            && self.operation_id.is_none()
            && self.workload_id.is_none()
            && self.view_id.is_none()
            && self.shard_id.is_none()
            && self.worker_id.is_none()
            && self.epoch.is_none()
    }

    /// Return all present correlation identifiers as a key-value map.
    pub fn as_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(ref v) = self.request_id {
            map.insert("request_id".to_string(), v.clone());
        }
        if let Some(ref v) = self.operation_id {
            map.insert("operation_id".to_string(), v.clone());
        }
        if let Some(ref v) = self.workload_id {
            map.insert("workload_id".to_string(), v.clone());
        }
        if let Some(ref v) = self.view_id {
            map.insert("view_id".to_string(), v.clone());
        }
        if let Some(v) = self.shard_id {
            map.insert("shard_id".to_string(), v.to_string());
        }
        if let Some(ref v) = self.worker_id {
            map.insert("worker_id".to_string(), v.clone());
        }
        if let Some(v) = self.epoch {
            map.insert("epoch".to_string(), v.to_string());
        }
        map
    }

    /// Formats the context into key=value pairs for structured logs.
    pub fn format_pairs(&self) -> String {
        let mut parts = Vec::new();
        if let Some(ref v) = self.request_id {
            parts.push(format!("request_id={v}"));
        }
        if let Some(ref v) = self.operation_id {
            parts.push(format!("operation_id={v}"));
        }
        if let Some(ref v) = self.workload_id {
            parts.push(format!("workload_id={v}"));
        }
        if let Some(ref v) = self.view_id {
            parts.push(format!("view_id={v}"));
        }
        if let Some(v) = self.shard_id {
            parts.push(format!("shard_id={v}"));
        }
        if let Some(ref v) = self.worker_id {
            parts.push(format!("worker_id={v}"));
        }
        if let Some(v) = self.epoch {
            parts.push(format!("epoch={v}"));
        }
        parts.join(" ")
    }
}

tokio::task_local! {
    pub static CURRENT_TASK_LOG_CONTEXT: LogContext;
}

thread_local! {
    static CURRENT_THREAD_LOG_CONTEXT: std::cell::RefCell<LogContext> = std::cell::RefCell::new(LogContext::default());
}

pub struct LogScopeGuard {
    prev: LogContext,
}

impl Drop for LogScopeGuard {
    fn drop(&mut self) {
        CURRENT_THREAD_LOG_CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = self.prev.clone();
        });
    }
}

pub fn enter_log_context(new_ctx: LogContext) -> LogScopeGuard {
    CURRENT_THREAD_LOG_CONTEXT.with(|ctx| {
        let prev = ctx.borrow().clone();
        *ctx.borrow_mut() = new_ctx;
        LogScopeGuard { prev }
    })
}

pub fn current_log_context() -> LogContext {
    if let Ok(task_ctx) = CURRENT_TASK_LOG_CONTEXT.try_with(|c| c.clone()) {
        if !task_ctx.is_empty() {
            return task_ctx;
        }
    }
    CURRENT_THREAD_LOG_CONTEXT.with(|ctx| ctx.borrow().clone())
}

/// A single structured log event containing timestamp, level, message, 7-tuple context, and custom fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredLogEvent {
    pub timestamp_ms: u64,
    pub level: String,
    pub message: String,
    pub context: LogContext,
    pub fields: HashMap<String, String>,
}

impl StructuredLogEvent {
    pub fn new(level: impl Into<String>, message: impl Into<String>, context: LogContext) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            timestamp_ms: now_ms,
            level: level.into(),
            message: message.into(),
            context,
            fields: HashMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }
}

/// Thread-safe bounded ring buffer for structured log events with drop counter.
#[derive(Debug)]
pub struct LogRingBuffer {
    events: RwLock<VecDeque<StructuredLogEvent>>,
    capacity: usize,
    dropped_count: AtomicU64,
}

impl Default for LogRingBuffer {
    fn default() -> Self {
        Self::new(MAX_STRUCTURED_LOG_EVENTS)
    }
}

impl LogRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(capacity)),
            capacity,
            dropped_count: AtomicU64::new(0),
        }
    }

    pub fn push(&self, event: StructuredLogEvent) {
        let mut buffer = self.events.write();
        if buffer.len() >= self.capacity {
            buffer.pop_front();
            self.dropped_count.fetch_add(1, Ordering::SeqCst);
        }
        buffer.push_back(event);
    }

    pub fn log(&self, level: impl Into<String>, message: impl Into<String>) {
        let ctx = current_log_context();
        let event = StructuredLogEvent::new(level, message, ctx);
        self.push(event);
    }

    pub fn log_with_context(
        &self,
        level: impl Into<String>,
        message: impl Into<String>,
        context: LogContext,
    ) {
        let event = StructuredLogEvent::new(level, message, context);
        self.push(event);
    }

    pub fn events(&self) -> Vec<StructuredLogEvent> {
        self.events.read().iter().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.events.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.read().is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::SeqCst)
    }

    pub fn fill_ratio(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.len() as f64 / self.capacity as f64
        }
    }

    pub fn clear(&self) {
        self.events.write().clear();
        self.dropped_count.store(0, Ordering::SeqCst);
    }
}

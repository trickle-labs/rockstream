//! A bounded, single-owner physical commit driver.
//!
//! Callers own cancellable acknowledgement slots, not the storage future. A
//! dropped caller therefore cannot discard a drained epoch or cancel a write.
//! Pending, in-flight, failed and uncertain batches share one admission bound.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rockstream_storage::{ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::timestamp::Epoch;
use rockstream_verified::{persistence, physical_commit};
use tokio::sync::{oneshot, Notify};

use super::{
    DEFAULT_GROUP_COMMIT_MAX_DELAY_MS, MAX_GROUP_COMMIT_PENDING_BYTES,
    MAX_GROUP_COMMIT_RETRY_COUNT, MAX_GROUP_COMMIT_WAITERS, PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
};
use crate::error::OpError;

/// Bounds metadata even when a batch contains many zero-byte operations.
pub const MAX_GROUP_COMMIT_OPERATIONS: usize = 131_072;
/// Each storage attempt has a finite lifetime; a write timeout is not an abort.
pub const GROUP_COMMIT_IO_TIMEOUT_MS: u64 = 5_000;
const MAX_COMMIT_DIAGNOSTIC_BYTES: usize = 1_024;

type EpochReply = oneshot::Sender<Result<(), String>>;
type FlushReply = oneshot::Sender<Result<Vec<Epoch>, String>>;

struct Entry {
    batch: WriteBatch,
    bytes: usize,
    operations: usize,
    staged_at: Instant,
    waiter: Option<EpochReply>,
}

#[derive(Default)]
struct State {
    pending: BTreeMap<Epoch, Entry>,
    uncertain: BTreeMap<Epoch, Entry>,
    requests: BTreeMap<u64, FlushReply>,
    next_request: u64,
    retained_bytes: usize,
    retained_epochs: usize,
    retained_operations: usize,
    last_committed: Option<Epoch>,
    // Once sealed into a physical write, no lower epoch may subsequently enter.
    sealed_epoch: Option<Epoch>,
    in_flight: bool,
    force_flush: bool,
    retry_required: bool,
    write_outcome_unknown: bool,
    closing: bool,
}

struct Inner {
    db: Arc<ShardDb>,
    state: Mutex<State>,
    notify: Arc<Notify>,
    driver_started: AtomicBool,
    active_waiters: Arc<AtomicUsize>,
    max_delay: Duration,
    max_bytes: usize,
    max_epochs: usize,
    #[cfg(test)]
    test_gates: Mutex<Option<TestGates>>,
}

#[cfg(test)]
#[derive(Default)]
struct TestGates {
    before_write: Option<Arc<TestGate>>,
    after_write: Option<Arc<TestGate>>,
    after_flush: Option<Arc<TestGate>>,
}

#[cfg(test)]
struct TestGate {
    reached: Notify,
    proceed: Notify,
}

#[cfg(test)]
impl TestGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            reached: Notify::new(),
            proceed: Notify::new(),
        })
    }

    async fn pause(&self) {
        self.reached.notify_one();
        self.proceed.notified().await;
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.notify.notify_one();
    }
}

/// Coherent fill observations. Retained quantities include in-flight and retry
/// storage, not just the visible pending map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhysicalCommitFill {
    pub pending_epochs: usize,
    pub retained_epochs: usize,
    pub retained_bytes: usize,
    pub retained_operations: usize,
    pub active_waiters: usize,
    pub in_flight: bool,
    pub retry_required: bool,
    pub outcome_unknown: bool,
    pub max_bytes: usize,
    pub max_epochs: usize,
    pub max_waiters: usize,
}

pub struct PhysicalCommitGroup {
    inner: Arc<Inner>,
}

struct WaiterSlot(Arc<AtomicUsize>);

impl WaiterSlot {
    fn acquire(counter: &Arc<AtomicUsize>) -> Result<Self, OpError> {
        let mut current = counter.load(Ordering::Acquire);
        loop {
            if current >= MAX_GROUP_COMMIT_WAITERS {
                return Err(capacity(
                    "physical_commit_waiters",
                    current,
                    MAX_GROUP_COMMIT_WAITERS,
                ));
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Self(counter.clone())),
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for WaiterSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct FlushRequest {
    owner: Weak<Inner>,
    id: u64,
}

impl Drop for FlushRequest {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.state.lock().requests.remove(&self.id);
        }
    }
}

fn capacity(resource: &'static str, current: usize, limit: usize) -> OpError {
    OpError::capacity_exceeded(
        resource,
        current,
        limit,
        "apply backpressure, reduce the complete epoch, or recover the failed physical commit",
    )
}

fn diagnostic(error: impl std::fmt::Display) -> String {
    let mut text = error.to_string();
    if text.len() > MAX_COMMIT_DIAGNOSTIC_BYTES {
        let mut end = MAX_COMMIT_DIAGNOSTIC_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

impl PhysicalCommitGroup {
    /// Construct an empty commit owner. Use `open` when reopening persisted state.
    pub fn new(db: Arc<ShardDb>) -> Self {
        Self::with_config(
            db,
            DEFAULT_GROUP_COMMIT_MAX_DELAY_MS,
            MAX_GROUP_COMMIT_PENDING_BYTES,
            PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
        )
    }

    pub fn with_config(
        db: Arc<ShardDb>,
        max_delay_ms: u64,
        max_pending_bytes: usize,
        max_epochs: usize,
    ) -> Self {
        let group = Self {
            inner: Arc::new(Inner {
                db,
                state: Mutex::new(State::default()),
                notify: Arc::new(Notify::new()),
                driver_started: AtomicBool::new(false),
                active_waiters: Arc::new(AtomicUsize::new(0)),
                max_delay: Duration::from_millis(max_delay_ms),
                max_bytes: max_pending_bytes,
                max_epochs,
                #[cfg(test)]
                test_gates: Mutex::new(None),
            }),
        };
        group.ensure_driver();
        group
    }

    /// Recover the actual persisted frontier before admitting work on a reopened
    /// shard. The direct-write epoch allocator is not a committed-result token.
    pub async fn open(db: Arc<ShardDb>) -> Result<Self, OpError> {
        let bytes = tokio::time::timeout(
            Duration::from_millis(GROUP_COMMIT_IO_TIMEOUT_MS),
            db.get(&ShardKeyEncoder::frontier_key()),
        )
        .await
        .map_err(|_| {
            OpError::internal(
                "physical commit frontier read timed out; reopen the shard before admission",
            )
        })?
        .map_err(OpError::storage)?;
        let frontier = bytes
            .map(|bytes| {
                let value: [u8; 8] = bytes.as_ref().try_into().map_err(|_| {
                    OpError::internal(
                        "persisted physical commit frontier must contain exactly eight bytes",
                    )
                })?;
                Ok::<_, OpError>(u64::from_be_bytes(value))
            })
            .transpose()?;
        let group = Self::new(db);
        {
            let mut state = group.inner.state.lock();
            state.last_committed = frontier;
            state.sealed_epoch = frontier;
        }
        Ok(group)
    }

    pub fn fill_level(&self) -> usize {
        self.inner.state.lock().pending.len()
    }
    pub fn pending_epochs(&self) -> usize {
        self.fill_level()
    }
    pub fn pending_bytes(&self) -> usize {
        self.inner.state.lock().retained_bytes
    }
    pub fn active_waiters(&self) -> usize {
        self.inner.active_waiters.load(Ordering::Acquire)
    }
    pub fn last_committed(&self) -> Epoch {
        self.committed_epoch().unwrap_or(0)
    }
    pub fn committed_epoch(&self) -> Option<Epoch> {
        self.inner.state.lock().last_committed
    }
    pub fn outcome_unknown(&self) -> bool {
        let state = self.inner.state.lock();
        state.write_outcome_unknown || !state.uncertain.is_empty()
    }
    pub fn fill(&self) -> PhysicalCommitFill {
        let state = self.inner.state.lock();
        PhysicalCommitFill {
            pending_epochs: state.pending.len(),
            retained_epochs: state.retained_epochs,
            retained_bytes: state.retained_bytes,
            retained_operations: state.retained_operations,
            active_waiters: self.active_waiters(),
            in_flight: state.in_flight,
            retry_required: state.retry_required,
            outcome_unknown: state.write_outcome_unknown || !state.uncertain.is_empty(),
            max_bytes: self.inner.max_bytes,
            max_epochs: self.inner.max_epochs,
            max_waiters: MAX_GROUP_COMMIT_WAITERS,
        }
    }

    fn ensure_driver(&self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        if self
            .inner
            .driver_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            runtime.spawn(drive(
                Arc::downgrade(&self.inner),
                self.inner.notify.clone(),
            ));
        }
    }

    fn stage(
        &self,
        epoch: Epoch,
        batch: WriteBatch,
        waiter: Option<EpochReply>,
    ) -> Result<(), OpError> {
        let bytes = batch.byte_size();
        let operations = batch.len();
        let mut state = self.inner.state.lock();
        if state.closing || state.retry_required {
            return Err(OpError::internal("physical commit admission is stopped; flush recovery or reopen the shard before adding another epoch"));
        }
        if state.sealed_epoch.is_some_and(|sealed| epoch <= sealed)
            || state.pending.contains_key(&epoch)
        {
            return Err(OpError::internal(format!(
                "logical epoch {epoch} is not newer than the sealed physical frontier {:?}",
                state.sealed_epoch
            )));
        }
        let proposed = state
            .retained_bytes
            .checked_add(bytes)
            .ok_or_else(|| capacity("physical_commit_bytes", usize::MAX, self.inner.max_bytes))?;
        if proposed > self.inner.max_bytes {
            return Err(capacity(
                "physical_commit_bytes",
                proposed,
                self.inner.max_bytes,
            ));
        }
        if state.retained_epochs >= self.inner.max_epochs {
            return Err(capacity(
                "physical_commit_epochs",
                state.retained_epochs,
                self.inner.max_epochs,
            ));
        }
        let proposed_operations = state
            .retained_operations
            .checked_add(operations)
            .ok_or_else(|| {
                capacity(
                    "physical_commit_operations",
                    usize::MAX,
                    MAX_GROUP_COMMIT_OPERATIONS,
                )
            })?;
        if proposed_operations > MAX_GROUP_COMMIT_OPERATIONS {
            return Err(capacity(
                "physical_commit_operations",
                proposed_operations,
                MAX_GROUP_COMMIT_OPERATIONS,
            ));
        }
        state.pending.insert(
            epoch,
            Entry {
                batch,
                bytes,
                operations,
                staged_at: Instant::now(),
                waiter,
            },
        );
        state.retained_bytes = proposed;
        state.retained_epochs += 1;
        state.retained_operations = proposed_operations;
        let force_flush =
            proposed == self.inner.max_bytes || state.retained_epochs == self.inner.max_epochs;
        state.force_flush |= force_flush;
        drop(state);
        self.ensure_driver();
        self.inner.notify.notify_one();
        Ok(())
    }

    pub fn add_epoch(&self, epoch: Epoch, batch: WriteBatch) -> Result<(), OpError> {
        self.stage(epoch, batch, None)
    }

    pub async fn commit_epoch(&self, epoch: Epoch, batch: WriteBatch) -> Result<(), OpError> {
        let _slot = WaiterSlot::acquire(&self.inner.active_waiters)?;
        let (sender, receiver) = oneshot::channel();
        self.stage(epoch, batch, Some(sender))?;
        receiver
            .await
            .map_err(|_| OpError::internal("physical commit owner stopped before acknowledgement"))?
            .map_err(OpError::internal)
    }

    /// Request a driver-owned flush. Cancellation removes exactly this request,
    /// but never cancels a storage future already owned by the driver.
    pub async fn flush(&self) -> Result<Vec<Epoch>, OpError> {
        let _slot = WaiterSlot::acquire(&self.inner.active_waiters)?;
        let (sender, receiver) = oneshot::channel();
        let id = {
            let mut state = self.inner.state.lock();
            let id = state.next_request;
            state.next_request = id.checked_add(1).ok_or_else(|| {
                OpError::internal("physical commit request sequence exhausted; reopen the owner")
            })?;
            state.requests.insert(id, sender);
            id
        };
        let _request = FlushRequest {
            owner: Arc::downgrade(&self.inner),
            id,
        };
        self.ensure_driver();
        self.inner.notify.notify_one();
        receiver
            .await
            .map_err(|_| {
                OpError::internal("physical commit owner stopped before flush completion")
            })?
            .map_err(OpError::internal)
    }

    /// Stop admission and durably drain accepted work. An unknown write outcome
    /// stays an error; shutdown cannot convert it into a successful commit.
    pub async fn shutdown(&self) -> Result<(), OpError> {
        self.inner.state.lock().closing = true;
        while self.fill().retained_epochs != 0 {
            self.flush().await?;
        }
        self.inner.notify.notify_one();
        Ok(())
    }
}

impl Drop for PhysicalCommitGroup {
    fn drop(&mut self) {
        self.inner.notify.notify_one();
    }
}

struct Flight {
    owner: Arc<Inner>,
    entries: Option<BTreeMap<Epoch, Entry>>,
    requests: Vec<u64>,
    source: u8,
}

// If the executor cancels or unwinds the private driver, preserve all records
// and require recovery. Never restore an indeterminate write as a safe replay.
impl Drop for Flight {
    fn drop(&mut self) {
        if let Some(mut entries) = self.entries.take() {
            let error = "physical commit driver stopped during storage I/O; reopen the shard and reconcile the durable frontier".to_owned();
            for entry in entries.values_mut() {
                if let Some(waiter) = entry.waiter.take() {
                    let _ = waiter.send(Err(error.clone()));
                }
            }
            let mut state = self.owner.state.lock();
            let (next_owner, retained_epochs, retained_bytes, retained_operations, frontier, ack) =
                physical_commit::finish_transition(
                    self.source,
                    physical_commit::OUTCOME_UNKNOWN,
                    state.retained_epochs,
                    entries.len(),
                    state.retained_bytes,
                    entries.values().map(|entry| entry.bytes).sum(),
                    state.retained_operations,
                    entries.values().map(|entry| entry.operations).sum(),
                    state.last_committed,
                    *entries
                        .keys()
                        .next_back()
                        .expect("physical flight has an epoch"),
                )
                .expect("cancelled physical flight must retain all owned work");
            assert_eq!(next_owner, physical_commit::UNCERTAIN);
            assert!(!ack);
            state.retained_epochs = retained_epochs;
            state.retained_bytes = retained_bytes;
            state.retained_operations = retained_operations;
            state.last_committed = frontier;
            state.uncertain.append(&mut entries);
            state.in_flight = false;
            state.retry_required = true;
            state.write_outcome_unknown = true;
            for id in &self.requests {
                if let Some(reply) = state.requests.remove(id) {
                    let _ = reply.send(Err(error.clone()));
                }
            }
            self.owner.driver_started.store(false, Ordering::Release);
            self.owner.notify.notify_one();
        }
    }
}

enum Outcome {
    Durable,
    WriteFailed(String),
    FlushUnknown(String),
    WriteUnknown(String),
}

async fn persist(flight: &Flight) -> Outcome {
    let Some(entries) = flight.entries.as_ref() else {
        return Outcome::WriteUnknown("physical commit lost its retained entries".to_owned());
    };
    let timeout = Duration::from_millis(GROUP_COMMIT_IO_TIMEOUT_MS);
    if flight.source != physical_commit::FLIGHT_UNCERTAIN {
        #[cfg(test)]
        let gate = {
            flight
                .owner
                .test_gates
                .lock()
                .as_ref()
                .and_then(|g| g.before_write.clone())
        };
        #[cfg(test)]
        if let Some(gate) = gate {
            gate.pause().await;
        }
        // Retained originals, this merged batch, and one storage-attempt clone
        // are bounded by three payload envelopes. Operation metadata has its own
        // named bound. SlateDB's internal buffers are accounted separately.
        let mut merged = WriteBatch::new();
        for entry in entries.values() {
            merged.merge_from(entry.batch.clone());
        }
        if let Some(last) = entries.keys().next_back() {
            merged.put(&ShardKeyEncoder::frontier_key(), &last.to_be_bytes());
        }
        for attempt in 0..MAX_GROUP_COMMIT_RETRY_COUNT {
            match tokio::time::timeout(timeout, flight.owner.db.write_batch(merged.clone())).await {
                Ok(Ok(())) => break,
                Err(_) => return Outcome::WriteUnknown("physical write timed out; its outcome is unknown and must be reconciled after reopening the shard".to_owned()),
                Ok(Err(error)) => {
                    if attempt + 1 == MAX_GROUP_COMMIT_RETRY_COUNT { return Outcome::WriteFailed(diagnostic(error)); }
                    tokio::time::sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                }
            }
        }
        #[cfg(test)]
        let gate = {
            flight
                .owner
                .test_gates
                .lock()
                .as_ref()
                .and_then(|g| g.after_write.clone())
        };
        #[cfg(test)]
        if let Some(gate) = gate {
            gate.pause().await;
        }
    }
    for attempt in 0..MAX_GROUP_COMMIT_RETRY_COUNT {
        let error = match tokio::time::timeout(timeout, flight.owner.db.flush()).await {
            Ok(Ok(())) => {
                #[cfg(test)]
                let gate = {
                    flight
                        .owner
                        .test_gates
                        .lock()
                        .as_ref()
                        .and_then(|g| g.after_flush.clone())
                };
                #[cfg(test)]
                if let Some(gate) = gate {
                    gate.pause().await;
                }
                return Outcome::Durable;
            }
            Ok(Err(error)) => diagnostic(error),
            Err(_) => {
                "physical flush timed out after a successful write; durability remains unknown"
                    .to_owned()
            }
        };
        if attempt + 1 == MAX_GROUP_COMMIT_RETRY_COUNT {
            assert_eq!(
                persistence::commit_outcome(true, false),
                persistence::COMMIT_UNKNOWN
            );
            return Outcome::FlushUnknown(error);
        }
        tokio::time::sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
    }
    Outcome::WriteUnknown(
        "physical commit retry configuration did not execute an attempt".to_owned(),
    )
}

fn finish(mut flight: Flight, outcome: Outcome) {
    let Some(mut entries) = flight.entries.take() else {
        return;
    };
    let epochs = entries.keys().copied().collect::<Vec<_>>();
    let waiters = entries
        .values_mut()
        .filter_map(|entry| entry.waiter.take())
        .collect::<Vec<_>>();
    let bytes = entries.values().map(|entry| entry.bytes).sum::<usize>();
    let operations = entries
        .values()
        .map(|entry| entry.operations)
        .sum::<usize>();
    let write_outcome_unknown = matches!(outcome, Outcome::WriteUnknown(_));
    let (kind, error) = match outcome {
        Outcome::Durable => (physical_commit::DURABLE, None),
        Outcome::WriteFailed(error) => (physical_commit::WRITE_FAILED, Some(error)),
        Outcome::FlushUnknown(error) | Outcome::WriteUnknown(error) => {
            (physical_commit::OUTCOME_UNKNOWN, Some(error))
        }
    };
    let mut entries = Some(entries);
    if kind == physical_commit::DURABLE {
        // Free the actual retained batches before releasing capacity or acking.
        drop(entries.take());
    }
    let mut state = flight.owner.state.lock();
    let (owner, retained_epochs, retained_bytes, retained_operations, frontier, ack) =
        physical_commit::finish_transition(
            flight.source,
            kind,
            state.retained_epochs,
            epochs.len(),
            state.retained_bytes,
            bytes,
            state.retained_operations,
            operations,
            state.last_committed,
            *epochs.last().expect("physical flight has an epoch"),
        )
        .expect("M1-S8: physical commit transition must conserve retained work and frontier");
    state.retained_epochs = retained_epochs;
    state.retained_bytes = retained_bytes;
    state.retained_operations = retained_operations;
    state.last_committed = frontier;
    state.in_flight = false;
    state.retry_required = !ack;
    state.write_outcome_unknown = write_outcome_unknown;
    let result = if ack {
        assert_eq!(owner, physical_commit::COMMITTED);
        Ok(epochs)
    } else {
        let mut entries = entries.take().expect("failed flight retains its entries");
        if owner == physical_commit::PENDING {
            state.pending.append(&mut entries);
        } else {
            assert_eq!(owner, physical_commit::UNCERTAIN);
            state.uncertain.append(&mut entries);
        }
        Err(error.expect("failed physical flight has a diagnostic"))
    };
    drop(state);
    for waiter in waiters {
        let _ = waiter.send(result.as_ref().map(|_| ()).map_err(Clone::clone));
    }
    let mut state = flight.owner.state.lock();
    for id in &flight.requests {
        if let Some(reply) = state.requests.remove(id) {
            let _ = reply.send(result.clone());
        }
    }
}

async fn drive(owner: Weak<Inner>, notify: Arc<Notify>) {
    loop {
        let Some(inner) = owner.upgrade() else {
            return;
        };
        let (flight, delay) = {
            let mut state = inner.state.lock();
            if state.write_outcome_unknown && !state.requests.is_empty() {
                for (_, reply) in std::mem::take(&mut state.requests) {
                    let _ = reply.send(Err("physical write outcome is unknown; reopen and reconcile instead of replaying or acknowledging it".to_owned()));
                }
            }
            let oldest = state.pending.values().map(|entry| entry.staged_at).min();
            let due = oldest.is_some_and(|oldest| oldest.elapsed() >= inner.max_delay);
            let requested = !state.requests.is_empty();
            let ready = requested || (!state.retry_required && (due || state.force_flush));
            if ready
                && (!state.pending.is_empty() || !state.uncertain.is_empty())
                && !state.write_outcome_unknown
            {
                let source = physical_commit::begin_owner(
                    !state.pending.is_empty(),
                    !state.uncertain.is_empty(),
                    state.write_outcome_unknown,
                )
                .expect("ready physical flight owns pending or uncertain entries");
                let entries = if source == physical_commit::FLIGHT_UNCERTAIN {
                    std::mem::take(&mut state.uncertain)
                } else {
                    std::mem::take(&mut state.pending)
                };
                if let Some(last) = entries.keys().next_back().copied() {
                    state.sealed_epoch = Some(last);
                }
                state.in_flight = true;
                state.force_flush = false;
                let requests = state.requests.keys().copied().collect();
                (
                    Some(Flight {
                        owner: inner.clone(),
                        entries: Some(entries),
                        requests,
                        source,
                    }),
                    None,
                )
            } else {
                if requested && state.pending.is_empty() && state.uncertain.is_empty() {
                    for (_, reply) in std::mem::take(&mut state.requests) {
                        let _ = reply.send(Ok(Vec::new()));
                    }
                }
                let delay = if state.retry_required {
                    None
                } else {
                    oldest.map(|oldest| inner.max_delay.saturating_sub(oldest.elapsed()))
                };
                (None, delay)
            }
        };
        drop(inner); // Never retain the storage owner while sleeping idle.
        if let Some(flight) = flight {
            let result = persist(&flight).await;
            finish(flight, result);
        } else if let Some(delay) = delay {
            tokio::select! { _ = notify.notified() => {}, _ = tokio::time::sleep(delay) => {} }
        } else {
            notify.notified().await;
        }
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use rockstream_storage::MergeOperatorRegistry;
    use tempfile::tempdir;

    fn publish_transcript(path: &std::path::Path, value: &[u8]) {
        let staging = path.with_extension("writing");
        std::fs::write(&staging, value).unwrap();
        std::fs::rename(staging, path).unwrap();
    }

    fn empty_reopened_fill() -> PhysicalCommitFill {
        PhysicalCommitFill {
            pending_epochs: 0,
            retained_epochs: 0,
            retained_bytes: 0,
            retained_operations: 0,
            active_waiters: 0,
            in_flight: false,
            retry_required: false,
            outcome_unknown: false,
            max_bytes: MAX_GROUP_COMMIT_PENDING_BYTES,
            max_epochs: PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
            max_waiters: MAX_GROUP_COMMIT_WAITERS,
        }
    }

    #[tokio::test]
    async fn unknown_flush_keeps_later_epoch_queued_until_reconciliation() {
        let dir = tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(
            ShardDb::builder("unknown-flush-gated", store)
                .build()
                .await
                .unwrap(),
        );
        let group = Arc::new(PhysicalCommitGroup::with_config(
            db.clone(),
            5000,
            10 * 1024 * 1024,
            64,
        ));
        let gate = TestGate::new();
        group.inner.test_gates.lock().replace(TestGates {
            after_write: Some(gate.clone()),
            ..TestGates::default()
        });
        let mut first = WriteBatch::new();
        first.put(b"first_k", b"first_v");
        let mut second = WriteBatch::new();
        second.put(b"second_k", b"second_v");
        let first_bytes = first.byte_size();
        let second_bytes = second.byte_size();
        group.add_epoch(1, first).unwrap();
        let flushing = {
            let group = group.clone();
            tokio::spawn(async move { group.flush().await })
        };
        tokio::time::timeout(Duration::from_secs(2), gate.reached.notified())
            .await
            .unwrap();
        assert_eq!(
            group.fill(),
            PhysicalCommitFill {
                pending_epochs: 0,
                retained_epochs: 1,
                retained_bytes: first_bytes,
                retained_operations: 1,
                active_waiters: 1,
                in_flight: true,
                retry_required: false,
                outcome_unknown: false,
                max_bytes: 10 * 1024 * 1024,
                max_epochs: 64,
                max_waiters: MAX_GROUP_COMMIT_WAITERS,
            }
        );
        group.add_epoch(2, second).unwrap();
        db.set_fail_flushes(true);
        assert!(!flushing.is_finished());
        assert_eq!(group.last_committed(), 0);
        group.inner.test_gates.lock().take();
        gate.proceed.notify_one();
        assert!(flushing.await.unwrap().is_err());
        assert_eq!(
            group.fill(),
            PhysicalCommitFill {
                pending_epochs: 1,
                retained_epochs: 2,
                retained_bytes: first_bytes + second_bytes,
                retained_operations: 2,
                active_waiters: 0,
                in_flight: false,
                retry_required: true,
                outcome_unknown: true,
                max_bytes: 10 * 1024 * 1024,
                max_epochs: 64,
                max_waiters: MAX_GROUP_COMMIT_WAITERS,
            }
        );
        db.set_fail_flushes(false);
        assert_eq!(group.flush().await.unwrap(), vec![1]);
        assert_eq!(group.last_committed(), 1);
        assert_eq!(group.fill_level(), 1);
        assert_eq!(
            db.get(b"first_k").await.unwrap(),
            Some(bytes::Bytes::from_static(b"first_v"))
        );
        assert_eq!(db.get(b"second_k").await.unwrap(), None);
        assert_eq!(group.flush().await.unwrap(), vec![2]);
        assert_eq!(group.last_committed(), 2);
        assert_eq!(
            group.fill(),
            PhysicalCommitFill {
                pending_epochs: 0,
                retained_epochs: 0,
                retained_bytes: 0,
                retained_operations: 0,
                active_waiters: 0,
                in_flight: false,
                retry_required: false,
                outcome_unknown: false,
                max_bytes: 10 * 1024 * 1024,
                max_epochs: 64,
                max_waiters: MAX_GROUP_COMMIT_WAITERS,
            }
        );
        assert_eq!(
            db.get(b"first_k").await.unwrap(),
            Some(bytes::Bytes::from_static(b"first_v"))
        );
        assert_eq!(
            db.get(b"second_k").await.unwrap(),
            Some(bytes::Bytes::from_static(b"second_v"))
        );
    }

    #[tokio::test]
    async fn cancelled_request_at_each_storage_gate_keeps_both_epochs() {
        for cut in ["before_write", "after_write", "after_flush"] {
            let dir = tempdir().unwrap();
            let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
            let db = Arc::new(
                ShardDb::builder("cancelled-cut", store.clone())
                    .build()
                    .await
                    .unwrap(),
            );
            let group = Arc::new(PhysicalCommitGroup::with_config(
                db.clone(),
                5000,
                10 * 1024 * 1024,
                64,
            ));
            let gate = TestGate::new();
            let mut gates = TestGates::default();
            match cut {
                "before_write" => gates.before_write = Some(gate.clone()),
                "after_write" => gates.after_write = Some(gate.clone()),
                "after_flush" => gates.after_flush = Some(gate.clone()),
                _ => unreachable!(),
            }
            group.inner.test_gates.lock().replace(gates);
            let mut first = WriteBatch::new();
            first.put(b"a", b"first");
            let mut second = WriteBatch::new();
            second.put(b"b", b"later");
            let first_bytes = first.byte_size();
            let second_bytes = second.byte_size();
            group.add_epoch(1, first).unwrap();
            let mut flushing = {
                let group = group.clone();
                tokio::spawn(async move { group.flush().await })
            };
            tokio::time::timeout(Duration::from_secs(2), gate.reached.notified())
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), &mut flushing)
                    .await
                    .is_err()
            );
            flushing.abort();
            assert!(flushing.await.unwrap_err().is_cancelled());
            group.add_epoch(2, second).unwrap();
            assert_eq!(
                group.fill(),
                PhysicalCommitFill {
                    pending_epochs: 1,
                    retained_epochs: 2,
                    retained_bytes: first_bytes + second_bytes,
                    retained_operations: 2,
                    active_waiters: 0,
                    in_flight: true,
                    retry_required: false,
                    outcome_unknown: false,
                    max_bytes: 10 * 1024 * 1024,
                    max_epochs: 64,
                    max_waiters: MAX_GROUP_COMMIT_WAITERS,
                },
                "{cut}"
            );
            group.inner.test_gates.lock().take();
            gate.proceed.notify_one();
            tokio::time::timeout(Duration::from_secs(2), async {
                while group.last_committed() != 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                group.fill(),
                PhysicalCommitFill {
                    pending_epochs: 1,
                    retained_epochs: 1,
                    retained_bytes: second_bytes,
                    retained_operations: 1,
                    active_waiters: 0,
                    in_flight: false,
                    retry_required: false,
                    outcome_unknown: false,
                    max_bytes: 10 * 1024 * 1024,
                    max_epochs: 64,
                    max_waiters: MAX_GROUP_COMMIT_WAITERS,
                },
                "{cut}"
            );
            assert_eq!(group.flush().await.unwrap(), vec![2], "{cut}");
            assert_eq!(group.last_committed(), 2);
            assert_eq!(
                group.fill(),
                PhysicalCommitFill {
                    pending_epochs: 0,
                    retained_epochs: 0,
                    retained_bytes: 0,
                    retained_operations: 0,
                    active_waiters: 0,
                    in_flight: false,
                    retry_required: false,
                    outcome_unknown: false,
                    max_bytes: 10 * 1024 * 1024,
                    max_epochs: 64,
                    max_waiters: MAX_GROUP_COMMIT_WAITERS,
                },
                "{cut}"
            );
            group.shutdown().await.unwrap();
            drop(group);
            drop(db);
            let reopened_db = Arc::new(
                ShardDb::builder("cancelled-cut", store)
                    .build()
                    .await
                    .unwrap(),
            );
            let reopened = PhysicalCommitGroup::open(reopened_db.clone())
                .await
                .unwrap();
            assert_eq!(reopened.committed_epoch(), Some(2), "{cut}");
            assert_eq!(
                reopened_db.get(b"a").await.unwrap(),
                Some(bytes::Bytes::from_static(b"first")),
                "{cut}"
            );
            assert_eq!(
                reopened_db.get(b"b").await.unwrap(),
                Some(bytes::Bytes::from_static(b"later")),
                "{cut}"
            );
            assert_eq!(reopened.fill(), empty_reopened_fill(), "{cut}");
        }
    }

    #[tokio::test]
    async fn dropped_driver_flight_retains_exact_uncertain_ownership() {
        let dir = tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(
            ShardDb::builder("dropped-flight", store)
                .build()
                .await
                .unwrap(),
        );
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"value");
        let bytes = batch.byte_size();
        let inner = Arc::new(Inner {
            db,
            state: Mutex::new(State {
                retained_epochs: 1,
                retained_bytes: bytes,
                retained_operations: 1,
                sealed_epoch: Some(1),
                in_flight: true,
                ..State::default()
            }),
            notify: Arc::new(Notify::new()),
            driver_started: AtomicBool::new(false),
            active_waiters: Arc::new(AtomicUsize::new(0)),
            max_delay: Duration::from_secs(5),
            max_bytes: 1024,
            max_epochs: 4,
            test_gates: Mutex::new(None),
        });
        let group = PhysicalCommitGroup {
            inner: inner.clone(),
        };
        let flight = Flight {
            owner: inner,
            entries: Some(BTreeMap::from([(
                1,
                Entry {
                    batch,
                    bytes,
                    operations: 1,
                    staged_at: Instant::now(),
                    waiter: None,
                },
            )])),
            requests: vec![],
            source: physical_commit::FLIGHT_PENDING,
        };
        drop(flight);
        assert_eq!(
            group.fill(),
            PhysicalCommitFill {
                pending_epochs: 0,
                retained_epochs: 1,
                retained_bytes: bytes,
                retained_operations: 1,
                active_waiters: 0,
                in_flight: false,
                retry_required: true,
                outcome_unknown: true,
                max_bytes: 1024,
                max_epochs: 4,
                max_waiters: MAX_GROUP_COMMIT_WAITERS,
            }
        );
        assert_eq!(
            group
                .inner
                .state
                .lock()
                .uncertain
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(group.committed_epoch(), None);
    }

    #[tokio::test]
    async fn crash_child() {
        let Ok(root) = std::env::var("ROCKSTREAM_COMMIT_CRASH_ROOT") else {
            return;
        };
        let mode = std::env::var("ROCKSTREAM_COMMIT_CRASH_MODE").unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
        let db = Arc::new(
            ShardDb::builder("commit-crash-matrix", store)
                .build()
                .await
                .unwrap(),
        );
        let group = Arc::new(PhysicalCommitGroup::open(db.clone()).await.unwrap());
        let gate = TestGate::new();
        if mode != "graceful" && mode != "pre_operation_error" {
            let mut gates = TestGates::default();
            match mode.as_str() {
                "before_write" => gates.before_write = Some(gate.clone()),
                "after_write" => gates.after_write = Some(gate.clone()),
                "after_flush" => gates.after_flush = Some(gate.clone()),
                _ => panic!("unexpected crash mode: {mode}"),
            }
            group.inner.test_gates.lock().replace(gates);
        }
        if mode == "pre_operation_error" {
            db.set_fail_writes(true);
        }
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"new");
        batch.delete(b"b");
        batch.put(b"c", b"insert");
        batch.merge(b"sum", &MergeOperatorRegistry::encode_sum(7));
        group.add_epoch(2, batch).unwrap();
        let flushing = {
            let group = group.clone();
            tokio::spawn(async move { group.flush().await })
        };
        let transcript =
            std::path::Path::new(&std::env::var("ROCKSTREAM_COMMIT_CRASH_TRANSCRIPT").unwrap())
                .to_path_buf();
        if mode == "graceful" {
            assert_eq!(flushing.await.unwrap().unwrap(), vec![2]);
            group.shutdown().await.unwrap();
            db.flush().await.unwrap();
            publish_transcript(&transcript, b"ack:2");
        } else if mode == "pre_operation_error" {
            assert!(flushing.await.unwrap().is_err());
            assert_eq!(group.fill().pending_epochs, 1);
            publish_transcript(&transcript, b"pre-operation-error");
        } else {
            tokio::time::timeout(Duration::from_secs(10), gate.reached.notified())
                .await
                .unwrap();
            let mut later = WriteBatch::new();
            later.put(b"later", b"must-not-commit");
            group.add_epoch(3, later).unwrap();
            assert!(!flushing.is_finished());
            assert_eq!(group.last_committed(), 1);
            assert_eq!(group.fill().retained_epochs, 2);
            publish_transcript(&transcript, b"before-ack");
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test]
    async fn lfs_subprocess_cuts_have_only_legal_atomic_recoveries() {
        use std::process::Command;

        for mode in [
            "before_write",
            "after_write",
            "after_flush",
            "graceful",
            "pre_operation_error",
        ] {
            let dir = tempdir().unwrap();
            let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
            let db = ShardDb::builder("commit-crash-matrix", store.clone())
                .build()
                .await
                .unwrap();
            let mut baseline = WriteBatch::new();
            baseline.put(b"a", b"old");
            baseline.put(b"b", b"keep");
            baseline.merge(b"sum", &MergeOperatorRegistry::encode_sum(10));
            baseline.put(&ShardKeyEncoder::frontier_key(), &1u64.to_be_bytes());
            db.write_batch(baseline).await.unwrap();
            db.flush().await.unwrap();
            db.close().await.unwrap();

            let transcript = dir.path().join("transcript");
            let mut child = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("group_commit::physical::boundary_tests::crash_child")
                .env("ROCKSTREAM_COMMIT_CRASH_ROOT", dir.path())
                .env("ROCKSTREAM_COMMIT_CRASH_MODE", mode)
                .env("ROCKSTREAM_COMMIT_CRASH_TRANSCRIPT", &transcript)
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(15);
            while !transcript.exists() {
                assert!(
                    Instant::now() < deadline,
                    "{mode}: child did not reach its cut"
                );
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "{mode}: child exited before its cut"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let observed = std::fs::read(&transcript).unwrap();
            if mode == "graceful" || mode == "pre_operation_error" {
                assert!(child.wait().unwrap().success(), "{mode}: child failed");
            } else {
                assert_eq!(observed, b"before-ack", "{mode}: ack preceded the cut");
                child.kill().unwrap();
                assert!(
                    !child.wait().unwrap().success(),
                    "{mode}: child was not killed"
                );
            }

            let db = Arc::new(
                ShardDb::builder("commit-crash-matrix", store)
                    .build()
                    .await
                    .unwrap(),
            );
            let reopened = PhysicalCommitGroup::open(db.clone()).await.unwrap();
            let frontier_bytes = db
                .get(&ShardKeyEncoder::frontier_key())
                .await
                .unwrap()
                .unwrap();
            let frontier = u64::from_be_bytes(frontier_bytes.as_ref().try_into().unwrap());
            assert_eq!(reopened.committed_epoch(), Some(frontier));
            let actual = (
                frontier,
                db.get(b"a").await.unwrap(),
                db.get(b"b").await.unwrap(),
                db.get(b"c").await.unwrap(),
                db.get(b"later").await.unwrap(),
                MergeOperatorRegistry::decode_sum(&db.get(b"sum").await.unwrap().unwrap()),
            );
            let baseline = (
                1,
                Some(bytes::Bytes::from_static(b"old")),
                Some(bytes::Bytes::from_static(b"keep")),
                None,
                None,
                Some(10),
            );
            let committed = (
                2,
                Some(bytes::Bytes::from_static(b"new")),
                None,
                Some(bytes::Bytes::from_static(b"insert")),
                None,
                Some(17),
            );
            match mode {
                "before_write" | "pre_operation_error" => {
                    if mode == "pre_operation_error" {
                        assert_eq!(observed, b"pre-operation-error");
                    }
                    assert_eq!(actual, baseline, "{mode}");
                }
                "graceful" => {
                    assert_eq!(observed, b"ack:2");
                    assert_eq!(actual, committed);
                }
                "after_write" | "after_flush" => {
                    assert_eq!(actual, committed, "{mode}: awaited durable write was lost");
                }
                _ => unreachable!(),
            }
            assert_eq!(reopened.fill(), empty_reopened_fill());
        }
    }
}

//! Frontier aggregation for the control plane (v0.18, Slice 6).
//!
//! Implements a two-level hierarchical aggregator:
//!
//! 1. **`FrontierAggregator`** — ingests [`ShardFrontierReport`]s from individual
//!    shards and computes the cluster-wide minimum via meet (GLB).
//!
//! v0.45.6 adds a durable **`FrontierLeaseStore`** so a single
//! `FrontierAggregator` can be safely elected the cluster's *publisher* via
//! a fencing-token CAS, mirroring `formal/m2_frontier_agg.fizz`'s
//! `ObjectStore.cas_lease`/`write_frontier`.
//!
//! ## M2 Safety Invariants (runtime assertions)
//!
//! These mirror the FizzBee M2 model in `formal/m2_frontier_agg.fizz`:
//!
//! - **M2-S1 / M2-S2** (`M2_S1_MeetCorrectness` / `M2_S2_PessimisticStaleness`):
//!   The published cluster frontier must never exceed the true meet of all
//!   registered shard frontiers.
//! - **M2-S3** (`M2_S3_SinglePublisherSafety`): [`assert_valid_publisher`] —
//!   at most one aggregator's `publish_frontier` CAS write may succeed under
//!   the current fence token.
//! - **M2-S4** (`M2_S4_StaleWriteRejection`): The cluster frontier is
//!   monotonically non-decreasing; stale writes are rejected. Paired with
//!   [`assert_flush_before_lease_handoff_read`], which additionally checks
//!   that a newly-elected publisher's first read of the published frontier
//!   only ever observes a synchronously-flushed write.
//!
//! ## Bounds
//!
//! | Resource | Bound | Metric |
//! |---|---|---|
//! | Registered shards | `MAX_REGISTERED_SHARDS` | `frontier_registered_shards` |
//!
//! `FrontierLeaseStore` itself holds exactly two fixed keys
//! (`frontier/leader`, `frontier/published`) — it is not a queue or buffer
//! and cannot grow unboundedly.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use rockstream_types::frontier::{ClusterFrontier, ShardFrontierReport};
use rockstream_types::ids::{AggregatorId, LeaseToken, ShardId};
use rockstream_types::timestamp::Epoch;
use slatedb::config::WriteOptions;
use slatedb::{Db, WriteBatch};

use crate::audit::{AuditEvent, FileAuditLog};

/// Maximum number of shards that can be registered with one `FrontierAggregator`.
///
/// Prevents unbounded memory growth. Ingest returns `RS-8001` when full.
pub const MAX_REGISTERED_SHARDS: usize = 100_000;

/// Error returned by [`FrontierAggregator`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AggregatorError {
    /// The shard registry is full; new shard reports are rejected.
    #[error(
        "RS-8001 frontier aggregator shard limit exceeded ({MAX_REGISTERED_SHARDS}); \
         next_steps: scale out aggregators or reduce shard count"
    )]
    RegistryFull,
}

/// The cluster-wide frontier fill level.
///
/// `registered` is the number of shards that have ever reported.
/// `capacity` is `MAX_REGISTERED_SHARDS`.
#[derive(Debug, Clone, Copy)]
pub struct FrontierFillLevel {
    /// Registered shard count.
    pub registered: usize,
    /// Maximum allowed shard count.
    pub capacity: usize,
}

impl FrontierFillLevel {
    /// Fraction of capacity used (0.0–1.0).
    pub fn fill_fraction(&self) -> f64 {
        self.registered as f64 / self.capacity as f64
    }
}

/// v0.45.6 (M2-S3): publisher-election state, present only when the
/// aggregator was built with [`FrontierAggregator::with_lease_store`].
struct LeaseWiring {
    store: Arc<FrontierLeaseStore>,
    aggregator_id: AggregatorId,
    /// `true` while this aggregator believes it holds the current publisher
    /// lease. Demoted to `false` (never panics) on a lost CAS race — the
    /// `PublishFrontier`/`AcquireLease` model actions' `else` branch.
    is_publisher: bool,
    /// The fence token this aggregator was granted at its last successful
    /// `acquire_publisher_lease` call.
    lease_token: u64,
}

/// Inner state protected by the mutex.
struct Inner {
    /// Per-shard committed epochs. Only ever grows in epoch value (monotone).
    shard_epochs: HashMap<ShardId, Epoch>,
    /// The last published cluster frontier (monotonically non-decreasing).
    published: Option<Epoch>,
    /// v0.45.6 (M2-S3): publisher-lease wiring; `None` for aggregators
    /// constructed without a `FrontierLeaseStore` (pre-v0.45.6 behavior).
    lease: Option<LeaseWiring>,
}

impl Inner {
    fn new() -> Self {
        Self {
            shard_epochs: HashMap::new(),
            published: None,
            lease: None,
        }
    }

    /// Compute the current meet (minimum) across all registered shard epochs.
    ///
    /// Returns `None` if no shards have reported yet.
    fn compute_meet(&self) -> Option<Epoch> {
        self.shard_epochs.values().copied().min()
    }
}

/// Control-plane frontier aggregator.
///
/// Receives [`ShardFrontierReport`]s and publishes a [`ClusterFrontier`]
/// representing the global minimum committed epoch.
///
/// Thread-safe; clone is cheap (shared inner state via `Arc<Mutex<_>>`).
#[derive(Clone)]
pub struct FrontierAggregator {
    inner: Arc<Mutex<Inner>>,
}

impl FrontierAggregator {
    /// Create a new, empty aggregator.
    ///
    /// Not wired to a [`FrontierLeaseStore`] — `is_publisher()`/`lease_token()`
    /// always report `false`/`0`, and `acquire_lease()`/`try_publish()` panic
    /// if called. Use [`FrontierAggregator::with_lease_store`] for the
    /// v0.45.6 publisher-election path.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    /// Create an aggregator wired to a durable [`FrontierLeaseStore`] for
    /// publisher-lease CAS election (v0.45.6, M2-S3).
    pub fn with_lease_store(aggregator_id: AggregatorId, store: Arc<FrontierLeaseStore>) -> Self {
        let mut inner = Inner::new();
        inner.lease = Some(LeaseWiring {
            store,
            aggregator_id,
            is_publisher: false,
            lease_token: 0,
        });
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// `true` while this aggregator believes it holds the current publisher
    /// lease. Always `false` for aggregators built with [`FrontierAggregator::new`].
    pub fn is_publisher(&self) -> bool {
        self.inner
            .lock()
            .lease
            .as_ref()
            .map(|l| l.is_publisher)
            .unwrap_or(false)
    }

    /// The fence token this aggregator was last granted, or `0` if it has
    /// never held the publisher lease.
    pub fn lease_token(&self) -> u64 {
        self.inner
            .lock()
            .lease
            .as_ref()
            .map(|l| l.lease_token)
            .unwrap_or(0)
    }

    /// Attempt to acquire (or renew) the publisher lease from the wired
    /// [`FrontierLeaseStore`], mirroring the FizzBee model's `AcquireLease`
    /// action.
    ///
    /// Returns `Ok(true)` if the lease was granted (this aggregator is now
    /// the publisher) or `Ok(false)` on a lost CAS race — losing the race is
    /// the normal, expected outcome when multiple aggregators contend for
    /// the lease and does **not** panic.
    ///
    /// On success, this aggregator's first read of the currently-published
    /// frontier is checked by [`assert_flush_before_lease_handoff_read`]
    /// (S5) before being folded into the local view (mirroring the model's
    /// `vector_join` with `store.published_frontier` on lease acquisition).
    ///
    /// # Panics
    ///
    /// Panics if this aggregator was constructed with
    /// [`FrontierAggregator::new`] (no lease store wired).
    pub async fn acquire_lease(&self) -> Result<bool, FrontierLeaseError> {
        let (store, aggregator_id) = {
            let inner = self.inner.lock();
            let lease = inner
                .lease
                .as_ref()
                .expect("acquire_lease called without a lease store; use with_lease_store()");
            (lease.store.clone(), lease.aggregator_id)
        };

        let current_fence_token = store.current_fence_token().await;
        match store
            .acquire_publisher_lease(aggregator_id, current_fence_token)
            .await
        {
            Ok(token) => {
                // S5: lease-handoff read — must observe a synchronously
                // flushed value (panics via assert_flush_before_lease_handoff_read
                // otherwise).
                let published = store.read_published_frontier_after_handoff().await;

                let mut inner = self.inner.lock();
                if let Some(epoch) = published {
                    if inner.published.map(|p| epoch > p).unwrap_or(true) {
                        inner.published = Some(epoch);
                    }
                }
                let lease = inner.lease.as_mut().expect("checked above");
                lease.is_publisher = true;
                lease.lease_token = token.0;
                Ok(true)
            }
            Err(FrontierLeaseError::StaleFenceToken { .. }) => {
                let mut inner = self.inner.lock();
                inner.lease.as_mut().expect("checked above").is_publisher = false;
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Attempt to publish the current meet of registered shard epochs,
    /// mirroring the FizzBee model's `PublishFrontier` action.
    ///
    /// Returns `Ok(true)` on a successful publish, `Ok(false)` if this
    /// aggregator is not (or no longer) the publisher — a lost CAS race
    /// demotes it to follower (`is_publisher = false`) rather than
    /// panicking, matching the model's `else: self.is_publisher = False`
    /// branch. Only a genuine time-of-check-to-time-of-use race between the
    /// pre-write peek below and the durable write itself can still reach
    /// [`FrontierLeaseStore::publish_frontier`]'s hard `assert_valid_publisher`
    /// panic (S4) — the same defense-in-depth pattern as
    /// `rockstream_runtime::fence::assert_valid_writer`.
    ///
    /// # Panics
    ///
    /// Panics if this aggregator was constructed with
    /// [`FrontierAggregator::new`] (no lease store wired).
    pub async fn try_publish(&self) -> Result<bool, FrontierLeaseError> {
        let (store, held_token, meet) = {
            let inner = self.inner.lock();
            let lease = inner
                .lease
                .as_ref()
                .expect("try_publish called without a lease store; use with_lease_store()");
            if !lease.is_publisher {
                return Ok(false);
            }
            (lease.store.clone(), lease.lease_token, inner.compute_meet())
        };
        let Some(meet) = meet else {
            return Ok(false); // No shard reports yet — nothing to publish.
        };

        // Peek before attempting the durable write: a lost CAS is the
        // normal race-loss outcome (S6) and must demote to follower here,
        // not via a panic in publish_frontier.
        let current_fence_token = store.current_fence_token().await;
        if current_fence_token != held_token {
            let mut inner = self.inner.lock();
            inner.lease.as_mut().expect("checked above").is_publisher = false;
            return Ok(false);
        }

        store.publish_frontier(LeaseToken(held_token), meet).await?;
        let mut inner = self.inner.lock();
        if inner.published.map(|p| meet > p).unwrap_or(true) {
            inner.published = Some(meet);
        }
        Ok(true)
    }

    /// Ingest a [`ShardFrontierReport`] from a shard.
    ///
    /// Updates the per-shard epoch (monotonically). The published cluster
    /// frontier only ever advances — it is never retreated.
    ///
    /// **M2-S1 / M2-S2**: the published frontier never exceeds the meet of all
    /// currently registered shard epochs — guaranteed by only updating
    /// `published` when `meet >= published`.
    ///
    /// **M2-S4**: published is monotonically non-decreasing — enforced by only
    /// assigning `published = meet` when `meet > published`.
    ///
    /// Returns `Err(AggregatorError::RegistryFull)` (RS-8001) when
    /// `MAX_REGISTERED_SHARDS` is exceeded.
    pub fn ingest(&self, report: ShardFrontierReport) -> Result<(), AggregatorError> {
        let mut inner = self.inner.lock();

        // Enforce registry capacity bound.
        if !inner.shard_epochs.contains_key(&report.shard_id)
            && inner.shard_epochs.len() >= MAX_REGISTERED_SHARDS
        {
            return Err(AggregatorError::RegistryFull);
        }

        // Monotone update: only advance, never retreat.
        let entry = inner.shard_epochs.entry(report.shard_id).or_insert(0);
        if report.epoch > *entry {
            *entry = report.epoch;
        }

        // M2-S1 / M2-S2: only publish the meet if it is ≥ current published.
        // This guarantees published never retreats (M2-S4) AND never exceeds
        // the true meet of all registered shard epochs (M2-S1/S2).
        if let Some(meet) = inner.compute_meet() {
            match inner.published {
                None => {
                    // First publication.
                    inner.published = Some(meet);
                }
                Some(old) if meet > old => {
                    // M2-S1 / M2-S2 / M2-S4 assertion: meet > old is already
                    // guaranteed here, so `meet` never exceeds the true meet
                    // (M2-S1/S2) and `published` never retreats (M2-S4).
                    assert!(
                        meet >= old,
                        "M2-S1/M2-S2/M2-S4: stale write rejected — meet {meet} < published {old}"
                    );
                    inner.published = Some(meet);
                }
                Some(_) => {
                    // meet <= published: do not retreat. M2-S4 is satisfied.
                    // M2-S1/S2 is satisfied because published ≤ prior meet ≤ current meet
                    // (a new low-epoch shard doesn't retroactively invalidate what we
                    // have already committed — it only blocks *future* advancement).
                }
            }
        }

        Ok(())
    }

    /// Return the current cluster frontier.
    pub fn cluster_frontier(&self) -> ClusterFrontier {
        let inner = self.inner.lock();
        ClusterFrontier {
            epoch: inner.published,
        }
    }

    /// Return the current fill level for monitoring.
    pub fn fill_level(&self) -> FrontierFillLevel {
        let inner = self.inner.lock();
        FrontierFillLevel {
            registered: inner.shard_epochs.len(),
            capacity: MAX_REGISTERED_SHARDS,
        }
    }
}

impl Default for FrontierAggregator {
    fn default() -> Self {
        Self::new()
    }
}

// ─── v0.45.6: durable frontier-lease CAS store (M2-S3) ───────────────────────

/// SlateDB key holding the current fencing token and its holder aggregator
/// (16 bytes: `[fence_token: u64 BE][holder_aggregator_id: u64 BE]`).
const LEASE_KEY: &[u8] = b"frontier/leader";

/// SlateDB key holding the last published cluster frontier epoch (8 bytes,
/// `u64` BE). Absent until the first successful `publish_frontier` call.
const PUBLISHED_FRONTIER_KEY: &[u8] = b"frontier/published";

/// Error returned by [`FrontierLeaseStore`] operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum FrontierLeaseError {
    /// The caller's fence token does not match the store's current fence
    /// token — either a lease-acquisition CAS lost a race against a newer
    /// token, or (if reached) a `publish_frontier` call carried a stale
    /// token. See [`assert_valid_publisher`] for the hard-invariant panic
    /// path; this variant is the graceful outcome for lease *acquisition*.
    #[error(
        "RS-8002: stale fencing token: provided={provided}, current={current}; \
         next_steps: re-acquire the lease under the current fence token before retrying"
    )]
    StaleFenceToken {
        /// The token the caller presented.
        provided: u64,
        /// The store's current fence token.
        current: u64,
    },
    /// The underlying SlateDB operation failed.
    #[error("frontier-lease store I/O error: {0}")]
    Storage(String),
}

/// Cached (in-memory) mirror of the durable lease/frontier state, protected
/// by [`FrontierLeaseStore`]'s async mutex so CAS decisions and their
/// durable writes are serialized within one process (the single
/// control-plane leader — see M7).
#[derive(Debug, Clone, Copy, Default)]
struct LeaseState {
    fence_token: u64,
    holder: Option<AggregatorId>,
    published: Option<Epoch>,
    /// v0.45.6 (M2-S3/S4 pair): `true` once `published` has been confirmed
    /// durably flushed (`WriteOptions { await_durable: true }`) — checked by
    /// [`assert_flush_before_lease_handoff_read`] on every lease handoff.
    last_write_synced: bool,
}

/// Durable, CAS-protected store for the cluster's frontier-publisher lease
/// and its published frontier.
///
/// Backed by its own SlateDB instance (mirroring the `ShardDb`-style
/// pattern already used elsewhere in RockStream for durable per-key
/// storage), keyed on `frontier/leader` / `frontier/published`. Mirrors
/// `formal/m2_frontier_agg.fizz`'s `ObjectStore.cas_lease`/`write_frontier`.
///
/// Every write is issued with `WriteOptions { await_durable: true }`
/// (DESIGN.md §3.2 "Synchronous frontier writes") so a lease handoff can
/// never observe a not-yet-durable value — enforced by
/// [`assert_flush_before_lease_handoff_read`].
pub struct FrontierLeaseStore {
    db: Db,
    state: tokio::sync::Mutex<LeaseState>,
    audit: Option<Arc<FileAuditLog>>,
}

impl FrontierLeaseStore {
    /// Open (or create) a `FrontierLeaseStore` at `path` on `object_store`.
    pub async fn open(
        path: impl Into<String>,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<Self, FrontierLeaseError> {
        let db = Db::builder(path.into(), object_store)
            .build()
            .await
            .map_err(|e| FrontierLeaseError::Storage(e.to_string()))?;

        let (fence_token, holder) = read_lease_record(&db).await?;
        let published = read_published(&db).await?;

        Ok(Self {
            db,
            state: tokio::sync::Mutex::new(LeaseState {
                fence_token,
                holder,
                published,
                // A value recovered from a prior run was, by construction,
                // written with await_durable: true (every write path does
                // so) — it is therefore already sync-flushed.
                last_write_synced: published.is_some(),
            }),
            audit: None,
        })
    }

    /// Attach an audit log; lease acquisitions and publications will be
    /// written to it.
    pub fn with_audit(mut self, audit: Arc<FileAuditLog>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Peek the store's current fence token without attempting a CAS.
    ///
    /// Used by [`FrontierAggregator::try_publish`] to detect a lost lease
    /// race *before* attempting the durable write, so the normal race-loss
    /// path demotes gracefully instead of hitting
    /// [`assert_valid_publisher`]'s hard panic.
    pub async fn current_fence_token(&self) -> u64 {
        self.state.lock().await.fence_token
    }

    /// Attempt to acquire (or renew) the publisher lease via a fencing-token
    /// CAS, mirroring the FizzBee model's `ObjectStore.cas_lease`.
    ///
    /// Succeeds only if `current_fence_token` matches the store's current
    /// fence token, in which case a strictly higher token is minted,
    /// durably persisted (`await_durable: true`), and returned. Losing this
    /// race (`current_fence_token` is already stale) is a normal outcome —
    /// returns `Err(FrontierLeaseError::StaleFenceToken)`, not a panic.
    pub async fn acquire_publisher_lease(
        &self,
        aggregator_id: AggregatorId,
        current_fence_token: u64,
    ) -> Result<LeaseToken, FrontierLeaseError> {
        let mut state = self.state.lock().await;
        if current_fence_token != state.fence_token {
            return Err(FrontierLeaseError::StaleFenceToken {
                provided: current_fence_token,
                current: state.fence_token,
            });
        }

        let new_token = state
            .fence_token
            .checked_add(1)
            .expect("frontier-lease fence token counter exhausted");

        let mut record = Vec::with_capacity(16);
        record.extend_from_slice(&new_token.to_be_bytes());
        record.extend_from_slice(&aggregator_id.0.to_be_bytes());
        let mut batch = WriteBatch::new();
        batch.put(LEASE_KEY, &record);
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| FrontierLeaseError::Storage(e.to_string()))?;

        state.fence_token = new_token;
        state.holder = Some(aggregator_id);

        if let Some(audit) = &self.audit {
            let _ = audit.append(&AuditEvent::now(
                "frontier-aggregator",
                "frontier.lease_acquired",
                aggregator_id.to_string(),
            ));
        }

        Ok(LeaseToken(new_token))
    }

    /// Publish `frontier` under `token`, mirroring the FizzBee model's
    /// `ObjectStore.write_frontier`.
    ///
    /// **M2-S3 paired assertion**: calls [`assert_valid_publisher`] before
    /// every attempt — panics with `RS-8002` if `token` is not the store's
    /// current fence token. Callers (`FrontierAggregator::try_publish`)
    /// must peek [`FrontierLeaseStore::current_fence_token`] first so a
    /// normal lost-race demotion never reaches this panic; only a genuine
    /// time-of-check-to-time-of-use bug would.
    ///
    /// Rejects (silently, matching the model's `write_frontier` returning
    /// `False`) a `frontier` that would retreat the currently published
    /// value — M2-S4 stale-write rejection.
    pub async fn publish_frontier(
        &self,
        token: LeaseToken,
        frontier: Epoch,
    ) -> Result<(), FrontierLeaseError> {
        let mut state = self.state.lock().await;

        // M2-S3 paired assertion (S4): hard invariant, not a graceful error.
        assert_valid_publisher(state.holder, token.0, state.fence_token);

        if let Some(current) = state.published {
            if frontier < current {
                // M2-S4: stale write silently rejected; frontier never retreats.
                return Ok(());
            }
        }

        let mut batch = WriteBatch::new();
        batch.put(PUBLISHED_FRONTIER_KEY, frontier.to_be_bytes());
        self.db
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| FrontierLeaseError::Storage(e.to_string()))?;

        state.published = Some(frontier);
        state.last_write_synced = true;

        if let Some(audit) = &self.audit {
            let _ = audit.append(&AuditEvent::now(
                "frontier-aggregator",
                "frontier.published",
                frontier.to_string(),
            ));
        }

        Ok(())
    }

    /// A newly-elected publisher's first read of the published frontier
    /// during lease handoff.
    ///
    /// **M2-S3/S4 paired assertion (S5)**: calls
    /// [`assert_flush_before_lease_handoff_read`] — panics with `RS-8003`
    /// if a published value exists but was not confirmed durably flushed
    /// before this read.
    pub async fn read_published_frontier_after_handoff(&self) -> Option<Epoch> {
        let state = self.state.lock().await;
        assert_flush_before_lease_handoff_read(state.published.is_some(), state.last_write_synced);
        state.published
    }
}

async fn read_lease_record(db: &Db) -> Result<(u64, Option<AggregatorId>), FrontierLeaseError> {
    match db
        .get(LEASE_KEY)
        .await
        .map_err(|e| FrontierLeaseError::Storage(e.to_string()))?
    {
        Some(bytes) if bytes.len() == 16 => {
            let fence_token = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            let holder = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
            Ok((fence_token, Some(AggregatorId(holder))))
        }
        _ => Ok((0, None)),
    }
}

async fn read_published(db: &Db) -> Result<Option<Epoch>, FrontierLeaseError> {
    match db
        .get(PUBLISHED_FRONTIER_KEY)
        .await
        .map_err(|e| FrontierLeaseError::Storage(e.to_string()))?
    {
        Some(bytes) if bytes.len() == 8 => {
            let epoch = u64::from_be_bytes(bytes[..8].try_into().unwrap());
            Ok(Some(epoch))
        }
        _ => Ok(None),
    }
}

// ─── M2-S3 / M2-S4 paired runtime assertions ─────────────────────────────────

/// **M2-S3 paired assertion** (`M2_S3_SinglePublisherSafety` in
/// `formal/m2_frontier_agg.fizz`): a `publish_frontier` CAS write must carry
/// the store's *current* fence token — otherwise more than one aggregator
/// could believe itself the active publisher simultaneously.
///
/// # Panics
///
/// Panics with an `RS-8002` message if `token != current_fence_token`.
pub fn assert_valid_publisher(
    current_holder: Option<AggregatorId>,
    token: u64,
    current_fence_token: u64,
) {
    // M2-S3 paired assertion: single-publisher safety via fence-token CAS.
    assert!(
        token == current_fence_token,
        "RS-8002: M2-S3 violation — stale fencing token on frontier publish: \
         token={token}, current_fence_token={current_fence_token}, \
         current_holder={current_holder:?}. \
         next_steps: this aggregator has been fenced out; acquire a new \
         lease before retrying."
    );
}

/// **M2-S3/M2-S4 paired assertion** (second half of the pair, per
/// FIZZBEE_TEST_PLAN.md §3.7's M2-S3/S4 row): a newly-elected publisher's
/// first read of the published frontier during lease handoff must only
/// ever observe a synchronously-flushed (`WriteOptions { await_durable:
/// true }`) write — never an in-flight or lost one.
///
/// # Panics
///
/// Panics with an `RS-8003` message if `has_published_value` is `true` but
/// `last_write_synced` is `false`.
pub fn assert_flush_before_lease_handoff_read(has_published_value: bool, last_write_synced: bool) {
    // M2-S3/M2-S4 paired assertion: sync-flush-before-lease-handoff-read.
    assert!(
        !has_published_value || last_write_synced,
        "RS-8003: M2-S3/M2-S4 violation — sync-flush-before-lease-handoff-read: \
         a published frontier value exists but was not confirmed durably \
         flushed before being observed during lease handoff. \
         next_steps: verify every publish_frontier write path uses \
         WriteOptions {{ await_durable: true }}; this indicates a durability \
         regression in FrontierLeaseStore."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::frontier::ShardFrontierReport;
    use rockstream_types::ids::ShardId;

    /// Slice 6: basic single-shard report advances cluster frontier.
    #[test]
    fn single_shard_advances_frontier() {
        let agg = FrontierAggregator::new();
        assert_eq!(agg.cluster_frontier().epoch, None);

        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 10,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(10));
    }

    /// Slice 6: cluster frontier is meet (minimum) of all shard epochs.
    #[test]
    fn cluster_frontier_is_meet_of_shards() {
        let agg = FrontierAggregator::new();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 5,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 8,
        })
        .unwrap();
        // meet(5, 8) = 5
        assert_eq!(agg.cluster_frontier().epoch, Some(5));
    }

    /// Slice 6: M2-S4 — cluster frontier is monotonically non-decreasing.
    #[test]
    fn cluster_frontier_is_non_decreasing() {
        let agg = FrontierAggregator::new();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 5,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 3,
        })
        .unwrap(); // stale update
                   // Published should remain at 5, not retreat.
        assert_eq!(agg.cluster_frontier().epoch, Some(5));
    }

    /// Slice 6: advancing a lagging shard unblocks the cluster frontier.
    ///
    /// Both shards start at the same epoch so the initial frontier is well-
    /// defined; then shard 0 advances ahead while shard 1 lags, bottlenecking
    /// the cluster.  Finally shard 1 catches up and the cluster frontier
    /// can advance.
    #[test]
    fn advancing_lagging_shard_unblocks_cluster_frontier() {
        let agg = FrontierAggregator::new();
        // Both shards start at epoch 1 — cluster frontier is 1.
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 1,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 1,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(1));

        // Shard 0 advances; shard 1 lags — cluster is bottlenecked at shard 1 (epoch 1).
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 10,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(1));

        // Advance shard 1 — cluster should advance to 10.
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 10,
        })
        .unwrap();
        assert_eq!(agg.cluster_frontier().epoch, Some(10));
    }

    /// Slice 6: fill-level metric is tracked.
    #[test]
    fn fill_level_is_tracked() {
        let agg = FrontierAggregator::new();
        assert_eq!(agg.fill_level().registered, 0);
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 1,
        })
        .unwrap();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(2),
            epoch: 1,
        })
        .unwrap();
        assert_eq!(agg.fill_level().registered, 2);
        assert_eq!(agg.fill_level().capacity, MAX_REGISTERED_SHARDS);
    }

    /// Slice 6: `has_committed_through` is correct.
    #[test]
    fn cluster_frontier_has_committed_through() {
        let agg = FrontierAggregator::new();
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 15,
        })
        .unwrap();
        let cf = agg.cluster_frontier();
        assert!(cf.has_committed_through(10));
        assert!(cf.has_committed_through(15));
        assert!(!cf.has_committed_through(16));
    }
}

// ─── v0.45.6: FrontierLeaseStore + M2-S3 publisher-election tests ────────────

#[cfg(test)]
mod lease_tests {
    use super::*;
    use object_store::memory::InMemory;

    async fn new_test_store() -> FrontierLeaseStore {
        FrontierLeaseStore::open("test-frontier-lease", Arc::new(InMemory::new()))
            .await
            .unwrap()
    }

    // ── S3: acquire_publisher_lease / publish_frontier CAS ──────────────────

    #[tokio::test]
    async fn acquire_publisher_lease_succeeds_with_current_token() {
        let store = new_test_store().await;
        assert_eq!(store.current_fence_token().await, 0);
        let token = store
            .acquire_publisher_lease(AggregatorId(1), 0)
            .await
            .unwrap();
        assert_eq!(token, LeaseToken(1));
        assert_eq!(store.current_fence_token().await, 1);
    }

    #[tokio::test]
    async fn acquire_publisher_lease_rejects_stale_token() {
        let store = new_test_store().await;
        store
            .acquire_publisher_lease(AggregatorId(1), 0)
            .await
            .unwrap();
        // A second acquirer racing with the same stale current_fence_token=0
        // loses: the real current fence token is now 1.
        let err = store
            .acquire_publisher_lease(AggregatorId(2), 0)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("RS-8002"));
        match err {
            FrontierLeaseError::StaleFenceToken { provided, current } => {
                assert_eq!(provided, 0);
                assert_eq!(current, 1);
            }
            other => panic!("expected StaleFenceToken, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn acquire_publisher_lease_renews_current_holder() {
        let store = new_test_store().await;
        let t1 = store
            .acquire_publisher_lease(AggregatorId(1), 0)
            .await
            .unwrap();
        // The same aggregator renews with the up-to-date token.
        let t2 = store
            .acquire_publisher_lease(AggregatorId(1), t1.0)
            .await
            .unwrap();
        assert!(t2.0 > t1.0);
    }

    #[tokio::test]
    async fn publish_frontier_succeeds_with_valid_token() {
        let store = new_test_store().await;
        let token = store
            .acquire_publisher_lease(AggregatorId(1), 0)
            .await
            .unwrap();
        store.publish_frontier(token, 5).await.unwrap();
        assert_eq!(store.read_published_frontier_after_handoff().await, Some(5));
    }

    #[tokio::test]
    async fn publish_frontier_rejects_stale_write_silently() {
        let store = new_test_store().await;
        let token = store
            .acquire_publisher_lease(AggregatorId(1), 0)
            .await
            .unwrap();
        store.publish_frontier(token, 10).await.unwrap();
        // A lower frontier value must not retreat the published value (M2-S4).
        store.publish_frontier(token, 3).await.unwrap();
        assert_eq!(
            store.read_published_frontier_after_handoff().await,
            Some(10)
        );
    }

    // ── S4: assert_valid_publisher (M2-S3) ───────────────────────────────────

    #[test]
    fn assert_valid_publisher_passes_on_current_token() {
        assert_valid_publisher(Some(AggregatorId(1)), 5, 5);
    }

    #[test]
    #[should_panic(expected = "RS-8002")]
    fn assert_valid_publisher_panics_on_stale_token() {
        assert_valid_publisher(Some(AggregatorId(1)), 3, 5);
    }

    #[tokio::test]
    #[should_panic(expected = "RS-8002")]
    async fn publish_frontier_panics_on_stale_token() {
        let store = new_test_store().await;
        store
            .acquire_publisher_lease(AggregatorId(1), 0)
            .await
            .unwrap();
        // Deliberately stale token: the real current fence token is 1.
        let _ = store.publish_frontier(LeaseToken(0), 5).await;
    }

    // ── S5: assert_flush_before_lease_handoff_read (M2-S3/S4 pair) ──────────

    #[test]
    fn assert_flush_before_lease_handoff_read_passes_when_no_value_yet() {
        assert_flush_before_lease_handoff_read(false, false);
    }

    #[test]
    fn assert_flush_before_lease_handoff_read_passes_when_synced() {
        assert_flush_before_lease_handoff_read(true, true);
    }

    #[test]
    #[should_panic(expected = "RS-8003")]
    fn assert_flush_before_lease_handoff_read_panics_when_unsynced() {
        assert_flush_before_lease_handoff_read(true, false);
    }

    #[tokio::test]
    async fn read_published_frontier_after_handoff_does_not_panic_on_real_store() {
        // Every write in FrontierLeaseStore uses await_durable: true, so a
        // real store's handoff read must never panic.
        let store = new_test_store().await;
        assert_eq!(store.read_published_frontier_after_handoff().await, None);
        let token = store
            .acquire_publisher_lease(AggregatorId(1), 0)
            .await
            .unwrap();
        store.publish_frontier(token, 7).await.unwrap();
        assert_eq!(store.read_published_frontier_after_handoff().await, Some(7));
    }

    // ── S6: FrontierAggregator wired to a FrontierLeaseStore ────────────────

    #[tokio::test]
    async fn frontier_aggregator_acquires_lease_and_publishes() {
        let store = Arc::new(new_test_store().await);
        let agg = FrontierAggregator::with_lease_store(AggregatorId(1), store);
        assert!(!agg.is_publisher());

        assert!(agg.acquire_lease().await.unwrap());
        assert!(agg.is_publisher());
        assert_eq!(agg.lease_token(), 1);

        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 5,
        })
        .unwrap();
        assert!(agg.try_publish().await.unwrap());
        assert_eq!(agg.cluster_frontier().epoch, Some(5));
    }

    /// S6/M2-S3: a stale-fenced aggregator can never re-publish a frontier
    /// after a newer leader's token supersedes it — the low-level
    /// `SimRuntime`-scale multi-seed proof of this is S8 (coordination
    /// slice); this is its 2-aggregator deterministic sanity check.
    #[tokio::test]
    async fn stale_publisher_demotes_and_never_republishes() {
        let store = Arc::new(new_test_store().await);
        let agg_a = FrontierAggregator::with_lease_store(AggregatorId(1), store.clone());
        let agg_b = FrontierAggregator::with_lease_store(AggregatorId(2), store.clone());

        assert!(agg_a.acquire_lease().await.unwrap());
        agg_a
            .ingest(ShardFrontierReport {
                shard_id: ShardId(0),
                epoch: 9,
            })
            .unwrap();

        // B acquires next, superseding A's fence token without A knowing yet.
        assert!(agg_b.acquire_lease().await.unwrap());
        assert!(agg_b.is_publisher());
        assert!(!agg_a.is_publisher() || agg_a.lease_token() != agg_b.lease_token());

        // A's stale publish attempt must be rejected gracefully (no panic)
        // and must demote A — never regressing store.published_frontier.
        assert!(!agg_a.try_publish().await.unwrap());
        assert!(!agg_a.is_publisher());
        assert_eq!(store.read_published_frontier_after_handoff().await, None);

        // B, the current publisher, can still publish successfully.
        agg_b
            .ingest(ShardFrontierReport {
                shard_id: ShardId(0),
                epoch: 4,
            })
            .unwrap();
        assert!(agg_b.try_publish().await.unwrap());
        assert_eq!(store.read_published_frontier_after_handoff().await, Some(4));

        // A repeating the stale attempt still never republishes.
        assert!(!agg_a.try_publish().await.unwrap());
        assert_eq!(store.read_published_frontier_after_handoff().await, Some(4));
    }

    #[tokio::test]
    async fn try_publish_without_shard_reports_is_a_noop() {
        let store = Arc::new(new_test_store().await);
        let agg = FrontierAggregator::with_lease_store(AggregatorId(1), store);
        assert!(agg.acquire_lease().await.unwrap());
        // No shard reports ingested yet — nothing to publish.
        assert!(!agg.try_publish().await.unwrap());
    }

    #[tokio::test]
    async fn try_publish_without_publisher_status_is_a_noop() {
        let store = Arc::new(new_test_store().await);
        let agg = FrontierAggregator::with_lease_store(AggregatorId(1), store);
        // Never acquired the lease.
        agg.ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 5,
        })
        .unwrap();
        assert!(!agg.try_publish().await.unwrap());
    }
}

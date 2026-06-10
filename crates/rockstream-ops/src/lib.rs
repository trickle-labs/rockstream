//! Operator trait and per-operator implementations for RockStream.
//!
//! v0.4: Z-set types (Arrow RecordBatch with `_weight`), stateless linear
//! operators (Filter, Project, Map), the `Operator` trait + `EpochOutput`,
//! `OperatorTask` event loop, credit-based scheduler, built-in sources
//! (GENERATE ROWS, Vec-delta), ViewSink, and the embedded single-process
//! runtime profile.
//!
//! v0.5: Stateful `AggregateOp` (SUM/COUNT/AVG with DBSP delta rule),
//! `GroupCommit` for coalescing N per-operator `WriteBatch` fragments into
//! one atomic `Db::write()`, per-shard op_state and shard_meta namespaces
//! (already in storage), and persisted frontier helpers.
//!
//! v0.6: `MinMaxOp` — non-invertible MIN/MAX aggregates via indexed-multiset
//! arrangement with cached extremum.  Crash-replay proved on LFS and MinIO.

pub mod aggregate;
pub mod distinct;
pub mod embedded;
pub mod error;
pub mod expr;
pub mod filter;
pub mod group_commit;
pub mod join;
pub mod map;
pub mod minmax;
pub mod op;
pub mod outer_join;
pub mod pipeline;
pub mod project;
pub mod scheduler;
pub mod sink;
pub mod snapshot;
pub mod source;
pub mod task;
pub mod time_window;
pub mod topk;
pub mod view_ref;
pub mod window;
pub mod zset;

pub use aggregate::{load_frontier, persist_agg_state, persist_frontier, AggState, AggregateOp};
pub use distinct::{
    load_distinct_state, persist_distinct_state, DistinctOp, DualArrangement, ExceptOp, IntersectOp,
};
pub use error::OpError;
pub use filter::FilterOp;
pub use group_commit::{GroupCommit, GROUP_COMMIT_MAX_BATCHES};
pub use join::JoinOp;
pub use map::MapOp;
pub use minmax::{persist_minmax_state, MinMaxKind, MinMaxOp, MinMaxState};
pub use op::{EpochOutput, Operator};
pub use outer_join::OuterJoinOp;
pub use project::ProjectOp;
pub use scheduler::CreditScheduler;
pub use sink::ViewSinkOp;
pub use snapshot::{SnapshotOp, SNAPSHOT_BUFFER_LIMIT};
pub use source::{GenerateRowsSource, VecDeltaSource};
pub use task::OperatorTask;
pub use time_window::{
    load_tumble_window_state, load_watermark, persist_tumble_window_state, persist_watermark,
    CompactionFilter, TumbleWindowOp, WatermarkState, TUMBLE_WINDOW_STATE_LIMIT,
};
pub use topk::{load_topk_state, persist_topk_state, TopKOp, TOPK_BUFFER_LIMIT};
pub use view_ref::{ViewRefOp, VIEW_REF_SCAN_LIMIT_BYTES};
pub use window::{load_window_state, persist_window_state, WindowOp, WINDOW_PARTITION_THRESHOLD};
pub use zset::ArrowZSet;

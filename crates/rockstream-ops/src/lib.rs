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
pub mod bench_regression;
pub mod compile;
pub mod debugger;
pub mod distinct;
pub mod embedded;
pub mod error;
pub mod expr;
pub mod factorized;
pub mod filter;
pub mod governor;
pub mod group_commit;
pub mod index_arrange;
pub mod join;
pub mod lateral;
pub mod live_exec;
pub mod map;
pub mod minmax;
pub mod nexmark_regression;
pub mod op;
pub mod outer_join;
pub mod pipeline;
pub mod project;
pub mod recursion;
pub mod scheduler;
pub mod shared_window;
pub mod sink;
pub mod snapshot;
pub mod source;
pub mod spill;
pub mod task;
pub mod tco;
pub mod time_window;
pub mod topk;
pub mod view_attach;
pub mod view_ref;
pub mod window;
pub mod zset;

pub use view_attach::{AttachedView, AttachmentDeltaBuffer, ViewAttachmentMetrics};

pub use spill::{SerdeSpill, SpillKey, SpillValue, SpillableArrangement};

pub use aggregate::{
    load_frontier, persist_agg_state, persist_bucketed_agg_state, persist_frontier, AggState,
    AggregateOp, BucketedAggregateOp,
};
pub use compile::{
    compile_plan, compile_plan_with_sink_id, compile_plan_with_sink_id_and_strategy,
    compile_plan_with_strategy, CompiledView,
};
pub use debugger::{
    decode_user_key, explain_view_op_ids, format_explain_op_ids, inspect_arrangement_db,
    inspect_arrangement_reader, ArrangementDebugResult, DecodedArrangementKey, OperatorNodeInfo,
};
pub use distinct::{
    load_distinct_state, persist_distinct_state, DistinctOp, DualArrangement, ExceptOp, IntersectOp,
};
pub use error::OpError;
pub use factorized::{
    FactorizedAggregateKind, FactorizedJoinAggregateOp, FactorizedStarJoinOp,
    MAX_FACTOR_PAYLOAD_BYTES, MAX_FACTOR_PAYLOAD_ROWS,
};
pub use filter::FilterOp;
pub use governor::{
    AmplificationDimension, DeltaAmplificationBudget, DeltaAmplificationCounters,
    DeltaAmplificationGovernor, PlanStrategy, DEFAULT_FACTORIZED_DELTA_BUDGET,
    FACTORIZED_SELECTION_RULE_VERSION,
};
pub use group_commit::{GroupCommit, GROUP_COMMIT_MAX_BATCHES};
pub use join::JoinOp;
pub use lateral::LateralOp;
pub use live_exec::{
    int64_schema, next_stateful_op_id, GroupKeyPacker, JoinKind, JoinPipeline, Stage,
    StatefulPipeline,
};
pub use map::MapOp;
pub use minmax::{persist_minmax_state, MinMaxKind, MinMaxOp, MinMaxState};
pub use op::{EpochOutput, Operator, OperatorEpochResult};
pub use outer_join::OuterJoinOp;
pub use pipeline::{LinearPipeline, StageTimestampTracker};
pub use project::ProjectOp;
pub use recursion::{
    load_recursion_state, persist_recursion_state, DistributedShardStatus, RecursionOp,
    RecursionStrategy, RECURSION_STATE_LIMIT,
};
pub use scheduler::CreditScheduler;
pub use shared_window::{
    SharedWindowError, SharedWindowFabric, SharedWindowFillLevel, MAX_SHARED_WINDOW_CONSUMERS,
    MAX_SHARED_WINDOW_QUERY_SLICES, MAX_SHARED_WINDOW_SLICES,
};
pub use sink::{read_view_output, ColumnValue, ViewSinkOp};
pub use snapshot::{SnapshotOp, SNAPSHOT_BUFFER_LIMIT};
pub use source::{GenerateRowsSource, VecDeltaSource};
pub use task::OperatorTask;
pub use time_window::{
    load_hop_window_state, load_session_window_state, load_tumble_window_state, load_watermark,
    persist_hop_window_state, persist_session_window_state, persist_tumble_window_state,
    persist_watermark, CompactionFilter, HopWindowOp, SessionWindowOp, TumbleWindowOp,
    WatermarkState, HOP_WINDOW_STATE_LIMIT, SESSION_WINDOW_STATE_LIMIT, TUMBLE_WINDOW_STATE_LIMIT,
};
pub use topk::{load_topk_state, persist_topk_state, TopKOp, TOPK_BUFFER_LIMIT};
pub use view_ref::{ViewRefOp, VIEW_REF_SCAN_LIMIT_BYTES};
pub use window::{load_window_state, persist_window_state, WindowOp, WINDOW_PARTITION_THRESHOLD};
pub use zset::ArrowZSet;

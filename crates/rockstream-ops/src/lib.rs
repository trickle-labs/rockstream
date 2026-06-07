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

pub mod aggregate;
pub mod embedded;
pub mod error;
pub mod expr;
pub mod filter;
pub mod group_commit;
pub mod map;
pub mod op;
pub mod pipeline;
pub mod project;
pub mod scheduler;
pub mod sink;
pub mod source;
pub mod task;
pub mod zset;

pub use aggregate::{load_frontier, persist_agg_state, persist_frontier, AggState, AggregateOp};
pub use error::OpError;
pub use filter::FilterOp;
pub use group_commit::{GroupCommit, GROUP_COMMIT_MAX_BATCHES};
pub use map::MapOp;
pub use op::{EpochOutput, Operator};
pub use project::ProjectOp;
pub use scheduler::CreditScheduler;
pub use sink::ViewSinkOp;
pub use source::{GenerateRowsSource, VecDeltaSource};
pub use task::OperatorTask;
pub use zset::ArrowZSet;

//! Operator trait and per-operator implementations for RockStream.
//!
//! v0.4: Z-set types (Arrow RecordBatch with `_weight`), stateless linear
//! operators (Filter, Project, Map), the `Operator` trait + `EpochOutput`,
//! `OperatorTask` event loop, credit-based scheduler, built-in sources
//! (GENERATE ROWS, Vec-delta), ViewSink, and the embedded single-process
//! runtime profile.

pub mod embedded;
pub mod error;
pub mod expr;
pub mod filter;
pub mod map;
pub mod op;
pub mod pipeline;
pub mod project;
pub mod scheduler;
pub mod sink;
pub mod source;
pub mod task;
pub mod zset;

pub use error::OpError;
pub use filter::FilterOp;
pub use map::MapOp;
pub use op::{EpochOutput, Operator};
pub use project::ProjectOp;
pub use scheduler::CreditScheduler;
pub use sink::ViewSinkOp;
pub use source::{GenerateRowsSource, VecDeltaSource};
pub use task::OperatorTask;
pub use zset::ArrowZSet;

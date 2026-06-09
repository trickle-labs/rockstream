//! PlanNode IR and physical OpNode graph for RockStream.
//!
//! Defines the `PlanNode` enum (the logical PlanIR from IVM.md §5) and the
//! physical `OpNode` graph used for execution. Each `PlanNode` carries enough
//! metadata for the planner to attach merge-law annotations.

pub mod dag;
pub mod explain;

use rockstream_types::ids::OperatorId;
use rockstream_types::merge_law::MergeLawId;
use serde::{Deserialize, Serialize};

/// Re-exported for backward compatibility — `NotMergeSafeReason` is now
/// defined in `rockstream_types::explain`.
pub use rockstream_types::explain::NotMergeSafeReason;

/// A logical plan node in the IVM plan IR.
///
/// The plan IR is a tree of declarative operations that describe *what* to
/// compute. The `DiffCtx` pass transforms this into a physical `OpNode` graph
/// that describes *how* to compute it incrementally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanNode {
    /// Read from a named source.
    Source { name: String },
    /// Filter rows by a predicate expression.
    Filter {
        input: Box<PlanNode>,
        predicate: Expr,
    },
    /// Project (select) columns.
    Project {
        input: Box<PlanNode>,
        columns: Vec<Expr>,
    },
    /// Apply a scalar function to each row (map).
    Map { input: Box<PlanNode>, func: Expr },
    /// Aggregate with group-by keys.
    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
    },
    /// Inner join on a condition (deprecated — use InnerJoin instead).
    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: Expr,
    },
    /// Inner equi-join with pre-computed key columns and dual arrangements (v0.8 — IVM-4).
    ///
    /// The join uses two separate arrangements (left_arr_id, right_arr_id) to
    /// efficiently probe matching rows. The join key is computed from the column
    /// indices in `left_keys` and `right_keys`.
    ///
    /// `semantics` carries metadata for specialized join handling (e.g. semijoins,
    /// TPC-H specific optimizations).
    InnerJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        /// Column indices in the left input forming the join key.
        left_keys: Vec<usize>,
        /// Column indices in the right input forming the join key.
        right_keys: Vec<usize>,
        /// Operator ID for the left arrangement.
        left_arr_id: OperatorId,
        /// Operator ID for the right arrangement.
        right_arr_id: OperatorId,
        /// Join semantics and metadata.
        semantics: JoinSemantics,
    },
    /// Outer / semi / anti equi-join with dual arrangements and unmatched tracking (v0.9 — IVM-5).
    ///
    /// Uses the same dual arrangements as `InnerJoin` plus an `unmatched_arr_id` arrangement
    /// tracking per-key match counts for correct NULL-padding retractions.
    ///
    /// For `Left`, `Full`, `Semi`, `Anti`: tracks right-side match counts per left-key.
    /// For `Right`, `Full`: also tracks left-side match counts per right-key.
    OuterJoin {
        kind: OuterJoinKind,
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_arr_id: OperatorId,
        right_arr_id: OperatorId,
        /// Arrangement ID for the unmatched-row tracking state.
        unmatched_arr_id: OperatorId,
    },
    /// Union of two inputs.
    Union {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
    },
    /// Window functions (v0.19).
    Window {
        input: Box<PlanNode>,
        window_exprs: Vec<WindowExpr>,
    },
    /// Tumbling time-window operator (v0.20).
    ///
    /// Groups rows into fixed-size, non-overlapping time windows of
    /// `window_size_ms` milliseconds. Windows close when the event-time
    /// watermark advances past the window end. `time_col` is the index of
    /// the column containing the event timestamp (i64 milliseconds, big-endian
    /// 8 bytes). Late rows (arriving after window close) are handled per
    /// `late_data_policy`. Watermark advances are tracked via `MaxRegister/v1`
    /// (semilattice, idempotent).
    TumbleWindow {
        input: Box<PlanNode>,
        /// Column index holding the event timestamp (i64 ms, BE 8 bytes).
        time_col: usize,
        /// Duration of each tumbling window in milliseconds.
        window_size_ms: i64,
        /// Policy for rows that arrive after the window has closed.
        late_data_policy: LateDataPolicy,
    },
    /// Top-K operator (v0.21).
    ///
    /// Maintains the top-`k` rows ranked by the column at `rank_col` (i64
    /// big-endian; descending: highest value = rank 1).  Optional
    /// `partition_by` columns create independent Top-K groups.
    ///
    /// Internally tracks `k + epsilon` rows as a buffer (epsilon = k) to
    /// handle the delete-refill path without a full state rescan.  When a
    /// ranked row is deleted the next-best row from the buffer fills its
    /// slot.  When a new row outranks the current k-th row a delta swap
    /// is emitted (retract the displaced row, insert the new row).
    TopK {
        input: Box<PlanNode>,
        /// Number of top rows to maintain per partition.
        k: usize,
        /// Column index to rank by (i64 BE; descending: highest = rank 1).
        rank_col: usize,
        /// Column indices to partition by.  Empty = single global partition.
        partition_by: Vec<usize>,
    },
    /// Recursive operator (v0.22).
    ///
    /// Computes the fixed-point of iterating `step` starting from `base`.
    /// Semi-naive evaluation: each iteration feeds only the new delta rows
    /// into `step`, not the entire accumulated relation.
    ///
    /// Convergence is detected when the iteration produces no new rows.
    /// `max_iterations` is a safety cap (typically 1024) that prevents
    /// infinite loops on non-convergent non-monotone queries.
    ///
    /// When `monotone = true`, the relation is insert-only: retractions are
    /// rejected at runtime with `RS-1509`.  Monotone recursion publishes a
    /// `complete_through` token once convergence is reached, enabling
    /// downstream operators to consume partial-progress results safely.
    ///
    /// When `monotone = false`, the DRed (Delete and Re-derive) strategy is
    /// required.  If DRed proves unsound under concurrent deletes the escape
    /// hatch is active: non-monotone deltas are rejected with `RS-1509` and
    /// the operator reports `not_merge_safe_reason=recursion_dred_required`.
    Recursion {
        /// The base relation providing the initial facts.
        base: Box<PlanNode>,
        /// The step relation: maps the current accumulated output back as
        /// input to derive new rows.  Typically a self-join or filter.
        step: Box<PlanNode>,
        /// Safety cap on the number of semi-naive iterations per epoch.
        max_iterations: usize,
        /// Whether this recursion is restricted to monotone (insert-only) terms.
        monotone: bool,
    },
    /// Snapshot source operator (v0.23).
    ///
    /// Delivers an existing relation as a sequence of insert-only bootstrap
    /// epochs.  Each epoch emits at most `batch_size` rows as positive-weight
    /// Z-set entries.  Once all rows have been delivered the operator reports
    /// `is_complete()` (the bootstrap frontier is reached).
    ///
    /// On connector position loss the caller may invoke `resume_from(N)` to
    /// skip the first `N` already-committed rows and re-deliver from row `N`
    /// onwards without duplication.
    Snapshot {
        /// Name of the source relation being snapshotted.
        source_name: String,
        /// Maximum number of rows to emit per bootstrap epoch.
        batch_size: usize,
    },
    /// View-reference operator (v0.24).
    ///
    /// Reads from an existing named materialized view, treating it as an
    /// upstream CDC source.  The downstream view inherits the upstream view's
    /// epoch cadence via topological scheduling (cadence inheritance):
    /// the downstream epoch N completes only after the upstream has completed
    /// epoch N.
    ///
    /// Diamond consistency is guaranteed structurally by the frontier meet:
    /// when two paths through the view DAG both feed into a common downstream
    /// view, that view advances only when all upstream paths have completed
    /// the same epoch.
    ///
    /// Cycles in the view DAG are rejected at compile time by
    /// [`rockstream_plan::dag::detect_cycle`], which returns `RS-1011`.
    ViewRef {
        /// Name of the upstream materialized view.
        view_name: String,
    },
    /// Lateral / set-returning function operator (v0.25).
    ///
    /// Applies a set-returning function (SRF) to each row of the input,
    /// producing zero or more output rows per input row (row-scoped
    /// recomputation).  This is the basis for `UNNEST`, `GENERATE_SERIES`,
    /// and JSON array expansion.
    ///
    /// The operator is stateless: it produces output rows deterministically
    /// from each input row with no arrangement.  Delta maintenance is
    /// correct because the SRF is applied independently per input row;
    /// a retracted input row retracts exactly the rows it produced.
    Lateral {
        /// The input plan whose rows are expanded by the SRF.
        input: Box<PlanNode>,
        /// The set-returning function to apply to each input row.
        func: LateralFunc,
    },

    // ── v0.4 additions ─────────────────────────────────────────────────────
    /// Materialize the input stream into a named view in shard storage (v0.4).
    ///
    /// Writes each output Z-set batch into the `view_output` namespace of the
    /// shard's `ShardDb`.  The `pk` field lists the column indices that form
    /// the view's primary key for upsert/retract keying.
    ViewSink {
        /// Name of the view being materialized.
        view_name: String,
        /// Primary-key column indices (used as the row identity key in storage).
        pk: Vec<usize>,
        /// Input plan producing the Z-set deltas to materialize.
        child: Box<PlanNode>,
    },

    /// Exchange operator stub (v0.4).
    ///
    /// In the embedded single-process runtime (v0.4), Exchange is always
    /// `Loopback`: data passes through without any network call or shuffle
    /// object.  Full hash/broadcast/range exchange over gRPC + durable
    /// shuffle objects is implemented in v0.16.
    Exchange {
        /// How this exchange partitions data.
        kind: ExchangeKind,
        /// The input plan to re-partition.
        child: Box<PlanNode>,
    },
}

/// How an Exchange operator partitions data across shards (v0.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeKind {
    /// Single-process loopback — no network, no shuffle objects.
    /// This is the only mode active in v0.4's embedded runtime.
    Loopback,
    /// Hash-partition by the plan's declared `partition_key`.
    Hash,
    /// Broadcast to every downstream shard.
    Broadcast,
    /// Route all data to a single shard (gather).
    Single,
    /// Range-partition by the declared `partition_key`.
    Range,
}

/// Join kind for outer, semi, and anti joins (v0.9 — IVM-5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OuterJoinKind {
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// Join semantics metadata for specialized join handling (v0.8 — IVM-4).
///
/// Carries metadata used for TPC-H optimizations and semi-join detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JoinSemantics {
    /// True if this join is nested inside a semi-join (e.g. TPC-H EC-01).
    pub inside_semijoin: bool,
    /// True if this join is a child of another join in the query tree.
    pub is_join_child: bool,
    /// True if the right side's old arrangement must be materialized (e.g. TPC-H Q07/Q21).
    pub r_old_materialize: bool,
}

/// Policy for late-arriving rows in time-window operators.
///
/// A row is "late" if its event timestamp falls within a window that has
/// already been closed by the watermark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LateDataPolicy {
    /// Silently discard the late row. The closed window output is unchanged.
    Drop,
    /// Retract the previous window output and re-emit including the late row.
    Update,
    /// Route the late row to a named side-channel sink.
    RouteToSink { sink_name: String },
}

/// A scalar expression in the plan IR.
///
/// This is intentionally minimal for v0.5. DataFusion expression integration
/// comes in Phase 2 (v0.11+).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Expr {
    /// A column reference by index.
    Column(usize),
    /// A literal value (stored as bytes for generality).
    Literal(Vec<u8>),
    /// A binary operation.
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// A scalar user-defined function call (v0.25).
    ///
    /// Represents a call to a named scalar UDF with the given argument
    /// expressions.  Scalar UDFs are stateless: they map each input row
    /// to a single output value with no arrangement.  The `DiffCtx` treats
    /// a `ScalarUdf` expression as stateless (same as a map/project node).
    ///
    /// Full UDF registration and type resolution is deferred to v0.26+.
    /// This variant exists to sketch the UDF hook surface and allow the
    /// UDAF annotation slot (see `UdafSpec`) to be anchored in the IR.
    ScalarUdf {
        /// Registered name of the scalar UDF.
        name: String,
        /// Argument expressions evaluated from the input row.
        args: Vec<Expr>,
    },
}

/// Binary operators for expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    And,
    Or,
}

/// An aggregate function expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateExpr {
    /// The aggregate function.
    pub func: AggregateFunc,
    /// The input expression to aggregate.
    pub input: Expr,
    /// Whether this is a DISTINCT aggregate.
    pub distinct: bool,
}

/// Built-in aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// Approximate distinct-value count using `HyperLogLog/v1` (v0.25).
    ///
    /// Returns a probabilistic estimate of the number of distinct values in
    /// the input column.  Typical error rate: ±3% with 64 HLL registers.
    /// The merge law is `HyperLogLog/v1` (semilattice, non-invertible);
    /// retraction-aware correctness requires `ExtremumRequiresRmw`.
    ApproxCountDistinct,
    /// Approximate membership test using `BloomUnion/v1` (v0.25).
    ///
    /// Accumulates input values into a 256-bit Bloom filter sketch.
    /// Membership queries are answered with a bounded false-positive rate.
    /// The merge law is `BloomUnion/v1` (semilattice, non-invertible);
    /// retraction-aware correctness requires `ExtremumRequiresRmw`.
    ApproxMembership,
}

/// Set-returning function (SRF) for the `Lateral` plan node (v0.25).
///
/// Each variant describes a function that maps one input row to zero or more
/// output rows.  The function is evaluated independently for each input row;
/// a retracted input row retracts exactly the rows it produced.
///
/// These are the "JSON/unnest/generate_series style" functions from the v0.25
/// scope.  More SRF variants will be added as the SQL surface grows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LateralFunc {
    /// `UNNEST(col)` — expands a column containing an array-like value
    /// (encoded as a sequence of length-prefixed entries) into one output
    /// row per element.
    ///
    /// `col` is the 0-based index of the array column in the input row.
    Unnest {
        /// Column index of the array/list column to expand.
        col: usize,
    },
    /// `GENERATE_SERIES(start, stop, step)` — emits one output row per value
    /// in the arithmetic sequence `start, start+step, …, stop`.
    ///
    /// If `step` is positive and `start > stop`, or `step` is negative and
    /// `start < stop`, the function produces no output rows.
    GenerateSeries {
        /// First value of the series (inclusive).
        start: i64,
        /// Last value of the series (inclusive).
        stop: i64,
        /// Step between values (must be non-zero).
        step: i64,
    },
    /// `JSON_EXTRACT_ARRAY(col)` — extracts a JSON array from a column and
    /// emits one output row per JSON element.
    ///
    /// Input column is expected to contain a JSON array encoded as UTF-8
    /// bytes.  Elements are emitted as raw JSON bytes in the output column.
    JsonExtractArray {
        /// Column index of the JSON column to expand.
        col: usize,
    },
}

/// UDAF (User-Defined Aggregate Function) interface specification (v0.25).
///
/// Documents the requirements that any user-defined aggregate function must
/// satisfy to be registered in the law catalog.  This sketch is the
/// "UDAF requirements documented before implementation" deliverable from the
/// v0.25 scope.
///
/// Full UDAF registration (DDL, runtime dispatch, law annotation slot) is
/// planned for v0.51+ (`CREATE MERGE LAW`).
///
/// # Algebraic requirements
///
/// For a UDAF to be used with a `MergeLaw` annotation it must satisfy:
/// - **Associativity**: `agg(agg(a, b), c) = agg(a, agg(b, c))`
/// - **Commutativity**: `agg(a, b) = agg(b, a)`
/// - **Identity element**: There exists an empty state `e` such that
///   `agg(e, a) = a` for all `a`.
/// - **Invertibility** (optional, for abelian-group law): There exists an
///   inverse operation `inv(a)` such that `agg(a, inv(a)) = e`.
///
/// If invertibility is not provided, the UDAF must be annotated with a
/// `NotMergeSafeReason` explaining the limitation.
///
/// # Wire format contract
///
/// The UDAF state must be serializable to a fixed-format byte buffer.
/// The identity state must be representable as a fixed-size all-zero buffer
/// for the storage layer to use as a compaction tombstone.
///
/// # Annotation slot
///
/// When a UDAF is registered with a `MergeLawId`, the planner attaches that
/// law ID to every `Aggregate` node using this UDAF.  The `EXPLAIN
/// INCREMENTAL` output will show `merge_law=<UdafName>/v<n>`.
///
/// If no `MergeLawId` is provided, the node is annotated with
/// `not_merge_safe_reason=unknown_udaf_properties`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UdafSpec {
    /// Registered name of the UDAF (e.g. "my_sum").
    pub name: String,
    /// Whether this UDAF satisfies the associativity + commutativity +
    /// identity requirements for a `CommutativeMonoid` merge law.
    pub is_commutative_monoid: bool,
    /// Whether this UDAF is idempotent: `agg(a, a) = a`.
    pub is_idempotent: bool,
    /// Whether this UDAF provides an inverse operation (abelian group).
    pub has_inverse: bool,
    /// Human-readable description of the merge semantics.
    pub description: String,
}

/// A window function expression (v0.19).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowExpr {
    pub func: WindowFunc,
    pub partition_by: Vec<usize>,
    pub order_by: Vec<usize>,
}

/// Window functions supported in v0.19.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Ntile(u64),
    Lag { offset: usize },
    Lead { offset: usize },
    SlidingSum { frame_rows: usize },
    SlidingAvg { frame_rows: usize },
}

/// Window operator IVM strategy (v0.19).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowStrategy {
    PartitionRecompute,
    SlidingAggregate,
}

/// A physical operator node in the execution graph.
///
/// Each `OpNode` corresponds to a running operator instance with a unique
/// `OperatorId`, an optional merge-law annotation, and references to its
/// input(s).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpNode {
    /// Unique operator ID within the pipeline.
    pub id: OperatorId,
    /// The kind of physical operator.
    pub kind: OpKind,
    /// Merge law used by this operator's arrangement (if any).
    pub merge_law: Option<MergeLawId>,
    /// Why this operator is NOT merge-safe (if applicable).
    /// From the closed `NotMergeSafeReason` enum.
    pub not_merge_safe_reason: Option<NotMergeSafeReason>,
    /// Input operator IDs.
    pub inputs: Vec<OperatorId>,
}

/// Physical operator kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    /// Read from source.
    Source { name: String },
    /// Stateless filter.
    Filter,
    /// Stateless projection.
    Project,
    /// Stateless map.
    Map,
    /// Stateful aggregate with an arrangement.
    Aggregate,
    /// Stateful join with dual arrangements.
    Join,
    /// Outer / semi / anti equi-join (v0.9 — IVM-5).
    OuterJoin {
        kind: OuterJoinKind,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
    },
    /// Stateless union.
    Union,
    /// Emit to output sink.
    Sink { name: String },
    /// Window function operator (v0.19).
    Window { strategy: WindowStrategy },
    /// Tumbling time-window operator (v0.20).
    ///
    /// Watermark state tracked via `MaxRegister/v1`; output emitted once per
    /// window when the watermark advances past the window end.
    TumbleWindow {
        window_size_ms: i64,
        late_data_policy: LateDataPolicy,
    },
    /// Top-K operator (v0.21).
    ///
    /// Emits the top-`k` rows per partition ranked by `rank_col` (i64 BE;
    /// descending).  Delta swaps are emitted when the ranked set changes.
    /// The `k + epsilon` buffer (epsilon = k) backs the delete-refill path.
    TopK {
        k: usize,
        rank_col: usize,
        partition_by: Vec<usize>,
    },
    /// Recursive operator (v0.22).
    ///
    /// Runs semi-naive fixed-point iteration until convergence or
    /// `max_iterations` is reached.  `monotone = true` enables
    /// `complete_through` partial-progress publication; `monotone = false`
    /// activates the DRed escape hatch (`not_merge_safe_reason =
    /// recursion_dred_required`) and rejects retractions at runtime.
    Recursion {
        /// Safety cap on semi-naive iterations per epoch.
        max_iterations: usize,
        /// Whether this operator is restricted to monotone (insert-only) terms.
        monotone: bool,
    },
    /// Snapshot source operator (v0.23).
    ///
    /// Emits a pre-existing relation row-by-row in insert-only bootstrap
    /// epochs.  `batch_size` controls the maximum rows per epoch.  Once all
    /// rows have been emitted the bootstrap frontier is reached.
    Snapshot {
        /// Name of the source relation being snapshotted.
        source_name: String,
        /// Maximum rows per bootstrap epoch.
        batch_size: usize,
    },
    /// View-reference operator (v0.24).
    ///
    /// Reads CDC output from an upstream materialized view.  This is
    /// structurally identical to a `Source` at the physical level — the
    /// upstream view pushes deltas to this operator each epoch.  Cadence
    /// inheritance and frontier meet are handled by the scheduler.
    ViewRef {
        /// Name of the upstream materialized view.
        view_name: String,
    },
    /// Lateral / set-returning function operator (v0.25).
    ///
    /// Applies a `LateralFunc` to each input row, producing zero or more
    /// output rows.  The operator is stateless: no arrangement is maintained.
    /// A retracted input row retracts exactly the output rows it produced.
    Lateral {
        /// The set-returning function applied to each input row.
        func: LateralFunc,
    },

    // ── v0.4 additions ─────────────────────────────────────────────────────
    /// View-sink operator: writes Z-set deltas to shard view_output (v0.4).
    ViewSink {
        /// Name of the materialized view.
        view_name: String,
        /// Primary-key column indices for row identity in storage.
        pk: Vec<usize>,
    },

    /// Exchange operator stub (v0.4).
    ///
    /// In v0.4's embedded runtime, always `Loopback` — passes data through
    /// with zero network calls and zero shuffle objects.
    Exchange {
        /// Partitioning strategy for this exchange.
        kind: ExchangeKind,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_node_source() {
        let plan = PlanNode::Source {
            name: "orders".into(),
        };
        assert!(matches!(plan, PlanNode::Source { .. }));
    }

    #[test]
    fn plan_node_filter_project() {
        let src = PlanNode::Source {
            name: "events".into(),
        };
        let filtered = PlanNode::Filter {
            input: Box::new(src),
            predicate: Expr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(Expr::Column(0)),
                right: Box::new(Expr::Literal(vec![0, 0, 0, 42])),
            },
        };
        let projected = PlanNode::Project {
            input: Box::new(filtered),
            columns: vec![Expr::Column(0), Expr::Column(1)],
        };
        assert!(matches!(projected, PlanNode::Project { .. }));
    }

    #[test]
    fn op_node_with_merge_law() {
        use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;

        let node = OpNode {
            id: OperatorId(1),
            kind: OpKind::Aggregate,
            merge_law: Some(WEIGHT_ADD_ID),
            not_merge_safe_reason: None,
            inputs: vec![OperatorId(0)],
        };
        assert_eq!(node.merge_law, Some(WEIGHT_ADD_ID));
    }

    #[test]
    fn not_merge_safe_reason_strings() {
        assert_eq!(
            NotMergeSafeReason::ExtremumRequiresRmw.as_str(),
            "extremum_requires_rmw"
        );
        assert_eq!(NotMergeSafeReason::ClampNotALaw.as_str(), "clamp_not_a_law");
    }
}

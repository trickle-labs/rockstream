# RockStream M2 Multi-Source Vector Frontier Model (FizzBee Verification Findings)

**Date**: 2026-06-15  
**Specification File**: [`formal/m2_frontier_agg.fizz`](file:///Users/grove/projects/rockstream/formal/m2_frontier_agg.fizz)  
**Status**: PASSED ✅  

## 1. Overview of Verification

The M2 frontier aggregation protocol has been generalized to model vector-antichain meet progress tracking (`FreshnessToken`) instead of a scalar epoch. In this model, progress is tracked as a vector of length `NUM_SOURCES = 2` across `NUM_SHARDS = 2` with `MAX_EPOCH = 1`.

The model checks the following safety and liveness invariants:
- **`M2_S1_MeetCorrectness` / `M2_S2_PessimisticStaleness`**: The published cluster frontier is bounded by the vector meet (element-wise minimum) of the true per-shard frontiers.
- **`M2_S3_SinglePublisherSafety`**: At most one aggregator publisher acts as the leader-writer using fencing tokens.
- **`M2_S4_StaleWriteRejection`**: The published cluster frontier only advances monotonically (i.e. element-wise non-decreasing).
- **`M2_L1_PublicationProgress` / `M2_L2_FailoverProgress`**: Under fairness guarantees, the cluster eventually publishes the target vector frontier `[1, 1]`.
- **`COV_M2`**: The cover check confirms that aggregator failovers and fencing occurrences are fully explored.

## 2. Model Checking Results

The FizzBee model checker was run headlessly over the spec:
- **Total Explored Nodes**: 251,889
- **Unique States**: 251,889
- **Model Check Duration**: ~1m 10s
- **Liveness Verification**: Checked and validated (PASSED)
- **Symmetry and Optimization**: State space optimized using a network buffer map of size `NUM_SHARDS` rather than unbounded message lists to collapse network-reordering permutations.

## 3. Regression / SimRuntime Pairings

The invariants proven in FizzBee map directly to assertions in `crates/rockstream-types/src/frontier.rs` and operator tasks in `crates/rockstream-ops/src/task.rs` to enforce:
1. Vector-meet lattice properties (commutativity, associativity, monotonicity).
2. Monotone advancement of operator input frontiers.

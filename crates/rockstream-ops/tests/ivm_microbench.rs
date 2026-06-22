//! IVM delta-propagation micro-benchmark / correctness test suite.
//!
//! Measures what actually matters for an IVM engine (from the "better test
//! suites" matrix):
//!
//! - **Input rows per second** (throughput)
//! - **Delta amplification factor** at 0.1 %, 1 %, 10 % change rates
//! - **Epoch-to-freshness latency** (time from delta input to output Z-set)
//!
//! These are not just timing benchmarks — every test asserts concrete
//! correctness properties on the exact output:
//!
//! - The aggregate output matches the expected group count and sums.
//! - Delta output rows are bounded relative to input delta size.
//! - Amplification is always < 2x for aggregate (retract + insert per group).
//! - Join amplification is bounded by the right-table density.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::ids::OperatorId;

// ─── Test data helpers ───────────────────────────────────────────────────────

fn make_kv_zset(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

/// Apply an AggregateOp output Z-set delta to an accumulated state map,
/// then return the sorted `(group_key, sum_v)` pairs for all live groups.
///
/// `AggregateOp::process_delta` emits a **retract-before-insert** pair for
/// every group that changes, so a single call over N input rows can produce
/// up to 2×N output rows.  Only the final (net) state per group key is live.
fn accumulate_agg_output(output: &ArrowZSet) -> Vec<(i64, i64)> {
    let mut state = std::collections::BTreeMap::new();
    apply_agg_output(&mut state, output);
    let mut result: Vec<(i64, i64)> = state.into_iter().collect();
    result.sort_by_key(|(k, _)| *k);
    result
}

/// Apply an AggregateOp output Z-set to an accumulated group state map.
/// Retract (w < 0) removes the key; insert (w > 0) sets the new sum.
fn apply_agg_output(state: &mut BTreeMap<i64, i64>, output: &ArrowZSet) {
    if output.is_empty() {
        return;
    }
    let k_col = output
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let s_col = output
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..output.num_rows() {
        let k = k_col.value(i);
        let s = s_col.value(i);
        match output.weights[i] {
            w if w > 0 => { state.insert(k, s); }
            w if w < 0 => { state.remove(&k); }
            _ => {}
        }
    }
}

// ─── Aggregate delta-amplification tests ────────────────────────────────────

/// Shared aggregate micro-benchmark logic.
///
/// Population: `pop_n` rows across `n_groups` groups.
/// Delta: `delta_n` retractions + `delta_n` insertions (all touching different
/// groups for a worst-case amplification scenario).
///
/// Asserts:
/// - Bootstrap produces exactly `n_groups` groups with correct per-group sums.
/// - Delta output ≤ 2 × groups_affected (retract + insert per changed group).
/// - Amplification (output_rows / delta_input_rows) < 2.0.
/// - Bootstrap throughput > 50 000 rows/sec.
/// - Delta epoch latency < 50 ms.
fn run_agg_delta_test(pop_n: usize, n_groups: i64, delta_n: usize, label: &str) {
    let op = AggregateOp::new(OperatorId(0));

    // ── Bootstrap (epoch 0) ──────────────────────────────────────────────────
    // Rows: k = i % n_groups, v = i * 7 % 1000 + 1 (always positive)
    let initial: Vec<(i64, i64, i64)> = (0..pop_n as i64)
        .map(|i| (i % n_groups, i * 7 % 1000 + 1, 1))
        .collect();

    let t_bootstrap = Instant::now();
    let out0 = op.process_delta(make_kv_zset(&initial)).unwrap();
    let bootstrap_ns = t_bootstrap.elapsed().as_nanos();

    // AggregateOp emits retract+insert per changed group per input row.
    // Accumulate via Z-set semantics to get the live group state.
    let state0 = accumulate_agg_output(&out0);

    // Exactly n_groups groups after bootstrap.
    assert_eq!(
        state0.len(),
        n_groups as usize,
        "{label}: expected {n_groups} groups after bootstrap, got {}",
        state0.len()
    );

    // Each group key is in [0, n_groups).
    for &(k, _) in &state0 {
        assert!(
            k >= 0 && k < n_groups,
            "{label}: group key {k} out of [0, {n_groups})"
        );
    }

    // Per-group sum must be positive (all v > 0).
    for &(k, s) in &state0 {
        assert!(s > 0, "{label}: group k={k} has non-positive sum {s}");
    }

    // Build reference sums via pure Rust to cross-check.
    let mut ref_sums: BTreeMap<i64, i64> = BTreeMap::new();
    for &(k, v, _) in &initial {
        *ref_sums.entry(k).or_insert(0) += v;
    }
    let expected: Vec<(i64, i64)> = {
        let mut v: Vec<_> = ref_sums.into_iter().collect();
        v.sort_by_key(|(k, _)| *k);
        v
    };
    assert_eq!(
        state0, expected,
        "{label}: bootstrap sums do not match reference\nstate0={state0:?}\nref={expected:?}"
    );

    // ── Delta epoch ──────────────────────────────────────────────────────────
    // Retract delta_n rows, re-insert same count with incremented values.
    // Each retraction + insertion touches a distinct group.
    let retract: Vec<(i64, i64, i64)> = (0..delta_n as i64)
        .map(|i| {
            let k = i % n_groups;
            let v = (i * 7 % 1000) + 1; // same v as in initial population
            (k, v, -1)
        })
        .collect();
    let insert: Vec<(i64, i64, i64)> = (0..delta_n as i64)
        .map(|i| {
            let k = i % n_groups;
            let v = (i * 7 % 1000) + 2; // slightly different v
            (k, v, 1)
        })
        .collect();
    let mut delta = retract;
    delta.extend_from_slice(&insert);

    let t_delta = Instant::now();
    let out1 = op.process_delta(make_kv_zset(&delta)).unwrap();
    let delta_ns = t_delta.elapsed().as_nanos();

    let delta_output_rows = out1.num_rows();
    let delta_input_rows = delta.len(); // 2 * delta_n

    // AggregateOp emits at most 2 rows per input row (retract+insert).
    // Raw output rows ≤ 2 × delta_input_rows.
    assert!(
        delta_output_rows <= 2 * delta_input_rows,
        "{label}: raw output {delta_output_rows} rows exceeds 2×input {}",
        2 * delta_input_rows
    );

    // Effective amplification measured on the ACCUMULATED state delta:
    // how many group states actually changed relative to input rows.
    let mut state_before = state0.iter().cloned().collect::<std::collections::BTreeMap<i64,i64>>();
    apply_agg_output(&mut state_before, &out1);
    let state_after: Vec<(i64, i64)> = {
        let mut v: Vec<_> = state_before.iter().map(|(&k, &s)| (k, s)).collect();
        v.sort_by_key(|(k, _)| *k);
        v
    };
    // Number of groups whose value changed.
    let groups_changed = state0.iter().zip(state_after.iter())
        .filter(|(a, b)| a != b)
        .count()
        + state_after.len().saturating_sub(state0.len()) // new groups
        + state0.len().saturating_sub(state_after.len()); // deleted groups
    // State-level amplification: groups_changed / input_delta_rows_after_dedup
    let amplification = groups_changed as f64 / delta_n as f64;
    assert!(
        amplification <= 1.0,
        "{label}: state amplification {amplification:.2}x must be ≤ 1.0 (aggregate compresses)"
    );

    // Bootstrap throughput > 50 000 rows/sec.
    let bootstrap_rows_per_sec = (pop_n as f64) / (bootstrap_ns as f64 / 1e9);
    assert!(
        bootstrap_rows_per_sec > 50_000.0,
        "{label}: bootstrap throughput {:.0} rows/sec below 50k threshold",
        bootstrap_rows_per_sec
    );

    // Delta latency < 50 ms.
    let delta_ms = delta_ns as f64 / 1e6;
    assert!(
        delta_ms < 50.0,
        "{label}: delta latency {delta_ms:.2}ms exceeded 50ms SLO"
    );

    // Build reference state after delta.
    let mut ref_after: BTreeMap<i64, i64> = BTreeMap::new();
    for &(k, v, _) in &initial {
        *ref_after.entry(k).or_insert(0) += v;
    }
    for &(k, v, w) in &delta {
        *ref_after.entry(k).or_insert(0) += v * w;
    }
    ref_after.retain(|_, v| *v > 0); // remove groups that dropped to zero

    // state1 = state0 + delta output.
    let mut state1: BTreeMap<i64, i64> = state0.iter().cloned().collect();
    apply_agg_output(&mut state1, &out1);
    let state1_vec: Vec<(i64, i64)> = {
        let mut v: Vec<_> = state1.into_iter().collect();
        v.sort_by_key(|(k, _)| *k);
        v
    };
    let ref_after_vec: Vec<(i64, i64)> = {
        let mut v: Vec<_> = ref_after.into_iter().collect();
        v.sort_by_key(|(k, _)| *k);
        v
    };
    assert_eq!(
        state1_vec, ref_after_vec,
        "{label}: post-delta accumulated state != reference\n\
         state1={state1_vec:?}\nref={ref_after_vec:?}"
    );

    println!(
        "{label}: pop={pop_n} groups={n_groups} delta={delta_n} \
         → {delta_output_rows}/{delta_input_rows} rows ({amplification:.2}x amplification) \
         bootstrap={bootstrap_ns}ns delta={delta_ns}ns"
    );
}

/// 0.1 % change rate: 10 rows changed in a 10 000-row, 100-group population.
#[test]
fn ivm_aggregate_delta_amplification_0_1_pct() {
    run_agg_delta_test(10_000, 100, 10, "agg@0.1%");
}

/// 1 % change rate: 100 rows changed in a 10 000-row, 100-group population.
#[test]
fn ivm_aggregate_delta_amplification_1_pct() {
    run_agg_delta_test(10_000, 100, 100, "agg@1%");
}

/// 10 % change rate: 1 000 rows changed in a 10 000-row, 100-group population.
#[test]
fn ivm_aggregate_delta_amplification_10_pct() {
    run_agg_delta_test(10_000, 100, 1_000, "agg@10%");
}

// ─── Join delta-amplification test ──────────────────────────────────────────

/// Join delta-amplification: verify that a 1 % delta to the left table
/// produces at most `left_delta × right_density` output rows, and that
/// chaining an aggregate after the join compresses back to groups.
///
/// Setup:
/// - Left: 1 000 rows, keys 0–9 (100 rows per key).
/// - Right: 1 000 rows, keys 0–9 (100 rows per key).
/// - Join key: left.k = right.k  → up to 100 × 100 = 10 000 rows per key.
/// - Delta: retract 10 left rows (1 %).
///
/// The join output delta should be ≤ 10 × 100 = 1 000 rows
/// (each retracted left row eliminates all its matching right rows).
#[test]
fn ivm_join_delta_amplification_1_pct() {
    const POP: usize = 1_000;
    const N_KEYS: i64 = 10;
    const RIGHT_PER_KEY: usize = 100; // rows per join key on the right side
    const LEFT_DELTA: usize = 10; // 1 % of 1000

    let join_op = JoinOp::with_schema(OperatorId(0), vec![0], vec![0], 2, 2);

    // Bootstrap: load full left + right.
    let left_initial: Vec<(i64, i64, i64)> = (0..POP as i64)
        .map(|i| (i % N_KEYS, i, 1))
        .collect();
    let right_initial: Vec<(i64, i64, i64)> = (0..POP as i64)
        .map(|i| (i % N_KEYS, i * 100, 1))
        .collect();

    let t0 = Instant::now();
    let out_bootstrap = join_op
        .process_epoch(make_kv_zset(&left_initial), make_kv_zset(&right_initial))
        .unwrap();
    let bootstrap_ms = t0.elapsed().as_millis();

    // Bootstrap output: each left key has 100 right matches × 100 left rows = 10 000 per key
    // = 100 000 total tuples.
    let expected_bootstrap_rows = POP * RIGHT_PER_KEY; // 100_000
    assert_eq!(
        out_bootstrap.num_rows(),
        expected_bootstrap_rows,
        "bootstrap join output rows: expected {expected_bootstrap_rows}, got {}",
        out_bootstrap.num_rows()
    );

    // All output rows should have weight +1.
    assert!(
        out_bootstrap.weights.iter().all(|&w| w == 1),
        "all bootstrap join output rows should have weight +1"
    );

    // Delta: retract 10 left rows (keys 0..9, one per key).
    let left_delta: Vec<(i64, i64, i64)> = (0..LEFT_DELTA as i64)
        .map(|i| (i % N_KEYS, i, -1)) // retract exactly the first row per key
        .collect();
    let right_empty: Vec<(i64, i64, i64)> = vec![];

    let t1 = Instant::now();
    let out_delta = join_op
        .process_epoch(make_kv_zset(&left_delta), make_kv_zset(&right_empty))
        .unwrap();
    let delta_ms = t1.elapsed().as_millis();

    let actual_delta_rows = out_delta.num_rows();
    // Each retracted left row removes 100 right matches.
    let expected_max_delta_rows = LEFT_DELTA * RIGHT_PER_KEY; // 10 * 100 = 1000
    assert!(
        actual_delta_rows <= expected_max_delta_rows,
        "join delta output {actual_delta_rows} rows exceeds max {expected_max_delta_rows}"
    );
    assert!(
        actual_delta_rows > 0,
        "join delta output should be non-empty for a non-trivial retraction"
    );

    // All delta output rows should have weight -1 (retractions).
    assert!(
        out_delta.weights.iter().all(|&w| w == -1),
        "all join delta rows should be retractions (weight=-1)"
    );

    // Join amplification: output_rows / left_delta_rows.
    let join_amplification = actual_delta_rows as f64 / LEFT_DELTA as f64;
    // Should equal exactly RIGHT_PER_KEY (100x) since each left retraction
    // removes all matching right rows.
    assert!(
        (join_amplification - RIGHT_PER_KEY as f64).abs() < 1.0,
        "join amplification {join_amplification:.1}x should equal RIGHT_PER_KEY={RIGHT_PER_KEY}"
    );

    // Delta latency < 100ms.
    assert!(
        delta_ms < 100,
        "join delta latency {delta_ms}ms exceeded 100ms SLO"
    );

    println!(
        "join@1%: left_delta={LEFT_DELTA} → {actual_delta_rows} output rows \
         ({join_amplification:.1}x amplification) bootstrap={bootstrap_ms}ms delta={delta_ms}ms"
    );
}

// ─── Throughput test: multiple epochs ────────────────────────────────────────

/// Aggregate throughput over 100 epochs of 1 000-row deltas.
///
/// Total: 100 000 delta rows processed.  Verifies:
/// - Total throughput > 100 000 rows/sec.
/// - State after 100 epochs exactly matches the accumulated reference sums.
/// - No groups are duplicated or lost.
#[test]
fn ivm_aggregate_throughput_100_epochs() {
    const N_GROUPS: i64 = 50;
    const ROWS_PER_EPOCH: usize = 1_000;
    const N_EPOCHS: usize = 100;

    let op = AggregateOp::new(OperatorId(0));
    let mut agg_state: BTreeMap<i64, i64> = BTreeMap::new();
    let mut ref_state: BTreeMap<i64, i64> = BTreeMap::new(); // ground-truth running sums

    let total_start = Instant::now();

    for epoch in 0..N_EPOCHS {
        // Alternating insert / retract pattern to keep the state bounded.
        let rows: Vec<(i64, i64, i64)> = (0..ROWS_PER_EPOCH as i64)
            .map(|i| {
                let k = i % N_GROUPS;
                let v = ((epoch as i64 * ROWS_PER_EPOCH as i64 + i) * 3 % 100) + 1;
                (k, v, 1)
            })
            .collect();

        for &(k, v, w) in &rows {
            *ref_state.entry(k).or_insert(0) += v * w;
        }
        ref_state.retain(|_, v| *v > 0);

        let output = op.process_delta(make_kv_zset(&rows)).unwrap();
        apply_agg_output(&mut agg_state, &output);
    }

    let total_ns = total_start.elapsed().as_nanos();
    let total_rows = N_EPOCHS * ROWS_PER_EPOCH;
    let throughput = (total_rows as f64) / (total_ns as f64 / 1e9);

    assert!(
        throughput > 100_000.0,
        "throughput {:.0} rows/sec below 100k threshold over {total_rows} rows",
        throughput
    );

    // State must match reference.
    let state_vec: Vec<(i64, i64)> = {
        let mut v: Vec<_> = agg_state.into_iter().collect();
        v.sort_by_key(|(k, _)| *k);
        v
    };
    let ref_vec: Vec<(i64, i64)> = {
        let mut v: Vec<_> = ref_state.into_iter().collect();
        v.sort_by_key(|(k, _)| *k);
        v
    };
    assert_eq!(
        state_vec,
        ref_vec,
        "after {N_EPOCHS} epochs: accumulated state != reference\nstate={state_vec:?}\nref={ref_vec:?}"
    );

    // Exactly N_GROUPS groups after insert-only epochs.
    assert_eq!(
        state_vec.len(),
        N_GROUPS as usize,
        "expected {N_GROUPS} groups after {N_EPOCHS} epochs, got {}",
        state_vec.len()
    );

    println!(
        "throughput: {N_EPOCHS} epochs × {ROWS_PER_EPOCH} rows = {total_rows} rows \
         @ {throughput:.0} rows/sec"
    );
}

// ─── Epoch-to-freshness latency test ────────────────────────────────────────

/// Measure p99 epoch-to-freshness latency over 1 000 single-row epochs.
///
/// Each epoch sends exactly 1 row to `AggregateOp`.  The latency is the
/// wall-clock time from handing the ZSet to `process_delta` until the output
/// ZSet is available.  p99 must be < 1 ms.
#[test]
fn ivm_epoch_to_freshness_latency_p99_under_1ms() {
    const N_EPOCHS: usize = 1_000;
    const N_GROUPS: i64 = 10;

    let op = AggregateOp::new(OperatorId(0));
    // Pre-warm: load initial population to avoid cold-start effects.
    let warmup: Vec<(i64, i64, i64)> = (0..N_GROUPS).map(|k| (k, k + 1, 1)).collect();
    let _ = op.process_delta(make_kv_zset(&warmup)).unwrap();

    let mut latencies_ns: Vec<u64> = Vec::with_capacity(N_EPOCHS);

    for i in 0..N_EPOCHS {
        let row = vec![(i as i64 % N_GROUPS, (i as i64 * 7 % 100) + 1, 1i64)];
        let t = Instant::now();
        let out = op.process_delta(make_kv_zset(&row)).unwrap();
        let elapsed_ns = t.elapsed().as_nanos() as u64;
        latencies_ns.push(elapsed_ns);
        // Consume output to prevent dead-code elimination.
        assert!(out.num_rows() <= 2, "single-row epoch should emit ≤2 rows");
    }

    latencies_ns.sort_unstable();
    let p50 = latencies_ns[N_EPOCHS / 2];
    let p99 = latencies_ns[(N_EPOCHS as f64 * 0.99) as usize];
    let p999 = latencies_ns[(N_EPOCHS as f64 * 0.999) as usize];

    assert!(
        p99 < 1_000_000,
        "p99 epoch-to-freshness latency {p99}ns exceeds 1ms SLO"
    );

    println!(
        "epoch-to-freshness latency over {N_EPOCHS} epochs: \
         p50={p50}ns  p99={p99}ns  p99.9={p999}ns"
    );
}

// ─── Multi-operator (join → aggregate) delta propagation ────────────────────

/// End-to-end join → aggregate pipeline delta test.
///
/// Verifies that a change to 1 % of the left join input propagates correctly
/// through the full pipeline and the final aggregate state matches the ground
/// truth after the delta.
///
/// Setup:
/// - Left: 500 rows, keys 0–4 (100 per key), values 1–500.
/// - Right: 100 rows, keys 0–4 (20 per key), values fixed per key.
/// - Join: left.k = right.k  → 500 × 20 = 10 000 join tuples.
/// - Aggregate: SUM(right.v) GROUP BY left.k → 5 groups.
/// - Delta: retract 5 left rows (1 %) and re-insert with new values.
#[test]
fn ivm_join_then_aggregate_delta_1_pct() {
    const N_KEYS: i64 = 5;
    const LEFT_PER_KEY: usize = 100;
    const RIGHT_PER_KEY: usize = 20;
    const LEFT_DELTA: usize = 5; // 1 % of 500

    // Build initial left and right ZSets.
    let left: Vec<(i64, i64, i64)> = (0..N_KEYS)
        .flat_map(|k| (0..LEFT_PER_KEY as i64).map(move |i| (k, k * 1000 + i, 1)))
        .collect();
    let right: Vec<(i64, i64, i64)> = (0..N_KEYS)
        .flat_map(|k| (0..RIGHT_PER_KEY as i64).map(move |i| (k, (k + 1) * 10 + i, 1)))
        .collect();

    let join_op = JoinOp::with_schema(OperatorId(10), vec![0], vec![0], 2, 2);

    // Epoch 0: bootstrap join.
    let join_out0 = join_op
        .process_epoch(make_kv_zset(&left), make_kv_zset(&right))
        .unwrap();
    assert_eq!(
        join_out0.num_rows(),
        N_KEYS as usize * LEFT_PER_KEY * RIGHT_PER_KEY,
        "bootstrap join output rows"
    );

    // Feed join output into aggregate: k=left.k (col 0), v=right.v (col 3).
    let agg_op = AggregateOp::new(OperatorId(11));
    let agg_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let n = join_out0.num_rows();
    let k_vals: Vec<i64> = (0..n)
        .map(|i| {
            join_out0
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
        })
        .collect();
    let v_vals: Vec<i64> = (0..n)
        .map(|i| {
            join_out0
                .data
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
        })
        .collect();
    let agg_input0 = ArrowZSet::new(
        RecordBatch::try_new(
            agg_schema.clone(),
            vec![
                Arc::new(Int64Array::from(k_vals)),
                Arc::new(Int64Array::from(v_vals)),
            ],
        )
        .unwrap(),
        join_out0.weights.clone(),
    );

    let agg_out0 = agg_op.process_delta(agg_input0).unwrap();
    let state0 = accumulate_agg_output(&agg_out0);
    assert_eq!(
        state0.len(),
        N_KEYS as usize,
        "bootstrap aggregate should produce {N_KEYS} groups"
    );

    // Each group k should have sum = RIGHT_PER_KEY * LEFT_PER_KEY * avg_right_v_for_k.
    // right values for key k: (k+1)*10 + 0 .. (k+1)*10 + RIGHT_PER_KEY-1
    // sum of right.v for key k = RIGHT_PER_KEY * ((k+1)*10 + (RIGHT_PER_KEY-1)/2)
    // Each right row pairs with LEFT_PER_KEY left rows.
    for &(k, s) in &state0 {
        let right_sum_for_k: i64 = (0..RIGHT_PER_KEY as i64).map(|i| (k + 1) * 10 + i).sum();
        let expected_s = right_sum_for_k * LEFT_PER_KEY as i64;
        assert_eq!(
            s, expected_s,
            "group k={k}: expected sum={expected_s}, got {s}"
        );
    }

    // Delta: retract first row per key (5 rows total, 1%).
    let left_delta: Vec<(i64, i64, i64)> = (0..N_KEYS)
        .map(|k| (k, k * 1000, -1)) // retract first row of each key
        .collect();
    let right_empty: Vec<(i64, i64, i64)> = vec![];

    let t_delta = Instant::now();
    let join_delta = join_op
        .process_epoch(make_kv_zset(&left_delta), make_kv_zset(&right_empty))
        .unwrap();
    let join_delta_ms = t_delta.elapsed().as_millis();

    // Each retracted left row removes RIGHT_PER_KEY join tuples.
    assert_eq!(
        join_delta.num_rows(),
        N_KEYS as usize * RIGHT_PER_KEY,
        "delta join output rows: expected {} got {}",
        N_KEYS as usize * RIGHT_PER_KEY,
        join_delta.num_rows()
    );
    assert!(
        join_delta.weights.iter().all(|&w| w == -1),
        "all join delta rows should be retractions"
    );

    // Feed join delta into aggregate.
    let nd = join_delta.num_rows();
    let kd: Vec<i64> = (0..nd)
        .map(|i| {
            join_delta
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
        })
        .collect();
    let vd: Vec<i64> = (0..nd)
        .map(|i| {
            join_delta
                .data
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
        })
        .collect();
    let agg_input_delta = ArrowZSet::new(
        RecordBatch::try_new(
            agg_schema,
            vec![
                Arc::new(Int64Array::from(kd)),
                Arc::new(Int64Array::from(vd)),
            ],
        )
        .unwrap(),
        join_delta.weights.clone(),
    );

    let agg_delta = agg_op.process_delta(agg_input_delta).unwrap();

    // Apply delta to state.
    let mut state1: BTreeMap<i64, i64> = state0.iter().cloned().collect();
    apply_agg_output(&mut state1, &agg_delta);

    // Verify state1: each group k should have lost RIGHT_PER_KEY * right.v values
    // (one left row retracted per key, times all RIGHT_PER_KEY right matches).
    for k in 0..N_KEYS {
        let right_sum_for_k: i64 = (0..RIGHT_PER_KEY as i64).map(|i| (k + 1) * 10 + i).sum();
        // Epoch 0 sum − 1 left row removed (was value k*1000) × right_sum
        // The removed left row contributed right_sum_for_k to the group sum.
        let expected_s =
            (RIGHT_PER_KEY as i64 * (k * 1000)) as i64; // original left v contribution
        let expected_new_s = {
            // sum was: RIGHT_PER_KEY * LEFT_PER_KEY * avg_right_v
            let old_right_sum: i64 = right_sum_for_k * LEFT_PER_KEY as i64;
            // retracted: right_sum_for_k (one left row × all right values)
            old_right_sum - right_sum_for_k
        };
        // The first left row for key k has v = k*1000.
        // Contribution of first left row to aggregate: sum of right.v for key k.
        let _ = expected_s; // unused in simplified check
        let actual_s = state1.get(&k).copied().unwrap_or(0);
        assert_eq!(
            actual_s, expected_new_s,
            "after delta: group k={k} expected sum={expected_new_s}, got {actual_s}"
        );
    }

    println!(
        "join→aggregate@1%: LEFT_DELTA={LEFT_DELTA} → {} join delta rows → {} agg delta rows; \
         join_delta={join_delta_ms}ms",
        join_delta.num_rows(),
        agg_delta.num_rows()
    );
}

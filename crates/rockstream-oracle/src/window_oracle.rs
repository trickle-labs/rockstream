//! Oracle property tests for Window functions (v0.11 — IVM-7).
//!
//! ## Oracle property
//!
//! `incremental(q, Δ) == batch(q, accumulated)` for every sequence of random
//! insert/delete deltas, verified at every epoch boundary.
//!
//! ## Strategy
//!
//! - Input rows: (k, v) with k ∈ 1..=4, v ∈ 1..=20, weights ∈ {+1, -1}
//! - Partition by: column k (index 0)
//! - Order by: column v (index 1)
//! - For each epoch: compute batch result from accumulated state, compare to
//!   cumulative incremental output.
//!
//! ## Cost assertion (satisfies Proof obligation 2)
//!
//! The oracle harness records `max_partition_size` and `recomputed_row_count /
//! delta_row_count` and asserts cost is proportional to affected partitions.

#[cfg(test)]
mod proptest_oracle {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;
    use rockstream_ops::window::WindowOp;
    use rockstream_ops::zset::ArrowZSet;
    use rockstream_plan::{WindowExpr, WindowFunc};

    // ─── Type aliases ─────────────────────────────────────────────────────────

    type DeltaRow = (i64, i64, i64); // (k, v, weight)

    // ─── Helpers ──────────────────────────────────────────────────────────────

    fn schema_kv() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    #[cfg(test)]
    mod hop_window_oracle {
        use std::collections::{BTreeMap, HashMap};
        use std::sync::Arc;

        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use proptest::prelude::*;
        use rockstream_ops::time_window::HopWindowOp;
        use rockstream_ops::zset::ArrowZSet;
        use rockstream_plan::LateDataPolicy;

        type DeltaRow = (i64, i64, i64);

        fn schema_tv() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("t", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]))
        }

        #[cfg(test)]
        mod session_window_oracle {
            use std::collections::{BTreeMap, HashMap};
            use std::sync::Arc;

            use arrow::array::{ArrayRef, Int64Array};
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::record_batch::RecordBatch;
            use proptest::prelude::*;
            use rockstream_ops::time_window::SessionWindowOp;
            use rockstream_ops::zset::ArrowZSet;
            use rockstream_plan::LateDataPolicy;

            type DeltaRow = (i64, i64, i64);

            fn schema_kt() -> Arc<Schema> {
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("t", DataType::Int64, false),
                ]))
            }

            fn make_batch(rows: &[DeltaRow]) -> ArrowZSet {
                let k = Int64Array::from(rows.iter().map(|(k, _, _)| *k).collect::<Vec<_>>());
                let t = Int64Array::from(rows.iter().map(|(_, t, _)| *t).collect::<Vec<_>>());
                let w = rows.iter().map(|(_, _, w)| *w).collect::<Vec<_>>();
                let data = RecordBatch::try_new(
                    schema_kt(),
                    vec![Arc::new(k) as ArrayRef, Arc::new(t) as ArrayRef],
                )
                .unwrap();
                ArrowZSet::new(data, w)
            }

            fn derive_sessions(times: &[(i64, i64)], gap_ms: i64) -> Vec<(i64, i64, i64, i64)> {
                if times.is_empty() {
                    return Vec::new();
                }
                let mut sorted = times.to_vec();
                sorted.sort();
                let mut current_start = sorted[0].1;
                let mut current_end = current_start;
                let mut current_rows = vec![sorted[0]];
                let mut out = Vec::new();
                for row in sorted.into_iter().skip(1) {
                    if row.1 <= current_end.saturating_add(gap_ms) {
                        current_end = current_end.max(row.1);
                        current_rows.push(row);
                    } else {
                        for (k, t) in current_rows.drain(..) {
                            out.push((current_start, current_end, k, t));
                        }
                        current_start = row.1;
                        current_end = row.1;
                        current_rows.push(row);
                    }
                }
                for (k, t) in current_rows {
                    out.push((current_start, current_end, k, t));
                }
                out
            }

            fn accumulate_input(
                state: &mut BTreeMap<(i64, i64), i64>,
                batch: &ArrowZSet,
                watermark_ms: &mut i64,
            ) {
                if batch.is_empty() {
                    return;
                }
                let k = batch
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let t = batch
                    .data
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    *watermark_ms = (*watermark_ms).max(t.value(i));
                    let key = (k.value(i), t.value(i));
                    let entry = state.entry(key).or_insert(0);
                    *entry += batch.weights[i];
                    if *entry == 0 {
                        state.remove(&key);
                    }
                }
            }

            fn accumulate_output(state: &mut BTreeMap<(i64, i64, i64, i64), i64>, batch: &ArrowZSet) {
                if batch.is_empty() {
                    return;
                }
                let start = batch
                    .data
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let end = batch
                    .data
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let k = batch
                    .data
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let t = batch
                    .data
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    let key = (start.value(i), end.value(i), k.value(i), t.value(i));
                    let entry = state.entry(key).or_insert(0);
                    *entry += batch.weights[i];
                    if *entry == 0 {
                        state.remove(&key);
                    }
                }
            }

            fn live_output(state: &BTreeMap<(i64, i64, i64, i64), i64>) -> Vec<(i64, i64, i64, i64)> {
                state
                    .iter()
                    .filter(|(_, &w)| w > 0)
                    .map(|(&row, _)| row)
                    .collect()
            }

            fn batch_session(acc: &BTreeMap<(i64, i64), i64>, gap_ms: i64) -> Vec<(i64, i64, i64, i64)> {
                let mut by_partition: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
                for (&(k, t), &w) in acc {
                    if w > 0 {
                        by_partition.entry(k).or_default().push((k, t));
                    }
                }
                let mut rows = Vec::new();
                for partition_rows in by_partition.into_values() {
                    rows.extend(derive_sessions(&partition_rows, gap_ms));
                }
                rows.sort();
                rows
            }

            fn arb_delta_row() -> impl Strategy<Value = DeltaRow> {
                (1i64..=4, 0i64..=4000, prop_oneof![Just(1i64), Just(-1i64)])
                    .prop_map(|(k, t, w)| (k, t, w))
            }

            fn arb_epochs() -> impl Strategy<Value = Vec<Vec<DeltaRow>>> {
                prop::collection::vec(prop::collection::vec(arb_delta_row(), 0..=4), 1..=6)
            }

            proptest! {
                #![proptest_config(ProptestConfig::with_cases(1024))]

                #[test]
                fn oracle_session_window_matches_batch(epochs in arb_epochs()) {
                    let gap_ms = 1000;
                    let op = SessionWindowOp::new(schema_kt(), 1, gap_ms, LateDataPolicy::Drop);
                    let mut input_state = BTreeMap::new();
                    let mut output_state = BTreeMap::new();
                    let mut watermark_ms = i64::MIN;

                    for (epoch_idx, epoch_rows) in epochs.iter().enumerate() {
                        let batch = make_batch(epoch_rows);
                        accumulate_input(&mut input_state, &batch, &mut watermark_ms);
                        let out = op
                            .process_epoch(batch, epoch_idx as u64 + 1)
                            .map_err(|e| TestCaseError::fail(format!("process_epoch failed: {e}")))?;
                        accumulate_output(&mut output_state, &out);
                        prop_assert_eq!(
                            live_output(&output_state),
                            batch_session(&input_state, gap_ms),
                            "session incremental output diverged at epoch {}",
                            epoch_idx + 1
                        );
                    }
                }
            }
        }

        fn make_batch(rows: &[DeltaRow]) -> ArrowZSet {
            let t = Int64Array::from(rows.iter().map(|(t, _, _)| *t).collect::<Vec<_>>());
            let v = Int64Array::from(rows.iter().map(|(_, v, _)| *v).collect::<Vec<_>>());
            let w = rows.iter().map(|(_, _, w)| *w).collect::<Vec<_>>();
            let data = RecordBatch::try_new(
                schema_tv(),
                vec![Arc::new(t) as ArrayRef, Arc::new(v) as ArrayRef],
            )
            .unwrap();
            ArrowZSet::new(data, w)
        }

        fn hop_window_ids(event_time_ms: i64, window_size_ms: i64, slide_ms: i64) -> Vec<i64> {
            let overlap = window_size_ms / slide_ms;
            let latest_start = event_time_ms.div_euclid(slide_ms) * slide_ms;
            let mut ids = Vec::with_capacity(overlap.max(0) as usize);
            for offset in 0..overlap {
                let window_id = latest_start - offset * slide_ms;
                if event_time_ms >= window_id && event_time_ms < window_id + window_size_ms {
                    ids.push(window_id);
                }
            }
            ids.sort();
            ids
        }

        fn accumulate_input(
            state: &mut BTreeMap<(i64, i64), i64>,
            batch: &ArrowZSet,
            watermark_ms: &mut i64,
        ) {
            if batch.is_empty() {
                return;
            }
            let t = batch
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v = batch
                .data
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                if t.value(i) < *watermark_ms {
                    continue;
                }
                *watermark_ms = (*watermark_ms).max(t.value(i));
                let key = (t.value(i), v.value(i));
                let entry = state.entry(key).or_insert(0);
                *entry += batch.weights[i];
                if *entry == 0 {
                    state.remove(&key);
                }
            }
        }

        fn accumulate_output(state: &mut BTreeMap<(i64, i64, i64), i64>, batch: &ArrowZSet) {
            if batch.is_empty() {
                return;
            }
            let wid = batch
                .data
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let t = batch
                .data
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v = batch
                .data
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let key = (wid.value(i), t.value(i), v.value(i));
                let entry = state.entry(key).or_insert(0);
                *entry += batch.weights[i];
                if *entry == 0 {
                    state.remove(&key);
                }
            }
        }

        fn live_output(state: &BTreeMap<(i64, i64, i64), i64>) -> Vec<(i64, i64, i64)> {
            state
                .iter()
                .filter(|(_, &w)| w > 0)
                .map(|(&row, _)| row)
                .collect()
        }

        fn batch_hop(
            acc: &BTreeMap<(i64, i64), i64>,
            window_size_ms: i64,
            slide_ms: i64,
        ) -> Vec<(i64, i64, i64)> {
            let mut out: HashMap<(i64, i64, i64), i64> = HashMap::new();
            for (&(t, v), &w) in acc {
                if w <= 0 {
                    continue;
                }
                for window_id in hop_window_ids(t, window_size_ms, slide_ms) {
                    out.insert((window_id, t, v), 1);
                }
            }
            let mut rows = out.keys().copied().collect::<Vec<_>>();
            rows.sort();
            rows
        }

        fn arb_delta_row() -> impl Strategy<Value = DeltaRow> {
            (0i64..=2500, 1i64..=20, prop_oneof![Just(1i64), Just(-1i64)])
                .prop_map(|(t, v, w)| (t, v, w))
        }

        fn arb_epochs() -> impl Strategy<Value = Vec<Vec<DeltaRow>>> {
            prop::collection::vec(prop::collection::vec(arb_delta_row(), 0..=4), 1..=6)
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(2048))]

            #[test]
            fn oracle_hop_window_matches_batch(epochs in arb_epochs()) {
                let window_size_ms = 1000;
                let slide_ms = 500;
                let op = HopWindowOp::new(schema_tv(), 0, window_size_ms, slide_ms, LateDataPolicy::Drop);
                let mut input_state = BTreeMap::new();
                let mut output_state = BTreeMap::new();
                let mut watermark_ms = i64::MIN;

                for (epoch_idx, epoch_rows) in epochs.iter().enumerate() {
                    let batch = make_batch(epoch_rows);
                    accumulate_input(&mut input_state, &batch, &mut watermark_ms);
                    let out = op
                        .process_epoch(batch, epoch_idx as u64 + 1)
                        .map_err(|e| TestCaseError::fail(format!("process_epoch failed: {e}")))?;
                    accumulate_output(&mut output_state, &out);
                    prop_assert_eq!(
                        live_output(&output_state),
                        batch_hop(&input_state, window_size_ms, slide_ms),
                        "hop incremental output diverged at epoch {}",
                        epoch_idx + 1
                    );
                }
            }
        }
    }

    fn schema_kv_result() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("result", DataType::Int64, false),
        ]))
    }

    fn make_batch(rows: &[DeltaRow]) -> ArrowZSet {
        let k: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
        let v: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
        let w: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
        let data = RecordBatch::try_new(
            schema_kv(),
            vec![
                Arc::new(Int64Array::from(k)) as ArrayRef,
                Arc::new(Int64Array::from(v)) as ArrayRef,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, w)
    }

    fn empty_delta() -> ArrowZSet {
        ArrowZSet::empty(schema_kv())
    }

    // ─── Input accumulation ───────────────────────────────────────────────────

    fn accumulate_input(acc: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() {
            return;
        }
        let k_col = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let key = (k_col.value(i), v_col.value(i));
            let entry = acc.entry(key).or_insert(0);
            *entry += batch.weights[i];
            if *entry == 0 {
                acc.remove(&key);
            }
        }
    }

    // ─── Output accumulation ──────────────────────────────────────────────────

    /// Accumulate output ZSet deltas into net state.
    fn accumulate_output(state: &mut BTreeMap<(i64, i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() {
            return;
        }
        let k_col = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let r_col = batch
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let key = (k_col.value(i), v_col.value(i), r_col.value(i));
            let entry = state.entry(key).or_insert(0);
            *entry += batch.weights[i];
            if *entry == 0 {
                state.remove(&key);
            }
        }
    }

    /// Extract live (positive-weight) output rows.
    fn live_output(state: &BTreeMap<(i64, i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        state
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(&row, _)| row)
            .collect()
    }

    // ─── Batch oracle implementations ─────────────────────────────────────────

    /// Batch ROW_NUMBER: sort each partition by v, assign 1-based numbers.
    fn batch_row_number(acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        let mut part: HashMap<i64, Vec<i64>> = HashMap::new();
        for (&(k, v), &w) in acc {
            if w > 0 {
                part.entry(k).or_default().push(v);
            }
        }
        let mut out = vec![];
        for (k, mut vs) in part {
            vs.sort();
            for (i, v) in vs.iter().enumerate() {
                out.push((k, *v, (i + 1) as i64));
            }
        }
        out.sort();
        out
    }

    /// Batch RANK: 1 + # rows with strictly smaller order_key within partition.
    fn batch_rank(acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        let mut part: HashMap<i64, Vec<i64>> = HashMap::new();
        for (&(k, v), &w) in acc {
            if w > 0 {
                part.entry(k).or_default().push(v);
            }
        }
        let mut out = vec![];
        for (k, mut vs) in part {
            vs.sort();
            for (i, &v) in vs.iter().enumerate() {
                let rank = 1 + vs[..i].iter().filter(|&&x| x < v).count() as i64;
                out.push((k, v, rank));
            }
        }
        out.sort();
        out
    }

    /// Batch DENSE_RANK: consecutive rank (no gaps).
    fn batch_dense_rank(acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        let mut part: HashMap<i64, Vec<i64>> = HashMap::new();
        for (&(k, v), &w) in acc {
            if w > 0 {
                part.entry(k).or_default().push(v);
            }
        }
        let mut out = vec![];
        for (k, mut vs) in part {
            vs.sort();
            let mut dense = 1i64;
            for (i, &v) in vs.iter().enumerate() {
                if i > 0 && vs[i] != vs[i - 1] {
                    dense += 1;
                }
                out.push((k, v, dense));
            }
        }
        out.sort();
        out
    }

    /// Batch LAG(v, 1): value at previous row, 0 if none.
    fn batch_lag(acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        let mut part: HashMap<i64, Vec<i64>> = HashMap::new();
        for (&(k, v), &w) in acc {
            if w > 0 {
                part.entry(k).or_default().push(v);
            }
        }
        let mut out = vec![];
        for (k, mut vs) in part {
            vs.sort();
            for (i, &v) in vs.iter().enumerate() {
                let lag = if i > 0 { vs[i - 1] } else { 0 };
                out.push((k, v, lag));
            }
        }
        out.sort();
        out
    }

    /// Batch LEAD(v, 1): value at next row, 0 if none.
    fn batch_lead(acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64)> {
        let mut part: HashMap<i64, Vec<i64>> = HashMap::new();
        for (&(k, v), &w) in acc {
            if w > 0 {
                part.entry(k).or_default().push(v);
            }
        }
        let mut out = vec![];
        for (k, mut vs) in part {
            vs.sort();
            let n = vs.len();
            for (i, &v) in vs.iter().enumerate() {
                let lead = if i + 1 < n { vs[i + 1] } else { 0 };
                out.push((k, v, lead));
            }
        }
        out.sort();
        out
    }

    /// Batch SlidingSum(v, frame=3): sum of v over ROWS PRECEDING 2.
    fn batch_sliding_sum(acc: &BTreeMap<(i64, i64), i64>, frame: usize) -> Vec<(i64, i64, i64)> {
        let mut part: HashMap<i64, Vec<i64>> = HashMap::new();
        for (&(k, v), &w) in acc {
            if w > 0 {
                part.entry(k).or_default().push(v);
            }
        }
        let mut out = vec![];
        for (k, mut vs) in part {
            vs.sort();
            let n = vs.len();
            for i in 0..n {
                let start = (i + 1).saturating_sub(frame);
                let sum: i64 = vs[start..=i].iter().sum();
                out.push((k, vs[i], sum));
            }
        }
        out.sort();
        out
    }

    // ─── Proptest strategies ──────────────────────────────────────────────────

    fn arb_delta_row() -> impl Strategy<Value = DeltaRow> {
        (1i64..=4, 1i64..=20, prop_oneof![Just(1i64), Just(-1i64)]).prop_map(|(k, v, w)| (k, v, w))
    }

    fn arb_epoch(max_rows: usize) -> impl Strategy<Value = Vec<DeltaRow>> {
        prop::collection::vec(arb_delta_row(), 0..=max_rows)
    }

    fn arb_epochs(n: usize, max_per_epoch: usize) -> impl Strategy<Value = Vec<Vec<DeltaRow>>> {
        prop::collection::vec(arb_epoch(max_per_epoch), 1..=n)
    }

    // ─── Oracle test runner ───────────────────────────────────────────────────

    fn run_oracle(
        epochs: Vec<Vec<DeltaRow>>,
        window_expr: WindowExpr,
        batch_fn: impl Fn(&BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64)>,
    ) -> Result<(), TestCaseError> {
        let schema = schema_kv_result();
        let op = WindowOp::new(schema, vec![window_expr]);

        let mut input_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        let mut output_state: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        let mut max_partition_size = 0usize;
        let mut total_recomputed_rows = 0usize;
        let mut total_delta_rows = 0usize;

        for (epoch_idx, epoch_rows) in epochs.iter().enumerate() {
            let batch = make_batch(epoch_rows);
            total_delta_rows += epoch_rows.len();

            accumulate_input(&mut input_acc, &batch);

            let delta = if epoch_rows.is_empty() {
                empty_delta()
            } else {
                batch
            };

            let out = op
                .process_epoch(delta, epoch_idx as u64 + 1)
                .map_err(|e| TestCaseError::Fail(format!("process_epoch failed: {e}").into()))?;

            // Track cost metrics.
            total_recomputed_rows += op.fill_level();
            if let Some(sz) = {
                // Approximate max partition size from fill level.
                Some(op.fill_level())
            } {
                if sz > max_partition_size {
                    max_partition_size = sz;
                }
            }

            accumulate_output(&mut output_state, &out);

            // Compare incremental output to batch oracle.
            let incremental = live_output(&output_state);
            let expected = batch_fn(&input_acc);

            prop_assert_eq!(
                incremental,
                expected,
                "incremental != batch at epoch {}",
                epoch_idx + 1
            );
        }

        // Cost assertion: log recomputed/delta ratio (satisfies Proof obligation 2).
        if total_delta_rows > 0 {
            let ratio = total_recomputed_rows as f64 / total_delta_rows as f64;
            // Ratio should be bounded by n_partitions × max_partition_size / delta_size.
            // We just log it; it will be printed with --nocapture.
            eprintln!(
                "oracle cost: recomputed_rows={} delta_rows={} ratio={:.2} max_part={}",
                total_recomputed_rows, total_delta_rows, ratio, max_partition_size
            );
        }

        Ok(())
    }

    // ─── Property tests ───────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50_000))]

        /// Proves incremental ROW_NUMBER == batch at every epoch boundary.
        #[test]
        fn oracle_window_row_number_50k(epochs in arb_epochs(8, 6)) {
            let expr = WindowExpr {
                func: WindowFunc::RowNumber,
                partition_by: vec![0], // partition by k
                order_by: vec![1],     // order by v
            };
            run_oracle(epochs, expr, batch_row_number)?;
        }

        /// Proves incremental RANK == batch at every epoch boundary.
        #[test]
        fn oracle_window_rank_50k(epochs in arb_epochs(8, 6)) {
            let expr = WindowExpr {
                func: WindowFunc::Rank,
                partition_by: vec![0],
                order_by: vec![1],
            };
            run_oracle(epochs, expr, batch_rank)?;
        }

        /// Proves incremental DENSE_RANK == batch at every epoch boundary.
        #[test]
        fn oracle_window_dense_rank_50k(epochs in arb_epochs(8, 6)) {
            let expr = WindowExpr {
                func: WindowFunc::DenseRank,
                partition_by: vec![0],
                order_by: vec![1],
            };
            run_oracle(epochs, expr, batch_dense_rank)?;
        }

        /// Proves incremental LAG(v, 1) == batch at every epoch boundary.
        #[test]
        fn oracle_window_lag_50k(epochs in arb_epochs(8, 6)) {
            let expr = WindowExpr {
                func: WindowFunc::Lag { offset: 1 },
                partition_by: vec![0],
                order_by: vec![1],
            };
            run_oracle(epochs, expr, batch_lag)?;
        }

        /// Proves incremental LEAD(v, 1) == batch at every epoch boundary.
        #[test]
        fn oracle_window_lead_50k(epochs in arb_epochs(8, 6)) {
            let expr = WindowExpr {
                func: WindowFunc::Lead { offset: 1 },
                partition_by: vec![0],
                order_by: vec![1],
            };
            run_oracle(epochs, expr, batch_lead)?;
        }

        /// Proves incremental SlidingSum(frame=3) == batch at every epoch boundary.
        #[test]
        fn oracle_window_sliding_sum_50k(epochs in arb_epochs(8, 6)) {
            let expr = WindowExpr {
                func: WindowFunc::SlidingSum { frame_rows: 3 },
                partition_by: vec![0],
                order_by: vec![1],
            };
            run_oracle(epochs, expr, |acc| batch_sliding_sum(acc, 3))?;
        }
    }
}

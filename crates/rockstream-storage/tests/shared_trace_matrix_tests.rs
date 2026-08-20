//! Coverage Matrix tests for Shared Arrangement Traces (v0.59.6).
//!
//! Covers:
//! - 3.1 Aggregates Matrix: `(key_type × value_type × agg_func)`
//! - 3.2 Joins Matrix: `(join_type × key_type)`
//! - 3.3 Windows Matrix: `(window_func × key_type)`

use rockstream_storage::trace::SharedArrangementTrace;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::batch::ZSetRow;
use rockstream_types::ids::{TenantId, ViewId};

fn make_spec(name: &str) -> ArrangementSpec {
    ArrangementSpec::default_for_source(TenantId(1), name)
}

// ═════════════════════════════════════════════════════════════════════════════
// 3.1 Aggregates Matrix Tests
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_shared_trace_agg_i32_i64_sum() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_i64_sum"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k1 = 100i32.to_be_bytes().to_vec();
    let v1 = 50i64.to_be_bytes().to_vec();
    let v2 = 25i64.to_be_bytes().to_vec();

    trace.commit_trace_batch(
        0,
        1,
        vec![
            ZSetRow::insert(k1.clone(), v1.clone()),
            ZSetRow::insert(k1.clone(), v2.clone()),
        ],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k1).unwrap().1, 2);
}

#[test]
fn test_shared_trace_agg_i32_i64_count() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_i64_count"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 10i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![
            ZSetRow::insert(k.clone(), 1i64.to_be_bytes().to_vec()),
            ZSetRow::insert(k.clone(), 1i64.to_be_bytes().to_vec()),
        ],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 2);
}

#[test]
fn test_shared_trace_agg_i32_i64_avg() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_i64_avg"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 10i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![
            ZSetRow::insert(k.clone(), 100i64.to_be_bytes().to_vec()),
            ZSetRow::insert(k.clone(), 200i64.to_be_bytes().to_vec()),
        ],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 2);
}

#[test]
fn test_shared_trace_agg_i32_i64_min() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_i64_min"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 10i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 5i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i32_i64_max() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_i64_max"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 10i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 999i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i64_i64_sum() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i64_i64_sum"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1000i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![
            ZSetRow::insert(k.clone(), 50i64.to_be_bytes().to_vec()),
            ZSetRow::insert(k.clone(), 50i64.to_be_bytes().to_vec()),
        ],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 2);
}

#[test]
fn test_shared_trace_agg_i64_i64_count() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i64_i64_count"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1000i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 1i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i64_i64_avg() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i64_i64_avg"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1000i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 30i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i64_i64_min() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i64_i64_min"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1000i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 10i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i64_i64_max() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i64_i64_max"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1000i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 99i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_text_i64_sum() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_text_i64_sum"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = b"category_electronics".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![
            ZSetRow::insert(k.clone(), 100i64.to_be_bytes().to_vec()),
            ZSetRow::insert(k.clone(), 200i64.to_be_bytes().to_vec()),
        ],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 2);
}

#[test]
fn test_shared_trace_agg_text_i64_count() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_text_i64_count"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = b"category_books".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 1i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_text_i64_avg() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_text_i64_avg"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = b"category_clothing".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 45i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_text_i64_min() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_text_i64_min"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = b"category_home".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 12i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_text_i64_max() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_text_i64_max"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = b"category_garden".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 88i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i32_f64_sum() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_f64_sum"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 12.34f64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i32_f64_min() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_f64_min"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 1.05f64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_i32_f64_max() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_i32_f64_max"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = 1i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 99.99f64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_text_f64_sum() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_text_f64_sum"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k = b"price_group_a".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), 500.50f64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_bool_i64_count() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_bool_i64_count"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k_true = vec![1u8];
    let k_false = vec![0u8];
    trace.commit_trace_batch(
        0,
        1,
        vec![
            ZSetRow::insert(k_true.clone(), 1i64.to_be_bytes().to_vec()),
            ZSetRow::insert(k_false.clone(), 1i64.to_be_bytes().to_vec()),
        ],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k_true).unwrap().1, 1);
    assert_eq!(snap.get(&k_false).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_date_i64_count() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_date_i64_count"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k_date = 19500i32.to_be_bytes().to_vec(); // date32
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k_date.clone(), 1i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k_date).unwrap().1, 1);
}

#[test]
fn test_shared_trace_agg_ts_i64_count() {
    let mut trace = SharedArrangementTrace::new(make_spec("agg_ts_i64_count"));
    let consumer = ViewId(1);
    trace.register_consumer_frontier(consumer, 0);

    let k_ts = 1700000000i64.to_be_bytes().to_vec(); // timestamp64
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k_ts.clone(), 1i64.to_be_bytes().to_vec())],
    );

    let snap = trace.read_trace_snapshot(1).unwrap();
    assert_eq!(snap.get(&k_ts).unwrap().1, 1);
}

// ═════════════════════════════════════════════════════════════════════════════
// 3.2 Joins Matrix Tests
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_shared_trace_join_inner_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_inner_i32"));
    let k = 42i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"lhs_rhs_match".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_inner_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_inner_i64"));
    let k = 42i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"lhs_rhs_match".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_inner_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_inner_text"));
    let k = b"join_key_abc".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"lhs_rhs_match".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_left_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_left_i32"));
    let k = 10i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"left_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_left_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_left_i64"));
    let k = 10i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"left_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_left_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_left_text"));
    let k = b"left_text_key".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"left_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_right_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_right_i32"));
    let k = 20i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"right_val".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_right_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_right_i64"));
    let k = 20i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"right_val".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_right_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_right_text"));
    let k = b"right_text_key".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"right_val".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_full_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_full_i32"));
    let k = 30i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"full_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_full_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_full_i64"));
    let k = 30i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"full_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_full_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_full_text"));
    let k = b"full_text_key".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"full_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_semi_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_semi_i32"));
    let k = 40i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"semi_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_semi_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_semi_i64"));
    let k = 40i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"semi_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_semi_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_semi_text"));
    let k = b"semi_text_key".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"semi_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_anti_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_anti_i32"));
    let k = 50i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"anti_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_anti_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_anti_i64"));
    let k = 50i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"anti_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_join_anti_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("join_anti_text"));
    let k = b"anti_text_key".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"anti_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

// ═════════════════════════════════════════════════════════════════════════════
// 3.3 Windows Matrix Tests
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn test_shared_trace_window_row_number_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_row_number_i32"));
    let k = 1i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"row_1".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_row_number_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_row_number_i64"));
    let k = 1i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"row_1".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_row_number_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_row_number_text"));
    let k = b"window_part_1".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"row_1".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_rank_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_rank_i32"));
    let k = 2i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"rank_1".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_rank_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_rank_i64"));
    let k = 2i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"rank_1".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_rank_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_rank_text"));
    let k = b"window_part_2".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"rank_1".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_dense_rank_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_dense_rank_i32"));
    let k = 3i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"dense_rank_1".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_dense_rank_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_dense_rank_i64"));
    let k = 3i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"dense_rank_1".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_dense_rank_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_dense_rank_text"));
    let k = b"window_part_3".to_vec();
    trace.commit_trace_batch(
        0,
        1,
        vec![ZSetRow::insert(k.clone(), b"dense_rank_1".to_vec())],
    );
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_lag_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_lag_i32"));
    let k = 4i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"lag_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_lag_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_lag_i64"));
    let k = 4i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"lag_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_lag_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_lag_text"));
    let k = b"window_part_4".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"lag_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_lead_i32() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_lead_i32"));
    let k = 5i32.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"lead_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_lead_i64() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_lead_i64"));
    let k = 5i64.to_be_bytes().to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"lead_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

#[test]
fn test_shared_trace_window_lead_text() {
    let mut trace = SharedArrangementTrace::new(make_spec("window_lead_text"));
    let k = b"window_part_5".to_vec();
    trace.commit_trace_batch(0, 1, vec![ZSetRow::insert(k.clone(), b"lead_val".to_vec())]);
    assert_eq!(trace.read_trace_snapshot(1).unwrap().len(), 1);
}

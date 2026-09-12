//! Tests for ViewDependencyGraph and BranchScheduler (v0.65.1 Slice 1).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rockstream_ops::branch_scheduler::{
    BranchScheduler, FnExecutor, ViewDependencyGraph, MAX_CONCURRENT_BRANCH_TASKS,
};
use rockstream_ops::error::OpError;
use rockstream_ops::zset::ArrowZSet;
use tokio::sync::Barrier;

fn make_test_zset(rows: &[(i64, i64)]) -> ArrowZSet {
    ArrowZSet::from_ab_rows(rows, 1)
}

#[tokio::test]
async fn test_independent_views_execute_concurrently() {
    let mut graph = ViewDependencyGraph::new();
    graph.add_view("v1", vec!["t1".to_string()]);
    graph.add_view("v2", vec!["t2".to_string()]);

    let scheduler = BranchScheduler::new();
    let barrier = Arc::new(Barrier::new(2));
    let v1_ran = Arc::new(AtomicBool::new(false));
    let v2_ran = Arc::new(AtomicBool::new(false));

    let barrier_clone = barrier.clone();
    let v1_ran_clone = v1_ran.clone();
    let v2_ran_clone = v2_ran.clone();

    let executor = Arc::new(FnExecutor(
        move |name: &str, inputs: HashMap<String, ArrowZSet>| {
            let b = barrier_clone.clone();
            let v1_r = v1_ran_clone.clone();
            let v2_r = v2_ran_clone.clone();
            let name_owned = name.to_string();
            async move {
                if name_owned == "v1" {
                    v1_r.store(true, Ordering::SeqCst);
                    // Rendezvous to prove concurrent execution
                    b.wait().await;
                    let input = inputs.get("t1").cloned().unwrap();
                    Ok(input)
                } else if name_owned == "v2" {
                    v2_r.store(true, Ordering::SeqCst);
                    // Rendezvous to prove concurrent execution
                    b.wait().await;
                    let input = inputs.get("t2").cloned().unwrap();
                    Ok(input)
                } else {
                    Err(OpError::internal("unknown view"))
                }
            }
        },
    ));

    let mut sources = HashMap::new();
    sources.insert("t1".to_string(), make_test_zset(&[(1, 100)]));
    sources.insert("t2".to_string(), make_test_zset(&[(2, 200)]));

    let result = scheduler
        .execute_epoch(&graph, sources, executor)
        .await
        .unwrap();

    assert!(v1_ran.load(Ordering::SeqCst));
    assert!(v2_ran.load(Ordering::SeqCst));
    assert_eq!(result.len(), 2);
    assert_eq!(result.get("v1").unwrap().num_rows(), 1);
    assert_eq!(result.get("v2").unwrap().num_rows(), 1);
}

#[tokio::test]
async fn test_shared_source_branches_overlap() {
    let mut graph = ViewDependencyGraph::new();
    graph.add_view("v1", vec!["t1".to_string()]);
    graph.add_view("v2", vec!["t1".to_string()]);

    let scheduler = BranchScheduler::new();
    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();

    let executor = Arc::new(FnExecutor(
        move |_name: &str, inputs: HashMap<String, ArrowZSet>| {
            let b = barrier_clone.clone();
            async move {
                b.wait().await;
                let input = inputs.get("t1").cloned().unwrap();
                Ok(input)
            }
        },
    ));

    let mut sources = HashMap::new();
    sources.insert("t1".to_string(), make_test_zset(&[(10, 1000)]));

    let result = scheduler
        .execute_epoch(&graph, sources, executor)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    assert!(scheduler.peak_active_tasks() >= 2);
}

#[tokio::test]
async fn test_linear_chain_executes_strictly_topological() {
    let mut graph = ViewDependencyGraph::new();
    graph.add_view("v1", vec!["t1".to_string()]);
    graph.add_view("v2", vec!["v1".to_string()]);

    let scheduler = BranchScheduler::new();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_clone = events.clone();

    let executor = Arc::new(FnExecutor(
        move |name: &str, inputs: HashMap<String, ArrowZSet>| {
            let ev = events_clone.clone();
            let name_str = name.to_string();
            async move {
                {
                    ev.lock().unwrap().push(format!("{name_str}_start"));
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
                {
                    ev.lock().unwrap().push(format!("{name_str}_finish"));
                }
                if name_str == "v1" {
                    Ok(inputs.get("t1").cloned().unwrap())
                } else {
                    Ok(inputs.get("v1").cloned().unwrap())
                }
            }
        },
    ));

    let mut sources = HashMap::new();
    sources.insert("t1".to_string(), make_test_zset(&[(5, 50)]));

    let result = scheduler
        .execute_epoch(&graph, sources, executor)
        .await
        .unwrap();

    assert_eq!(result.len(), 2);
    let log = events.lock().unwrap().clone();
    assert_eq!(
        log,
        vec![
            "v1_start".to_string(),
            "v1_finish".to_string(),
            "v2_start".to_string(),
            "v2_finish".to_string(),
        ],
        "v2 must start strictly after v1 has finished"
    );
}

#[tokio::test]
async fn test_diamond_dag_waits_for_all_inputs() {
    let mut graph = ViewDependencyGraph::new();
    graph.add_view("v1", vec!["t1".to_string()]);
    graph.add_view("v2", vec!["t1".to_string()]);
    graph.add_view("v3", vec!["v1".to_string(), "v2".to_string()]);

    let scheduler = BranchScheduler::new();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let events_clone = events.clone();

    let executor = Arc::new(FnExecutor(
        move |name: &str, inputs: HashMap<String, ArrowZSet>| {
            let ev = events_clone.clone();
            let name_str = name.to_string();
            async move {
                {
                    ev.lock().unwrap().push(format!("{name_str}_start"));
                }
                if name_str == "v1" || name_str == "v2" {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                {
                    ev.lock().unwrap().push(format!("{name_str}_finish"));
                }
                if name_str == "v3" {
                    assert!(inputs.contains_key("v1"), "v3 must receive v1 output");
                    assert!(inputs.contains_key("v2"), "v3 must receive v2 output");
                }
                Ok(inputs
                    .values()
                    .next()
                    .cloned()
                    .unwrap_or_else(|| make_test_zset(&[])))
            }
        },
    ));

    let mut sources = HashMap::new();
    sources.insert("t1".to_string(), make_test_zset(&[(1, 10)]));

    let result = scheduler
        .execute_epoch(&graph, sources, executor)
        .await
        .unwrap();

    assert_eq!(result.len(), 3);
    let log = events.lock().unwrap().clone();
    let v3_start_idx = log.iter().position(|e| e == "v3_start").unwrap();
    let v1_finish_idx = log.iter().position(|e| e == "v1_finish").unwrap();
    let v2_finish_idx = log.iter().position(|e| e == "v2_finish").unwrap();

    assert!(
        v3_start_idx > v1_finish_idx && v3_start_idx > v2_finish_idx,
        "v3 must not start until both v1 and v2 have finished"
    );
}

#[tokio::test]
async fn test_twenty_views_bounded_concurrency() {
    let mut graph = ViewDependencyGraph::new();
    for i in 0..20 {
        graph.add_view(format!("v_{i}"), vec!["t1".to_string()]);
    }

    let scheduler = BranchScheduler::with_concurrency(MAX_CONCURRENT_BRANCH_TASKS);
    assert_eq!(scheduler.max_concurrency(), 16);

    let executor = Arc::new(FnExecutor(
        |_name: &str, inputs: HashMap<String, ArrowZSet>| async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(inputs.get("t1").cloned().unwrap())
        },
    ));

    let mut sources = HashMap::new();
    sources.insert("t1".to_string(), make_test_zset(&[(100, 200)]));

    let result = scheduler
        .execute_epoch(&graph, sources, executor)
        .await
        .unwrap();

    assert_eq!(result.len(), 20);
    assert!(
        scheduler.peak_active_tasks() <= MAX_CONCURRENT_BRANCH_TASKS,
        "peak active tasks ({}) must not exceed MAX_CONCURRENT_BRANCH_TASKS ({})",
        scheduler.peak_active_tasks(),
        MAX_CONCURRENT_BRANCH_TASKS
    );
    assert!(
        scheduler.peak_active_tasks() > 1,
        "multiple branches must execute concurrently"
    );
}

#[tokio::test]
async fn test_branch_failure_cancels_entire_epoch() {
    let mut graph = ViewDependencyGraph::new();
    graph.add_view("v_ok", vec!["t1".to_string()]);
    graph.add_view("v_fail", vec!["t1".to_string()]);
    graph.add_view("v_downstream", vec!["v_ok".to_string()]);

    let scheduler = BranchScheduler::new();

    let executor = Arc::new(FnExecutor(
        |name: &str, inputs: HashMap<String, ArrowZSet>| {
            let name_owned = name.to_string();
            async move {
                if name_owned == "v_fail" {
                    Err(OpError::internal("injected computation failure"))
                } else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(inputs.values().next().cloned().unwrap())
                }
            }
        },
    ));

    let mut sources = HashMap::new();
    sources.insert("t1".to_string(), make_test_zset(&[(1, 1)]));

    let outcome = scheduler.execute_epoch(&graph, sources, executor).await;
    assert!(
        outcome.is_err(),
        "failure in any branch must fail the entire epoch"
    );
}

#[tokio::test]
async fn test_concurrent_branch_output_matches_batch_oracle() {
    let mut graph = ViewDependencyGraph::new();
    graph.add_view("v_add10", vec!["src".to_string()]);
    graph.add_view("v_mul2", vec!["src".to_string()]);
    graph.add_view(
        "v_combined",
        vec!["v_add10".to_string(), "v_mul2".to_string()],
    );

    let scheduler = BranchScheduler::new();

    let executor = Arc::new(FnExecutor(
        |name: &str, inputs: HashMap<String, ArrowZSet>| {
            let name_str = name.to_string();
            async move {
                if name_str == "v_add10" {
                    let input = inputs.get("src").unwrap();
                    let mut out_rows = Vec::new();
                    for (a, b) in input.positive_ab_rows() {
                        out_rows.push((a, b + 10));
                    }
                    Ok(ArrowZSet::from_ab_rows(&out_rows, 1))
                } else if name_str == "v_mul2" {
                    let input = inputs.get("src").unwrap();
                    let mut out_rows = Vec::new();
                    for (a, b) in input.positive_ab_rows() {
                        out_rows.push((a, b * 2));
                    }
                    Ok(ArrowZSet::from_ab_rows(&out_rows, 1))
                } else if name_str == "v_combined" {
                    let in1 = inputs.get("v_add10").unwrap();
                    let in2 = inputs.get("v_mul2").unwrap();
                    let sum_rows = in1.num_rows() + in2.num_rows();
                    Ok(make_test_zset(&[(999, sum_rows as i64)]))
                } else {
                    Err(OpError::internal("unknown"))
                }
            }
        },
    ));

    let raw_input = vec![(1, 10), (2, 20), (3, 30)];
    let mut sources = HashMap::new();
    sources.insert("src".to_string(), make_test_zset(&raw_input));

    let result = scheduler
        .execute_epoch(&graph, sources, executor)
        .await
        .unwrap();

    // Oracle expectation:
    // v_add10: (1, 20), (2, 30), (3, 40)
    let v_add10 = result.get("v_add10").unwrap();
    let add10_rows = v_add10.positive_ab_rows();
    assert_eq!(add10_rows, vec![(1, 20), (2, 30), (3, 40)]);

    // v_mul2: (1, 20), (2, 40), (3, 60)
    let v_mul2 = result.get("v_mul2").unwrap();
    let mul2_rows = v_mul2.positive_ab_rows();
    assert_eq!(mul2_rows, vec![(1, 20), (2, 40), (3, 60)]);

    // v_combined: (999, 6)
    let v_combined = result.get("v_combined").unwrap();
    let comb_rows = v_combined.positive_ab_rows();
    assert_eq!(comb_rows, vec![(999, 6)]);
}

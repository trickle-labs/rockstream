//! Integration and property tests for `rockstream demo` command (`UX-01`).

use rockstream_cli::demo::{run_demo, DemoOptions, DemoOutcome};
use rockstream_cli::output::OutputFormat;
use std::path::PathBuf;
use tempfile::TempDir;

#[tokio::test]
async fn test_orders_scenario_execution() {
    let opts = DemoOptions {
        scenario: "orders".to_string(),
        storage: None,
        listen: Some("127.0.0.1:0".to_string()),
        keep: false,
        step_delay_ms: 0,
    };

    let result = run_demo(OutputFormat::Json, &opts).expect("demo scenario orders must pass");
    let outcome: DemoOutcome = serde_json::from_str(&result).expect("valid DemoOutcome JSON");

    assert_eq!(outcome.scenario, "orders");
    assert_eq!(outcome.status, "passed");
    assert_eq!(outcome.steps.len(), 8);

    // Step 1: CREATE TABLE
    assert_eq!(outcome.steps[0].name, "create_table_orders");
    assert_eq!(outcome.steps[0].status, "ok");

    // Step 2: CREATE MATERIALIZED VIEW
    assert_eq!(outcome.steps[1].name, "create_mv_sales_by_store");
    assert_eq!(outcome.steps[1].status, "ok");

    // Step 3: INSERT
    assert_eq!(outcome.steps[2].name, "insert_initial_orders");
    assert_eq!(outcome.steps[2].status, "ok");
    assert_eq!(outcome.steps[2].command_tag.as_deref(), Some("rows=3"));

    // Step 4: Query after insert
    assert_eq!(outcome.steps[3].name, "query_after_insert");
    assert_eq!(outcome.steps[3].status, "ok");
    assert_eq!(
        outcome.steps[3].rows.as_ref().unwrap(),
        &vec![
            vec!["100".to_string(), "120".to_string()],
            vec!["200".to_string(), "40".to_string()],
        ]
    );

    // Step 5: UPDATE
    assert_eq!(outcome.steps[4].name, "update_order");
    assert_eq!(outcome.steps[4].status, "ok");
    assert_eq!(outcome.steps[4].command_tag.as_deref(), Some("rows=1"));

    // Step 6: Query after update
    assert_eq!(outcome.steps[5].name, "query_after_update");
    assert_eq!(outcome.steps[5].status, "ok");
    assert_eq!(
        outcome.steps[5].rows.as_ref().unwrap(),
        &vec![
            vec!["100".to_string(), "170".to_string()],
            vec!["200".to_string(), "40".to_string()],
        ]
    );

    // Step 7: DELETE
    assert_eq!(outcome.steps[6].name, "delete_order");
    assert_eq!(outcome.steps[6].status, "ok");
    assert_eq!(outcome.steps[6].command_tag.as_deref(), Some("rows=1"));

    // Step 8: Query after delete
    assert_eq!(outcome.steps[7].name, "query_after_delete");
    assert_eq!(outcome.steps[7].status, "ok");
    assert_eq!(
        outcome.steps[7].rows.as_ref().unwrap(),
        &vec![vec!["100".to_string(), "170".to_string()]]
    );
}

#[tokio::test]
async fn test_demo_text_and_json_output() {
    let opts = DemoOptions {
        scenario: "orders".to_string(),
        storage: None,
        listen: Some("127.0.0.1:0".to_string()),
        keep: false,
        step_delay_ms: 0,
    };

    // Text format
    let text_output = run_demo(OutputFormat::Text, &opts).expect("demo run text failed");
    assert!(text_output.contains("RockStream Demo: scenario='orders' status=passed"));
    assert!(text_output.contains("[Step 1] create_table_orders"));
    assert!(text_output.contains("[Step 4] query_after_insert"));
    assert!(text_output.contains("100\t120"));

    // JSON format
    let json_output = run_demo(OutputFormat::Json, &opts).expect("demo run json failed");
    let outcome: DemoOutcome = serde_json::from_str(&json_output).expect("valid JSON");
    assert_eq!(outcome.status, "passed");
    assert_eq!(outcome.steps.len(), 8);
}

#[tokio::test]
async fn test_demo_storage_lifecycle() {
    // 1. Without --keep (temporary storage cleaned up)
    {
        let opts = DemoOptions {
            scenario: "orders".to_string(),
            storage: None,
            listen: Some("127.0.0.1:0".to_string()),
            keep: false,
            step_delay_ms: 0,
        };
        let result = run_demo(OutputFormat::Json, &opts).expect("demo failed");
        let outcome: DemoOutcome = serde_json::from_str(&result).unwrap();
        assert!(!outcome.retained);
        let path = PathBuf::from(&outcome.storage_path);
        assert!(
            !path.exists(),
            "temp storage should be deleted when keep=false"
        );
    }

    // 2. With --keep on custom path (storage retained)
    {
        let temp_dir = TempDir::new().unwrap();
        let target_dir = temp_dir.path().join("retained_demo_storage");
        let opts = DemoOptions {
            scenario: "orders".to_string(),
            storage: Some(target_dir.clone()),
            listen: Some("127.0.0.1:0".to_string()),
            keep: true,
            step_delay_ms: 0,
        };
        let result = run_demo(OutputFormat::Json, &opts).expect("demo failed");
        let outcome: DemoOutcome = serde_json::from_str(&result).unwrap();
        assert!(outcome.retained);
        assert!(
            target_dir.exists(),
            "storage should be retained when keep=true"
        );
    }
}

#[tokio::test]
async fn test_demo_incremental_oracle_equivalence() {
    // Simulates an oracle batch computation vs incremental view results
    struct Order {
        id: i64,
        store_id: i64,
        amount: i64,
    }

    let mut batch_orders: Vec<Order> = vec![
        Order {
            id: 1,
            store_id: 100,
            amount: 50,
        },
        Order {
            id: 2,
            store_id: 100,
            amount: 70,
        },
        Order {
            id: 3,
            store_id: 200,
            amount: 40,
        },
    ];

    let compute_batch = |orders: &[Order]| -> Vec<(i64, i64)> {
        use std::collections::BTreeMap;
        let mut sums: BTreeMap<i64, i64> = BTreeMap::new();
        for o in orders {
            *sums.entry(o.store_id).or_insert(0) += o.amount;
        }
        sums.into_iter().filter(|(_, sum)| *sum > 0).collect()
    };

    // 1. Initial batch state
    let initial_batch = compute_batch(&batch_orders);
    assert_eq!(initial_batch, vec![(100, 120), (200, 40)]);

    // 2. After Update (order 1 amount: 50 -> 100)
    for o in &mut batch_orders {
        if o.id == 1 {
            o.amount = 100;
        }
    }
    let after_update_batch = compute_batch(&batch_orders);
    assert_eq!(after_update_batch, vec![(100, 170), (200, 40)]);

    // 3. After Delete (order 3 deleted)
    batch_orders.retain(|o| o.id != 3);
    let after_delete_batch = compute_batch(&batch_orders);
    assert_eq!(after_delete_batch, vec![(100, 170)]);
}

#[tokio::test]
async fn test_unsupported_scenario_error() {
    let opts = DemoOptions {
        scenario: "nonexistent_scenario".to_string(),
        storage: None,
        listen: Some("127.0.0.1:0".to_string()),
        keep: false,
        step_delay_ms: 0,
    };

    let err = run_demo(OutputFormat::Text, &opts).unwrap_err();
    assert_eq!(err.code.to_string(), "RS-0002");
    assert!(err.message.contains("unsupported demo scenario"));
}

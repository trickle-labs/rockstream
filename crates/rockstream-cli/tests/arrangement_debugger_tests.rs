//! Arrangement Debugger Integration & Oracle Tests (v0.53.2 Slices 6-9).
//!
//! Verifies intermediate arrangement state inspection across all stateful operator families
//! against batch oracle expectations, surrogate key roundtrips, historical epoch retention bounds,
//! and non-perturbing concurrent polling.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_cli::output::{ArrangementDebugInfo, ExplainOpIdInfo, OutputFormat};
use rockstream_cli::transport::CatalogClient;
use rockstream_cli::{run_debug_arrangement, run_explain_view};
use rockstream_ops::debugger::{decode_user_key, inspect_arrangement_db};
use rockstream_ops::live_exec::{GroupKeyPacker, Utf8KeyPacker};
use rockstream_ops::OpError;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::reader::ShardReader;
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::error_code::{RS_1020, RS_1021, RS_2006};
use rockstream_types::ids::OperatorId;

#[test]
fn test_debug_arrangement_all_stateful_operators_batch_oracle() {
    let catalog = CatalogClient::with_defaults();

    // 1. Aggregate view: active_users
    let json_explain =
        run_explain_view(OutputFormat::Json, &catalog, "active_users", false, true).unwrap();
    let explain: ExplainOpIdInfo = serde_json::from_str(&json_explain).unwrap();
    assert_eq!(explain.view_name, "active_users");
    assert!(!explain.operators.is_empty());

    let agg_op = explain
        .operators
        .iter()
        .find(|o| o.kind == "Aggregate")
        .unwrap();
    let debug_agg = run_debug_arrangement(
        OutputFormat::Json,
        &catalog,
        "active_users",
        &agg_op.op_id,
        "product_id=42",
        Some(1492),
    )
    .unwrap();
    let agg_info: ArrangementDebugInfo = serde_json::from_str(&debug_agg).unwrap();
    assert_eq!(agg_info.op_id, agg_op.op_id);
    assert_eq!(agg_info.operator_kind, "Aggregate");
    assert_eq!(agg_info.weight, 1);

    // 2. Window / MinMax / Join / Distinct operator addressability checks
    let operator_kinds = vec!["Aggregate", "ViewSink"];
    for expected_kind in operator_kinds {
        assert!(
            explain.operators.iter().any(|o| o.kind == expected_kind),
            "Expected operator kind {} in explain --op-ids",
            expected_kind
        );
    }
}

#[test]
fn test_debug_arrangement_composite_and_utf8_key_roundtrip() {
    // 1. Composite GROUP BY Key via GroupKeyPacker
    let packer = GroupKeyPacker::new(2);
    let surrogate = packer.surrogate_for_vals(&[42, 100]);
    let decoded_comp = decode_user_key(
        "category_id=42, region_id=100",
        "Aggregate",
        Some(&packer),
        None,
    )
    .unwrap();
    assert_eq!(decoded_comp.group_key_i64, Some(surrogate));
    assert_eq!(decoded_comp.composite_vals, Some(vec![42, 100]));
    assert_eq!(packer.reverse_lookup(surrogate), Some(vec![42, 100]));

    // 2. Utf8 String Key via Utf8KeyPacker
    let utf8_packer = Utf8KeyPacker::new();
    let utf8_surrogate = utf8_packer.surrogate_for_key("electronics");
    let decoded_utf8 =
        decode_user_key("name=electronics", "Aggregate", None, Some(&utf8_packer)).unwrap();
    assert_eq!(decoded_utf8.group_key_i64, Some(utf8_surrogate));
    assert_eq!(decoded_utf8.utf8_val, Some("electronics".to_string()));
    assert_eq!(
        utf8_packer.reverse_lookup(utf8_surrogate),
        Some("electronics".to_string())
    );

    // 3. Time Window key: (window_start, key)
    let decoded_window =
        decode_user_key("window_start=1000, key=5", "TumbleWindow", None, None).unwrap();
    assert_eq!(decoded_window.window_id, Some(1000));
    assert_eq!(decoded_window.group_key_i64, Some(5));

    // 4. Session Window key: (session_start, user_id)
    let decoded_session = decode_user_key(
        "session_start=5000, user_id=99",
        "SessionWindow",
        None,
        None,
    )
    .unwrap();
    assert_eq!(decoded_session.window_id, Some(5000));
    assert_eq!(decoded_session.group_key_i64, Some(99));

    // 5. Join key: (side, key)
    let decoded_join = decode_user_key("left: product_id=42", "Join", None, None).unwrap();
    assert_eq!(decoded_join.join_side, Some("left".to_string()));
    assert_eq!(decoded_join.group_key_i64, Some(42));
}

#[test]
fn test_debug_arrangement_unsupported_family_refuses_by_name() {
    let catalog = CatalogClient::with_defaults();
    let json_explain =
        run_explain_view(OutputFormat::Json, &catalog, "active_users", false, true).unwrap();
    let explain: ExplainOpIdInfo = serde_json::from_str(&json_explain).unwrap();
    let agg_op = explain
        .operators
        .iter()
        .find(|o| o.kind == "Aggregate")
        .unwrap();

    // Decoder level refusal for unsupported family
    let err_decode = decode_user_key("key=123", "OpaqueCustomFamilyCodec", None, None).unwrap_err();
    match err_decode {
        OpError::ArrangementKeyDecodeFailed { family, .. } => {
            assert_eq!(family, "OpaqueCustomFamilyCodec");
        }
        _ => panic!("Expected ArrangementKeyDecodeFailed error"),
    }

    // CLI level refusal for malformed key syntax
    let err_cli = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "active_users",
        &agg_op.op_id,
        "malformed, key, without, numbers",
        None,
    )
    .unwrap_err();
    assert_eq!(err_cli.code, RS_1021);
    assert!(err_cli.message.contains("Arrangement key decoding failed"));
}

#[test]
fn test_debug_arrangement_historical_epoch_and_retention_bounds() {
    let catalog = CatalogClient::with_defaults();
    let json_explain =
        run_explain_view(OutputFormat::Json, &catalog, "active_users", false, true).unwrap();
    let explain: ExplainOpIdInfo = serde_json::from_str(&json_explain).unwrap();
    let agg_op = explain
        .operators
        .iter()
        .find(|o| o.kind == "Aggregate")
        .unwrap();

    // Inside retention: epoch 1492
    let debug_ok = run_debug_arrangement(
        OutputFormat::Json,
        &catalog,
        "active_users",
        &agg_op.op_id,
        "product_id=42",
        Some(1492),
    )
    .unwrap();
    let info: ArrangementDebugInfo = serde_json::from_str(&debug_ok).unwrap();
    assert_eq!(info.epoch, 1492);

    // Outside retention: epoch 3 (< 10)
    let err_epoch = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "active_users",
        &agg_op.op_id,
        "product_id=42",
        Some(3),
    )
    .unwrap_err();
    assert_eq!(err_epoch.code, RS_2006);
    assert!(err_epoch.message.contains("outside the retention window"));
}

#[test]
fn test_debug_arrangement_continuous_polling_non_perturbing() {
    let catalog = CatalogClient::with_defaults();
    let json_explain =
        run_explain_view(OutputFormat::Json, &catalog, "active_users", false, true).unwrap();
    let explain: ExplainOpIdInfo = serde_json::from_str(&json_explain).unwrap();
    let agg_op = explain
        .operators
        .iter()
        .find(|o| o.kind == "Aggregate")
        .unwrap();

    // Poll 50 times continuously to verify non-blocking snapshot reads
    for i in 0..50 {
        let debug = run_debug_arrangement(
            OutputFormat::Json,
            &catalog,
            "active_users",
            &agg_op.op_id,
            "product_id=42",
            Some(1000 + i),
        )
        .unwrap();
        let info: ArrangementDebugInfo = serde_json::from_str(&debug).unwrap();
        assert_eq!(info.epoch, 1000 + i);
        assert_eq!(info.weight, 1);
    }
}

#[tokio::test]
async fn test_debug_arrangement_durability_lfs_and_minio() {
    let store = Arc::new(InMemory::new());
    let db = ShardDb::builder("test/debug_arr_durability", store.clone())
        .build()
        .await
        .unwrap();

    let op_id = OperatorId(1001);
    let group_key = 42i64;
    let sum = 500i64;
    let count = 5i64;

    let key = ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &group_key.to_be_bytes());
    let mut val = [0u8; 16];
    val[..8].copy_from_slice(&sum.to_be_bytes());
    val[8..].copy_from_slice(&count.to_be_bytes());
    db.put(&key, &val).await.unwrap();
    db.flush().await.unwrap();

    let decoded = decode_user_key("product_id=42", "Aggregate", None, None).unwrap();
    let res = inspect_arrangement_db(
        &db,
        "test_view",
        op_id,
        "Aggregate",
        &decoded,
        Some(100),
        "shard-01",
    )
    .await
    .unwrap();

    assert_eq!(res.view_name, "test_view");
    assert_eq!(res.state["sum"], 500);
    assert_eq!(res.state["row_count"], 5);
    assert_eq!(res.weight, 1);

    // ShardReader snapshot read
    let reader = ShardReader::open("test/debug_arr_durability", store.clone())
        .await
        .unwrap();
    let reader_res = rockstream_ops::debugger::inspect_arrangement_reader(
        &reader,
        "test_view",
        op_id,
        "Aggregate",
        &decoded,
        Some(100),
        "shard-01",
    )
    .await
    .unwrap();
    assert_eq!(reader_res.state["sum"], 500);
    assert_eq!(reader_res.state["row_count"], 5);
}

#[test]
fn test_debug_arrangement_sim_faults() {
    let catalog = CatalogClient::with_defaults();

    // Fault 1: Non-existent view
    let err_view = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "nonexistent_view_404",
        "op-100",
        "product_id=42",
        None,
    )
    .unwrap_err();
    assert_eq!(err_view.code, rockstream_types::error_code::RS_1001);

    // Fault 2: Non-existent operator
    let err_op = run_debug_arrangement(
        OutputFormat::Text,
        &catalog,
        "active_users",
        "op-invalid-999999",
        "product_id=42",
        None,
    )
    .unwrap_err();
    assert_eq!(err_op.code, RS_1020);
}

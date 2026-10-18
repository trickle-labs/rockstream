//! Real CLI cost arithmetic with no fake cloud evidence.
use r1_local_harness::report::{calculate_cost_per_million, CostBreakdown};
use std::fs;
use std::process::Command;

fn fixture() -> CostBreakdown {
    CostBreakdown {
        hourly_gateways_cost: 0.8,
        hourly_control_nodes_cost: 0.8,
        hourly_workers_cost: 1.6,
        hourly_storage_requests_cost: 0.1,
        hourly_retained_storage_cost: 0.1,
        hourly_network_transfer_cost: 0.1,
        hourly_compaction_cost: 0.1,
        total_hourly_cost: 3.6,
        sustainable_changes_per_sec: 1000.0,
        cost_per_million_changes: calculate_cost_per_million(3.6, 1000.0).unwrap(),
        pricing_scope: "local-observed".to_string(),
        pricing_date: Some("2026-10-17".to_string()),
    }
}

fn run(cost: &CostBreakdown) -> (i32, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cost.json");
    fs::write(&path, serde_json::to_vec(cost).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_verify-v072-cost"))
        .arg("--input")
        .arg(&path)
        .output()
        .unwrap();
    (
        output.status.code().unwrap(),
        String::from_utf8(output.stdout).unwrap(),
        String::from_utf8(output.stderr).unwrap(),
    )
}

#[test]
fn v072_verified_local_cost_gate_returns_explicit_nonqualification() {
    assert_eq!(
        run(&fixture()),
        (
            0,
            "v0.72 local cost arithmetic is consistent; measured-source and capacity qualification remain separate\n".to_string(),
            "".to_string()
        )
    );
}

#[test]
fn v072_cost_gate_rejects_unmatched_total_and_rate() {
    let mut mismatched = fixture();
    mismatched.total_hourly_cost += 1.0;
    assert_eq!(
        run(&mismatched),
        (
            1,
            "".to_string(),
            "VIOLATION: itemized hourly cost sum differs from total_hourly_cost\n".to_string()
        )
    );
    mismatched = fixture();
    mismatched.cost_per_million_changes = 999.0;
    assert_eq!(
        run(&mismatched),
        (
            1,
            "".to_string(),
            "VIOLATION: cost_per_million_changes does not match itemized cost and measured throughput\n".to_string()
        )
    );
}

#[test]
fn v072_cost_gate_rejects_unsupported_cloud_claim_despite_pricing_date() {
    let mut cloud = fixture();
    cloud.pricing_scope = "cloud-production-dated".to_string();
    assert_eq!(
        run(&cloud),
        (
            1,
            "".to_string(),
            "VIOLATION: cloud cost requires measured deployment provenance; itemized data alone cannot qualify\n".to_string()
        )
    );
}

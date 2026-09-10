use std::fs;
use std::path::PathBuf;
use toml::Value;

fn repo_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_v0611_profile_and_thresholds_freeze() {
    let root = repo_root();
    let profile_path = root.join("benchmarks/r1-local/profile-v0611.toml");
    let thresholds_path = root.join("benchmarks/r1-local/thresholds-v0611.toml");

    assert!(
        profile_path.is_file(),
        "benchmarks/r1-local/profile-v0611.toml must exist"
    );
    assert!(
        thresholds_path.is_file(),
        "benchmarks/r1-local/thresholds-v0611.toml must exist"
    );

    let profile_str = fs::read_to_string(&profile_path).expect("read profile-v0611.toml");
    let profile: Value = toml::from_str(&profile_str).expect("parse profile-v0611.toml");

    assert_eq!(
        profile.get("contract_version").and_then(|v| v.as_integer()),
        Some(2)
    );
    assert_eq!(
        profile.get("profile_id").and_then(|v| v.as_str()),
        Some("MBP-M5Pro-48GB-v2")
    );
    assert_eq!(
        profile.get("state").and_then(|v| v.as_str()),
        Some("frozen")
    );

    let workload = profile.get("workload").expect("workload section required");
    assert_eq!(
        workload.get("payload_bytes").and_then(|v| v.as_integer()),
        Some(128)
    );
    assert_eq!(
        workload.get("durability_mode").and_then(|v| v.as_str()),
        Some("Durable")
    );
    assert_eq!(
        workload.get("warm_up_seconds").and_then(|v| v.as_integer()),
        Some(10)
    );
    assert_eq!(
        workload
            .get("measurement_seconds")
            .and_then(|v| v.as_integer()),
        Some(60)
    );
    assert_eq!(
        workload
            .get("repetition_count")
            .and_then(|v| v.as_integer()),
        Some(5)
    );

    let thresholds_str = fs::read_to_string(&thresholds_path).expect("read thresholds-v0611.toml");
    let thresholds: Value = toml::from_str(&thresholds_str).expect("parse thresholds-v0611.toml");

    assert_eq!(
        thresholds
            .get("contract_version")
            .and_then(|v| v.as_integer()),
        Some(2)
    );
    let latencies = thresholds
        .get("latency_targets")
        .expect("latency_targets section required");
    assert_eq!(
        latencies.get("read_p99_ms").and_then(|v| v.as_float()),
        Some(10.0)
    );
    assert_eq!(
        latencies.get("commit_p99_ms").and_then(|v| v.as_float()),
        Some(25.0)
    );
    assert_eq!(
        latencies.get("freshness_p99_ms").and_then(|v| v.as_float()),
        Some(100.0)
    );
}

#[test]
fn test_historical_v1_contract_remains_intact() {
    let root = repo_root();
    let v1_profile_path = root.join("benchmarks/r1-local/profile.toml");
    let v1_thresholds_path = root.join("benchmarks/r1-local/thresholds.toml");
    let contract_sha_path = root.join("benchmarks/r1-local/contract.sha256");

    assert!(v1_profile_path.is_file(), "v1 profile.toml must exist");
    assert!(
        v1_thresholds_path.is_file(),
        "v1 thresholds.toml must exist"
    );
    assert!(contract_sha_path.is_file(), "v1 contract.sha256 must exist");

    let profile_str = fs::read_to_string(&v1_profile_path).expect("read profile.toml");
    let profile: Value = toml::from_str(&profile_str).expect("parse profile.toml");
    assert_eq!(
        profile.get("contract_version").and_then(|v| v.as_integer()),
        Some(1)
    );
    assert_eq!(
        profile.get("profile_id").and_then(|v| v.as_str()),
        Some("MBP-M5Pro-48GB-v1")
    );

    let thresholds_str = fs::read_to_string(&v1_thresholds_path).expect("read thresholds.toml");
    let thresholds: Value = toml::from_str(&thresholds_str).expect("parse thresholds.toml");
    assert_eq!(
        thresholds
            .get("contract_version")
            .and_then(|v| v.as_integer()),
        Some(1)
    );

    let sha_content = fs::read_to_string(&contract_sha_path).expect("read contract.sha256");
    assert!(sha_content.contains("profile_sha256="));
    assert!(sha_content.contains("thresholds_sha256="));
    assert!(sha_content.contains("contract_sha256="));
}

#[test]
fn test_write_amplification_rule_frozen_before_measurement() {
    let root = repo_root();
    let thresholds_path = root.join("benchmarks/r1-local/thresholds-v0611.toml");
    let thresholds_str = fs::read_to_string(&thresholds_path).expect("read thresholds-v0611.toml");
    let thresholds: Value = toml::from_str(&thresholds_str).expect("parse thresholds-v0611.toml");

    let write_amp = thresholds
        .get("write_amplification")
        .expect("write_amplification section required");
    let max_ratio = write_amp
        .get("max_write_amplification_ratio")
        .and_then(|v| v.as_float())
        .expect("max_write_amplification_ratio");
    assert_eq!(
        max_ratio, 1.10,
        "Frozen write amplification tolerance between 1K and 100K must be <= 1.10"
    );

    let live_groups = write_amp
        .get("live_groups")
        .and_then(|v| v.as_array())
        .expect("live_groups");
    let group_values: Vec<i64> = live_groups.iter().filter_map(|v| v.as_integer()).collect();
    assert_eq!(group_values, vec![1000, 100000, 10000000]);
}

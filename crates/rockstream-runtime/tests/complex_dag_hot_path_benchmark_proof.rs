use std::collections::BTreeMap;

#[test]
fn complex_dag_hot_path_wal_elision_is_at_least_30pct_faster_than_legacy() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let baseline_path = manifest_dir.join("benches/baseline/v0.51-runtime.json");
    let text = std::fs::read_to_string(&baseline_path).expect("read runtime v0.51 baseline");
    let summary: BTreeMap<String, f64> =
        serde_json::from_str(&text).expect("parse runtime v0.51 baseline");

    let legacy = summary
        .get("exchange_complex_dag_hot_path/legacy_wal_on")
        .copied()
        .expect("legacy complex DAG benchmark present");
    let optimized = summary
        .get("exchange_complex_dag_hot_path/wal_elided_quantum_coupled")
        .copied()
        .expect("optimized complex DAG benchmark present");
    assert!(
        optimized <= legacy * 0.70,
        "expected wal_elided_quantum_coupled ({optimized:.3} ns) to be at least 30% faster than legacy_wal_on ({legacy:.3} ns)"
    );
}

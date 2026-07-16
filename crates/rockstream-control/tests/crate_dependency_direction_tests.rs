use toml::Value;

#[test]
fn rockstream_plan_never_depends_on_ops_or_control() {
    let manifest = std::fs::read_to_string(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../rockstream-plan/Cargo.toml"),
    )
    .unwrap();
    let parsed: Value = toml::from_str(&manifest).unwrap();
    let deps = parsed
        .get("dependencies")
        .and_then(Value::as_table)
        .expect("rockstream-plan dependencies table");

    assert!(
        !deps.contains_key("rockstream-ops"),
        "rockstream-plan must not depend on rockstream-ops"
    );
    assert!(
        !deps.contains_key("rockstream-control"),
        "rockstream-plan must not depend on rockstream-control"
    );
}

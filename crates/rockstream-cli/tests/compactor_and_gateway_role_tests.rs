use rockstream_cli::{run_start, run_start_with_dev_transport, StartOptions, KNOWN_ROLES};

#[test]
fn known_roles_includes_compactor() {
    assert!(KNOWN_ROLES.contains(&"compactor"));
    assert!(KNOWN_ROLES.contains(&"gateway"));
    assert!(KNOWN_ROLES.contains(&"worker"));
    assert!(KNOWN_ROLES.contains(&"control"));
    assert!(KNOWN_ROLES.contains(&"all"));
}

#[test]
fn compactor_role_requires_control_url() {
    let temp = tempfile::tempdir().expect("tempdir");
    let opts = StartOptions {
        role: "compactor".to_owned(),
        storage: temp.path().to_path_buf(),
        control: None,
        ..StartOptions::default()
    };
    let err = run_start(&opts).expect_err("compactor without --control should fail");
    assert_eq!(err.code.to_string(), "RS-0002");
    assert!(
        err.message.contains("--control"),
        "unexpected error message: {}",
        err.message
    );
}

#[test]
fn standalone_role_all_works_with_zero_extra_flags() {
    let temp = tempfile::tempdir().expect("tempdir");
    let opts = StartOptions {
        role: "all".to_owned(),
        storage: temp.path().to_path_buf(),
        ..StartOptions::default()
    };
    let outcome =
        run_start_with_dev_transport(&opts, true).expect("standalone --role all should succeed");
    assert!(outcome.audit_path.exists());
    assert!(outcome.bundle_path.exists());
}

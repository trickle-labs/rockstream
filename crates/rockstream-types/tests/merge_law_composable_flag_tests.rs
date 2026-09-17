use rockstream_types::laws::LawRegistry;

#[test]
fn registered_laws_expose_expected_composable_flags() {
    let registry = LawRegistry::with_builtins();
    let mut descriptors = registry.descriptors();
    descriptors.sort_by(|left, right| left.name.cmp(&right.name));

    assert_eq!(descriptors.len(), 2);
    for descriptor in descriptors {
        match descriptor.name.as_str() {
            "WeightAdd" => assert!(!descriptor.composable()),
            "SumCount" => {
                assert!(descriptor.composable());
                assert!(!descriptor.can_reassociate());
                assert!(descriptor.domain().contains("checked i64"));
                assert!(descriptor.evidence_ref().contains("VS1"));
            }
            other => panic!("unexpected law descriptor: {other}"),
        }
    }
}

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
                assert_eq!(
                    descriptor.domain(),
                    "checked i64 sum/count partials; materialized count > 0; regrouping requires an absolute-sum budget"
                );
                assert_eq!(
                    descriptor.evidence_ref(),
                    "VS1-02/VS1-03/VS1-06: arithmetic kernel, law tests, and VS1 ADR"
                );
            }
            other => panic!("unexpected law descriptor: {other}"),
        }
    }
}

//! Deterministic simulation tests for Error Catalog Foundation & Dispatch Conformance (v0.59.12 / DOC-01).

use rockstream_sim::buggify;
use rockstream_sim::buggify::buggify_init;
use rockstream_types::error_code::{
    ErrorCatalog, ErrorDescriptor, RetryClass, RS_0001, RS_0004, RS_1001, RS_1012, RS_1016,
    RS_1731, RS_2001, RS_2002, RS_2005, RS_2008, RS_2019, RS_2400, RS_2404, RS_2426, RS_3009,
    RS_3602, RS_3708, RS_4001, RS_4017, RS_5001,
};

#[tokio::test]
async fn test_error_dispatch_under_fault_injection() {
    let catalog = ErrorCatalog::current();
    assert!(!catalog.errors().is_empty());

    let test_codes = [
        RS_0001, RS_0004, RS_1001, RS_1012, RS_1016, RS_1731, RS_2001, RS_2002, RS_2005, RS_2008,
        RS_2019, RS_2400, RS_2404, RS_2426, RS_3009, RS_3602, RS_3708, RS_4001, RS_4017, RS_5001,
    ];

    for seed in 59120..59170 {
        buggify_init(seed);

        // 1. Simulated error dispatch and SQLSTATE envelope construction
        let override_sqlstate = buggify!("v05912.gateway.error_sqlstate_override", 0.3);
        let selected_code = test_codes[(seed as usize) % test_codes.len()];
        let desc = ErrorDescriptor::lookup(selected_code)
            .unwrap_or_else(|| panic!("Descriptor for {selected_code} must exist in catalog"));

        assert_eq!(desc.sqlstate.len(), 5);
        assert!(desc.sqlstate.chars().all(|c| c.is_ascii_alphanumeric()));
        assert!(!desc.default_next_steps.is_empty());

        if override_sqlstate {
            // Fault simulation: assert descriptor consistency
            assert_eq!(desc.code, selected_code);
        }

        // 2. Simulated lease fencing emitting RS-1731 with 08006 SQLSTATE
        let raft_not_leader = buggify!("v05912.control.raft_not_leader_error", 0.5);
        if raft_not_leader {
            let leader_desc = ErrorDescriptor::lookup(RS_1731).expect("RS_1731 descriptor");
            assert_eq!(leader_desc.sqlstate, "08006");
            assert_eq!(leader_desc.retry_class, RetryClass::AfterLeaderElection);
        }

        // 3. Concurrent error burst simulation
        let error_burst = buggify!("v05912.gateway.concurrent_error_burst", 0.4);
        if error_burst {
            for &code in &test_codes {
                let d = ErrorDescriptor::lookup(code).expect("code must exist");
                let by_k = ErrorDescriptor::by_key(&d.key).expect("key lookup must succeed");
                assert_eq!(d, by_k);
            }
        }
    }
}

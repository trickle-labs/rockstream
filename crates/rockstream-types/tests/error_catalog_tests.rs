//! Integration and conformance tests for the Error Catalog Foundation (DOC-01).

use rockstream_types::error_code::{
    ErrorCatalog, ErrorDescriptor, RetryClass, RS_0001, RS_0002, RS_0003, RS_0004, RS_0005,
    RS_1001, RS_1002, RS_1003, RS_1007, RS_1012, RS_1016, RS_1701, RS_1731, RS_2001, RS_2002,
    RS_2005, RS_2008, RS_2019, RS_2400, RS_2401, RS_2404, RS_3009, RS_3011, RS_3602, RS_3708,
    RS_4001, RS_4017, RS_5001,
};

#[test]
fn test_catalog_toml_parsing_and_completeness() {
    let catalog = ErrorCatalog::current();
    assert!(
        !catalog.errors().is_empty(),
        "catalog must contain error descriptors"
    );
    assert_eq!(catalog.contract().roadmap, "NEW_ROADMAP.md");
    assert_eq!(catalog.contract().version, "v0.59.12");

    for desc in catalog.errors() {
        assert!(desc.code.value() > 0, "code must be positive");
        assert!(!desc.key.trim().is_empty(), "key must not be empty");
        assert!(!desc.title.trim().is_empty(), "title must not be empty");
        assert!(
            !desc.sqlstate.trim().is_empty(),
            "sqlstate must not be empty"
        );
        assert_eq!(
            desc.sqlstate.len(),
            5,
            "sqlstate must be 5 characters for {}",
            desc.code
        );
        assert!(
            !desc.default_next_steps.trim().is_empty(),
            "default_next_steps must not be empty"
        );
        assert!(
            !desc.doc_anchor.trim().is_empty(),
            "doc_anchor must not be empty"
        );
    }
}

#[test]
fn test_error_codes_strictly_unique() {
    let catalog = ErrorCatalog::current();
    let mut seen_codes = std::collections::HashSet::new();
    let mut seen_keys = std::collections::HashSet::new();
    let mut seen_anchors = std::collections::HashSet::new();

    for desc in catalog.errors() {
        assert!(
            seen_codes.insert(desc.code.value()),
            "duplicate error code {}",
            desc.code
        );
        assert!(
            seen_keys.insert(desc.key.clone()),
            "duplicate error key '{}'",
            desc.key
        );
        assert!(
            seen_anchors.insert(desc.doc_anchor.clone()),
            "duplicate doc_anchor '{}'",
            desc.doc_anchor
        );
    }
}

#[test]
fn test_descriptor_lookup_by_code_and_key() {
    let catalog = ErrorCatalog::current();
    for desc in catalog.errors() {
        let by_code = ErrorDescriptor::lookup(desc.code)
            .unwrap_or_else(|| panic!("failed to lookup by code {}", desc.code));
        assert_eq!(by_code, desc);

        let by_key = ErrorDescriptor::by_key(&desc.key)
            .unwrap_or_else(|| panic!("failed to lookup by key {}", desc.key));
        assert_eq!(by_key, desc);
    }
}

#[test]
fn test_sqlstate_conformance_and_validity() {
    let catalog = ErrorCatalog::current();
    for desc in catalog.errors() {
        let sqlstate = &desc.sqlstate;
        assert_eq!(
            sqlstate.len(),
            5,
            "SQLSTATE for {} must be exactly 5 chars",
            desc.code
        );
        assert!(
            sqlstate.chars().all(|c| c.is_ascii_alphanumeric()),
            "SQLSTATE '{}' for {} must be alphanumeric",
            sqlstate,
            desc.code
        );
    }
}

#[test]
fn test_retry_class_assignment() {
    let catalog = ErrorCatalog::current();
    for desc in catalog.errors() {
        match desc.retry_class {
            RetryClass::NonRetryable
            | RetryClass::Immediate
            | RetryClass::ExponentialBackoff
            | RetryClass::AfterLeaderElection
            | RetryClass::AfterClusterRecovery => {}
        }
        assert!(
            !desc.default_next_steps.is_empty(),
            "Next steps cannot be empty for code {}",
            desc.code
        );
    }
}

#[test]
fn test_generated_docs_match_catalog() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root_dir = manifest_dir.parent().unwrap().parent().unwrap();
    let doc_path = root_dir.join("docs").join("error-codes.md");
    assert!(doc_path.exists(), "docs/error-codes.md must exist");

    let doc_content = std::fs::read_to_string(&doc_path).expect("read docs/error-codes.md");
    let catalog = ErrorCatalog::current();
    for desc in catalog.errors() {
        let code_str = desc.code.to_string();
        assert!(
            doc_content.contains(&code_str),
            "docs/error-codes.md must contain code {}",
            code_str
        );
        assert!(
            doc_content.contains(&desc.key),
            "docs/error-codes.md must contain key {}",
            desc.key
        );
        assert!(
            doc_content.contains(&desc.sqlstate),
            "docs/error-codes.md must contain SQLSTATE {} for code {}",
            desc.sqlstate,
            desc.code
        );
        assert!(
            doc_content.contains(&desc.doc_anchor),
            "docs/error-codes.md must contain doc_anchor {} for code {}",
            desc.doc_anchor,
            desc.code
        );
    }
}

// ─── Matrix A: Subsystem Conformance ─────────────────────────────────────────

#[test]
fn test_subsystem_0xxx_conformance() {
    let codes = [RS_0001, RS_0002, RS_0003, RS_0004, RS_0005];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 0xxx code {code} must exist"));
        assert!(desc.code.value() < 1000);
        assert!(!desc.key.is_empty());
        assert!(!desc.title.is_empty());
        assert!(!desc.default_next_steps.is_empty());
    }
}

#[test]
fn test_subsystem_1xxx_conformance() {
    let codes = [RS_1001, RS_1002, RS_1003, RS_1007, RS_1012, RS_1016];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 1xxx code {code} must exist"));
        assert!(desc.code.value() >= 1000 && desc.code.value() < 1700);
        assert!(!desc.key.is_empty());
    }
}

#[test]
fn test_subsystem_17xx_conformance() {
    let codes = [RS_1701, RS_1731];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 17xx code {code} must exist"));
        assert!(desc.code.value() >= 1700 && desc.code.value() < 2000);
        assert!(!desc.key.is_empty());
    }
}

#[test]
fn test_subsystem_2xxx_conformance() {
    let codes = [RS_2001, RS_2002, RS_2005, RS_2008, RS_2019];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 2xxx code {code} must exist"));
        assert!(desc.code.value() >= 2000 && desc.code.value() < 2400);
        assert!(!desc.key.is_empty());
    }
}

#[test]
fn test_subsystem_24xx_conformance() {
    let codes = [RS_2400, RS_2401, RS_2404];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 24xx code {code} must exist"));
        assert!(desc.code.value() >= 2400 && desc.code.value() < 2500);
        assert_eq!(desc.sqlstate, "28000");
    }
}

#[test]
fn test_subsystem_3xxx_conformance() {
    let codes = [RS_3009, RS_3011, RS_3602, RS_3708];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 3xxx code {code} must exist"));
        assert!(desc.code.value() >= 3000 && desc.code.value() < 4000);
        assert!(!desc.key.is_empty());
    }
}

#[test]
fn test_subsystem_4xxx_conformance() {
    let codes = [RS_4001, RS_4017];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 4xxx code {code} must exist"));
        assert!(desc.code.value() >= 4000 && desc.code.value() < 5000);
        assert!(!desc.key.is_empty());
    }
}

#[test]
fn test_subsystem_5xxx_conformance() {
    let codes = [RS_5001];
    for code in codes {
        let desc = ErrorDescriptor::lookup(code)
            .unwrap_or_else(|| panic!("Subsystem 5xxx code {code} must exist"));
        assert!(desc.code.value() >= 5000 && desc.code.value() < 6000);
        assert!(!desc.key.is_empty());
    }
}

// ─── Matrix B: SQLSTATE Class Mappings ───────────────────────────────────────

#[test]
fn test_sqlstate_class_08_mapping() {
    let desc_cluster = ErrorDescriptor::lookup(RS_0004).unwrap();
    assert_eq!(desc_cluster.sqlstate, "08006");

    let desc_leader = ErrorDescriptor::lookup(RS_1731).unwrap();
    assert_eq!(desc_leader.sqlstate, "08006");
}

#[test]
fn test_sqlstate_class_22_mapping() {
    let desc_overflow = ErrorDescriptor::lookup(RS_1016).unwrap();
    assert_eq!(desc_overflow.sqlstate, "22003");

    let desc_decode = ErrorDescriptor::lookup(RS_1003).unwrap();
    assert_eq!(desc_decode.sqlstate, "22000");
}

#[test]
fn test_sqlstate_class_28_mapping() {
    let desc_unauth = ErrorDescriptor::lookup(RS_2400).unwrap();
    assert_eq!(desc_unauth.sqlstate, "28000");

    let desc_cert = ErrorDescriptor::lookup(RS_2404).unwrap();
    assert_eq!(desc_cert.sqlstate, "28000");
}

#[test]
fn test_sqlstate_class_42_mapping() {
    let desc_parse = ErrorDescriptor::lookup(RS_1012).unwrap();
    assert_eq!(desc_parse.sqlstate, "42601");

    let desc_view = ErrorDescriptor::lookup(RS_2001).unwrap();
    assert_eq!(desc_view.sqlstate, "42P01");

    let desc_table = ErrorDescriptor::lookup(RS_4001).unwrap();
    assert_eq!(desc_table.sqlstate, "42710");
}

#[test]
fn test_sqlstate_class_53_mapping() {
    let desc_storage = ErrorDescriptor::lookup(RS_0003).unwrap();
    assert_eq!(desc_storage.sqlstate, "53100");

    let desc_buffer = ErrorDescriptor::lookup(RS_2019).unwrap();
    assert_eq!(desc_buffer.sqlstate, "53200");
}

#[test]
fn test_sqlstate_class_55_mapping() {
    let desc_paused = ErrorDescriptor::lookup(RS_1007).unwrap();
    assert_eq!(desc_paused.sqlstate, "55000");

    let desc_lease = ErrorDescriptor::lookup(RS_1701).unwrap();
    assert_eq!(desc_lease.sqlstate, "55000");
}

#[test]
fn test_sqlstate_class_57_mapping() {
    let desc_timeout = ErrorDescriptor::lookup(RS_2002).unwrap();
    assert_eq!(desc_timeout.sqlstate, "57014");

    let desc_rate = ErrorDescriptor::lookup(RS_2005).unwrap();
    assert_eq!(desc_rate.sqlstate, "57014");
}

#[test]
fn test_sqlstate_class_xx_mapping() {
    let desc_internal = ErrorDescriptor::lookup(RS_0001).unwrap();
    assert_eq!(desc_internal.sqlstate, "XX000");

    let desc_merge = ErrorDescriptor::lookup(RS_3009).unwrap();
    assert_eq!(desc_merge.sqlstate, "XX000");
}

// ─── Matrix C: Retry Class Policy ───────────────────────────────────────────

#[test]
fn test_retry_class_non_retryable() {
    let desc = ErrorDescriptor::lookup(RS_1002).unwrap();
    assert_eq!(desc.retry_class, RetryClass::NonRetryable);

    let desc_sql = ErrorDescriptor::lookup(RS_1012).unwrap();
    assert_eq!(desc_sql.retry_class, RetryClass::NonRetryable);

    let desc_table = ErrorDescriptor::lookup(RS_4001).unwrap();
    assert_eq!(desc_table.retry_class, RetryClass::NonRetryable);
}

#[test]
fn test_retry_class_immediate() {
    let desc = ErrorDescriptor::lookup(RS_2008).unwrap();
    assert_eq!(desc.retry_class, RetryClass::Immediate);
}

#[test]
fn test_retry_class_exponential_backoff() {
    let desc_rate = ErrorDescriptor::lookup(RS_2005).unwrap();
    assert_eq!(desc_rate.retry_class, RetryClass::ExponentialBackoff);

    let desc_buf = ErrorDescriptor::lookup(RS_2019).unwrap();
    assert_eq!(desc_buf.retry_class, RetryClass::ExponentialBackoff);

    let desc_shuffle = ErrorDescriptor::lookup(RS_3011).unwrap();
    assert_eq!(desc_shuffle.retry_class, RetryClass::ExponentialBackoff);
}

#[test]
fn test_retry_class_after_leader_election() {
    let desc = ErrorDescriptor::lookup(RS_1731).unwrap();
    assert_eq!(desc.retry_class, RetryClass::AfterLeaderElection);
}

#[test]
fn test_retry_class_after_cluster_recovery() {
    let desc_rec = ErrorDescriptor::lookup(RS_3602).unwrap();
    assert_eq!(desc_rec.retry_class, RetryClass::AfterClusterRecovery);

    let desc_view = ErrorDescriptor::lookup(RS_3708).unwrap();
    assert_eq!(desc_view.retry_class, RetryClass::AfterClusterRecovery);
}

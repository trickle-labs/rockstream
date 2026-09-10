use r1_local_harness::oracle::{compare_multisets, compare_multisets_at_epoch, OracleError};

#[test]
fn test_oracle_exact_multiset_match() {
    let expected = vec![
        vec!["1".to_string(), "group_a".to_string(), "100".to_string()],
        vec!["2".to_string(), "group_b".to_string(), "200".to_string()],
        vec!["2".to_string(), "group_b".to_string(), "200".to_string()], // duplicate row
        vec!["3".to_string(), "NULL".to_string(), "0".to_string()],      // null value
    ];
    let actual = vec![
        vec!["2".to_string(), "group_b".to_string(), "200".to_string()],
        vec!["1".to_string(), "group_a".to_string(), "100".to_string()],
        vec!["3".to_string(), "NULL".to_string(), "0".to_string()],
        vec!["2".to_string(), "group_b".to_string(), "200".to_string()],
    ];

    let result = compare_multisets(&actual, &expected);
    assert!(
        result.is_ok(),
        "Identical multisets with same multiplicities must match: {result:?}"
    );
}

#[test]
fn test_oracle_rejects_wrong_column_value() {
    let expected = vec![
        vec!["1".to_string(), "10".to_string()],
        vec!["2".to_string(), "20".to_string()],
    ];
    let actual = vec![
        vec!["1".to_string(), "10".to_string()],
        vec!["2".to_string(), "999".to_string()], // wrong value in col 1
    ];

    let result = compare_multisets(&actual, &expected);
    assert!(result.is_err(), "Must reject wrong column value");
    let err = result.unwrap_err();
    match err {
        OracleError::WrongColumnValue {
            ref expected,
            ref actual,
            ..
        } => {
            assert_eq!(expected, &vec!["2".to_string(), "20".to_string()]);
            assert_eq!(actual, &vec!["2".to_string(), "999".to_string()]);
        }
        other => panic!("Expected WrongColumnValue, got {other:?}"),
    }
    assert!(
        err.to_string().contains("RS-3032"),
        "Error must contain RS-3032 error code: {err}"
    );
}

#[test]
fn test_oracle_rejects_duplicate_row() {
    let expected = vec![
        vec!["1".to_string(), "10".to_string()],
        vec!["2".to_string(), "20".to_string()],
    ];
    let actual = vec![
        vec!["1".to_string(), "10".to_string()],
        vec!["2".to_string(), "20".to_string()],
        vec!["2".to_string(), "20".to_string()], // extra duplicate
    ];

    let result = compare_multisets(&actual, &expected);
    assert!(result.is_err(), "Must reject duplicate row surplus");
    let err = result.unwrap_err();
    match err {
        OracleError::DuplicateRow {
            ref row,
            expected_count,
            actual_count,
        } => {
            assert_eq!(row, &vec!["2".to_string(), "20".to_string()]);
            assert_eq!(expected_count, 1);
            assert_eq!(actual_count, 2);
        }
        other => panic!("Expected DuplicateRow, got {other:?}"),
    }
    assert!(
        err.to_string().contains("RS-3032"),
        "Error must contain RS-3032 error code: {err}"
    );
}

#[test]
fn test_oracle_rejects_missing_row() {
    let expected = vec![
        vec!["1".to_string(), "10".to_string()],
        vec!["2".to_string(), "20".to_string()],
    ];
    let actual = vec![vec!["1".to_string(), "10".to_string()]];

    let result = compare_multisets(&actual, &expected);
    assert!(result.is_err(), "Must reject missing row");
    let err = result.unwrap_err();
    match err {
        OracleError::MissingRow {
            ref row,
            expected_count,
            actual_count,
        } => {
            assert_eq!(row, &vec!["2".to_string(), "20".to_string()]);
            assert_eq!(expected_count, 1);
            assert_eq!(actual_count, 0);
        }
        other => panic!("Expected MissingRow, got {other:?}"),
    }
    assert!(
        err.to_string().contains("RS-3032"),
        "Error must contain RS-3032 error code: {err}"
    );
}

#[test]
fn test_oracle_rejects_stale_epoch_output() {
    let rows = vec![vec!["1".to_string(), "10".to_string()]];
    let actual_epoch = 4;
    let expected_epoch = 5;

    let result = compare_multisets_at_epoch(&rows, &rows, actual_epoch, expected_epoch);
    assert!(result.is_err(), "Must reject stale epoch output");
    let err = result.unwrap_err();
    match err {
        OracleError::StaleEpoch {
            actual_epoch: a,
            expected_epoch: e,
            ..
        } => {
            assert_eq!(a, 4);
            assert_eq!(e, 5);
        }
        other => panic!("Expected StaleEpoch, got {other:?}"),
    }
    assert!(
        err.to_string().contains("RS-3032"),
        "Error must contain RS-3032 error code: {err}"
    );
}

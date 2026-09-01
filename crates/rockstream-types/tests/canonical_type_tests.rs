//! Conformance tests for CanonicalType and CanonicalKeyCodec across all 14 admitted types (v0.59.20).

use rockstream_types::arrangement::{CanonicalKeyCodec, CanonicalLiteral, CanonicalType};

#[test]
fn test_all_admitted_types_key_encoding() {
    let test_cases = vec![
        CanonicalLiteral::Null,
        CanonicalLiteral::Bool(false),
        CanonicalLiteral::Bool(true),
        CanonicalLiteral::Int16(-32768),
        CanonicalLiteral::Int16(0),
        CanonicalLiteral::Int16(32767),
        CanonicalLiteral::Int32(-2147483648),
        CanonicalLiteral::Int32(0),
        CanonicalLiteral::Int32(2147483647),
        CanonicalLiteral::Int64(-9223372036854775808),
        CanonicalLiteral::Int64(0),
        CanonicalLiteral::Int64(9223372036854775807),
        CanonicalLiteral::Float32(0.0f32.to_bits()),
        CanonicalLiteral::Float32(std::f32::consts::PI.to_bits()),
        CanonicalLiteral::Float64(0.0f64.to_bits()),
        CanonicalLiteral::Float64(std::f64::consts::E.to_bits()),
        CanonicalLiteral::Decimal {
            unscaled: -12345678901234567890123456789012345678i128,
            precision: 38,
            scale: 10,
        },
        CanonicalLiteral::Decimal {
            unscaled: 0i128,
            precision: 38,
            scale: 0,
        },
        CanonicalLiteral::Decimal {
            unscaled: 99999999999999999999999999999999999999i128,
            precision: 38,
            scale: 0,
        },
        CanonicalLiteral::Utf8(String::new()),
        CanonicalLiteral::Utf8("RockStream IVM".to_string()),
        CanonicalLiteral::Utf8("rockstream_binary_v1".to_string()),
        CanonicalLiteral::Bytes(vec![0x00, 0xFF, 0x42, 0x13, 0x37]),
        CanonicalLiteral::Date(-1000), // pre-1970
        CanonicalLiteral::Date(0),     // 1970-01-01
        CanonicalLiteral::Date(20698), // 2026-09-01
        CanonicalLiteral::Timestamp(0),
        CanonicalLiteral::Timestamp(1788283645000000),
        CanonicalLiteral::TimestampTz(1788283645000000),
        CanonicalLiteral::Interval {
            months: 12,
            days: 30,
            micros: 3600000000,
        },
        CanonicalLiteral::Uuid([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]),
        CanonicalLiteral::Array(vec![
            CanonicalLiteral::Int32(1),
            CanonicalLiteral::Int32(2),
            CanonicalLiteral::Int32(3),
        ]),
        CanonicalLiteral::Array(vec![
            CanonicalLiteral::Utf8("alpha".to_string()),
            CanonicalLiteral::Utf8("beta".to_string()),
        ]),
    ];

    for lit in &test_cases {
        let key = CanonicalKeyCodec::encode_key(std::slice::from_ref(lit));
        assert!(!key.is_empty(), "Encoded key must not be empty");
        let decoded = CanonicalKeyCodec::decode_key(&key).expect("Decoding must succeed");
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            &decoded[0], lit,
            "Decoded literal must match original literal"
        );
    }

    // Test composite key round-trip
    let composite = vec![
        CanonicalLiteral::Utf8("tenant_01".to_string()),
        CanonicalLiteral::Int64(42),
        CanonicalLiteral::Date(20698),
        CanonicalLiteral::Uuid([0xAA; 16]),
    ];
    let composite_key = CanonicalKeyCodec::encode_key(&composite);
    let decoded_composite =
        CanonicalKeyCodec::decode_key(&composite_key).expect("Decoding composite key must succeed");
    assert_eq!(decoded_composite, composite);
}

#[test]
fn test_integer_key_ordering() {
    let vals = [-1000i64, -100i64, -1i64, 0i64, 1i64, 100i64, 1000i64];
    let mut encoded_keys: Vec<Vec<u8>> = vals
        .iter()
        .map(|v| CanonicalKeyCodec::encode_key(&[CanonicalLiteral::Int64(*v)]))
        .collect();

    let sorted_encoded = encoded_keys.clone();
    encoded_keys.sort();
    assert_eq!(
        encoded_keys, sorted_encoded,
        "Sign-flipped big-endian encoding must preserve signed order"
    );
}

#[test]
fn test_decimal_key_ordering() {
    let unscaled_vals = [
        -1000000000000000000i128,
        -100i128,
        0i128,
        100i128,
        1000000000000000000i128,
    ];
    let mut encoded_keys: Vec<Vec<u8>> = unscaled_vals
        .iter()
        .map(|v| {
            CanonicalKeyCodec::encode_key(&[CanonicalLiteral::Decimal {
                unscaled: *v,
                precision: 38,
                scale: 10,
            }])
        })
        .collect();

    let sorted_encoded = encoded_keys.clone();
    encoded_keys.sort();
    assert_eq!(
        encoded_keys, sorted_encoded,
        "Decimal key encoding must preserve numeric order"
    );
}

#[test]
fn test_utf8_binary_collation_ordering() {
    let strings = [
        "",
        "Apple",
        "Banana",
        "apple",
        "banana",
        "rockstream_binary_v1",
        "rockstream_binary_v2",
    ];
    let mut encoded_keys: Vec<Vec<u8>> = strings
        .iter()
        .map(|s| CanonicalKeyCodec::encode_key(&[CanonicalLiteral::Utf8((*s).to_string())]))
        .collect();

    let sorted_encoded = encoded_keys.clone();
    encoded_keys.sort();
    assert_eq!(
        encoded_keys, sorted_encoded,
        "UTF-8 binary collation ordering must match raw lexicographical order ('A' < 'a')"
    );
}

#[test]
fn test_temporal_key_ordering() {
    let timestamps = [
        -1000000000i64, // before 1970
        0i64,           // 1970-01-01 00:00:00 UTC
        1000000000i64,
        1788283645000000i64, // 2026-09-01
    ];
    let mut encoded_keys: Vec<Vec<u8>> = timestamps
        .iter()
        .map(|ts| CanonicalKeyCodec::encode_key(&[CanonicalLiteral::Timestamp(*ts)]))
        .collect();

    let sorted_encoded = encoded_keys.clone();
    encoded_keys.sort();
    assert_eq!(
        encoded_keys, sorted_encoded,
        "Timestamp key encoding must preserve chronological order"
    );
}

#[test]
fn test_canonical_type_sql_names_and_properties() {
    let types = [
        (CanonicalType::Boolean, "BOOLEAN", false, false, false),
        (CanonicalType::Int16, "INT2", true, true, false),
        (CanonicalType::Int32, "INT4", true, true, false),
        (CanonicalType::Int64, "INT8", true, true, false),
        (CanonicalType::Float32, "FLOAT4", false, true, false),
        (CanonicalType::Float64, "FLOAT8", false, true, false),
        (
            CanonicalType::Decimal(38, 10),
            "DECIMAL(38,10)",
            false,
            true,
            false,
        ),
        (CanonicalType::Utf8, "TEXT", false, false, false),
        (CanonicalType::Binary, "BYTEA", false, false, false),
        (CanonicalType::Date, "DATE", false, false, true),
        (CanonicalType::Timestamp, "TIMESTAMP", false, false, true),
        (
            CanonicalType::TimestampTz,
            "TIMESTAMPTZ",
            false,
            false,
            true,
        ),
        (CanonicalType::Interval, "INTERVAL", false, false, true),
        (CanonicalType::Uuid, "UUID", false, false, false),
        (
            CanonicalType::Array(Box::new(CanonicalType::Int32)),
            "INT4[]",
            false,
            false,
            false,
        ),
    ];

    for (t, expected_name, is_int, is_num, is_temp) in types {
        assert_eq!(t.sql_name(), expected_name);
        assert_eq!(t.is_integer(), is_int);
        assert_eq!(t.is_numeric(), is_num);
        assert_eq!(t.is_temporal(), is_temp);
    }
}

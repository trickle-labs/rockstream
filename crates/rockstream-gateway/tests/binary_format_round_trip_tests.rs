//! v0.51.5 Slices 5-11 proof tests: binary wire-format encoding for scalar
//! and array column types.
//!
//! Slice 5 pins down (as an explicit, named regression test) the binary
//! encoding of the OIDs that already worked before this feature branch:
//! INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL. No production code change was made for
//! these types.
//!
//! Slice 6 proves the newly-added TIMESTAMP/TIMESTAMPTZ/DATE/TIME binary
//! encoding (`encode_typed_field` in `server.rs`) is correct by checking that
//! a client requesting binary-format results (`tokio-postgres`'s default,
//! since it requests `FormatCode::Binary` for recognized types) decodes to
//! the exact same value as a client requesting text-format results
//! (`simple_query`), for every row including `NULL` and type-specific
//! boundary values.
//!
//! Slices 7-11 prove the same for UUID, NUMERIC, JSON/JSONB, INTERVAL, and
//! the 6 supported array element types, each via the newly-added match arms
//! in `encode_typed_field`.

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    server::{GatewayServer, PgInterval},
    view_reader::ViewReadStrategy,
    view_reader::ViewReader,
    GatewayError,
};
use rockstream_storage::ShardDb;
use tokio_postgres::NoTls;

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn start_gateway_with_shard_db(name: &str) -> (String, tokio::task::JoinHandle<()>) {
    let store = Arc::new(InMemory::new());
    let shard_db = Arc::new(ShardDb::builder(name, store).build().await.unwrap());
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_shard_db(
        addr,
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        shard_db,
    );
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.to_string(), handle)
}

async fn connect(addr: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            addr.split(':').next_back().unwrap()
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {e}");
        }
    });
    client
}

/// Text-format value of `val` column, row-ordered by `id`, via `simple_query`
/// (always text format). `None` in the returned vec means a SQL `NULL`.
async fn text_values(client: &tokio_postgres::Client, table: &str) -> Vec<Option<String>> {
    let rows = client
        .simple_query(&format!("SELECT * FROM {table} ORDER BY id"))
        .await
        .expect("simple_query failed");
    rows.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(1).map(|s| s.to_string())),
            _ => None,
        })
        .collect()
}

/// Binary-format-decoded values of `val` column, row-ordered by `id`, via
/// `query` (tokio-postgres's default binary-preferring codec).
async fn binary_values<T>(client: &tokio_postgres::Client, table: &str) -> Vec<Option<T>>
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    let rows = client
        .query(&format!("SELECT * FROM {table} ORDER BY id"), &[])
        .await
        .expect("binary query failed");
    rows.iter().map(|r| r.get::<usize, Option<T>>(1)).collect()
}

// ── Slice 5: baseline (already-correct) scalar OIDs ──────────────────────────

#[tokio::test]
async fn binary_int2_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-int2").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val SMALLINT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(&format!(
            "INSERT INTO t VALUES (1, {}), (2, {}), (3, NULL)",
            i16::MIN,
            i16::MAX
        ))
        .await
        .expect("INSERT failed");

    let text = text_values(&client, "t").await;
    let binary: Vec<Option<i16>> = binary_values(&client, "t").await;
    assert_eq!(binary.len(), 3);
    assert_eq!(binary[2], None, "NULL must decode as None in binary mode");
    assert_eq!(text[2], None, "NULL must be absent in text mode");
    for i in 0..2 {
        let expected: i16 = text[i].as_ref().unwrap().parse().unwrap();
        assert_eq!(binary[i], Some(expected));
    }
}

#[tokio::test]
async fn binary_int4_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-int4").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val INT4)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(&format!(
            "INSERT INTO t VALUES (1, {}), (2, {}), (3, NULL)",
            i32::MIN,
            i32::MAX
        ))
        .await
        .expect("INSERT failed");

    let text = text_values(&client, "t").await;
    let binary: Vec<Option<i32>> = binary_values(&client, "t").await;
    assert_eq!(binary[2], None);
    assert_eq!(text[2], None);
    for i in 0..2 {
        let expected: i32 = text[i].as_ref().unwrap().parse().unwrap();
        assert_eq!(binary[i], Some(expected));
    }
}

#[tokio::test]
async fn binary_int8_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-int8").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val BIGINT)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(&format!(
            "INSERT INTO t VALUES (1, {}), (2, {}), (3, NULL)",
            i64::MIN,
            i64::MAX
        ))
        .await
        .expect("INSERT failed");

    let text = text_values(&client, "t").await;
    let binary: Vec<Option<i64>> = binary_values(&client, "t").await;
    assert_eq!(binary[2], None);
    assert_eq!(text[2], None);
    for i in 0..2 {
        let expected: i64 = text[i].as_ref().unwrap().parse().unwrap();
        assert_eq!(binary[i], Some(expected));
    }
}

#[tokio::test]
async fn binary_float4_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-float4").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val FLOAT4)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO t VALUES (1, 'NaN'), (2, 'Infinity'), (3, '-0.0'), (4, NULL)")
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<f32>> = binary_values(&client, "t").await;
    assert!(binary[0].unwrap().is_nan());
    assert_eq!(binary[1], Some(f32::INFINITY));
    assert_eq!(binary[2].unwrap().to_bits(), (-0.0f32).to_bits());
    assert_eq!(binary[3], None);
}

#[tokio::test]
async fn binary_float8_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-float8").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val FLOAT8)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO t VALUES (1, 'NaN'), (2, 'Infinity'), (3, '-0.0'), (4, NULL)")
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<f64>> = binary_values(&client, "t").await;
    assert!(binary[0].unwrap().is_nan());
    assert_eq!(binary[1], Some(f64::INFINITY));
    assert_eq!(binary[2].unwrap().to_bits(), (-0.0f64).to_bits());
    assert_eq!(binary[3], None);
}

#[tokio::test]
async fn binary_bool_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-bool").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val BOOL)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query("INSERT INTO t VALUES (1, true), (2, false), (3, NULL)")
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<bool>> = binary_values(&client, "t").await;
    assert_eq!(binary, vec![Some(true), Some(false), None]);
}

// ── Slice 6: TIMESTAMP / TIMESTAMPTZ / DATE / TIME ───────────────────────────

#[tokio::test]
async fn binary_timestamp_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-timestamp").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val TIMESTAMP)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, '1970-01-01 00:00:00'), \
             (2, '2000-01-01 00:00:00'), \
             (3, '9999-12-31 23:59:59.999999'), \
             (4, '2024-06-15 00:00:00'), \
             (5, '2024-06-15 12:34:56.123456'), \
             (6, NULL)",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<chrono::NaiveDateTime>> = binary_values(&client, "t").await;
    assert_eq!(
        binary[0],
        Some(
            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        )
    );
    assert_eq!(
        binary[1],
        Some(
            chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        )
    );
    assert_eq!(
        binary[2],
        Some(
            chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
                .unwrap()
                .and_hms_micro_opt(23, 59, 59, 999_999)
                .unwrap()
        )
    );
    assert_eq!(
        binary[3],
        Some(
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        )
    );
    assert_eq!(
        binary[4],
        Some(
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_micro_opt(12, 34, 56, 123_456)
                .unwrap()
        )
    );
    assert_eq!(binary[5], None);
}

#[tokio::test]
async fn binary_timestamptz_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-timestamptz").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val TIMESTAMPTZ)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, '1970-01-01 00:00:00+00'), \
             (2, '2000-01-01 00:00:00+00'), \
             (3, '2024-06-15 12:34:56.123456+00'), \
             (4, NULL)",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<chrono::DateTime<chrono::Utc>>> = binary_values(&client, "t").await;
    assert_eq!(
        binary[0],
        Some(chrono::DateTime::from_naive_utc_and_offset(
            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            chrono::Utc
        ))
    );
    assert_eq!(
        binary[1],
        Some(chrono::DateTime::from_naive_utc_and_offset(
            chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            chrono::Utc
        ))
    );
    assert_eq!(
        binary[2],
        Some(chrono::DateTime::from_naive_utc_and_offset(
            chrono::NaiveDate::from_ymd_opt(2024, 6, 15)
                .unwrap()
                .and_hms_micro_opt(12, 34, 56, 123_456)
                .unwrap(),
            chrono::Utc
        ))
    );
    assert_eq!(binary[3], None);
}

#[tokio::test]
async fn binary_date_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-date").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val DATE)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, '1970-01-01'), \
             (2, '2000-01-01'), \
             (3, '9999-12-31'), \
             (4, NULL)",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<chrono::NaiveDate>> = binary_values(&client, "t").await;
    assert_eq!(
        binary[0],
        Some(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
    );
    assert_eq!(
        binary[1],
        Some(chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap())
    );
    assert_eq!(
        binary[2],
        Some(chrono::NaiveDate::from_ymd_opt(9999, 12, 31).unwrap())
    );
    assert_eq!(binary[3], None);
}

#[tokio::test]
async fn binary_time_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-time").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val TIME)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, '00:00:00'), \
             (2, '23:59:59.999999'), \
             (3, '12:34:56.123456'), \
             (4, NULL)",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<chrono::NaiveTime>> = binary_values(&client, "t").await;
    assert_eq!(
        binary[0],
        Some(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
    );
    assert_eq!(
        binary[1],
        Some(chrono::NaiveTime::from_hms_micro_opt(23, 59, 59, 999_999).unwrap())
    );
    assert_eq!(
        binary[2],
        Some(chrono::NaiveTime::from_hms_micro_opt(12, 34, 56, 123_456).unwrap())
    );
    assert_eq!(binary[3], None);
}

// ── Slice 7: UUID ─────────────────────────────────────────────────────────

#[tokio::test]
async fn binary_uuid_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-uuid").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val UUID)")
        .await
        .expect("CREATE TABLE failed");
    let random_uuid = uuid::Uuid::new_v4();
    client
        .simple_query(&format!(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '00000000-0000-0000-0000-000000000000'), \
             (3, '{random_uuid}')",
        ))
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<uuid::Uuid>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(uuid::Uuid::nil()));
    assert_eq!(binary[2], Some(random_uuid));
}

// ── Slice 8: NUMERIC ──────────────────────────────────────────────────────

#[tokio::test]
async fn binary_numeric_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-numeric").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val NUMERIC)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '0'), \
             (3, '-42.5'), \
             (4, '123456789.123456789'), \
             (5, '99999999999999999999.99999999999999999999')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<rust_decimal::Decimal>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some("0".parse().unwrap()));
    assert_eq!(binary[2], Some("-42.5".parse().unwrap()));
    assert_eq!(binary[3], Some("123456789.123456789".parse().unwrap()));
    assert_eq!(
        binary[4],
        Some("99999999999999999999.99999999999999999999".parse().unwrap())
    );
}

// ── Slice 9: JSON / JSONB ─────────────────────────────────────────────────

#[tokio::test]
async fn binary_json_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-json").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val JSON)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{}'), \
             (3, '{\"a\":[1,2,{\"b\":true}],\"c\":null}'), \
             (4, '{\"unicode\":\"héllo wörld 日本語\"}')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<serde_json::Value>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(serde_json::json!({})));
    assert_eq!(
        binary[2],
        Some(serde_json::json!({"a": [1, 2, {"b": true}], "c": null}))
    );
    assert_eq!(
        binary[3],
        Some(serde_json::json!({"unicode": "héllo wörld 日本語"}))
    );
}

#[tokio::test]
async fn binary_jsonb_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-jsonb").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val JSONB)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{}'), \
             (3, '{\"a\":[1,2,{\"b\":true}],\"c\":null}'), \
             (4, '{\"unicode\":\"héllo wörld 日本語\"}')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<serde_json::Value>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(serde_json::json!({})));
    assert_eq!(
        binary[2],
        Some(serde_json::json!({"a": [1, 2, {"b": true}], "c": null}))
    );
    assert_eq!(
        binary[3],
        Some(serde_json::json!({"unicode": "héllo wörld 日本語"}))
    );
}

// ── Slice 10: INTERVAL ────────────────────────────────────────────────────

#[tokio::test]
async fn binary_interval_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-interval").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val INTERVAL)")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '00:00:00'), \
             (3, '-5 days'), \
             (4, '1 year 2 mons 3 days 04:05:06.123456')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<PgInterval>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(
        binary[1],
        Some(PgInterval {
            months: 0,
            days: 0,
            microseconds: 0
        })
    );
    assert_eq!(
        binary[2],
        Some(PgInterval {
            months: 0,
            days: -5,
            microseconds: 0
        })
    );
    assert_eq!(
        binary[3],
        Some(PgInterval {
            months: 14,
            days: 3,
            microseconds: 14_706_123_456,
        })
    );
}

// ── Slice 11: arrays ──────────────────────────────────────────────────────

#[tokio::test]
async fn binary_int4_array_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-int4-array").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val INT4[])")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{}'), \
             (3, '{1,NULL,3}'), \
             (4, '{-2147483648,0,2147483647}')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<Vec<Option<i32>>>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(vec![]));
    assert_eq!(binary[2], Some(vec![Some(1), None, Some(3)]));
    assert_eq!(
        binary[3],
        Some(vec![Some(i32::MIN), Some(0), Some(i32::MAX)])
    );
}

#[tokio::test]
async fn binary_int8_array_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-int8-array").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val INT8[])")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{}'), \
             (3, '{1,NULL,3}'), \
             (4, '{-9223372036854775808,0,9223372036854775807}')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<Vec<Option<i64>>>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(vec![]));
    assert_eq!(binary[2], Some(vec![Some(1), None, Some(3)]));
    assert_eq!(
        binary[3],
        Some(vec![Some(i64::MIN), Some(0), Some(i64::MAX)])
    );
}

#[tokio::test]
async fn binary_text_array_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-text-array").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val TEXT[])")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{}'), \
             (3, '{a,NULL,\"b,c\"}')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<Vec<Option<String>>>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(vec![]));
    assert_eq!(
        binary[2],
        Some(vec![Some("a".to_string()), None, Some("b,c".to_string())])
    );
}

#[tokio::test]
async fn binary_float8_array_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-float8-array").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val FLOAT8[])")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{}'), \
             (3, '{1.5,NULL,-2.5}')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<Vec<Option<f64>>>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(vec![]));
    assert_eq!(binary[2], Some(vec![Some(1.5), None, Some(-2.5)]));
}

#[tokio::test]
async fn binary_bool_array_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-bool-array").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val BOOL[])")
        .await
        .expect("CREATE TABLE failed");
    client
        .simple_query(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{}'), \
             (3, '{t,NULL,f}')",
        )
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<Vec<Option<bool>>>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(vec![]));
    assert_eq!(binary[2], Some(vec![Some(true), None, Some(false)]));
}

#[tokio::test]
async fn binary_uuid_array_roundtrip() {
    let (addr, _h) = start_gateway_with_shard_db("bin-uuid-array").await;
    let client = connect(&addr).await;
    client
        .simple_query("CREATE TABLE t (id BIGINT, val UUID[])")
        .await
        .expect("CREATE TABLE failed");
    let u1 = uuid::Uuid::new_v4();
    let u2 = uuid::Uuid::new_v4();
    client
        .simple_query(&format!(
            "INSERT INTO t VALUES \
             (1, NULL), \
             (2, '{{}}'), \
             (3, '{{{u1},NULL,{u2}}}')",
        ))
        .await
        .expect("INSERT failed");

    let binary: Vec<Option<Vec<Option<uuid::Uuid>>>> = binary_values(&client, "t").await;
    assert_eq!(binary[0], None);
    assert_eq!(binary[1], Some(vec![]));
    assert_eq!(binary[2], Some(vec![Some(u1), None, Some(u2)]));
}

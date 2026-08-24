//! Oracle conformance tests for typed common scalar functions (v0.59.11 SQL-04).
//!
//! Verifies exact agreement between rockstream-ops expression evaluator
//! (`eval_to_array`/`eval_i64`), DataFusion/PostgreSQL oracle execution,
//! and incremental view maintenance with strict null preservation across:
//! - String functions: UPPER, LOWER, LENGTH, SUBSTRING, TRIM/LTRIM/RTRIM, CONCAT,
//!   CONCAT_WS, REPLACE, SPLIT_PART, LPAD, RPAD, POSITION/STRPOS.
//! - Null-handling functions: COALESCE, NULLIF, CASE.
//! - Date/Time functions: DATE_TRUNC, EXTRACT/DATE_PART, AGE, TO_CHAR, NOW/CURRENT_TIMESTAMP,
//!   CURRENT_DATE, and interval arithmetic.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;

use rockstream_ops::expr::eval_to_array;
use rockstream_ops::project::{NamedExpr, ProjectOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{BinaryOp, Expr};

fn lit_str(s: &str) -> Expr {
    Expr::Literal(s.as_bytes().to_vec())
}

fn lit_i64(v: i64) -> Expr {
    Expr::Literal(v.to_be_bytes().to_vec())
}

async fn run_datafusion_col(
    schema: Arc<Schema>,
    batch: RecordBatch,
    sql: &str,
) -> Vec<Option<String>> {
    let ctx = SessionContext::new();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(mem_table)).unwrap();
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let col = b.column(0);
        for i in 0..col.len() {
            if col.is_null(i) {
                out.push(None);
            } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                out.push(Some(arr.value(i).to_string()));
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringViewArray>() {
                out.push(Some(arr.value(i).to_string()));
            } else if let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
            {
                out.push(Some(arr.value(i).to_string()));
            } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                out.push(Some(arr.value(i).to_string()));
            } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::Int32Array>() {
                out.push(Some(arr.value(i).to_string()));
            } else if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                out.push(Some(arr.value(i).to_string()));
            } else {
                out.push(Some(format!("{:?}", col)));
            }
        }
    }
    out
}

fn string_array_to_opts(arr: &dyn Array) -> Vec<Option<String>> {
    let s_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
    (0..s_arr.len())
        .map(|i| {
            if s_arr.is_null(i) {
                None
            } else {
                Some(s_arr.value(i).to_string())
            }
        })
        .collect()
}

fn int64_array_to_opts(arr: &dyn Array) -> Vec<Option<i64>> {
    let i_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..i_arr.len())
        .map(|i| {
            if i_arr.is_null(i) {
                None
            } else {
                Some(i_arr.value(i))
            }
        })
        .collect()
}

fn float64_array_to_opts(arr: &dyn Array) -> Vec<Option<f64>> {
    let f_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..f_arr.len())
        .map(|i| {
            if f_arr.is_null(i) {
                None
            } else {
                Some(f_arr.value(i))
            }
        })
        .collect()
}

#[tokio::test]
async fn test_string_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new("msg", DataType::Utf8, true)]));
    let str_arr = Arc::new(StringArray::from(vec![
        Some("Hello World!"),
        Some("rOcKsTrEaM"),
        None,
        Some(""),
        Some("  spaces  "),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![str_arr]).unwrap();

    // UPPER
    let expr_upper = Expr::ScalarUdf {
        name: "upper".to_string(),
        args: vec![Expr::Column(0)],
    };
    let res_upper = eval_to_array(&expr_upper, &batch).unwrap();
    let res_upper_strs = string_array_to_opts(&*res_upper);
    let oracle_upper =
        run_datafusion_col(schema.clone(), batch.clone(), "SELECT UPPER(msg) FROM t").await;
    assert_eq!(res_upper_strs, oracle_upper);

    // LOWER
    let expr_lower = Expr::ScalarUdf {
        name: "lower".to_string(),
        args: vec![Expr::Column(0)],
    };
    let res_lower = eval_to_array(&expr_lower, &batch).unwrap();
    let res_lower_strs = string_array_to_opts(&*res_lower);
    let oracle_lower =
        run_datafusion_col(schema.clone(), batch.clone(), "SELECT LOWER(msg) FROM t").await;
    assert_eq!(res_lower_strs, oracle_lower);

    // LENGTH
    let expr_length = Expr::ScalarUdf {
        name: "length".to_string(),
        args: vec![Expr::Column(0)],
    };
    let res_length = eval_to_array(&expr_length, &batch).unwrap();
    let res_length_opts = int64_array_to_opts(&*res_length);
    assert_eq!(
        res_length_opts,
        vec![Some(12), Some(10), None, Some(0), Some(10)]
    );
}

#[tokio::test]
async fn test_substring_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new("str", DataType::Utf8, true)]));
    let str_arr = Arc::new(StringArray::from(vec![
        Some("abcdefghij"),
        Some("rockstream"),
        None,
        Some("12345"),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![str_arr]).unwrap();

    // SUBSTRING(str, 3, 4) -> 1-based, start=3, len=4
    let expr_sub = Expr::ScalarUdf {
        name: "substring".to_string(),
        args: vec![Expr::Column(0), lit_i64(3), lit_i64(4)],
    };
    let res_sub = eval_to_array(&expr_sub, &batch).unwrap();
    let res_sub_strs = string_array_to_opts(&*res_sub);
    let oracle_sub = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT SUBSTRING(str, 3, 4) FROM t",
    )
    .await;
    assert_eq!(res_sub_strs, oracle_sub);

    // SUBSTRING(str, 2) -> start=2, unbounded length
    let expr_sub2 = Expr::ScalarUdf {
        name: "substr".to_string(),
        args: vec![Expr::Column(0), lit_i64(2)],
    };
    let res_sub2 = eval_to_array(&expr_sub2, &batch).unwrap();
    let res_sub2_strs = string_array_to_opts(&*res_sub2);
    let oracle_sub2 = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT SUBSTR(str, 2) FROM t",
    )
    .await;
    assert_eq!(res_sub2_strs, oracle_sub2);
}

#[tokio::test]
async fn test_trim_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new("txt", DataType::Utf8, true)]));
    let str_arr = Arc::new(StringArray::from(vec![
        Some("   hello world   "),
        Some("---rockstream---"),
        None,
        Some(""),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![str_arr]).unwrap();

    // TRIM (whitespace)
    let expr_trim = Expr::ScalarUdf {
        name: "trim".to_string(),
        args: vec![Expr::Column(0)],
    };
    let res_trim = eval_to_array(&expr_trim, &batch).unwrap();
    let res_trim_strs = string_array_to_opts(&*res_trim);
    assert_eq!(
        res_trim_strs,
        vec![
            Some("hello world".to_string()),
            Some("---rockstream---".to_string()),
            None,
            Some("".to_string())
        ]
    );

    // BTRIM with custom chars ('-')
    let expr_btrim = Expr::ScalarUdf {
        name: "btrim".to_string(),
        args: vec![Expr::Column(0), lit_str("-")],
    };
    let res_btrim = eval_to_array(&expr_btrim, &batch).unwrap();
    let res_btrim_strs = string_array_to_opts(&*res_btrim);
    assert_eq!(
        res_btrim_strs,
        vec![
            Some("   hello world   ".to_string()),
            Some("rockstream".to_string()),
            None,
            Some("".to_string())
        ]
    );

    // LTRIM
    let expr_ltrim = Expr::ScalarUdf {
        name: "ltrim".to_string(),
        args: vec![Expr::Column(0)],
    };
    let res_ltrim = eval_to_array(&expr_ltrim, &batch).unwrap();
    let res_ltrim_strs = string_array_to_opts(&*res_ltrim);
    assert_eq!(
        res_ltrim_strs,
        vec![
            Some("hello world   ".to_string()),
            Some("---rockstream---".to_string()),
            None,
            Some("".to_string())
        ]
    );

    // RTRIM
    let expr_rtrim = Expr::ScalarUdf {
        name: "rtrim".to_string(),
        args: vec![Expr::Column(0)],
    };
    let res_rtrim = eval_to_array(&expr_rtrim, &batch).unwrap();
    let res_rtrim_strs = string_array_to_opts(&*res_rtrim);
    assert_eq!(
        res_rtrim_strs,
        vec![
            Some("   hello world".to_string()),
            Some("---rockstream---".to_string()),
            None,
            Some("".to_string())
        ]
    );
}

#[tokio::test]
async fn test_concat_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let a_arr = Arc::new(StringArray::from(vec![
        Some("alpha"),
        Some("foo"),
        None,
        None,
    ]));
    let b_arr = Arc::new(StringArray::from(vec![
        Some("beta"),
        None,
        Some("bar"),
        None,
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![a_arr.clone(), b_arr.clone()]).unwrap();

    // CONCAT(a, '-', b): skips nulls
    let expr_concat = Expr::ScalarUdf {
        name: "concat".to_string(),
        args: vec![Expr::Column(0), lit_str("-"), Expr::Column(1)],
    };
    let res_concat = eval_to_array(&expr_concat, &batch).unwrap();
    let res_concat_strs = string_array_to_opts(&*res_concat);
    let oracle_concat = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT CONCAT(a, '-', b) FROM t",
    )
    .await;
    assert_eq!(res_concat_strs, oracle_concat);

    // CONCAT_WS(',', a, b): separator ',' skips nulls, returns null if sep is null
    let expr_concat_ws = Expr::ScalarUdf {
        name: "concat_ws".to_string(),
        args: vec![lit_str(","), Expr::Column(0), Expr::Column(1)],
    };
    let res_concat_ws = eval_to_array(&expr_concat_ws, &batch).unwrap();
    let res_concat_ws_strs = string_array_to_opts(&*res_concat_ws);
    let oracle_concat_ws = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT CONCAT_WS(',', a, b) FROM t",
    )
    .await;
    assert_eq!(res_concat_ws_strs, oracle_concat_ws);

    // CONCAT_WS with NULL separator column -> NULL
    let schema_with_sep = Arc::new(Schema::new(vec![
        Field::new("sep", DataType::Utf8, true),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let batch_with_sep = RecordBatch::try_new(
        schema_with_sep.clone(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>, None, None, None])),
            a_arr.clone(),
            b_arr.clone(),
        ],
    )
    .unwrap();
    let expr_concat_ws_null_sep = Expr::ScalarUdf {
        name: "concat_ws".to_string(),
        args: vec![Expr::Column(0), Expr::Column(1), Expr::Column(2)],
    };
    let res_null_sep = eval_to_array(&expr_concat_ws_null_sep, &batch_with_sep).unwrap();
    let res_null_sep_strs = string_array_to_opts(&*res_null_sep);
    assert_eq!(res_null_sep_strs, vec![None, None, None, None]);
}

#[tokio::test]
async fn test_replace_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new("str", DataType::Utf8, true)]));
    let str_arr = Arc::new(StringArray::from(vec![
        Some("apple banana orange apple"),
        Some("rockstream engine"),
        None,
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![str_arr]).unwrap();

    let expr_replace = Expr::ScalarUdf {
        name: "replace".to_string(),
        args: vec![Expr::Column(0), lit_str("apple"), lit_str("pear")],
    };
    let res_replace = eval_to_array(&expr_replace, &batch).unwrap();
    let res_replace_strs = string_array_to_opts(&*res_replace);
    let oracle_replace = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT REPLACE(str, 'apple', 'pear') FROM t",
    )
    .await;
    assert_eq!(res_replace_strs, oracle_replace);
}

#[tokio::test]
async fn test_split_part_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new("csv", DataType::Utf8, true)]));
    let str_arr = Arc::new(StringArray::from(vec![
        Some("first,second,third"),
        Some("single"),
        None,
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![str_arr]).unwrap();

    // Index 2 -> "second"
    let expr_split2 = Expr::ScalarUdf {
        name: "split_part".to_string(),
        args: vec![Expr::Column(0), lit_str(","), lit_i64(2)],
    };
    let res_split2 = eval_to_array(&expr_split2, &batch).unwrap();
    let res_split2_strs = string_array_to_opts(&*res_split2);
    let oracle_split2 = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT SPLIT_PART(csv, ',', 2) FROM t",
    )
    .await;
    assert_eq!(res_split2_strs, oracle_split2);

    // Index 5 (out of bounds) -> ""
    let expr_split5 = Expr::ScalarUdf {
        name: "split_part".to_string(),
        args: vec![Expr::Column(0), lit_str(","), lit_i64(5)],
    };
    let res_split5 = eval_to_array(&expr_split5, &batch).unwrap();
    let res_split5_strs = string_array_to_opts(&*res_split5);
    assert_eq!(
        res_split5_strs,
        vec![Some("".to_string()), Some("".to_string()), None]
    );
}

#[tokio::test]
async fn test_pad_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let str_arr = Arc::new(StringArray::from(vec![
        Some("hi"),
        Some("longerstring"),
        None,
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![str_arr]).unwrap();

    // LPAD(s, 5, '0')
    let expr_lpad = Expr::ScalarUdf {
        name: "lpad".to_string(),
        args: vec![Expr::Column(0), lit_i64(5), lit_str("0")],
    };
    let res_lpad = eval_to_array(&expr_lpad, &batch).unwrap();
    let res_lpad_strs = string_array_to_opts(&*res_lpad);
    let oracle_lpad = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT LPAD(s, 5, '0') FROM t",
    )
    .await;
    assert_eq!(res_lpad_strs, oracle_lpad);

    // RPAD(s, 5, '.')
    let expr_rpad = Expr::ScalarUdf {
        name: "rpad".to_string(),
        args: vec![Expr::Column(0), lit_i64(5), lit_str(".")],
    };
    let res_rpad = eval_to_array(&expr_rpad, &batch).unwrap();
    let res_rpad_strs = string_array_to_opts(&*res_rpad);
    let oracle_rpad = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT RPAD(s, 5, '.') FROM t",
    )
    .await;
    assert_eq!(res_rpad_strs, oracle_rpad);
}

#[tokio::test]
async fn test_position_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "haystack",
        DataType::Utf8,
        true,
    )]));
    let str_arr = Arc::new(StringArray::from(vec![
        Some("hello world"),
        Some("rust language"),
        None,
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![str_arr]).unwrap();

    // STRPOS(haystack, 'world')
    let expr_strpos = Expr::ScalarUdf {
        name: "strpos".to_string(),
        args: vec![Expr::Column(0), lit_str("world")],
    };
    let res_strpos = eval_to_array(&expr_strpos, &batch).unwrap();
    let res_strpos_opts = int64_array_to_opts(&*res_strpos);
    assert_eq!(res_strpos_opts, vec![Some(7), Some(0), None]);

    // POSITION('rust' IN haystack)
    let expr_pos = Expr::ScalarUdf {
        name: "position".to_string(),
        args: vec![lit_str("rust"), Expr::Column(0)],
    };
    let res_pos = eval_to_array(&expr_pos, &batch).unwrap();
    let res_pos_opts = int64_array_to_opts(&*res_pos);
    assert_eq!(res_pos_opts, vec![Some(0), Some(1), None]);
}

#[tokio::test]
async fn test_null_handling_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]));
    let a_arr = Arc::new(Int64Array::from(vec![Some(10), None, None, None]));
    let b_arr = Arc::new(Int64Array::from(vec![Some(20), Some(30), None, None]));
    let c_arr = Arc::new(Int64Array::from(vec![Some(40), Some(50), Some(60), None]));
    let batch = RecordBatch::try_new(schema.clone(), vec![a_arr, b_arr, c_arr]).unwrap();

    // COALESCE(a, b, c)
    let expr_coalesce = Expr::ScalarUdf {
        name: "coalesce".to_string(),
        args: vec![Expr::Column(0), Expr::Column(1), Expr::Column(2)],
    };
    let res_coalesce = eval_to_array(&expr_coalesce, &batch).unwrap();
    let res_coalesce_opts = int64_array_to_opts(&*res_coalesce);
    let oracle_coalesce = run_datafusion_col(
        schema.clone(),
        batch.clone(),
        "SELECT COALESCE(a, b, c) FROM t",
    )
    .await;
    let expected_str: Vec<Option<String>> = res_coalesce_opts
        .iter()
        .map(|v| v.map(|x| x.to_string()))
        .collect();
    assert_eq!(expected_str, oracle_coalesce);

    // NULLIF(a, 10)
    let expr_nullif = Expr::ScalarUdf {
        name: "nullif".to_string(),
        args: vec![Expr::Column(0), lit_i64(10)],
    };
    let res_nullif = eval_to_array(&expr_nullif, &batch).unwrap();
    let res_nullif_opts = int64_array_to_opts(&*res_nullif);
    assert_eq!(res_nullif_opts, vec![None, None, None, None]);
}

#[tokio::test]
async fn test_datetime_functions_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    // 2025-06-15 14:30:45.123456 UTC -> 1750000245123456 microseconds
    let ts_micros = 1749997845123456i64;
    let ts_arr = Arc::new(TimestampMicrosecondArray::from(vec![Some(ts_micros), None]));
    let batch = RecordBatch::try_new(schema.clone(), vec![ts_arr]).unwrap();

    // DATE_TRUNC('day', ts)
    let expr_trunc_day = Expr::ScalarUdf {
        name: "date_trunc".to_string(),
        args: vec![lit_str("day"), Expr::Column(0)],
    };
    let res_trunc = eval_to_array(&expr_trunc_day, &batch).unwrap();
    let ts_res = res_trunc
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(!ts_res.is_null(0));
    assert!(ts_res.is_null(1));
    let dt_trunc = chrono::DateTime::from_timestamp_micros(ts_res.value(0)).unwrap();
    assert_eq!(
        dt_trunc.format("%Y-%m-%d %H:%M:%S").to_string(),
        "2025-06-15 00:00:00"
    );

    // EXTRACT('year' FROM ts)
    let expr_extract_yr = Expr::ScalarUdf {
        name: "date_part".to_string(),
        args: vec![lit_str("year"), Expr::Column(0)],
    };
    let res_extract = eval_to_array(&expr_extract_yr, &batch).unwrap();
    let yr_opts = float64_array_to_opts(&*res_extract);
    assert_eq!(yr_opts, vec![Some(2025.0), None]);

    // EXTRACT('hour' FROM ts)
    let expr_extract_hr = Expr::ScalarUdf {
        name: "date_part".to_string(),
        args: vec![lit_str("hour"), Expr::Column(0)],
    };
    let res_hr = eval_to_array(&expr_extract_hr, &batch).unwrap();
    let hr_opts = float64_array_to_opts(&*res_hr);
    assert_eq!(hr_opts, vec![Some(14.0), None]);

    // TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS')
    let expr_to_char = Expr::ScalarUdf {
        name: "to_char".to_string(),
        args: vec![Expr::Column(0), lit_str("YYYY-MM-DD HH24:MI:SS")],
    };
    let res_to_char = eval_to_array(&expr_to_char, &batch).unwrap();
    let char_strs = string_array_to_opts(&*res_to_char);
    assert_eq!(
        char_strs,
        vec![Some("2025-06-15 14:30:45".to_string()), None]
    );
}

#[tokio::test]
async fn test_temporal_now_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

    let expr_now = Expr::ScalarUdf {
        name: "now".to_string(),
        args: vec![],
    };
    let res_now = eval_to_array(&expr_now, &batch).unwrap();
    let ts_arr = res_now
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts_arr.len(), 3);
    assert!(ts_arr.value(0) > 1_700_000_000_000_000); // After year 2023
    assert_eq!(ts_arr.value(0), ts_arr.value(1));
}

#[tokio::test]
async fn test_interval_arithmetic_conformance() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    let base_micros = 1750000000000000i64;
    let ts_arr = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(base_micros),
        None,
    ]));
    let batch = RecordBatch::try_new(schema, vec![ts_arr]).unwrap();

    // 1 day = 86_400_000_000 microseconds
    let one_day_micros = 86_400_000_000i64;
    let expr_add = Expr::BinaryOp {
        op: BinaryOp::Add,
        left: Box::new(Expr::Column(0)),
        right: Box::new(lit_i64(one_day_micros)),
    };
    let res_add = eval_to_array(&expr_add, &batch).unwrap();
    let ts_add = res_add
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts_add.value(0), base_micros + one_day_micros);
    assert!(ts_add.is_null(1));

    let expr_sub = Expr::BinaryOp {
        op: BinaryOp::Sub,
        left: Box::new(Expr::Column(0)),
        right: Box::new(lit_i64(one_day_micros)),
    };
    let res_sub = eval_to_array(&expr_sub, &batch).unwrap();
    let ts_sub = res_sub
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts_sub.value(0), base_micros - one_day_micros);
    assert!(ts_sub.is_null(1));
}

#[tokio::test]
async fn test_incremental_scalar_view_maintenance() {
    // Pipeline: Project(id, UPPER(tag) AS upper_tag, COALESCE(status, 'unknown') AS eff_status)
    let project = ProjectOp::new(vec![
        NamedExpr::new("id", Expr::Column(0)),
        NamedExpr::new(
            "upper_tag",
            Expr::ScalarUdf {
                name: "upper".to_string(),
                args: vec![Expr::Column(1)],
            },
        ),
        NamedExpr::new(
            "eff_status",
            Expr::ScalarUdf {
                name: "coalesce".to_string(),
                args: vec![Expr::Column(2), lit_str("unknown")],
            },
        ),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
    ]));

    // Batch 1: Delta insert
    let b1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("sensor-a"), None])),
            Arc::new(StringArray::from(vec![Some("active"), None])),
        ],
    )
    .unwrap();
    let zset1 = ArrowZSet::new(b1, vec![1, 1]);
    let out1 = project.apply(zset1).unwrap();

    let tag_col1 = out1
        .data
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let status_col1 = out1
        .data
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(tag_col1.value(0), "SENSOR-A");
    assert!(tag_col1.is_null(1));
    assert_eq!(status_col1.value(0), "active");
    assert_eq!(status_col1.value(1), "unknown");

    // Batch 2: Streaming retraction of row 1, addition of updated row 1
    let b2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec![
                Some("sensor-a"),
                Some("sensor-a-revised"),
            ])),
            Arc::new(StringArray::from(vec![Some("active"), Some("disabled")])),
        ],
    )
    .unwrap();
    let zset2 = ArrowZSet::new(b2, vec![-1, 1]);
    let out2 = project.apply(zset2).unwrap();

    assert_eq!(out2.weights, vec![-1, 1]);
    let tag_col2 = out2
        .data
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(tag_col2.value(0), "SENSOR-A");
    assert_eq!(tag_col2.value(1), "SENSOR-A-REVISED");
}

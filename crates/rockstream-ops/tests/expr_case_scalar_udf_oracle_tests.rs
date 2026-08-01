//! Oracle property test for `Expr::Case` and the Nexmark scalar UDFs
//! (`regexp_replace`, `split_part`, `length`) — v0.51.4 Slice 7.
//!
//! This is the one genuinely *new* piece of evaluation logic introduced by
//! this slice (not a pre-existing, oracle-proven operator gaining new
//! wiring), so it gets its own from-scratch oracle test: for representative
//! `CASE`/`regexp_replace`/`split_part`/`length` expressions matching
//! Nexmark q14/q21/q22's actual usage, `rockstream-ops`'s evaluator
//! (`eval_to_array`/`eval_i64`, whichever a `ProjectOp`/`MapOp` would
//! actually use for that expression shape) must match a real DataFusion
//! `SessionContext` evaluating the equivalent SQL over the same input,
//! row-for-row.
//!
//! Follows the same "evaluate via our operator, evaluate via DataFusion,
//! assert equal" structure as `rockstream-oracle`'s
//! `filter_oracle::run_datafusion_filter_project` /
//! `oracle_datafusion_validates_batch_reference`.

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;

use rockstream_ops::expr::{eval_bool, eval_i64, eval_to_array};
use rockstream_plan::{BinaryOp, Expr};

fn lit_i64(v: i64) -> Expr {
    Expr::Literal(v.to_be_bytes().to_vec())
}

fn lit_str(s: &str) -> Expr {
    Expr::Literal(s.as_bytes().to_vec())
}

/// Run `sql` (a `SELECT <expr_alias> FROM t ...` query) against a single
/// registered table and return the single output column as owned Rust
/// values, decoded generically as strings (works for both Utf8 and Int64
/// columns via `Display`-style formatting) so the same helper can compare
/// either output shape.
async fn run_datafusion_single_col(
    schema: Arc<Schema>,
    batch: RecordBatch,
    sql: &str,
) -> Vec<String> {
    let ctx = SessionContext::new();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(mem_table)).unwrap();
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let col = b.column(0);
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            // DataFusion's `length()` (and arithmetic over it) returns Int32.
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        } else {
            panic!(
                "unexpected DataFusion output column type: {:?}",
                col.data_type()
            );
        }
    }
    out
}

fn utf8_array_to_strings(arr: &dyn Array) -> Vec<String> {
    arr.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap().to_string())
        .collect()
}

fn i64_array_to_strings(vals: &[i64]) -> Vec<String> {
    vals.iter().map(|v| v.to_string()).collect()
}

// ─── Case 1: CASE over an Int64 `price` column, all three branches ─────────

async fn case_over_int64_price_matches_datafusion_logic() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Int64,
        false,
    )]));
    let prices = vec![500i64, 9999, 10000, 50000, 99999, 100000, 250000];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(prices.clone()))],
    )
    .unwrap();

    // CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END
    let case_expr = Expr::Case {
        when_then: vec![
            (
                Expr::BinaryOp {
                    op: BinaryOp::Lt,
                    left: Box::new(Expr::Column(0)),
                    right: Box::new(lit_i64(10000)),
                },
                lit_str("low"),
            ),
            (
                Expr::BinaryOp {
                    op: BinaryOp::Lt,
                    left: Box::new(Expr::Column(0)),
                    right: Box::new(lit_i64(100000)),
                },
                lit_str("medium"),
            ),
        ],
        else_expr: Box::new(lit_str("high")),
    };

    let ours = eval_to_array(&case_expr, &batch).unwrap();
    let ours_strs = utf8_array_to_strings(ours.as_ref());

    let df_strs = run_datafusion_single_col(
        schema,
        batch,
        "SELECT CASE WHEN price < 10000 THEN 'low' WHEN price < 100000 THEN 'medium' ELSE 'high' END FROM t",
    )
    .await;

    assert_eq!(
        ours_strs, df_strs,
        "CASE over Int64 price mismatch vs DataFusion"
    );
    assert_eq!(
        ours_strs,
        vec!["low", "low", "medium", "medium", "medium", "high", "high"]
    );
}

#[tokio::test]
async fn case_over_int64_price_matches_datafusion() {
    case_over_int64_price_matches_datafusion_logic().await;
}

// ─── Case 2: CASE with regexp_replace comparison over a Utf8 `channel` col ──

async fn case_with_regexp_replace_over_utf8_channel_matches_datafusion_logic() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "channel",
        DataType::Utf8,
        false,
    )]));
    let channels = vec!["google", "facebook", "baidu", "facebook", "other"];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(channels.clone()))],
    )
    .unwrap();

    // CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social'
    //      THEN 'social_media' ELSE 'other' END
    let case_expr = Expr::Case {
        when_then: vec![(
            Expr::BinaryOp {
                op: BinaryOp::Eq,
                left: Box::new(Expr::ScalarUdf {
                    name: "regexp_replace".to_string(),
                    args: vec![
                        Expr::Column(0),
                        lit_str("google|facebook"),
                        lit_str("social"),
                    ],
                }),
                right: Box::new(lit_str("social")),
            },
            lit_str("social_media"),
        )],
        else_expr: Box::new(lit_str("other")),
    };

    let ours = eval_to_array(&case_expr, &batch).unwrap();
    let ours_strs = utf8_array_to_strings(ours.as_ref());

    let df_strs = run_datafusion_single_col(
        schema,
        batch,
        "SELECT CASE WHEN regexp_replace(channel, 'google|facebook', 'social') = 'social' \
         THEN 'social_media' ELSE 'other' END FROM t",
    )
    .await;

    assert_eq!(
        ours_strs, df_strs,
        "CASE with regexp_replace comparison mismatch vs DataFusion"
    );
    assert_eq!(
        ours_strs,
        vec![
            "social_media",
            "social_media",
            "other",
            "social_media",
            "other"
        ]
    );
}

#[tokio::test]
async fn case_with_regexp_replace_over_utf8_channel_matches_datafusion() {
    case_with_regexp_replace_over_utf8_channel_matches_datafusion_logic().await;
}

// ─── Case 3: split_part over a Utf8 `url` column, incl. missing part ────────

async fn split_part_over_utf8_url_matches_datafusion_logic() {
    let schema = Arc::new(Schema::new(vec![Field::new("url", DataType::Utf8, false)]));
    // No `//` in these (unlike a real `http://` URL) so `/`-splitting is
    // unambiguous: part counts are exactly the path-segment counts.
    let urls = vec![
        "example.com/a/b/c/d", // 4th part = "c"
        "example.com/a/b",     // only 3 parts -> 4th part missing -> ""
        "example.com/x/y/z/w", // 4th part = "z"
    ];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(urls.clone()))],
    )
    .unwrap();

    let expr = Expr::ScalarUdf {
        name: "split_part".to_string(),
        args: vec![Expr::Column(0), lit_str("/"), lit_i64(4)],
    };

    let ours = eval_to_array(&expr, &batch).unwrap();
    let ours_strs = utf8_array_to_strings(ours.as_ref());

    let df_strs =
        run_datafusion_single_col(schema, batch, "SELECT split_part(url, '/', 4) FROM t").await;

    assert_eq!(ours_strs, df_strs, "split_part mismatch vs DataFusion");
    assert_eq!(ours_strs, vec!["c", "", "z"]);
}

#[tokio::test]
async fn split_part_over_utf8_url_matches_datafusion() {
    split_part_over_utf8_url_matches_datafusion_logic().await;
}

// ─── Case 4: length(extra) - length(replace(extra, 'a', '')) i64 arithmetic ─

async fn length_minus_length_replace_over_utf8_extra_matches_datafusion_logic() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "extra",
        DataType::Utf8,
        false,
    )]));
    let extras = vec!["banana", "apple", "kiwi", "aaa", ""];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(extras.clone()))],
    )
    .unwrap();

    // length(extra) - length(replace(extra, 'a', ''))  (counts occurrences of 'a')
    let expr = Expr::BinaryOp {
        op: BinaryOp::Sub,
        left: Box::new(Expr::ScalarUdf {
            name: "length".to_string(),
            args: vec![Expr::Column(0)],
        }),
        right: Box::new(Expr::ScalarUdf {
            name: "length".to_string(),
            args: vec![Expr::ScalarUdf {
                name: "replace".to_string(),
                args: vec![Expr::Column(0), lit_str("a"), lit_str("")],
            }],
        }),
    };

    let ours = eval_i64(&expr, &batch).unwrap();
    let ours_strs = i64_array_to_strings(&ours);

    let df_strs = run_datafusion_single_col(
        schema,
        batch,
        "SELECT length(extra) - length(replace(extra, 'a', '')) FROM t",
    )
    .await;

    assert_eq!(
        ours_strs, df_strs,
        "length(x) - length(replace(x, 'a', '')) mismatch vs DataFusion"
    );
    assert_eq!(ours, vec![3, 1, 0, 3, 0]);
}

#[tokio::test]
async fn length_minus_length_replace_over_utf8_extra_matches_datafusion() {
    length_minus_length_replace_over_utf8_extra_matches_datafusion_logic().await;
}

/// Umbrella test named per the plan's exit-test naming
/// (`case_and_scalar_udf_eval_matches_datafusion_oracle`): re-runs every
/// representative expression above and asserts the oracle property holds
/// for all of them in one place.
#[tokio::test]
async fn case_and_scalar_udf_eval_matches_datafusion_oracle() {
    case_over_int64_price_matches_datafusion_logic().await;
    case_with_regexp_replace_over_utf8_channel_matches_datafusion_logic().await;
    split_part_over_utf8_url_matches_datafusion_logic().await;
    length_minus_length_replace_over_utf8_extra_matches_datafusion_logic().await;

    // Also sanity-check eval_bool directly for the Utf8 Eq/Ne path used by
    // the CASE `when` clauses above (regexp_replace(...) = 'social').
    let schema = Arc::new(Schema::new(vec![Field::new(
        "channel",
        DataType::Utf8,
        false,
    )]));
    let channels = vec!["google", "baidu"];
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(channels))]).unwrap();
    let when = Expr::BinaryOp {
        op: BinaryOp::Eq,
        left: Box::new(Expr::ScalarUdf {
            name: "regexp_replace".to_string(),
            args: vec![
                Expr::Column(0),
                lit_str("google|facebook"),
                lit_str("social"),
            ],
        }),
        right: Box::new(lit_str("social")),
    };
    let bools = eval_bool(&when, &batch).unwrap();
    assert_eq!(bools, vec![true, false]);
}

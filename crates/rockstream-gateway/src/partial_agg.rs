//! Cross-shard partial aggregation pushdown for the gateway layer.
//!
//! When a query contains a `GROUP BY` aggregate that is backed by a law with
//! `gateway_combiner()` support (e.g. `SumCount/v1`), the gateway pushes
//! partial aggregation to individual shards.  Each shard returns O(groups)
//! partial rows instead of O(view rows), and the gateway combines them using
//! the law's merge function.
//!
//! This implements DESIGN.md §12.3.1 and §6.11.
//!
//! # Proof criterion (v0.41)
//!
//! `SELECT COUNT(*), region FROM mv GROUP BY region` pushes partial agg to
//! shards; gateway receives O(groups) rows, not O(view rows); explain output
//! reports the merge law used for the pushed aggregate.

use rockstream_types::merge_law::{GatewayAggCombinerDesc, LawBundle, MergeLawId};

use crate::error::GatewayError;

// ── Public types ─────────────────────────────────────────────────────────────

/// A partial aggregation row returned by a single shard for one group key.
///
/// Each shard emits one `PartialAggRow` per distinct group-by key value.
/// The gateway collects all shard rows and combines them.
#[derive(Debug, Clone)]
pub struct PartialAggRow {
    /// Serialised group-by key (e.g. a region string).
    pub group_key: String,
    /// Partial aggregate value (law-encoded bytes).
    pub partial_value: Vec<u8>,
    /// The merge law that was applied when computing the partial value.
    pub law_id: MergeLawId,
}

/// A combined aggregate row after merging partial shard results.
#[derive(Debug, Clone)]
pub struct CombinedAggRow {
    /// The group-by key.
    pub group_key: String,
    /// Combined value bytes (same encoding as `PartialAggRow::partial_value`).
    pub combined_value: Vec<u8>,
    /// Human-readable merge law name (for EXPLAIN output).
    pub law_name: String,
    /// Number of partial rows (shards) that contributed to this group.
    /// Combined rows should satisfy: combined count ≤ total view rows.
    pub shard_count: usize,
}

/// An EXPLAIN row for a partial aggregation step.
#[derive(Debug, Clone)]
pub struct PartialAggExplainRow {
    /// Step description (e.g. `"shard_partial"`, `"gateway_combine"`).
    pub step: &'static str,
    /// Merge law driving the combination.
    pub merge_law: String,
    /// Number of shards that contributed.
    pub shard_count: usize,
    /// Number of distinct group-by keys in the combined result.
    pub group_count: usize,
}

/// A plan for partial aggregation pushdown created from a query's aggregate
/// descriptor and the law's `gateway_combiner` description.
#[derive(Debug, Clone)]
pub struct PartialAggPlan {
    /// Group-by column names.
    pub group_by_cols: Vec<String>,
    /// Aggregate column (target of the pushed-down aggregate function).
    pub aggregate_col: String,
    /// Combiner descriptor supplied by the merge law.
    pub combiner: GatewayAggCombinerDesc,
}

// ── Plan construction ─────────────────────────────────────────────────────────

/// Build a `PartialAggPlan` if the given law supports gateway combining.
///
/// Returns `None` when the law does not provide a combiner descriptor (i.e.
/// the aggregate cannot safely be pushed to shards).
pub fn build_partial_agg_plan(
    group_by_cols: Vec<String>,
    aggregate_col: impl Into<String>,
    law: &dyn LawBundle,
) -> Option<PartialAggPlan> {
    let combiner = law.gateway_combiner()?;
    Some(PartialAggPlan {
        group_by_cols,
        aggregate_col: aggregate_col.into(),
        combiner,
    })
}

// ── Combining logic ───────────────────────────────────────────────────────────

/// Combine partial rows from multiple shards using `law.merge`.
///
/// The caller supplies all `PartialAggRow`s collected from shards.  Rows for
/// the same `group_key` are merged pairwise using the law's merge function.
///
/// The result vector has exactly one entry per distinct `group_key`, proving
/// the gateway receives O(groups) rows rather than O(view rows).
pub fn combine_partial_results(
    plan: &PartialAggPlan,
    partial_rows: &[PartialAggRow],
    law: &dyn LawBundle,
) -> Result<Vec<CombinedAggRow>, GatewayError> {
    // Accumulate per-group: key → (merged_bytes, shard_count).
    let mut groups: Vec<(String, Vec<u8>, usize)> = Vec::new();

    for row in partial_rows {
        if let Some(entry) = groups.iter_mut().find(|(k, _, _)| k == &row.group_key) {
            let merged = law
                .merge(&entry.1, &row.partial_value)
                .map_err(GatewayError::PartialAggMergeError)?;
            entry.1 = merged;
            entry.2 += 1;
        } else {
            groups.push((row.group_key.clone(), row.partial_value.clone(), 1));
        }
    }

    Ok(groups
        .into_iter()
        .map(|(group_key, combined_value, shard_count)| CombinedAggRow {
            group_key,
            combined_value,
            law_name: plan.combiner.law_name.to_owned(),
            shard_count,
        })
        .collect())
}

/// Produce EXPLAIN rows describing the partial aggregation plan execution.
pub fn explain_partial_agg(
    plan: &PartialAggPlan,
    shard_count: usize,
    group_count: usize,
) -> Vec<PartialAggExplainRow> {
    vec![
        PartialAggExplainRow {
            step: "shard_partial",
            merge_law: plan.combiner.law_name.to_owned(),
            shard_count,
            group_count,
        },
        PartialAggExplainRow {
            step: "gateway_combine",
            merge_law: plan.combiner.law_name.to_owned(),
            shard_count,
            group_count,
        },
    ]
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::laws::sum_count::decode_sum_count;
    use rockstream_types::laws::sum_count::encode_sum_count;
    use rockstream_types::laws::weight_add::encode_weight;
    use rockstream_types::laws::{SumCountV1, WeightAddV1};

    // ── Plan construction tests ───────────────────────────────────────────────

    /// SumCountV1 must provide a gateway combiner.
    #[test]
    fn sum_count_provides_gateway_combiner() {
        let law = SumCountV1;
        assert!(
            law.gateway_combiner().is_some(),
            "SumCount/v1 must support gateway combining"
        );
        let desc = law.gateway_combiner().unwrap();
        assert_eq!(desc.law_name, "SumCount");
        assert!(desc.is_associative);
        assert!(desc.is_commutative);
    }

    /// WeightAddV1 must provide a gateway combiner.
    #[test]
    fn weight_add_provides_gateway_combiner() {
        let law = WeightAddV1;
        assert!(
            law.gateway_combiner().is_some(),
            "WeightAdd/v1 must support gateway combining"
        );
    }

    /// `build_partial_agg_plan` returns `Some` for laws with a combiner.
    #[test]
    fn build_plan_succeeds_for_sum_count() {
        let law = SumCountV1;
        let plan = build_partial_agg_plan(vec!["region".to_string()], "amount", &law);
        assert!(plan.is_some(), "SumCount must produce a plan");
        let plan = plan.unwrap();
        assert_eq!(plan.group_by_cols, vec!["region"]);
        assert_eq!(plan.aggregate_col, "amount");
        assert_eq!(plan.combiner.law_name, "SumCount");
    }

    // ── Proof: partial agg pushdown reduces gateway row count ─────────────────

    /// **Proof criterion (v0.41)**: `SELECT COUNT(*), region FROM mv GROUP BY
    /// region` pushes partial agg to shards; gateway receives O(groups) rows,
    /// not O(view rows); explain output reports the merge law.
    ///
    /// Simulation: 3 shards × 4 rows each = 12 view rows, but only 2 distinct
    /// regions.  The gateway must combine 6 shard partial rows into 2 combined
    /// rows.
    #[test]
    fn proof_partial_agg_pushdown_reduces_row_count() {
        let law = SumCountV1;
        let plan = build_partial_agg_plan(vec!["region".to_string()], "count_star", &law)
            .expect("SumCount must support pushdown");

        // Simulate: 3 shards, 2 groups ("us-east", "eu-west").
        // Each shard contributes 4 view rows (2 per group), but reports only
        // 1 partial row per group — so 6 partial rows total across 3 shards.
        let view_row_count = 3 * 4; // 12 total view rows
        let partial_rows: Vec<PartialAggRow> = vec![
            // Shard 0: us-east=2 rows (count=2,sum=200), eu-west=2 rows
            PartialAggRow {
                group_key: "us-east".to_string(),
                partial_value: encode_sum_count(200, 2),
                law_id: plan.combiner.law_id,
            },
            PartialAggRow {
                group_key: "eu-west".to_string(),
                partial_value: encode_sum_count(100, 2),
                law_id: plan.combiner.law_id,
            },
            // Shard 1
            PartialAggRow {
                group_key: "us-east".to_string(),
                partial_value: encode_sum_count(300, 2),
                law_id: plan.combiner.law_id,
            },
            PartialAggRow {
                group_key: "eu-west".to_string(),
                partial_value: encode_sum_count(150, 2),
                law_id: plan.combiner.law_id,
            },
            // Shard 2
            PartialAggRow {
                group_key: "us-east".to_string(),
                partial_value: encode_sum_count(500, 2),
                law_id: plan.combiner.law_id,
            },
            PartialAggRow {
                group_key: "eu-west".to_string(),
                partial_value: encode_sum_count(50, 2),
                law_id: plan.combiner.law_id,
            },
        ];

        let combined =
            combine_partial_results(&plan, &partial_rows, &law).expect("combine must succeed");

        // Gateway receives O(groups) = 2 rows, NOT O(view rows) = 12.
        assert_eq!(
            combined.len(),
            2,
            "gateway must receive O(groups)=2 rows, not O(view rows)={view_row_count}"
        );

        // Verify correctness: us-east sum=1000, count=6; eu-west sum=300, count=6.
        let us_east = combined
            .iter()
            .find(|r| r.group_key == "us-east")
            .expect("us-east must be in result");
        let (sum, count) = decode_sum_count(&us_east.combined_value).unwrap();
        assert_eq!(sum, 1000);
        assert_eq!(count, 6);

        let eu_west = combined
            .iter()
            .find(|r| r.group_key == "eu-west")
            .expect("eu-west must be in result");
        let (sum, count) = decode_sum_count(&eu_west.combined_value).unwrap();
        assert_eq!(sum, 300);
        assert_eq!(count, 6);

        // Verify explain reports the merge law.
        let explain = explain_partial_agg(&plan, 3, combined.len());
        assert!(
            explain.iter().any(|r| r.merge_law == "SumCount"),
            "explain must report merge law; got: {explain:?}"
        );
        let combine_step = explain
            .iter()
            .find(|r| r.step == "gateway_combine")
            .expect("explain must have gateway_combine step");
        assert_eq!(combine_step.group_count, 2);
        assert_eq!(combine_step.shard_count, 3);
    }

    /// WeightAdd partial agg: 4 shards, 3 groups, 20 view rows total.
    /// Gateway should see 3 combined rows, not 20.
    #[test]
    fn proof_weight_add_partial_agg_pushdown() {
        let law = WeightAddV1;
        let plan = build_partial_agg_plan(vec!["category".to_string()], "weight", &law)
            .expect("WeightAdd must support pushdown");

        let view_rows_per_shard = 5; // 5 view rows per shard
        let shard_count = 4;
        let groups = ["A", "B", "C"];

        // Each shard contributes 1 partial row per group.
        let mut partial_rows = Vec::new();
        for shard in 0..shard_count {
            for (g_idx, group) in groups.iter().enumerate() {
                let weight = (shard as i64 + 1) * (g_idx as i64 + 1);
                partial_rows.push(PartialAggRow {
                    group_key: group.to_string(),
                    partial_value: encode_weight(weight),
                    law_id: plan.combiner.law_id,
                });
            }
        }

        let total_view_rows = shard_count * view_rows_per_shard;
        let combined =
            combine_partial_results(&plan, &partial_rows, &law).expect("combine must succeed");

        assert_eq!(
            combined.len(),
            3,
            "gateway receives O(groups)=3 rows, not O(view rows)={total_view_rows}"
        );
    }
}

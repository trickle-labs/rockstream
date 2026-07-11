//! Partition key derivation shared by the Iceberg and Delta cold-tier sinks
//! (v0.44 slice 7 — `partition_by` support, DESIGN.md §13.6.1).
//!
//! `partition_by` entries accept either a plain column reference (`region`)
//! or a `date_trunc('<unit>', <column>)` transform for
//! `unit` in `{year, month, day, hour}`, matching the roadmap's example
//! (`date_trunc('day', created_at)`).

use std::collections::HashMap;

use arrow::array::UInt32Array;
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use crate::sink_connector::SinkError;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PartitionExpr {
    Column(String),
    DateTrunc { unit: String, column: String },
}

fn parse_partition_expr(spec: &str) -> Result<PartitionExpr, SinkError> {
    let trimmed = spec.trim();
    if let Some(rest) = trimmed.strip_prefix("date_trunc(") {
        let rest = rest.strip_suffix(')').ok_or_else(|| {
            SinkError::Io(format!(
                "invalid partition_by expression '{trimmed}': missing closing paren"
            ))
        })?;
        let mut parts = rest.splitn(2, ',');
        let unit = parts
            .next()
            .ok_or_else(|| SinkError::Io(format!("invalid date_trunc expression '{trimmed}'")))?
            .trim()
            .trim_matches(|c| c == '\'' || c == '"')
            .to_string();
        let column = parts
            .next()
            .ok_or_else(|| SinkError::Io(format!("invalid date_trunc expression '{trimmed}'")))?
            .trim()
            .to_string();
        if !matches!(unit.as_str(), "year" | "month" | "day" | "hour") {
            return Err(SinkError::Io(format!(
                "unsupported date_trunc unit '{unit}' in partition_by expression '{trimmed}'; \
                 expected one of year|month|day|hour"
            )));
        }
        Ok(PartitionExpr::DateTrunc { unit, column })
    } else {
        Ok(PartitionExpr::Column(trimmed.to_string()))
    }
}

fn truncate_iso_like(value: &str, unit: &str) -> String {
    let take_chars = match unit {
        "year" => 4,
        "month" => 7,
        "day" => 10,
        "hour" => 13,
        _ => value.len(),
    };
    value.chars().take(take_chars).collect()
}

fn field_name(expr: &PartitionExpr) -> String {
    match expr {
        PartitionExpr::Column(col) => col.clone(),
        PartitionExpr::DateTrunc { unit, column } => format!("{unit}_{column}"),
    }
}

fn partition_value(
    expr: &PartitionExpr,
    batch: &RecordBatch,
    row: usize,
) -> Result<String, SinkError> {
    let column_name = match expr {
        PartitionExpr::Column(col) => col.as_str(),
        PartitionExpr::DateTrunc { column, .. } => column.as_str(),
    };
    let idx = batch.schema().index_of(column_name).map_err(|_| {
        SinkError::Io(format!(
            "partition_by references unknown column '{column_name}'"
        ))
    })?;
    let raw = array_value_to_string(batch.column(idx), row)
        .map_err(|error| SinkError::Io(error.to_string()))?;
    match expr {
        PartitionExpr::Column(_) => Ok(raw),
        PartitionExpr::DateTrunc { unit, .. } => Ok(truncate_iso_like(&raw, unit)),
    }
}

fn sanitize_partition_value(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Splits `batch` into one sub-batch per distinct partition-key tuple derived
/// from `partition_by`. Returns `[(path_suffix, sub_batch)]` sorted by
/// `path_suffix` for determinism. If `partition_by` is empty, returns a
/// single `("", batch)` pair — preserving the pre-v0.44 unpartitioned layout.
pub fn split_batch_by_partition(
    batch: &RecordBatch,
    partition_by: &[String],
) -> Result<Vec<(String, RecordBatch)>, SinkError> {
    if partition_by.is_empty() {
        return Ok(vec![(String::new(), batch.clone())]);
    }

    let exprs = partition_by
        .iter()
        .map(|spec| parse_partition_expr(spec))
        .collect::<Result<Vec<_>, _>>()?;

    let mut groups: HashMap<String, Vec<u32>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for row in 0..batch.num_rows() {
        let mut path_parts = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            let value = partition_value(expr, batch, row)?;
            path_parts.push(format!(
                "{}={}",
                field_name(expr),
                sanitize_partition_value(&value)
            ));
        }
        let suffix = path_parts.join("/");
        if !groups.contains_key(&suffix) {
            order.push(suffix.clone());
        }
        groups.entry(suffix).or_default().push(row as u32);
    }

    order.sort();
    let mut out = Vec::with_capacity(order.len());
    for suffix in order {
        let indices = UInt32Array::from(groups.remove(&suffix).expect("suffix was just inserted"));
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                take(column.as_ref(), &indices, None)
                    .map_err(|error| SinkError::Io(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let sub_batch = RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| SinkError::Io(error.to_string()))?;
        out.push((suffix, sub_batch));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let regions: ArrayRef = Arc::new(StringArray::from(vec!["NA", "EU", "NA", "APAC", "EU"]));
        RecordBatch::try_new(schema, vec![ids, regions]).unwrap()
    }

    #[test]
    fn unpartitioned_returns_single_group() {
        let batch = make_batch();
        let groups = split_batch_by_partition(&batch, &[]).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, "");
        assert_eq!(groups[0].1.num_rows(), 5);
    }

    #[test]
    fn partitions_by_plain_column() {
        let batch = make_batch();
        let groups = split_batch_by_partition(&batch, &["region".to_string()]).unwrap();
        let suffixes: Vec<&str> = groups.iter().map(|(suffix, _)| suffix.as_str()).collect();
        assert_eq!(suffixes, vec!["region=APAC", "region=EU", "region=NA"]);
        let rows_by_suffix: HashMap<&str, usize> = groups
            .iter()
            .map(|(suffix, batch)| (suffix.as_str(), batch.num_rows()))
            .collect();
        assert_eq!(rows_by_suffix["region=NA"], 2);
        assert_eq!(rows_by_suffix["region=EU"], 2);
        assert_eq!(rows_by_suffix["region=APAC"], 1);
    }

    #[test]
    fn rejects_unknown_column() {
        let batch = make_batch();
        let result = split_batch_by_partition(&batch, &["missing_col".to_string()]);
        assert!(matches!(result, Err(SinkError::Io(_))));
    }

    #[test]
    fn rejects_unsupported_date_trunc_unit() {
        let batch = make_batch();
        let result = split_batch_by_partition(&batch, &["date_trunc('week', region)".to_string()]);
        assert!(matches!(result, Err(SinkError::Io(_))));
    }
}

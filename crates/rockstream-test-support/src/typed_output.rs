//! Exact typed weighted-result comparison shared with the SQL oracle (#111).

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::ScalarValue;
use std::collections::HashMap;

/// Compare full Arrow schemas and typed signed bags, ignoring only row order.
pub fn assert_weighted_eq(
    actual: &RecordBatch,
    actual_weights: &[i64],
    expected: &RecordBatch,
    expected_weights: &[i64],
) {
    assert_eq!(actual.schema(), expected.schema());
    let mut actual_result = empty_result(actual.schema().as_ref());
    let mut expected_result = empty_result(expected.schema().as_ref());
    add_batch_rows(&mut actual_result, actual, actual_weights);
    add_batch_rows(&mut expected_result, expected, expected_weights);
    assert_eq!(compare_results(&expected_result, &actual_result), Ok(()));
}

/// Build explicit nullable Int64 fixtures without inferring a schema from values.
pub fn int64_batch(
    schema: arrow::datatypes::SchemaRef,
    rows: &[(&[Option<i64>], i64)],
) -> (RecordBatch, Vec<i64>) {
    assert!(rows
        .iter()
        .all(|(row, _)| row.len() == schema.fields().len()));
    let columns = (0..schema.fields().len())
        .map(|col| {
            std::sync::Arc::new(arrow::array::Int64Array::from_iter(
                rows.iter().map(|(row, _)| row[col]),
            )) as arrow::array::ArrayRef
        })
        .collect();
    (
        RecordBatch::try_new(schema, columns).unwrap(),
        rows.iter().map(|(_, weight)| *weight).collect(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleField {
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedResult {
    pub fields: Vec<OracleField>,
    pub rows: HashMap<Vec<ScalarValue>, i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowWeightMismatch {
    pub row: Vec<ScalarValue>,
    pub expected_weight: i64,
    pub actual_weight: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleMismatch {
    Schema {
        expected: Vec<OracleField>,
        actual: Vec<OracleField>,
    },
    Rows(Vec<RowWeightMismatch>),
}

pub fn empty_result(schema: &Schema) -> TypedResult {
    TypedResult {
        // The planner IR carries positional columns but drops projection aliases.
        fields: schema
            .fields()
            .iter()
            .map(|field| OracleField {
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
            .collect(),
        rows: HashMap::new(),
    }
}

pub fn add_batch_rows(result: &mut TypedResult, batch: &RecordBatch, weights: &[i64]) {
    assert_eq!(weights.len(), batch.num_rows());
    for (row_idx, &weight) in weights.iter().enumerate() {
        let row: Vec<ScalarValue> = batch
            .columns()
            .iter()
            .map(|column| ScalarValue::try_from_array(column, row_idx).unwrap())
            .collect();
        let net = result.rows.entry(row.clone()).or_insert(0);
        *net += weight;
        if *net == 0 {
            result.rows.remove(&row);
        }
    }
}

pub fn compare_results(expected: &TypedResult, actual: &TypedResult) -> Result<(), OracleMismatch> {
    if expected.fields != actual.fields {
        return Err(OracleMismatch::Schema {
            expected: expected.fields.clone(),
            actual: actual.fields.clone(),
        });
    }

    let rows: std::collections::HashSet<_> = expected
        .rows
        .keys()
        .chain(actual.rows.keys())
        .cloned()
        .collect();
    let mut differences: Vec<_> = rows
        .into_iter()
        .filter_map(|row| {
            let expected_weight = expected.rows.get(&row).copied().unwrap_or(0);
            let actual_weight = actual.rows.get(&row).copied().unwrap_or(0);
            (expected_weight != actual_weight).then_some(RowWeightMismatch {
                row,
                expected_weight,
                actual_weight,
            })
        })
        .collect();
    differences.sort_by_key(|difference| format!("{:?}", difference.row));
    if differences.is_empty() {
        Ok(())
    } else {
        Err(OracleMismatch::Rows(differences))
    }
}

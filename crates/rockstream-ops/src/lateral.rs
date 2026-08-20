use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, LargeListArray, LargeStringArray, ListArray, StringArray,
    UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_plan::LateralFunc;
use rockstream_types::error_code::RS_1013;

use crate::error::OpError;
use crate::join::concat_zsets;
use crate::op::Operator;
use crate::zset::ArrowZSet;

pub struct LateralOp {
    output_schema: SchemaRef,
    func: LateralFunc,
}

impl LateralOp {
    pub fn new(input_schema: SchemaRef, func: LateralFunc) -> Result<Self, OpError> {
        let output_schema = output_schema(&input_schema, &func)?;
        Ok(Self {
            output_schema,
            func,
        })
    }

    pub fn apply(&self, input: ArrowZSet) -> Result<ArrowZSet, OpError> {
        if input.is_empty() {
            return Ok(ArrowZSet::empty(self.output_schema.clone()));
        }

        let frontier = input.frontier.clone();
        let mut outputs = Vec::new();
        for row_idx in 0..input.num_rows() {
            if let Some(batch) = self.expand_row(&input, row_idx)? {
                outputs.push(batch);
            }
        }
        let mut out = concat_zsets(outputs, self.output_schema.clone())?;
        out.frontier = frontier;
        Ok(out)
    }

    fn expand_row(&self, input: &ArrowZSet, row_idx: usize) -> Result<Option<ArrowZSet>, OpError> {
        match &self.func {
            LateralFunc::Unnest { col } => {
                self.expand_replacing_column(input, row_idx, *col, |col, row_idx| {
                    match col.data_type() {
                        DataType::List(_) => expand_list_value(
                            col.as_any()
                                .downcast_ref::<ListArray>()
                                .expect("checked data_type"),
                            row_idx,
                        ),
                        DataType::LargeList(_) => expand_large_list_value(
                            col.as_any()
                                .downcast_ref::<LargeListArray>()
                                .expect("checked data_type"),
                            row_idx,
                        ),
                        other => Err(OpError::ColumnTypeMismatch {
                            expected: "List or LargeList".to_string(),
                            got: format!("{other:?}"),
                            code: RS_1013,
                        }),
                    }
                })
            }
            LateralFunc::GenerateSeries { start, stop, step } => {
                if *step == 0 {
                    return Err(OpError::InvalidLiteral {
                        detail: "use a non-zero GENERATE_SERIES step".to_string(),
                        code: RS_1013,
                    });
                }
                let values = generate_series_values(*start, *stop, *step);
                if values.is_empty() {
                    return Ok(None);
                }
                let generated = Arc::new(Int64Array::from(values)) as ArrayRef;
                self.build_row_batch(input, row_idx, None, generated)
            }
            LateralFunc::JsonExtractArray { col } => {
                self.expand_replacing_column(input, row_idx, *col, |column, row_idx| {
                    let values = match column.data_type() {
                        DataType::Utf8 => expand_json_array(
                            column
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .expect("checked data_type"),
                            row_idx,
                        )?,
                        DataType::LargeUtf8 => expand_large_json_array(
                            column
                                .as_any()
                                .downcast_ref::<LargeStringArray>()
                                .expect("checked data_type"),
                            row_idx,
                        )?,
                        other => {
                            return Err(OpError::ColumnTypeMismatch {
                                expected: "Utf8 or LargeUtf8".to_string(),
                                got: format!("{other:?}"),
                                code: RS_1013,
                            });
                        }
                    };
                    Ok(values.map(|vals| Arc::new(vals) as ArrayRef))
                })
            }
        }
    }

    fn expand_replacing_column<F>(
        &self,
        input: &ArrowZSet,
        row_idx: usize,
        col: usize,
        expand: F,
    ) -> Result<Option<ArrowZSet>, OpError>
    where
        F: Fn(&dyn Array, usize) -> Result<Option<ArrayRef>, OpError>,
    {
        let column = input
            .data
            .columns()
            .get(col)
            .ok_or(OpError::ColumnOutOfBounds {
                index: col,
                num_cols: input.data.num_columns(),
                code: RS_1013,
            })?;
        let Some(expanded) = expand(column.as_ref(), row_idx)? else {
            return Ok(None);
        };
        self.build_row_batch(input, row_idx, Some(col), expanded)
    }

    fn build_row_batch(
        &self,
        input: &ArrowZSet,
        row_idx: usize,
        replace_col: Option<usize>,
        expanded: ArrayRef,
    ) -> Result<Option<ArrowZSet>, OpError> {
        if expanded.is_empty() {
            return Ok(None);
        }
        let take_idx = UInt32Array::from(vec![row_idx as u32; expanded.len()]);
        let mut cols = Vec::with_capacity(self.output_schema.fields().len());
        for (idx, column) in input.data.columns().iter().enumerate() {
            if Some(idx) == replace_col {
                cols.push(expanded.clone());
            } else {
                cols.push(take(column.as_ref(), &take_idx, None).map_err(OpError::arrow)?);
            }
        }
        if replace_col.is_none() {
            cols.push(expanded.clone());
        }
        let data =
            RecordBatch::try_new(self.output_schema.clone(), cols).map_err(OpError::arrow)?;
        Ok(Some(ArrowZSet::new(
            data,
            vec![input.weights[row_idx]; expanded.len()],
        )))
    }

    pub fn state_bytes(&self) -> u64 {
        0
    }
}

impl Operator for LateralOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.apply(delta)
    }

    fn name(&self) -> &str {
        "LateralOp"
    }

    fn state_bytes(&self) -> u64 {
        0
    }
}

fn output_schema(input_schema: &SchemaRef, func: &LateralFunc) -> Result<SchemaRef, OpError> {
    match func {
        LateralFunc::Unnest { col } => {
            rewrite_field_schema(input_schema, *col, |field| match field.data_type() {
                DataType::List(item) | DataType::LargeList(item) => Ok(Field::new(
                    field.name(),
                    item.data_type().clone(),
                    item.is_nullable(),
                )),
                other => Err(OpError::ColumnTypeMismatch {
                    expected: "List or LargeList".to_string(),
                    got: format!("{other:?}"),
                    code: RS_1013,
                }),
            })
        }
        LateralFunc::GenerateSeries { .. } => {
            let mut fields = input_schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>();
            fields.push(Field::new("generate_series", DataType::Int64, false));
            Ok(Arc::new(Schema::new(fields)))
        }
        LateralFunc::JsonExtractArray { col } => {
            rewrite_field_schema(input_schema, *col, |field| match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    Ok(Field::new(field.name(), DataType::Utf8, true))
                }
                other => Err(OpError::ColumnTypeMismatch {
                    expected: "Utf8 or LargeUtf8".to_string(),
                    got: format!("{other:?}"),
                    code: RS_1013,
                }),
            })
        }
    }
}

fn rewrite_field_schema<F>(
    input_schema: &SchemaRef,
    col: usize,
    rewrite: F,
) -> Result<SchemaRef, OpError>
where
    F: Fn(&Field) -> Result<Field, OpError>,
{
    let mut fields = input_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    let field = fields.get(col).ok_or(OpError::ColumnOutOfBounds {
        index: col,
        num_cols: fields.len(),
        code: RS_1013,
    })?;
    fields[col] = rewrite(field)?;
    Ok(Arc::new(Schema::new(fields)))
}

fn expand_list_value(list: &ListArray, row_idx: usize) -> Result<Option<ArrayRef>, OpError> {
    if list.is_null(row_idx) {
        return Ok(None);
    }
    Ok(Some(list.value(row_idx)))
}

fn expand_large_list_value(
    list: &LargeListArray,
    row_idx: usize,
) -> Result<Option<ArrayRef>, OpError> {
    if list.is_null(row_idx) {
        return Ok(None);
    }
    Ok(Some(list.value(row_idx)))
}

fn expand_json_array(values: &StringArray, row_idx: usize) -> Result<Option<StringArray>, OpError> {
    if values.is_null(row_idx) {
        return Ok(None);
    }
    let parsed: serde_json::Value =
        serde_json::from_str(values.value(row_idx)).map_err(|e| OpError::InvalidLiteral {
            detail: format!("provide a valid JSON array string for JSON_EXTRACT_ARRAY: {e}"),
            code: RS_1013,
        })?;
    json_array_to_strings(parsed)
}

fn expand_large_json_array(
    values: &LargeStringArray,
    row_idx: usize,
) -> Result<Option<StringArray>, OpError> {
    if values.is_null(row_idx) {
        return Ok(None);
    }
    let parsed: serde_json::Value =
        serde_json::from_str(values.value(row_idx)).map_err(|e| OpError::InvalidLiteral {
            detail: format!("provide a valid JSON array string for JSON_EXTRACT_ARRAY: {e}"),
            code: RS_1013,
        })?;
    json_array_to_strings(parsed)
}

fn json_array_to_strings(parsed: serde_json::Value) -> Result<Option<StringArray>, OpError> {
    match parsed {
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                return Ok(None);
            }
            Ok(Some(StringArray::from(
                items
                    .into_iter()
                    .map(|item| Some(item.to_string()))
                    .collect::<Vec<_>>(),
            )))
        }
        other => Err(OpError::InvalidLiteral {
            detail: format!("provide a JSON array string for JSON_EXTRACT_ARRAY, got {other}"),
            code: RS_1013,
        }),
    }
}

fn generate_series_values(start: i64, stop: i64, step: i64) -> Vec<i64> {
    let mut out = Vec::new();
    if step > 0 {
        let mut current = start;
        while current <= stop {
            out.push(current);
            current += step;
        }
    } else {
        let mut current = start;
        while current >= stop {
            out.push(current);
            current += step;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use arrow::array::{ArrayRef, Int64Array, ListArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use arrow::record_batch::RecordBatch;

    fn make_unnest_batch(rows: &[(i64, Vec<i64>, i64)]) -> ArrowZSet {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));
        let ids = Int64Array::from(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
        let tags = ListArray::from_iter_primitive::<Int64Type, _, _>(
            rows.iter()
                .map(|(_, values, _)| Some(values.iter().copied().map(Some).collect::<Vec<_>>())),
        );
        let weights = rows.iter().map(|(_, _, w)| *w).collect::<Vec<_>>();
        let data = RecordBatch::try_new(
            schema,
            vec![Arc::new(ids) as ArrayRef, Arc::new(tags) as ArrayRef],
        )
        .unwrap();
        ArrowZSet::new(data, weights)
    }

    fn make_series_batch(rows: &[(i64, i64)]) -> ArrowZSet {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids = Int64Array::from(rows.iter().map(|(id, _)| *id).collect::<Vec<_>>());
        let weights = rows.iter().map(|(_, w)| *w).collect::<Vec<_>>();
        let data = RecordBatch::try_new(schema, vec![Arc::new(ids) as ArrayRef]).unwrap();
        ArrowZSet::new(data, weights)
    }

    fn collect_pairs(batch: &ArrowZSet) -> Vec<((i64, i64), i64)> {
        let left = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| ((left.value(i), right.value(i)), batch.weights[i]))
            .collect()
    }

    fn accumulate_pairs(state: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
        for ((a, b), w) in collect_pairs(batch) {
            let entry = state.entry((a, b)).or_insert(0);
            *entry += w;
            if *entry == 0 {
                state.remove(&(a, b));
            }
        }
    }

    #[test]
    fn lateral_unnest_expands_json_array_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));
        let op = LateralOp::new(schema, LateralFunc::Unnest { col: 1 }).unwrap();
        let out = op
            .apply(make_unnest_batch(&[
                (1, vec![10, 20], 1),
                (2, vec![], 1),
                (3, vec![30], 2),
            ]))
            .unwrap();
        assert_eq!(
            collect_pairs(&out),
            vec![((1, 10), 1), ((1, 20), 1), ((3, 30), 2)]
        );
    }

    #[test]
    fn lateral_retraction_removes_exactly_produced_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ]));
        let op = LateralOp::new(schema, LateralFunc::Unnest { col: 1 }).unwrap();
        let insert = op.apply(make_unnest_batch(&[(7, vec![4, 5], 1)])).unwrap();
        let delete = op.apply(make_unnest_batch(&[(7, vec![4, 5], -1)])).unwrap();
        assert_eq!(collect_pairs(&insert), vec![((7, 4), 1), ((7, 5), 1)]);
        assert_eq!(collect_pairs(&delete), vec![((7, 4), -1), ((7, 5), -1)]);
        let mut state = BTreeMap::new();
        accumulate_pairs(&mut state, &insert);
        accumulate_pairs(&mut state, &delete);
        assert!(
            state.is_empty(),
            "retraction must cancel exactly its expansion"
        );
    }

    #[test]
    fn lateral_generate_series_arithmetic_sequence() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let op = LateralOp::new(
            schema,
            LateralFunc::GenerateSeries {
                start: 2,
                stop: 6,
                step: 2,
            },
        )
        .unwrap();
        let out = op.apply(make_series_batch(&[(11, 1), (12, -1)])).unwrap();
        assert_eq!(
            collect_pairs(&out),
            vec![
                ((11, 2), 1),
                ((11, 4), 1),
                ((11, 6), 1),
                ((12, 2), -1),
                ((12, 4), -1),
                ((12, 6), -1),
            ]
        );
    }
}

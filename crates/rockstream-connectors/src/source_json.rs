use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::source_connector::SourceError;

pub(crate) type JsonRow = Vec<serde_json::Value>;

pub(crate) fn json_rows_to_batch(
    schema: &SchemaRef,
    records: &[JsonRow],
    source: &str,
) -> Result<RecordBatch, SourceError> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let values = records
            .iter()
            .map(|row| {
                row.get(index).ok_or_else(|| SourceError::PollDeltaFailed {
                    reason: format!(
                        "{source} record has {} column(s), but source schema has {}",
                        row.len(),
                        schema.fields().len()
                    ),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let column: ArrayRef = match field.data_type() {
            DataType::Int64 => Arc::new(Int64Array::from(
                values
                    .into_iter()
                    .map(|value| json_i64(value, source))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .into_iter()
                    .map(|value| json_i32(value, source))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            DataType::Utf8 => Arc::new(StringArray::from(
                values
                    .into_iter()
                    .map(|value| json_string(value, source))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            DataType::Boolean => Arc::new(BooleanArray::from(
                values
                    .into_iter()
                    .map(|value| json_bool(value, source))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            DataType::Decimal128(precision, scale) => Arc::new(
                Decimal128Array::from(
                    values
                        .into_iter()
                        .map(|value| json_decimal(value, *scale, source))
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| SourceError::PollDeltaFailed {
                    reason: format!(
                        "{source} decimal column '{}' is invalid: {error}",
                        field.name()
                    ),
                })?,
            ),
            data_type => {
                return Err(SourceError::PollDeltaFailed {
                    reason: format!(
                        "{source} source does not support bound column '{}' with type {data_type}",
                        field.name()
                    ),
                });
            }
        };
        columns.push(column);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|error| SourceError::PollDeltaFailed {
        reason: format!("failed to build {source} RecordBatch: {error}"),
    })
}

fn json_i64(value: &serde_json::Value, source: &str) -> Result<Option<i64>, SourceError> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .map(Some)
        .ok_or_else(|| SourceError::PollDeltaFailed {
            reason: format!("{source} value '{value}' is not an Int64"),
        })
}

fn json_i32(value: &serde_json::Value, source: &str) -> Result<Option<i32>, SourceError> {
    json_i64(value, source)?
        .map(i32::try_from)
        .transpose()
        .map_err(|_| SourceError::PollDeltaFailed {
            reason: format!("{source} value '{value}' is outside the Int32 range"),
        })
}

fn json_string(value: &serde_json::Value, source: &str) -> Result<Option<String>, SourceError> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|text| Some(text.to_string()))
        .ok_or_else(|| SourceError::PollDeltaFailed {
            reason: format!("{source} value '{value}' is not a UTF-8 string"),
        })
}

fn json_bool(value: &serde_json::Value, source: &str) -> Result<Option<bool>, SourceError> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .map(Some)
        .ok_or_else(|| SourceError::PollDeltaFailed {
            reason: format!("{source} value '{value}' is not a boolean"),
        })
}

fn json_decimal(
    value: &serde_json::Value,
    scale: i8,
    source: &str,
) -> Result<Option<i128>, SourceError> {
    if value.is_null() {
        return Ok(None);
    }
    let text = value
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| value.to_string());
    let negative = text.starts_with('-');
    let digits = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(&text);
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let scale = usize::try_from(scale).map_err(|_| SourceError::PollDeltaFailed {
        reason: format!("{source} decimal scale {scale} is invalid"),
    })?;
    if fraction.len() > scale
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(SourceError::PollDeltaFailed {
            reason: format!(
                "{source} value '{text}' cannot be represented at decimal scale {scale}"
            ),
        });
    }
    let factor = 10_i128
        .checked_pow(scale as u32)
        .ok_or_else(|| SourceError::PollDeltaFailed {
            reason: format!("{source} decimal scale {scale} overflows"),
        })?;
    let whole = whole
        .parse::<i128>()
        .map_err(|_| SourceError::PollDeltaFailed {
            reason: format!("{source} value '{text}' is not a decimal"),
        })?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i128>()
            .map_err(|_| SourceError::PollDeltaFailed {
                reason: format!("{source} value '{text}' is not a decimal"),
            })?
            .checked_mul(10_i128.pow((scale - fraction.len()) as u32))
            .ok_or_else(|| SourceError::PollDeltaFailed {
                reason: format!("{source} value '{text}' overflows decimal"),
            })?
    };
    let unscaled = whole
        .checked_mul(factor)
        .and_then(|whole| whole.checked_add(fraction))
        .ok_or_else(|| SourceError::PollDeltaFailed {
            reason: format!("{source} value '{text}' overflows decimal"),
        })?;
    Ok(Some(if negative { -unscaled } else { unscaled }))
}

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticError {
    Overflow,
    InvalidLength { expected: usize, actual: usize },
}

impl fmt::Display for ArithmeticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("arithmetic overflow"),
            Self::InvalidLength { expected, actual } => {
                write!(f, "expected {expected} bytes, got {actual}")
            }
        }
    }
}

pub fn checked_add_i64(left: i64, right: i64) -> Result<i64, ArithmeticError> {
    rockstream_verified::arithmetic::checked_i64_add(left, right).ok_or(ArithmeticError::Overflow)
}

pub fn checked_mul_i64(left: i64, right: i64) -> Result<i64, ArithmeticError> {
    rockstream_verified::arithmetic::checked_i64_mul(left, right).ok_or(ArithmeticError::Overflow)
}

pub fn checked_add_u64(left: u64, right: u64) -> Result<u64, ArithmeticError> {
    rockstream_verified::arithmetic::checked_u64_add(left, right).ok_or(ArithmeticError::Overflow)
}

pub fn checked_neg_i64(value: i64) -> Result<i64, ArithmeticError> {
    rockstream_verified::arithmetic::checked_i64_neg(value).ok_or(ArithmeticError::Overflow)
}

pub fn checked_i128_to_i64(value: i128) -> Result<i64, ArithmeticError> {
    rockstream_verified::arithmetic::checked_i128_to_i64(value).ok_or(ArithmeticError::Overflow)
}

pub fn encode_i64(value: i64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

pub fn decode_i64(bytes: &[u8]) -> Result<i64, ArithmeticError> {
    rockstream_verified::arithmetic::decode_i64(bytes).ok_or(ArithmeticError::InvalidLength {
        expected: 8,
        actual: bytes.len(),
    })
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64, ArithmeticError> {
    rockstream_verified::codecs::decode_u64_be(bytes).ok_or(ArithmeticError::InvalidLength {
        expected: 8,
        actual: bytes.len(),
    })
}

pub fn admit_i64_reassociation(values: &[i64]) -> bool {
    values.iter().try_fold(0u128, |total, value| {
        total.checked_add((*value as i128).unsigned_abs())
    }) <= Some(i64::MAX as u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_arithmetic_has_exact_i64_boundaries() {
        assert_eq!(checked_add_i64(i64::MAX, 0), Ok(i64::MAX));
        assert_eq!(checked_add_i64(i64::MAX, 1), Err(ArithmeticError::Overflow));
        assert_eq!(
            checked_add_i64(i64::MIN, -1),
            Err(ArithmeticError::Overflow)
        );
        assert_eq!(checked_mul_i64(i64::MAX, 1), Ok(i64::MAX));
        assert_eq!(
            checked_mul_i64(i64::MIN, -1),
            Err(ArithmeticError::Overflow)
        );
        assert_eq!(checked_add_u64(u64::MAX, 0), Ok(u64::MAX));
        assert_eq!(checked_add_u64(u64::MAX, 1), Err(ArithmeticError::Overflow));
        assert_eq!(checked_neg_i64(i64::MIN), Err(ArithmeticError::Overflow));
    }

    #[test]
    fn fixed_width_codecs_reject_truncation_and_trailing_bytes() {
        let encoded = encode_i64(-42);
        assert_eq!(decode_i64(&encoded), Ok(-42));
        assert_eq!(
            decode_i64(&encoded[..7]),
            Err(ArithmeticError::InvalidLength {
                expected: 8,
                actual: 7,
            })
        );
        assert_eq!(
            decode_i64(&[encoded.as_slice(), &[0]].concat()),
            Err(ArithmeticError::InvalidLength {
                expected: 8,
                actual: 9,
            })
        );
        let unsigned = encode_u64(u64::MAX);
        assert_eq!(decode_u64(&unsigned), Ok(u64::MAX));
    }

    #[test]
    fn regrouping_admission_uses_absolute_sum_budget() {
        assert!(admit_i64_reassociation(&[i64::MAX, 0]));
        assert!(!admit_i64_reassociation(&[i64::MAX, 1, -1]));
        assert!(!admit_i64_reassociation(&[i64::MIN, -1]));
    }
}

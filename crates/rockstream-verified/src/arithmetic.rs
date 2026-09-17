use vstd::prelude::*;

verus! {
    // verus-claim: VS1-02
    pub fn checked_i64_add(left: i64, right: i64) -> (result: Option<i64>)
        ensures
            result.is_some() ==> result.unwrap() as int == left as int + right as int,
    {
        left.checked_add(right)
    }

    // verus-claim: VS1-02
    pub fn checked_i64_mul(left: i64, right: i64) -> (result: Option<i64>)
        ensures
            result.is_some() ==> result.unwrap() as int == left as int * right as int,
    {
        left.checked_mul(right)
    }

    // verus-claim: VS1-02
    pub fn checked_u64_add(left: u64, right: u64) -> (result: Option<u64>)
        ensures
            result.is_some() ==> result.unwrap() as int == left as int + right as int,
    {
        left.checked_add(right)
    }

    // verus-claim: VS1-02
    pub fn checked_i64_neg(value: i64) -> (result: Option<i64>)
        ensures
            result.is_some() ==> result.unwrap() as int == -(value as int),
            result.is_none() ==> value == i64::MIN,
    {
        value.checked_mul(-1)
    }

    // verus-claim: VS1-02
    pub fn checked_i128_to_i64(value: i128) -> (result: Option<i64>)
        ensures
            result.is_some() ==> result.unwrap() as int == value as int,
    {
        i64::try_from(value).ok()
    }

    // verus-claim: VS1-02
    pub fn decode_i64(bytes: &[u8]) -> (result: Option<i64>)
        ensures
            bytes.len() != 8 ==> result.is_none(),
            bytes.len() == 8 ==> result.is_some(),
    {
        if bytes.len() != 8 {
            None
        } else {
            let bits = ((bytes[0] as u64) << 56)
                | ((bytes[1] as u64) << 48)
                | ((bytes[2] as u64) << 40)
                | ((bytes[3] as u64) << 32)
                | ((bytes[4] as u64) << 24)
                | ((bytes[5] as u64) << 16)
                | ((bytes[6] as u64) << 8)
                | bytes[7] as u64;
            Some(bits as i64)
        }
    }

    // verus-claim: VS1-02
    pub fn decode_u64(bytes: &[u8]) -> (result: Option<u64>)
        ensures
            bytes.len() != 8 ==> result.is_none(),
            bytes.len() == 8 ==> result.is_some(),
    {
        if bytes.len() != 8 {
            None
        } else {
            Some(((bytes[0] as u64) << 56)
                | ((bytes[1] as u64) << 48)
                | ((bytes[2] as u64) << 40)
                | ((bytes[3] as u64) << 32)
                | ((bytes[4] as u64) << 24)
                | ((bytes[5] as u64) << 16)
                | ((bytes[6] as u64) << 8)
                | bytes[7] as u64)
        }
    }
}

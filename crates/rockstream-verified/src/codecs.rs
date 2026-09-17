use vstd::prelude::*;

verus! {
    // verus-claim: VS2-03
    pub fn encode_u64_be(value: u64) -> (result: [u8; 8])
        ensures result == value.to_be_bytes(),
    {
        value.to_be_bytes()
    }

    // verus-claim: VS2-03
    pub fn decode_u64_be(bytes: &[u8]) -> (result: Option<u64>)
        ensures
            bytes.len() != 8 ==> result.is_none(),
            bytes.len() == 8 ==> result.is_some(),
    {
        if bytes.len() != 8 {
            None
        } else {
            Some(u64::from_be_bytes(bytes.try_into().unwrap()))
        }
    }

    // verus-claim: VS2-03
    pub fn encode_u128_be(value: u128) -> (result: [u8; 16])
        ensures result == value.to_be_bytes(),
    {
        value.to_be_bytes()
    }

    // verus-claim: VS2-03
    pub fn decode_u128_be(bytes: &[u8]) -> (result: Option<u128>)
        ensures
            bytes.len() != 16 ==> result.is_none(),
            bytes.len() == 16 ==> result.is_some(),
    {
        if bytes.len() != 16 {
            None
        } else {
            Some(u128::from_be_bytes(bytes.try_into().unwrap()))
        }
    }

    // verus-claim: VS2-03
    pub fn checked_usize_to_u32(value: usize) -> (result: Option<u32>)
        ensures result.is_some() ==> result.unwrap() as int == value as int,
    {
        u32::try_from(value).ok()
    }
}

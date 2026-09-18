use vstd::prelude::*;

verus! {
    // verus-claim: VS2-03
    pub fn encode_u64_be(value: u64) -> (result: [u8; 8])
        ensures result == [
            (value >> 56) as u8,
            (value >> 48) as u8,
            (value >> 40) as u8,
            (value >> 32) as u8,
            (value >> 24) as u8,
            (value >> 16) as u8,
            (value >> 8) as u8,
            value as u8,
        ],
    {
        [
            (value >> 56) as u8,
            (value >> 48) as u8,
            (value >> 40) as u8,
            (value >> 32) as u8,
            (value >> 24) as u8,
            (value >> 16) as u8,
            (value >> 8) as u8,
            value as u8,
        ]
    }

    // verus-claim: VS2-03
    pub fn decode_u64_be(bytes: &[u8]) -> (result: Option<u64>)
        ensures
            bytes.len() != 8 ==> result.is_none(),
            bytes.len() == 8 ==> result.is_some(),
            bytes.len() == 8 ==> result.unwrap() == (
                ((bytes[0] as u64) << 56)
                    | ((bytes[1] as u64) << 48)
                    | ((bytes[2] as u64) << 40)
                    | ((bytes[3] as u64) << 32)
                    | ((bytes[4] as u64) << 24)
                    | ((bytes[5] as u64) << 16)
                    | ((bytes[6] as u64) << 8)
                    | bytes[7] as u64
            ),
    {
        if bytes.len() != 8 {
            None
        } else {
            Some(
                ((bytes[0] as u64) << 56)
                    | ((bytes[1] as u64) << 48)
                    | ((bytes[2] as u64) << 40)
                    | ((bytes[3] as u64) << 32)
                    | ((bytes[4] as u64) << 24)
                    | ((bytes[5] as u64) << 16)
                    | ((bytes[6] as u64) << 8)
                    | bytes[7] as u64,
            )
        }
    }

    // verus-claim: VS2-03
    pub fn encode_u128_be(value: u128) -> (result: [u8; 16])
        ensures result == [
            (value >> 120) as u8,
            (value >> 112) as u8,
            (value >> 104) as u8,
            (value >> 96) as u8,
            (value >> 88) as u8,
            (value >> 80) as u8,
            (value >> 72) as u8,
            (value >> 64) as u8,
            (value >> 56) as u8,
            (value >> 48) as u8,
            (value >> 40) as u8,
            (value >> 32) as u8,
            (value >> 24) as u8,
            (value >> 16) as u8,
            (value >> 8) as u8,
            value as u8,
        ],
    {
        [
            (value >> 120) as u8,
            (value >> 112) as u8,
            (value >> 104) as u8,
            (value >> 96) as u8,
            (value >> 88) as u8,
            (value >> 80) as u8,
            (value >> 72) as u8,
            (value >> 64) as u8,
            (value >> 56) as u8,
            (value >> 48) as u8,
            (value >> 40) as u8,
            (value >> 32) as u8,
            (value >> 24) as u8,
            (value >> 16) as u8,
            (value >> 8) as u8,
            value as u8,
        ]
    }

    // verus-claim: VS2-03
    pub fn decode_u128_be(bytes: &[u8]) -> (result: Option<u128>)
        ensures
            bytes.len() != 16 ==> result.is_none(),
            bytes.len() == 16 ==> result.is_some(),
            bytes.len() == 16 ==> result.unwrap() == (
                ((bytes[0] as u128) << 120)
                    | ((bytes[1] as u128) << 112)
                    | ((bytes[2] as u128) << 104)
                    | ((bytes[3] as u128) << 96)
                    | ((bytes[4] as u128) << 88)
                    | ((bytes[5] as u128) << 80)
                    | ((bytes[6] as u128) << 72)
                    | ((bytes[7] as u128) << 64)
                    | ((bytes[8] as u128) << 56)
                    | ((bytes[9] as u128) << 48)
                    | ((bytes[10] as u128) << 40)
                    | ((bytes[11] as u128) << 32)
                    | ((bytes[12] as u128) << 24)
                    | ((bytes[13] as u128) << 16)
                    | ((bytes[14] as u128) << 8)
                    | bytes[15] as u128
            ),
    {
        if bytes.len() != 16 {
            None
        } else {
            Some(
                ((bytes[0] as u128) << 120)
                    | ((bytes[1] as u128) << 112)
                    | ((bytes[2] as u128) << 104)
                    | ((bytes[3] as u128) << 96)
                    | ((bytes[4] as u128) << 88)
                    | ((bytes[5] as u128) << 80)
                    | ((bytes[6] as u128) << 72)
                    | ((bytes[7] as u128) << 64)
                    | ((bytes[8] as u128) << 56)
                    | ((bytes[9] as u128) << 48)
                    | ((bytes[10] as u128) << 40)
                    | ((bytes[11] as u128) << 32)
                    | ((bytes[12] as u128) << 24)
                    | ((bytes[13] as u128) << 16)
                    | ((bytes[14] as u128) << 8)
                    | bytes[15] as u128,
            )
        }
    }

    // verus-claim: VS2-03
    pub fn checked_usize_to_u32(value: usize) -> (result: Option<u32>)
        ensures
            result.is_some() <==> value as int <= u32::MAX as int,
            result.is_some() ==> result.unwrap() as int == value as int,
    {
        u32::try_from(value).ok()
    }
}

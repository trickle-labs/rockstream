use vstd::prelude::*;

verus! {
    pub const SIGN_BIT: u64 = 0x8000_0000_0000_0000;

    // verus-claim: VS2-02
    pub fn signed_order_key(value: i64, invert: bool) -> (result: u64)
        ensures
            !invert ==> result == ((value as u64) ^ SIGN_BIT),
            invert ==> result == !((value as u64) ^ SIGN_BIT),
    {
        let ascending = (value as u64) ^ SIGN_BIT;
        if invert { !ascending } else { ascending }
    }

    // verus-claim: VS2-02
    pub fn signed_order_key_decode(value: u64, invert: bool) -> (result: i64)
        ensures
            result == (if invert { !(value) } else { value } ^ SIGN_BIT) as i64,
    {
        let ascending = if invert { !value } else { value };
        (ascending ^ SIGN_BIT) as i64
    }

    // verus-claim: VS2-02
    pub proof fn signed_order_key_round_trips(value: i64, invert: bool)
        ensures (if invert {
            (!(!((value as u64) ^ SIGN_BIT)) ^ SIGN_BIT) as i64 == value
        } else {
            (((value as u64) ^ SIGN_BIT) ^ SIGN_BIT) as i64 == value
        }),
    {
        if invert {
            assert((!(!((value as u64) ^ SIGN_BIT)) ^ SIGN_BIT) as i64 == value)
                by (bit_vector);
        } else {
            assert((((value as u64) ^ SIGN_BIT) ^ SIGN_BIT) as i64 == value)
                by (bit_vector);
        }
    }

    // verus-claim: VS2-02
    pub proof fn signed_order_key_preserves_order(left: i64, right: i64)
        requires (left as int) < (right as int),
        ensures (((left as u64) ^ SIGN_BIT) as int) < (((right as u64) ^ SIGN_BIT) as int),
            ((!((right as u64) ^ SIGN_BIT)) as int) < ((!((left as u64) ^ SIGN_BIT)) as int),
    {
        assert((((left as u64) ^ SIGN_BIT) as int) == (left as int) + 9223372036854775808)
            by (bit_vector);
        assert((((right as u64) ^ SIGN_BIT) as int) == (right as int) + 9223372036854775808)
            by (bit_vector);
        assert((!((right as u64) ^ SIGN_BIT)) as int
            == 18446744073709551615 - (((right as u64) ^ SIGN_BIT) as int))
            by (bit_vector);
        assert((!((left as u64) ^ SIGN_BIT)) as int
            == 18446744073709551615 - (((left as u64) ^ SIGN_BIT) as int))
            by (bit_vector);
    }
}

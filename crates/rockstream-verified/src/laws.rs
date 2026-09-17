use vstd::prelude::*;

verus! {
    // verus-claim: VS1-03
    pub proof fn i64_add_identity(value: int)
        ensures value + 0 == value,
    {
    }

    // verus-claim: VS1-03
    pub proof fn i64_add_commutative(left: int, right: int)
        ensures left + right == right + left,
    {
    }

    // verus-claim: VS1-03
    pub proof fn i64_add_associative(left: int, middle: int, right: int)
        ensures (left + middle) + right == left + (middle + right),
    {
    }

    // verus-claim: VS1-03
    pub proof fn i64_add_inverse(value: int)
        ensures value + (-value) == 0,
    {
    }
}

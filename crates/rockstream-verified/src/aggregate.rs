use vstd::prelude::*;

verus! {
    // verus-claim: VS3-04
    pub fn transition(
        old_sum: i64,
        old_count: i64,
        delta_sum: i128,
        delta_count: i64,
    ) -> (result: Option<(i64, i64)>)
        requires
            old_count >= 0,
            old_count == 0 ==> old_sum == 0,
        ensures
            result.is_some() ==> {
                let state = result.unwrap();
                state.1 >= 0
                    && state.1 as int == old_count as int + delta_count as int
                    && (state.1 == 0 ==> state.0 == 0)
                    && (state.1 > 0 ==> state.0 as int == old_sum as int + delta_sum)
            },
    {
        if old_count < 0 || (old_count == 0 && old_sum != 0) {
            return None;
        }
        let new_count = old_count.checked_add(delta_count)?;
        if new_count < 0 {
            return None;
        }

        let total_sum = (old_sum as i128).checked_add(delta_sum)?;
        if new_count == 0 {
            return if total_sum == 0 {
                Some((0, 0))
            } else {
                None
            };
        }

        let new_sum = i64::try_from(total_sum).ok()?;
        Some((new_sum, new_count))
    }
}

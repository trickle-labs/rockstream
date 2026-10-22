use vstd::prelude::*;

verus! {
    // verus-claim: VS7-01
    pub fn same_owner(reservation: usize, channel: usize) -> (result: bool)
        ensures result == (reservation == channel),
    {
        reservation == channel
    }

    // verus-claim: VS7-01
    pub fn release_accounting(
        owner_matches: bool,
        rows: u32,
        batches: usize,
        bytes: usize,
        total_bytes: u64,
        total_requests: u64,
        released_rows: u32,
        released_bytes: usize,
    ) -> (result: (u32, usize, usize, u64, u64))
        ensures
            !owner_matches ==> result.0 == rows && result.1 == batches && result.2 == bytes,
            owner_matches && rows >= released_rows && batches > 0 && bytes >= released_bytes
                ==> result.0 == rows - released_rows
                    && result.1 == batches - 1
                    && result.2 == bytes - released_bytes,
            total_bytes >= released_bytes as u64
                ==> result.3 == total_bytes - released_bytes as u64,
            total_requests > 0 ==> result.4 == total_requests - 1,
            owner_matches && bytes >= released_bytes && total_bytes >= released_bytes as u64
                ==> result.3 as int - result.2 as int == total_bytes as int - bytes as int,
            owner_matches && batches > 0 && total_requests > 0
                ==> result.4 as int - result.1 as int == total_requests as int - batches as int,
    {
        let (next_rows, next_batches, next_bytes) = if owner_matches {
            (
                rows.saturating_sub(released_rows),
                batches.saturating_sub(1),
                bytes.saturating_sub(released_bytes),
            )
        } else {
            (rows, batches, bytes)
        };
        (
            next_rows,
            next_batches,
            next_bytes,
            total_bytes.saturating_sub(released_bytes as u64),
            total_requests.saturating_sub(1),
        )
    }
}

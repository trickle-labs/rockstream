use vstd::prelude::*;

verus! {
    pub const INVALID: u8 = 0;
    pub const INCOMPLETE: u8 = 1;
    pub const COMPLETE: u8 = 2;
    pub const MAX_PACKET_BYTES: usize = 64 * 1024 * 1024;

    // verus-claim: VS7-03
    pub fn read_length(bytes: &[u8], offset: usize) -> (result: Option<i32>)
        ensures
            result.is_some() ==> offset <= bytes.len() && bytes.len() - offset >= 4,
            offset > bytes.len() || bytes.len() - offset < 4 ==> result.is_none(),
    {
        let end = offset.checked_add(4)?;
        if end > bytes.len() {
            return None;
        }
        let raw = ((bytes[offset] as u32) << 24)
            | ((bytes[offset + 1] as u32) << 16)
            | ((bytes[offset + 2] as u32) << 8)
            | bytes[offset + 3] as u32;
        Some(raw as i32)
    }

    // verus-claim: VS7-03
    pub fn frame_span(available: usize, offset: usize, raw_length: Option<i32>) -> (result: (u8, usize, usize))
        ensures
            result.0 == INCOMPLETE ==> result.1 == 0 && result.2 == 0,
            result.0 == COMPLETE ==> raw_length.is_some()
                && raw_length.unwrap() >= 4
                && (raw_length.unwrap() as usize) <= MAX_PACKET_BYTES
                && result.1 == offset + 4
                && result.2 == offset + raw_length.unwrap() as usize
                && result.1 <= result.2
                && result.2 <= available,
            result.0 == INVALID ==> raw_length.is_some(),
    {
        let Some(raw_length) = raw_length else {
            return (INCOMPLETE, 0, 0);
        };
        if raw_length < 4 || raw_length as usize > MAX_PACKET_BYTES {
            return (INVALID, 0, 0);
        }
        let Some(body_start) = offset.checked_add(4) else {
            return (INVALID, 0, 0);
        };
        let Some(end) = offset.checked_add(raw_length as usize) else {
            return (INVALID, 0, 0);
        };
        if available < end {
            (INCOMPLETE, 0, 0)
        } else {
            (COMPLETE, body_start, end)
        }
    }
}

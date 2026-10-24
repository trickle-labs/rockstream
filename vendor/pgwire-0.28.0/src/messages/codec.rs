use std::str;

use bytes::{Buf, BufMut, BytesMut};

use crate::error::{PgWireError, PgWireResult};

/// Get null-terminated string, returns None when empty cstring read.
///
/// Note that this implementation will also advance cursor by 1 after reading
/// empty cstring. This behaviour works for how postgres wire protocol handling
/// key-value pairs, which is ended by a single `\0`
pub(crate) fn get_cstring(buf: &mut BytesMut) -> Option<String> {
    let mut i = 0;

    // with bound check to prevent invalid format
    while i < buf.remaining() && buf[i] != b'\0' {
        i += 1;
    }

    // i+1: include the '\0'
    // move cursor to the end of cstring
    // Unterminated strings are invalid; the packet reader will detect
    // the unconsumed malformed body instead of splitting beyond its end.
    if i == buf.remaining() {
        return None;
    }
    let string_buf = buf.split_to(i + 1);

    if i == 0 {
        None
    } else {
        Some(String::from_utf8_lossy(&string_buf[..i]).into_owned())
    }
}

/// Put null-termianted string
///
/// You can put empty string by giving `""` as input.
pub(crate) fn put_cstring(buf: &mut BytesMut, input: &str) {
    buf.put_slice(input.as_bytes());
    buf.put_u8(b'\0');
}

pub(crate) fn put_option_cstring(buf: &mut BytesMut, input: &Option<String>) {
    if let Some(input) = input {
        put_cstring(buf, input);
    } else {
        buf.put_u8(b'\0');
    }
}

/// Try to read message length from buf, without actually move the cursor
pub(crate) fn get_length(buf: &BytesMut, offset: usize) -> Option<usize> {
    if buf.remaining() >= 4 + offset {
        Some((&buf[offset..4 + offset]).get_i32() as usize)
    } else {
        None
    }
}

/// Check if message_length matches and move the cursor to right position then
/// call the `decode_fn` for the body
/// Maximum encoded packet length including the 4-byte length field.
const MAX_PACKET_BYTES: usize = 64 * 1024 * 1024;

/// Parse a complete, bounded message body. Invalid lengths and fields must
/// fail closed without consuming another frame from the stream.
pub(crate) fn decode_packet<T, F>(
    buf: &mut BytesMut,
    offset: usize,
    decode_fn: F,
) -> PgWireResult<Option<T>>
where
    F: Fn(&mut BytesMut, usize) -> PgWireResult<T>,
{
    if let Some(msg_len) = get_length(buf, offset) {
        if !(4..=MAX_PACKET_BYTES).contains(&msg_len) {
            return Err(PgWireError::InvalidMessageLength(msg_len));
        }
        let total = msg_len
            .checked_add(offset)
            .ok_or(PgWireError::InvalidMessageLength(msg_len))?;
        if buf.remaining() >= total {
            buf.advance(offset + 4);
            let mut body = buf.split_to(msg_len - 4);
            // Defensive isolation for the legacy 0.28 message decoders.
            // Framing checks are necessary even when panic=abort is enabled.
            let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                decode_fn(&mut body, msg_len)
            }))
            .map_err(|_| PgWireError::InvalidMessageBody)??;
            if body.has_remaining() {
                return Err(PgWireError::InvalidMessageBody);
            }
            return Ok(Some(decoded));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod frame_regressions {
    use super::*;
    use crate::messages::PgWireFrontendMessage;

    #[test]
    fn zero_and_negative_length_copy_frames_are_rejected_without_panicking() {
        for bytes in [&b"d\x00\x00\x00\x00"[..], &b"d\xff\xff\xff\xff"[..]] {
            let result = std::panic::catch_unwind(|| {
                PgWireFrontendMessage::decode(&mut BytesMut::from(bytes))
            });
            assert!(result.is_ok(), "invalid framing must not panic");
            assert!(result.unwrap().is_err(), "invalid framing must fail closed");
        }
    }

    #[test]
    fn missing_query_terminator_is_rejected() {
        let mut bytes = BytesMut::from(&b"Q\x00\x00\x00\x06xy"[..]);
        assert!(PgWireFrontendMessage::decode(&mut bytes).is_err());
        let mut no_body = BytesMut::from(&b"Q\x00\x00\x00\x04"[..]);
        assert!(PgWireFrontendMessage::decode(&mut no_body).is_err());
        let mut empty_sql = BytesMut::from(&b"Q\x00\x00\x00\x05\x00"[..]);
        assert!(matches!(
            PgWireFrontendMessage::decode(&mut empty_sql),
            Ok(Some(PgWireFrontendMessage::Query(_)))
        ));
    }

    #[test]
    fn malformed_parse_requires_both_cstrings_and_exact_oid_payload() {
        // The longer scheduled fuzz minimized another abort-on-panic to
        // 50 00 00 00 04: Parse with a zero-byte body.
        for input in [
            &b"P\x00\x00\x00\x04"[..],
            &b"P\x00\x00\x00\x05\x00"[..],
            &b"P\x00\x00\x00\x06\x00\x00"[..],
            &b"P\x00\x00\x00\x08\x00\x00\x00\x01"[..],
            &b"P\x00\x00\x00\x0a\x00\x00\x00\x01\x00\x00"[..],
        ] {
            let mut input = BytesMut::from(input);
            assert!(
                PgWireFrontendMessage::decode(&mut input).is_err(),
                "incomplete Parse frame must fail without panicking"
            );
        }
        let mut valid = BytesMut::from(&b"P\x00\x00\x00\x08\x00\x00\x00\x00"[..]);
        assert!(matches!(
            PgWireFrontendMessage::decode(&mut valid),
            Ok(Some(PgWireFrontendMessage::Parse(_)))
        ));
    }

    #[test]
    fn malformed_execute_without_portal_name_and_row_count_fails_closed() {
        // The scheduled 2026-10-22 fuzz run minimized a second panic to this
        // five-byte Execute message with an empty body.
        for payload in [
            &b"E\x00\x00\x00\x04"[..],
            &b"E\x00\x00\x00\x05\x00"[..],
            &b"E\x00\x00\x00\x08\x00\x00\x00\x00"[..],
        ] {
            let mut frame = BytesMut::from(payload);
            assert!(
                PgWireFrontendMessage::decode(&mut frame).is_err(),
                "short Execute must be rejected"
            );
        }
        let mut valid = BytesMut::from(&b"E\x00\x00\x00\x09\x00\x00\x00\x00\x00"[..]);
        assert!(matches!(
            PgWireFrontendMessage::decode(&mut valid),
            Ok(Some(PgWireFrontendMessage::Execute(_)))
        ));
    }

    #[test]
    fn complete_copy_data_remains_accepted() {
        let mut bytes = BytesMut::from(&b"d\x00\x00\x00\x06hi"[..]);
        assert!(matches!(
            PgWireFrontendMessage::decode(&mut bytes),
            Ok(Some(PgWireFrontendMessage::CopyData(_)))
        ));
    }
}

// pub(crate) fn get_and_ensure_message_type(buf: &mut BytesMut, t: u8) -> PgWireResult<()> {
//     let msg_type = buf[0];
//     // ensure the type is corrent
//     if msg_type != t {
//         return Err(PgWireError::InvalidMessageType(t, msg_type));
//     }

//     Ok(())
// }

pub(crate) fn option_string_len(s: &Option<String>) -> usize {
    1 + s.as_ref().map(|s| s.len()).unwrap_or(0)
}

use std::str;

use bytes::{Buf, BufMut, BytesMut};
use rockstream_verified::pgwire_framing;

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
pub(crate) fn get_length(buf: &BytesMut, offset: usize) -> Option<i32> {
    pgwire_framing::read_length(buf.as_ref(), offset)
}

/// Check if message_length matches and move the cursor to right position then
/// call the `decode_fn` for the body
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
    let raw_length = get_length(buf, offset);
    let (status, body_start, end) = pgwire_framing::frame_span(buf.remaining(), offset, raw_length);
    if status == pgwire_framing::INVALID {
        return Err(PgWireError::InvalidMessageLength(
            raw_length.unwrap() as usize
        ));
    }
    if status == pgwire_framing::COMPLETE {
        buf.advance(body_start);
        let mut body = buf.split_to(end - body_start);
        // Defensive isolation for the legacy 0.28 message decoders.
        // Framing checks are necessary even when panic=abort is enabled.
        let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decode_fn(&mut body, end - offset)
        }))
        .map_err(|_| PgWireError::InvalidMessageBody)??;
        if body.has_remaining() {
            return Err(PgWireError::InvalidMessageBody);
        }
        return Ok(Some(decoded));
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
}

#[cfg(test)]
mod framing_properties {
    use super::*;
    use crate::messages::{
        extendedquery::{Bind, Execute, Parse},
        simplequery::Query,
        startup::Startup,
        Message, PgWireFrontendMessage,
    };
    use bytes::Bytes;

    fn decode_startup(buf: &mut BytesMut) -> PgWireResult<Option<String>> {
        Startup::decode(buf).map(|message| message.map(|message| format!("{message:?}")))
    }

    fn decode_frontend(buf: &mut BytesMut) -> PgWireResult<Option<String>> {
        PgWireFrontendMessage::decode(buf)
            .map(|message| message.map(|message| format!("{message:?}")))
    }

    fn canonical_frames() -> Vec<(
        &'static str,
        BytesMut,
        String,
        fn(&mut BytesMut) -> PgWireResult<Option<String>>,
    )> {
        let mut startup = Startup::new();
        startup.parameters.insert("user".into(), "alice".into());
        startup.parameters.insert("database".into(), "db".into());
        let mut bytes = BytesMut::new();
        startup.encode(&mut bytes).unwrap();
        let mut cases = vec![(
            "Startup",
            bytes,
            format!("{startup:?}"),
            decode_startup as fn(&mut BytesMut) -> PgWireResult<Option<String>>,
        )];
        for (name, message) in [
            (
                "Query",
                PgWireFrontendMessage::Query(Query::new("SELECT 1".into())),
            ),
            (
                "Parse",
                PgWireFrontendMessage::Parse(Parse::new(
                    Some("stmt".into()),
                    "SELECT $1".into(),
                    vec![23],
                )),
            ),
            (
                "Bind",
                PgWireFrontendMessage::Bind(Bind::new(
                    Some("portal".into()),
                    Some("stmt".into()),
                    vec![0],
                    vec![Some(Bytes::from_static(b"7")), None],
                    vec![0],
                )),
            ),
            (
                "Execute",
                PgWireFrontendMessage::Execute(Execute::new(Some("portal".into()), 10)),
            ),
        ] {
            let mut bytes = BytesMut::new();
            message.encode(&mut bytes).unwrap();
            cases.push((name, bytes, format!("{message:?}"), decode_frontend));
        }
        cases
    }

    #[test]
    fn every_valid_frame_split_preserves_bytes_then_consumes_only_its_frame() {
        let mut following = BytesMut::new();
        let next = PgWireFrontendMessage::Query(Query::new("SELECT 2".into()));
        next.encode(&mut following).unwrap();
        for (name, frame, expected, decode) in canonical_frames() {
            for split in 0..frame.len() {
                let mut input = BytesMut::from(&frame[..split]);
                let before = input.clone();
                assert_eq!(decode(&mut input).unwrap(), None, "{name} split {split}");
                assert_eq!(
                    input, before,
                    "{name} split {split} consumed incomplete input"
                );
                input.extend_from_slice(&frame[split..]);
                input.extend_from_slice(&following);
                assert_eq!(
                    decode(&mut input).unwrap(),
                    Some(expected.clone()),
                    "{name} split {split}"
                );
                assert_eq!(
                    input, following,
                    "{name} split {split} consumed the next frame"
                );
                assert_eq!(
                    decode_frontend(&mut input).unwrap(),
                    Some(format!("{next:?}")),
                    "{name} split {split}"
                );
                assert!(input.is_empty(), "{name} split {split} left bytes");
            }
        }
    }

    #[test]
    fn invalid_lengths_fail_without_consumption_and_complete_bad_bodies_leave_next_frame() {
        let mut following = BytesMut::new();
        PgWireFrontendMessage::Query(Query::new("SELECT 2".into()))
            .encode(&mut following)
            .unwrap();
        for (name, frame, _, decode) in canonical_frames() {
            let offset = usize::from(name != "Startup");
            for length in [0i32, 3, -1, pgwire_framing::MAX_PACKET_BYTES as i32 + 1] {
                let mut input = frame.clone();
                input[offset..offset + 4].copy_from_slice(&length.to_be_bytes());
                let before = input.clone();
                assert_eq!(
                    decode(&mut input).unwrap_err().to_string(),
                    format!(
                        "Invalid PostgreSQL protocol message length: {}",
                        length as usize
                    ),
                    "{name} length {length}"
                );
                assert_eq!(input, before, "{name} invalid length consumed bytes");
            }
        }

        let mut cases = canonical_frames();
        for (name, mut frame, _, decode) in cases.drain(..) {
            match name {
                "Startup" | "Query" => *frame.last_mut().unwrap() = b'x',
                "Parse" => {
                    let count = frame.len() - 6;
                    frame[count..count + 2].copy_from_slice(&2u16.to_be_bytes());
                }
                "Bind" => {
                    let count = frame.len() - 4;
                    frame[count..count + 2].copy_from_slice(&2u16.to_be_bytes());
                }
                "Execute" => {
                    let name_end = 5 + frame[5..].iter().position(|byte| *byte == 0).unwrap();
                    frame[name_end] = b'x';
                }
                _ => unreachable!(),
            }
            frame.extend_from_slice(&following);
            assert_eq!(
                decode(&mut frame).unwrap_err().to_string(),
                "Invalid or truncated PostgreSQL protocol message body",
                "{name}"
            );
            assert_eq!(
                frame, following,
                "{name} malformed body consumed the next frame"
            );
        }
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

use bytes::{Buf, BufMut, Bytes};

use super::{codec, Message};
use crate::error::{PgWireError, PgWireResult};

/// Request from frontend to parse a prepared query string
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Parse {
    pub name: Option<String>,
    pub query: String,
    pub type_oids: Vec<u32>,
}

pub const MESSAGE_TYPE_BYTE_PARSE: u8 = b'P';

impl Message for Parse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PARSE)
    }

    fn message_length(&self) -> usize {
        4 + codec::option_string_len(&self.name) // name
            + (1 + self.query.len()) // query
            + 2 + (4 * self.type_oids.len()) // type oids
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        codec::put_option_cstring(buf, &self.name);
        codec::put_cstring(buf, &self.query);

        buf.put_u16(self.type_oids.len() as u16);
        for oid in &self.type_oids {
            buf.put_u32(*oid);
        }

        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        // Check the complete Parse body before consuming any bytes.
        let Some(name_end) = buf.iter().position(|byte| *byte == 0) else {
            return Err(PgWireError::InvalidMessageBody);
        };
        let remaining = &buf[name_end + 1..];
        let Some(query_end) = remaining.iter().position(|byte| *byte == 0) else {
            return Err(PgWireError::InvalidMessageBody);
        };
        let fields = &remaining[query_end + 1..];
        if fields.len() < 2 {
            return Err(PgWireError::InvalidMessageBody);
        }
        let count = u16::from_be_bytes([fields[0], fields[1]]) as usize;
        if fields.len() - 2 != count * 4 {
            return Err(PgWireError::InvalidMessageBody);
        }
        let name = codec::get_cstring(buf);
        let query = codec::get_cstring(buf).unwrap_or_else(|| "".to_owned());
        let type_oid_count = buf.get_u16();

        let mut type_oids = Vec::with_capacity(type_oid_count as usize);
        for _ in 0..type_oid_count {
            type_oids.push(buf.get_u32());
        }

        Ok(Parse {
            name,
            query,
            type_oids,
        })
    }
}

/// Response for Parse command, sent from backend to frontend
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct ParseComplete;

pub const MESSAGE_TYPE_BYTE_PARSE_COMPLETE: u8 = b'1';

impl Message for ParseComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PARSE_COMPLETE)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    #[inline]
    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    #[inline]
    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(ParseComplete)
    }
}

/// Closing the prepared statement or portal
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Close {
    pub target_type: u8,
    pub name: Option<String>,
}

pub const TARGET_TYPE_BYTE_STATEMENT: u8 = b'S';
pub const TARGET_TYPE_BYTE_PORTAL: u8 = b'P';

pub const MESSAGE_TYPE_BYTE_CLOSE: u8 = b'C';

/// Close and Describe carry a target kind followed by exactly one C-string.
/// Check the entire body before reading any field: raw decoder fuzzing uses
/// panic=abort, where the outer decode_packet catch_unwind cannot recover.
fn validate_targeted_name_body(buf: &bytes::BytesMut) -> PgWireResult<()> {
    if buf.len() < 2
        || !matches!(buf[0], TARGET_TYPE_BYTE_STATEMENT | TARGET_TYPE_BYTE_PORTAL)
        || buf[buf.len() - 1] != 0
        || buf[1..buf.len() - 1].contains(&0)
    {
        return Err(crate::error::PgWireError::InvalidMessageBody);
    }
    Ok(())
}

impl Message for Close {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_CLOSE)
    }

    fn message_length(&self) -> usize {
        4 + 1 + codec::option_string_len(&self.name)
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        buf.put_u8(self.target_type);
        codec::put_option_cstring(buf, &self.name);
        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        validate_targeted_name_body(buf)?;
        let target_type = buf.get_u8();
        let name = codec::get_cstring(buf);

        Ok(Close { target_type, name })
    }
}

/// Response for Close command, sent from backend to frontend
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct CloseComplete;

pub const MESSAGE_TYPE_BYTE_CLOSE_COMPLETE: u8 = b'3';

impl Message for CloseComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_CLOSE_COMPLETE)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    #[inline]
    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    #[inline]
    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(CloseComplete)
    }
}

/// Bind command, for executing prepared statement
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Bind {
    pub portal_name: Option<String>,
    pub statement_name: Option<String>,
    pub parameter_format_codes: Vec<i16>,
    // None for Null data, TODO: consider wrapping this together with DataRow in
    // data.rs
    pub parameters: Vec<Option<Bytes>>,

    pub result_column_format_codes: Vec<i16>,
}

pub const MESSAGE_TYPE_BYTE_BIND: u8 = b'B';

impl Message for Bind {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_BIND)
    }

    fn message_length(&self) -> usize {
        4 + codec::option_string_len(&self.portal_name) + codec::option_string_len(&self.statement_name)
            + 2 // parameter_format_code len
            + (2 * self.parameter_format_codes.len()) // parameter_format_codes
            + 2 // parameters len
            + self.parameters.iter().map(|p| 4 + p.as_ref().map(|data| data.len()).unwrap_or(0)).sum::<usize>() // parameters
            + 2 // result_format_code len
            + (2 * self.result_column_format_codes.len()) // result_format_codes
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        codec::put_option_cstring(buf, &self.portal_name);
        codec::put_option_cstring(buf, &self.statement_name);

        buf.put_u16(self.parameter_format_codes.len() as u16);
        for c in &self.parameter_format_codes {
            buf.put_i16(*c);
        }

        buf.put_u16(self.parameters.len() as u16);
        for v in &self.parameters {
            if let Some(v) = v {
                buf.put_i32(v.len() as i32);
                buf.put_slice(v.as_ref());
            } else {
                buf.put_i32(-1);
            }
        }

        buf.put_i16(self.result_column_format_codes.len() as i16);
        for c in &self.result_column_format_codes {
            buf.put_i16(*c);
        }

        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        if !buf.contains(&0) {
            return Err(PgWireError::InvalidMessageBody);
        }
        let portal_name = codec::get_cstring(buf);
        if !buf.contains(&0) {
            return Err(PgWireError::InvalidMessageBody);
        }
        let statement_name = codec::get_cstring(buf);

        if buf.remaining() < 2 {
            return Err(PgWireError::InvalidMessageBody);
        }
        let parameter_format_code_len = buf.get_u16();
        if buf.remaining() < parameter_format_code_len as usize * 2 + 2 {
            return Err(PgWireError::InvalidMessageBody);
        }
        let mut parameter_format_codes = Vec::with_capacity(parameter_format_code_len as usize);

        for _ in 0..parameter_format_code_len {
            parameter_format_codes.push(buf.get_i16());
        }

        let parameter_len = buf.get_u16();
        let mut parameters = Vec::with_capacity(parameter_len as usize);
        for _ in 0..parameter_len {
            if buf.remaining() < 4 {
                return Err(PgWireError::InvalidMessageBody);
            }
            let data_len = buf.get_i32();

            if data_len >= 0 {
                if buf.remaining() < data_len as usize {
                    return Err(PgWireError::InvalidMessageBody);
                }
                parameters.push(Some(buf.split_to(data_len as usize).freeze()));
            } else if data_len == -1 {
                parameters.push(None);
            } else {
                return Err(PgWireError::InvalidMessageBody);
            }
        }

        if buf.remaining() < 2 {
            return Err(PgWireError::InvalidMessageBody);
        }
        let result_column_format_code_len = buf.get_i16();
        if result_column_format_code_len < 0
            || buf.remaining() < result_column_format_code_len as usize * 2
        {
            return Err(PgWireError::InvalidMessageBody);
        }
        let mut result_column_format_codes =
            Vec::with_capacity(result_column_format_code_len as usize);
        for _ in 0..result_column_format_code_len {
            result_column_format_codes.push(buf.get_i16());
        }

        Ok(Bind {
            portal_name,
            statement_name,

            parameter_format_codes,
            parameters,

            result_column_format_codes,
        })
    }
}

/// Success response for `Bind`
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct BindComplete;

pub const MESSAGE_TYPE_BYTE_BIND_COMPLETE: u8 = b'2';

impl Message for BindComplete {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_BIND_COMPLETE)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    #[inline]
    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    #[inline]
    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(BindComplete)
    }
}

/// Describe command fron frontend to backend. For getting information of
/// particular portal or statement
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Describe {
    pub target_type: u8,
    pub name: Option<String>,
}

pub const MESSAGE_TYPE_BYTE_DESCRIBE: u8 = b'D';

impl Message for Describe {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_DESCRIBE)
    }

    fn message_length(&self) -> usize {
        4 + 1 + codec::option_string_len(&self.name)
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        buf.put_u8(self.target_type);
        codec::put_option_cstring(buf, &self.name);
        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        validate_targeted_name_body(buf)?;
        let target_type = buf.get_u8();
        let name = codec::get_cstring(buf);

        Ok(Describe { target_type, name })
    }
}

/// Execute portal by its name
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Execute {
    pub name: Option<String>,
    pub max_rows: i32,
}

pub const MESSAGE_TYPE_BYTE_EXECUTE: u8 = b'E';

impl Message for Execute {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_EXECUTE)
    }

    fn message_length(&self) -> usize {
        4 + codec::option_string_len(&self.name) + 4
    }

    fn encode_body(&self, buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        codec::put_option_cstring(buf, &self.name);
        buf.put_i32(self.max_rows);
        Ok(())
    }

    fn decode_body(buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        // Execute requires a NUL-terminated portal name followed by a
        // four-byte max_rows field. Check both before calling Buf::get_i32;
        // fuzz builds can abort on a panic before catch_unwind can recover.
        let Some(name_end) = buf.iter().position(|byte| *byte == 0) else {
            return Err(crate::error::PgWireError::InvalidMessageBody);
        };
        if buf.len().saturating_sub(name_end + 1) < 4 {
            return Err(crate::error::PgWireError::InvalidMessageBody);
        }
        let name = codec::get_cstring(buf);
        let max_rows = buf.get_i32();

        Ok(Execute { name, max_rows })
    }
}

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Flush;

pub const MESSAGE_TYPE_BYTE_FLUSH: u8 = b'H';

impl Message for Flush {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_FLUSH)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(Flush)
    }
}

/// Execute portal by its name
#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct Sync;

pub const MESSAGE_TYPE_BYTE_SYNC: u8 = b'S';

impl Message for Sync {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_SYNC)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(Sync)
    }
}

#[non_exhaustive]
#[derive(PartialEq, Eq, Debug, new)]
pub struct PortalSuspended;

pub const MESSAGE_TYPE_BYTE_PORTAL_SUSPENDED: u8 = b's';

impl Message for PortalSuspended {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_PORTAL_SUSPENDED)
    }

    #[inline]
    fn message_length(&self) -> usize {
        4
    }

    fn encode_body(&self, _buf: &mut bytes::BytesMut) -> PgWireResult<()> {
        Ok(())
    }

    fn decode_body(_buf: &mut bytes::BytesMut, _: usize) -> PgWireResult<Self> {
        Ok(PortalSuspended)
    }
}

#[cfg(test)]
mod targeted_name_regressions {
    use super::{Close, Describe, Message, TARGET_TYPE_BYTE_PORTAL, TARGET_TYPE_BYTE_STATEMENT};
    use bytes::BytesMut;

    #[test]
    fn malformed_close_and_describe_bodies_do_not_panic_or_consume_input() {
        for body in [
            &b""[..],                    // no target byte
            &b"P"[..],                   // target without name terminator
            &b"X\x00"[..],               // unsupported target kind
            &b"Sunterminated"[..],       // missing NUL
            &b"S\x00junk"[..],           // extra bytes following NUL
            &b"P\x00\x00"[..],            // duplicate NUL
        ] {
            for tag in [b'C', b'D'] {
                let mut input = BytesMut::from(body);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if tag == b'C' {
                        Close::decode_body(&mut input, body.len() + 4).map(|_| ())
                    } else {
                        Describe::decode_body(&mut input, body.len() + 4).map(|_| ())
                    }
                }));
                assert!(result.is_ok(), "tag {tag} must not panic on {body:?}");
                assert!(result.unwrap().is_err(), "tag {tag} must reject {body:?}");
                assert_eq!(&input[..], body, "invalid body must not be consumed");
            }
        }
    }

    #[test]
    fn valid_close_and_describe_frames_keep_existing_roundtrips() {
        for target_type in [TARGET_TYPE_BYTE_STATEMENT, TARGET_TYPE_BYTE_PORTAL] {
            for name in [None, Some("named".to_owned())] {
                let close = Close {
                    target_type,
                    name: name.clone(),
                };
                let mut close_frame = BytesMut::new();
                close.encode(&mut close_frame).unwrap();
                assert_eq!(Close::decode(&mut close_frame).unwrap(), Some(close));
                assert!(close_frame.is_empty());

                let describe = Describe { target_type, name };
                let mut describe_frame = BytesMut::new();
                describe.encode(&mut describe_frame).unwrap();
                assert_eq!(Describe::decode(&mut describe_frame).unwrap(), Some(describe));
                assert!(describe_frame.is_empty());
            }
        }
    }
}

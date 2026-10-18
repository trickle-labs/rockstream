#![no_main]
use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use pgwire::messages::{startup::Startup, Message, PgWireFrontendMessage};

fuzz_target!(|data: &[u8]| {
    let _ = Startup::decode(&mut BytesMut::from(data));
    let _ = PgWireFrontendMessage::decode(&mut BytesMut::from(data));
});

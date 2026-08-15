#![no_main]
use libfuzzer_sys::fuzz_target;
use rockstream_connectors::kafka_source::decode_kafka_payload;

fuzz_target!(|data: &[u8]| {
    let _ = decode_kafka_payload(data);
});

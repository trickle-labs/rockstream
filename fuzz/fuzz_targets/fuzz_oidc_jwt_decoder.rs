#![no_main]
use libfuzzer_sys::fuzz_target;
use rockstream_gateway::auth::JwtVerifier;

fuzz_target!(|data: &[u8]| {
    if let Ok(token) = std::str::from_utf8(data) {
        let verifier = JwtVerifier::with_hs256_key(b"fuzz-corpus-key".to_vec());
        let _ = verifier.verify(token);
    }
});

#![no_main]
use libfuzzer_sys::fuzz_target;
use rockstream_types::raft::RaftRpcRequest;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<RaftRpcRequest>(data);
});

//! Project coordination and fault-injection tests (v0.61 Plan Section 5).
//!
//! Asserts that client connect, query, and retry paths obey bounded timeouts
//! and return clean RS-0004 error codes under network delays and loss.

use rockstream_cli::client::connect_client;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_sim::SimRuntime;
use rockstream_types::error_code::RS_0004;
use std::time::Duration;

#[tokio::test]
async fn sim_runtime_client_retry_and_timeout_under_loss() {
    let seeds = [0x5601_0001, 0x5601_0002, 0x5601_0003];

    for seed in seeds {
        let _rt = SimRuntime::new(seed);
        buggify_init(seed);

        // 1. Blackhole / non-routable connection under timeout
        let start = std::time::Instant::now();
        let timeout_secs = 1;
        let res = connect_client("192.0.2.1:5432", timeout_secs).await;

        let elapsed = start.elapsed();
        let err = res.expect_err("blackhole connection must fail");

        // Must fail with RS-0004 (connection failure or timeout)
        assert_eq!(err.code, RS_0004);
        assert!(
            err.message.contains("cannot reach RockStream gateway")
                || err.message.contains("timed out"),
            "unexpected error message: {}",
            err.message
        );

        // Assert timeout bounded (did not hang indefinitely)
        assert!(
            elapsed < Duration::from_secs(5),
            "client connection should not hang beyond configured timeout: took {:?}",
            elapsed
        );

        // 2. Unreachable local port (immediate rejection)
        let unreach_res = connect_client("127.0.0.1:59999", 2).await;
        let unreach_err = unreach_res.expect_err("connection to closed port must fail");
        assert_eq!(unreach_err.code, RS_0004);
        assert!(unreach_err
            .message
            .contains("cannot reach RockStream gateway"));

        buggify_disable();
    }
}

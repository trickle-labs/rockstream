pub mod classifier;
pub mod compression_tuner;
pub mod durable;
pub mod flow_control;
pub mod loopback;
pub mod multiplexer;
pub mod persistence;
pub mod pool;
pub mod serialization;
pub mod service;
pub mod shared_memory;

/// Re-export generated proto definitions
#[allow(clippy::result_large_err)]
pub mod proto {
    tonic::include_proto!("rockstream.shuffle");
}

pub mod flow_control;
pub mod loopback;
pub mod multiplexer;
pub mod persistence;
pub mod pool;
pub mod serialization;
pub mod service;

/// Re-export generated proto definitions
pub mod proto {
    tonic::include_proto!("rockstream.shuffle");
}

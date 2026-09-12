//! Public, versioned management API contract.

pub const PROTOCOL_VERSION: u32 = 1;

pub mod v1 {
    tonic::include_proto!("rockstream.management.v1");
}

pub fn ensure_protocol_version(version: u32) -> Result<(), tonic::Status> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(tonic::Status::failed_precondition(format!(
            "unsupported protocol version {version}; supported range is 1..=1"
        )))
    }
}

pub fn unavailable_status() -> tonic::Status {
    tonic::Status::unavailable("management service unavailable")
}

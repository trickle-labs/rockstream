use std::str::FromStr;

use rockstream_runtime::exchange::service::{ExchangeRegistry, ShuffleServer};
use rockstream_types::compatibility::{ProtocolVersion, SupportedVersionRange};
use tonic::metadata::{MetadataMap, MetadataValue};

fn metadata(version: ProtocolVersion) -> MetadataMap {
    let mut metadata = MetadataMap::new();
    metadata.insert(
        "protocol_version",
        MetadataValue::from_str(&version.0.to_string()).unwrap(),
    );
    metadata
}

#[allow(clippy::result_large_err)]
fn validate(
    receiver_range: SupportedVersionRange,
    sender_version: ProtocolVersion,
) -> Result<ProtocolVersion, tonic::Status> {
    ShuffleServer::new(ExchangeRegistry::new())
        .with_supported_protocol_range(receiver_range)
        .validate_protocol_version(&metadata(sender_version))
}

#[test]
fn v1_header_delivers_shuffle_frame() {
    assert_eq!(
        validate(SupportedVersionRange::v1_only(), ProtocolVersion::V1).unwrap(),
        ProtocolVersion::V1
    );
}

#[test]
fn n_plus_1_to_n_is_accepted() {
    assert!(validate(SupportedVersionRange::v1_only(), ProtocolVersion::V1).is_ok());
}

#[test]
fn n_to_n_plus_1_is_accepted() {
    assert!(validate(SupportedVersionRange::v1_through_v2(), ProtocolVersion::V1).is_ok());
}

#[test]
fn newer_protocol_refused_rs5021_not_rs5002() {
    let error = validate(SupportedVersionRange::v1_only(), ProtocolVersion::V2).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("RS-5021"));
    assert!(!error.message().contains("RS-5002"));
}

#[test]
fn missing_protocol_header_refused_rs5021() {
    let error = ShuffleServer::new(ExchangeRegistry::new())
        .validate_protocol_version(&MetadataMap::new())
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("RS-5021"));
}

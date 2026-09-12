use rockstream_management_proto::{ensure_protocol_version, v1, PROTOCOL_VERSION};

#[test]
fn management_v1_contract_has_all_fourteen_methods() {
    let proto = include_str!("../proto/management/v1/management.proto");
    let service = proto
        .split("service ManagementService {")
        .nth(1)
        .unwrap()
        .split_once("\n}")
        .unwrap()
        .0;
    let methods = service
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("rpc "))
        .collect::<Vec<_>>();

    assert_eq!(
        methods,
        [
            "rpc GetClusterStatus(GetClusterStatusRequest) returns (GetClusterStatusResponse);",
            "rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);",
            "rpc GetNode(GetNodeRequest) returns (GetNodeResponse);",
            "rpc ListShards(ListShardsRequest) returns (ListShardsResponse);",
            "rpc GetShard(GetShardRequest) returns (GetShardResponse);",
            "rpc ListOperations(ListOperationsRequest) returns (ListOperationsResponse);",
            "rpc GetOperation(GetOperationRequest) returns (GetOperationResponse);",
            "rpc GetConfigSummary(GetConfigSummaryRequest) returns (GetConfigSummaryResponse);",
            "rpc GetCapabilities(GetCapabilitiesRequest) returns (GetCapabilitiesResponse);",
            "rpc GetHealth(GetHealthRequest) returns (GetHealthResponse);",
            "rpc DrainWorker(DrainWorkerRequest) returns (DrainWorkerResponse);",
            "rpc MigrateShard(MigrateShardRequest) returns (MigrateShardResponse);",
            "rpc CreateBackup(CreateBackupRequest) returns (CreateBackupResponse);",
            "rpc CancelOperation(CancelOperationRequest) returns (CancelOperationResponse);",
        ]
    );

    // These generated types also prove tonic emitted both sides of the contract.
    let _client = std::any::type_name::<
        v1::management_service_client::ManagementServiceClient<tonic::transport::Channel>,
    >();
    let _server = std::any::type_name::<
        v1::management_service_server::ManagementServiceServer<TestService>,
    >();
    let _registered = tonic::transport::Server::builder().add_service(
        v1::management_service_server::ManagementServiceServer::new(TestService),
    );
}

#[test]
fn every_request_and_response_carries_protocol_version() {
    macro_rules! assert_version_field {
        ($($message:ty),+ $(,)?) => {$({
            let mut message = <$message>::default();
            message.protocol_version = PROTOCOL_VERSION;
            assert_eq!(message.protocol_version, 1);
        })+};
    }

    assert_version_field!(
        v1::GetClusterStatusRequest,
        v1::GetClusterStatusResponse,
        v1::ListNodesRequest,
        v1::ListNodesResponse,
        v1::GetNodeRequest,
        v1::GetNodeResponse,
        v1::ListShardsRequest,
        v1::ListShardsResponse,
        v1::GetShardRequest,
        v1::GetShardResponse,
        v1::ListOperationsRequest,
        v1::ListOperationsResponse,
        v1::GetOperationRequest,
        v1::GetOperationResponse,
        v1::GetConfigSummaryRequest,
        v1::GetConfigSummaryResponse,
        v1::GetCapabilitiesRequest,
        v1::GetCapabilitiesResponse,
        v1::GetHealthRequest,
        v1::GetHealthResponse,
        v1::DrainWorkerRequest,
        v1::DrainWorkerResponse,
        v1::MigrateShardRequest,
        v1::MigrateShardResponse,
        v1::CreateBackupRequest,
        v1::CreateBackupResponse,
        v1::CancelOperationRequest,
        v1::CancelOperationResponse,
    );
}

#[test]
fn management_v1_rejects_incompatible_versions() {
    assert!(ensure_protocol_version(1).is_ok());
    let error = ensure_protocol_version(2).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        error.message(),
        "unsupported protocol version 2; supported range is 1..=1"
    );
}

#[test]
fn unavailable_management_service_uses_unavailable_status() {
    let error = rockstream_management_proto::unavailable_status();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "management service unavailable");
}

struct TestService;

#[tonic::async_trait]
impl v1::management_service_server::ManagementService for TestService {
    async fn get_cluster_status(
        &self,
        _: tonic::Request<v1::GetClusterStatusRequest>,
    ) -> Result<tonic::Response<v1::GetClusterStatusResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn list_nodes(
        &self,
        _: tonic::Request<v1::ListNodesRequest>,
    ) -> Result<tonic::Response<v1::ListNodesResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn get_node(
        &self,
        _: tonic::Request<v1::GetNodeRequest>,
    ) -> Result<tonic::Response<v1::GetNodeResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn list_shards(
        &self,
        _: tonic::Request<v1::ListShardsRequest>,
    ) -> Result<tonic::Response<v1::ListShardsResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn get_shard(
        &self,
        _: tonic::Request<v1::GetShardRequest>,
    ) -> Result<tonic::Response<v1::GetShardResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn list_operations(
        &self,
        _: tonic::Request<v1::ListOperationsRequest>,
    ) -> Result<tonic::Response<v1::ListOperationsResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn get_operation(
        &self,
        _: tonic::Request<v1::GetOperationRequest>,
    ) -> Result<tonic::Response<v1::GetOperationResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn get_config_summary(
        &self,
        _: tonic::Request<v1::GetConfigSummaryRequest>,
    ) -> Result<tonic::Response<v1::GetConfigSummaryResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn get_capabilities(
        &self,
        _: tonic::Request<v1::GetCapabilitiesRequest>,
    ) -> Result<tonic::Response<v1::GetCapabilitiesResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn get_health(
        &self,
        _: tonic::Request<v1::GetHealthRequest>,
    ) -> Result<tonic::Response<v1::GetHealthResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn drain_worker(
        &self,
        _: tonic::Request<v1::DrainWorkerRequest>,
    ) -> Result<tonic::Response<v1::DrainWorkerResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn migrate_shard(
        &self,
        _: tonic::Request<v1::MigrateShardRequest>,
    ) -> Result<tonic::Response<v1::MigrateShardResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn create_backup(
        &self,
        _: tonic::Request<v1::CreateBackupRequest>,
    ) -> Result<tonic::Response<v1::CreateBackupResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
    async fn cancel_operation(
        &self,
        _: tonic::Request<v1::CancelOperationRequest>,
    ) -> Result<tonic::Response<v1::CancelOperationResponse>, tonic::Status> {
        Err(rockstream_management_proto::unavailable_status())
    }
}

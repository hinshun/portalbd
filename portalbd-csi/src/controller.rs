//! CSI Controller service implementation.
//!
//! Thin gRPC layer that delegates to DriverState for business logic.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::csi;
use crate::driver::DriverState;
use crate::error::{Error, Require};

pub struct ControllerService {
    state: Arc<DriverState>,
}

impl ControllerService {
    pub fn new(state: Arc<DriverState>) -> Self {
        Self { state }
    }

    fn required_capacity(range: Option<&csi::CapacityRange>) -> i64 {
        match range {
            Some(r) if r.required_bytes > 0 => r.required_bytes,
            Some(r) if r.limit_bytes > 0 => r.limit_bytes,
            _ => crate::driver::DEFAULT_VOLUME_SIZE,
        }
    }

    fn validate_capabilities(caps: &[csi::VolumeCapability]) -> Result<(), Status> {
        if caps.is_empty() {
            return Err(Status::invalid_argument("volume capabilities are required"));
        }
        for cap in caps {
            if cap.access_type.is_none() {
                return Err(Error::MissingCapability.into());
            }
            if let Some(mode) = &cap.access_mode {
                use csi::volume_capability::access_mode::Mode;
                match mode.mode() {
                    Mode::SingleNodeWriter | Mode::SingleNodeReaderOnly => {}
                    _ => return Err(Error::UnsupportedAccessMode.into()),
                }
            }
        }
        Ok(())
    }

    fn volume_to_proto(meta: &crate::driver::VolumeMetadata) -> csi::Volume {
        csi::Volume {
            volume_id: meta.volume_id.clone(),
            capacity_bytes: meta.capacity_bytes,
            volume_context: HashMap::from([(
                "nbd_address".to_string(),
                meta.nbd_address.as_str().to_string(),
            )]),
            content_source: None,
            accessible_topology: vec![],
        }
    }
}

#[tonic::async_trait]
impl csi::controller_server::Controller for ControllerService {
    async fn create_volume(
        &self,
        request: Request<csi::CreateVolumeRequest>,
    ) -> Result<Response<csi::CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        let name = req.name.require("volume name")?;
        Self::validate_capabilities(&req.volume_capabilities)?;

        let capacity = Self::required_capacity(req.capacity_range.as_ref());
        let meta = self
            .state
            .create_volume(&name, capacity, req.parameters)
            .await?;

        Ok(Response::new(csi::CreateVolumeResponse {
            volume: Some(Self::volume_to_proto(&meta)),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<csi::DeleteVolumeRequest>,
    ) -> Result<Response<csi::DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id.require("volume ID")?;

        self.state.delete_volume(&volume_id).await?;
        Ok(Response::new(csi::DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        _request: Request<csi::ControllerPublishVolumeRequest>,
    ) -> Result<Response<csi::ControllerPublishVolumeResponse>, Status> {
        // Not needed for NBD-based drivers; node handles attachment
        Ok(Response::new(csi::ControllerPublishVolumeResponse {
            publish_context: HashMap::new(),
        }))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: Request<csi::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<csi::ControllerUnpublishVolumeResponse>, Status> {
        // Not needed for NBD-based drivers; node handles detachment
        Ok(Response::new(csi::ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<csi::ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<csi::ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id.require("volume ID")?;

        // Verify volume exists
        self.state.get_volume_info(&volume_id)?;

        if let Err(e) = Self::validate_capabilities(&req.volume_capabilities) {
            return Ok(Response::new(csi::ValidateVolumeCapabilitiesResponse {
                confirmed: None,
                message: e.to_string(),
            }));
        }

        Ok(Response::new(csi::ValidateVolumeCapabilitiesResponse {
            confirmed: Some(csi::validate_volume_capabilities_response::Confirmed {
                volume_context: HashMap::new(),
                volume_capabilities: req.volume_capabilities,
                parameters: HashMap::new(),
                mutable_parameters: HashMap::new(),
            }),
            message: String::new(),
        }))
    }

    async fn list_volumes(
        &self,
        request: Request<csi::ListVolumesRequest>,
    ) -> Result<Response<csi::ListVolumesResponse>, Status> {
        let req = request.into_inner();

        let all_volumes = self.state.list_volumes()?;

        // Handle pagination
        let start = req.starting_token.parse::<usize>().unwrap_or(0);
        let max = if req.max_entries > 0 {
            req.max_entries as usize
        } else {
            all_volumes.len()
        };

        let entries: Vec<_> = all_volumes
            .iter()
            .skip(start)
            .take(max)
            .map(|meta| csi::list_volumes_response::Entry {
                volume: Some(Self::volume_to_proto(meta)),
                status: None,
            })
            .collect();

        let next_token = if start + entries.len() < all_volumes.len() {
            (start + entries.len()).to_string()
        } else {
            String::new()
        };

        Ok(Response::new(csi::ListVolumesResponse {
            entries,
            next_token,
        }))
    }

    async fn get_capacity(
        &self,
        _request: Request<csi::GetCapacityRequest>,
    ) -> Result<Response<csi::GetCapacityResponse>, Status> {
        // For now, report unlimited capacity
        Ok(Response::new(csi::GetCapacityResponse {
            available_capacity: i64::MAX,
            maximum_volume_size: None,
            minimum_volume_size: None,
        }))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<csi::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<csi::ControllerGetCapabilitiesResponse>, Status> {
        use csi::controller_service_capability::rpc::Type;

        let capabilities = [
            Type::CreateDeleteVolume,
            Type::ListVolumes,
            Type::GetCapacity,
            Type::GetVolume,
        ]
        .into_iter()
        .map(|t| csi::ControllerServiceCapability {
            r#type: Some(csi::controller_service_capability::Type::Rpc(
                csi::controller_service_capability::Rpc { r#type: t as i32 },
            )),
        })
        .collect();

        Ok(Response::new(csi::ControllerGetCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn create_snapshot(
        &self,
        _request: Request<csi::CreateSnapshotRequest>,
    ) -> Result<Response<csi::CreateSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots not yet implemented"))
    }

    async fn delete_snapshot(
        &self,
        _request: Request<csi::DeleteSnapshotRequest>,
    ) -> Result<Response<csi::DeleteSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots not yet implemented"))
    }

    async fn list_snapshots(
        &self,
        _request: Request<csi::ListSnapshotsRequest>,
    ) -> Result<Response<csi::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("snapshots not yet implemented"))
    }

    async fn get_snapshot(
        &self,
        _request: Request<csi::GetSnapshotRequest>,
    ) -> Result<Response<csi::GetSnapshotResponse>, Status> {
        Err(Status::unimplemented("snapshots not yet implemented"))
    }

    async fn controller_expand_volume(
        &self,
        _request: Request<csi::ControllerExpandVolumeRequest>,
    ) -> Result<Response<csi::ControllerExpandVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "volume expansion not yet implemented",
        ))
    }

    async fn controller_get_volume(
        &self,
        request: Request<csi::ControllerGetVolumeRequest>,
    ) -> Result<Response<csi::ControllerGetVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id.require("volume ID")?;
        let meta = self.state.get_volume_info(&volume_id)?;

        Ok(Response::new(csi::ControllerGetVolumeResponse {
            volume: Some(Self::volume_to_proto(&meta)),
            status: Some(csi::controller_get_volume_response::VolumeStatus {
                volume_condition: Some(csi::VolumeCondition {
                    abnormal: false,
                    message: String::new(),
                }),
                published_node_ids: vec![],
            }),
        }))
    }

    async fn controller_modify_volume(
        &self,
        _request: Request<csi::ControllerModifyVolumeRequest>,
    ) -> Result<Response<csi::ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "volume modification not yet implemented",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{BYTES_PER_GIB, DEFAULT_VOLUME_SIZE};
    use crate::test_util::TestFixture;
    use tonic::Code;

    fn single_node_writer_capability() -> csi::VolumeCapability {
        csi::VolumeCapability {
            access_type: Some(csi::volume_capability::AccessType::Mount(
                csi::volume_capability::MountVolume {
                    fs_type: "ext4".to_string(),
                    mount_flags: vec![],
                    volume_mount_group: String::new(),
                },
            )),
            access_mode: Some(csi::volume_capability::AccessMode {
                mode: csi::volume_capability::access_mode::Mode::SingleNodeWriter as i32,
            }),
        }
    }

    /// Builder for CreateVolumeRequest to reduce test boilerplate.
    struct CreateVolumeRequestBuilder {
        name: String,
        capacity_bytes: i64,
        capabilities: Vec<csi::VolumeCapability>,
    }

    impl CreateVolumeRequestBuilder {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                capacity_bytes: BYTES_PER_GIB,
                capabilities: vec![single_node_writer_capability()],
            }
        }

        fn capacity(mut self, bytes: i64) -> Self {
            self.capacity_bytes = bytes;
            self
        }

        fn capabilities(mut self, caps: Vec<csi::VolumeCapability>) -> Self {
            self.capabilities = caps;
            self
        }

        fn build(self) -> csi::CreateVolumeRequest {
            csi::CreateVolumeRequest {
                name: self.name,
                capacity_range: Some(csi::CapacityRange {
                    required_bytes: self.capacity_bytes,
                    limit_bytes: 0,
                }),
                volume_capabilities: self.capabilities,
                parameters: HashMap::new(),
                secrets: HashMap::new(),
                volume_content_source: None,
                accessibility_requirements: None,
                mutable_parameters: HashMap::new(),
            }
        }
    }

    #[tokio::test]
    async fn create_volume_is_idempotent_for_same_name_and_capacity() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let request = CreateVolumeRequestBuilder::new("test-volume").build();

        let response1 = csi::controller_server::Controller::create_volume(
            &service,
            Request::new(request.clone()),
        )
        .await
        .expect("first CreateVolume should succeed");

        let response2 =
            csi::controller_server::Controller::create_volume(&service, Request::new(request))
                .await
                .expect("second CreateVolume should succeed (idempotent)");

        let vol1 = response1.into_inner().volume.expect("volume should exist");
        let vol2 = response2.into_inner().volume.expect("volume should exist");

        assert_eq!(vol1.volume_id, vol2.volume_id);
        assert_eq!(vol1.capacity_bytes, vol2.capacity_bytes);
    }

    #[tokio::test]
    async fn create_volume_returns_already_exists_for_smaller_capacity() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();

        // Create with 1 GiB
        let request1 = CreateVolumeRequestBuilder::new("test-volume").build();
        csi::controller_server::Controller::create_volume(&service, Request::new(request1))
            .await
            .expect("first CreateVolume should succeed");

        // Try to create with 2 GiB - should fail because existing is smaller
        let request2 = CreateVolumeRequestBuilder::new("test-volume")
            .capacity(2 * BYTES_PER_GIB)
            .build();
        let err =
            csi::controller_server::Controller::create_volume(&service, Request::new(request2))
                .await
                .expect_err("CreateVolume with larger capacity should fail");

        assert_eq!(err.code(), Code::AlreadyExists);
    }

    #[tokio::test]
    async fn create_volume_requires_volume_capabilities() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let request = CreateVolumeRequestBuilder::new("test-volume")
            .capabilities(vec![])
            .build();

        let err =
            csi::controller_server::Controller::create_volume(&service, Request::new(request))
                .await
                .expect_err("CreateVolume without capabilities should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_volume_requires_name() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let request = CreateVolumeRequestBuilder::new("").build();

        let err =
            csi::controller_server::Controller::create_volume(&service, Request::new(request))
                .await
                .expect_err("CreateVolume without name should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn create_volume_uses_default_size_when_no_capacity_specified() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let mut request = CreateVolumeRequestBuilder::new("test-volume").build();
        request.capacity_range = None;

        let response =
            csi::controller_server::Controller::create_volume(&service, Request::new(request))
                .await
                .expect("CreateVolume should succeed");

        let volume = response.into_inner().volume.expect("volume should exist");
        assert!(volume.capacity_bytes >= DEFAULT_VOLUME_SIZE);
    }

    #[tokio::test]
    async fn create_volume_rejects_unsupported_access_modes() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();

        let multi_node_cap = csi::VolumeCapability {
            access_type: Some(csi::volume_capability::AccessType::Mount(
                csi::volume_capability::MountVolume {
                    fs_type: "ext4".to_string(),
                    mount_flags: vec![],
                    volume_mount_group: String::new(),
                },
            )),
            access_mode: Some(csi::volume_capability::AccessMode {
                mode: csi::volume_capability::access_mode::Mode::MultiNodeMultiWriter as i32,
            }),
        };

        let request = CreateVolumeRequestBuilder::new("test-volume")
            .capabilities(vec![multi_node_cap])
            .build();
        let err =
            csi::controller_server::Controller::create_volume(&service, Request::new(request))
                .await
                .expect_err("CreateVolume with unsupported access mode should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn delete_volume_is_idempotent_for_nonexistent_volume() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let request = csi::DeleteVolumeRequest {
            volume_id: "nonexistent-volume-id".to_string(),
            secrets: HashMap::new(),
        };

        csi::controller_server::Controller::delete_volume(&service, Request::new(request.clone()))
            .await
            .expect("DeleteVolume for nonexistent volume should succeed (idempotent)");

        csi::controller_server::Controller::delete_volume(&service, Request::new(request))
            .await
            .expect("second call should also succeed");
    }

    #[tokio::test]
    async fn delete_volume_requires_volume_id() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let request = csi::DeleteVolumeRequest {
            volume_id: String::new(),
            secrets: HashMap::new(),
        };

        let err =
            csi::controller_server::Controller::delete_volume(&service, Request::new(request))
                .await
                .expect_err("DeleteVolume without volume_id should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn validate_volume_capabilities_confirms_supported_capabilities() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();

        // First create a volume
        let create_request = CreateVolumeRequestBuilder::new("test-volume").build();
        let create_response = csi::controller_server::Controller::create_volume(
            &service,
            Request::new(create_request),
        )
        .await
        .expect("CreateVolume should succeed");

        let volume_id = create_response.into_inner().volume.unwrap().volume_id;

        let validate_request = csi::ValidateVolumeCapabilitiesRequest {
            volume_id,
            volume_context: HashMap::new(),
            volume_capabilities: vec![single_node_writer_capability()],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            mutable_parameters: HashMap::new(),
        };

        let response = csi::controller_server::Controller::validate_volume_capabilities(
            &service,
            Request::new(validate_request),
        )
        .await
        .expect("ValidateVolumeCapabilities should succeed");

        assert!(response.into_inner().confirmed.is_some());
    }

    #[tokio::test]
    async fn validate_volume_capabilities_returns_not_found_for_missing_volume() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let request = csi::ValidateVolumeCapabilitiesRequest {
            volume_id: "nonexistent-volume".to_string(),
            volume_context: HashMap::new(),
            volume_capabilities: vec![single_node_writer_capability()],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            mutable_parameters: HashMap::new(),
        };

        let err = csi::controller_server::Controller::validate_volume_capabilities(
            &service,
            Request::new(request),
        )
        .await
        .expect_err("ValidateVolumeCapabilities for nonexistent volume should fail");

        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn list_volumes_returns_empty_when_no_volumes() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();

        let response = csi::controller_server::Controller::list_volumes(
            &service,
            Request::new(csi::ListVolumesRequest {
                max_entries: 0,
                starting_token: String::new(),
            }),
        )
        .await
        .expect("ListVolumes should succeed");

        let list = response.into_inner();
        assert!(list.entries.is_empty());
        assert!(list.next_token.is_empty());
    }

    #[tokio::test]
    async fn list_volumes_supports_pagination() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();

        // Create 3 volumes
        for i in 0..3 {
            let request = CreateVolumeRequestBuilder::new(&format!("test-volume-{}", i)).build();
            csi::controller_server::Controller::create_volume(&service, Request::new(request))
                .await
                .expect("CreateVolume should succeed");
        }

        // List with max_entries=2
        let response1 = csi::controller_server::Controller::list_volumes(
            &service,
            Request::new(csi::ListVolumesRequest {
                max_entries: 2,
                starting_token: String::new(),
            }),
        )
        .await
        .expect("ListVolumes should succeed");

        let list1 = response1.into_inner();
        assert_eq!(list1.entries.len(), 2);
        assert!(!list1.next_token.is_empty());

        // List next page
        let response2 = csi::controller_server::Controller::list_volumes(
            &service,
            Request::new(csi::ListVolumesRequest {
                max_entries: 2,
                starting_token: list1.next_token,
            }),
        )
        .await
        .expect("ListVolumes should succeed");

        let list2 = response2.into_inner();
        assert_eq!(list2.entries.len(), 1);
        assert!(list2.next_token.is_empty());
    }

    #[tokio::test]
    async fn controller_get_volume_returns_not_found_for_missing_volume() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();
        let request = csi::ControllerGetVolumeRequest {
            volume_id: "nonexistent-volume".to_string(),
        };

        let err = csi::controller_server::Controller::controller_get_volume(
            &service,
            Request::new(request),
        )
        .await
        .expect_err("ControllerGetVolume for nonexistent volume should fail");

        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn controller_get_capabilities_returns_expected_capabilities() {
        let fixture = TestFixture::new();
        let service = fixture.controller_service();

        let response = csi::controller_server::Controller::controller_get_capabilities(
            &service,
            Request::new(csi::ControllerGetCapabilitiesRequest {}),
        )
        .await
        .expect("ControllerGetCapabilities should succeed");

        let caps = response.into_inner().capabilities;
        let has_cap = |t: csi::controller_service_capability::rpc::Type| {
            caps.iter().any(|cap| {
                matches!(
                    &cap.r#type,
                    Some(csi::controller_service_capability::Type::Rpc(rpc))
                        if rpc.r#type == t as i32
                )
            })
        };

        assert!(has_cap(
            csi::controller_service_capability::rpc::Type::CreateDeleteVolume
        ));
        assert!(has_cap(
            csi::controller_service_capability::rpc::Type::ListVolumes
        ));
        assert!(has_cap(
            csi::controller_service_capability::rpc::Type::GetVolume
        ));
    }
}

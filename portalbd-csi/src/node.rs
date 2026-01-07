//! CSI Node service implementation.
//!
//! Thin gRPC layer that delegates to DriverState for business logic.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::csi;
use crate::driver::DriverState;
use crate::error::{Error, Require};

pub struct NodeService {
    state: Arc<DriverState>,
}

impl NodeService {
    pub fn new(state: Arc<DriverState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl csi::node_server::Node for NodeService {
    async fn node_stage_volume(
        &self,
        request: Request<csi::NodeStageVolumeRequest>,
    ) -> Result<Response<csi::NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id.require("volume ID")?;
        let staging_path = req.staging_target_path.require("staging target path")?;
        let volume_cap = req.volume_capability.require("volume capability")?;

        if !self.state.volume_exists(&volume_id) {
            return Err(Error::VolumeNotFound(volume_id).into());
        }

        // Extract filesystem type and mount flags if mount access type
        let (fs_type, mount_flags) = match volume_cap.access_type {
            Some(csi::volume_capability::AccessType::Mount(mount)) => {
                let fs = if mount.fs_type.is_empty() {
                    "ext4"
                } else {
                    &mount.fs_type
                };
                (Some(fs.to_string()), mount.mount_flags)
            }
            _ => (None, vec![]),
        };

        self.state
            .stage_volume(&volume_id, &staging_path, fs_type.as_deref(), &mount_flags)
            .await?;

        Ok(Response::new(csi::NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<csi::NodeUnstageVolumeRequest>,
    ) -> Result<Response<csi::NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id.require("volume ID")?;
        let staging_path = req.staging_target_path.require("staging target path")?;

        self.state.unstage_volume(&volume_id, &staging_path).await?;
        Ok(Response::new(csi::NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        request: Request<csi::NodePublishVolumeRequest>,
    ) -> Result<Response<csi::NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id.require("volume ID")?;
        let target_path = req.target_path.require("target path")?;
        let volume_cap = req.volume_capability.require("volume capability")?;

        if req.staging_target_path.is_empty() {
            return Err(Status::failed_precondition(
                "staging target path is required",
            ));
        }

        let is_block = matches!(
            volume_cap.access_type,
            Some(csi::volume_capability::AccessType::Block(_))
        );

        self.state
            .publish_volume(
                &volume_id,
                &req.staging_target_path,
                &target_path,
                req.readonly,
                is_block,
            )
            .await?;

        Ok(Response::new(csi::NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<csi::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<csi::NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = req.volume_id.require("volume ID")?;
        let target_path = req.target_path.require("target path")?;

        self.state
            .unpublish_volume(&volume_id, &target_path)
            .await?;
        Ok(Response::new(csi::NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        _request: Request<csi::NodeGetVolumeStatsRequest>,
    ) -> Result<Response<csi::NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("volume stats not yet implemented"))
    }

    async fn node_expand_volume(
        &self,
        _request: Request<csi::NodeExpandVolumeRequest>,
    ) -> Result<Response<csi::NodeExpandVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "volume expansion not yet implemented",
        ))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<csi::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<csi::NodeGetCapabilitiesResponse>, Status> {
        use csi::node_service_capability::rpc::Type;

        let capabilities = vec![csi::NodeServiceCapability {
            r#type: Some(csi::node_service_capability::Type::Rpc(
                csi::node_service_capability::Rpc {
                    r#type: Type::StageUnstageVolume as i32,
                },
            )),
        }];

        Ok(Response::new(csi::NodeGetCapabilitiesResponse {
            capabilities,
        }))
    }

    async fn node_get_info(
        &self,
        _request: Request<csi::NodeGetInfoRequest>,
    ) -> Result<Response<csi::NodeGetInfoResponse>, Status> {
        Ok(Response::new(csi::NodeGetInfoResponse {
            node_id: self.state.config.node_id.clone(),
            max_volumes_per_node: 0,
            accessible_topology: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TestFixture;
    use std::collections::HashMap;
    use tonic::Code;

    fn mount_capability() -> csi::VolumeCapability {
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

    fn stage_request(
        volume_id: &str,
        path: &str,
        cap: Option<csi::VolumeCapability>,
    ) -> csi::NodeStageVolumeRequest {
        csi::NodeStageVolumeRequest {
            volume_id: volume_id.to_string(),
            publish_context: HashMap::new(),
            staging_target_path: path.to_string(),
            volume_capability: cap,
            secrets: HashMap::new(),
            volume_context: HashMap::new(),
        }
    }

    fn publish_request(
        volume_id: &str,
        staging: &str,
        target: &str,
    ) -> csi::NodePublishVolumeRequest {
        csi::NodePublishVolumeRequest {
            volume_id: volume_id.to_string(),
            publish_context: HashMap::new(),
            staging_target_path: staging.to_string(),
            target_path: target.to_string(),
            volume_capability: Some(mount_capability()),
            readonly: false,
            secrets: HashMap::new(),
            volume_context: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn node_get_info_returns_node_id() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();

        let response = csi::node_server::Node::node_get_info(
            &service,
            Request::new(csi::NodeGetInfoRequest {}),
        )
        .await
        .expect("NodeGetInfo should succeed");

        let info = response.into_inner();
        assert!(!info.node_id.is_empty());
        assert_eq!(info.node_id, "test-node");
    }

    #[tokio::test]
    async fn node_get_capabilities_returns_stage_unstage() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();

        let response = csi::node_server::Node::node_get_capabilities(
            &service,
            Request::new(csi::NodeGetCapabilitiesRequest {}),
        )
        .await
        .expect("NodeGetCapabilities should succeed");

        let caps = response.into_inner().capabilities;
        let has_stage_unstage = caps.iter().any(|cap| {
            matches!(
                &cap.r#type,
                Some(csi::node_service_capability::Type::Rpc(rpc))
                    if rpc.r#type == csi::node_service_capability::rpc::Type::StageUnstageVolume as i32
            )
        });
        assert!(has_stage_unstage);
    }

    #[tokio::test]
    async fn node_stage_volume_requires_volume_id() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = stage_request("", "/staging", Some(mount_capability()));

        let err = csi::node_server::Node::node_stage_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn node_stage_volume_requires_staging_target_path() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = stage_request("test-volume", "", Some(mount_capability()));

        let err = csi::node_server::Node::node_stage_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn node_stage_volume_requires_volume_capability() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = stage_request("test-volume", "/staging", None);

        let err = csi::node_server::Node::node_stage_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn node_stage_volume_returns_not_found_for_missing_volume() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = stage_request("nonexistent-volume", "/staging", Some(mount_capability()));

        let err = csi::node_server::Node::node_stage_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn node_unstage_volume_is_idempotent() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = csi::NodeUnstageVolumeRequest {
            volume_id: "nonexistent-volume".to_string(),
            staging_target_path: "/nonexistent/path".to_string(),
        };

        csi::node_server::Node::node_unstage_volume(&service, Request::new(request.clone()))
            .await
            .expect("first call should succeed");

        csi::node_server::Node::node_unstage_volume(&service, Request::new(request))
            .await
            .expect("second call should also succeed");
    }

    #[tokio::test]
    async fn node_unstage_volume_requires_volume_id() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = csi::NodeUnstageVolumeRequest {
            volume_id: String::new(),
            staging_target_path: "/staging".to_string(),
        };

        let err = csi::node_server::Node::node_unstage_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn node_publish_volume_requires_staging_target_path() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = publish_request("test-volume", "", "/target");

        let err = csi::node_server::Node::node_publish_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn node_publish_volume_requires_volume_id() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = publish_request("", "/staging", "/target");

        let err = csi::node_server::Node::node_publish_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn node_unpublish_volume_is_idempotent() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = csi::NodeUnpublishVolumeRequest {
            volume_id: "nonexistent-volume".to_string(),
            target_path: "/nonexistent/path".to_string(),
        };

        csi::node_server::Node::node_unpublish_volume(&service, Request::new(request.clone()))
            .await
            .expect("first call should succeed");

        csi::node_server::Node::node_unpublish_volume(&service, Request::new(request))
            .await
            .expect("second call should also succeed");
    }

    #[tokio::test]
    async fn node_unpublish_volume_requires_volume_id() {
        let fixture = TestFixture::new();
        let service = fixture.node_service();
        let request = csi::NodeUnpublishVolumeRequest {
            volume_id: String::new(),
            target_path: "/target".to_string(),
        };

        let err = csi::node_server::Node::node_unpublish_volume(&service, Request::new(request))
            .await
            .expect_err("should fail");

        assert_eq!(err.code(), Code::InvalidArgument);
    }
}

//! CSI Identity service implementation.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::csi;
use crate::driver::DriverState;

pub struct IdentityService {
    state: Arc<DriverState>,
}

impl IdentityService {
    pub fn new(state: Arc<DriverState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl csi::identity_server::Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<csi::GetPluginInfoRequest>,
    ) -> Result<Response<csi::GetPluginInfoResponse>, Status> {
        Ok(Response::new(csi::GetPluginInfoResponse {
            name: self.state.config.name.clone(),
            vendor_version: self.state.config.version.clone(),
            manifest: Default::default(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<csi::GetPluginCapabilitiesRequest>,
    ) -> Result<Response<csi::GetPluginCapabilitiesResponse>, Status> {
        Ok(Response::new(csi::GetPluginCapabilitiesResponse {
            capabilities: vec![csi::PluginCapability {
                r#type: Some(csi::plugin_capability::Type::Service(
                    csi::plugin_capability::Service {
                        r#type: csi::plugin_capability::service::Type::ControllerService as i32,
                    },
                )),
            }],
        }))
    }

    async fn probe(
        &self,
        _request: Request<csi::ProbeRequest>,
    ) -> Result<Response<csi::ProbeResponse>, Status> {
        Ok(Response::new(csi::ProbeResponse { ready: Some(true) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csi;
    use crate::test_util::TestFixture;

    #[tokio::test]
    async fn get_plugin_info_returns_valid_name_and_version() {
        let fixture = TestFixture::new();
        let service = fixture.identity_service();

        let response = csi::identity_server::Identity::get_plugin_info(
            &service,
            Request::new(csi::GetPluginInfoRequest {}),
        )
        .await
        .expect("GetPluginInfo should succeed");

        let info = response.into_inner();

        assert!(!info.name.is_empty());
        assert!(info.name.len() <= 63);
        assert!(info.name.contains('.'));
        assert!(!info.vendor_version.is_empty());
    }

    #[tokio::test]
    async fn get_plugin_capabilities_returns_controller_service() {
        let fixture = TestFixture::new();
        let service = fixture.identity_service();

        let response = csi::identity_server::Identity::get_plugin_capabilities(
            &service,
            Request::new(csi::GetPluginCapabilitiesRequest {}),
        )
        .await
        .expect("GetPluginCapabilities should succeed");

        let caps = response.into_inner().capabilities;
        let has_controller = caps.iter().any(|cap| {
            matches!(
                &cap.r#type,
                Some(csi::plugin_capability::Type::Service(svc))
                    if svc.r#type == csi::plugin_capability::service::Type::ControllerService as i32
            )
        });
        assert!(has_controller);
    }
}

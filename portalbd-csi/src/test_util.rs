//! Shared test utilities for CSI driver tests.

use std::sync::Arc;
use tempfile::TempDir;

use crate::controller::ControllerService;
use crate::driver::{Config, DRIVER_NAME, DRIVER_VERSION, DriverState};
use crate::identity::IdentityService;
use crate::node::NodeService;

/// Test fixture providing isolated driver state for each test.
pub struct TestFixture {
    pub state: Arc<DriverState>,
    _temp_dir: TempDir,
}

impl TestFixture {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("failed to create temp dir");
        let config = Config {
            name: DRIVER_NAME.to_string(),
            version: DRIVER_VERSION.to_string(),
            node_id: "test-node".to_string(),
            endpoint: "unix:///tmp/test.sock".to_string(),
            data_dir: temp_dir.path().to_path_buf(),
        };
        let state = Arc::new(DriverState::new(config));
        Self {
            state,
            _temp_dir: temp_dir,
        }
    }

    pub fn identity_service(&self) -> IdentityService {
        IdentityService::new(Arc::clone(&self.state))
    }

    pub fn controller_service(&self) -> ControllerService {
        ControllerService::new(Arc::clone(&self.state))
    }

    pub fn node_service(&self) -> NodeService {
        NodeService::new(Arc::clone(&self.state))
    }
}

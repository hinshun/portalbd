//! CSI driver implementation.
//!
//! This module contains the core driver components:
//! - `Config`: Driver configuration
//! - `Driver`: The main CSI driver that runs gRPC servers
//! - `DriverState`: Shared state managing volumes (in `state` submodule)
//! - `VolumeMetadata`: Persisted volume information (in `state` submodule)

mod state;

pub use state::{DriverState, VolumeMetadata};

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::UnixListener;
use tonic::transport::Server;
use tracing::{info, warn};

use crate::controller::ControllerService;
use crate::csi::{
    FILE_DESCRIPTOR_SET, controller_server::ControllerServer, identity_server::IdentityServer,
    node_server::NodeServer,
};
use crate::error::{Error, Result};
use crate::identity::IdentityService;
use crate::node::NodeService;

/// Execute a cleanup operation, logging any errors without failing.
///
/// Use this for cleanup paths where we want to attempt recovery but
/// cannot let cleanup failures mask the original error.
fn log_cleanup_error<F, E>(operation: &str, f: F)
where
    F: FnOnce() -> std::result::Result<(), E>,
    E: std::fmt::Display,
{
    if let Err(e) = f() {
        warn!(operation, error = %e, "cleanup failed");
    }
}

pub const DRIVER_NAME: &str = "portalbd.csi.kyro.dev";
pub const DRIVER_VERSION: &str = "0.1.0";

/// Minimum volume size: 1 GiB.
pub const MIN_VOLUME_SIZE: i64 = 1 << 30;
/// Default volume size: 10 GiB.
pub const DEFAULT_VOLUME_SIZE: i64 = 10 << 30;
/// Bytes per GiB.
pub const BYTES_PER_GIB: i64 = 1 << 30;

/// Base port for NBD server allocation.
const NBD_BASE_PORT: u16 = 10809;
/// Number of ports in the NBD port range.
const NBD_PORT_RANGE: u16 = 1000;
/// Maximum number of NBD devices to scan.
pub const NBD_DEVICES_MAX: u32 = 16;
/// Maximum number of mount options to prevent argv overflow.
const MOUNT_OPTIONS_MAX: usize = 32;
/// Maximum number of volumes that can be created.
///
/// This prevents unbounded growth of the volume metadata store.
const VOLUMES_MAX: usize = 1024;

// Compile-time assertions to verify constant relationships and bounds.
const _: () = {
    // Ensure port range doesn't overflow u16
    assert!(NBD_BASE_PORT as u32 + NBD_PORT_RANGE as u32 <= u16::MAX as u32);

    // Ensure minimum volume size is at least 1 GiB
    assert!(MIN_VOLUME_SIZE == 1 << 30);

    // Ensure default volume size is at least minimum size
    assert!(DEFAULT_VOLUME_SIZE >= MIN_VOLUME_SIZE);

    // Ensure volume limit is reasonable
    assert!(VOLUMES_MAX > 0);
    assert!(VOLUMES_MAX <= 10_000);

    // Ensure NBD device limit is at least 1
    assert!(NBD_DEVICES_MAX >= 1);
};

/// Driver configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub name: String,
    pub version: String,
    pub node_id: String,
    pub endpoint: String,
    pub data_dir: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            name: DRIVER_NAME.to_string(),
            version: DRIVER_VERSION.to_string(),
            node_id: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            endpoint: "unix:///var/run/csi/csi.sock".to_string(),
            data_dir: PathBuf::from("/var/lib/portalbd-csi"),
        }
    }
}

/// The CSI driver.
pub struct Driver {
    state: Arc<DriverState>,
}

impl Driver {
    pub fn new(config: Config) -> Result<Self> {
        fs::create_dir_all(&config.data_dir)?;
        Ok(Self {
            state: Arc::new(DriverState::new(config)),
        })
    }

    /// Start existing volumes on driver startup.
    async fn start_existing_volumes(&self) -> Result<()> {
        let volumes_dir = self.state.config.data_dir.join("volumes");
        if !volumes_dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&volumes_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }

            let volume_id = entry.file_name().to_string_lossy().to_string();
            let metadata_path = self.state.volume_dir(&volume_id).join("metadata.json");

            if !metadata_path.exists() {
                continue;
            }

            match self.state.start_volume(&volume_id).await {
                Ok(()) => info!(volume_id, "started volume"),
                Err(e) => warn!(volume_id, error = %e, "failed to start volume"),
            }
        }

        Ok(())
    }

    /// Run the CSI driver.
    pub async fn run(&self) -> Result<()> {
        if let Err(e) = self.start_existing_volumes().await {
            warn!(error = %e, "failed to start some existing volumes");
        }

        let endpoint = &self.state.config.endpoint;
        info!(
            name = %self.state.config.name,
            version = %self.state.config.version,
            endpoint,
            "starting CSI driver"
        );

        if let Some(path) = endpoint.strip_prefix("unix://") {
            self.run_unix(path).await
        } else if let Some(addr) = endpoint.strip_prefix("tcp://") {
            let addr: SocketAddr = addr.parse().map_err(|_| Error::InvalidEndpoint {
                endpoint: endpoint.clone(),
            })?;
            self.run_tcp(addr).await
        } else {
            Err(Error::InvalidEndpoint {
                endpoint: endpoint.clone(),
            })
        }
    }

    async fn run_unix(&self, path: &str) -> Result<()> {
        let _ = fs::remove_file(path);
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }

        let listener = UnixListener::bind(path)?;
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);

        self.serve_grpc(incoming).await
    }

    async fn run_tcp(&self, addr: SocketAddr) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        self.serve_grpc(incoming).await
    }

    async fn serve_grpc<S, IO, E>(&self, incoming: S) -> Result<()>
    where
        S: tokio_stream::Stream<Item = std::result::Result<IO, E>> + Send + 'static,
        IO: tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + tonic::transport::server::Connected
            + Send
            + Unpin
            + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Server::builder()
            .add_service(reflection)
            .add_service(IdentityServer::new(IdentityService::new(
                self.state.clone(),
            )))
            .add_service(ControllerServer::new(ControllerService::new(
                self.state.clone(),
            )))
            .add_service(NodeServer::new(NodeService::new(self.state.clone())))
            .serve_with_incoming(incoming)
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(())
    }
}

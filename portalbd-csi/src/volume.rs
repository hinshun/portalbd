//! Volume management.
//!
//! Each volume runs its own portalbd Daemon instance.

use std::sync::Arc;

use portalbd::{Daemon, NbdError};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::error::Result;

/// A running volume instance wrapping a portalbd Daemon.
pub struct VolumeInstance {
    daemon: Arc<Daemon>,
    nbd_handle: Option<JoinHandle<std::result::Result<(), NbdError>>>,
}

impl VolumeInstance {
    /// Start a new volume with the given portalbd configuration.
    pub async fn start(config: portalbd::Config) -> Result<Self> {
        debug_assert!(
            config.device.disk_size_gb > 0,
            "disk_size_gb must be positive"
        );

        let nbd_address = config.nbd.address.clone();
        info!(
            nbd_address = %nbd_address,
            storage = config.storage.url.as_deref().unwrap_or("memory://"),
            "VolumeInstance::start: creating daemon"
        );

        let daemon = Daemon::from_config(config).await?;
        info!(nbd_address = %daemon.nbd_address(), "VolumeInstance::start: daemon created, binding listener");

        // Bind the TCP listener - this is the "ready" check
        let listener = TcpListener::bind(&nbd_address)
            .await
            .map_err(crate::error::Error::Io)?;

        info!(nbd_address = %daemon.nbd_address(), "VolumeInstance::start: starting NBD server");

        // Spawn the listen task
        let daemon_clone = daemon.clone();
        let nbd_handle = tokio::spawn(async move { daemon_clone.listen(listener).await });

        let daemon = Arc::new(daemon);

        info!(
            nbd_address = %daemon.nbd_address(),
            size_bytes = daemon.size_bytes(),
            storage = daemon.config().storage.url.as_deref().unwrap_or("memory://"),
            "VolumeInstance::start: volume started successfully"
        );

        Ok(Self {
            daemon,
            nbd_handle: Some(nbd_handle),
        })
    }

    /// Stop the volume, closing the NBD server.
    ///
    /// This method is defensive: it attempts to abort the NBD handle and flush
    /// pending writes, but logs errors rather than failing if individual steps
    /// fail. This ensures cleanup proceeds even if one step has issues.
    pub async fn stop(mut self) -> Result<()> {
        let nbd_address = self.daemon.nbd_address().to_string();

        // Abort the NBD server handle first
        if let Some(handle) = self.nbd_handle.take() {
            handle.abort();
            // Wait for abort to complete, ignoring any errors (task was cancelled)
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(nbd_address = %nbd_address, error = %e, "NBD server exited with error");
                }
                Err(_) => {
                    // JoinError from abort - expected, ignore
                }
            }
        }

        // Flush pending writes - this is the critical step that must succeed
        if let Err(e) = self.daemon.flush().await {
            warn!(nbd_address = %nbd_address, error = %e, "failed to flush during stop");
            return Err(e.into());
        }

        info!(nbd_address = %nbd_address, "volume stopped");
        Ok(())
    }

    /// Get the NBD address for this volume.
    pub fn nbd_address(&self) -> &str {
        self.daemon.nbd_address()
    }

    /// Get the volume size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.daemon.size_bytes()
    }

    /// Get the underlying daemon configuration.
    pub fn config(&self) -> &portalbd::Config {
        self.daemon.config()
    }

    /// Check if the NBD server is still running.
    pub fn is_running(&self) -> bool {
        self.nbd_handle
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }
}

impl Drop for VolumeInstance {
    fn drop(&mut self) {
        if let Some(handle) = self.nbd_handle.take() {
            handle.abort();
        }
    }
}

//! Driver state management.
//!
//! This module contains DriverState which manages:
//! - Volume lifecycle (create, delete, list)
//! - Volume metadata persistence
//! - NBD connection management
//! - Mount operations

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use nbd::NbdDevice;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::error::{Error, Result};
use crate::types::NbdAddress;
use crate::volume::VolumeInstance;

use super::{
    BYTES_PER_GIB, Config, MOUNT_OPTIONS_MAX, NBD_BASE_PORT, NBD_DEVICES_MAX, NBD_PORT_RANGE,
    VOLUMES_MAX, log_cleanup_error,
};

/// Volume metadata persisted to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMetadata {
    pub volume_id: String,
    pub volume_name: String,
    pub capacity_bytes: i64,
    pub nbd_address: NbdAddress,
    /// Storage URL for the volume data (e.g., "file:///path/to/data" or "s3://bucket/prefix").
    pub storage_url: String,
    #[serde(default)]
    pub parameters: HashMap<String, String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl VolumeMetadata {
    /// Validate that the metadata is consistent and usable.
    ///
    /// This is called after deserialization to catch corrupted or manually
    /// edited metadata files that might have invalid values.
    pub fn validate(&self) -> Result<()> {
        if self.volume_id.is_empty() {
            return Err(Error::InvalidMetadata("volume_id is empty".to_string()));
        }
        if self.volume_name.is_empty() {
            return Err(Error::InvalidMetadata("volume_name is empty".to_string()));
        }
        if self.capacity_bytes <= 0 {
            return Err(Error::InvalidMetadata(format!(
                "capacity_bytes must be positive, got {}",
                self.capacity_bytes
            )));
        }
        if self.storage_url.is_empty() {
            return Err(Error::InvalidMetadata("storage_url is empty".to_string()));
        }
        Ok(())
    }
}

/// Shared driver state.
pub struct DriverState {
    pub config: Config,
    volumes: RwLock<HashMap<String, VolumeInstance>>,
    /// Connected NBD devices, keyed by device path (e.g., "/dev/nbd0").
    /// Uses std::sync::Mutex because NbdDevice is not Send.
    nbd_devices: Mutex<HashMap<String, NbdDevice>>,
    /// Allocated NBD ports to detect collisions.
    /// Maps port number to volume_id that owns it.
    allocated_ports: Mutex<HashMap<u16, String>>,
}

impl DriverState {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            volumes: RwLock::new(HashMap::new()),
            nbd_devices: Mutex::new(HashMap::new()),
            allocated_ports: Mutex::new(HashMap::new()),
        }
    }

    pub fn volume_dir(&self, volume_id: &str) -> PathBuf {
        self.config.data_dir.join("volumes").join(volume_id)
    }

    /// Allocate an NBD port for a volume.
    ///
    /// Uses hash-based allocation with collision detection. If the hash-based
    /// port is already in use, linearly probes for an available port.
    ///
    /// Returns the allocated port and registers it in the allocated_ports map.
    pub fn allocate_nbd_port(&self, volume_id: &str) -> Result<u16> {
        debug_assert!(!volume_id.is_empty(), "volume_id must not be empty");

        let mut ports = self
            .allocated_ports
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        // Check if this volume already has an allocated port
        for (&port, id) in ports.iter() {
            if id == volume_id {
                return Ok(port);
            }
        }

        // Compute hash-based starting port
        let mut hash: i32 = 0;
        for c in volume_id.chars() {
            hash = hash.wrapping_mul(31).wrapping_add(c as i32);
        }
        let base_offset = (hash.unsigned_abs() % u32::from(NBD_PORT_RANGE)) as u16;

        // Linear probe for available port
        for i in 0..NBD_PORT_RANGE {
            let port = NBD_BASE_PORT + ((base_offset + i) % NBD_PORT_RANGE);
            if let Entry::Vacant(e) = ports.entry(port) {
                e.insert(volume_id.to_string());
                return Ok(port);
            }
        }

        // All ports exhausted - this shouldn't happen with VOLUMES_MAX < NBD_PORT_RANGE
        Err(Error::VolumeLimitExceeded {
            count: ports.len(),
            max: NBD_PORT_RANGE as usize,
        })
    }

    /// Release a previously allocated port.
    pub fn release_nbd_port(&self, volume_id: &str) {
        let mut ports = self
            .allocated_ports
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        ports.retain(|_, id| id != volume_id);
    }

    /// Start volume if not already running.
    pub async fn start_volume(&self, volume_id: &str) -> Result<()> {
        // Check if already running
        {
            let volumes = self.volumes.read().await;
            if let Some(instance) = volumes.get(volume_id)
                && instance.is_running()
            {
                return Ok(());
            }
        }

        let meta = self.load_volume_metadata(volume_id)?;
        let config = self.build_portalbd_config(&meta);
        let instance = VolumeInstance::start(config).await?;

        self.volumes
            .write()
            .await
            .insert(volume_id.to_string(), instance);
        Ok(())
    }

    /// Build a portalbd::Config from volume metadata.
    fn build_portalbd_config(&self, meta: &VolumeMetadata) -> portalbd::Config {
        portalbd::Config {
            device: portalbd::DeviceConfig {
                disk_size_gb: (meta.capacity_bytes / BYTES_PER_GIB) as u64,
            },
            storage: portalbd::StorageConfig {
                url: Some(meta.storage_url.clone()),
            },
            nbd: portalbd::NbdConfig {
                address: meta.nbd_address.as_str().to_string(),
            },
            socket: self.volume_dir(&meta.volume_id).join("portalbd.sock"),
            ..Default::default()
        }
    }

    /// Check if volume directory exists on disk.
    pub fn volume_exists(&self, volume_id: &str) -> bool {
        self.volume_dir(volume_id).exists()
    }

    /// Stop and remove volume instance.
    pub async fn stop_volume(&self, volume_id: &str) -> Result<()> {
        if let Some(instance) = self.volumes.write().await.remove(volume_id) {
            instance.stop().await?;
        }
        Ok(())
    }

    /// Find volume ID by name.
    pub fn find_volume_by_name(&self, name: &str) -> Result<Option<String>> {
        let volumes_dir = self.config.data_dir.join("volumes");
        if !volumes_dir.exists() {
            return Ok(None);
        }

        for entry in fs::read_dir(&volumes_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let volume_id = entry.file_name().to_string_lossy().to_string();
            if let Ok(meta) = self.load_volume_metadata(&volume_id)
                && meta.volume_name == name
            {
                return Ok(Some(volume_id));
            }
        }
        Ok(None)
    }

    pub fn save_volume_metadata(&self, volume_id: &str, meta: &VolumeMetadata) -> Result<()> {
        let path = self.volume_dir(volume_id).join("metadata.json");
        let data = serde_json::to_string_pretty(meta)?;
        fs::write(path, data)?;
        Ok(())
    }

    pub fn load_volume_metadata(&self, volume_id: &str) -> Result<VolumeMetadata> {
        let path = self.volume_dir(volume_id).join("metadata.json");
        let data = fs::read_to_string(path)?;
        let meta: VolumeMetadata = serde_json::from_str(&data)?;
        meta.validate()?;
        Ok(meta)
    }

    /// Create a new volume with the given parameters.
    pub async fn create_volume(
        &self,
        name: &str,
        capacity_bytes: i64,
        parameters: HashMap<String, String>,
    ) -> Result<VolumeMetadata> {
        // Check volume limit before creating
        let current_count = self.list_volumes()?.len();
        if current_count >= VOLUMES_MAX {
            return Err(Error::VolumeLimitExceeded {
                count: current_count,
                max: VOLUMES_MAX,
            });
        }

        // Idempotency: return existing volume if name matches
        if let Some(volume_id) = self.find_volume_by_name(name)? {
            let meta = self.load_volume_metadata(&volume_id)?;
            if meta.capacity_bytes < capacity_bytes {
                return Err(Error::VolumeAlreadyExists(name.to_string()));
            }
            return Ok(meta);
        }

        let capacity_gb = std::cmp::max(1, capacity_bytes / BYTES_PER_GIB);
        let volume_id = uuid::Uuid::new_v4().to_string();
        let volume_dir = self.volume_dir(&volume_id);

        fs::create_dir_all(&volume_dir)?;

        let nbd_port = self.allocate_nbd_port(&volume_id)?;
        let nbd_address = NbdAddress::from_parts("127.0.0.1", nbd_port);

        let storage_url = parameters
            .get("storageUrl")
            .cloned()
            .unwrap_or_else(|| format!("file://{}/data", volume_dir.display()));

        let meta = VolumeMetadata {
            volume_id: volume_id.clone(),
            volume_name: name.to_string(),
            capacity_bytes: capacity_gb * BYTES_PER_GIB,
            nbd_address,
            storage_url,
            parameters,
            created_at: chrono::Utc::now(),
        };

        if let Err(e) = self.save_volume_metadata(&volume_id, &meta) {
            self.cleanup_volume(&volume_id).await;
            return Err(e);
        }

        if let Err(e) = self.start_volume(&volume_id).await {
            self.cleanup_volume(&volume_id).await;
            return Err(e);
        }

        info!(volume_id, name, capacity_gb, "volume created");
        Ok(meta)
    }

    /// Delete a volume by ID.
    pub async fn delete_volume(&self, volume_id: &str) -> Result<()> {
        if !self.volume_dir(volume_id).exists() {
            return Ok(());
        }

        self.cleanup_volume(volume_id).await;
        info!(volume_id, "volume deleted");
        Ok(())
    }

    /// Clean up all resources for a volume.
    ///
    /// This is the single place where port release happens, called by both
    /// delete_volume and create_volume error paths.
    async fn cleanup_volume(&self, volume_id: &str) {
        // Stop running instance if any
        if let Err(e) = self.stop_volume(volume_id).await {
            warn!(volume_id, error = %e, "failed to stop volume during cleanup");
        }

        // Release allocated port
        self.release_nbd_port(volume_id);

        // Remove volume directory
        let volume_dir = self.volume_dir(volume_id);
        if volume_dir.exists() {
            log_cleanup_error("remove volume dir", || fs::remove_dir_all(&volume_dir));
        }
    }

    /// List all volumes.
    pub fn list_volumes(&self) -> Result<Vec<VolumeMetadata>> {
        let volumes_dir = self.config.data_dir.join("volumes");
        if !volumes_dir.exists() {
            return Ok(vec![]);
        }

        let mut volumes = Vec::new();
        for entry in fs::read_dir(&volumes_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let volume_id = entry.file_name().to_string_lossy().to_string();
            if let Ok(meta) = self.load_volume_metadata(&volume_id) {
                volumes.push(meta);
            }
        }
        Ok(volumes)
    }

    /// Get volume info by ID.
    pub fn get_volume_info(&self, volume_id: &str) -> Result<VolumeMetadata> {
        if !self.volume_exists(volume_id) {
            return Err(Error::VolumeNotFound(volume_id.to_string()));
        }
        self.load_volume_metadata(volume_id)
    }

    // === Node operations ===

    /// Stage a volume: connect NBD, optionally format and mount.
    pub async fn stage_volume(
        &self,
        volume_id: &str,
        staging_path: &str,
        fs_type: Option<&str>,
        mount_flags: &[String],
    ) -> Result<()> {
        let device_file = format!("{staging_path}/device");

        // Check if already staged
        if Path::new(&device_file).exists()
            && let Ok(data) = fs::read_to_string(&device_file)
            && !data.is_empty()
        {
            return Ok(());
        }

        let meta = self.load_volume_metadata(volume_id)?;
        self.start_volume(volume_id).await?;

        let nbd_device = self.find_available_nbd()?;
        self.connect_nbd(&nbd_device, &meta.nbd_address).await?;

        // Create staging directory and record device
        if let Err(e) = fs::create_dir_all(staging_path) {
            log_cleanup_error("disconnect NBD after staging dir creation failure", || {
                self.disconnect_nbd(&nbd_device)
            });
            return Err(e.into());
        }

        if let Err(e) = fs::write(&device_file, &nbd_device) {
            log_cleanup_error("disconnect NBD after device file write failure", || {
                self.disconnect_nbd(&nbd_device)
            });
            return Err(e.into());
        }

        // If filesystem mount requested, format and mount
        if let Some(fs_type) = fs_type {
            let mount_path = format!("{staging_path}/mount");

            if let Err(e) = fs::create_dir_all(&mount_path) {
                log_cleanup_error("disconnect NBD after mount dir creation failure", || {
                    self.disconnect_nbd(&nbd_device)
                });
                return Err(e.into());
            }

            if let Err(e) = self.format_if_needed(&nbd_device, fs_type).await {
                log_cleanup_error("disconnect NBD after format failure", || {
                    self.disconnect_nbd(&nbd_device)
                });
                return Err(e);
            }

            if let Err(e) = self
                .mount(&nbd_device, &mount_path, fs_type, mount_flags)
                .await
            {
                log_cleanup_error("disconnect NBD after mount failure", || {
                    self.disconnect_nbd(&nbd_device)
                });
                return Err(e);
            }
        }

        info!(volume_id, device = %nbd_device, "volume staged");
        Ok(())
    }

    /// Unstage a volume: unmount and disconnect NBD.
    ///
    /// This is async because unmount can block waiting for I/O to complete,
    /// and we need to keep the tokio runtime free so portalbd can process
    /// the NBD requests that the unmount generates.
    pub async fn unstage_volume(&self, volume_id: &str, staging_path: &str) -> Result<()> {
        let device_file = format!("{staging_path}/device");

        let nbd_device = match fs::read_to_string(&device_file) {
            Ok(data) => data.trim().to_string(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        let mount_path = format!("{staging_path}/mount");
        if self.is_mount_point(&mount_path) {
            self.unmount_async(&mount_path).await?;
        }

        log_cleanup_error("disconnect NBD during unstage", || {
            self.disconnect_nbd(&nbd_device)
        });

        log_cleanup_error("remove staging dir during unstage", || {
            fs::remove_dir_all(staging_path)
        });

        info!(volume_id, "volume unstaged");
        Ok(())
    }

    /// Publish a volume: bind mount from staging to target.
    pub async fn publish_volume(
        &self,
        volume_id: &str,
        staging_path: &str,
        target_path: &str,
        readonly: bool,
        is_block: bool,
    ) -> Result<()> {
        let device_file = format!("{staging_path}/device");
        let nbd_device = fs::read_to_string(&device_file)
            .map_err(|_| Error::VolumeNotStaged(volume_id.to_string()))?
            .trim()
            .to_string();

        if self.is_mount_point(target_path) {
            return Ok(());
        }

        if is_block {
            if let Some(parent) = Path::new(target_path).parent() {
                fs::create_dir_all(parent)?;
            }
            // Remove any existing file at target path (may not exist, so ignore errors)
            log_cleanup_error("remove existing target file before block publish", || {
                fs::remove_file(target_path)
            });
            fs::File::create(target_path)?;

            if let Err(e) = self
                .mount(&nbd_device, target_path, "", &["bind".to_string()])
                .await
            {
                log_cleanup_error("remove target file after block mount failure", || {
                    fs::remove_file(target_path)
                });
                return Err(e);
            }
        } else {
            let staging_mount = format!("{staging_path}/mount");
            fs::create_dir_all(target_path)?;

            let mut options = vec!["bind".to_string()];
            if readonly {
                options.push("ro".to_string());
            }

            self.mount(&staging_mount, target_path, "", &options)
                .await?;
        }

        info!(volume_id, target = target_path, "volume published");
        Ok(())
    }

    /// Unpublish a volume: unmount from target.
    pub async fn unpublish_volume(&self, volume_id: &str, target_path: &str) -> Result<()> {
        if !self.is_mount_point(target_path) {
            log_cleanup_error("remove target dir (not mounted)", || {
                fs::remove_dir_all(target_path)
            });
            return Ok(());
        }

        self.unmount_async(target_path).await?;
        log_cleanup_error("remove target dir after unmount", || {
            fs::remove_dir_all(target_path)
        });

        info!(volume_id, target = target_path, "volume unpublished");
        Ok(())
    }

    // === NBD and mount helpers ===

    /// Find an available NBD device by scanning /dev/nbd*.
    ///
    /// An NBD device is considered available if its size (in /sys/block/nbdN/size)
    /// is zero, indicating no active connection.
    fn find_available_nbd(&self) -> Result<String> {
        for i in 0..NBD_DEVICES_MAX {
            let device = format!("/dev/nbd{i}");
            if !Path::new(&device).exists() {
                continue;
            }
            let size_file = format!("/sys/block/nbd{i}/size");
            if let Ok(data) = fs::read_to_string(&size_file)
                && data.trim() == "0"
            {
                return Ok(device);
            }
        }
        Err(Error::NoAvailableNbdDevice)
    }

    /// Connect an NBD device to a remote server.
    ///
    /// Uses the native Rust NBD client instead of shelling out to nbd-client.
    async fn connect_nbd(&self, device: &str, address: &NbdAddress) -> Result<()> {
        debug_assert!(!device.is_empty(), "device must not be empty");

        let host = address.host();
        let port: u16 = address.port_str().parse().map_err(|_| Error::NbdConnect {
            device: device.to_string(),
            address: address.as_str().to_string(),
            source: std::io::Error::other("invalid port"),
        })?;

        let nbd_device = NbdDevice::connect(device, host, port, "portalbd")
            .await
            .map_err(|e| Error::NbdConnect {
                device: device.to_string(),
                address: address.as_str().to_string(),
                source: std::io::Error::other(e.to_string()),
            })?;

        // Store the device handle to keep the connection alive
        self.nbd_devices
            .lock()
            .unwrap()
            .insert(device.to_string(), nbd_device);

        Ok(())
    }

    /// Disconnect an NBD device from its server.
    fn disconnect_nbd(&self, device: &str) -> Result<()> {
        debug_assert!(!device.is_empty(), "device must not be empty");

        let nbd_device = self.nbd_devices.lock().unwrap().remove(device);

        if let Some(nbd_device) = nbd_device {
            if let Err(e) = nbd_device.disconnect() {
                warn!(device, error = %e, "NBD disconnect failed");
                return Err(Error::NbdDisconnect {
                    device: device.to_string(),
                    source: std::io::Error::other(e.to_string()),
                });
            }
        } else {
            // Device not in our map - try direct disconnect (cleanup scenario)
            if let Err(e) = nbd::disconnect_device(device) {
                warn!(device, error = %e, "NBD direct disconnect failed");
            }
        }

        Ok(())
    }

    async fn format_if_needed(&self, device: &str, fs_type: &str) -> Result<()> {
        let device = device.to_string();
        let fs_type = fs_type.to_string();

        // Run filesystem operations in blocking task to avoid blocking the async runtime.
        // This is critical because the NBD server needs to respond to reads from blkid/mkfs.
        tokio::task::spawn_blocking(move || {
            let blkid = std::process::Command::new("blkid")
                .arg(&device)
                .output()
                .map_err(Error::Io)?;
            if blkid.status.success() {
                return Ok(()); // Already formatted
            }

            let output = match fs_type.as_str() {
                "ext4" => std::process::Command::new("mkfs.ext4")
                    .args(["-F", &device])
                    .output()
                    .map_err(Error::Io)?,
                "xfs" => std::process::Command::new("mkfs.xfs")
                    .args(["-f", &device])
                    .output()
                    .map_err(Error::Io)?,
                _ => std::process::Command::new("mkfs")
                    .args(["-t", &fs_type, &device])
                    .output()
                    .map_err(Error::Io)?,
            };

            if !output.status.success() {
                return Err(Error::Format {
                    device,
                    fs_type,
                    message: String::from_utf8_lossy(&output.stderr).trim().to_string(),
                });
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(format!("task join error: {e}"))))?
    }

    /// Mount a filesystem or bind mount.
    ///
    /// # Errors
    /// Returns an error if there are too many mount options or the mount command fails.
    async fn mount(
        &self,
        source: &str,
        target: &str,
        fs_type: &str,
        options: &[String],
    ) -> Result<()> {
        debug_assert!(!source.is_empty(), "source must not be empty");
        debug_assert!(!target.is_empty(), "target must not be empty");

        // Guard against argv overflow from excessive mount options
        if options.len() > MOUNT_OPTIONS_MAX {
            return Err(Error::TooManyMountOptions {
                count: options.len(),
                max: MOUNT_OPTIONS_MAX,
            });
        }

        let source = source.to_string();
        let target = target.to_string();
        let fs_type = fs_type.to_string();
        let options = options.to_vec();

        // Run mount in blocking task to avoid blocking the async runtime.
        tokio::task::spawn_blocking(move || {
            let mut cmd = std::process::Command::new("mount");
            if !fs_type.is_empty() {
                cmd.args(["-t", &fs_type]);
            }
            for opt in &options {
                cmd.args(["-o", opt]);
            }
            cmd.args([&source, &target]);

            let output = cmd.output().map_err(|e| Error::Mount {
                mount_source: source.clone(),
                target: target.clone(),
                source: e,
            })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                return Err(Error::Mount {
                    mount_source: source,
                    target,
                    source: std::io::Error::other(stderr),
                });
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(format!("task join error: {e}"))))?
    }

    /// Unmount a filesystem asynchronously.
    ///
    /// This runs umount in a blocking task so it doesn't block the tokio runtime.
    /// This is critical because umount can block waiting for I/O to complete,
    /// and portalbd needs the runtime to process those I/O requests.
    async fn unmount_async(&self, target: &str) -> Result<()> {
        debug_assert!(!target.is_empty(), "target must not be empty");

        let target = target.to_string();

        tokio::task::spawn_blocking(move || {
            let output = std::process::Command::new("umount")
                .arg(&target)
                .output()
                .map_err(|e| Error::Unmount {
                    target: target.clone(),
                    source: e,
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                return Err(Error::Unmount {
                    target,
                    source: std::io::Error::other(stderr),
                });
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Io(std::io::Error::other(format!("task join error: {e}"))))?
    }

    fn is_mount_point(&self, path: &str) -> bool {
        std::process::Command::new("mountpoint")
            .args(["-q", path])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DRIVER_NAME, DRIVER_VERSION, NBD_BASE_PORT, NBD_PORT_RANGE};
    use tempfile::TempDir;

    fn test_config(temp_dir: &TempDir) -> Config {
        Config {
            name: DRIVER_NAME.to_string(),
            version: DRIVER_VERSION.to_string(),
            node_id: "test-node".to_string(),
            endpoint: "unix:///tmp/test.sock".to_string(),
            data_dir: temp_dir.path().to_path_buf(),
        }
    }

    /// Test that port allocation is deterministic for the same volume ID.
    #[test]
    fn allocate_nbd_port_is_deterministic() {
        let temp_dir = TempDir::new().unwrap();
        let state = DriverState::new(test_config(&temp_dir));

        let port1 = state.allocate_nbd_port("test-volume-id").unwrap();
        let port2 = state.allocate_nbd_port("test-volume-id").unwrap();

        assert_eq!(port1, port2, "same volume ID should get same port");
    }

    /// Test that different volume IDs get different ports.
    #[test]
    fn allocate_nbd_port_varies_by_volume_id() {
        let temp_dir = TempDir::new().unwrap();
        let state = DriverState::new(test_config(&temp_dir));

        let port1 = state.allocate_nbd_port("volume-1").unwrap();
        let port2 = state.allocate_nbd_port("volume-2").unwrap();

        // Verify ports are in valid range
        assert!((NBD_BASE_PORT..NBD_BASE_PORT + NBD_PORT_RANGE).contains(&port1));
        assert!((NBD_BASE_PORT..NBD_BASE_PORT + NBD_PORT_RANGE).contains(&port2));

        // With collision detection, different volumes get different ports
        assert_ne!(
            port1, port2,
            "different volume IDs should get different ports"
        );
    }

    /// Test that port collision detection works.
    #[test]
    fn allocate_nbd_port_handles_collision() {
        let temp_dir = TempDir::new().unwrap();
        let state = DriverState::new(test_config(&temp_dir));

        // Allocate many ports - some will have hash collisions
        let mut ports = std::collections::HashSet::new();
        for i in 0..100 {
            let port = state.allocate_nbd_port(&format!("volume-{}", i)).unwrap();
            assert!(ports.insert(port), "port {} was allocated twice", port);
        }
    }

    /// Test that releasing a port allows reuse.
    #[test]
    fn release_nbd_port_allows_reuse() {
        let temp_dir = TempDir::new().unwrap();
        let state = DriverState::new(test_config(&temp_dir));

        let port1 = state.allocate_nbd_port("volume-1").unwrap();
        state.release_nbd_port("volume-1");

        // Allocating for the same volume ID should get the same port
        let port2 = state.allocate_nbd_port("volume-1").unwrap();
        assert_eq!(
            port1, port2,
            "should get same hash-based port after release"
        );
    }

    /// Test get_volume_info returns error for nonexistent volume.
    #[test]
    fn get_volume_info_error_for_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let state = DriverState::new(test_config(&temp_dir));

        let result = state.get_volume_info("nonexistent-id");
        assert!(result.is_err());
    }
}

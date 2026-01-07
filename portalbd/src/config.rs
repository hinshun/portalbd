//! Configuration for portalbd.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut, S3CopyIfNotExists};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use slatedb::config::{CompressionCodec, SstBlockSize};
use url::Url;

use crate::error::ConfigError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub device: DeviceConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub nbd: NbdConfig,
    #[serde(default = "default_socket_path")]
    pub socket: PathBuf,
    #[serde(default)]
    pub lsm: LsmConfig,
    #[serde(default)]
    pub record: Option<RecordConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            device: DeviceConfig::default(),
            storage: StorageConfig::default(),
            nbd: NbdConfig::default(),
            socket: default_socket_path(),
            lsm: LsmConfig::default(),
            record: None,
        }
    }
}

/// Configuration for recording NBD operations to a trace file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordConfig {
    /// Path to write the trace file.
    pub path: PathBuf,

    /// Workload name for the trace (e.g., "mkfs.ext4", "pgbench").
    #[serde(default = "default_workload_name")]
    pub workload: String,
}

fn default_workload_name() -> String {
    "unnamed".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DeviceConfig {
    pub disk_size_gb: u64,
}

impl Default for DeviceConfig {
    fn default() -> Self {
        Self { disk_size_gb: 1 }
    }
}

impl DeviceConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.disk_size_gb == 0 {
            return Err(ConfigError::InvalidValue {
                field: "disk_size_gb",
                reason: "must be > 0",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    #[serde(default)]
    pub url: Option<String>,
}

impl StorageConfig {
    pub fn build_object_store(&self) -> Result<(Arc<dyn ObjectStore>, String), ConfigError> {
        let Some(ref url_str) = self.url else {
            return Ok((Arc::new(InMemory::new()), "portalbd".to_string()));
        };

        let url = Url::parse(url_str).map_err(|e| ConfigError::ParseError(e.to_string()))?;

        match url.scheme() {
            "file" => {
                let path = url.path();
                std::fs::create_dir_all(path).map_err(ConfigError::ReadError)?;
                let store = LocalFileSystem::new_with_prefix(path)
                    .map_err(|e| ConfigError::ParseError(e.to_string()))?;
                Ok((Arc::new(store), "portalbd".to_string()))
            }
            "s3" => {
                let bucket = url.host_str().ok_or(ConfigError::InvalidValue {
                    field: "storage.url",
                    reason: "S3 URL must have bucket",
                })?;
                let path = url.path().trim_start_matches('/');
                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .with_copy_if_not_exists(S3CopyIfNotExists::Header(
                        "If-None-Match".to_string(),
                        "*".to_string(),
                    ))
                    .build()
                    .map_err(|e| ConfigError::ParseError(e.to_string()))?;
                Ok((Arc::new(store), path.to_string()))
            }
            scheme => Err(ConfigError::UnsupportedScheme {
                scheme: scheme.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NbdConfig {
    #[serde(default = "default_nbd_address")]
    pub address: String,
}

impl Default for NbdConfig {
    fn default() -> Self {
        Self {
            address: default_nbd_address(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LsmConfig {
    pub flush_interval_ms: Option<u64>,
    pub l0_sst_size_bytes: Option<u64>,
    pub l0_max_ssts: Option<u32>,
    pub max_unflushed_bytes: Option<u64>,
    pub filter_bits_per_key: Option<u32>,
    pub block_cache_size_bytes: Option<u64>,
    /// Compression codec for SST blocks. Default is Lz4.
    /// Compression reduces storage size and network I/O at the cost of CPU.
    pub compression_codec: Option<CompressionCodec>,
    /// SST block size. Default is 4KiB.
    /// Larger blocks reduce per-block overhead (CRC32, metadata) but increase
    /// read amplification for small random reads.
    pub sst_block_size: Option<SstBlockSize>,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            // Manual flush only - relies on explicit flush() calls for durability.
            // This eliminates timed flushes and improves write batching.
            flush_interval_ms: Some(0),
            // 256 MB L0 SST size - reduces flush frequency, improves batch efficiency.
            l0_sst_size_bytes: Some(268_435_456),
            l0_max_ssts: None,
            max_unflushed_bytes: None,
            filter_bits_per_key: None,
            block_cache_size_bytes: None,
            // LZ4 compression - improves throughput by reducing data size.
            compression_codec: Some(CompressionCodec::Lz4),
            // 64KB blocks - reduces per-block metadata overhead for write-heavy workloads.
            sst_block_size: Some(SstBlockSize::Block64Kib),
        }
    }
}

impl LsmConfig {
    pub fn flush_interval(&self) -> Option<Duration> {
        self.flush_interval_ms.map(Duration::from_millis)
    }
}

fn default_nbd_address() -> String {
    "127.0.0.1:10809".to_string()
}

fn default_socket_path() -> PathBuf {
    PathBuf::from("/run/portalbd/portalbd.sock")
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(ConfigError::ReadError)?;
        let config: Config =
            toml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))?;
        config.device.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_config_rejects_zero_size() {
        let config = DeviceConfig { disk_size_gb: 0 };
        assert!(config.validate().is_err());
    }

    #[test]
    fn device_config_accepts_valid_size() {
        let config = DeviceConfig { disk_size_gb: 100 };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn storage_config_defaults_to_memory() {
        let config = StorageConfig::default();
        let (_, path) = config.build_object_store().unwrap();
        assert_eq!(path, "portalbd");
    }

    #[test]
    fn storage_config_unsupported_scheme() {
        let config = StorageConfig {
            url: Some("ftp://example.com/data".to_string()),
        };
        let result = config.build_object_store();
        assert!(matches!(result, Err(ConfigError::UnsupportedScheme { .. })));
    }

    #[test]
    fn storage_config_invalid_url() {
        let config = StorageConfig {
            url: Some("not a valid url".to_string()),
        };
        let result = config.build_object_store();
        assert!(matches!(result, Err(ConfigError::ParseError(_))));
    }

    #[test]
    fn nbd_config_defaults() {
        let config = NbdConfig::default();
        assert_eq!(config.address, "127.0.0.1:10809");
    }

    #[test]
    fn lsm_config_flush_interval() {
        let config = LsmConfig::default();
        // Default is manual flush only (0ms)
        assert_eq!(config.flush_interval(), Some(Duration::from_millis(0)));

        let config_with_interval = LsmConfig {
            flush_interval_ms: Some(100),
            ..Default::default()
        };
        assert_eq!(
            config_with_interval.flush_interval(),
            Some(Duration::from_millis(100))
        );
    }
}

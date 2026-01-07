//! Control protocol for portalbd <-> portalctl communication over Unix Domain Socket.

use serde::{Deserialize, Serialize};

use crate::types::SnapshotInfo;

/// Request from portalctl to portalbd.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Create a snapshot with the given name.
    SnapshotCreate { name: String },
    /// List all snapshots.
    SnapshotList,
    /// Get snapshot by name.
    SnapshotGet { name: String },
    /// Delete snapshot by name.
    SnapshotDelete { name: String },
    /// Restore to snapshot (sets active checkpoint for CoW reads).
    SnapshotRestore { name: String },
    /// Get daemon status.
    Status,
}

/// Response from portalbd to portalctl.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ok,
    Error(String),
    /// Snapshot info response.
    Snapshot(SnapshotInfo),
    /// Snapshot list response.
    SnapshotList(Vec<SnapshotInfo>),
    /// Status response.
    Status(DaemonStatus),
}

/// Daemon status information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    /// Device size in bytes.
    pub size_bytes: u64,
    /// Block count.
    pub block_count: u64,
    /// Number of snapshots.
    pub snapshot_count: usize,
    /// NBD server address.
    pub nbd_address: String,
}

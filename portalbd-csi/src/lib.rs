//! portalbd-csi: CSI driver for portalbd.
//!
//! This crate implements the Container Storage Interface (CSI) specification
//! to expose portalbd block devices to container orchestrators like Kubernetes.

// Generated protobuf code has doc formatting issues
#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::doc_lazy_continuation)]
// tonic::Status is large by design (176 bytes)
#![allow(clippy::result_large_err)]

pub mod controller;
pub mod driver;
pub mod error;
pub mod identity;
pub mod node;
pub mod types;
pub mod volume;

pub mod csi {
    tonic::include_proto!("csi.v1");

    pub use prost_types::Timestamp;

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("csi_descriptor");
}

pub use driver::{Config, Driver};
pub use error::{Error, Result};
pub use types::NbdAddress;
pub use volume::VolumeInstance;

#[cfg(test)]
pub(crate) mod test_util;

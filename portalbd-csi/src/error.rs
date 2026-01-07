//! Error types for the CSI driver.

use std::io;
use thiserror::Error;
use tonic::Status;

pub type Result<T> = std::result::Result<T, Error>;

/// Extension trait for validating required fields.
///
/// Returns `INVALID_ARGUMENT` status if the field is missing or empty.
pub trait Require<T> {
    fn require(self, name: &str) -> std::result::Result<T, Status>;
}

impl Require<String> for String {
    fn require(self, name: &str) -> std::result::Result<String, Status> {
        if self.is_empty() {
            Err(Status::invalid_argument(format!("{name} is required")))
        } else {
            Ok(self)
        }
    }
}

impl<T> Require<T> for Option<T> {
    fn require(self, name: &str) -> std::result::Result<T, Status> {
        self.ok_or_else(|| Status::invalid_argument(format!("{name} is required")))
    }
}

/// CSI driver errors with structured context.
#[derive(Debug, Error)]
pub enum Error {
    #[error("volume not found: {0}")]
    VolumeNotFound(String),

    #[error("volume already exists with smaller capacity: {0}")]
    VolumeAlreadyExists(String),

    #[error(
        "no available NBD device (scanned {0} devices)",
        crate::driver::NBD_DEVICES_MAX
    )]
    NoAvailableNbdDevice,

    #[error("volume limit exceeded: {count} volumes exist, maximum is {max}")]
    VolumeLimitExceeded { count: usize, max: usize },

    #[error("invalid endpoint: {endpoint}")]
    InvalidEndpoint { endpoint: String },

    #[error("invalid NBD address (expected host:port): {address}")]
    InvalidAddress { address: String },

    #[error("volume not staged: {0}")]
    VolumeNotStaged(String),

    #[error("volume capability access_type is required")]
    MissingCapability,

    #[error(
        "unsupported access mode: only SINGLE_NODE_WRITER and SINGLE_NODE_READER_ONLY are supported"
    )]
    UnsupportedAccessMode,

    #[error("failed to connect NBD device {device} to {address}: {source}")]
    NbdConnect {
        device: String,
        address: String,
        source: io::Error,
    },

    #[error("failed to disconnect NBD device {device}: {source}")]
    NbdDisconnect { device: String, source: io::Error },

    #[error("failed to mount {mount_source} on {target}: {source}")]
    Mount {
        mount_source: String,
        target: String,
        #[source]
        source: io::Error,
    },

    #[error("failed to unmount {target}: {source}")]
    Unmount { target: String, source: io::Error },

    #[error("failed to format {device} as {fs_type}: {message}")]
    Format {
        device: String,
        fs_type: String,
        message: String,
    },

    #[error("too many mount options: {count} exceeds maximum of {max}")]
    TooManyMountOptions { count: usize, max: usize },

    #[error("invalid volume metadata: {0}")]
    InvalidMetadata(String),

    #[error("portalbd error: {0}")]
    Store(#[from] portalbd::Error),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        match &err {
            // NOT_FOUND: Volume does not exist
            Error::VolumeNotFound(_) => tonic::Status::not_found(err.to_string()),

            // ALREADY_EXISTS: Volume exists but with incompatible parameters
            Error::VolumeAlreadyExists(_) => tonic::Status::already_exists(err.to_string()),

            // RESOURCE_EXHAUSTED: No capacity available
            Error::NoAvailableNbdDevice | Error::VolumeLimitExceeded { .. } => {
                tonic::Status::resource_exhausted(err.to_string())
            }

            // FAILED_PRECONDITION: Operation cannot proceed in current state
            Error::VolumeNotStaged(_) => tonic::Status::failed_precondition(err.to_string()),

            // INVALID_ARGUMENT: Client provided invalid input
            Error::InvalidEndpoint { .. }
            | Error::InvalidAddress { .. }
            | Error::MissingCapability
            | Error::UnsupportedAccessMode
            | Error::TooManyMountOptions { .. } => tonic::Status::invalid_argument(err.to_string()),

            // INTERNAL: Infrastructure errors
            Error::NbdConnect { .. }
            | Error::NbdDisconnect { .. }
            | Error::Mount { .. }
            | Error::Unmount { .. }
            | Error::Format { .. }
            | Error::InvalidMetadata(_)
            | Error::Store(_)
            | Error::Io(_)
            | Error::Json(_) => tonic::Status::internal(err.to_string()),
        }
    }
}

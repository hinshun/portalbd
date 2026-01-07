//! NBD (Network Block Device) protocol implementation.
//!
//! This crate provides a pure Rust implementation of the NBD protocol,
//! including both client and protocol types for building NBD servers.
//!
//! Based on https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
//!
//! # Features
//!
//! - `device` - Enable Linux kernel NBD device support (requires root)

mod client;
mod protocol;

#[cfg(feature = "device")]
mod device;

pub use client::NbdClient;
pub use protocol::*;

#[cfg(feature = "device")]
pub use device::{NbdDevice, disconnect_device};

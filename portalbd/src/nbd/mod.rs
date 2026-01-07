//! NBD (Network Block Device) server implementation.
//!
//! This module wraps the `nbd` crate's protocol types with a portalbd-specific
//! server that uses `BlockStore` for storage via the `TransmissionHandler` trait.
//!
//! # Architecture
//!
//! - [`NbdServer`] handles a single NBD connection (handshake, negotiation, I/O)
//! - [`Listener`] trait abstracts over connection sources (TCP, Unix, channels)
//! - [`StreamListener`] accepts streams from a channel (for testing/benchmarks)
//!
//! For serving multiple connections, use `Daemon::listen()` which accepts any
//! `Listener` implementation.

mod handler;
mod listener;
mod server;

pub use handler::{BlockStoreHandler, HandlerResult, TransmissionHandler};
pub use listener::{Listener, StreamListener};
// Re-export protocol types from the nbd crate
pub use nbd::{NbdCommand, NbdReply, NbdRequest};
pub use server::{NbdExport, NbdServer};

//! Linux kernel NBD device support.
//!
//! This module provides functionality to connect and disconnect NBD block devices
//! using kernel ioctls, replacing the need for the `nbd-client` binary.
//!
//! # Example
//!
//! ```ignore
//! use nbd::NbdDevice;
//!
//! // Connect /dev/nbd0 to a server
//! let device = NbdDevice::connect("/dev/nbd0", "127.0.0.1", 10809, "export").await?;
//!
//! // The device is now available as a block device
//! // ...
//!
//! // Disconnect when done
//! device.disconnect()?;
//! ```

use std::fs::OpenOptions;
use std::io;
use std::net::TcpStream;
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::thread::JoinHandle;

use nix::libc;

use crate::NbdClient;
use crate::protocol::*;

// NBD ioctl numbers from linux/nbd.h (architecture-independent)
#[allow(dead_code)]
mod ioctl {
    use nix::libc;
    pub const NBD_SET_SOCK: libc::c_ulong = 0xab00;
    pub const NBD_SET_BLKSIZE: libc::c_ulong = 0xab01;
    pub const NBD_SET_SIZE: libc::c_ulong = 0xab02;
    pub const NBD_DO_IT: libc::c_ulong = 0xab03;
    pub const NBD_CLEAR_SOCK: libc::c_ulong = 0xab04;
    pub const NBD_CLEAR_QUE: libc::c_ulong = 0xab05;
    pub const NBD_SET_SIZE_BLOCKS: libc::c_ulong = 0xab07;
    pub const NBD_DISCONNECT: libc::c_ulong = 0xab08;
    pub const NBD_SET_TIMEOUT: libc::c_ulong = 0xab09;
    pub const NBD_SET_FLAGS: libc::c_ulong = 0xab0a;
}
use ioctl::*;

/// Default block size for NBD devices.
const DEFAULT_BLOCK_SIZE: u64 = 512;

/// A connected NBD device.
///
/// The device will be automatically disconnected when dropped.
pub struct NbdDevice {
    device_path: String,
    /// Kept open to maintain the fd valid for the DO_IT thread.
    _device_fd: OwnedFd,
    do_it_handle: Option<JoinHandle<io::Result<()>>>,
    _socket: TcpStream,
}

impl NbdDevice {
    /// Connect an NBD device to a remote server.
    ///
    /// This performs the full connection sequence:
    /// 1. Opens a TCP connection to the server
    /// 2. Performs the NBD protocol handshake
    /// 3. Configures the kernel NBD device via ioctls
    /// 4. Starts the device (NBD_DO_IT runs in a background thread)
    ///
    /// # Arguments
    ///
    /// * `device_path` - Path to the NBD device (e.g., "/dev/nbd0")
    /// * `host` - Server hostname or IP address
    /// * `port` - Server port
    /// * `export_name` - Name of the export to connect to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TCP connection fails
    /// - Protocol handshake fails
    /// - Device cannot be opened
    /// - Ioctl calls fail
    pub async fn connect(
        device_path: &str,
        host: &str,
        port: u16,
        export_name: &str,
    ) -> Result<Self, NbdError> {
        let device_path = device_path.to_string();
        let host = host.to_string();
        let export_name = export_name.to_string();

        // Run the blocking connection in spawn_blocking
        tokio::task::spawn_blocking(move || {
            Self::connect_sync(&device_path, &host, port, &export_name)
        })
        .await
        .map_err(|e| NbdError::Io(io::Error::other(format!("task join error: {e}"))))?
    }

    /// Synchronous version of connect for use in blocking contexts.
    fn connect_sync(
        device_path: &str,
        host: &str,
        port: u16,
        export_name: &str,
    ) -> Result<Self, NbdError> {
        // Connect to the NBD server
        let addr = format!("{host}:{port}");
        let socket = TcpStream::connect(&addr)?;
        socket.set_nodelay(true)?;

        // Clone the socket for the async handshake
        let handshake_socket = socket.try_clone()?;

        // Perform the handshake using tokio in a new runtime
        // We need a runtime because NbdClient::connect is async
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .map_err(|e| NbdError::Io(io::Error::other(format!("runtime error: {e}"))))?;

        let (size_bytes, _flags) = rt.block_on(async {
            let async_socket = tokio::net::TcpStream::from_std(handshake_socket)?;
            let client = NbdClient::connect(async_socket, export_name).await?;
            Ok::<_, NbdError>((client.size_bytes, client.transmission_flags))
        })?;

        // Open the NBD device
        let device_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(device_path)?;

        let device_fd = device_file.as_raw_fd();
        let socket_fd = socket.as_raw_fd();

        // Configure the device via ioctls
        unsafe {
            // Clear any previous state
            if libc::ioctl(device_fd, NBD_CLEAR_SOCK) < 0 {
                // Ignore error - device might not have been connected
            }

            // Set block size (use 512 for compatibility)
            if libc::ioctl(device_fd, NBD_SET_BLKSIZE, DEFAULT_BLOCK_SIZE) < 0 {
                return Err(NbdError::Io(io::Error::last_os_error()));
            }

            // Set device size
            if libc::ioctl(device_fd, NBD_SET_SIZE, size_bytes) < 0 {
                return Err(NbdError::Io(io::Error::last_os_error()));
            }

            // Set the socket
            if libc::ioctl(device_fd, NBD_SET_SOCK, socket_fd) < 0 {
                return Err(NbdError::Io(io::Error::last_os_error()));
            }
        }

        // Convert to OwnedFd to transfer ownership
        let device_fd = unsafe { OwnedFd::from_raw_fd(device_file.into_raw_fd()) };

        // Spawn a thread to run NBD_DO_IT (it blocks until disconnect)
        let do_it_fd = device_fd.as_raw_fd();
        let do_it_handle = std::thread::spawn(move || {
            unsafe {
                if libc::ioctl(do_it_fd, NBD_DO_IT) < 0 {
                    // ENOTCONN is expected when we disconnect
                    let err = io::Error::last_os_error();
                    if err.raw_os_error() != Some(libc::ENOTCONN) {
                        return Err(err);
                    }
                }
            }
            Ok(())
        });

        Ok(Self {
            device_path: device_path.to_string(),
            _device_fd: device_fd,
            do_it_handle: Some(do_it_handle),
            _socket: socket,
        })
    }

    /// Get the device path.
    pub fn device_path(&self) -> &str {
        &self.device_path
    }

    /// Disconnect the NBD device from the server.
    ///
    /// This sends the disconnect ioctl and waits for the device to stop.
    pub fn disconnect(mut self) -> Result<(), NbdError> {
        self.disconnect_inner()
    }

    fn disconnect_inner(&mut self) -> Result<(), NbdError> {
        // Follow the exact pattern from nbd-client disconnect():
        // Open the device fresh (like nbd-client -d does) and do the ioctls.
        // See: https://github.com/NetworkBlockDevice/nbd/blob/master/nbd-client.c
        //
        // nbd-client disconnect() opens the device fresh rather than reusing
        // the fd from the process running NBD_DO_IT.
        //
        // NOTE: There's a known kernel bug where systemd-udevd's inotify watching
        // can keep the device open and prevent proper cleanup. Workaround is to
        // add udev rule: ACTION=="add|change", KERNEL=="nbd*", OPTIONS:="nowatch"
        // See: https://bugs.launchpad.net/ubuntu/+source/linux/+bug/1896350

        let device_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.device_path)?;

        let fd = device_file.as_raw_fd();

        unsafe {
            // NBD_DISCONNECT tells kernel to send NBD_CMD_DISC to server
            libc::ioctl(fd, NBD_DISCONNECT);
            // NBD_CLEAR_SOCK clears socket and pending requests
            libc::ioctl(fd, NBD_CLEAR_SOCK);
        }

        // Don't wait for the DO_IT thread - it will exit on its own
        // when NBD_CLEAR_SOCK causes NBD_DO_IT to return
        let _ = self.do_it_handle.take();

        Ok(())
    }
}

impl Drop for NbdDevice {
    fn drop(&mut self) {
        // Best effort disconnect on drop
        let _ = self.disconnect_inner();
    }
}

/// Disconnect an NBD device by path.
///
/// This is a convenience function for disconnecting a device without
/// having an `NbdDevice` handle. Useful for cleanup scenarios.
///
/// Follows the same sequence as nbd-client disconnect():
/// 1. NBD_DISCONNECT - tells kernel to send NBD_CMD_DISC to server
/// 2. NBD_CLEAR_SOCK - clears socket and pending requests
pub fn disconnect_device(device_path: &str) -> Result<(), NbdError> {
    let device_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(device_path)?;

    let fd = device_file.as_raw_fd();

    unsafe {
        if libc::ioctl(fd, NBD_DISCONNECT) < 0 {
            // Ignore errors - device may already be disconnected
        }

        if libc::ioctl(fd, NBD_CLEAR_SOCK) < 0 {
            // Ignore errors during cleanup
        }
    }

    Ok(())
}

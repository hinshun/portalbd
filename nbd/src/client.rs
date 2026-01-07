//! NBD client implementation.
//!
//! This client implements the NBD protocol for connecting to NBD servers.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::protocol::*;

/// NBD client for connecting to NBD servers.
pub struct NbdClient<S> {
    stream: S,
    handle_counter: AtomicU64,
    /// Size of the export in bytes.
    pub size_bytes: u64,
    /// Transmission flags advertised by the server.
    pub transmission_flags: u16,
}

impl<S> NbdClient<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Connect to an NBD server and negotiate the export.
    pub async fn connect(mut stream: S, export_name: &str) -> Result<Self, NbdError> {
        // Read server hello
        let mut hello = [0u8; 18];
        stream.read_exact(&mut hello).await?;

        let magic = u64::from_be_bytes(hello[0..8].try_into().unwrap());
        if magic != NBD_MAGIC {
            return Err(NbdError::InvalidMagic {
                expected: NBD_MAGIC as u32,
                actual: magic as u32,
            });
        }

        let opts_magic = u64::from_be_bytes(hello[8..16].try_into().unwrap());
        if opts_magic != NBD_OPTS_MAGIC {
            return Err(NbdError::NegotiationFailed {
                reason: "invalid opts magic",
            });
        }

        let flags = u16::from_be_bytes(hello[16..18].try_into().unwrap());
        let no_zeroes = (flags & NBD_FLAG_NO_ZEROES) != 0;

        // Send client flags
        let client_flags =
            NBD_FLAG_C_FIXED_NEWSTYLE | if no_zeroes { NBD_FLAG_C_NO_ZEROES } else { 0 };
        stream.write_all(&client_flags.to_be_bytes()).await?;

        // Send NBD_OPT_GO
        let name_bytes = export_name.as_bytes();
        let opt_data_len = 4 + name_bytes.len() + 2; // name_len + name + info_count

        let mut opt_header = [0u8; 16];
        opt_header[0..8].copy_from_slice(&NBD_OPTS_MAGIC.to_be_bytes());
        opt_header[8..12].copy_from_slice(&NBD_OPT_GO.to_be_bytes());
        opt_header[12..16].copy_from_slice(&(opt_data_len as u32).to_be_bytes());
        stream.write_all(&opt_header).await?;

        // Write option data: name length, name, info request count (0)
        stream
            .write_all(&(name_bytes.len() as u32).to_be_bytes())
            .await?;
        stream.write_all(name_bytes).await?;
        stream.write_all(&0u16.to_be_bytes()).await?; // no info requests

        // Read option replies until we get NBD_REP_ACK
        let mut size_bytes = 0u64;
        let mut transmission_flags = 0u16;
        loop {
            let mut reply_header = [0u8; 20];
            stream.read_exact(&mut reply_header).await?;

            let reply_type = u32::from_be_bytes(reply_header[12..16].try_into().unwrap());
            let reply_len = u32::from_be_bytes(reply_header[16..20].try_into().unwrap()) as usize;

            // Bound allocation to prevent DoS from malicious servers
            if reply_len > OPTION_REPLY_MAX_BYTES {
                return Err(NbdError::RequestTooLarge {
                    length_bytes: reply_len as u32,
                    max_bytes: OPTION_REPLY_MAX_BYTES as u32,
                });
            }

            let mut reply_data = vec![0u8; reply_len];
            stream.read_exact(&mut reply_data).await?;

            if reply_type == NBD_REP_INFO && reply_len >= 12 {
                let info_type = u16::from_be_bytes(reply_data[0..2].try_into().unwrap());
                if info_type == NBD_INFO_EXPORT {
                    // Extract size and flags from INFO_EXPORT
                    size_bytes = u64::from_be_bytes(reply_data[2..10].try_into().unwrap());
                    transmission_flags = u16::from_be_bytes(reply_data[10..12].try_into().unwrap());
                }
            } else if reply_type == NBD_REP_ACK {
                break;
            } else if reply_type >= 0x80000000 {
                return Err(NbdError::NegotiationFailed {
                    reason: "option negotiation failed",
                });
            }
        }

        Ok(Self {
            stream,
            handle_counter: AtomicU64::new(0),
            size_bytes,
            transmission_flags,
        })
    }

    fn next_handle(&self) -> u64 {
        self.handle_counter.fetch_add(1, Ordering::SeqCst)
    }

    async fn send_request(
        &mut self,
        command: NbdCommand,
        offset: u64,
        length: u32,
    ) -> Result<u64, NbdError> {
        let handle = self.next_handle();
        let req = NbdRequest {
            flags: 0,
            command,
            handle,
            offset,
            length,
        };
        self.stream.write_all(&req.to_bytes()).await?;
        Ok(handle)
    }

    async fn read_reply(&mut self) -> Result<NbdReply, NbdError> {
        let mut buf = [0u8; NbdReply::SIZE_BYTES];
        self.stream.read_exact(&mut buf).await?;
        NbdReply::from_bytes(&buf)
    }

    async fn execute_request(
        &mut self,
        command: NbdCommand,
        offset: u64,
        length: u32,
    ) -> Result<(), NbdError> {
        let handle = self.send_request(command, offset, length).await?;
        let reply = self.read_reply().await?;

        if reply.handle != handle {
            return Err(NbdError::HandleMismatch {
                expected: handle,
                actual: reply.handle,
            });
        }
        if reply.error != NBD_OK {
            return Err(NbdError::ServerError { code: reply.error });
        }
        Ok(())
    }

    /// Read data from the device.
    pub async fn read(&mut self, offset: u64, length: u32) -> Result<Bytes, NbdError> {
        let handle = self.send_request(NbdCommand::Read, offset, length).await?;
        let reply = self.read_reply().await?;

        if reply.handle != handle {
            return Err(NbdError::HandleMismatch {
                expected: handle,
                actual: reply.handle,
            });
        }
        if reply.error != NBD_OK {
            return Err(NbdError::ServerError { code: reply.error });
        }

        let mut data = BytesMut::with_capacity(length as usize);
        data.resize(length as usize, 0);
        self.stream.read_exact(&mut data).await.map_err(|e| {
            NbdError::Io(io::Error::other(format!(
                "failed to read {} bytes of response data at offset {}: {}",
                length, offset, e
            )))
        })?;
        Ok(data.freeze())
    }

    /// Write data to the device.
    pub async fn write(&mut self, offset: u64, data: &[u8]) -> Result<(), NbdError> {
        let handle = self
            .send_request(NbdCommand::Write, offset, data.len() as u32)
            .await?;
        self.stream.write_all(data).await?;

        let reply = self.read_reply().await?;
        if reply.handle != handle {
            return Err(NbdError::HandleMismatch {
                expected: handle,
                actual: reply.handle,
            });
        }
        if reply.error != NBD_OK {
            return Err(NbdError::ServerError { code: reply.error });
        }
        Ok(())
    }

    /// Flush pending writes.
    pub async fn flush(&mut self) -> Result<(), NbdError> {
        self.execute_request(NbdCommand::Flush, 0, 0).await
    }

    /// Trim a range (hint that data is no longer needed).
    ///
    /// For ranges larger than `u32::MAX`, split into multiple requests.
    pub async fn trim(&mut self, offset: u64, length: u32) -> Result<(), NbdError> {
        self.execute_request(NbdCommand::Trim, offset, length).await
    }

    /// Write zeroes to a range.
    ///
    /// For ranges larger than `u32::MAX`, split into multiple requests.
    pub async fn write_zeroes(&mut self, offset: u64, length: u32) -> Result<(), NbdError> {
        self.execute_request(NbdCommand::WriteZeroes, offset, length)
            .await
    }

    /// Disconnect from the server.
    pub async fn disconnect(mut self) -> Result<(), NbdError> {
        self.send_request(NbdCommand::Disconnect, 0, 0).await?;
        Ok(())
    }

    /// Check if the server supports flush.
    pub fn supports_flush(&self) -> bool {
        (self.transmission_flags & NBD_FLAG_SEND_FLUSH) != 0
    }

    /// Check if the server supports trim.
    pub fn supports_trim(&self) -> bool {
        (self.transmission_flags & NBD_FLAG_SEND_TRIM) != 0
    }

    /// Check if the server supports write zeroes.
    pub fn supports_write_zeroes(&self) -> bool {
        (self.transmission_flags & NBD_FLAG_SEND_WRITE_ZEROES) != 0
    }

    /// Check if the export is read-only.
    pub fn is_read_only(&self) -> bool {
        (self.transmission_flags & NBD_FLAG_READ_ONLY) != 0
    }
}
